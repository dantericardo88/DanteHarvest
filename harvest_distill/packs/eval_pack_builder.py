"""
EvalPackBuilder — build EvalPack from WorkflowPack + test cases.

PRD gap: the evalPack schema existed but no builder was implemented.

Converts a promoted WorkflowPack into an EvalPack by:
1. Generating TaskCase entries from each pack step
2. Attaching success_metrics
3. Sealing with a pack.created chain entry

Fail-closed: empty task_cases raises PackagingError.
Zero-ambiguity: no default metrics injected — caller must provide them.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import PackagingError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_distill.packs.pack_schemas import (
    EvalPack,
    PackType,
    PromotionStatus,
    TaskCase,
    WorkflowPack,
)


class EvalPackBuilder:
    """
    Build an EvalPack from a WorkflowPack.

    Usage:
        builder = EvalPackBuilder(chain_writer)
        task_cases = [
            TaskCase(case_id="tc-1", description="Submit invoice",
                     expected_outputs=["confirmation_number"]),
        ]
        eval_pack = await builder.build(
            workflow_pack=pack,
            task_cases=task_cases,
            success_metrics={"min_pass_rate": 0.85},
            run_id="run-001",
        )
    """

    def __init__(self, chain_writer: Optional[ChainWriter] = None):
        self.chain_writer = chain_writer

    async def build(
        self,
        workflow_pack: WorkflowPack,
        task_cases: List[TaskCase],
        success_metrics: Dict[str, Any],
        run_id: str,
        benchmark_name: Optional[str] = None,
        replay_environment: Optional[str] = None,
    ) -> EvalPack:
        """
        Build an EvalPack.
        Raises PackagingError if task_cases is empty (fail-closed).
        """
        if not task_cases:
            raise PackagingError(
                f"Cannot build EvalPack from WorkflowPack {workflow_pack.pack_id}: "
                "task_cases must not be empty"
            )

        pack_id = str(uuid4())
        name = benchmark_name or f"eval_{workflow_pack.title.lower().replace(' ', '_')}"

        pack = EvalPack(
            pack_id=pack_id,
            benchmark_name=name,
            task_cases=task_cases,
            success_metrics=success_metrics,
            replay_environment=replay_environment,
            promotion_status=PromotionStatus.CANDIDATE,
        )

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="eval_pack.created",
                machine="eval_pack_builder",
                data={
                    "pack_id": pack_id,
                    "benchmark_name": name,
                    "task_case_count": len(task_cases),
                    "source_workflow_id": workflow_pack.pack_id,
                },
            ))

        return pack

    @staticmethod
    def cases_from_workflow(workflow_pack: WorkflowPack) -> List[TaskCase]:
        """
        Auto-generate one TaskCase per WorkflowPack step.
        Useful for bootstrapping an EvalPack from an existing workflow.
        """
        return [
            TaskCase(
                case_id=f"tc-{i}",
                description=f"Execute step: {step.action}",
                oracle_rules=[f"step {step.id} must complete without error"],
            )
            for i, step in enumerate(workflow_pack.steps)
        ]
