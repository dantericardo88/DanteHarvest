"""
PackBuilder — assemble WorkflowPack and SkillPack from ProcedureGraphs.

Converts distilled ProcedureGraphs into typed Pack schemas ready for
promotion.  Emits pack.created chain entry.
Fail-closed: empty graphs raise PackagingError.
"""

from __future__ import annotations

from typing import Optional
from uuid import uuid4

from harvest_core.control.exceptions import PackagingError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_distill.packs.pack_schemas import (
    EvalSummary,
    PackStep,
    PackType,
    PromotionStatus,
    SkillPack,
    WorkflowPack,
)
from harvest_distill.procedures.procedure_inferrer import ProcedureGraph


class PackBuilder:
    """
    Build WorkflowPack and SkillPack from ProcedureGraph + evidence.

    Usage:
        builder = PackBuilder(chain_writer)
        pack = await builder.build_workflow_pack(graph, run_id, project_id)
    """

    def __init__(self, chain_writer: Optional[ChainWriter] = None):
        self.chain_writer = chain_writer

    async def build_workflow_pack(
        self,
        graph: ProcedureGraph,
        run_id: str,
        project_id: str,
        goal: str = "",
        receipt_id: Optional[str] = None,
        replay_pass_rate: float = 0.0,
        sample_size: int = 1,
    ) -> WorkflowPack:
        if not graph.steps:
            raise PackagingError(
                f"Cannot build WorkflowPack from empty ProcedureGraph {graph.graph_id}"
            )

        pack_id = str(uuid4())
        steps = [
            PackStep(
                id=s.step_id,
                action=s.action_type,
                evidence_refs=s.evidence_refs,
            )
            for s in graph.steps
        ]

        pack = WorkflowPack(
            pack_id=pack_id,
            title=graph.title,
            goal=goal or f"Execute workflow: {graph.title}",
            steps=steps,
            evidence_refs=graph.source_span_ids,
            promotion_status=PromotionStatus.CANDIDATE,
            eval_summary=EvalSummary(
                replay_pass_rate=replay_pass_rate,
                sample_size=sample_size,
            ),
        )

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="pack.created",
                machine="pack_builder",
                data={
                    "pack_id": pack_id,
                    "pack_type": PackType.WORKFLOW.value,
                    "project_id": project_id,
                    "step_count": len(steps),
                    "confidence": graph.confidence,
                    "receipt_id": receipt_id or "",
                },
            ))

        return pack

    async def build_skill_pack(
        self,
        graph: ProcedureGraph,
        run_id: str,
        project_id: str,
        skill_name: str,
        trigger_context: str = "",
        action_template: str = "",
        receipt_id: Optional[str] = None,
    ) -> SkillPack:
        if not graph.steps:
            raise PackagingError(
                f"Cannot build SkillPack from empty ProcedureGraph {graph.graph_id}"
            )

        pack_id = str(uuid4())

        pack = SkillPack(
            pack_id=pack_id,
            skill_name=skill_name,
            trigger_context=trigger_context or f"When performing: {graph.title}",
            action_template=action_template or " → ".join(s.action_type for s in graph.steps),
            evidence_refs=graph.source_span_ids,
            promotion_status=PromotionStatus.CANDIDATE,
        )

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="pack.created",
                machine="pack_builder",
                data={
                    "pack_id": pack_id,
                    "pack_type": PackType.SKILL.value,
                    "project_id": project_id,
                    "skill_name": skill_name,
                    "confidence": graph.confidence,
                    "receipt_id": receipt_id or "",
                },
            ))

        return pack
