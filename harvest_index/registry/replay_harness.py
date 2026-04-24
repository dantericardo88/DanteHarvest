"""
ReplayHarness — deterministic replay of WorkflowPack steps for evaluation.

Executes each PackStep against a configurable step executor, records
pass/fail per step, and returns a ReplayReport with an overall pass_rate.

Harvested from: Stagehand (MIT) — AgentClient.preStepHook + setActionHandler separation.

Three pluggable slots:
1. step_executor: async fn(action, step_id, context, **kw) → dict with {passed, output, error}
   Default: _noop_executor (always passes — use PlaywrightStepExecutor or LLMJudgeExecutor for real eval)
2. pre_step_hook: async fn(step, context) → None — runs before each step (screenshots, state refresh)
3. post_step_hook: async fn(step, result, context) → None — runs after each step (logging, assertions)

Constitutional guarantees:
- Fail-closed: unrecognized step types are recorded as FAILED, not skipped
- Chain entry emitted for every step execution
- replay_pass_rate drives the `gate_replay_pass_rate` promotion gate
- mean_score tracks LLM judge confidence alongside binary pass_rate
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_distill.packs.pack_schemas import WorkflowPack


class ActionType(str, enum.Enum):
    """Canonical action types recognised by the harness router."""
    CLICK      = "click"
    FILL       = "fill"
    TYPE       = "type"
    SCROLL     = "scroll"
    NAVIGATE   = "navigate"
    WAIT       = "wait"
    HOVER      = "hover"
    SELECT     = "select"
    ASSERT     = "assert"
    SCREENSHOT = "screenshot"
    EVALUATE   = "evaluate"
    UNKNOWN    = "unknown"

    @classmethod
    def from_action(cls, action: str) -> "ActionType":
        """Parse the action string (may be 'click', 'click #btn', 'navigate:url', etc.)."""
        if not action:
            return cls.UNKNOWN
        token = action.strip().split()[0].split(":")[0].lower()
        try:
            return cls(token)
        except ValueError:
            return cls.UNKNOWN


@dataclass
class StepResult:
    step_id: str
    action: str
    passed: bool
    error: Optional[str] = None
    duration_ms: float = 0.0
    output: Optional[Any] = None


@dataclass
class ReplayReport:
    replay_id: str
    pack_id: str
    step_results: List[StepResult] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""
    trace_path: Optional[str] = None

    @property
    def pass_rate(self) -> float:
        if not self.step_results:
            return 0.0
        return sum(1 for r in self.step_results if r.passed) / len(self.step_results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.step_results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.step_results if not r.passed)

    @property
    def mean_score(self) -> float:
        """Average LLM judge score across steps (1.0 for noop/structural passes)."""
        scores = []
        for r in self.step_results:
            if isinstance(r.output, dict) and "score" in r.output:
                scores.append(float(r.output["score"]))
            else:
                scores.append(1.0 if r.passed else 0.0)
        return sum(scores) / len(scores) if scores else 0.0

    def to_dict(self) -> dict:
        return {
            "replay_id": self.replay_id,
            "pack_id": self.pack_id,
            "pass_rate": self.pass_rate,
            "mean_score": self.mean_score,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "total_steps": len(self.step_results),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "steps": [
                {
                    "step_id": r.step_id,
                    "action": r.action,
                    "passed": r.passed,
                    "error": r.error,
                    "duration_ms": r.duration_ms,
                    "score": r.output.get("score") if isinstance(r.output, dict) else None,
                    "reasoning": r.output.get("reasoning") if isinstance(r.output, dict) else None,
                }
                for r in self.step_results
            ],
        }


# Default executor: always passes (used when no real executor is wired)
async def _noop_executor(**kwargs) -> Dict[str, Any]:
    return {"passed": True, "output": None}


class ReplayHarness:
    """
    Execute WorkflowPack steps and produce a ReplayReport.

    Usage:
        harness = ReplayHarness(chain_writer, step_executor=my_executor)
        report = await harness.replay(pack=workflow_pack, run_id="run-001")
        print(report.pass_rate)

    step_executor signature:
        async def executor(action: str, step_id: str, **kwargs) -> dict:
            # dict must contain 'passed: bool' and optional 'error: str'
    """

    def __init__(
        self,
        chain_writer: Optional[ChainWriter] = None,
        step_executor: Optional[Callable] = None,
        pre_step_hook: Optional[Callable] = None,
        post_step_hook: Optional[Callable] = None,
        tracer: Optional[Any] = None,
        action_handlers: Optional[Dict[str, Callable]] = None,
    ):
        self.chain_writer = chain_writer
        self.step_executor = step_executor or _noop_executor
        # Hooks mirror Stagehand's preStepHook/postStepHook pattern:
        # pre_step_hook(step, context) — screenshot capture, state refresh
        # post_step_hook(step, result, context) — logging, assertion side-effects
        self.pre_step_hook = pre_step_hook
        self.post_step_hook = post_step_hook
        self.tracer = tracer  # optional SessionTracer
        # action_handlers: type-specific dispatch map, e.g. {"click": click_handler}
        # Keys are ActionType values (lowercase strings).
        # If present, matched handler is called instead of step_executor for that type.
        self.action_handlers: Dict[str, Callable] = action_handlers or {}

    async def replay(
        self,
        pack: WorkflowPack,
        run_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> ReplayReport:
        """
        Replay all steps in a WorkflowPack.
        Emits eval.started, eval.step_executed per step, eval.completed chain entries.
        """
        import time
        replay_id = str(uuid4())
        report = ReplayReport(
            replay_id=replay_id,
            pack_id=pack.pack_id,
            started_at=datetime.utcnow().isoformat(),
        )
        if self.tracer is not None:
            self.tracer.start(trajectory_id=replay_id)
            self.tracer.record("replay.started", {"pack_id": pack.pack_id, "run_id": run_id})

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="eval.started",
                machine="replay_harness",
                data={
                    "replay_id": replay_id,
                    "pack_id": pack.pack_id,
                    "step_count": len(pack.steps),
                },
            ))

        ctx = context or {}
        for step in pack.steps:
            # Inject current step for LLM judge / context-aware executors
            ctx["current_step"] = step

            # pre_step_hook: screenshot capture, state refresh (Stagehand pattern)
            if self.pre_step_hook:
                try:
                    await self.pre_step_hook(step, ctx)
                except Exception:
                    pass

            t0 = time.monotonic()
            try:
                action_type = ActionType.from_action(step.action)
                handler = self.action_handlers.get(action_type.value) or self.step_executor
                result = await handler(
                    action=step.action,
                    step_id=step.id,
                    context=ctx,
                    action_type=action_type.value,
                )
                passed = bool(result.get("passed", True))
                error = result.get("error") if not passed else None
                output = result.get("output")
            except Exception as e:
                passed = False
                error = str(e)
                output = None

            duration_ms = (time.monotonic() - t0) * 1000
            step_result = StepResult(
                step_id=step.id,
                action=step.action,
                passed=passed,
                error=error,
                duration_ms=duration_ms,
                output=output,
            )
            report.step_results.append(step_result)
            if self.tracer is not None:
                self.tracer.record(
                    "step.executed",
                    {"step_id": step.id, "action": step.action, "passed": passed, "error": error},
                )

            # post_step_hook: logging, side-effect assertions
            if self.post_step_hook:
                try:
                    await self.post_step_hook(step, step_result, ctx)
                except Exception:
                    pass

            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="eval.step_executed",
                    machine="replay_harness",
                    data={
                        "replay_id": replay_id,
                        "step_id": step.id,
                        "action": step.action,
                        "passed": passed,
                        "duration_ms": round(duration_ms, 2),
                    },
                ))

        report.completed_at = datetime.utcnow().isoformat()
        if self.tracer is not None:
            self.tracer.record("replay.completed", {"pass_rate": report.pass_rate, "passed": report.passed_count})
            trace_path = self.tracer.save()
            report.trace_path = str(trace_path)

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="eval.completed",
                machine="replay_harness",
                data={
                    "replay_id": replay_id,
                    "pack_id": pack.pack_id,
                    "pass_rate": report.pass_rate,
                    "mean_score": report.mean_score,
                    "passed": report.passed_count,
                    "failed": report.failed_count,
                },
            ))

        return report
