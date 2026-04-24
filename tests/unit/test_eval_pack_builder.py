"""Tests for EvalPackBuilder — EvalPack construction from WorkflowPack."""

import pytest
from harvest_core.control.exceptions import PackagingError
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_distill.packs.eval_pack_builder import EvalPackBuilder
from harvest_distill.packs.pack_schemas import (
    PackStep,
    TaskCase,
    WorkflowPack,
)


def make_workflow_pack(step_count: int = 2) -> WorkflowPack:
    return WorkflowPack(
        pack_id="wf-001",
        title="Invoice Workflow",
        goal="Process invoices end to end",
        steps=[
            PackStep(id=f"s{i}", action=f"Step {i} action")
            for i in range(step_count)
        ],
    )


def make_task_cases(n: int = 2):
    return [
        TaskCase(
            case_id=f"tc-{i}",
            description=f"Test case {i}",
            oracle_rules=[f"step {i} must complete"],
        )
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_successful_build():
    builder = EvalPackBuilder()
    pack = await builder.build(
        workflow_pack=make_workflow_pack(),
        task_cases=make_task_cases(2),
        success_metrics={"min_pass_rate": 0.85},
        run_id="run-001",
    )
    assert pack.pack_id is not None
    assert len(pack.task_cases) == 2
    assert pack.success_metrics["min_pass_rate"] == 0.85


@pytest.mark.asyncio
async def test_empty_task_cases_raises():
    builder = EvalPackBuilder()
    with pytest.raises(PackagingError, match="task_cases must not be empty"):
        await builder.build(
            workflow_pack=make_workflow_pack(),
            task_cases=[],
            success_metrics={"min_pass_rate": 0.9},
            run_id="run-001",
        )


@pytest.mark.asyncio
async def test_chain_signal_emitted(tmp_path):
    writer = ChainWriter(tmp_path / "chain.jsonl", "run-002")
    builder = EvalPackBuilder(chain_writer=writer)
    await builder.build(
        workflow_pack=make_workflow_pack(),
        task_cases=make_task_cases(3),
        success_metrics={"min_pass_rate": 0.8},
        run_id="run-002",
    )
    entries = writer.read_all()
    signals = [e.signal for e in entries]
    assert "eval_pack.created" in signals


@pytest.mark.asyncio
async def test_json_serializable():
    builder = EvalPackBuilder()
    pack = await builder.build(
        workflow_pack=make_workflow_pack(),
        task_cases=make_task_cases(1),
        success_metrics={"min_pass_rate": 0.9},
        run_id="run-003",
    )
    data = pack.model_dump()
    assert isinstance(data, dict)
    assert data["benchmark_name"] is not None


@pytest.mark.asyncio
async def test_success_metrics_passthrough():
    builder = EvalPackBuilder()
    metrics = {"min_pass_rate": 0.95, "max_latency_ms": 2000, "error_budget": 0.05}
    pack = await builder.build(
        workflow_pack=make_workflow_pack(),
        task_cases=make_task_cases(1),
        success_metrics=metrics,
        run_id="run-004",
    )
    assert pack.success_metrics == metrics


def test_cases_from_workflow():
    workflow = make_workflow_pack(step_count=3)
    cases = EvalPackBuilder.cases_from_workflow(workflow)
    assert len(cases) == 3
    assert all(c.case_id.startswith("tc-") for c in cases)
    assert all("must complete without error" in c.oracle_rules[0] for c in cases)
