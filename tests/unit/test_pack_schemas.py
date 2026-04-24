"""Unit tests for pack schemas."""

import pytest

from harvest_distill.packs.pack_schemas import (
    EvalPack,
    PackType,
    PromotionStatus,
    SkillPack,
    SpecializationPack,
    TaskCase,
    WorkflowPack,
    PackStep,
    EvalSummary,
)


class TestWorkflowPack:
    def test_creates_with_minimal_fields(self):
        pack = WorkflowPack(
            pack_id="wf-001",
            title="Test Workflow",
            goal="Do the thing",
        )
        assert pack.pack_type == PackType.WORKFLOW
        assert pack.promotion_status == PromotionStatus.CANDIDATE

    def test_steps_and_eval(self):
        pack = WorkflowPack(
            pack_id="wf-002",
            title="Invoice reconciliation",
            goal="Match invoices to ledger",
            steps=[PackStep(id="s1", action="Open QuickBooks", evidence_refs=["seg_014"])],
            eval_summary=EvalSummary(replay_pass_rate=0.94, sample_size=18),
        )
        assert len(pack.steps) == 1
        assert pack.eval_summary.replay_pass_rate == 0.94

    def test_serializes_to_dict(self):
        pack = WorkflowPack(pack_id="wf-003", title="T", goal="G")
        d = pack.model_dump()
        assert d["pack_id"] == "wf-003"


class TestSkillPack:
    def test_creates_with_required_fields(self):
        skill = SkillPack(
            pack_id="sk-001",
            skill_name="click_submit",
            trigger_context="form visible",
            action_template="click(selector=submit)",
        )
        assert skill.pack_type == PackType.SKILL


class TestSpecializationPack:
    def test_creates_with_domain(self):
        spec = SpecializationPack(pack_id="sp-001", domain="accounting")
        assert spec.domain == "accounting"
        assert spec.pack_type == PackType.SPECIALIZATION


class TestEvalPack:
    def test_creates_with_task_cases(self):
        pack = EvalPack(
            pack_id="ev-001",
            benchmark_name="invoice-reconciliation-bench",
            task_cases=[
                TaskCase(
                    case_id="tc-001",
                    description="Reconcile single invoice",
                    expected_outputs=["matched"],
                )
            ],
        )
        assert len(pack.task_cases) == 1
        assert pack.pack_type == PackType.EVAL
