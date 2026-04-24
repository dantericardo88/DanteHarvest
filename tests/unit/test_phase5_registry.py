"""Unit tests for Phase 5 — PackRegistry and ReplayHarness."""

import pytest
from harvest_index.registry.pack_registry import PackRegistry, RegistryError
from harvest_index.registry.replay_harness import ReplayHarness
from harvest_distill.packs.pack_schemas import (
    WorkflowPack, SkillPack, PackStep, EvalSummary, PackType,
)
from harvest_core.provenance.chain_writer import ChainWriter


def _make_workflow_pack(title: str = "Test Workflow") -> WorkflowPack:
    from uuid import uuid4
    return WorkflowPack(
        pack_id=str(uuid4()),
        title=title,
        goal="test goal",
        steps=[PackStep(id="s1", action="click #btn")],
        eval_summary=EvalSummary(replay_pass_rate=0.9, sample_size=10),
    )


def _make_skill_pack(skill_name: str = "test_skill") -> SkillPack:
    from uuid import uuid4
    return SkillPack(
        pack_id=str(uuid4()),
        skill_name=skill_name,
        trigger_context="when button visible",
        action_template="click the button",
    )


class TestPackRegistry:
    def _registry(self, tmp_path) -> PackRegistry:
        return PackRegistry(root=str(tmp_path / "reg"))

    def test_register_workflow_pack(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        entry = reg.register(pack, receipt_id="receipt-001")
        assert entry.pack_id == pack.pack_id
        assert entry.promotion_status == "candidate"
        assert entry.receipt_id == "receipt-001"

    def test_register_skill_pack(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_skill_pack()
        entry = reg.register(pack)
        assert entry.pack_id == pack.pack_id
        assert entry.pack_type == PackType.SKILL.value

    def test_promote_requires_receipt(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        entry = reg.register(pack, receipt_id=None)
        with pytest.raises(RegistryError, match="[Rr]eceipt"):
            reg.promote(entry.pack_id)

    def test_promote_with_receipt_succeeds(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        entry = reg.register(pack, receipt_id="r-001")
        promoted = reg.promote(entry.pack_id)
        assert promoted.promotion_status == "promoted"

    def test_promote_can_attach_receipt_at_review_time(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        entry = reg.register(pack, receipt_id=None)
        promoted = reg.promote(entry.pack_id, receipt_id="r-review")
        assert promoted.promotion_status == "promoted"
        assert promoted.receipt_id == "r-review"

    def test_promote_already_promoted_raises(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        reg.register(pack, receipt_id="r-001")
        reg.promote(pack.pack_id)
        with pytest.raises(RegistryError):
            reg.promote(pack.pack_id)

    def test_reject_sets_status(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        reg.register(pack)
        entry = reg.reject(pack.pack_id, reason="low quality")
        assert entry.promotion_status == "rejected"

    def test_get_nonexistent_raises(self, tmp_path):
        reg = self._registry(tmp_path)
        with pytest.raises(RegistryError):
            reg.get("nonexistent-id")

    def test_list_by_status(self, tmp_path):
        reg = self._registry(tmp_path)
        p1 = _make_workflow_pack("A")
        p2 = _make_workflow_pack("B")
        reg.register(p1, receipt_id="r1")
        reg.register(p2, receipt_id="r2")
        reg.promote(p1.pack_id)

        promoted = reg.list(status="promoted")
        candidates = reg.list(status="candidate")
        assert len(promoted) == 1
        assert len(candidates) == 1

    def test_list_by_pack_type(self, tmp_path):
        reg = self._registry(tmp_path)
        reg.register(_make_workflow_pack())
        reg.register(_make_skill_pack())
        workflows = reg.list(pack_type=PackType.WORKFLOW.value)
        assert len(workflows) == 1

    def test_load_pack_json(self, tmp_path):
        reg = self._registry(tmp_path)
        pack = _make_workflow_pack()
        reg.register(pack)
        raw = reg.load_pack_json(pack.pack_id)
        assert raw["pack_id"] == pack.pack_id

    def test_index_persists(self, tmp_path):
        root = str(tmp_path / "reg")
        reg1 = PackRegistry(root=root)
        reg1.register(_make_workflow_pack())
        reg2 = PackRegistry(root=root)
        assert len(reg2.list()) == 1

    def test_stats(self, tmp_path):
        reg = self._registry(tmp_path)
        reg.register(_make_workflow_pack(), receipt_id="r1")
        reg.promote(_make_workflow_pack().pack_id) if False else None
        stats = reg.stats()
        assert stats["total"] >= 1
        assert "by_status" in stats


class TestReplayHarness:
    @pytest.mark.asyncio
    async def test_noop_executor_all_pass(self):
        pack = _make_workflow_pack()
        pack.steps = [PackStep(id="s1", action="click"), PackStep(id="s2", action="type")]
        harness = ReplayHarness()
        report = await harness.replay(pack, run_id="r1")
        assert report.pass_rate == 1.0
        assert report.passed_count == 2
        assert report.failed_count == 0

    @pytest.mark.asyncio
    async def test_failing_executor_records_failures(self):
        async def always_fail(**kwargs):
            return {"passed": False, "error": "step failed"}

        pack = _make_workflow_pack()
        harness = ReplayHarness(step_executor=always_fail)
        report = await harness.replay(pack, run_id="r1")
        assert report.pass_rate == 0.0
        assert report.failed_count == 1

    @pytest.mark.asyncio
    async def test_executor_exception_recorded_as_failure(self):
        async def crash(*args, **kwargs):
            raise RuntimeError("boom")

        pack = _make_workflow_pack()
        harness = ReplayHarness(step_executor=crash)
        report = await harness.replay(pack, run_id="r1")
        assert report.step_results[0].passed is False
        assert "boom" in (report.step_results[0].error or "")

    @pytest.mark.asyncio
    async def test_emits_chain_signals(self, tmp_path):
        writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
        pack = _make_workflow_pack()
        harness = ReplayHarness(chain_writer=writer)
        await harness.replay(pack, run_id="r1")
        signals = [e.signal for e in writer.read_all()]
        assert "eval.started" in signals
        assert "eval.step_executed" in signals
        assert "eval.completed" in signals

    @pytest.mark.asyncio
    async def test_report_to_dict_structure(self):
        pack = _make_workflow_pack()
        harness = ReplayHarness()
        report = await harness.replay(pack, run_id="r1")
        d = report.to_dict()
        assert "pass_rate" in d
        assert "steps" in d
        assert d["pack_id"] == pack.pack_id

    @pytest.mark.asyncio
    async def test_empty_pack_pass_rate_zero(self):
        pack = _make_workflow_pack()
        pack.steps = []
        harness = ReplayHarness()
        report = await harness.replay(pack, run_id="r1")
        assert report.pass_rate == 0.0
