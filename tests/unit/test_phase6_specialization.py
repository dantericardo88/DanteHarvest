"""Unit tests for Phase 6 — SpecializationPackBuilder and DanteAgents export."""

import pytest
from harvest_distill.packs.specialization_builder import SpecializationPackBuilder
from harvest_distill.packs.dante_agents_contract import DanteAgentsExporter, HarvestHandoff
from harvest_index.registry.pack_registry import PackRegistry
from harvest_distill.packs.pack_schemas import (
    WorkflowPack, PackStep, EvalSummary, PackType, PromotionStatus,
)
from harvest_core.control.exceptions import PackagingError


def _promoted_workflow(registry: PackRegistry) -> str:
    from uuid import uuid4
    pack = WorkflowPack(
        pack_id=str(uuid4()),
        title="Test Workflow",
        goal="do something",
        steps=[PackStep(id="s1", action="click")],
        eval_summary=EvalSummary(replay_pass_rate=0.9, sample_size=5),
    )
    registry.register(pack, receipt_id="r-001")
    registry.promote(pack.pack_id)
    return pack.pack_id


class TestSpecializationPackBuilder:
    @pytest.mark.asyncio
    async def test_build_without_registry(self):
        builder = SpecializationPackBuilder()
        spec = await builder.build(
            domain="accounting",
            run_id="r1",
            glossary={"GL": "General Ledger"},
        )
        assert spec.domain == "accounting"
        assert spec.glossary["GL"] == "General Ledger"
        assert spec.pack_id

    @pytest.mark.asyncio
    async def test_build_with_promoted_pack_refs(self, tmp_path):
        registry = PackRegistry(root=str(tmp_path / "reg"))
        wf_id = _promoted_workflow(registry)

        builder = SpecializationPackBuilder(registry=registry)
        spec = await builder.build(
            domain="finance",
            run_id="r1",
            workflow_pack_ids=[wf_id],
        )
        assert wf_id in spec.workflow_refs

    @pytest.mark.asyncio
    async def test_build_rejects_non_promoted_pack(self, tmp_path):
        from uuid import uuid4
        registry = PackRegistry(root=str(tmp_path / "reg"))
        pack = WorkflowPack(
            pack_id=str(uuid4()), title="Draft", goal="draft",
            steps=[PackStep(id="s1", action="click")],
        )
        registry.register(pack)  # CANDIDATE — not promoted

        builder = SpecializationPackBuilder(registry=registry)
        with pytest.raises(PackagingError, match="promoted"):
            await builder.build(
                domain="finance", run_id="r1",
                workflow_pack_ids=[pack.pack_id],
            )

    @pytest.mark.asyncio
    async def test_build_emits_chain_signal(self, tmp_path):
        from harvest_core.provenance.chain_writer import ChainWriter
        writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
        builder = SpecializationPackBuilder(chain_writer=writer)
        await builder.build(domain="ops", run_id="r1")
        signals = [e.signal for e in writer.read_all()]
        assert "specialization.created" in signals

    @pytest.mark.asyncio
    async def test_specialization_pack_fields(self):
        builder = SpecializationPackBuilder()
        spec = await builder.build(
            domain="legal",
            run_id="r1",
            taxonomy={"contracts": ["NDA", "SLA"]},
            disallowed_actions=["delete_record"],
        )
        assert "contracts" in spec.taxonomy
        assert "delete_record" in spec.disallowed_actions


class TestDanteAgentsExporter:
    def test_export_promoted_pack(self, tmp_path):
        registry = PackRegistry(root=str(tmp_path / "reg"))
        wf_id = _promoted_workflow(registry)

        exporter = DanteAgentsExporter(registry)
        handoff = exporter.export(wf_id, domain="accounting")

        assert handoff.pack_id == wf_id
        assert handoff.domain == "accounting"
        assert handoff.receipt_id == "r-001"
        assert handoff.handoff_id

    def test_export_candidate_raises(self, tmp_path):
        from uuid import uuid4
        registry = PackRegistry(root=str(tmp_path / "reg"))
        pack = WorkflowPack(
            pack_id=str(uuid4()), title="Draft", goal="draft",
            steps=[PackStep(id="s1", action="click")],
        )
        registry.register(pack)

        exporter = DanteAgentsExporter(registry)
        with pytest.raises(PackagingError, match="promoted"):
            exporter.export(pack.pack_id)

    def test_export_all_returns_promoted_only(self, tmp_path):
        from uuid import uuid4
        registry = PackRegistry(root=str(tmp_path / "reg"))
        wf_id = _promoted_workflow(registry)

        # Also register a CANDIDATE (not promoted)
        candidate = WorkflowPack(
            pack_id=str(uuid4()), title="Draft", goal="g",
            steps=[PackStep(id="s1", action="type")],
        )
        registry.register(candidate)

        exporter = DanteAgentsExporter(registry)
        handoffs = exporter.export_all()
        assert len(handoffs) == 1
        assert handoffs[0].pack_id == wf_id

    def test_handoff_to_json(self, tmp_path):
        registry = PackRegistry(root=str(tmp_path / "reg"))
        wf_id = _promoted_workflow(registry)
        exporter = DanteAgentsExporter(registry)
        handoff = exporter.export(wf_id)
        import json
        data = json.loads(handoff.to_json())
        assert "pack" in data
        assert "handoff_id" in data

    def test_write_export_bundle(self, tmp_path):
        registry = PackRegistry(root=str(tmp_path / "reg"))
        _promoted_workflow(registry)
        _promoted_workflow(registry)

        exporter = DanteAgentsExporter(registry)
        paths = exporter.write_export_bundle(output_dir=tmp_path / "exports")
        assert len(paths) == 2
        for p in paths:
            assert p.exists()
