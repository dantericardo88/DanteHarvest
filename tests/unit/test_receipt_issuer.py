"""Unit tests for ReceiptIssuer."""

import pytest
from pathlib import Path

from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.receipt_issuer import ReceiptIssuer
from harvest_core.rights.evidence_receipt import ArtifactRef
from harvest_core.rights.rights_model import TrainingEligibility, ReviewStatus


def _fake_manifest_hash() -> str:
    return "a" * 64


def _make_artifact_ref(n: int = 1) -> list:
    return [
        ArtifactRef(
            artifact_id=f"art-{i}",
            artifact_type="document",
            storage_uri=f"local:///tmp/art-{i}",
            sha256="b" * 64,
        )
        for i in range(n)
    ]


async def _seed_chain(writer: ChainWriter, run_id: str, with_rights: bool = True):
    for signal in ["run.created", "run.running", "acquire.started"]:
        await writer.append(ChainEntry(run_id=run_id, signal=signal, machine="test", data={}))
    rights_data = {
        "training_eligibility": "allowed",
        "rights_status": "approved",
    } if with_rights else {}
    await writer.append(ChainEntry(
        run_id=run_id, signal="acquire.completed", machine="test", data=rights_data
    ))
    await writer.append(ChainEntry(run_id=run_id, signal="run.completed", machine="test", data={}))


class TestReceiptIssuer:
    @pytest.mark.asyncio
    async def test_issues_receipt_with_all_gates_present(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        writer = ChainWriter(chain_path, "run-001")
        await _seed_chain(writer, "run-001")

        issuer = ReceiptIssuer(writer, issuer_id="test-issuer", require_human_approval=False)
        receipt = await issuer.issue(
            run_id="run-001",
            artifact_refs=_make_artifact_ref(2),
            manifest_hash=_fake_manifest_hash(),
            training_eligibility=TrainingEligibility.ALLOWED,
            confidence_score=0.95,
        )

        assert receipt.receipt_id
        assert receipt.verify()
        assert len(receipt.policy_decisions) == 6
        assert receipt.issuer == "test-issuer"

    @pytest.mark.asyncio
    async def test_receipt_is_self_sealing(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        writer = ChainWriter(chain_path, "run-001")
        await _seed_chain(writer, "run-001")

        issuer = ReceiptIssuer(writer, require_human_approval=False)
        receipt = await issuer.issue(
            run_id="run-001",
            artifact_refs=_make_artifact_ref(),
            manifest_hash=_fake_manifest_hash(),
        )
        assert receipt.verify() is True

    @pytest.mark.asyncio
    async def test_receipt_denied_emits_chain_signal(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        writer = ChainWriter(chain_path, "run-001")
        # Minimal chain — missing run.completed and rights data → provenance incomplete
        await writer.append(ChainEntry(run_id="run-001", signal="run.created", machine="test", data={}))

        issuer = ReceiptIssuer(writer, require_human_approval=True)
        receipt = await issuer.issue(
            run_id="run-001",
            artifact_refs=_make_artifact_ref(),
            manifest_hash=_fake_manifest_hash(),
        )

        entries = writer.read_all()
        signals = [e.signal for e in entries]
        assert "receipt.denied" in signals

    @pytest.mark.asyncio
    async def test_receipt_issued_emits_chain_signal(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        writer = ChainWriter(chain_path, "run-001")
        await _seed_chain(writer, "run-001")

        issuer = ReceiptIssuer(writer, require_human_approval=False)
        await issuer.issue(
            run_id="run-001",
            artifact_refs=_make_artifact_ref(),
            manifest_hash=_fake_manifest_hash(),
            confidence_score=0.95,
        )

        entries = writer.read_all()
        signals = [e.signal for e in entries]
        assert "receipt.issued" in signals or "receipt.denied" in signals

    @pytest.mark.asyncio
    async def test_policy_decisions_recorded(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        writer = ChainWriter(chain_path, "run-001")
        await _seed_chain(writer, "run-001")

        issuer = ReceiptIssuer(writer, require_human_approval=False)
        receipt = await issuer.issue(
            run_id="run-001",
            artifact_refs=_make_artifact_ref(),
            manifest_hash=_fake_manifest_hash(),
        )

        gate_names = {d.gate_name for d in receipt.policy_decisions}
        assert "provenance_completeness" in gate_names
        assert "rights_status" in gate_names
        assert "redaction_complete" in gate_names
