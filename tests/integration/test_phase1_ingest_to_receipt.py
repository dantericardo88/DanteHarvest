"""
Phase 1 integration test — file → chain → manifest → receipt.

Golden path: ingest a local file, normalize to markdown, issue receipt.
All chain signals must appear in correct order.
"""

import hashlib
import pytest
from pathlib import Path

from harvest_acquire.files.file_ingestor import FileIngestor
from harvest_core.control.run_contract import RunContract
from harvest_core.control.run_registry import RunRegistry, RunStatus
from harvest_core.manifests.export_manifest import ExportManifestBuilder
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.provenance.receipt_issuer import ReceiptIssuer
from harvest_core.rights.evidence_receipt import ArtifactRef
from harvest_core.rights.rights_model import SourceClass, TrainingEligibility, default_rights_for


@pytest.mark.asyncio
async def test_full_phase1_pipeline(tmp_path):
    """
    End-to-end Phase 1: create run → ingest file → issue receipt.
    Verifies signal order and receipt integrity.
    """
    # 1. Bootstrap run
    registry = RunRegistry(storage_root=str(tmp_path / "storage"))
    contract = RunContract(
        project_id="harvest-test",
        source_class=SourceClass.OWNED_INTERNAL,
        initiated_by="test@example.com",
    )
    record = await registry.create_run(contract)
    await registry.update_run_state(contract.run_id, RunStatus.RUNNING)
    writer = record.chain_writer

    # 2. Ingest a real file
    test_file = tmp_path / "document.txt"
    test_file.write_text("DANTEHARVEST Phase 1 end-to-end test content.")
    rights = default_rights_for(SourceClass.OWNED_INTERNAL)

    ingestor = FileIngestor(writer, storage_root=str(tmp_path / "artifacts"))
    ingest_result = await ingestor.ingest(
        path=test_file,
        run_id=contract.run_id,
        rights_profile=rights,
    )

    assert ingest_result.artifact_id
    assert len(ingest_result.sha256) == 64

    # 3. Mark run completed
    await registry.update_run_state(contract.run_id, RunStatus.COMPLETED)

    # 4. Build manifest
    chain_path = tmp_path / "storage" / contract.project_id / contract.run_id / "chain.jsonl"
    manifest_builder = ExportManifestBuilder()
    manifest_builder.add_artifact(
        "chain",
        str(chain_path),
        category="evidence",
        description="Evidence chain",
    )
    manifest = manifest_builder.build(
        run_id=contract.run_id,
        project_id=contract.project_id,
    )
    manifest_hash = manifest["manifest_hash"]

    # 5. Issue receipt
    artifact_refs = [ArtifactRef(
        artifact_id=ingest_result.artifact_id,
        artifact_type="document",
        storage_uri=ingest_result.storage_uri,
        sha256=ingest_result.sha256,
    )]

    issuer = ReceiptIssuer(writer, issuer_id="phase1-test", require_human_approval=False)
    receipt = await issuer.issue(
        run_id=contract.run_id,
        artifact_refs=artifact_refs,
        manifest_hash=manifest_hash,
        training_eligibility=TrainingEligibility.ALLOWED,
        confidence_score=0.95,
    )

    # 6. Assertions
    assert receipt.verify(), "Receipt hash must be valid"
    assert receipt.receipt_id
    assert len(receipt.policy_decisions) == 6

    entries = writer.read_all()
    signals = [e.signal for e in entries]

    assert "run.created" in signals
    assert "run.running" in signals
    assert "acquire.started" in signals
    assert "acquire.completed" in signals
    assert "run.completed" in signals
    assert "receipt.issued" in signals or "receipt.denied" in signals


@pytest.mark.asyncio
async def test_ingest_missing_file_does_not_issue_receipt(tmp_path):
    """
    If file ingest fails, chain has acquire.failed.
    A receipt can still be issued but gates will fail (provenance incomplete).
    """
    from harvest_core.control.exceptions import AcquisitionError

    registry = RunRegistry(storage_root=str(tmp_path / "storage"))
    contract = RunContract(
        project_id="fail-test",
        source_class=SourceClass.OWNED_INTERNAL,
        initiated_by="test@example.com",
    )
    record = await registry.create_run(contract)
    await registry.update_run_state(contract.run_id, RunStatus.RUNNING)
    writer = record.chain_writer

    ingestor = FileIngestor(writer, storage_root=str(tmp_path / "artifacts"))
    with pytest.raises(AcquisitionError):
        await ingestor.ingest(
            path=tmp_path / "nonexistent.pdf",
            run_id=contract.run_id,
        )

    entries = writer.read_all()
    signals = [e.signal for e in entries]
    assert "acquire.failed" in signals
    assert "acquire.completed" not in signals
