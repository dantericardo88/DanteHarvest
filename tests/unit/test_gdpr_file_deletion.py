"""
Tests for GDPR actual file deletion and retention cron wiring.

Covers:
- test_erasure_deletes_file_on_disk
- test_erasure_missing_file_no_crash
- test_deletion_log_records_artifact
- test_deletion_log_records_success_flag
- test_wire_to_cron_schedules_job
"""

from __future__ import annotations

from pathlib import Path

from harvest_core.rights.gdpr_compliance import GDPRComplianceManager
from harvest_core.rights.retention_enforcer import RetentionEnforcer
from harvest_core.rights.retention_scheduler import RetentionScheduler
from harvest_core.rights.rights_model import RetentionClass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_enforcer(tmp_path: Path) -> RetentionEnforcer:
    return RetentionEnforcer(store_path=tmp_path / "retention")


def _make_manager(tmp_path: Path, enforcer: RetentionEnforcer | None = None) -> GDPRComplianceManager:
    if enforcer is None:
        enforcer = _make_enforcer(tmp_path)
    return GDPRComplianceManager(enforcer, log_dir=tmp_path / "gdpr")


# ---------------------------------------------------------------------------
# test_erasure_deletes_file_on_disk
# ---------------------------------------------------------------------------

def test_erasure_deletes_file_on_disk(tmp_path: Path) -> None:
    """Submitting an erasure request for an artifact that has a file path
    must result in the file being physically removed from disk."""
    artifact_file = tmp_path / "artifact_001.bin"
    artifact_file.write_bytes(b"sensitive data")
    assert artifact_file.exists()

    enforcer = _make_enforcer(tmp_path)
    enforcer.register(
        artifact_id="art-001",
        retention_class=RetentionClass.SHORT,
        artifact_path=str(artifact_file),
    )

    mgr = _make_manager(tmp_path, enforcer)
    receipt = mgr.submit_erasure_request(
        subject_id="user-abc",
        artifact_ids=["art-001"],
    )

    assert "art-001" in receipt.erased_artifact_ids
    assert not artifact_file.exists(), "File should have been deleted by erasure"


# ---------------------------------------------------------------------------
# test_erasure_missing_file_no_crash
# ---------------------------------------------------------------------------

def test_erasure_missing_file_no_crash(tmp_path: Path) -> None:
    """Erasure of an artifact whose file does not exist must not raise."""
    enforcer = _make_enforcer(tmp_path)
    enforcer.register(
        artifact_id="art-002",
        retention_class=RetentionClass.SHORT,
        artifact_path=str(tmp_path / "does_not_exist.bin"),
    )

    mgr = _make_manager(tmp_path, enforcer)
    # Should not raise even though the file is absent
    receipt = mgr.submit_erasure_request(
        subject_id="user-xyz",
        artifact_ids=["art-002"],
    )

    assert "art-002" in receipt.erased_artifact_ids


# ---------------------------------------------------------------------------
# test_deletion_log_records_artifact
# ---------------------------------------------------------------------------

def test_deletion_log_records_artifact(tmp_path: Path) -> None:
    """After processing an erasure, get_deletion_log() must contain an entry
    for the artifact that was processed."""
    artifact_file = tmp_path / "art003.bin"
    artifact_file.write_bytes(b"hello")

    enforcer = _make_enforcer(tmp_path)
    enforcer.register(
        artifact_id="art-003",
        retention_class=RetentionClass.SHORT,
        artifact_path=str(artifact_file),
    )

    mgr = _make_manager(tmp_path, enforcer)
    mgr.submit_erasure_request(subject_id="user-1", artifact_ids=["art-003"])

    log = mgr.get_deletion_log()
    assert len(log) >= 1
    artifact_ids_in_log = [entry["artifact_id"] for entry in log]
    assert "art-003" in artifact_ids_in_log


# ---------------------------------------------------------------------------
# test_deletion_log_records_success_flag
# ---------------------------------------------------------------------------

def test_deletion_log_success_true_for_existing_file(tmp_path: Path) -> None:
    """Deleting an existing file records success=True in the deletion log."""
    artifact_file = tmp_path / "art004.bin"
    artifact_file.write_bytes(b"data")

    enforcer = _make_enforcer(tmp_path)
    enforcer.register(
        artifact_id="art-004",
        retention_class=RetentionClass.SHORT,
        artifact_path=str(artifact_file),
    )

    mgr = _make_manager(tmp_path, enforcer)
    mgr.submit_erasure_request(subject_id="user-2", artifact_ids=["art-004"])

    log = mgr.get_deletion_log()
    entry = next((e for e in log if e["artifact_id"] == "art-004"), None)
    assert entry is not None
    assert entry["success"] is True


def test_deletion_log_success_for_missing_file(tmp_path: Path) -> None:
    """Deleting a non-existent file with missing_ok=True still records success=True
    (unlink(missing_ok=True) does not raise)."""
    enforcer = _make_enforcer(tmp_path)
    enforcer.register(
        artifact_id="art-005",
        retention_class=RetentionClass.SHORT,
        artifact_path=str(tmp_path / "never_existed.bin"),
    )

    mgr = _make_manager(tmp_path, enforcer)
    mgr.submit_erasure_request(subject_id="user-3", artifact_ids=["art-005"])

    log = mgr.get_deletion_log()
    entry = next((e for e in log if e["artifact_id"] == "art-005"), None)
    assert entry is not None
    # missing_ok=True means unlink doesn't raise, so success=True
    assert entry["success"] is True


# ---------------------------------------------------------------------------
# test_wire_to_cron_schedules_job
# ---------------------------------------------------------------------------

def test_wire_to_cron_schedules_job(tmp_path: Path) -> None:
    """wire_to_cron(scheduler) must register a job that appears in
    scheduler.get_due_jobs() for an IntervalTrigger with last_fire=None."""
    from harvest_ui.api.job_scheduler import JobScheduler, IntervalTrigger
    from harvest_ui.api.job_store import JobStore

    store = JobStore(storage_root=str(tmp_path / "jobs"))

    async def _noop(job_id, params, store):
        pass

    scheduler = JobScheduler(store=store, runner_fn=_noop)

    enforcer = _make_enforcer(tmp_path)
    retention_scheduler = RetentionScheduler(enforcer, interval_s=86400)

    scheduled_job = retention_scheduler.wire_to_cron(scheduler, job_id="test-retention-gc", hours=24)

    assert scheduled_job is not None
    assert scheduled_job.job_id == "test-retention-gc"

    # An IntervalTrigger job with last_fire=None is always due
    due = scheduler.get_due_jobs()
    due_ids = [j.job_id for j in due]
    assert "test-retention-gc" in due_ids
