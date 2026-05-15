"""Tests for harvest_core.rights.gdpr_compliance."""
import pytest
from pathlib import Path
from datetime import datetime, timezone


def _make_enforcer(tmp_path):
    from harvest_core.rights.retention_enforcer import RetentionEnforcer
    return RetentionEnforcer(store_path=tmp_path / "retention")


def _make_manager(tmp_path):
    from harvest_core.rights.gdpr_compliance import GDPRComplianceManager
    enforcer = _make_enforcer(tmp_path)
    return GDPRComplianceManager(enforcer, log_dir=tmp_path / "gdpr")


def test_erasure_request_fields():
    from harvest_core.rights.gdpr_compliance import ErasureRequest
    import time
    req = ErasureRequest(
        request_id="req-1",
        subject_id="user-abc",
        artifact_ids=["art-1", "art-2"],
        submitted_at=time.time(),
    )
    assert req.subject_id == "user-abc"
    assert len(req.artifact_ids) == 2
    assert req.deadline_days == 30
    assert not req.fulfilled


def test_erasure_request_overdue_when_past_deadline():
    from harvest_core.rights.gdpr_compliance import ErasureRequest
    req = ErasureRequest(
        request_id="req-2",
        subject_id="u",
        artifact_ids=[],
        submitted_at=0.0,  # epoch — definitely past 30-day deadline
    )
    assert req.is_overdue


def test_erasure_request_not_overdue_when_fulfilled():
    from harvest_core.rights.gdpr_compliance import ErasureRequest
    req = ErasureRequest(
        request_id="r",
        subject_id="u",
        artifact_ids=[],
        submitted_at=0.0,
        fulfilled=True,
    )
    assert not req.is_overdue


def test_submit_erasure_request_not_found_artifacts(tmp_path):
    mgr = _make_manager(tmp_path)
    receipt = mgr.submit_erasure_request("user-x", ["nonexistent-art"])
    assert receipt.subject_id == "user-x"
    assert "nonexistent-art" in receipt.not_found
    assert receipt.erased_artifact_ids == []


def test_submit_erasure_request_erases_tracked_artifact(tmp_path):
    from harvest_core.rights.rights_model import RetentionClass
    enforcer = _make_enforcer(tmp_path)
    from harvest_core.rights.gdpr_compliance import GDPRComplianceManager
    mgr = GDPRComplianceManager(enforcer, log_dir=tmp_path / "gdpr")

    enforcer.register("art-del", RetentionClass.MEDIUM)
    receipt = mgr.submit_erasure_request("user-y", ["art-del"])
    assert "art-del" in receipt.erased_artifact_ids
    # Should no longer be in enforcer
    assert enforcer.get_record("art-del") is None


def test_submit_erasure_request_legal_hold_skipped(tmp_path):
    from harvest_core.rights.rights_model import RetentionClass
    enforcer = _make_enforcer(tmp_path)
    from harvest_core.rights.gdpr_compliance import GDPRComplianceManager
    mgr = GDPRComplianceManager(enforcer, log_dir=tmp_path / "gdpr")

    enforcer.register("art-hold", RetentionClass.LEGAL_HOLD)
    receipt = mgr.submit_erasure_request("user-z", ["art-hold"])
    assert "art-hold" in receipt.skipped_legal_hold
    assert "art-hold" not in receipt.erased_artifact_ids


def test_erasure_receipt_written_to_log(tmp_path):
    mgr = _make_manager(tmp_path)
    mgr.submit_erasure_request("user-log", ["art-missing"])
    receipts = mgr.erasure_receipts()
    assert len(receipts) == 1
    assert receipts[0].subject_id == "user-log"


def test_generate_compliance_report_empty(tmp_path):
    mgr = _make_manager(tmp_path)
    report = mgr.generate_compliance_report()
    assert report.total_tracked == 0
    assert report.erasure_requests_total == 0
    assert report.fulfillment_rate == 1.0


def test_generate_compliance_report_with_data(tmp_path):
    from harvest_core.rights.rights_model import RetentionClass
    enforcer = _make_enforcer(tmp_path)
    from harvest_core.rights.gdpr_compliance import GDPRComplianceManager
    mgr = GDPRComplianceManager(enforcer, log_dir=tmp_path / "gdpr")

    enforcer.register("a1", RetentionClass.SHORT)
    enforcer.register("a2", RetentionClass.MEDIUM)
    mgr.submit_erasure_request("u", ["a1"])

    report = mgr.generate_compliance_report()
    assert report.total_tracked >= 1
    assert report.erasure_requests_total == 1
    assert report.erasure_requests_fulfilled == 1
    assert report.fulfillment_rate == 1.0


def test_compliance_report_to_json(tmp_path):
    import json
    mgr = _make_manager(tmp_path)
    report = mgr.generate_compliance_report()
    j = report.to_json()
    data = json.loads(j)
    assert "report_id" in data
    assert "generated_at" in data


def test_pending_requests_empty_initially(tmp_path):
    mgr = _make_manager(tmp_path)
    assert mgr.pending_requests() == []
