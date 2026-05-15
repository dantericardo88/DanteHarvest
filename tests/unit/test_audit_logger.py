"""Tests for harvest_core.audit.audit_logger."""
import json
import time
import pytest
from pathlib import Path
from harvest_core.audit.audit_logger import AuditLogger, AuditEvent, AuditEventType


def _make_logger(tmp_path):
    return AuditLogger(log_dir=tmp_path / "audit")


def test_log_returns_audit_event(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.APPROVE, operator="alice", resource_id="wf-001")
    assert isinstance(ev, AuditEvent)
    assert ev.event_type == AuditEventType.APPROVE
    assert ev.operator == "alice"
    assert ev.resource_id == "wf-001"
    assert ev.outcome == "success"


def test_log_writes_to_jsonl(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.PROMOTE, operator="bob", resource_id="pack-1")
    log_file = tmp_path / "audit" / "audit.jsonl"
    assert log_file.exists()
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == 1
    d = json.loads(lines[0])
    assert d["event_type"] == "PROMOTE"
    assert d["operator"] == "bob"


def test_log_append_only(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.CRAWL_START, operator="sys")
    logger.log(event_type=AuditEventType.CRAWL_STOP, operator="sys")
    log_file = tmp_path / "audit" / "audit.jsonl"
    lines = [l for l in log_file.read_text().splitlines() if l.strip()]
    assert len(lines) == 2


def test_log_default_operator(tmp_path):
    logger = AuditLogger(log_dir=tmp_path / "audit", default_operator="harvest-bot")
    ev = logger.log(event_type=AuditEventType.ARTIFACT_INGEST)
    assert ev.operator == "harvest-bot"


def test_log_all_event_types_do_not_raise(tmp_path):
    logger = _make_logger(tmp_path)
    for attr in dir(AuditEventType):
        if attr.startswith("_"):
            continue
        event_type = getattr(AuditEventType, attr)
        if isinstance(event_type, str):
            ev = logger.log(event_type=event_type)
            assert ev.event_type == event_type


def test_query_by_event_type(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.APPROVE, operator="alice")
    logger.log(event_type=AuditEventType.REJECT, operator="bob")
    logger.log(event_type=AuditEventType.APPROVE, operator="carol")
    results = logger.query(event_type=AuditEventType.APPROVE)
    assert len(results) == 2
    assert all(e.event_type == AuditEventType.APPROVE for e in results)


def test_query_by_operator(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.EXPORT, operator="alice")
    logger.log(event_type=AuditEventType.EXPORT, operator="bob")
    logger.log(event_type=AuditEventType.EXPORT, operator="alice")
    results = logger.query(operator="alice")
    assert len(results) == 2


def test_query_by_resource_id(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.PROMOTE, resource_id="pack-1")
    logger.log(event_type=AuditEventType.DEMOTE, resource_id="pack-2")
    results = logger.query(resource_id="pack-1")
    assert len(results) == 1
    assert results[0].resource_id == "pack-1"


def test_query_by_outcome(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.KEY_ROTATE, outcome="success")
    logger.log(event_type=AuditEventType.KEY_ROTATE, outcome="failure")
    results = logger.query(outcome="failure")
    assert len(results) == 1
    assert results[0].outcome == "failure"


def test_query_with_limit(tmp_path):
    logger = _make_logger(tmp_path)
    for i in range(10):
        logger.log(event_type=AuditEventType.CUSTOM, details={"i": i})
    results = logger.query(limit=3)
    assert len(results) == 3


def test_query_by_time_range(tmp_path):
    logger = _make_logger(tmp_path)
    t0 = time.time()
    logger.log(event_type=AuditEventType.CUSTOM, details={"order": "first"})
    t1 = time.time()
    logger.log(event_type=AuditEventType.CUSTOM, details={"order": "second"})
    results = logger.query(since=t1)
    assert len(results) == 1
    assert results[0].details["order"] == "second"


def test_recent_returns_n_events(tmp_path):
    logger = _make_logger(tmp_path)
    for i in range(5):
        logger.log(event_type=AuditEventType.CUSTOM, details={"i": i})
    recent = logger.recent(n=3)
    assert len(recent) == 3


def test_stats_counts_by_type(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.APPROVE)
    logger.log(event_type=AuditEventType.APPROVE)
    logger.log(event_type=AuditEventType.REJECT)
    stats = logger.stats()
    assert stats["total"] == 3
    assert stats["by_type"][AuditEventType.APPROVE] == 2
    assert stats["by_type"][AuditEventType.REJECT] == 1


def test_log_details_roundtrip(tmp_path):
    logger = _make_logger(tmp_path)
    details = {"reason": "manual review", "score": 0.95, "tags": ["legal", "gdpr"]}
    ev = logger.log(event_type=AuditEventType.REVIEW_DECISION, details=details)
    results = logger.query(event_type=AuditEventType.REVIEW_DECISION)
    assert results[0].details == details


def test_audit_event_iso_timestamp(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.CHAIN_SEAL)
    iso = ev.iso_timestamp
    assert "T" in iso
    assert iso.endswith("+00:00") or iso.endswith("Z") or "+" in iso


def test_log_session_id(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.REPLAY_RUN, session_id="sess-abc")
    assert ev.session_id == "sess-abc"
    results = logger.query()
    assert results[0].session_id == "sess-abc"


def test_empty_log_returns_empty_query(tmp_path):
    logger = _make_logger(tmp_path)
    assert logger.query() == []
    assert logger.recent() == []
    assert logger.stats() == {"total": 0, "by_type": {}}
