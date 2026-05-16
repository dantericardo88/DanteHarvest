"""Tests for tamper-evident chaining and query in AuditLogger."""
import json
import time

import pytest

from harvest_core.audit.audit_logger import (
    AuditLogger,
    AuditEvent,
    AuditEventType,
    _compute_entry_hash,
    _GENESIS_HASH,
)


def _make_logger(tmp_path):
    return AuditLogger(log_dir=tmp_path / "audit")


# ------------------------------------------------------------------
# Hash fields present on every new entry
# ------------------------------------------------------------------

def test_log_entry_has_entry_hash(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.APPROVE, operator="alice")
    assert ev.entry_hash is not None
    assert len(ev.entry_hash) == 64  # SHA-256 hex


def test_log_entry_has_prev_hash(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.APPROVE, operator="alice")
    assert ev.prev_hash is not None


def test_first_entry_prev_hash_is_genesis(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.APPROVE)
    assert ev.prev_hash == _GENESIS_HASH


def test_second_entry_prev_hash_equals_first_entry_hash(tmp_path):
    logger = _make_logger(tmp_path)
    ev1 = logger.log(event_type=AuditEventType.APPROVE)
    ev2 = logger.log(event_type=AuditEventType.REJECT)
    assert ev2.prev_hash == ev1.entry_hash


def test_hashes_stored_in_jsonl(tmp_path):
    logger = _make_logger(tmp_path)
    ev = logger.log(event_type=AuditEventType.PROMOTE, resource_id="pack-1")
    log_file = tmp_path / "audit" / "audit.jsonl"
    d = json.loads(log_file.read_text().splitlines()[0])
    assert d["entry_hash"] == ev.entry_hash
    assert d["prev_hash"] == ev.prev_hash


# ------------------------------------------------------------------
# verify_chain_integrity — happy path
# ------------------------------------------------------------------

def test_verify_chain_integrity_valid_empty_log(tmp_path):
    logger = _make_logger(tmp_path)
    result = logger.verify_chain_integrity()
    assert result["valid"] is True
    assert result["entries_checked"] == 0
    assert result["errors"] == []


def test_verify_chain_integrity_valid_single_entry(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.APPROVE)
    result = logger.verify_chain_integrity()
    assert result["valid"] is True
    assert result["entries_checked"] == 1


def test_verify_chain_integrity_valid_multiple_entries(tmp_path):
    logger = _make_logger(tmp_path)
    for etype in (AuditEventType.APPROVE, AuditEventType.REJECT, AuditEventType.PROMOTE):
        logger.log(event_type=etype)
    result = logger.verify_chain_integrity()
    assert result["valid"] is True
    assert result["entries_checked"] == 3


# ------------------------------------------------------------------
# verify_chain_integrity — tamper detection
# ------------------------------------------------------------------

def test_verify_chain_integrity_detects_tampered_entry(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.APPROVE, operator="alice")
    logger.log(event_type=AuditEventType.REJECT, operator="bob")

    log_file = tmp_path / "audit" / "audit.jsonl"
    lines = log_file.read_text(encoding="utf-8").splitlines()

    # Tamper with the first entry — change operator
    first = json.loads(lines[0])
    first["operator"] = "eve"  # tampered!
    lines[0] = json.dumps(first)
    log_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Re-create logger from same dir so it re-reads from disk
    logger2 = _make_logger(tmp_path)
    result = logger2.verify_chain_integrity()
    assert result["valid"] is False
    assert len(result["errors"]) > 0


def test_verify_chain_integrity_detects_hash_field_tamper(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.EXPORT)

    log_file = tmp_path / "audit" / "audit.jsonl"
    lines = log_file.read_text(encoding="utf-8").splitlines()
    entry = json.loads(lines[0])
    entry["entry_hash"] = "0" * 64  # forge the hash
    log_file.write_text(json.dumps(entry) + "\n", encoding="utf-8")

    logger2 = _make_logger(tmp_path)
    result = logger2.verify_chain_integrity()
    assert result["valid"] is False


# ------------------------------------------------------------------
# query — structured field filtering
# ------------------------------------------------------------------

def test_query_by_event_type(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.APPROVE, operator="alice")
    logger.log(event_type=AuditEventType.REJECT, operator="bob")
    logger.log(event_type=AuditEventType.APPROVE, operator="carol")
    results = logger.query(event_type=AuditEventType.APPROVE)
    assert len(results) == 2
    assert all(e.event_type == AuditEventType.APPROVE for e in results)


def test_query_by_resource_id(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.PROMOTE, resource_id="pack-1")
    logger.log(event_type=AuditEventType.PROMOTE, resource_id="pack-2")
    results = logger.query(resource_id="pack-1")
    assert len(results) == 1
    assert results[0].resource_id == "pack-1"


def test_query_by_since(tmp_path):
    logger = _make_logger(tmp_path)
    logger.log(event_type=AuditEventType.CUSTOM, details={"order": "first"})
    t1 = time.time()
    logger.log(event_type=AuditEventType.CUSTOM, details={"order": "second"})
    results = logger.query(since=t1)
    assert len(results) == 1
    assert results[0].details["order"] == "second"


def test_query_with_limit(tmp_path):
    logger = _make_logger(tmp_path)
    for i in range(10):
        logger.log(event_type=AuditEventType.CUSTOM, details={"i": i})
    results = logger.query(limit=3)
    assert len(results) == 3


def test_query_empty_returns_empty(tmp_path):
    logger = _make_logger(tmp_path)
    assert logger.query(event_type=AuditEventType.APPROVE) == []


# ------------------------------------------------------------------
# Chain continuity after logger reconstruction
# ------------------------------------------------------------------

def test_chain_continues_after_reconstruction(tmp_path):
    """New logger instance picks up prev_hash from disk correctly."""
    log_dir = tmp_path / "audit"
    logger1 = AuditLogger(log_dir=log_dir)
    ev1 = logger1.log(event_type=AuditEventType.APPROVE)

    logger2 = AuditLogger(log_dir=log_dir)
    ev2 = logger2.log(event_type=AuditEventType.REJECT)

    assert ev2.prev_hash == ev1.entry_hash

    result = logger2.verify_chain_integrity()
    assert result["valid"] is True
    assert result["entries_checked"] == 2
