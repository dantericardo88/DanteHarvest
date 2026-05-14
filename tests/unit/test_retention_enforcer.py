"""Unit tests for RetentionEnforcer — GDPR retention lifecycle enforcement."""

from datetime import datetime, timedelta, timezone

from harvest_core.rights.retention_enforcer import (
    RetentionEnforcer,
    ArtifactRecord,
    ExpiredArtifact,
    compute_expiry,
    DEFAULT_WINDOWS,
)
from harvest_core.rights.rights_model import RetentionClass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_enforcer(tmp_path, event_callback=None) -> RetentionEnforcer:
    return RetentionEnforcer(store_path=tmp_path, event_callback=event_callback)


# ---------------------------------------------------------------------------
# compute_expiry helper
# ---------------------------------------------------------------------------

class TestComputeExpiry:
    def test_short_class_expires_within_24h(self):
        now = utc_now()
        expiry = compute_expiry(RetentionClass.SHORT, registered_at=now)
        assert expiry is not None
        assert expiry > now
        assert expiry <= now + timedelta(hours=25)

    def test_legal_hold_returns_none(self):
        expiry = compute_expiry(RetentionClass.LEGAL_HOLD)
        assert expiry is None

    def test_long_class_expires_after_years(self):
        now = utc_now()
        expiry = compute_expiry(RetentionClass.LONG, registered_at=now)
        assert expiry is not None
        assert expiry > now + timedelta(days=365)

    def test_custom_window_overrides_default(self):
        now = utc_now()
        custom = {RetentionClass.SHORT: timedelta(hours=1)}
        expiry = compute_expiry(RetentionClass.SHORT, registered_at=now, windows=custom)
        assert expiry is not None
        assert expiry <= now + timedelta(hours=2)

    def test_returns_datetime(self):
        result = compute_expiry(RetentionClass.MEDIUM)
        assert isinstance(result, datetime)


# ---------------------------------------------------------------------------
# RetentionEnforcer — register
# ---------------------------------------------------------------------------

class TestRetentionEnforcerRegister:
    def test_register_returns_artifact_record(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        rec = enforcer.register("art-001", RetentionClass.SHORT)
        assert isinstance(rec, ArtifactRecord)
        assert rec.artifact_id == "art-001"
        assert rec.retention_class == RetentionClass.SHORT.value

    def test_register_sets_expires_at_for_short(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        rec = enforcer.register("art-002", RetentionClass.SHORT)
        assert rec.expires_at is not None

    def test_register_legal_hold_has_no_expiry(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        rec = enforcer.register("art-hold", RetentionClass.LEGAL_HOLD)
        assert rec.expires_at is None

    def test_register_persists_to_disk(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("art-persist", RetentionClass.MEDIUM, artifact_path="/tmp/x.bin")

        # Load fresh instance — should see the record
        enforcer2 = make_enforcer(tmp_path)
        rec = enforcer2.get_record("art-persist")
        assert rec is not None
        assert rec.artifact_path == "/tmp/x.bin"

    def test_register_with_metadata(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        rec = enforcer.register("art-meta", RetentionClass.LONG, metadata={"source": "test"})
        assert rec.metadata["source"] == "test"

    def test_register_overwrites_existing(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("art-dup", RetentionClass.SHORT)
        rec2 = enforcer.register("art-dup", RetentionClass.LONG)
        assert rec2.retention_class == RetentionClass.LONG.value


# ---------------------------------------------------------------------------
# RetentionEnforcer — sweep
# ---------------------------------------------------------------------------

class TestRetentionEnforcerSweep:
    def test_sweep_empty_returns_empty(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        assert enforcer.sweep() == []

    def test_sweep_finds_expired_artifact(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-old", RetentionClass.SHORT, registered_at=past)

        expired = enforcer.sweep()
        assert len(expired) == 1
        assert expired[0].artifact_id == "art-old"

    def test_sweep_does_not_find_non_expired(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("art-fresh", RetentionClass.SHORT)  # just registered → not expired

        expired = enforcer.sweep()
        assert not any(e.artifact_id == "art-fresh" for e in expired)

    def test_sweep_skips_legal_hold(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        # Register with a time far in the past — LEGAL_HOLD still should not expire
        past = utc_now() - timedelta(days=3650)
        enforcer.register("art-hold", RetentionClass.LEGAL_HOLD, registered_at=past)

        expired = enforcer.sweep()
        assert not any(e.artifact_id == "art-hold" for e in expired)

    def test_sweep_with_explicit_now(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("art-future", RetentionClass.SHORT)

        # Sweep 48 hours in the future
        future_now = utc_now() + timedelta(hours=48)
        expired = enforcer.sweep(now=future_now)
        assert any(e.artifact_id == "art-future" for e in expired)

    def test_sweep_returns_expired_artifact_type(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-x", RetentionClass.SHORT, registered_at=past)

        result = enforcer.sweep()
        assert all(isinstance(e, ExpiredArtifact) for e in result)

    def test_sweep_multiple_expired(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        for i in range(5):
            enforcer.register(f"art-{i}", RetentionClass.SHORT, registered_at=past)

        expired = enforcer.sweep()
        assert len(expired) == 5


# ---------------------------------------------------------------------------
# RetentionEnforcer — gc
# ---------------------------------------------------------------------------

class TestRetentionEnforcerGC:
    def test_gc_dry_run_does_not_delete(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-dry", RetentionClass.SHORT, registered_at=past)

        deleted = enforcer.gc(dry_run=True)
        assert len(deleted) == 1
        # Record should still be in the registry
        assert enforcer.get_record("art-dry") is not None

    def test_gc_removes_record_from_registry(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-gc", RetentionClass.SHORT, registered_at=past)

        enforcer.gc()
        assert enforcer.get_record("art-gc") is None

    def test_gc_deletes_artifact_file(self, tmp_path):
        store_dir = tmp_path / "store"
        artifact_file = tmp_path / "artifact.bin"
        artifact_file.write_bytes(b"data")

        enforcer = RetentionEnforcer(store_path=store_dir)
        past = utc_now() - timedelta(hours=48)
        enforcer.register(
            "art-file", RetentionClass.SHORT,
            artifact_path=str(artifact_file),
            registered_at=past,
        )

        enforcer.gc()
        assert not artifact_file.exists()

    def test_gc_tolerates_missing_artifact_file(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register(
            "art-missing", RetentionClass.SHORT,
            artifact_path="/nonexistent/path/art.bin",
            registered_at=past,
        )
        # Should not raise
        deleted = enforcer.gc()
        assert len(deleted) == 1

    def test_gc_calls_event_callback(self, tmp_path):
        events = []
        def callback(artifact_id, event_type, data):
            events.append((artifact_id, event_type))

        enforcer = RetentionEnforcer(store_path=tmp_path, event_callback=callback)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-cb", RetentionClass.SHORT, registered_at=past)

        enforcer.gc()
        assert len(events) == 1
        assert events[0] == ("art-cb", "artifact.expired")

    def test_gc_empty_returns_empty(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        assert enforcer.gc() == []

    def test_gc_persists_deletion(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-persist-del", RetentionClass.SHORT, registered_at=past)
        enforcer.gc()

        # Fresh instance should not see the deleted record
        enforcer2 = make_enforcer(tmp_path)
        assert enforcer2.get_record("art-persist-del") is None


# ---------------------------------------------------------------------------
# RetentionEnforcer — list_records / stats
# ---------------------------------------------------------------------------

class TestRetentionEnforcerQuery:
    def test_list_records_all(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("a1", RetentionClass.SHORT)
        enforcer.register("a2", RetentionClass.LONG)
        records = enforcer.list_records()
        assert len(records) == 2

    def test_list_records_filtered_by_class(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("a1", RetentionClass.SHORT)
        enforcer.register("a2", RetentionClass.SHORT)
        enforcer.register("a3", RetentionClass.LONG)

        short_records = enforcer.list_records(retention_class=RetentionClass.SHORT)
        assert len(short_records) == 2

    def test_stats_total_tracked(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        for i in range(4):
            enforcer.register(f"art-{i}", RetentionClass.MEDIUM)
        stats = enforcer.stats()
        assert stats["total_tracked"] == 4

    def test_stats_by_class(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        enforcer.register("a1", RetentionClass.SHORT)
        enforcer.register("a2", RetentionClass.SHORT)
        enforcer.register("a3", RetentionClass.LONG)
        stats = enforcer.stats()
        assert stats["by_class"][RetentionClass.SHORT.value] == 2
        assert stats["by_class"][RetentionClass.LONG.value] == 1

    def test_stats_pending_expiry(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        past = utc_now() - timedelta(hours=48)
        enforcer.register("art-exp", RetentionClass.SHORT, registered_at=past)
        enforcer.register("art-fresh", RetentionClass.SHORT)  # not expired
        stats = enforcer.stats()
        assert stats["pending_expiry"] == 1

    def test_get_record_none_for_unknown(self, tmp_path):
        enforcer = make_enforcer(tmp_path)
        assert enforcer.get_record("nonexistent") is None


# ---------------------------------------------------------------------------
# DEFAULT_WINDOWS
# ---------------------------------------------------------------------------

class TestDefaultWindows:
    def test_all_retention_classes_have_window(self):
        for rc in RetentionClass:
            assert rc in DEFAULT_WINDOWS

    def test_legal_hold_window_is_max(self):
        assert DEFAULT_WINDOWS[RetentionClass.LEGAL_HOLD] == timedelta.max

    def test_short_window_is_24h(self):
        assert DEFAULT_WINDOWS[RetentionClass.SHORT] == timedelta(hours=24)
