"""Tests for harvest_core.local_first.local_store."""
import tempfile
import os
import pytest
from harvest_core.local_first.local_store import LocalFirstStore, SyncMetadata


@pytest.fixture
def tmp_store(tmp_path):
    return LocalFirstStore(str(tmp_path / "store"))


@pytest.fixture
def tmp_meta(tmp_path):
    return SyncMetadata(str(tmp_path / "meta"))


# ---------------------------------------------------------------------------
# LocalFirstStore tests
# ---------------------------------------------------------------------------

class TestLocalFirstStoreWrite:
    def test_write_creates_local_file(self, tmp_store):
        artifact_hash = tmp_store.write("art-001", {"key": "value"})
        assert tmp_store.exists("art-001")
        assert isinstance(artifact_hash, str)
        assert len(artifact_hash) == 64  # sha256 hex


class TestLocalFirstStoreRead:
    def test_read_returns_dict_when_offline(self, tmp_store):
        tmp_store.write("art-002", {"content": "hello"})
        tmp_store.set_offline_mode(True)
        result = tmp_store.read("art-002")
        assert result == {"content": "hello"}

    def test_read_returns_none_for_missing(self, tmp_store):
        assert tmp_store.read("nonexistent") is None


class TestLocalFirstStoreExists:
    def test_exists_returns_true_after_write(self, tmp_store):
        tmp_store.write("art-003", {"x": 1})
        assert tmp_store.exists("art-003") is True

    def test_exists_returns_false_for_missing(self, tmp_store):
        assert tmp_store.exists("art-999") is False


class TestLocalFirstStoreDelete:
    def test_delete_removes_file(self, tmp_store):
        tmp_store.write("art-004", {"x": 1})
        assert tmp_store.exists("art-004")
        result = tmp_store.delete("art-004")
        assert result is True
        assert not tmp_store.exists("art-004")

    def test_delete_returns_false_for_missing(self, tmp_store):
        assert tmp_store.delete("nonexistent") is False


class TestLocalFirstStoreSyncStatus:
    def test_get_sync_status_includes_is_synced(self, tmp_store):
        status = tmp_store.get_sync_status()
        assert "is_synced" in status

    def test_get_sync_status_includes_pending_uploads(self, tmp_store):
        tmp_store.write("art-005", {"y": 2})
        status = tmp_store.get_sync_status()
        assert "pending_uploads" in status
        assert status["pending_uploads"] >= 1


# ---------------------------------------------------------------------------
# SyncMetadata tests
# ---------------------------------------------------------------------------

class TestSyncMetadataPendingUpload:
    def test_mark_pending_upload_adds_to_pending_list(self, tmp_meta):
        tmp_meta.mark_pending_upload("art-010")
        assert "art-010" in tmp_meta.get_pending_uploads()

    def test_mark_pending_upload_no_duplicates(self, tmp_meta):
        tmp_meta.mark_pending_upload("art-011")
        tmp_meta.mark_pending_upload("art-011")
        assert tmp_meta.get_pending_uploads().count("art-011") == 1


class TestSyncMetadataMarkSynced:
    def test_mark_synced_removes_from_pending_list(self, tmp_meta):
        tmp_meta.mark_pending_upload("art-020")
        tmp_meta.mark_synced("art-020")
        assert "art-020" not in tmp_meta.get_pending_uploads()


class TestSyncMetadataConflicts:
    def test_record_conflict_adds_unresolved_conflict(self, tmp_meta):
        tmp_meta.record_conflict("art-030", "aaa", "bbb")
        conflicts = tmp_meta.get_conflicts()
        assert len(conflicts) == 1
        assert conflicts[0]["artifact_id"] == "art-030"
        assert conflicts[0]["resolved"] is False

    def test_resolve_conflict_marks_conflict_as_resolved(self, tmp_meta):
        tmp_meta.record_conflict("art-031", "aaa", "bbb")
        tmp_meta.resolve_conflict("art-031", resolution="local")
        conflicts = tmp_meta.get_conflicts()
        assert len(conflicts) == 0

    def test_resolve_conflict_records_resolution(self, tmp_meta):
        tmp_meta.record_conflict("art-032", "xxx", "yyy")
        tmp_meta.resolve_conflict("art-032", resolution="remote")
        # All conflicts (including resolved) still in raw _meta
        all_conflicts = tmp_meta._meta["conflicts"]
        resolved = [c for c in all_conflicts if c["artifact_id"] == "art-032"]
        assert resolved[0]["resolved"] is True
        assert resolved[0]["resolution"] == "remote"
