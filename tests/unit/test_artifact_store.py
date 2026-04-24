"""Unit tests for ArtifactStore."""

import pytest
from pathlib import Path
from harvest_index.artifacts.artifact_store import ArtifactStore
from harvest_core.control.exceptions import StorageError


class TestArtifactStore:
    def _make_store(self, tmp_path) -> ArtifactStore:
        return ArtifactStore(root=str(tmp_path / "store"))

    def test_put_and_get_file(self, tmp_path):
        store = self._make_store(tmp_path)
        src = tmp_path / "doc.txt"
        src.write_text("hello harvest")

        record = store.put(src, run_id="run-1", project_id="proj-1", source_type="document")
        retrieved = store.get(record.artifact_id)

        assert retrieved.artifact_id == record.artifact_id
        assert retrieved.sha256 == record.sha256
        assert retrieved.source_type == "document"

    def test_put_deduplicates_identical_content(self, tmp_path):
        store = self._make_store(tmp_path)
        src1 = tmp_path / "a.txt"
        src2 = tmp_path / "b.txt"
        src1.write_text("identical")
        src2.write_text("identical")

        r1 = store.put(src1, run_id="run-1", project_id="proj-1")
        r2 = store.put(src2, run_id="run-2", project_id="proj-1")

        assert r1.artifact_id == r2.artifact_id
        assert len(store.list()) == 1

    def test_put_text(self, tmp_path):
        store = self._make_store(tmp_path)
        record = store.put_text(
            text="some markdown content",
            filename="chunk.md",
            run_id="run-1",
            project_id="proj-1",
            source_type="text",
        )
        assert record.artifact_id
        assert store.read_text(record.artifact_id) == "some markdown content"

    def test_get_nonexistent_raises(self, tmp_path):
        store = self._make_store(tmp_path)
        with pytest.raises(StorageError):
            store.get("nonexistent-id")

    def test_list_by_run_id(self, tmp_path):
        store = self._make_store(tmp_path)
        store.put_text("a", "a.md", run_id="run-A", project_id="proj-1")
        store.put_text("b", "b.md", run_id="run-B", project_id="proj-1")

        results = store.list(run_id="run-A")
        assert len(results) == 1
        assert results[0].run_id == "run-A"

    def test_list_all(self, tmp_path):
        store = self._make_store(tmp_path)
        store.put_text("x", "x.md", run_id="r1", project_id="p1")
        store.put_text("y", "y.md", run_id="r2", project_id="p1")
        assert len(store.list()) == 2

    def test_delete_removes_record(self, tmp_path):
        store = self._make_store(tmp_path)
        record = store.put_text("hello", "h.md", run_id="r1", project_id="p1")
        store.delete(record.artifact_id)
        with pytest.raises(StorageError):
            store.get(record.artifact_id)

    def test_stats_returns_counts(self, tmp_path):
        store = self._make_store(tmp_path)
        store.put_text("doc content", "doc.md", run_id="r1", project_id="p1", source_type="document")
        store.put_text("img content", "img.md", run_id="r1", project_id="p1", source_type="image")
        stats = store.stats()
        assert stats["total_artifacts"] == 2
        assert stats["by_type"]["document"] == 1
        assert stats["by_type"]["image"] == 1

    def test_index_persists_across_instances(self, tmp_path):
        root = str(tmp_path / "store")
        store1 = ArtifactStore(root=root)
        store1.put_text("persisted", "p.md", run_id="r1", project_id="p1")

        store2 = ArtifactStore(root=root)
        assert len(store2.list()) == 1

    def test_put_missing_source_raises(self, tmp_path):
        store = self._make_store(tmp_path)
        with pytest.raises(StorageError):
            store.put(Path(tmp_path / "missing.pdf"), run_id="r1", project_id="p1")
