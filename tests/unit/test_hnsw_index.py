"""Tests for HNSWIndex — persistent approximate nearest-neighbor index."""

import pickle
import pytest
from pathlib import Path

from harvest_index.search.hnsw_index import (
    HNSWIndex,
    ANNResult,
    _FlatIndex,
    _cosine,
)
from harvest_core.control.exceptions import StorageError


# ---------------------------------------------------------------------------
# Sample vectors (384-dim approximations with small dims for speed)
# ---------------------------------------------------------------------------

DIM = 8  # small dim for test speed


def make_vector(*values: float, dim: int = DIM) -> list:
    """Create a vector padded with zeros."""
    v = list(values) + [0.0] * (dim - len(values))
    return v[:dim]


V_INVOICE = make_vector(0.9, 0.8, 0.1, 0.0, 0.1, 0.0, 0.0, 0.0)
V_VENDOR  = make_vector(0.1, 0.9, 0.8, 0.0, 0.0, 0.1, 0.0, 0.0)
V_LEGAL   = make_vector(0.0, 0.0, 0.0, 0.9, 0.8, 0.1, 0.1, 0.0)
V_QUERY   = make_vector(0.85, 0.75, 0.15, 0.0, 0.0, 0.0, 0.0, 0.0)


# ---------------------------------------------------------------------------
# Cosine helper tests
# ---------------------------------------------------------------------------

class TestCosineHelper:
    def test_identical_vectors(self):
        score = _cosine(V_INVOICE, V_INVOICE)
        assert score > 0.99

    def test_orthogonal_vectors(self):
        a = make_vector(1.0, 0.0, 0.0)
        b = make_vector(0.0, 1.0, 0.0)
        score = _cosine(a, b)
        assert score < 0.01

    def test_zero_vector_returns_zero(self):
        a = [0.0] * DIM
        b = V_INVOICE
        score = _cosine(a, b)
        assert score == 0.0


# ---------------------------------------------------------------------------
# FlatIndex tests
# ---------------------------------------------------------------------------

class TestFlatIndex:
    def test_add_and_search(self):
        idx = _FlatIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"title": "Invoice"})
        idx.add("p2", V_VENDOR, {"title": "Vendor"})
        results = idx.search(V_QUERY, k=2)
        assert len(results) > 0
        scores, ids = zip(*results)
        # Invoice vector closest to query
        assert ids[0] == "p1"

    def test_empty_returns_empty(self):
        idx = _FlatIndex(dim=DIM)
        results = idx.search(V_QUERY, k=5)
        assert results == []

    def test_delete_removes_entry(self):
        idx = _FlatIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {})
        idx.delete("p1")
        assert len(idx) == 0

    def test_delete_nonexistent_no_error(self):
        idx = _FlatIndex(dim=DIM)
        idx.delete("nonexistent")  # should not raise

    def test_len(self):
        idx = _FlatIndex(dim=DIM)
        assert len(idx) == 0
        idx.add("p1", V_INVOICE, {})
        assert len(idx) == 1

    def test_overwrite_entry(self):
        idx = _FlatIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"title": "v1"})
        idx.add("p1", V_VENDOR, {"title": "v2"})
        assert len(idx) == 1
        entry = idx.get_entry("p1")
        assert entry.metadata["title"] == "v2"


# ---------------------------------------------------------------------------
# HNSWIndex tests (using flat fallback — hnswlib optional)
# ---------------------------------------------------------------------------

class TestHNSWIndex:
    def test_init_no_persist(self):
        idx = HNSWIndex(dim=DIM)
        assert len(idx) == 0
        assert idx.backend in ("flat", "hnswlib")

    def test_add_and_search(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"pack_type": "workflowPack", "title": "Invoice"})
        idx.add("p2", V_VENDOR, {"pack_type": "skillPack", "title": "Vendor"})
        results = idx.search(V_QUERY, k=2)
        assert len(results) >= 1
        assert all(isinstance(r, ANNResult) for r in results)

    def test_search_returns_ann_result(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"pack_type": "workflowPack"})
        results = idx.search(V_QUERY, k=1)
        assert len(results) == 1
        r = results[0]
        assert isinstance(r.pack_id, str)
        assert 0.0 <= r.score <= 1.0
        assert isinstance(r.metadata, dict)

    def test_search_empty_returns_empty(self):
        idx = HNSWIndex(dim=DIM)
        results = idx.search(V_QUERY, k=5)
        assert results == []

    def test_filter_by_pack_type(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"pack_type": "workflowPack"})
        idx.add("p2", V_VENDOR, {"pack_type": "skillPack"})
        idx.add("p3", V_LEGAL, {"pack_type": "workflowPack"})

        results = idx.search(V_QUERY, k=10, filter_pack_type="workflowPack")
        types = {r.metadata.get("pack_type") for r in results}
        assert types == {"workflowPack"}

    def test_filter_by_nonexistent_type_returns_empty(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"pack_type": "workflowPack"})
        results = idx.search(V_QUERY, k=5, filter_pack_type="evalPack")
        assert results == []

    def test_delete_removes_from_search(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {"pack_type": "workflowPack"})
        idx.add("p2", V_VENDOR, {"pack_type": "skillPack"})
        idx.delete("p1")
        results = idx.search(V_QUERY, k=5)
        ids = [r.pack_id for r in results]
        assert "p1" not in ids

    def test_len_tracking(self):
        idx = HNSWIndex(dim=DIM)
        assert len(idx) == 0
        idx.add("p1", V_INVOICE, {})
        assert len(idx) == 1
        idx.add("p2", V_VENDOR, {})
        assert len(idx) == 2
        idx.delete("p1")
        assert len(idx) == 1

    def test_scores_sorted_descending(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {})
        idx.add("p2", V_VENDOR, {})
        idx.add("p3", V_LEGAL, {})
        results = idx.search(V_QUERY, k=3)
        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# Persistence tests
# ---------------------------------------------------------------------------

class TestHNSWIndexPersistence:
    def test_save_and_load_flat(self, tmp_path):
        path = tmp_path / "index.bin"
        idx = HNSWIndex(dim=DIM, persist_path=path, backend="flat")
        idx.add("p1", V_INVOICE, {"title": "Invoice"})
        idx.add("p2", V_VENDOR, {"title": "Vendor"})
        idx.save()
        assert path.exists()

        idx2 = HNSWIndex(dim=DIM, persist_path=path, backend="flat")
        idx2.load()
        assert len(idx2) == 2
        results = idx2.search(V_QUERY, k=2)
        assert len(results) >= 1

    def test_load_nonexistent_no_error(self, tmp_path):
        path = tmp_path / "nonexistent.bin"
        idx = HNSWIndex(dim=DIM, persist_path=path)
        idx.load()  # should not raise
        assert len(idx) == 0

    def test_load_corrupted_raises_storage_error(self, tmp_path):
        path = tmp_path / "corrupt.bin"
        path.write_bytes(b"not a valid pickle")
        idx = HNSWIndex(dim=DIM, persist_path=path, backend="flat")
        with pytest.raises(StorageError):
            idx.load()

    def test_save_without_persist_path_no_error(self):
        idx = HNSWIndex(dim=DIM)
        idx.add("p1", V_INVOICE, {})
        idx.save()  # no persist_path — should be a no-op

    def test_save_atomic_write(self, tmp_path):
        path = tmp_path / "index.bin"
        idx = HNSWIndex(dim=DIM, persist_path=path, backend="flat")
        idx.add("p1", V_INVOICE, {})
        idx.save()
        # Temp file should not remain
        tmp = path.with_suffix(".tmp")
        assert not tmp.exists()
        assert path.exists()

    def test_roundtrip_metadata_preserved(self, tmp_path):
        path = tmp_path / "index.bin"
        idx = HNSWIndex(dim=DIM, persist_path=path, backend="flat")
        idx.add("p1", V_INVOICE, {"pack_type": "workflowPack", "title": "My Workflow"})
        idx.save()

        idx2 = HNSWIndex(dim=DIM, persist_path=path, backend="flat")
        idx2.load()
        results = idx2.search(V_INVOICE, k=1)
        assert results[0].metadata.get("title") == "My Workflow"
        assert results[0].metadata.get("pack_type") == "workflowPack"
