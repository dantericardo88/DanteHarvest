"""Tests for harvest_index.vector.hnsw_index — HNSWIndex implementation."""

import json
import pytest
from pathlib import Path

from harvest_index.vector.hnsw_index import HNSWIndex, VectorEntry


DIM = 4


def vec(*values, dim=DIM):
    """Build a padded vector of length dim."""
    v = list(values) + [0.0] * (dim - len(values))
    return v[:dim]


V_A = vec(1.0, 0.0, 0.0, 0.0)
V_B = vec(0.0, 1.0, 0.0, 0.0)
V_C = vec(0.0, 0.0, 1.0, 0.0)
V_QUERY = vec(0.9, 0.1, 0.0, 0.0)


class TestHNSWIndexBasic:
    def test_upsert_adds_entry(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        assert len(idx) == 1

    def test_len_returns_correct_count(self):
        idx = HNSWIndex(dim=DIM)
        assert len(idx) == 0
        idx.upsert("a", V_A)
        idx.upsert("b", V_B)
        assert len(idx) == 2

    def test_upsert_overwrite_does_not_increase_count(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.upsert("a", V_B)
        assert len(idx) == 1

    def test_upsert_wrong_dim_raises(self):
        idx = HNSWIndex(dim=DIM)
        with pytest.raises(ValueError):
            idx.upsert("a", [1.0, 2.0])  # wrong dim

    def test_get_returns_entry(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A, metadata={"type": "doc"})
        entry = idx.get("a")
        assert isinstance(entry, VectorEntry)
        assert entry.id == "a"
        assert entry.metadata["type"] == "doc"

    def test_get_nonexistent_returns_none(self):
        idx = HNSWIndex(dim=DIM)
        assert idx.get("nonexistent") is None


class TestHNSWIndexSearch:
    def test_search_returns_sorted_by_distance(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.upsert("b", V_B)
        idx.upsert("c", V_C)
        results = idx.search(V_QUERY, k=3)
        # V_A is closest to V_QUERY (both mainly in first dimension)
        assert results[0][0] == "a"
        distances = [r[1] for r in results]
        assert distances == sorted(distances)

    def test_search_returns_k_results(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.upsert("b", V_B)
        idx.upsert("c", V_C)
        results = idx.search(V_QUERY, k=2)
        assert len(results) == 2

    def test_search_empty_returns_empty(self):
        idx = HNSWIndex(dim=DIM)
        results = idx.search(V_QUERY, k=5)
        assert results == []

    def test_search_result_tuple_structure(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A, {"x": 1})
        results = idx.search(V_QUERY, k=1)
        assert len(results) == 1
        id_, dist, meta = results[0]
        assert isinstance(id_, str)
        assert isinstance(dist, float)
        assert isinstance(meta, dict)

    def test_search_filter_by_metadata(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A, {"type": "doc"})
        idx.upsert("b", V_B, {"type": "image"})
        idx.upsert("c", V_C, {"type": "doc"})
        results = idx.search(V_QUERY, k=10, filter={"type": "doc"})
        ids = [r[0] for r in results]
        assert "b" not in ids
        assert all(r[2]["type"] == "doc" for r in results)

    def test_search_filter_no_match_returns_empty(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A, {"type": "doc"})
        results = idx.search(V_QUERY, k=5, filter={"type": "missing"})
        assert results == []

    def test_search_metadata_preserved_in_results(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A, {"title": "Alpha", "score": 42})
        results = idx.search(V_QUERY, k=1)
        assert results[0][2]["title"] == "Alpha"
        assert results[0][2]["score"] == 42


class TestHNSWIndexCosineDistance:
    def test_identical_vectors_distance_zero(self):
        idx = HNSWIndex(dim=DIM, metric="cosine")
        dist = idx._cosine_distance(V_A, V_A)
        assert dist < 1e-9

    def test_orthogonal_vectors_distance_one(self):
        idx = HNSWIndex(dim=DIM, metric="cosine")
        dist = idx._cosine_distance(V_A, V_B)
        assert abs(dist - 1.0) < 1e-9

    def test_zero_vector_returns_one(self):
        idx = HNSWIndex(dim=DIM, metric="cosine")
        dist = idx._cosine_distance([0.0] * DIM, V_A)
        assert dist == 1.0


class TestHNSWIndexUpsertBatch:
    def test_upsert_batch_returns_count(self):
        idx = HNSWIndex(dim=DIM)
        items = [
            {"id": "a", "vector": V_A, "metadata": {"x": 1}},
            {"id": "b", "vector": V_B, "metadata": {"x": 2}},
            {"id": "c", "vector": V_C},
        ]
        count = idx.upsert_batch(items)
        assert count == 3

    def test_upsert_batch_adds_all_entries(self):
        idx = HNSWIndex(dim=DIM)
        items = [
            {"id": "a", "vector": V_A},
            {"id": "b", "vector": V_B},
        ]
        idx.upsert_batch(items)
        assert len(idx) == 2

    def test_upsert_batch_metadata_preserved(self):
        idx = HNSWIndex(dim=DIM)
        items = [{"id": "a", "vector": V_A, "metadata": {"label": "first"}}]
        idx.upsert_batch(items)
        assert idx.get("a").metadata["label"] == "first"

    def test_upsert_batch_empty_list_returns_zero(self):
        idx = HNSWIndex(dim=DIM)
        count = idx.upsert_batch([])
        assert count == 0


class TestHNSWIndexDelete:
    def test_delete_removes_entry(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        result = idx.delete("a")
        assert result is True
        assert len(idx) == 0

    def test_delete_returns_false_for_nonexistent(self):
        idx = HNSWIndex(dim=DIM)
        result = idx.delete("nonexistent")
        assert result is False

    def test_delete_removes_from_search_results(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.upsert("b", V_B)
        idx.delete("a")
        results = idx.search(V_QUERY, k=5)
        ids = [r[0] for r in results]
        assert "a" not in ids


class TestHNSWIndexGetStats:
    def test_get_stats_returns_dict(self):
        idx = HNSWIndex(dim=DIM)
        stats = idx.get_stats()
        assert isinstance(stats, dict)

    def test_get_stats_correct_dim(self):
        idx = HNSWIndex(dim=DIM)
        stats = idx.get_stats()
        assert stats["dim"] == DIM

    def test_get_stats_correct_count(self):
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.upsert("b", V_B)
        stats = idx.get_stats()
        assert stats["count"] == 2

    def test_get_stats_metric(self):
        idx = HNSWIndex(dim=DIM, metric="euclidean")
        stats = idx.get_stats()
        assert stats["metric"] == "euclidean"

    def test_get_stats_m_and_ef(self):
        idx = HNSWIndex(dim=DIM, m=8, ef_construction=100)
        stats = idx.get_stats()
        assert stats["m"] == 8
        assert stats["ef_construction"] == 100


class TestHNSWIndexPersistence:
    def test_save_and_load_roundtrip(self, tmp_path):
        path = str(tmp_path / "index.json")
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A, {"title": "Alpha"})
        idx.upsert("b", V_B, {"title": "Beta"})
        idx.save(path)

        idx2 = HNSWIndex.load(path)
        assert len(idx2) == 2
        entry_a = idx2.get("a")
        assert entry_a is not None
        assert entry_a.metadata["title"] == "Alpha"

    def test_save_creates_valid_json(self, tmp_path):
        path = str(tmp_path / "index.json")
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.save(path)
        with open(path) as f:
            data = json.load(f)
        assert "entries" in data
        assert data["dim"] == DIM

    def test_load_preserves_search_results(self, tmp_path):
        path = str(tmp_path / "index.json")
        idx = HNSWIndex(dim=DIM)
        idx.upsert("a", V_A)
        idx.upsert("b", V_B)
        idx.save(path)

        idx2 = HNSWIndex.load(path)
        results = idx2.search(V_QUERY, k=2)
        assert results[0][0] == "a"

    def test_load_preserves_metric(self, tmp_path):
        path = str(tmp_path / "index.json")
        idx = HNSWIndex(dim=DIM, metric="euclidean")
        idx.save(path)
        idx2 = HNSWIndex.load(path)
        assert idx2.metric == "euclidean"
