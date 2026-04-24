"""Tests for PackVectorStore — local TF-IDF mode (no Qdrant required)."""

import pytest
from harvest_index.search.pack_vector_store import PackVectorStore, VectorSearchResult


def make_store() -> PackVectorStore:
    return PackVectorStore()


def test_upsert_and_query_basic():
    store = make_store()
    store.upsert("pack-001", "submit invoice to accounting department", metadata={"pack_type": "workflow", "title": "Invoice"})
    results = store.query("invoice accounting", limit=5)
    assert len(results) == 1
    assert results[0].pack_id == "pack-001"
    assert results[0].score > 0


def test_query_empty_corpus_returns_empty():
    store = make_store()
    results = store.query("anything", limit=5)
    assert results == []


def test_query_empty_string_returns_empty():
    store = make_store()
    store.upsert("pack-001", "some content here", metadata={})
    results = store.query("", limit=5)
    assert results == []


def test_upsert_multiple_and_ranked():
    store = make_store()
    store.upsert("pack-001", "invoice payment accounting finance", metadata={"pack_type": "workflow", "title": "Invoice"})
    store.upsert("pack-002", "browser automation web scraping navigation", metadata={"pack_type": "workflow", "title": "Browser"})
    store.upsert("pack-003", "invoice receipt payment finance", metadata={"pack_type": "workflow", "title": "Receipt"})
    results = store.query("invoice finance", limit=3)
    pack_ids = [r.pack_id for r in results]
    assert "pack-001" in pack_ids
    assert "pack-003" in pack_ids
    assert results[0].score >= results[1].score


def test_filter_by_type():
    store = make_store()
    store.upsert("pack-001", "invoice workflow payment", metadata={"pack_type": "workflow", "title": "W1"})
    store.upsert("pack-002", "invoice skill payment", metadata={"pack_type": "skill", "title": "S1"})
    results = store.query("invoice payment", limit=5, filter_by_type="skill")
    assert len(results) == 1
    assert results[0].pack_id == "pack-002"


def test_upsert_overwrites():
    store = make_store()
    store.upsert("pack-001", "original content about accounting", metadata={"title": "Original"})
    store.upsert("pack-001", "completely different content about robots", metadata={"title": "Updated"})
    results = store.query("robots", limit=5)
    assert len(results) == 1
    assert results[0].title == "Updated"


def test_delete_removes_from_index():
    store = make_store()
    store.upsert("pack-001", "invoice payment content", metadata={})
    store.delete("pack-001")
    results = store.query("invoice payment", limit=5)
    assert results == []


def test_len_tracks_correctly():
    store = make_store()
    assert len(store) == 0
    store.upsert("pack-001", "content one", metadata={})
    assert len(store) == 1
    store.upsert("pack-002", "content two", metadata={})
    assert len(store) == 2
    store.delete("pack-001")
    assert len(store) == 1


def test_limit_respected():
    store = make_store()
    for i in range(10):
        store.upsert(f"pack-{i:03d}", f"invoice payment workflow content item {i}", metadata={})
    results = store.query("invoice payment workflow", limit=3)
    assert len(results) <= 3


def test_result_fields_populated():
    store = make_store()
    store.upsert("pack-001", "invoice workflow content", metadata={
        "pack_type": "workflow",
        "title": "My Invoice Pack",
        "extra": "data",
    })
    results = store.query("invoice workflow", limit=1)
    assert len(results) == 1
    r = results[0]
    assert r.pack_id == "pack-001"
    assert r.pack_type == "workflow"
    assert r.title == "My Invoice Pack"
    assert r.metadata.get("extra") == "data"
    assert isinstance(r.score, float)
