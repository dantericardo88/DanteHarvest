"""
Phase 8 — Hybrid BM25+dense vector search tests.

Verifies:
1. query(mode="tfidf") uses TF-IDF index
2. query(mode="hybrid") combines both indices
3. hybrid alpha weighting affects ordering
4. rerank() falls back gracefully without sentence-transformers
5. mode="auto" still works as before
"""

import pytest
from unittest.mock import MagicMock, patch

from harvest_index.search.pack_vector_store import PackVectorStore, VectorSearchResult


def _make_store_with_both_indices():
    """Returns a PackVectorStore with both TF-IDF and dense indices populated."""
    from harvest_index.search.pack_vector_store import _LocalTFIDFIndex, _LocalDenseIndex

    store = PackVectorStore(use_embeddings=False)
    assert store._local_index is not None

    # Populate TF-IDF
    store._local_index.upsert("p1", "invoice payment accounting workflow", {"pack_type": "workflow", "title": "Invoice"})
    store._local_index.upsert("p2", "customer onboarding email notification", {"pack_type": "workflow", "title": "Onboard"})
    store._local_index.upsert("p3", "invoice receipt approval process", {"pack_type": "workflow", "title": "Receipt"})

    # Add a mock dense index
    mock_engine = MagicMock()
    query_vectors = {
        "invoice": [1.0, 0.0, 0.0],
        "customer": [0.0, 1.0, 0.0],
    }
    doc_vectors = {
        "p1": [0.9, 0.1, 0.0],
        "p2": [0.1, 0.9, 0.0],
        "p3": [0.8, 0.2, 0.0],
    }
    mock_engine.embed.side_effect = lambda t: query_vectors.get(t.split()[0], [0.5, 0.5, 0.0])
    mock_engine.is_available.return_value = True

    from harvest_index.search.pack_vector_store import _LocalDenseIndex
    dense = _LocalDenseIndex(mock_engine)
    for pid, vec in doc_vectors.items():
        mock_engine.embed.return_value = vec
        dense.upsert(pid, f"text for {pid}", {"pack_type": "workflow", "title": pid})

    store._dense_index = dense
    return store


# ---------------------------------------------------------------------------
# mode="tfidf" (explicit)
# ---------------------------------------------------------------------------

def test_query_tfidf_mode_explicit():
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice payment workflow", {"pack_type": "workflow", "title": "Invoice"})
    store._local_index.upsert("p2", "customer email notification", {"pack_type": "workflow", "title": "Email"})
    results = store.query("invoice payment", mode="tfidf")
    assert len(results) > 0
    assert results[0].pack_id == "p1"


def test_query_tfidf_returns_empty_for_empty_corpus():
    store = PackVectorStore(use_embeddings=False)
    results = store.query("anything", mode="tfidf")
    assert results == []


# ---------------------------------------------------------------------------
# mode="hybrid"
# ---------------------------------------------------------------------------

def test_query_hybrid_returns_results():
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice payment", {"pack_type": "workflow", "title": "P1"})
    store._local_index.upsert("p2", "customer service", {"pack_type": "workflow", "title": "P2"})
    results = store.query("invoice", mode="hybrid")
    assert isinstance(results, list)


def test_query_hybrid_no_dense_falls_back_to_tfidf():
    """If no dense index, hybrid should return tfidf results."""
    store = PackVectorStore(use_embeddings=False)
    assert store._dense_index is None
    store._local_index.upsert("p1", "invoice accounting", {"pack_type": "workflow", "title": "P1"})
    results = store.query("invoice", mode="hybrid")
    assert len(results) > 0
    assert results[0].pack_id == "p1"


def test_query_hybrid_alpha_zero_is_pure_dense():
    """alpha=0.0 means 100% weight on dense scores."""
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice", {"pack_type": "workflow", "title": "P1"})
    store._local_index.upsert("p2", "customer", {"pack_type": "workflow", "title": "P2"})

    mock_dense = MagicMock()
    mock_dense.query.return_value = [
        VectorSearchResult(pack_id="p2", score=0.95, pack_type="workflow", title="P2"),
        VectorSearchResult(pack_id="p1", score=0.30, pack_type="workflow", title="P1"),
    ]
    store._dense_index = mock_dense

    results = store.query("something", mode="hybrid", hybrid_alpha=0.0)
    assert results[0].pack_id == "p2"


def test_query_hybrid_alpha_one_is_pure_tfidf():
    """alpha=1.0 means 100% weight on TF-IDF scores."""
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice payment accounting", {"pack_type": "workflow", "title": "P1"})
    store._local_index.upsert("p2", "customer onboarding", {"pack_type": "workflow", "title": "P2"})

    mock_dense = MagicMock()
    mock_dense.query.return_value = [
        VectorSearchResult(pack_id="p2", score=0.99, pack_type="workflow", title="P2"),
    ]
    store._dense_index = mock_dense

    results = store.query("invoice payment", mode="hybrid", hybrid_alpha=1.0)
    assert results[0].pack_id == "p1"


# ---------------------------------------------------------------------------
# mode="auto" backwards compatibility
# ---------------------------------------------------------------------------

def test_query_auto_mode_still_works():
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice workflow", {"pack_type": "workflow", "title": "P1"})
    results = store.query("invoice", mode="auto")
    assert len(results) == 1
    assert results[0].pack_id == "p1"


def test_query_default_mode_is_auto():
    """Calling query() without mode= should work (uses 'auto' default)."""
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice workflow", {"pack_type": "workflow", "title": "P1"})
    results = store.query("invoice")
    assert len(results) == 1


# ---------------------------------------------------------------------------
# rerank()
# ---------------------------------------------------------------------------

def test_rerank_without_sentence_transformers_returns_original():
    store = PackVectorStore(use_embeddings=False)
    results = [
        VectorSearchResult(pack_id="p1", score=0.9, pack_type="w", title="First"),
        VectorSearchResult(pack_id="p2", score=0.8, pack_type="w", title="Second"),
    ]
    with patch.dict("sys.modules", {"sentence_transformers": None}):
        reranked = store.rerank(results, "invoice")
    assert [r.pack_id for r in reranked] == ["p1", "p2"]


def test_rerank_empty_returns_empty():
    store = PackVectorStore(use_embeddings=False)
    assert store.rerank([], "invoice") == []


def test_rerank_top_k_limits_results():
    store = PackVectorStore(use_embeddings=False)
    results = [
        VectorSearchResult(pack_id=f"p{i}", score=float(i), pack_type="w", title=f"P{i}")
        for i in range(5)
    ]
    with patch.dict("sys.modules", {"sentence_transformers": None}):
        reranked = store.rerank(results, "test", top_k=3)
    assert len(reranked) == 3


# ---------------------------------------------------------------------------
# hybrid_alpha default
# ---------------------------------------------------------------------------

def test_query_hybrid_default_alpha():
    """Default alpha=0.4 produces valid results."""
    store = PackVectorStore(use_embeddings=False)
    store._local_index.upsert("p1", "invoice", {"pack_type": "workflow", "title": "P1"})
    results = store.query("invoice", mode="hybrid")
    assert isinstance(results, list)


# ---------------------------------------------------------------------------
# query() signature has mode and hybrid_alpha
# ---------------------------------------------------------------------------

def test_query_signature_has_mode_param():
    import inspect
    sig = inspect.signature(PackVectorStore.query)
    assert "mode" in sig.parameters
    assert "hybrid_alpha" in sig.parameters
    assert sig.parameters["mode"].default == "auto"
    assert sig.parameters["hybrid_alpha"].default == 0.4
