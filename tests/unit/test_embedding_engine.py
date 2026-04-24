"""Tests for EmbeddingEngine — local sentence-transformers semantic embeddings."""

import pytest
import sys
from unittest.mock import patch, MagicMock
from harvest_index.search.embedding_engine import EmbeddingEngine, CachedEmbeddingEngine
from harvest_core.control.exceptions import StorageError


def test_embed_empty_string_returns_zeros():
    engine = EmbeddingEngine()
    vec = engine.embed("")
    assert isinstance(vec, list)
    assert len(vec) == engine.dim
    assert all(v == 0.0 for v in vec)


def test_dim_default():
    engine = EmbeddingEngine()
    assert engine.dim == 384


def test_embed_batch_empty():
    engine = EmbeddingEngine()
    result = engine.embed_batch([])
    assert result == []


def test_sentence_transformers_not_installed_raises():
    engine = EmbeddingEngine()
    with patch.dict(sys.modules, {"sentence_transformers": None}):
        # Force reload of the model
        engine._model = None
        with pytest.raises(StorageError, match="sentence-transformers"):
            engine._embed_local("test text")


def test_is_available_returns_bool():
    engine = EmbeddingEngine()
    result = engine.is_available()
    assert isinstance(result, bool)


def test_cached_engine_writes_to_disk(tmp_path):
    engine = CachedEmbeddingEngine(cache_dir=str(tmp_path / "embed_cache"))
    mock_vec = [0.1, 0.2, 0.3]

    with patch.object(EmbeddingEngine, "embed", return_value=mock_vec):
        vec1 = engine.embed("hello world")
        vec2 = engine.embed("hello world")

    assert vec1 == mock_vec
    assert vec2 == mock_vec
    cache_files = list((tmp_path / "embed_cache").glob("*.json"))
    assert len(cache_files) == 1


def test_cached_engine_reads_from_cache(tmp_path):
    import json
    cache_dir = tmp_path / "embed_cache"
    cache_dir.mkdir()

    import hashlib
    text = "cached text"
    key = hashlib.sha256(text.encode()).hexdigest()
    cached_vec = [0.5, 0.6, 0.7]
    (cache_dir / f"{key}.json").write_text(json.dumps(cached_vec))

    engine = CachedEmbeddingEngine(cache_dir=str(cache_dir))
    with patch.object(EmbeddingEngine, "embed", side_effect=AssertionError("should not call")):
        vec = engine.embed(text)

    assert vec == cached_vec
