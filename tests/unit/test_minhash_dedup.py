"""Tests for MinHashDeduplicator — near-duplicate detection via MinHash + LSH."""
import json
import pytest
from harvest_index.artifacts.minhash_dedup import (
    MinHashDeduplicator,
    DedupEntry,
    _word_shingles,
    _minhash_signature,
    _jaccard_from_signatures,
    _lsh_band_keys,
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def test_word_shingles_basic():
    shingles = _word_shingles("the quick brown fox jumps", k=3)
    assert "the quick brown" in shingles
    assert "quick brown fox" in shingles
    assert "brown fox jumps" in shingles


def test_word_shingles_short_text():
    shingles = _word_shingles("hello world", k=4)
    # fewer words than k → returns individual words
    assert len(shingles) >= 1


def test_word_shingles_empty():
    shingles = _word_shingles("", k=4)
    assert len(shingles) == 0


def test_minhash_signature_length():
    shingles = _word_shingles("test content here", k=2)
    sig = _minhash_signature(shingles, num_hashes=32)
    assert len(sig) == 32


def test_minhash_signature_empty_shingles():
    import sys
    sig = _minhash_signature(frozenset(), num_hashes=8)
    assert sig == [sys.maxsize] * 8


def test_jaccard_identical_signatures():
    shingles = _word_shingles("identical content text", k=2)
    sig = _minhash_signature(shingles, num_hashes=64)
    assert _jaccard_from_signatures(sig, sig) == 1.0


def test_jaccard_different_signatures():
    sig_a = _minhash_signature(_word_shingles("apple pie recipe baking", k=2), num_hashes=64)
    sig_b = _minhash_signature(_word_shingles("quantum physics particles energy", k=2), num_hashes=64)
    # Not necessarily 0 but should be much less than 1
    sim = _jaccard_from_signatures(sig_a, sig_b)
    assert sim < 0.5


def test_jaccard_different_length_returns_zero():
    assert _jaccard_from_signatures([1, 2, 3], [1, 2]) == 0.0


def test_lsh_band_keys_count():
    sig = list(range(128))
    keys = _lsh_band_keys(sig, num_bands=16)
    assert len(keys) == 16


def test_lsh_band_keys_structure():
    sig = list(range(32))
    keys = _lsh_band_keys(sig, num_bands=4)
    # First band should start with band_idx=0
    assert keys[0][0] == 0
    assert keys[1][0] == 1


# ---------------------------------------------------------------------------
# MinHashDeduplicator
# ---------------------------------------------------------------------------

def test_dedup_empty_index():
    dedup = MinHashDeduplicator()
    assert len(dedup) == 0


def test_dedup_add_not_duplicate():
    dedup = MinHashDeduplicator()
    result = dedup.check_and_add("art-001", "Unique content about Python programming")
    assert not result.is_duplicate
    assert result.match_type == "none"
    assert len(dedup) == 1


def test_dedup_exact_duplicate():
    dedup = MinHashDeduplicator()
    content = "Exact same content for deduplication testing"
    dedup.check_and_add("art-001", content)
    result = dedup.check_and_add("art-002", content)
    assert result.is_duplicate
    assert result.match_type == "exact"
    assert result.similarity == 1.0
    assert result.matched_id == "art-001"


def test_dedup_near_duplicate():
    dedup = MinHashDeduplicator(threshold=0.7, num_hashes=64)
    base = "The quick brown fox jumps over the lazy dog near the river bank today"
    near = "The quick brown fox jumps over the lazy dog near the river bank yesterday"
    dedup.check_and_add("art-001", base)
    result = dedup.check_and_add("art-002", near)
    assert result.is_duplicate
    assert result.match_type == "near"
    assert result.matched_id == "art-001"


def test_dedup_distinct_documents():
    dedup = MinHashDeduplicator(threshold=0.85)
    dedup.check_and_add("art-001", "Python programming language tutorial for beginners")
    result = dedup.check_and_add("art-002", "Quantum mechanics and particle physics fundamentals")
    assert not result.is_duplicate


def test_dedup_remove():
    dedup = MinHashDeduplicator()
    content = "Content to remove"
    dedup.check_and_add("art-001", content)
    assert len(dedup) == 1
    dedup.remove("art-001")
    assert len(dedup) == 0
    # After removal, same content should be addable again
    result = dedup.check_and_add("art-002", content)
    assert not result.is_duplicate


def test_dedup_remove_nonexistent_noop():
    dedup = MinHashDeduplicator()
    dedup.remove("does-not-exist")  # should not raise


def test_dedup_cross_source():
    dedup = MinHashDeduplicator()
    content = "Cross-source document content that is identical"
    dedup.check_and_add("art-001", content, source_id="github")
    result = dedup.check_and_add("art-002", content, source_id="s3")
    assert result.is_duplicate
    assert result.match_type == "exact"


def test_dedup_multiple_unique_docs():
    dedup = MinHashDeduplicator()
    docs = [
        ("a1", "Machine learning and neural networks"),
        ("a2", "Database design and SQL optimization"),
        ("a3", "Web development with React and TypeScript"),
        ("a4", "Docker containerization and Kubernetes"),
        ("a5", "Cloud architecture AWS Azure GCP"),
    ]
    for aid, content in docs:
        result = dedup.check_and_add(aid, content)
        assert not result.is_duplicate
    assert len(dedup) == 5


def test_dedup_never_raises_on_empty_content():
    dedup = MinHashDeduplicator()
    result = dedup.check_and_add("art-empty", "")
    assert isinstance(result.is_duplicate, bool)


def test_dedup_never_raises_on_single_word():
    dedup = MinHashDeduplicator()
    result = dedup.check_and_add("art-word", "hello")
    assert isinstance(result.is_duplicate, bool)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_dedup_save_load_roundtrip(tmp_path):
    dedup = MinHashDeduplicator(threshold=0.8, num_hashes=64, num_bands=8, shingle_k=3)
    dedup.check_and_add("art-001", "First document content here")
    dedup.check_and_add("art-002", "Second document content here")

    path = str(tmp_path / "dedup.json")
    dedup.save(path)

    restored = MinHashDeduplicator.load(path)
    assert len(restored) == 2
    assert restored.threshold == 0.8
    assert restored.num_hashes == 64


def test_dedup_save_load_exact_dedup_still_works(tmp_path):
    dedup = MinHashDeduplicator()
    content = "Persisted content for dedup check"
    dedup.check_and_add("art-001", content)

    path = str(tmp_path / "dedup.json")
    dedup.save(path)

    restored = MinHashDeduplicator.load(path)
    result = restored.check_and_add("art-002", content)
    assert result.is_duplicate
    assert result.match_type == "exact"


def test_dedup_save_atomic(tmp_path):
    dedup = MinHashDeduplicator()
    dedup.check_and_add("art-001", "test content")
    path = str(tmp_path / "subdir" / "dedup.json")
    dedup.save(path)
    from pathlib import Path
    assert Path(path).exists()
    assert not Path(path + ".tmp").exists()


def test_dedup_loaded_near_dedup_still_works(tmp_path):
    dedup = MinHashDeduplicator(threshold=0.7, num_hashes=64)
    base = "The quick brown fox jumps over the lazy dog in the morning sunshine"
    dedup.check_and_add("art-001", base)

    path = str(tmp_path / "dedup.json")
    dedup.save(path)

    restored = MinHashDeduplicator.load(path)
    near = "The quick brown fox jumps over the lazy dog in the morning sunlight"
    result = restored.check_and_add("art-002", near)
    assert result.is_duplicate
