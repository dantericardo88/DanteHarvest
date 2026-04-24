"""Tests for DedupIndex — SimHash near-duplicate detection."""

from harvest_index.artifacts.dedup import DedupIndex, simhash, hamming_distance


def test_identical_text_simhash():
    text = "hello world this is a test"
    assert simhash(text) == simhash(text)


def test_different_text_simhash():
    a = simhash("hello world invoice payment")
    b = simhash("completely unrelated content about cats")
    assert a != b


def test_hamming_distance_identical():
    h = simhash("same text")
    assert hamming_distance(h, h) == 0


def test_hamming_distance_different():
    a = simhash("invoice payment workflow")
    b = simhash("unrelated cats dogs birds")
    assert hamming_distance(a, b) > 3


def test_empty_string_returns_zero():
    assert simhash("") == 0


def test_dedup_index_near_duplicate_detected():
    index = DedupIndex(threshold=3)
    index.add("art-001", "hello world content here for invoice")
    assert index.is_near_duplicate("hello world content here for invoice!") is True


def test_dedup_index_distinct_not_flagged():
    index = DedupIndex(threshold=3)
    index.add("art-001", "invoice payment workflow accounting")
    assert index.is_near_duplicate("completely unrelated cats and dogs content") is False


def test_dedup_index_add_returns_fingerprint():
    index = DedupIndex()
    fp = index.add("art-001", "some content here")
    assert isinstance(fp, int)
    assert fp >= 0


def test_dedup_index_exclude_self():
    index = DedupIndex(threshold=3)
    index.add("art-001", "exact same text content")
    assert index.is_near_duplicate("exact same text content", exclude_id="art-001") is False


def test_dedup_index_find_near_duplicates():
    index = DedupIndex(threshold=3)
    text = "invoice payment workflow content processing"
    index.add("art-001", text)
    index.add("art-002", "completely unrelated dogs cats birds fish")
    dups = index.find_near_duplicates(text)
    assert "art-001" in dups
    assert "art-002" not in dups


def test_dedup_index_remove():
    index = DedupIndex(threshold=3)
    index.add("art-001", "invoice payment workflow content")
    index.remove("art-001")
    assert index.is_near_duplicate("invoice payment workflow content") is False


def test_dedup_index_clear():
    index = DedupIndex(threshold=3)
    index.add("art-001", "content one")
    index.add("art-002", "content two")
    index.clear()
    assert len(index) == 0
