"""Unit tests for MerkleChainManifest — Merkle-root sealing of the evidence chain."""

import json

import pytest

from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.merkle_chain import (
    MerkleChainManifest,
    MerkleManifest,
    _build_merkle_root,
    _sha256_hex,
)
from harvest_core.control.exceptions import ChainError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_entry(run_id: str = "run-001", seq: int = 1, signal: str = "acquire.started") -> ChainEntry:
    entry = ChainEntry(run_id=run_id, signal=signal, machine="test", data={"seq": seq})
    entry.sequence = seq
    entry.content_hash = entry.compute_hash()
    return entry


def make_entries(count: int) -> list:
    return [make_entry(seq=i + 1, signal=f"acquire.step") for i in range(count)]


# ---------------------------------------------------------------------------
# _sha256_hex
# ---------------------------------------------------------------------------

class TestSha256Hex:
    def test_returns_64_char_hex(self):
        result = _sha256_hex("hello")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self):
        assert _sha256_hex("abc") == _sha256_hex("abc")

    def test_different_inputs_differ(self):
        assert _sha256_hex("a") != _sha256_hex("b")

    def test_empty_string(self):
        result = _sha256_hex("")
        assert len(result) == 64


# ---------------------------------------------------------------------------
# _build_merkle_root
# ---------------------------------------------------------------------------

class TestBuildMerkleRoot:
    def test_empty_list_returns_hash_of_empty(self):
        root = _build_merkle_root([])
        assert root == _sha256_hex("")

    def test_single_leaf(self):
        leaf = _sha256_hex("only")
        root = _build_merkle_root([leaf])
        assert root == leaf

    def test_two_leaves(self):
        l1 = _sha256_hex("left")
        l2 = _sha256_hex("right")
        root = _build_merkle_root([l1, l2])
        expected = _sha256_hex(l1 + l2)
        assert root == expected

    def test_three_leaves_pads_to_four(self):
        leaves = [_sha256_hex(f"leaf_{i}") for i in range(3)]
        root = _build_merkle_root(leaves)
        assert len(root) == 64

    def test_deterministic(self):
        leaves = [_sha256_hex(f"x{i}") for i in range(7)]
        assert _build_merkle_root(leaves) == _build_merkle_root(leaves)

    def test_different_order_yields_different_root(self):
        leaves = [_sha256_hex(f"l{i}") for i in range(4)]
        root1 = _build_merkle_root(leaves)
        root2 = _build_merkle_root(list(reversed(leaves)))
        assert root1 != root2

    def test_large_list(self):
        leaves = [_sha256_hex(f"entry_{i}") for i in range(100)]
        root = _build_merkle_root(leaves)
        assert len(root) == 64


# ---------------------------------------------------------------------------
# MerkleManifest dataclass
# ---------------------------------------------------------------------------

class TestMerkleManifest:
    def test_to_dict_contains_expected_keys(self):
        m = MerkleManifest(
            chain_path="/tmp/chain.jsonl",
            sealed_at="2026-01-01T00:00:00+00:00",
            entry_count=3,
            leaf_hashes=["aaa", "bbb", "ccc"],
            merkle_root="rrr",
        )
        d = m.to_dict()
        assert "chain_path" in d
        assert "merkle_root" in d
        assert "leaf_hashes" in d
        assert d["entry_count"] == 3

    def test_from_dict_roundtrip(self):
        m = MerkleManifest(
            chain_path="/tmp/test.jsonl",
            sealed_at="2026-01-01T00:00:00+00:00",
            entry_count=5,
            leaf_hashes=["a", "b", "c", "d", "e"],
            merkle_root="root_hash",
        )
        restored = MerkleManifest.from_dict(m.to_dict())
        assert restored.merkle_root == m.merkle_root
        assert restored.entry_count == m.entry_count
        assert restored.leaf_hashes == m.leaf_hashes


# ---------------------------------------------------------------------------
# MerkleChainManifest — seal
# ---------------------------------------------------------------------------

class TestMerkleChainManifestSeal:
    def test_seal_creates_manifest_file(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)

        entries = make_entries(5)
        manifest = mcm.seal(entries)

        assert mcm.manifest_path.exists()
        assert manifest.entry_count == 5
        assert len(manifest.leaf_hashes) == 5
        assert len(manifest.merkle_root) == 64

    def test_seal_empty_entries(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)

        manifest = mcm.seal([])
        assert manifest.entry_count == 0
        assert manifest.leaf_hashes == []
        assert manifest.merkle_root == _sha256_hex("")

    def test_seal_manifest_is_valid_json(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)
        mcm.seal(make_entries(3))

        with open(mcm.manifest_path) as f:
            data = json.load(f)
        assert data["entry_count"] == 3
        assert "merkle_root" in data

    def test_seal_is_deterministic_for_same_entries(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        entries = make_entries(4)

        mcm1 = MerkleChainManifest(chain_path)
        m1 = mcm1.seal(entries)

        mcm2 = MerkleChainManifest(chain_path)
        m2 = mcm2.seal(entries)

        assert m1.merkle_root == m2.merkle_root

    def test_seal_different_entries_yield_different_root(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)

        m1 = mcm.seal(make_entries(4))
        m2 = mcm.seal(make_entries(5))
        assert m1.merkle_root != m2.merkle_root

    def test_is_not_sealed_before_seal(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)
        assert not mcm.is_sealed()

    def test_is_sealed_after_seal(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)
        mcm.seal(make_entries(2))
        assert mcm.is_sealed()


# ---------------------------------------------------------------------------
# MerkleChainManifest — verify
# ---------------------------------------------------------------------------

class TestMerkleChainManifestVerify:
    def test_verify_passes_for_unchanged_chain(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        entries = make_entries(6)
        mcm = MerkleChainManifest(chain_path)
        mcm.seal(entries)

        ok, reason = mcm.verify(entries)
        assert ok is True
        assert reason is None

    def test_verify_fails_for_missing_manifest(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)

        with pytest.raises(ChainError, match="No manifest"):
            mcm.verify(make_entries(3))

    def test_verify_fails_when_entry_count_changed(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        entries = make_entries(4)
        mcm = MerkleChainManifest(chain_path)
        mcm.seal(entries)

        ok, reason = mcm.verify(make_entries(5))
        assert ok is False
        assert isinstance(reason, str) and "count" in reason.lower()

    def test_verify_fails_when_entry_content_changed(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        entries = make_entries(4)
        mcm = MerkleChainManifest(chain_path)
        mcm.seal(entries)

        tampered = list(entries)
        tampered[2] = make_entry(seq=99, signal="tampered.event")

        ok, reason = mcm.verify(tampered)
        assert ok is False
        assert reason is not None

    def test_verify_empty_chain(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)
        mcm.seal([])

        ok, reason = mcm.verify([])
        assert ok is True
        assert reason is None

    def test_load_manifest(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)
        entries = make_entries(3)
        sealed = mcm.seal(entries)

        loaded = mcm.load_manifest()
        assert loaded.merkle_root == sealed.merkle_root
        assert loaded.entry_count == 3

    def test_load_manifest_raises_when_missing(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)

        with pytest.raises(ChainError):
            mcm.load_manifest()

    def test_verify_fails_for_none_entries(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        chain_path.touch()
        mcm = MerkleChainManifest(chain_path)
        mcm.seal(make_entries(2))

        ok, reason = mcm.verify(None)
        assert ok is False
