"""Unit tests for merkle_chain_sealing improvements — AutoSealingChainWriter."""

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from harvest_core.provenance.auto_sealing_chain_writer import (
    AutoSealingChainWriter,
    ChainSealError,
    _sign_manifest,
    _verify_manifest,
)
from harvest_core.provenance.chain_entry import ChainEntry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_entry(run_id: str = "test-run", seq: int = 1, signal: str = "acquire.started") -> ChainEntry:
    entry = ChainEntry(run_id=run_id, signal=signal, machine="test", data={"seq": seq})
    entry.sequence = seq
    entry.content_hash = entry.compute_hash()
    return entry


def make_writer(tmp_path: Path, seal_every_n: int = 1) -> AutoSealingChainWriter:
    chain_file = tmp_path / "chain.jsonl"
    chain_file.touch()
    return AutoSealingChainWriter(
        chain_file_path=chain_file,
        run_id="test-run",
        seal_every_n=seal_every_n,
    )


# ---------------------------------------------------------------------------
# ChainSealError
# ---------------------------------------------------------------------------

class TestChainSealError:
    def test_is_exception_subclass(self):
        assert issubclass(ChainSealError, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(ChainSealError, match="oops"):
            raise ChainSealError("oops")


# ---------------------------------------------------------------------------
# _sign_manifest
# ---------------------------------------------------------------------------

class TestSignManifest:
    def test_returns_64_char_hex(self):
        manifest = {"chain_path": "/tmp/x.jsonl", "entry_count": 3}
        sig = _sign_manifest(manifest)
        assert isinstance(sig, str)
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_deterministic_for_same_input(self):
        manifest = {"a": 1, "b": "two"}
        assert _sign_manifest(manifest) == _sign_manifest(manifest)

    def test_different_manifest_yields_different_signature(self):
        m1 = {"entry_count": 1}
        m2 = {"entry_count": 2}
        assert _sign_manifest(m1) != _sign_manifest(m2)

    def test_explicit_key_changes_signature(self):
        manifest = {"data": "hello"}
        sig_default = _sign_manifest(manifest)
        sig_custom = _sign_manifest(manifest, key=b"custom-key")
        assert sig_default != sig_custom

    def test_key_order_independent_via_sort_keys(self):
        m1 = {"b": 2, "a": 1}
        m2 = {"a": 1, "b": 2}
        assert _sign_manifest(m1) == _sign_manifest(m2)


# ---------------------------------------------------------------------------
# _verify_manifest
# ---------------------------------------------------------------------------

class TestVerifyManifest:
    def test_returns_true_for_matching_signature(self):
        manifest = {"chain_path": "/tmp/chain.jsonl", "entry_count": 5}
        sig = _sign_manifest(manifest)
        assert _verify_manifest(manifest, sig) is True

    def test_returns_false_for_wrong_signature(self):
        manifest = {"entry_count": 3}
        assert _verify_manifest(manifest, "a" * 64) is False

    def test_returns_false_for_tampered_manifest(self):
        manifest = {"entry_count": 3}
        sig = _sign_manifest(manifest)
        tampered = {"entry_count": 99}
        assert _verify_manifest(tampered, sig) is False

    def test_explicit_key_round_trip(self):
        key = b"explicit-signing-key"
        manifest = {"run_id": "run-007"}
        sig = _sign_manifest(manifest, key=key)
        assert _verify_manifest(manifest, sig, key=key) is True

    def test_wrong_key_fails_verification(self):
        manifest = {"run_id": "run-007"}
        sig = _sign_manifest(manifest, key=b"key-a")
        assert _verify_manifest(manifest, sig, key=b"key-b") is False


# ---------------------------------------------------------------------------
# seal_now — fail closed
# ---------------------------------------------------------------------------

class TestSealNowFailClosed:
    def test_seal_now_raises_ChainSealError_on_merkle_failure(self, tmp_path):
        writer = make_writer(tmp_path)
        # Patch _merkle.seal to raise
        writer._merkle.seal = MagicMock(side_effect=RuntimeError("corrupt state"))
        # Inject a fake entry so read_all returns something
        writer.read_all = MagicMock(return_value=[make_entry()])
        with pytest.raises(ChainSealError, match="Merkle seal failed"):
            writer.seal_now()

    def test_seal_now_does_not_swallow_errors(self, tmp_path):
        writer = make_writer(tmp_path)
        writer._merkle.seal = MagicMock(side_effect=ValueError("hash mismatch"))
        writer.read_all = MagicMock(return_value=[make_entry()])
        raised = False
        try:
            writer.seal_now()
        except ChainSealError:
            raised = True
        assert raised, "ChainSealError must be raised, not swallowed"

    def test_seal_now_returns_true_on_success(self, tmp_path):
        writer = make_writer(tmp_path)
        # Empty chain — returns True without sealing
        writer.read_all = MagicMock(return_value=[])
        assert writer.seal_now() is True

    def test_manifest_contains_signature_after_seal(self, tmp_path):
        writer = make_writer(tmp_path)
        # Append a real entry and seal via the async path
        async def run():
            entry = make_entry()
            await writer.append(entry)

        asyncio.run(run())
        manifest_path = writer.manifest_path
        assert manifest_path.exists()
        data = json.loads(manifest_path.read_text())
        assert "_signature" in data
        assert len(data["_signature"]) == 64


# ---------------------------------------------------------------------------
# verify_chain_integrity
# ---------------------------------------------------------------------------

class TestVerifyChainIntegrity:
    def test_returns_dict_with_required_keys(self, tmp_path):
        writer = make_writer(tmp_path)
        result = writer.verify_chain_integrity()
        assert set(result.keys()) >= {"valid", "entries", "errors"}

    def test_valid_false_when_not_sealed(self, tmp_path):
        writer = make_writer(tmp_path, seal_every_n=0)
        result = writer.verify_chain_integrity()
        assert result["valid"] is False
        assert result["entries"] == 0
        assert any("sealed" in e for e in result["errors"])

    def test_valid_true_after_successful_seal(self, tmp_path):
        writer = make_writer(tmp_path)

        async def run():
            await writer.append(make_entry(seq=1))
            await writer.append(make_entry(seq=2))

        asyncio.run(run())
        result = writer.verify_chain_integrity()
        assert result["valid"] is True
        assert result["entries"] == 2
        assert result["errors"] == []

    def test_entries_count_matches_chain(self, tmp_path):
        writer = make_writer(tmp_path)

        async def run():
            for i in range(5):
                await writer.append(make_entry(seq=i + 1))

        asyncio.run(run())
        result = writer.verify_chain_integrity()
        assert result["entries"] == 5

    def test_detects_tampered_signature(self, tmp_path):
        writer = make_writer(tmp_path)

        async def run():
            await writer.append(make_entry(seq=1))

        asyncio.run(run())

        # Tamper with the manifest signature
        manifest_path = writer.manifest_path
        data = json.loads(manifest_path.read_text())
        data["_signature"] = "0" * 64
        manifest_path.write_text(json.dumps(data))

        result = writer.verify_chain_integrity()
        assert result["valid"] is False
        assert any("signature" in e.lower() for e in result["errors"])

    def test_detects_missing_signature(self, tmp_path):
        writer = make_writer(tmp_path)

        async def run():
            await writer.append(make_entry(seq=1))

        asyncio.run(run())

        # Remove signature from manifest
        manifest_path = writer.manifest_path
        data = json.loads(manifest_path.read_text())
        data.pop("_signature", None)
        manifest_path.write_text(json.dumps(data))

        result = writer.verify_chain_integrity()
        assert result["valid"] is False
        assert any("signature" in e.lower() or "unsigned" in e.lower() for e in result["errors"])


# ---------------------------------------------------------------------------
# get_chain_manifest
# ---------------------------------------------------------------------------

class TestGetChainManifest:
    def test_raises_ChainSealError_when_not_sealed(self, tmp_path):
        writer = make_writer(tmp_path, seal_every_n=0)
        with pytest.raises(ChainSealError, match="sealed"):
            writer.get_chain_manifest()

    def test_returns_dict_after_seal(self, tmp_path):
        writer = make_writer(tmp_path)

        async def run():
            await writer.append(make_entry(seq=1))

        asyncio.run(run())
        manifest = writer.get_chain_manifest()
        assert isinstance(manifest, dict)
        assert "merkle_root" in manifest
        assert "_signature" in manifest

    def test_manifest_signature_is_valid(self, tmp_path):
        writer = make_writer(tmp_path)

        async def run():
            await writer.append(make_entry(seq=1))

        asyncio.run(run())
        manifest = writer.get_chain_manifest()
        sig = manifest.pop("_signature")
        assert _verify_manifest(manifest, sig) is True
