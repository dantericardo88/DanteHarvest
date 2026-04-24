"""Unit tests for ChainEntry and ChainWriter."""

import asyncio
import json
from pathlib import Path

import pytest

from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.control.exceptions import ChainError


def make_entry(run_id: str = "run-001", signal: str = "acquire.started") -> ChainEntry:
    return ChainEntry(run_id=run_id, signal=signal, machine="acquire", data={"url": "https://example.com"})


class TestChainEntry:
    def test_compute_hash_is_deterministic(self):
        e = make_entry()
        assert e.compute_hash() == e.compute_hash()

    def test_hash_excludes_hash_field(self):
        e = make_entry()
        h1 = e.compute_hash()
        e.content_hash = "something_else"
        assert e.compute_hash() == h1

    def test_jsonl_roundtrip(self):
        e = make_entry()
        e.sequence = 1
        e.content_hash = e.compute_hash()
        line = e.to_jsonl_line()
        restored = ChainEntry.from_jsonl_line(line)
        assert restored.run_id == e.run_id
        assert restored.signal == e.signal
        assert restored.content_hash == e.content_hash

    def test_invalid_signal_format_raises(self):
        with pytest.raises(Exception):
            ChainEntry(run_id="r", signal="noseparator", machine="m")


class TestChainWriter:
    @pytest.mark.asyncio
    async def test_append_assigns_sequence(self, tmp_path):
        writer = ChainWriter(tmp_path / "chain.jsonl", "run-001")
        entry = make_entry()
        result = await writer.append(entry)
        assert result.sequence == 1
        assert result.content_hash is not None

    @pytest.mark.asyncio
    async def test_append_increments_sequence(self, tmp_path):
        writer = ChainWriter(tmp_path / "chain.jsonl", "run-001")
        for i in range(3):
            e = make_entry()
            result = await writer.append(e)
            assert result.sequence == i + 1

    @pytest.mark.asyncio
    async def test_run_id_mismatch_raises(self, tmp_path):
        writer = ChainWriter(tmp_path / "chain.jsonl", "run-001")
        entry = make_entry(run_id="run-WRONG")
        with pytest.raises(ChainError):
            await writer.append(entry)

    @pytest.mark.asyncio
    async def test_verify_integrity_passes(self, tmp_path):
        writer = ChainWriter(tmp_path / "chain.jsonl", "run-001")
        for _ in range(5):
            await writer.append(make_entry())
        ok, err = writer.verify_integrity()
        assert ok is True
        assert err is None

    @pytest.mark.asyncio
    async def test_read_all_returns_all_entries(self, tmp_path):
        writer = ChainWriter(tmp_path / "chain.jsonl", "run-001")
        for _ in range(4):
            await writer.append(make_entry())
        entries = writer.read_all()
        assert len(entries) == 4
