"""Tests for harvest_core.provenance.auto_sealing_chain_writer."""
import asyncio
import pytest
from pathlib import Path


def _make_entry(sequence=1, signal="observe.screenshot", data=None, run_id="run-001"):
    from harvest_core.provenance.chain_entry import ChainEntry
    import socket
    return ChainEntry(
        sequence=sequence,
        run_id=run_id,
        signal=signal,
        machine=socket.gethostname(),
        data=data or {"url": "https://example.com"},
    )


@pytest.mark.asyncio
async def test_append_seals_after_each_entry(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    entry = _make_entry()
    await writer.append(entry)
    assert writer.seal_count == 1
    assert writer.is_sealed


@pytest.mark.asyncio
async def test_append_batch_seals_once(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    entries = [_make_entry(i) for i in range(3)]
    await writer.append_batch(entries)
    assert writer.seal_count >= 1
    assert writer.is_sealed


@pytest.mark.asyncio
async def test_seal_every_n_batches(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=3,
    )
    for i in range(2):
        await writer.append(_make_entry(i + 1))
    # 2 appends, seal_every_n=3 → not yet sealed
    assert writer.seal_count == 0

    await writer.append(_make_entry(3))
    # 3rd append triggers seal
    assert writer.seal_count == 1


@pytest.mark.asyncio
async def test_seal_every_n_zero_never_auto_seals(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=0,
    )
    for i in range(5):
        await writer.append(_make_entry(i + 1))
    assert writer.seal_count == 0


@pytest.mark.asyncio
async def test_seal_now_explicit(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=0,
    )
    await writer.append(_make_entry())
    assert writer.seal_count == 0
    result = writer.seal_now()
    assert result is True
    assert writer.seal_count == 1


def test_seal_now_empty_chain_returns_true(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    result = writer.seal_now()
    assert result is True  # fail-open: no entries, still returns True
    assert writer.seal_count == 0


@pytest.mark.asyncio
async def test_verify_after_seal(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    await writer.append(_make_entry())
    ok, reason = writer.verify()
    assert ok is True
    assert reason is None


def test_verify_unsealed_returns_false(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=0,
    )
    ok, reason = writer.verify()
    assert ok is False
    assert reason is not None


@pytest.mark.asyncio
async def test_last_seal_at_is_set(tmp_path):
    import time
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    t0 = time.time()
    await writer.append(_make_entry())
    assert writer.last_seal_at is not None
    assert writer.last_seal_at >= t0


@pytest.mark.asyncio
async def test_last_manifest_set_after_seal(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    assert writer.last_manifest is None
    await writer.append(_make_entry())
    assert writer.last_manifest is not None


@pytest.mark.asyncio
async def test_seal_count_increments(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    for i in range(3):
        await writer.append(_make_entry(i + 1))
    assert writer.seal_count == 3


@pytest.mark.asyncio
async def test_manifest_path_exists_after_seal(tmp_path):
    from harvest_core.provenance.auto_sealing_chain_writer import AutoSealingChainWriter
    writer = AutoSealingChainWriter(
        chain_file_path=tmp_path / "chain.jsonl",
        run_id="run-001",
        seal_every_n=1,
    )
    await writer.append(_make_entry())
    assert writer.manifest_path.exists()
