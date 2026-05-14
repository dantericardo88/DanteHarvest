"""
Unit tests for StreamingExporter.

Fully mocked — no aiohttp, no network.  CI-safe.
Uses pytest-asyncio (asyncio_mode=auto) for async tests.
"""

from __future__ import annotations

import asyncio
import json
import pytest

from harvest_distill.export.streaming_exporter import StreamingExporter, _StopSentinel
from harvest_distill.packs.dante_agents_contract import HarvestHandoff


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_handoff(pack_id: str = "wf-001", pack_type: str = "workflowPack") -> HarvestHandoff:
    return HarvestHandoff(
        handoff_id=f"hh-{pack_id}",
        pack_id=pack_id,
        pack_type=pack_type,
        domain="test",
        receipt_id="rcpt-001",
        confidence_score=0.95,
        exported_at="2026-01-01T00:00:00",
        pack_json={"title": "Test Pack"},
        consumption_hints={"agent_role": "executor"},
    )


@pytest.fixture
def queue() -> asyncio.Queue:
    return asyncio.Queue()


@pytest.fixture
def exporter(queue: asyncio.Queue) -> StreamingExporter:
    return StreamingExporter(queue)


# ---------------------------------------------------------------------------
# NDJSON format
# ---------------------------------------------------------------------------

class TestNDJSONStream:
    async def test_yields_ndjson_lines(self, queue, exporter):
        handoff = make_handoff()
        queue.put_nowait(handoff)
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="ndjson")]
        assert len(chunks) == 1
        assert chunks[0].endswith("\n")
        data = json.loads(chunks[0].strip())
        assert data["handoff_id"] == "hh-wf-001"
        assert data["pack_type"] == "workflowPack"

    async def test_ndjson_is_default_format(self, queue, exporter):
        queue.put_nowait(make_handoff())
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs()]
        assert len(chunks) == 1
        assert "\n" in chunks[0]

    async def test_multiple_packs_yielded_in_order(self, queue, exporter):
        for i in range(3):
            queue.put_nowait(make_handoff(pack_id=f"wf-{i:03d}"))
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="ndjson")]
        assert len(chunks) == 3
        ids = [json.loads(c)["pack_id"] for c in chunks]
        assert ids == ["wf-000", "wf-001", "wf-002"]


# ---------------------------------------------------------------------------
# SSE format
# ---------------------------------------------------------------------------

class TestSSEStream:
    async def test_yields_sse_format(self, queue, exporter):
        queue.put_nowait(make_handoff())
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="sse")]
        assert len(chunks) == 1
        assert chunks[0].startswith("data: ")
        assert chunks[0].endswith("\n\n")
        payload = json.loads(chunks[0].removeprefix("data: ").strip())
        assert "handoff_id" in payload

    async def test_sse_double_newline_terminator(self, queue, exporter):
        queue.put_nowait(make_handoff())
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="sse")]
        assert chunks[0].endswith("\n\n")


# ---------------------------------------------------------------------------
# WebSocket format
# ---------------------------------------------------------------------------

class TestWebSocketStream:
    async def test_yields_raw_json(self, queue, exporter):
        queue.put_nowait(make_handoff("sk-001", "skillPack"))
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="websocket")]
        assert len(chunks) == 1
        data = json.loads(chunks[0])
        assert data["pack_id"] == "sk-001"
        assert data["pack_type"] == "skillPack"

    async def test_websocket_no_trailing_newline(self, queue, exporter):
        queue.put_nowait(make_handoff())
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="websocket")]
        assert not chunks[0].endswith("\n")


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestStreamingExporterErrors:
    async def test_invalid_format_raises_value_error(self, exporter):
        with pytest.raises(ValueError, match="Unknown stream format"):
            async for _ in exporter.stream_packs(format="csv"):  # type: ignore[arg-type]
                pass

    async def test_stop_sentinel_ends_generator(self, queue, exporter):
        queue.put_nowait(StreamingExporter.STOP)

        chunks = [c async for c in exporter.stream_packs(format="ndjson")]
        assert chunks == []

    async def test_timeout_ends_generator(self, queue):
        fast_exporter = StreamingExporter(queue, timeout=0.05)
        chunks = [c async for c in fast_exporter.stream_packs(format="ndjson")]
        assert chunks == []

    async def test_task_done_called_for_each_pack(self, queue, exporter):
        queue.put_nowait(make_handoff("wf-a"))
        queue.put_nowait(make_handoff("wf-b"))
        queue.put_nowait(StreamingExporter.STOP)

        async for _ in exporter.stream_packs(format="ndjson"):
            pass
        # If task_done is missing, queue.join() will hang and the test times out
        await asyncio.wait_for(queue.join(), timeout=1.0)


# ---------------------------------------------------------------------------
# collect() helper
# ---------------------------------------------------------------------------

class TestCollect:
    async def test_collect_returns_list(self, queue, exporter):
        queue.put_nowait(make_handoff("ev-001", "evalPack"))
        queue.put_nowait(StreamingExporter.STOP)

        result = await exporter.collect(format="ndjson")
        assert isinstance(result, list)
        assert len(result) == 1

    async def test_collect_max_packs_limits_output(self, queue):
        limited_exporter = StreamingExporter(queue, timeout=0.1)
        for i in range(5):
            queue.put_nowait(make_handoff(f"wf-{i:03d}"))
        queue.put_nowait(StreamingExporter.STOP)

        result = await limited_exporter.collect(format="ndjson", max_packs=2)
        assert len(result) == 2

    async def test_collect_sse_format(self, queue, exporter):
        queue.put_nowait(make_handoff())
        queue.put_nowait(StreamingExporter.STOP)

        result = await exporter.collect(format="sse")
        assert result[0].startswith("data: ")


# ---------------------------------------------------------------------------
# Sentinel singleton (sync — no async needed)
# ---------------------------------------------------------------------------

class TestStopSentinel:
    def test_sentinel_is_singleton(self):
        s1 = _StopSentinel()
        s2 = _StopSentinel()
        assert s1 is s2

    def test_stop_class_attr_is_stop_sentinel(self):
        assert isinstance(StreamingExporter.STOP, _StopSentinel)
