"""
StreamingExporter — async generator that yields HarvestHandoff packs as they are promoted.

Event-driven (not batch-only). Subscribes to pack promotion events via an asyncio.Queue.
Supports:
  - SSE  (server-sent events)
  - NDJSON stream
  - WebSocket push   (caller owns the socket; we yield formatted chunks)

Usage:
    queue: asyncio.Queue[HarvestHandoff] = asyncio.Queue()
    exporter = StreamingExporter(queue)

    # producer: put a promoted handoff on the queue
    await queue.put(handoff)

    # consumer: iterate the generator
    async for chunk in exporter.stream_packs(format="ndjson"):
        send_to_client(chunk)

Constitutional guarantees:
- Fail-closed: unknown format raises ValueError immediately, before any yield.
- Non-blocking: queue.get() is awaited; caller controls event loop.
- Sentinel: put StreamingExporter.STOP sentinel to end the generator cleanly.
"""

from __future__ import annotations

import asyncio
import json
from typing import AsyncGenerator, Literal

from harvest_distill.packs.dante_agents_contract import HarvestHandoff


StreamFormat = Literal["ndjson", "sse", "websocket"]

_VALID_FORMATS: frozenset[str] = frozenset({"ndjson", "sse", "websocket"})


class _StopSentinel:
    """Singleton sentinel placed on the queue to stop the generator."""
    _instance: "_StopSentinel | None" = None

    def __new__(cls) -> "_StopSentinel":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


class StreamingExporter:
    """
    Async generator that streams HarvestHandoff packs as they are promoted.

    Args:
        queue:   asyncio.Queue fed by the pack-promotion pipeline.
        timeout: seconds to wait for the next pack before raising StopAsyncIteration
                 (None = wait forever).
    """

    STOP = _StopSentinel()

    def __init__(
        self,
        queue: asyncio.Queue,
        timeout: float | None = None,
    ) -> None:
        self._queue = queue
        self._timeout = timeout

    async def stream_packs(
        self,
        format: StreamFormat = "ndjson",  # noqa: A002
    ) -> AsyncGenerator[str, None]:
        """
        Async generator that yields formatted strings for each promoted HarvestHandoff.

        Args:
            format: one of "ndjson", "sse", "websocket"

        Yields:
            Formatted string chunks ready for the wire.

        Raises:
            ValueError: if format is not recognised (raised before first yield).
            asyncio.TimeoutError: if timeout is set and no pack arrives in time.
        """
        if format not in _VALID_FORMATS:
            raise ValueError(
                f"Unknown stream format '{format}'. "
                f"Valid options: {sorted(_VALID_FORMATS)}"
            )

        while True:
            try:
                if self._timeout is not None:
                    item = await asyncio.wait_for(
                        self._queue.get(), timeout=self._timeout
                    )
                else:
                    item = await self._queue.get()
            except asyncio.TimeoutError:
                return

            if item is self.STOP or isinstance(item, _StopSentinel):
                self._queue.task_done()
                return

            handoff: HarvestHandoff = item
            payload = json.dumps(handoff.to_dict(), default=str)

            if format == "ndjson":
                yield payload + "\n"
            elif format == "sse":
                yield f"data: {payload}\n\n"
            elif format == "websocket":
                yield payload

            self._queue.task_done()

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    async def collect(
        self,
        format: StreamFormat = "ndjson",  # noqa: A002
        max_packs: int | None = None,
    ) -> list[str]:
        """
        Drain the queue into a list (useful for testing).

        Args:
            format:    wire format passed to stream_packs().
            max_packs: stop after this many packs (None = until STOP sentinel).
        """
        results: list[str] = []
        count = 0
        async for chunk in self.stream_packs(format=format):
            results.append(chunk)
            count += 1
            if max_packs is not None and count >= max_packs:
                break
        return results
