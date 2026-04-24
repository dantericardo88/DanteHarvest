"""
RetryPolicy — exponential backoff with jitter for URL acquisition.

Harvested from: Crawl4AI RateLimiter patterns.

Wraps an async callable with configurable retry, backoff, and chain
entry emission per attempt.  Fail-closed: exhausted retries raise
the last exception, never silently return None.

Zero-ambiguity: max_retries=0 means no retries (immediate fail-through).
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Coroutine, List, Optional, Set, Type

from harvest_core.control.exceptions import AcquisitionError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


class RetryPolicy:
    """
    Exponential backoff retry wrapper for async acquisition calls.

    delay_n = base_delay * (backoff_factor ** attempt) + random(0, jitter)

    Usage:
        policy = RetryPolicy(max_retries=3, base_delay=1.0, backoff_factor=2.0)
        result = await policy.execute(
            fn=ingestor.ingest,
            run_id="run-001",
            chain_writer=writer,
            path=some_path,
        )
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        backoff_factor: float = 2.0,
        jitter: float = 0.5,
        retry_on_status: Optional[Set[int]] = None,
        retry_on_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.retry_on_status = retry_on_status or {429, 503, 502, 504}
        self.retry_on_exceptions = retry_on_exceptions or [AcquisitionError]

    async def execute(
        self,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        run_id: str,
        chain_writer: Optional[ChainWriter] = None,
        context: Optional[dict] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Call fn(**kwargs), retrying up to max_retries times on failure.
        Emits acquire.retry chain entry per retry attempt.
        Raises the last exception after exhaustion (fail-closed).
        """
        last_exc: Optional[Exception] = None
        ctx = context or {}

        for attempt in range(self.max_retries + 1):
            try:
                return await fn(**kwargs)
            except tuple(self.retry_on_exceptions) as exc:  # type: ignore[misc]
                last_exc = exc
                if attempt >= self.max_retries:
                    break

                delay = self._compute_delay(attempt)

                if chain_writer:
                    await chain_writer.append(ChainEntry(
                        run_id=run_id,
                        signal="acquire.retry",
                        machine="retry_policy",
                        data={
                            "attempt": attempt + 1,
                            "max_retries": self.max_retries,
                            "delay_seconds": round(delay, 3),
                            "error": str(exc),
                            **{k: str(v) for k, v in ctx.items()},
                        },
                    ))

                await asyncio.sleep(delay)

        raise last_exc  # type: ignore[misc]

    def _compute_delay(self, attempt: int) -> float:
        backoff = self.base_delay * (self.backoff_factor ** attempt)
        jitter_amount = random.uniform(0, self.jitter)
        return backoff + jitter_amount

    def with_no_sleep(self) -> "_FastRetryPolicy":
        """Return a variant that skips asyncio.sleep for testing."""
        return _FastRetryPolicy(
            max_retries=self.max_retries,
            base_delay=self.base_delay,
            backoff_factor=self.backoff_factor,
            jitter=self.jitter,
            retry_on_exceptions=self.retry_on_exceptions,
        )


class _FastRetryPolicy(RetryPolicy):
    """Test variant: no actual sleep, instantaneous retry."""

    async def execute(self, fn, run_id, chain_writer=None, context=None, **kwargs):
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                return await fn(**kwargs)
            except tuple(self.retry_on_exceptions) as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    break
                if chain_writer:
                    await chain_writer.append(ChainEntry(
                        run_id=run_id,
                        signal="acquire.retry",
                        machine="retry_policy",
                        data={"attempt": attempt + 1, "error": str(exc)},
                    ))
        raise last_exc
