"""
JobScheduler — background retry loop and scheduled job management.

Wave 3a: job_scheduling_and_storage — retry logic + schedule endpoint.

Features:
- RetryPolicy: exponential backoff with configurable max_attempts
- JobScheduler: background asyncio task that retries failed jobs
- schedule_retry(): re-queue a failed job for retry with delay
- Integrates with JobStore (file-backed, local-first)

Constitutional guarantees:
- Fail-closed: retry loop errors never crash the server process
- Local-first: no Redis/Celery — pure asyncio + file-backed store
- Zero-ambiguity: job.retry_count and job.next_retry_at are explicit fields
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Optional

from harvest_ui.api.job_store import Job, JobStore


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------

@dataclass
class RetryPolicy:
    """Exponential backoff retry configuration."""
    max_attempts: int = 3
    base_delay_s: float = 5.0      # initial delay before first retry
    backoff_factor: float = 2.0    # multiply delay by this each attempt
    max_delay_s: float = 300.0     # cap at 5 minutes

    def delay_for_attempt(self, attempt: int) -> float:
        """Return the delay in seconds for the given attempt number (1-indexed)."""
        delay = self.base_delay_s * (self.backoff_factor ** (attempt - 1))
        return min(delay, self.max_delay_s)

    def should_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts


# ---------------------------------------------------------------------------
# JobScheduler
# ---------------------------------------------------------------------------

class JobScheduler:
    """
    Background asyncio scheduler that retries failed jobs.

    Usage:
        scheduler = JobScheduler(store, runner_fn, policy=RetryPolicy(max_attempts=3))
        task = asyncio.create_task(scheduler.run())
        ...
        task.cancel()

    runner_fn: async function(job_id, params, store) → None
               Called to re-run a job that needs retry.
    """

    def __init__(
        self,
        store: JobStore,
        runner_fn: Callable[[str, Dict[str, Any], JobStore], Coroutine[Any, Any, None]],
        policy: Optional[RetryPolicy] = None,
        poll_interval_s: float = 10.0,
    ):
        self._store = store
        self._runner_fn = runner_fn
        self._policy = policy or RetryPolicy()
        self._poll_interval = poll_interval_s
        self._running = False

    async def run(self) -> None:
        """Main scheduler loop — polls for retryable jobs and re-runs them."""
        self._running = True
        while self._running:
            try:
                await self._tick()
            except Exception:
                pass  # Never let scheduler errors crash the server
            await asyncio.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False

    async def _tick(self) -> None:
        """Single scheduler tick: find eligible retry jobs and launch them."""
        now = time.time()
        failed_jobs = self._store.list_jobs(status="failed")
        for job in failed_jobs:
            retry_count = job.params.get("_retry_count", 0)
            next_retry_at = job.params.get("_next_retry_at", 0.0)

            if not self._policy.should_retry(retry_count):
                continue  # Exhausted retries

            if now < next_retry_at:
                continue  # Not yet due

            # Mark as processing and increment retry counter
            delay = self._policy.delay_for_attempt(retry_count + 1)
            new_params = dict(job.params)
            new_params["_retry_count"] = retry_count + 1
            new_params["_next_retry_at"] = now + delay
            self._store.update(
                job.job_id,
                status="processing",
                params=new_params,
                error=None,
            )
            asyncio.create_task(self._runner_fn(job.job_id, new_params, self._store))

    def schedule_retry(self, job_id: str, delay_s: Optional[float] = None) -> Optional[Job]:
        """
        Manually schedule a failed job for immediate (or delayed) retry.

        Args:
            job_id: ID of the job to retry
            delay_s: Seconds to wait before retrying (default: policy base_delay_s)

        Returns:
            Updated Job or None if job not found / not in failed state.
        """
        job = self._store.get(job_id)
        if job is None or job.status not in ("failed", "pending"):
            return None

        retry_count = job.params.get("_retry_count", 0)
        if not self._policy.should_retry(retry_count):
            return None  # Exhausted

        actual_delay = delay_s if delay_s is not None else self._policy.delay_for_attempt(retry_count + 1)
        new_params = dict(job.params)
        new_params["_retry_count"] = retry_count + 1
        new_params["_next_retry_at"] = time.time() + actual_delay

        return self._store.update(
            job_id,
            status="failed",  # Keep as failed until scheduler picks it up
            params=new_params,
        )

    @property
    def policy(self) -> RetryPolicy:
        return self._policy
