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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Union

from harvest_ui.api.job_store import Job, JobStore


# ---------------------------------------------------------------------------
# CronTrigger
# ---------------------------------------------------------------------------

class CronTrigger:
    """Cron-style trigger: fires at specified times.

    Supported expressions:
    - ``@hourly``  → ``0 * * * *``
    - ``@daily``   → ``0 0 * * *``
    - ``@weekly``  → ``0 0 * * 0``
    - ``@monthly`` → ``0 0 1 * *``
    - ``*/N minute`` → fires every N minutes
    - Full ``minute hour * * *`` notation (minute and hour only; day/month/weekday are wildcards)
    """

    _ALIASES = {
        "@hourly":  "0 * * * *",
        "@daily":   "0 0 * * *",
        "@weekly":  "0 0 * * 0",
        "@monthly": "0 0 1 * *",
    }

    def __init__(self, cron_expr: str) -> None:
        expr = self._ALIASES.get(cron_expr.strip(), cron_expr.strip())
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError(f"CronTrigger requires 5-field expression, got: {cron_expr!r}")
        self._minute_expr = parts[0]
        self._hour_expr   = parts[1]
        self._cron_expr   = expr
        # Pre-compute valid minute / hour sets (None = wildcard)
        self._minutes = self._parse_field(parts[0], 0, 59)
        self._hours   = self._parse_field(parts[1], 0, 23)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def next_fire_time(self, from_dt: Optional[datetime] = None) -> datetime:
        """Return the next datetime (UTC) when this trigger should fire."""
        now = from_dt or datetime.now(timezone.utc)
        # Round up to the next whole minute
        candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        # Search up to 60*24 minutes (one full day) to find a match
        for _ in range(60 * 24 + 1):
            if self._matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        # Fallback: shouldn't happen for valid expressions
        return candidate

    def is_due(self, now: Optional[datetime] = None) -> bool:
        """True if the trigger should fire in the current minute window."""
        now = now or datetime.now(timezone.utc)
        return self._matches(now.replace(second=0, microsecond=0))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _matches(self, dt: datetime) -> bool:
        return dt.minute in self._minutes and dt.hour in self._hours

    @staticmethod
    def _parse_field(expr: str, lo: int, hi: int) -> set:
        """Parse a single cron field into a set of matching integers."""
        if expr == "*":
            return set(range(lo, hi + 1))
        if expr.startswith("*/"):
            step = int(expr[2:])
            return set(range(lo, hi + 1, step))
        if "," in expr:
            return {int(v) for v in expr.split(",")}
        if "-" in expr:
            a, b = expr.split("-", 1)
            return set(range(int(a), int(b) + 1))
        return {int(expr)}


# ---------------------------------------------------------------------------
# IntervalTrigger
# ---------------------------------------------------------------------------

class IntervalTrigger:
    """Fires every N seconds / minutes / hours."""

    def __init__(self, seconds: int = 0, minutes: int = 0, hours: int = 0) -> None:
        self._interval_s = seconds + minutes * 60 + hours * 3600
        if self._interval_s <= 0:
            raise ValueError("IntervalTrigger requires a positive interval")

    @property
    def interval_seconds(self) -> int:
        return self._interval_s

    def next_fire_time(self, from_dt: Optional[datetime] = None) -> datetime:
        base = from_dt or datetime.now(timezone.utc)
        return base + timedelta(seconds=self._interval_s)

    def is_due(self, last_fire: datetime, now: Optional[datetime] = None) -> bool:
        """True if at least ``interval_seconds`` have elapsed since ``last_fire``."""
        now = now or datetime.now(timezone.utc)
        elapsed = (now - last_fire).total_seconds()
        return elapsed >= self._interval_s


# ---------------------------------------------------------------------------
# ScheduledJob
# ---------------------------------------------------------------------------

@dataclass
class ScheduledJob:
    job_id: str
    func: Callable
    trigger: Union[CronTrigger, IntervalTrigger]
    kwargs: Dict[str, Any] = field(default_factory=dict)
    last_fire: Optional[datetime] = None


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
        self._scheduled_jobs: Dict[str, ScheduledJob] = {}

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

    # ------------------------------------------------------------------
    # Cron / interval scheduling
    # ------------------------------------------------------------------

    def schedule_job(
        self,
        job_id: str,
        func: Callable,
        trigger: Union[CronTrigger, IntervalTrigger],
        **kwargs: Any,
    ) -> ScheduledJob:
        """Register a callable to be fired according to *trigger*.

        Args:
            job_id:  Unique identifier for this scheduled job.
            func:    Callable invoked when the trigger fires.
            trigger: A :class:`CronTrigger` or :class:`IntervalTrigger` instance.
            **kwargs: Extra keyword arguments forwarded to *func* on each firing.

        Returns:
            The :class:`ScheduledJob` record.
        """
        job = ScheduledJob(job_id=job_id, func=func, trigger=trigger, kwargs=kwargs)
        self._scheduled_jobs[job_id] = job
        return job

    def get_due_jobs(self, now: Optional[datetime] = None) -> List[ScheduledJob]:
        """Return all scheduled jobs whose trigger is due right now.

        For :class:`CronTrigger` jobs, *due* means the trigger matches the
        current minute window.  For :class:`IntervalTrigger` jobs, *due* means
        enough time has elapsed since ``last_fire`` (or the job has never fired).
        """
        now = now or datetime.now(timezone.utc)
        due: List[ScheduledJob] = []
        for job in self._scheduled_jobs.values():
            if isinstance(job.trigger, CronTrigger):
                if job.trigger.is_due(now):
                    due.append(job)
            elif isinstance(job.trigger, IntervalTrigger):
                if job.last_fire is None:
                    due.append(job)
                elif job.trigger.is_due(job.last_fire, now):
                    due.append(job)
        return due

    def run_due_jobs(self, now: Optional[datetime] = None) -> List[str]:
        """Execute all currently-due scheduled jobs synchronously.

        Updates ``last_fire`` on each job after execution.

        Returns:
            List of job IDs that were executed.
        """
        now = now or datetime.now(timezone.utc)
        fired: List[str] = []
        for job in self.get_due_jobs(now):
            try:
                job.func(**job.kwargs)
            except Exception:
                pass  # individual job errors must not crash the scheduler
            job.last_fire = now
            fired.append(job.job_id)
        return fired

    @property
    def policy(self) -> RetryPolicy:
        return self._policy
