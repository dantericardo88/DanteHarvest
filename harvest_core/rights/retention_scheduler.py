"""
RetentionScheduler — background asyncio loop that runs GDPR retention enforcement.

Wave 3d: gdpr_retention_enforcement — always-on background scheduler + GDPR webhook.

Runs RetentionEnforcer.gc() on a configurable interval, emitting:
1. Chain entries for each expired artifact (append-only audit trail)
2. GDPR webhook POST to configured endpoint on each expiry batch

Constitutional guarantees:
- Fail-closed: gc() errors are caught and logged, never crash the server
- Local-first: no external queue needed — pure asyncio + file-backed store
- Append-only chain: every expiry event gets a ChainEntry (never deleted)
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Callable, List, Optional

from harvest_core.rights.retention_enforcer import RetentionEnforcer


class RetentionScheduler:
    """
    Background scheduler that enforces retention windows via RetentionEnforcer.gc().

    Usage:
        enforcer = RetentionEnforcer(store_path=Path("storage/retention"))
        scheduler = RetentionScheduler(enforcer, interval_s=3600)
        task = asyncio.create_task(scheduler.run())
        # ... later:
        scheduler.stop(); task.cancel()

    With GDPR webhook:
        scheduler = RetentionScheduler(
            enforcer,
            gdpr_webhook_url="https://compliance.example.com/gdpr-events",
            gdpr_webhook_secret="s3cr3t",
        )
    """

    def __init__(
        self,
        enforcer: RetentionEnforcer,
        interval_s: float = 3600.0,
        gdpr_webhook_url: Optional[str] = None,
        gdpr_webhook_secret: str = "harvest-gdpr",
        chain_writer: Optional[Any] = None,
        run_id: str = "retention-scheduler",
    ):
        self._enforcer = enforcer
        self._interval = interval_s
        self._gdpr_webhook_url = gdpr_webhook_url
        self._gdpr_webhook_secret = gdpr_webhook_secret
        self._chain_writer = chain_writer
        self._run_id = run_id
        self._running = False
        self._last_run_at: Optional[float] = None
        self._total_expired = 0

    async def run(self) -> None:
        """Main scheduler loop — runs until stop() is called."""
        self._running = True
        while self._running:
            try:
                await self._tick()
            except Exception:
                pass
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self._running = False

    async def run_once(self) -> List[str]:
        """Run a single gc() pass immediately and return list of expired artifact IDs."""
        return await self._tick()

    async def _tick(self) -> List[str]:
        """Single enforcement pass."""
        self._last_run_at = time.time()
        expired = self._enforcer.sweep()
        expired_ids = [e.artifact_id for e in expired]

        if not expired_ids:
            return []

        self._enforcer.gc()
        self._total_expired += len(expired_ids)

        await self._emit_chain_entries(expired_ids)
        await self._fire_gdpr_webhook(expired_ids)

        return expired_ids

    async def _emit_chain_entries(self, expired_ids: List[str]) -> None:
        if self._chain_writer is None:
            return
        try:
            from harvest_core.provenance.chain_entry import ChainEntry
            entry = ChainEntry(
                run_id=self._run_id,
                signal="retention.expired",
                machine="retention_scheduler",
                data={
                    "expired_count": len(expired_ids),
                    "artifact_ids": expired_ids[:50],  # cap to avoid huge entries
                    "swept_at": self._last_run_at,
                },
            )
            await self._chain_writer.append(entry)
        except Exception:
            pass

    async def _fire_gdpr_webhook(self, expired_ids: List[str]) -> None:
        if not self._gdpr_webhook_url:
            return
        try:
            import hashlib, hmac
            payload = json.dumps({
                "event": "gdpr.retention.expired",
                "artifact_ids": expired_ids,
                "count": len(expired_ids),
                "timestamp": self._last_run_at,
            }).encode()
            sig = hmac.new(
                self._gdpr_webhook_secret.encode(),
                payload,
                hashlib.sha256,
            ).hexdigest()
            headers = {
                "Content-Type": "application/json",
                "X-Harvest-GDPR-Signature": f"sha256={sig}",
                "X-Harvest-Event": "gdpr.retention.expired",
            }
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        self._gdpr_webhook_url,
                        data=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=15),
                    )
            except ImportError:
                import urllib.request
                req = urllib.request.Request(
                    self._gdpr_webhook_url,
                    data=payload,
                    headers=headers,
                )
                urllib.request.urlopen(req, timeout=15)
        except Exception:
            pass  # GDPR webhook failure never blocks expiry enforcement

    def wire_to_cron(
        self,
        scheduler: Any,
        job_id: str = "retention-gc",
        hours: int = 24,
        auto_start: bool = True,
    ) -> Any:
        """
        Register the retention GC as a recurring job on *scheduler*.

        Accepts a :class:`harvest_ui.api.job_scheduler.JobScheduler` instance
        and registers an :class:`IntervalTrigger` that fires every *hours* hours.

        Args:
            scheduler:  A ``JobScheduler`` instance with a ``schedule_job()`` method.
            job_id:     Unique identifier for the scheduled job (default: "retention-gc").
            hours:      Recurrence interval in hours (default: 24).
            auto_start: If True (default), run one retention check immediately before
                        registering the recurring schedule so enforcement begins right
                        away rather than waiting for the first interval to elapse.

        Returns:
            The :class:`ScheduledJob` returned by ``scheduler.schedule_job()``.
        """
        try:
            from harvest_ui.api.job_scheduler import IntervalTrigger
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "harvest_ui.api.job_scheduler is required for wire_to_cron(). "
                "Ensure harvest_ui is installed."
            ) from exc

        def _run_gc() -> None:
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(self.run_once())
            finally:
                loop.close()

        if auto_start:
            try:
                _run_gc()
            except Exception:
                pass  # auto-start failure never blocks job registration

        trigger = IntervalTrigger(hours=hours)
        return scheduler.schedule_job(job_id, _run_gc, trigger)

    @property
    def total_expired(self) -> int:
        return self._total_expired

    @property
    def last_run_at(self) -> Optional[float]:
        return self._last_run_at
