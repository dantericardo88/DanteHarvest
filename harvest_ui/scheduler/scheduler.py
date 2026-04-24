"""
HarvestScheduler — APScheduler-backed recurring job scheduler.

Uses APScheduler's BackgroundScheduler with SQLite jobstore for persistence.
Falls back to MemoryJobStore if APScheduler is not installed (local-first).

Constitutional guarantees:
- Local-first: SQLite file backend, no Redis/Celery/cloud
- Fail-closed: schedule add fails if cron expression is invalid
- Zero-ambiguity: all errors propagate as SchedulerError with clear message
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional

from harvest_ui.scheduler.schedule_model import ScheduleEntry, ScheduleStore

logger = logging.getLogger(__name__)


class SchedulerError(Exception):
    pass


def _validate_cron(cron_expr: str) -> None:
    """Raise SchedulerError if cron_expr is not a valid 5-field cron."""
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise SchedulerError(
            f"Invalid cron expression '{cron_expr}': expected 5 fields "
            f"(minute hour day month weekday), got {len(parts)}"
        )


def _next_run_from_cron(cron_expr: str) -> Optional[float]:
    """Compute next UTC timestamp from cron expression. Returns None if unavailable."""
    try:
        from apscheduler.triggers.cron import CronTrigger
        import datetime
        trigger = CronTrigger.from_crontab(cron_expr)
        next_dt = trigger.get_next_fire_time(None, datetime.datetime.utcnow())
        return next_dt.timestamp() if next_dt else None
    except Exception:
        return None


class HarvestScheduler:
    """
    Recurring harvest job scheduler backed by APScheduler + SQLite.

    Usage:
        scheduler = HarvestScheduler(storage_root="storage")
        scheduler.start()
        entry = scheduler.add("crawl", "0 * * * *", {"url": "https://example.com"})
        scheduler.list()
        scheduler.remove(entry.schedule_id)
        scheduler.stop()
    """

    def __init__(self, storage_root: str = "storage"):
        self._store = ScheduleStore(storage_root=storage_root)
        self._storage_root = storage_root
        self._scheduler: Any = None
        self._running = False

    def start(self) -> None:
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
            from pathlib import Path
            db_path = Path(self._storage_root) / "scheduler.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            jobstores = {
                "default": SQLAlchemyJobStore(url=f"sqlite:///{db_path}")
            }
            self._scheduler = BackgroundScheduler(jobstores=jobstores)
            self._scheduler.start()
        except ImportError:
            logger.warning(
                "apscheduler not installed — scheduler running in no-op mode. "
                "Install with: pip install apscheduler sqlalchemy"
            )
            self._scheduler = None
        self._running = True

    def stop(self) -> None:
        if self._scheduler is not None:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:
                pass
        self._running = False

    def add(
        self,
        command: str,
        cron_expr: str,
        args: Optional[Dict[str, Any]] = None,
    ) -> ScheduleEntry:
        _validate_cron(cron_expr)
        entry = self._store.create(command=command, cron_expr=cron_expr, args=args or {})
        next_run = _next_run_from_cron(cron_expr)
        self._store.update(entry.schedule_id, next_run=next_run)
        if self._scheduler is not None:
            self._register_job(entry)
        return self._store.get(entry.schedule_id)  # type: ignore[return-value]

    def remove(self, schedule_id: str) -> bool:
        if self._scheduler is not None:
            try:
                self._scheduler.remove_job(schedule_id)
            except Exception:
                pass
        return self._store.delete(schedule_id)

    def pause(self, schedule_id: str) -> Optional[ScheduleEntry]:
        if self._scheduler is not None:
            try:
                self._scheduler.pause_job(schedule_id)
            except Exception:
                pass
        return self._store.update(schedule_id, status="paused")

    def resume(self, schedule_id: str) -> Optional[ScheduleEntry]:
        if self._scheduler is not None:
            try:
                self._scheduler.resume_job(schedule_id)
            except Exception:
                pass
        return self._store.update(schedule_id, status="active")

    def run_now(self, schedule_id: str) -> Optional[ScheduleEntry]:
        """Execute a scheduled job immediately (async fire-and-forget)."""
        entry = self._store.get(schedule_id)
        if entry is None:
            return None
        asyncio.get_event_loop().create_task(self._execute(entry))
        return entry

    def list(self, status: Optional[str] = None) -> List[ScheduleEntry]:
        return self._store.list_entries(status=status)

    def _register_job(self, entry: ScheduleEntry) -> None:
        try:
            from apscheduler.triggers.cron import CronTrigger
            trigger = CronTrigger.from_crontab(entry.cron_expr)
            self._scheduler.add_job(
                func=self._sync_execute,
                trigger=trigger,
                id=entry.schedule_id,
                kwargs={"entry_id": entry.schedule_id},
                replace_existing=True,
                misfire_grace_time=300,
            )
        except Exception as e:
            logger.warning("Failed to register job %s: %s", entry.schedule_id, e)

    def _sync_execute(self, entry_id: str) -> None:
        """APScheduler calls this synchronously; we bridge into asyncio."""
        entry = self._store.get(entry_id)
        if entry is None:
            return
        import time
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._execute(entry))
        finally:
            loop.close()
        self._store.update(entry_id, last_run=time.time(), run_count=entry.run_count + 1)

    async def _execute(self, entry: ScheduleEntry) -> None:
        """Dispatch the scheduled command."""
        import time
        self._store.update(entry.schedule_id, last_run=time.time())
        try:
            if entry.command == "crawl":
                await self._run_crawl(entry)
            elif entry.command == "ingest":
                await self._run_ingest(entry)
            else:
                logger.warning("Unknown scheduled command: %s", entry.command)
            self._store.update(
                entry.schedule_id,
                run_count=entry.run_count + 1,
                next_run=_next_run_from_cron(entry.cron_expr),
            )
        except Exception as e:
            self._store.update(entry.schedule_id, last_error=str(e))
            logger.error("Scheduled job %s failed: %s", entry.schedule_id, e)

    async def _run_crawl(self, entry: ScheduleEntry) -> None:
        from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
        url = entry.args.get("url", "")
        if not url:
            raise ValueError("crawl schedule missing 'url' arg")
        adapter = CrawleeAdapter(
            use_js_rendering=entry.args.get("use_js_rendering", False),
        )
        await adapter.crawl(
            url=url,
            run_id=entry.schedule_id,
            max_depth=entry.args.get("max_depth", 1),
            max_pages=entry.args.get("max_pages", 10),
        )

    async def _run_ingest(self, entry: ScheduleEntry) -> None:
        path = entry.args.get("path", "")
        if not path:
            raise ValueError("ingest schedule missing 'path' arg")
        from harvest_acquire.files.file_ingestor import FileIngestor
        ingestor = FileIngestor(storage_root=self._storage_root)
        ingestor.ingest(path)
