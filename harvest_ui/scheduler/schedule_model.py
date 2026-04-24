"""
ScheduleEntry — persistent schedule record for recurring harvest jobs.

Stored as JSON files under <storage_root>/schedules/<id>.json.
No external database required — local-first file backend.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


@dataclass
class ScheduleEntry:
    schedule_id: str
    command: str          # "crawl" | "ingest" | "watch"
    cron_expr: str        # standard 5-field cron (e.g. "0 * * * *")
    args: Dict[str, Any]  # command-specific args (url, depth, path, ...)
    created_at: float
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    status: str = "active"   # active | paused | deleted
    last_error: Optional[str] = None
    run_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ScheduleEntry":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


class ScheduleStore:
    """File-backed persistent store for schedule entries."""

    def __init__(self, storage_root: str = "storage"):
        self._root = Path(storage_root) / "schedules"
        self._root.mkdir(parents=True, exist_ok=True)

    def create(
        self,
        command: str,
        cron_expr: str,
        args: Dict[str, Any],
    ) -> ScheduleEntry:
        entry = ScheduleEntry(
            schedule_id=str(uuid4()),
            command=command,
            cron_expr=cron_expr,
            args=args,
            created_at=time.time(),
        )
        self._write(entry)
        return entry

    def get(self, schedule_id: str) -> Optional[ScheduleEntry]:
        path = self._root / f"{schedule_id}.json"
        if not path.exists():
            return None
        return ScheduleEntry.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def list_entries(self, status: Optional[str] = None) -> List[ScheduleEntry]:
        entries = []
        for p in self._root.glob("*.json"):
            try:
                e = ScheduleEntry.from_dict(json.loads(p.read_text(encoding="utf-8")))
                if status is None or e.status == status:
                    entries.append(e)
            except Exception:
                pass
        return sorted(entries, key=lambda e: e.created_at)

    def update(self, schedule_id: str, **kwargs: Any) -> Optional[ScheduleEntry]:
        entry = self.get(schedule_id)
        if entry is None:
            return None
        for k, v in kwargs.items():
            if hasattr(entry, k):
                setattr(entry, k, v)
        self._write(entry)
        return entry

    def delete(self, schedule_id: str) -> bool:
        entry = self.update(schedule_id, status="deleted")
        return entry is not None

    def _write(self, entry: ScheduleEntry) -> None:
        tmp = self._root / f"{entry.schedule_id}.json.tmp"
        tmp.write_text(json.dumps(entry.to_dict(), indent=2), encoding="utf-8")
        tmp.replace(self._root / f"{entry.schedule_id}.json")
