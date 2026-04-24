"""
File-backed async job store for the extraction API.

Jobs are written as JSON files under <storage_root>/jobs/<job_id>.json.
No external dependencies — fully local-first.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


JOB_STATES = frozenset({"pending", "processing", "completed", "failed"})


@dataclass
class Job:
    job_id: str
    kind: str  # "scrape" | "extract" | "crawl"
    status: str  # pending | processing | completed | failed
    created_at: float
    updated_at: float
    url: str
    params: Dict[str, Any]
    result: Optional[Any] = None
    error: Optional[str] = None
    pages: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Job":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


class JobStore:
    """Persist and retrieve jobs from disk."""

    def __init__(self, storage_root: str = "storage"):
        self._root = Path(storage_root) / "jobs"
        self._root.mkdir(parents=True, exist_ok=True)

    def create(self, kind: str, url: str, params: Dict[str, Any]) -> Job:
        now = time.time()
        job = Job(
            job_id=str(uuid4()),
            kind=kind,
            status="pending",
            created_at=now,
            updated_at=now,
            url=url,
            params=params,
        )
        self._write(job)
        return job

    def get(self, job_id: str) -> Optional[Job]:
        path = self._root / f"{job_id}.json"
        if not path.exists():
            return None
        return Job.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def update(self, job_id: str, **kwargs) -> Optional[Job]:
        job = self.get(job_id)
        if job is None:
            return None
        for k, v in kwargs.items():
            if hasattr(job, k):
                setattr(job, k, v)
        job.updated_at = time.time()
        self._write(job)
        return job

    def list_jobs(self, kind: Optional[str] = None, status: Optional[str] = None) -> List[Job]:
        jobs = []
        for p in self._root.glob("*.json"):
            try:
                job = Job.from_dict(json.loads(p.read_text(encoding="utf-8")))
                if kind and job.kind != kind:
                    continue
                if status and job.status != status:
                    continue
                jobs.append(job)
            except Exception:
                pass
        return sorted(jobs, key=lambda j: j.created_at, reverse=True)

    def _write(self, job: Job) -> None:
        tmp = self._root / f"{job.job_id}.json.tmp"
        tmp.write_text(json.dumps(job.to_dict(), indent=2), encoding="utf-8")
        tmp.replace(self._root / f"{job.job_id}.json")
