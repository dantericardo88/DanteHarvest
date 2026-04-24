"""
RunRegistry — local-first run store with JSON persistence.

Tracks lifecycle of every RunContract. Emits chain entries on every
state change. Fail-closed: get_run() raises HarvestError on unknown run_id.

One-door doctrine: all run state changes go through update_run_state().
"""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from harvest_core.control.exceptions import HarvestError
from harvest_core.control.run_contract import RunContract
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Valid state transitions
_TRANSITIONS: Dict[RunStatus, set] = {
    RunStatus.PENDING:   {RunStatus.RUNNING, RunStatus.CANCELLED},
    RunStatus.RUNNING:   {RunStatus.PAUSED, RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED},
    RunStatus.PAUSED:    {RunStatus.RUNNING, RunStatus.CANCELLED},
    RunStatus.COMPLETED: set(),  # terminal
    RunStatus.FAILED:    set(),  # terminal
    RunStatus.CANCELLED: set(),  # terminal
}


class RunRecord:
    __slots__ = ("contract", "status", "chain_writer", "created_at", "updated_at", "error")

    def __init__(self, contract: RunContract, storage_root: str = "storage"):
        self.contract = contract
        self.status = RunStatus.PENDING
        self.chain_writer = ChainWriter(
            Path(contract.chain_file_path(storage_root)), contract.run_id
        )
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "contract": self.contract.model_dump(mode="json"),
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict, storage_root: str) -> "RunRecord":
        record = cls(RunContract(**data["contract"]), storage_root)
        record.status = RunStatus(data["status"])
        record.created_at = datetime.fromisoformat(data["created_at"])
        record.updated_at = datetime.fromisoformat(data["updated_at"])
        record.error = data.get("error")
        return record


class RunRegistry:
    """
    In-memory registry of active and recent runs.

    Usage:
        registry = RunRegistry(storage_root="storage")
        record = await registry.create_run(contract)
        await registry.update_run_state(run_id, RunStatus.RUNNING)
    """

    def __init__(self, storage_root: str = "storage"):
        self.storage_root = storage_root
        self._index_path = Path(storage_root) / "runs_index.json"
        self._runs: Dict[str, RunRecord] = self._load_runs()

    async def create_run(self, contract: RunContract) -> RunRecord:
        """Register a new run and emit the run.created chain entry."""
        if contract.run_id in self._runs:
            raise HarvestError(f"Run '{contract.run_id}' already registered")

        record = RunRecord(contract, self.storage_root)
        self._runs[contract.run_id] = record
        self._save_runs()

        await record.chain_writer.append(ChainEntry(
            run_id=contract.run_id,
            signal="run.created",
            machine="registry",
            data={
                "project_id": contract.project_id,
                "source_class": contract.source_class.value,
                "initiated_by": contract.initiated_by,
                "mode": contract.mode.value,
            },
        ))
        return record

    def get_run(self, run_id: str) -> RunRecord:
        """Return the RunRecord for run_id. Fail-closed: raises if not found."""
        record = self._runs.get(run_id)
        if record is None:
            raise HarvestError(f"Unknown run_id: '{run_id}'")
        return record

    async def update_run_state(
        self,
        run_id: str,
        new_status: RunStatus,
        error: Optional[str] = None,
    ) -> RunRecord:
        """
        Transition run to new_status. Emits chain entry. Fail-closed on invalid transitions.
        """
        record = self.get_run(run_id)
        allowed = _TRANSITIONS[record.status]

        if new_status not in allowed:
            raise HarvestError(
                f"Invalid transition {record.status} → {new_status} for run '{run_id}'",
                {"allowed": [s.value for s in allowed]},
            )

        old_status = record.status
        record.status = new_status
        record.updated_at = datetime.utcnow()
        if error:
            record.error = error
        self._save_runs()

        await record.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal=f"run.{new_status.value}",
            machine="registry",
            data={
                "from_status": old_status.value,
                "to_status": new_status.value,
                **({"error": error} if error else {}),
            },
        ))
        return record

    def list_runs(self, project_id: Optional[str] = None) -> List[RunRecord]:
        """Return all runs, optionally filtered by project_id."""
        records = list(self._runs.values())
        if project_id:
            records = [r for r in records if r.contract.project_id == project_id]
        return records

    def get_stats(self) -> Dict[str, int]:
        counts: Dict[str, int] = {s.value: 0 for s in RunStatus}
        for record in self._runs.values():
            counts[record.status.value] += 1
        counts["total"] = len(self._runs)
        return counts

    def _load_runs(self) -> Dict[str, RunRecord]:
        if not self._index_path.exists():
            return {}
        try:
            raw = json.loads(self._index_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        records: Dict[str, RunRecord] = {}
        for run_id, data in raw.items():
            try:
                records[run_id] = RunRecord.from_dict(data, self.storage_root)
            except Exception:
                continue
        return records

    def _save_runs(self) -> None:
        self._index_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            run_id: record.to_dict()
            for run_id, record in self._runs.items()
        }
        tmp = self._index_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self._index_path)
