"""
AuditLogger — structured operator audit log wired to the evidence chain.

Wave 7d: audit_log_completeness — full operator audit trail (5→9).

Every significant action in DanteHarvest generates an audit event:
  APPROVE, REJECT, DEFER, ERASURE, EXPORT, KEY_ROTATE, PROMOTE, DEMOTE,
  CRAWL_START, CRAWL_STOP, CONFIG_CHANGE, LOGIN, REPLAY_RUN, etc.

Each event is:
1. Written to an append-only JSONL audit log (local disk)
2. Appended to the evidence chain (cryptographic tamper evidence)
3. Queryable via AuditLog.query(operator=..., event_type=..., since=...)

Constitutional guarantees:
- Append-only: audit log is never truncated or overwritten
- Tamper-evident: every event hashed into evidence chain
- Fail-open: chain append failure does NOT block the audit write
- Local-first: all audit state on local disk, zero external calls
"""

from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from uuid import uuid4


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------

class AuditEventType:
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    DEFER = "DEFER"
    ERASURE = "ERASURE"
    EXPORT = "EXPORT"
    KEY_ROTATE = "KEY_ROTATE"
    PROMOTE = "PROMOTE"
    DEMOTE = "DEMOTE"
    CRAWL_START = "CRAWL_START"
    CRAWL_STOP = "CRAWL_STOP"
    CONFIG_CHANGE = "CONFIG_CHANGE"
    REPLAY_RUN = "REPLAY_RUN"
    REVIEW_DECISION = "REVIEW_DECISION"
    GDPR_REQUEST = "GDPR_REQUEST"
    RETENTION_GC = "RETENTION_GC"
    ARTIFACT_INGEST = "ARTIFACT_INGEST"
    PACK_REGISTER = "PACK_REGISTER"
    PACK_DIFF = "PACK_DIFF"
    CHAIN_SEAL = "CHAIN_SEAL"
    OBSERVATION_START = "OBSERVATION_START"
    OBSERVATION_STOP = "OBSERVATION_STOP"
    CUSTOM = "CUSTOM"


# ---------------------------------------------------------------------------
# AuditEvent
# ---------------------------------------------------------------------------

@dataclass
class AuditEvent:
    event_id: str
    event_type: str
    operator: str                   # user ID, machine name, or service name
    session_id: Optional[str]
    resource_id: Optional[str]      # pack_id, artifact_id, etc.
    resource_type: Optional[str]    # "pack" | "artifact" | "key" | "config" | ...
    outcome: str                    # "success" | "failure" | "skipped"
    details: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    machine: str = field(default_factory=socket.gethostname)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "AuditEvent":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})

    @property
    def iso_timestamp(self) -> str:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------

class AuditLogger:
    """
    Structured audit logger that writes to both JSONL log and evidence chain.

    Usage:
        logger = AuditLogger(log_dir=Path("storage/audit"))
        event = logger.log(
            event_type=AuditEventType.APPROVE,
            operator="user-abc",
            resource_id="wf-001",
            resource_type="pack",
            outcome="success",
            details={"reason": "manual review passed"},
        )
        # event is now in both audit log and evidence chain
    """

    LOG_FILE = "audit.jsonl"

    def __init__(
        self,
        log_dir: Path,
        chain_writer=None,          # Optional ChainWriter for evidence chain integration
        default_operator: str = "system",
    ):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self._log_dir / self.LOG_FILE
        self._chain_writer = chain_writer
        self._default_operator = default_operator

    def log(
        self,
        event_type: str,
        operator: Optional[str] = None,
        resource_id: Optional[str] = None,
        resource_type: Optional[str] = None,
        outcome: str = "success",
        details: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ) -> AuditEvent:
        """
        Record an audit event. Writes to JSONL log and optionally to chain.
        Returns the AuditEvent (never raises — fail-open).
        """
        event = AuditEvent(
            event_id=str(uuid4()),
            event_type=event_type,
            operator=operator or self._default_operator,
            session_id=session_id,
            resource_id=resource_id,
            resource_type=resource_type,
            outcome=outcome,
            details=details or {},
        )
        self._append_log(event)
        self._append_chain(event)
        return event

    def query(
        self,
        event_type: Optional[str] = None,
        operator: Optional[str] = None,
        resource_id: Optional[str] = None,
        outcome: Optional[str] = None,
        since: Optional[float] = None,
        until: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[AuditEvent]:
        """
        Query the audit log with optional filters.
        Returns events in chronological order.
        """
        results = []
        for event in self._iter_events():
            if event_type and event.event_type != event_type:
                continue
            if operator and event.operator != operator:
                continue
            if resource_id and event.resource_id != resource_id:
                continue
            if outcome and event.outcome != outcome:
                continue
            if since and event.timestamp < since:
                continue
            if until and event.timestamp > until:
                continue
            results.append(event)
            if limit and len(results) >= limit:
                break
        return results

    def recent(self, n: int = 50) -> List[AuditEvent]:
        """Return the N most recent audit events."""
        all_events = list(self._iter_events())
        return all_events[-n:]

    def stats(self) -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        total = 0
        for event in self._iter_events():
            counts[event.event_type] = counts.get(event.event_type, 0) + 1
            total += 1
        return {"total": total, "by_type": counts}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _append_log(self, event: AuditEvent) -> None:
        try:
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event.to_dict()) + "\n")
        except Exception:
            pass

    def _append_chain(self, event: AuditEvent) -> None:
        if not self._chain_writer:
            return
        try:
            import asyncio
            from harvest_core.provenance.chain_entry import ChainEntry
            entry = ChainEntry(
                run_id=event.session_id or "audit",
                signal="audit.event",
                machine=event.machine,
                data={
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "operator": event.operator,
                    "resource_id": event.resource_id,
                    "outcome": event.outcome,
                },
            )
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self._chain_writer.append(entry))
            else:
                loop.run_until_complete(self._chain_writer.append(entry))
        except Exception:
            pass

    def _iter_events(self) -> Iterator[AuditEvent]:
        if not self._log_path.exists():
            return
        for line in self._log_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield AuditEvent.from_dict(json.loads(line))
            except Exception:
                pass
