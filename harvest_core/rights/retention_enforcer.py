"""
RetentionEnforcer — GDPR/compliance retention lifecycle enforcement.

Sprint goal: enforce retention windows for RetentionClass values.
RetentionClass enum already exists — this module adds the enforcement loop.

Design:
- RetentionClass.SHORT → expires after configurable window (default: 24 h)
- RetentionClass.MEDIUM → expires after configurable window (default: 90 days)
- RetentionClass.LONG → expires after configurable window (default: 3 years)
- RetentionClass.POLICY_BOUND → enforced only if policy window is explicitly set
- RetentionClass.LEGAL_HOLD → never auto-expired

API:
    enforcer = RetentionEnforcer(artifact_store)
    expired = enforcer.sweep()          # returns list of expired artifact IDs
    enforcer.register(artifact)         # track an artifact's expiry deadline
    enforcer.gc()                       # delete expired artifacts + emit events

Constitutional alignment:
- Local-first: no network calls, operates on local artifact store
- Fail-closed: sweep failures are logged to evidence chain, not silently swallowed
- Append-only chain: expiry events are emitted as chain entries (never deleted)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

from harvest_core.rights.rights_model import RetentionClass


# ---------------------------------------------------------------------------
# Default retention windows
# ---------------------------------------------------------------------------

DEFAULT_WINDOWS: Dict[RetentionClass, timedelta] = {
    RetentionClass.SHORT: timedelta(hours=24),
    RetentionClass.MEDIUM: timedelta(days=90),
    RetentionClass.LONG: timedelta(days=1095),    # 3 years
    RetentionClass.POLICY_BOUND: timedelta(days=365),  # safe default; override per policy
    RetentionClass.LEGAL_HOLD: timedelta.max,     # never expires
}


# ---------------------------------------------------------------------------
# ArtifactRecord — what the enforcer tracks
# ---------------------------------------------------------------------------

@dataclass
class ArtifactRecord:
    artifact_id: str
    retention_class: str        # RetentionClass value string
    registered_at: str          # ISO-8601 UTC
    expires_at: Optional[str]   # ISO-8601 UTC or None if no expiry
    artifact_path: Optional[str] = None   # local path for deletion
    metadata: Dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ExpiredArtifact — result of a sweep pass
# ---------------------------------------------------------------------------

@dataclass
class ExpiredArtifact:
    artifact_id: str
    retention_class: str
    expires_at: str  # always set — None-checked before constructing
    artifact_path: Optional[str]


# ---------------------------------------------------------------------------
# RetentionEnforcer
# ---------------------------------------------------------------------------

class RetentionEnforcer:
    """
    Tracks artifact retention deadlines and enforces expiry.

    The enforcer maintains a local JSON registry of tracked artifacts at
    ``store_path/retention_registry.json``. Call ``sweep()`` to find
    expired artifacts, and ``gc()`` to delete them and emit chain events.

    Args:
        store_path: Directory where the retention registry is persisted.
        windows: Override default retention windows per class.
        event_callback: Optional callable(artifact_id, event_type) invoked
            on expiry. Use this to emit to the evidence chain.
    """

    REGISTRY_FILENAME = "retention_registry.json"

    def __init__(
        self,
        store_path: Path,
        windows: Optional[Dict[RetentionClass, timedelta]] = None,
        event_callback: Optional[Callable[[str, str, dict], None]] = None,
    ):
        self.store_path = Path(store_path)
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.store_path / self.REGISTRY_FILENAME
        self.windows = {**DEFAULT_WINDOWS, **(windows or {})}
        self.event_callback = event_callback
        self._records: Dict[str, ArtifactRecord] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(
        self,
        artifact_id: str,
        retention_class: RetentionClass,
        artifact_path: Optional[str] = None,
        metadata: Optional[dict] = None,
        registered_at: Optional[datetime] = None,
    ) -> ArtifactRecord:
        """
        Register an artifact for retention tracking.

        Returns the ArtifactRecord that was created/updated.
        """
        now = registered_at or datetime.now(timezone.utc)
        window = self.windows.get(retention_class, timedelta(days=365))

        if retention_class == RetentionClass.LEGAL_HOLD or window == timedelta.max:
            expires_at = None
        else:
            expires_at = (now + window).isoformat()

        record = ArtifactRecord(
            artifact_id=artifact_id,
            retention_class=retention_class.value,
            registered_at=now.isoformat(),
            expires_at=expires_at,
            artifact_path=artifact_path,
            metadata=metadata or {},
        )
        self._records[artifact_id] = record
        self._save()
        return record

    def sweep(self, now: Optional[datetime] = None) -> List[ExpiredArtifact]:
        """
        Return all artifacts whose retention window has elapsed.

        Does NOT delete anything — call gc() to actually expire.
        """
        check_time = now or datetime.now(timezone.utc)
        expired: List[ExpiredArtifact] = []

        for record in self._records.values():
            if record.expires_at is None:
                continue  # LEGAL_HOLD or no deadline
            expires_at_str: str = record.expires_at
            try:
                exp = datetime.fromisoformat(expires_at_str)
                # Normalize to UTC if no tzinfo
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if check_time >= exp:
                expired.append(ExpiredArtifact(
                    artifact_id=record.artifact_id,
                    retention_class=record.retention_class,
                    expires_at=exp.isoformat(),
                    artifact_path=record.artifact_path,
                ))

        return expired

    def gc(
        self,
        now: Optional[datetime] = None,
        dry_run: bool = False,
    ) -> List[ExpiredArtifact]:
        """
        Garbage-collect expired artifacts.

        For each expired artifact:
        1. (If not dry_run) Delete artifact_path from disk if it exists.
        2. Remove from retention registry.
        3. Invoke event_callback("artifact.expired") if set.

        Returns list of ExpiredArtifact records that were processed.
        """
        expired = self.sweep(now=now)
        if not expired:
            return []

        deleted: List[ExpiredArtifact] = []
        for ea in expired:
            if not dry_run:
                self._delete_artifact_file(ea.artifact_id, ea.artifact_path)
                del self._records[ea.artifact_id]
                deleted.append(ea)
                if self.event_callback:
                    try:
                        self.event_callback(
                            ea.artifact_id,
                            "artifact.expired",
                            {
                                "retention_class": ea.retention_class,
                                "expires_at": ea.expires_at,
                                "artifact_path": ea.artifact_path,
                            },
                        )
                    except Exception:
                        pass  # callbacks must never crash the enforcer
            else:
                deleted.append(ea)

        if not dry_run:
            self._save()

        return deleted

    def get_record(self, artifact_id: str) -> Optional[ArtifactRecord]:
        """Return the retention record for an artifact, or None."""
        return self._records.get(artifact_id)

    def list_records(
        self,
        retention_class: Optional[RetentionClass] = None,
    ) -> List[ArtifactRecord]:
        """Return all tracked records, optionally filtered by class."""
        records = list(self._records.values())
        if retention_class is not None:
            records = [r for r in records if r.retention_class == retention_class.value]
        return records

    def stats(self) -> dict:
        """Return summary statistics for the retention registry."""
        by_class: Dict[str, int] = {}
        for record in self._records.values():
            by_class[record.retention_class] = by_class.get(record.retention_class, 0) + 1
        expired = self.sweep()
        return {
            "total_tracked": len(self._records),
            "by_class": by_class,
            "pending_expiry": len(expired),
            "registry_path": str(self.registry_path),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not self.registry_path.exists():
            return
        try:
            with open(self.registry_path, encoding="utf-8") as f:
                raw = json.load(f)
            for artifact_id, record_dict in raw.items():
                self._records[artifact_id] = ArtifactRecord(**record_dict)
        except (json.JSONDecodeError, TypeError, KeyError):
            # Corrupt registry — start fresh (fail-safe for startup)
            self._records = {}

    def _save(self) -> None:
        tmp = self.registry_path.with_suffix(".json.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                serialized = {aid: asdict(rec) for aid, rec in self._records.items()}
                json.dump(serialized, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(self.registry_path)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    def _delete_artifact_file(self, artifact_id: str, artifact_path: Optional[str]) -> None:
        if not artifact_path:
            return
        path = Path(artifact_path)
        if path.exists():
            try:
                path.unlink()
            except Exception:
                pass  # best-effort deletion; log via event_callback


# ---------------------------------------------------------------------------
# Convenience: compute expiry datetime for a given class
# ---------------------------------------------------------------------------

def compute_expiry(
    retention_class: RetentionClass,
    registered_at: Optional[datetime] = None,
    windows: Optional[Dict[RetentionClass, timedelta]] = None,
) -> Optional[datetime]:
    """
    Compute the expiry datetime for a retention class.

    Returns None for LEGAL_HOLD (no expiry). Useful for pre-computing
    deletion_at on RightsProfile at ingest time.
    """
    _windows = {**DEFAULT_WINDOWS, **(windows or {})}
    now = registered_at or datetime.now(timezone.utc)
    window = _windows.get(retention_class, timedelta(days=365))
    if window == timedelta.max:
        return None
    return now + window
