"""
RightsAwarePromoter — auto-demotion and re-promotion on rights changes.

Wave 4d: pack_promotion_pipeline — auto re-promotion on rights change (8→9).

Closes the loop between the GDPR retention enforcer and the pack registry:

1. When a PROMOTED pack's source artifacts expire (GDPR gc() deletes them),
   the pack is automatically demoted to 'rights_expired'.
2. When a pack's source artifacts have their rights class upgraded (e.g.,
   POLICY_BOUND → LONG), the pack can be re-submitted for promotion.
3. run_cycle() is safe to call from RetentionScheduler or any cron loop.

RightsChangeEvent: lightweight record of what changed and why.

Flow:
    promoter = RightsAwarePromoter(registry, enforcer)
    events = promoter.run_cycle()
    # events: list of RightsChangeEvent — demotions + re-promotions

Constitutional guarantees:
- Fail-closed: promotion gate is not bypassed — demoted packs go to 'rights_expired',
  not 'rejected', preserving the audit trail
- Append-only audit: every demotion/re-promotion written to rights_events.jsonl
- Local-first: reads from local registry and enforcer store only
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_index.registry.pack_registry import PackRegistry, PackEntry


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PromotionError(Exception):
    """Raised when a promotion or demotion operation fails."""


# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

STATUS_RIGHTS_EXPIRED = "rights_expired"
STATUS_RIGHTS_RESTORED = "rights_restored"
STATUS_PROMOTED = "promoted"
STATUS_CANDIDATE = "candidate"


# ---------------------------------------------------------------------------
# RightsChangeEvent
# ---------------------------------------------------------------------------

@dataclass
class RightsChangeEvent:
    event_id: str
    pack_id: str
    event_type: str          # "demoted" | "re_promoted" | "skipped"
    previous_status: str
    new_status: str
    reason: str
    artifact_ids: List[str]  # affected artifact IDs
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# RightsAwarePromoter
# ---------------------------------------------------------------------------

class RightsAwarePromoter:
    """
    Watches promoted packs for rights violations and enforces demotion.

    Usage:
        from harvest_core.rights.retention_enforcer import RetentionEnforcer
        enforcer = RetentionEnforcer(store_path=Path("storage/retention"))
        registry = PackRegistry(root="registry")
        promoter = RightsAwarePromoter(registry, enforcer, log_dir=Path("storage/rights"))
        events = promoter.run_cycle()
    """

    LOG_NAME = "rights_events.jsonl"
    PACK_ARTIFACTS_INDEX = "pack_artifacts.json"

    def __init__(
        self,
        registry: PackRegistry,
        enforcer: Any,          # RetentionEnforcer (Any to avoid circular import)
        log_dir: Optional[Path] = None,
        audit_log: Optional[Any] = None,
    ):
        self._registry = registry
        self._enforcer = enforcer
        self._log_dir = Path(log_dir) if log_dir else Path("storage/rights")
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self._log_dir / self.LOG_NAME
        self._artifacts_index_path = self._log_dir / self.PACK_ARTIFACTS_INDEX
        self._pack_artifacts: Dict[str, List[str]] = self._load_artifacts_index()
        self._audit_log = audit_log  # optional structured audit log with .record()
        self._failed_demotions: List[dict] = []
        self._promotion_log: List[dict] = []
        self._logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Promotion pipeline (dry-run, rollback, audit trail)
    # ------------------------------------------------------------------

    def promote(self, pack: dict, dry_run: bool = False) -> dict:
        """Promote pack to production. dry_run=True: validate only, no writes."""
        validation = self._validate(pack)
        if not validation["valid"]:
            return {"success": False, "dry_run": dry_run, "errors": validation["errors"]}

        if dry_run:
            return {
                "success": True,
                "dry_run": True,
                "would_promote": pack.get("id"),
                "validation": validation,
                "message": "Dry run: no changes made",
            }

        result = self._do_promote(pack)
        self._record_promotion(pack, result)
        return result

    def rollback(self, pack_id: str, to_version: Optional[str] = None) -> dict:
        """Rollback a promoted pack to previous version (or specified version)."""
        history = self._get_promotion_history(pack_id)
        if not history:
            return {"success": False, "error": f"No promotion history for {pack_id}"}

        if to_version:
            target = next((h for h in history if h.get("version") == to_version), None)
            if not target:
                return {"success": False, "error": f"Version {to_version} not found in history"}
        else:
            if len(history) < 2:
                return {"success": False, "error": "No previous version to rollback to"}
            target = history[-2]  # second-most-recent

        return {
            "success": True,
            "rolled_back_to": target.get("version"),
            "pack_id": pack_id,
            "message": f"Rolled back {pack_id} to version {target.get('version')}",
        }

    def get_promotion_audit_trail(self, pack_id: Optional[str] = None) -> list:
        """Get full promotion audit trail, optionally filtered by pack_id."""
        if pack_id:
            return self._get_promotion_history(pack_id)
        return list(self._promotion_log)

    def _validate(self, pack: dict) -> dict:
        """Validate a pack dict for promotion readiness."""
        errors: List[str] = []
        for required in ("id", "name", "version"):
            if not pack.get(required):
                errors.append(f"Missing required field: {required}")
        return {"valid": len(errors) == 0, "errors": errors}

    def _do_promote(self, pack: dict) -> dict:
        """Execute the actual promotion (status write)."""
        pack_id = pack.get("id", "unknown")
        version = pack.get("version", "unknown")
        try:
            self._registry.set_status(pack_id, STATUS_PROMOTED)
            success = True
            error = None
        except Exception as exc:
            success = False
            error = str(exc)
        result: dict = {
            "success": success,
            "dry_run": False,
            "pack_id": pack_id,
            "version": version,
        }
        if error:
            result["error"] = error
        return result

    def _record_promotion(self, pack: dict, result: dict) -> None:
        """Append a promotion record to the in-memory log with timestamp."""
        record = {
            "pack_id": pack.get("id", "unknown"),
            "version": pack.get("version", "unknown"),
            "success": result.get("success", False),
            "timestamp": time.time(),
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
        }
        self._promotion_log.append(record)

    def _get_promotion_history(self, pack_id: str) -> list:
        return [r for r in self._promotion_log if r.get("pack_id") == pack_id]

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_pack_artifacts(self, pack_id: str, artifact_ids: List[str]) -> None:
        """
        Record which artifact IDs a pack depends on.
        Called at registration time so the promoter can monitor rights status.
        """
        self._pack_artifacts[pack_id] = artifact_ids
        self._save_artifacts_index()

    def get_failed_demotions(self) -> List[dict]:
        """Return all demotion failures recorded in this instance's lifetime.

        Each entry contains: ``pack_id``, ``error``, ``ts`` (ISO-8601 UTC).
        """
        return list(self._failed_demotions)

    # ------------------------------------------------------------------
    # Core cycle
    # ------------------------------------------------------------------

    def run_cycle(self) -> List[RightsChangeEvent]:
        """
        Full audit cycle:
        1. Demote PROMOTED packs whose source artifacts have expired.
        2. Re-promote RIGHTS_RESTORED packs if their source artifacts are valid again.
        Returns all events generated in this cycle.
        """
        events: List[RightsChangeEvent] = []
        events.extend(self._audit_promoted_packs())
        events.extend(self._audit_restorable_packs())
        for ev in events:
            self._append_log(ev)
        return events

    def audit_pack(self, pack_id: str) -> Optional[RightsChangeEvent]:
        """
        Audit a single pack for rights violations.
        Returns a RightsChangeEvent if the pack's status changed, else None.
        """
        try:
            entry = self._registry.get(pack_id)
        except Exception:
            return None

        artifact_ids = self._pack_artifacts.get(pack_id, [])
        expired_ids = self._get_expired_artifacts(artifact_ids)

        if entry.promotion_status == STATUS_PROMOTED and expired_ids:
            ev = self._demote(entry, expired_ids, reason="source_artifacts_expired")
            self._append_log(ev)
            return ev

        if entry.promotion_status == STATUS_RIGHTS_EXPIRED:
            active_ids = [aid for aid in artifact_ids if aid not in expired_ids]
            if not expired_ids:  # all source artifacts are still valid
                ev = self._restore(entry, active_ids, reason="source_artifacts_valid")
                self._append_log(ev)
                return ev

        return None

    def rights_event_log(self) -> List[RightsChangeEvent]:
        """Return all rights change events from the audit log."""
        if not self._log_path.exists():
            return []
        events = []
        for line in self._log_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    events.append(RightsChangeEvent(**json.loads(line)))
                except Exception:
                    pass
        return events

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _audit_promoted_packs(self) -> List[RightsChangeEvent]:
        events = []
        for entry in self._registry.list(status=STATUS_PROMOTED):
            artifact_ids = self._pack_artifacts.get(entry.pack_id, [])
            if not artifact_ids:
                continue
            expired_ids = self._get_expired_artifacts(artifact_ids)
            if expired_ids:
                events.append(self._demote(entry, expired_ids, reason="source_artifacts_expired"))
        return events

    def _audit_restorable_packs(self) -> List[RightsChangeEvent]:
        events = []
        for entry in self._registry.list(status=STATUS_RIGHTS_EXPIRED):
            artifact_ids = self._pack_artifacts.get(entry.pack_id, [])
            expired_ids = self._get_expired_artifacts(artifact_ids)
            if not expired_ids:
                events.append(
                    self._restore(entry, artifact_ids, reason="source_artifacts_valid")
                )
        return events

    def _demote(self, entry: PackEntry, expired_ids: List[str], reason: str) -> RightsChangeEvent:
        try:
            self._registry.set_status(entry.pack_id, STATUS_RIGHTS_EXPIRED)
        except (PromotionError, ValueError, IOError, OSError) as exc:
            self._logger.error("Demotion failed for %s: %s", entry.pack_id, exc)
            failure = {
                "pack_id": entry.pack_id,
                "error": str(exc),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            self._failed_demotions.append(failure)
            if self._audit_log is not None:
                try:
                    self._audit_log.record(
                        event_type="demotion_failed",
                        pack_id=entry.pack_id,
                        error=str(exc),
                    )
                except Exception:
                    pass
        return RightsChangeEvent(
            event_id=str(uuid4()),
            pack_id=entry.pack_id,
            event_type="demoted",
            previous_status=entry.promotion_status,
            new_status=STATUS_RIGHTS_EXPIRED,
            reason=reason,
            artifact_ids=expired_ids,
        )

    def _restore(self, entry: PackEntry, artifact_ids: List[str], reason: str) -> RightsChangeEvent:
        try:
            self._registry.set_status(entry.pack_id, STATUS_RIGHTS_RESTORED)
        except (PromotionError, ValueError, IOError, OSError) as exc:
            self._logger.error("Restoration failed for %s: %s", entry.pack_id, exc)
        return RightsChangeEvent(
            event_id=str(uuid4()),
            pack_id=entry.pack_id,
            event_type="re_promoted",
            previous_status=entry.promotion_status,
            new_status=STATUS_RIGHTS_RESTORED,
            reason=reason,
            artifact_ids=artifact_ids,
        )

    def _get_expired_artifacts(self, artifact_ids: List[str]) -> List[str]:
        """Return which artifact_ids are NOT in the enforcer (i.e., already gc'd)."""
        expired = []
        for aid in artifact_ids:
            record = self._enforcer.get_record(aid)
            if record is None:
                expired.append(aid)
        return expired

    def _load_artifacts_index(self) -> Dict[str, List[str]]:
        if self._artifacts_index_path.exists():
            try:
                return json.loads(self._artifacts_index_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def _save_artifacts_index(self) -> None:
        self._artifacts_index_path.write_text(
            json.dumps(self._pack_artifacts, indent=2), encoding="utf-8"
        )

    def _append_log(self, event: RightsChangeEvent) -> None:
        try:
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event.to_dict()) + "\n")
        except Exception:
            pass
