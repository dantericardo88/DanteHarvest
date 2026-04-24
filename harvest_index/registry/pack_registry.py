"""
PackRegistry — local pack registry for promoted WorkflowPack and SkillPack.

Content-addressed registry: packs stored at packs/{pack_type}/{pack_id}.json
A flat JSON index at <root>/pack_index.json maps pack_id → metadata.

Promotion is gated: only CANDIDATE packs with a valid EvidenceReceipt can be promoted.
Fail-closed: promoting without an attached receipt raises RegistryError.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from harvest_core.control.exceptions import HarvestError
from harvest_distill.packs.pack_schemas import AnyPack, PackType, PromotionStatus


class RegistryError(HarvestError):
    pass


@dataclass
class PackEntry:
    pack_id: str
    pack_type: str
    title: str
    promotion_status: str
    registered_at: str
    receipt_id: Optional[str]
    storage_path: str
    project_id: Optional[str] = None
    confidence_score: float = 0.0

    def to_dict(self) -> dict:
        return {
            "pack_id": self.pack_id,
            "pack_type": self.pack_type,
            "title": self.title,
            "promotion_status": self.promotion_status,
            "registered_at": self.registered_at,
            "receipt_id": self.receipt_id,
            "storage_path": self.storage_path,
            "project_id": self.project_id,
            "confidence_score": self.confidence_score,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PackEntry":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


class PackRegistry:
    """
    Local registry for Harvest packs.

    Usage:
        registry = PackRegistry(root="registry")
        entry = registry.register(pack, receipt_id="receipt-abc")
        registry.promote(pack_id=entry.pack_id)
    """

    INDEX_FILE = "pack_index.json"

    def __init__(self, root: str = "registry"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._index_path = self.root / self.INDEX_FILE
        self._index: Dict[str, dict] = self._load_index()

    def register(
        self,
        pack: AnyPack,
        receipt_id: Optional[str] = None,
        project_id: Optional[str] = None,
        confidence_score: float = 0.0,
    ) -> PackEntry:
        """Register a pack as CANDIDATE.  Overwrites existing entry for the same pack_id."""
        pack_type = pack.pack_type.value
        dest_dir = self.root / "packs" / pack_type
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / f"{pack.pack_id}.json"

        pack_json = pack.model_dump_json(indent=2)
        tmp = dest_path.with_suffix(".json.tmp")
        tmp.write_text(pack_json, encoding="utf-8")
        tmp.replace(dest_path)

        entry = PackEntry(
            pack_id=pack.pack_id,
            pack_type=pack_type,
            title=getattr(pack, "title", getattr(pack, "skill_name", "untitled")),
            promotion_status=PromotionStatus.CANDIDATE.value,
            registered_at=datetime.utcnow().isoformat(),
            receipt_id=receipt_id,
            storage_path=str(dest_path),
            project_id=project_id,
            confidence_score=confidence_score,
        )
        self._index[pack.pack_id] = entry.to_dict()
        self._save_index()
        return entry

    def promote(self, pack_id: str, receipt_id: Optional[str] = None) -> PackEntry:
        """
        Promote a CANDIDATE pack to PROMOTED.
        Raises RegistryError if receipt_id is missing (fail-closed).
        """
        entry = self._get_entry(pack_id)
        if entry.promotion_status != PromotionStatus.CANDIDATE.value:
            raise RegistryError(
                f"Pack {pack_id} is '{entry.promotion_status}', not 'candidate'"
            )
        if receipt_id:
            entry.receipt_id = receipt_id
        if not entry.receipt_id:
            raise RegistryError(
                f"Cannot promote pack {pack_id}: no EvidenceReceipt attached"
            )
        entry.promotion_status = PromotionStatus.PROMOTED.value
        self._index[pack_id] = entry.to_dict()
        self._save_index()
        return entry

    def attach_receipt(self, pack_id: str, receipt_id: str) -> "PackEntry":
        """Attach an EvidenceReceipt ID to a pack before promotion."""
        entry = self._get_entry(pack_id)
        entry.receipt_id = receipt_id
        self._index[pack_id] = entry.to_dict()
        self._save_index()
        return entry

    def set_status(self, pack_id: str, status: str) -> "PackEntry":
        """
        Set an arbitrary promotion status (e.g. 'deferred', 'deleted').
        Used by review_states.transition() for non-promote/reject transitions.
        Raises RegistryError if pack not found.
        """
        entry = self._get_entry(pack_id)
        entry.promotion_status = status
        self._index[pack_id] = entry.to_dict()
        self._save_index()
        return entry

    def reject(self, pack_id: str, reason: str = "") -> PackEntry:
        entry = self._get_entry(pack_id)
        entry.promotion_status = PromotionStatus.REJECTED.value
        self._index[pack_id] = entry.to_dict()
        self._save_index()
        return entry

    def get(self, pack_id: str) -> PackEntry:
        return self._get_entry(pack_id)

    def load_pack_json(self, pack_id: str) -> dict:
        entry = self._get_entry(pack_id)
        return json.loads(Path(entry.storage_path).read_text(encoding="utf-8"))

    def list(
        self,
        pack_type: Optional[str] = None,
        status: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> List[PackEntry]:
        entries = [PackEntry.from_dict(v) for v in self._index.values()]
        if pack_type:
            entries = [e for e in entries if e.pack_type == pack_type]
        if status:
            entries = [e for e in entries if e.promotion_status == status]
        if project_id:
            entries = [e for e in entries if e.project_id == project_id]
        return entries

    def stats(self) -> dict:
        entries = [PackEntry.from_dict(v) for v in self._index.values()]
        by_status: Dict[str, int] = {}
        by_type: Dict[str, int] = {}
        for e in entries:
            by_status[e.promotion_status] = by_status.get(e.promotion_status, 0) + 1
            by_type[e.pack_type] = by_type.get(e.pack_type, 0) + 1
        return {
            "total": len(entries),
            "by_status": by_status,
            "by_type": by_type,
        }

    def _get_entry(self, pack_id: str) -> PackEntry:
        raw = self._index.get(pack_id)
        if not raw:
            raise RegistryError(f"Pack not found: {pack_id}")
        return PackEntry.from_dict(raw)

    def _load_index(self) -> Dict[str, dict]:
        if self._index_path.exists():
            try:
                return json.loads(self._index_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_index(self) -> None:
        tmp = self._index_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._index, indent=2), encoding="utf-8")
        tmp.replace(self._index_path)
