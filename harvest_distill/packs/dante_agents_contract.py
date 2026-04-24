"""
DanteAgents integration contract for DANTEHARVEST.

Defines the export format and handoff protocol that DanteAgents and DanteCode
consume from Harvest.  Each exported pack is wrapped in a HarvestHandoff that
contains the pack, its receipt reference, and consumption metadata.

This module is the single exit point for promoted packs — all downstream
consumers import from here.  Fail-closed: only PROMOTED packs can be exported.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import PackagingError
from harvest_distill.packs.pack_schemas import (
    AnyPack,
    EvalPack,
    PackType,
    PromotionStatus,
    SkillPack,
    SpecializationPack,
    WorkflowPack,
)
from harvest_index.registry.pack_registry import PackRegistry


@dataclass
class HarvestHandoff:
    """
    Export container passed from DANTEHARVEST to DanteAgents/DanteCode.

    Carries the pack JSON, receipt ID, confidence score, and consumption hints.
    """
    handoff_id: str
    pack_id: str
    pack_type: str
    domain: str
    receipt_id: Optional[str]
    confidence_score: float
    exported_at: str
    pack_json: Dict[str, Any]
    consumption_hints: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "handoff_id": self.handoff_id,
            "pack_id": self.pack_id,
            "pack_type": self.pack_type,
            "domain": self.domain,
            "receipt_id": self.receipt_id,
            "confidence_score": self.confidence_score,
            "exported_at": self.exported_at,
            "consumption_hints": self.consumption_hints,
            "pack": self.pack_json,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def write(self, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.to_json(), encoding="utf-8")


class DanteAgentsExporter:
    """
    Export promoted packs from DANTEHARVEST for DanteAgents/DanteCode consumption.

    Only PROMOTED packs can be exported (fail-closed).

    Usage:
        exporter = DanteAgentsExporter(registry)
        handoff = exporter.export(pack_id="wf-001", domain="accounting")
        handoff.write(Path("exports/wf-001.json"))
    """

    def __init__(self, registry: PackRegistry):
        self.registry = registry

    def export(
        self,
        pack_id: str,
        domain: str = "general",
        consumption_hints: Optional[Dict[str, Any]] = None,
    ) -> HarvestHandoff:
        """
        Export a single promoted pack as a HarvestHandoff.
        Raises PackagingError if the pack is not PROMOTED.
        """
        entry = self.registry.get(pack_id)
        if entry.promotion_status != PromotionStatus.PROMOTED.value:
            raise PackagingError(
                f"Pack {pack_id} cannot be exported: status is "
                f"'{entry.promotion_status}', expected 'promoted'"
            )

        pack_json = self.registry.load_pack_json(pack_id)

        return HarvestHandoff(
            handoff_id=str(uuid4()),
            pack_id=pack_id,
            pack_type=entry.pack_type,
            domain=domain,
            receipt_id=entry.receipt_id,
            confidence_score=entry.confidence_score,
            exported_at=datetime.utcnow().isoformat(),
            pack_json=pack_json,
            consumption_hints=consumption_hints or self._default_hints(entry.pack_type),
        )

    def export_all(
        self,
        domain: str = "general",
        pack_type: Optional[str] = None,
    ) -> List[HarvestHandoff]:
        """Export all promoted packs, optionally filtered by type."""
        entries = self.registry.list(status=PromotionStatus.PROMOTED.value, pack_type=pack_type)
        return [
            self.export(e.pack_id, domain=domain)
            for e in entries
        ]

    def write_export_bundle(
        self,
        output_dir: Path,
        domain: str = "general",
        pack_type: Optional[str] = None,
    ) -> List[Path]:
        """Export all promoted packs to a directory, one file per pack."""
        handoffs = self.export_all(domain=domain, pack_type=pack_type)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for h in handoffs:
            dest = output_dir / f"{h.pack_type}_{h.pack_id}.json"
            h.write(dest)
            paths.append(dest)
        return paths

    def _default_hints(self, pack_type: str) -> Dict[str, Any]:
        hints = {
            PackType.WORKFLOW.value: {
                "agent_role": "executor",
                "replay_required": True,
                "human_in_loop": False,
            },
            PackType.SKILL.value: {
                "agent_role": "tool_user",
                "replay_required": False,
                "human_in_loop": False,
            },
            PackType.SPECIALIZATION.value: {
                "agent_role": "domain_specialist",
                "replay_required": False,
                "human_in_loop": True,
            },
            PackType.EVAL.value: {
                "agent_role": "evaluator",
                "replay_required": True,
                "human_in_loop": True,
            },
        }
        return hints.get(pack_type, {})
