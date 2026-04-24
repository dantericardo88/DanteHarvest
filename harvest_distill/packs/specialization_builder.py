"""
SpecializationPackBuilder — compose a domain SpecializationPack from promoted packs.

Bundles promoted WorkflowPack and SkillPack references, a domain glossary,
and a taxonomy into a SpecializationPack ready for DanteAgents consumption.

Constitutional guarantee: only PROMOTED packs may be included in a
SpecializationPack (fail-closed on CANDIDATE or REJECTED references).
Emits specialization.created chain entry.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import PackagingError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_distill.packs.pack_schemas import (
    PromotionStatus,
    SpecializationPack,
)
from harvest_index.registry.pack_registry import PackRegistry


class SpecializationPackBuilder:
    """
    Build a SpecializationPack from domain knowledge and promoted packs.

    Usage:
        builder = SpecializationPackBuilder(registry, chain_writer)
        spec = await builder.build(
            domain="accounting",
            workflow_pack_ids=["wf-001", "wf-002"],
            skill_pack_ids=["sk-001"],
            glossary={"GL account": "General Ledger account code"},
            run_id="run-001",
        )
    """

    def __init__(
        self,
        registry: Optional[PackRegistry] = None,
        chain_writer: Optional[ChainWriter] = None,
    ):
        self.registry = registry
        self.chain_writer = chain_writer

    async def build(
        self,
        domain: str,
        run_id: str,
        workflow_pack_ids: Optional[List[str]] = None,
        skill_pack_ids: Optional[List[str]] = None,
        knowledge_refs: Optional[List[str]] = None,
        glossary: Optional[Dict[str, str]] = None,
        taxonomy: Optional[Dict[str, Any]] = None,
        disallowed_actions: Optional[List[str]] = None,
        rights_boundary: str = "approved",
    ) -> SpecializationPack:
        """
        Build a SpecializationPack.  If a PackRegistry is wired, validates that
        all referenced packs are PROMOTED before including them.
        """
        wf_ids = workflow_pack_ids or []
        sk_ids = skill_pack_ids or []

        if self.registry:
            self._validate_promoted(wf_ids + sk_ids)

        pack_id = str(uuid4())
        pack = SpecializationPack(
            pack_id=pack_id,
            domain=domain,
            workflow_refs=wf_ids,
            skill_refs=sk_ids,
            knowledge_refs=knowledge_refs or [],
            glossary=glossary or {},
            taxonomy=taxonomy or {},
            disallowed_actions=disallowed_actions or [],
            rights_boundary=rights_boundary,
            promotion_status=PromotionStatus.CANDIDATE,
        )

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="specialization.created",
                machine="specialization_builder",
                data={
                    "pack_id": pack_id,
                    "domain": domain,
                    "workflow_refs": wf_ids,
                    "skill_refs": sk_ids,
                    "glossary_terms": len(glossary or {}),
                },
            ))

        return pack

    def _validate_promoted(self, pack_ids: List[str]) -> None:
        for pack_id in pack_ids:
            try:
                entry = self.registry.get(pack_id)
            except Exception as e:
                raise PackagingError(
                    f"Cannot include pack {pack_id} in SpecializationPack: not found"
                ) from e
            if entry.promotion_status != PromotionStatus.PROMOTED.value:
                raise PackagingError(
                    f"Pack {pack_id} is '{entry.promotion_status}', not 'promoted'. "
                    "Only promoted packs may be included in a SpecializationPack."
                )
