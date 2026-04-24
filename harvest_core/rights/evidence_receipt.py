"""
EvidenceReceipt — immutable promotion artifact.

Every pack promoted to the Harvest registry must carry an EvidenceReceipt.
The receipt is the bridge between raw acquisition and downstream consumers
(DanteAgents, DanteCode, sovereign training runtimes).

Schema defined in PRD §Computer Apprenticeship Design — Canonical artifact layers.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from harvest_core.rights.rights_model import ReviewStatus, TrainingEligibility


class ArtifactRef(BaseModel):
    """Lightweight pointer to a Harvest artifact."""
    artifact_id: str
    artifact_type: str
    storage_uri: str
    sha256: str


class PolicyDecision(BaseModel):
    """Record of a single policy gate check."""
    gate_name: str
    passed: bool
    details: Optional[str] = None
    decided_at: datetime = Field(default_factory=datetime.utcnow)
    decided_by: Optional[str] = None  # service ID or reviewer identity


class ReplayResult(BaseModel):
    """Summary of replay/eval pass for a candidate pack."""
    pack_id: str
    pass_rate: float = Field(ge=0.0, le=1.0)
    sample_size: int = Field(gt=0)
    eval_environment: Optional[str] = None
    run_at: datetime = Field(default_factory=datetime.utcnow)


class EvidenceReceipt(BaseModel):
    """
    Immutable promotion receipt — created once, never modified.

    Fields:
        receipt_id: UUID of this receipt.
        artifact_refs: All source artifacts backing the promoted pack.
        manifest_hash: SHA-256 of the export manifest at promotion time.
        policy_decisions: Record of every gate check that was evaluated.
        approvals: Identity list of human or service approvers.
        training_eligibility: Final training eligibility verdict.
        replay_result: Replay/eval summary if available.
        retention_clock: When this receipt (and its artifacts) expire.
        issuer: Service or user that created the receipt.
        issued_at: When the receipt was created.
        receipt_hash: SHA-256 of this receipt's canonical JSON (self-sealing).
    """

    receipt_id: str
    artifact_refs: List[ArtifactRef] = Field(default_factory=list)
    manifest_hash: str
    policy_decisions: List[PolicyDecision] = Field(default_factory=list)
    approvals: List[str] = Field(default_factory=list)
    training_eligibility: TrainingEligibility = TrainingEligibility.UNKNOWN
    replay_result: Optional[ReplayResult] = None
    retention_clock: Optional[datetime] = None
    issuer: str
    issued_at: datetime = Field(default_factory=datetime.utcnow)
    receipt_hash: Optional[str] = None

    @model_validator(mode="after")
    def seal_receipt(self) -> "EvidenceReceipt":
        if self.receipt_hash is None:
            self.receipt_hash = self._compute_hash()
        return self

    def _compute_hash(self) -> str:
        data = self.model_dump(exclude={"receipt_hash"}, mode="json")
        canonical = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def verify(self) -> bool:
        """Return True if the receipt has not been tampered with."""
        return self.receipt_hash == self._compute_hash()

    def all_gates_passed(self) -> bool:
        return all(d.passed for d in self.policy_decisions)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def create(
        cls,
        receipt_id: str,
        artifact_refs: List[ArtifactRef],
        manifest_hash: str,
        policy_decisions: List[PolicyDecision],
        approvals: List[str],
        training_eligibility: TrainingEligibility,
        issuer: str,
        replay_result: Optional[ReplayResult] = None,
        retention_clock: Optional[datetime] = None,
    ) -> "EvidenceReceipt":
        """Factory that seals the receipt on creation."""
        return cls(
            receipt_id=receipt_id,
            artifact_refs=artifact_refs,
            manifest_hash=manifest_hash,
            policy_decisions=policy_decisions,
            approvals=approvals,
            training_eligibility=training_eligibility,
            issuer=issuer,
            replay_result=replay_result,
            retention_clock=retention_clock,
        )
