"""
ReceiptIssuer — builds EvidenceReceipt from chain + manifest.

Reads the chain for a completed run, evaluates all six promotion gates,
and issues a self-sealing EvidenceReceipt.  Fail-closed: any gate that
cannot be evaluated is recorded as failed.

Constitutional guarantees:
- Receipt issued ONLY after evaluating all required signals
- Every gate decision recorded in policy_decisions
- Receipt immutable after issuance (sealed via receipt_hash)
- Emits receipt.issued or receipt.denied chain entry
"""

from __future__ import annotations

from typing import List, Optional
from uuid import uuid4

from harvest_core.evaluation.gates import evaluate_promotion, PromotionResult
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.rights.evidence_receipt import (
    ArtifactRef,
    EvidenceReceipt,
    PolicyDecision,
)
from harvest_core.rights.rights_model import TrainingEligibility


class ReceiptIssuer:
    """
    Issue an EvidenceReceipt for a completed Harvest run.

    Usage:
        issuer = ReceiptIssuer(chain_writer, issuer_id="harvest-service")
        receipt = await issuer.issue(
            run_id="run-001",
            artifact_refs=[...],
            manifest_hash="abc123...",
        )
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        issuer_id: str = "receipt_issuer",
        require_human_approval: bool = False,
    ):
        self.chain_writer = chain_writer
        self.issuer_id = issuer_id
        self.require_human_approval = require_human_approval

    async def issue(
        self,
        run_id: str,
        artifact_refs: List[ArtifactRef],
        manifest_hash: str,
        approvals: Optional[List[str]] = None,
        training_eligibility: TrainingEligibility = TrainingEligibility.UNKNOWN,
        confidence_score: float = 0.0,
    ) -> EvidenceReceipt:
        """
        Build and seal an EvidenceReceipt from the current chain state.
        Emits receipt.issued on success, receipt.denied on failure.
        """
        entries = self.chain_writer.read_all()
        approval_list = approvals or []

        gate_kwargs = self._derive_gate_kwargs(entries, approval_list, confidence_score)
        promotion: PromotionResult = evaluate_promotion(**gate_kwargs)

        policy_decisions = [
            PolicyDecision(
                gate_name=g.gate_name,
                passed=g.passed,
                details=g.reason,
                decided_by=self.issuer_id,
            )
            for g in promotion.gates
        ]

        receipt = EvidenceReceipt.create(
            receipt_id=str(uuid4()),
            artifact_refs=artifact_refs,
            manifest_hash=manifest_hash,
            policy_decisions=policy_decisions,
            approvals=approval_list,
            training_eligibility=training_eligibility,
            issuer=self.issuer_id,
        )

        signal = "receipt.issued" if promotion.eligible else "receipt.denied"
        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal=signal,
            machine="receipt_issuer",
            data={
                "receipt_id": receipt.receipt_id,
                "receipt_hash": receipt.receipt_hash,
                "all_gates_passed": promotion.eligible,
                "gates_failed": promotion.failing_gates,
                "artifact_count": len(artifact_refs),
            },
        ))

        return receipt

    def _derive_gate_kwargs(
        self, entries: list, approvals: List[str], confidence_score: float
    ) -> dict:
        signal_set = {e.signal for e in entries}

        provenance_score = self._compute_provenance_score(signal_set)
        rights_status = self._extract_rights_status(entries)
        replay_pass_rate = self._extract_replay_pass_rate(entries)
        is_deterministic = any(
            s.startswith("normalize.") or s.startswith("distill.")
            for s in signal_set
        )
        redaction_complete = "redaction.completed" in signal_set or (
            "redaction.required" not in signal_set
        )
        has_human_signoff = len(approvals) > 0

        return {
            "provenance_score": provenance_score,
            "rights_status": rights_status,
            "replay_pass_rate": replay_pass_rate,
            "is_deterministic": is_deterministic,
            "redaction_complete": redaction_complete,
            "has_human_signoff": has_human_signoff,
            "confidence_score": confidence_score,
            "requires_human_signoff": self.require_human_approval,
        }

    def _compute_provenance_score(self, signal_set: set) -> float:
        required = {
            "run.created", "run.running", "run.completed",
            "acquire.started", "acquire.completed",
        }
        found = required & signal_set
        return len(found) / len(required)

    def _extract_rights_status(self, entries: list) -> str:
        for e in reversed(entries):
            if e.signal == "acquire.completed":
                status = e.data.get("rights_status", "")
                if status:
                    return status
        return "pending"

    def _extract_replay_pass_rate(self, entries: list) -> float:
        for e in reversed(entries):
            if e.signal == "eval.completed":
                return float(e.data.get("pass_rate", 0.0))
        return 0.0
