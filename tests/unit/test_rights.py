"""Unit tests for RightsProfile and EvidenceReceipt."""

import pytest

from harvest_core.rights.rights_model import (
    ReviewStatus,
    RightsProfile,
    SourceClass,
    TrainingEligibility,
    default_rights_for,
)
from harvest_core.rights.evidence_receipt import (
    ArtifactRef,
    EvidenceReceipt,
    PolicyDecision,
)


class TestRightsProfile:
    def test_owned_internal_defaults_allow_training(self):
        profile = default_rights_for(SourceClass.OWNED_INTERNAL)
        assert profile.training_eligibility == TrainingEligibility.ALLOWED

    def test_customer_confidential_forbids_training(self):
        profile = default_rights_for(SourceClass.CUSTOMER_CONFIDENTIAL)
        assert profile.training_eligibility == TrainingEligibility.FORBIDDEN

    def test_personal_device_forbids_training(self):
        profile = default_rights_for(SourceClass.PERSONAL_DEVICE_MEMORY)
        assert profile.training_eligibility == TrainingEligibility.FORBIDDEN

    def test_promotion_eligibility_requires_approved(self):
        profile = default_rights_for(SourceClass.OWNED_INTERNAL)
        # Default is OWNER_ASSERTED — not yet eligible
        assert not profile.is_promotion_eligible()
        profile.review_status = ReviewStatus.OWNER_ASSERTED_AND_REVIEWED
        assert profile.is_promotion_eligible()

    def test_legal_hold_blocks_promotion(self):
        profile = default_rights_for(SourceClass.OWNED_INTERNAL)
        profile.review_status = ReviewStatus.OWNER_ASSERTED_AND_REVIEWED
        profile.legal_hold = True
        assert not profile.is_promotion_eligible()

    def test_requires_redaction_blocks_promotion(self):
        profile = default_rights_for(SourceClass.OWNED_INTERNAL)
        profile.review_status = ReviewStatus.OWNER_ASSERTED_AND_REVIEWED
        profile.requires_redaction = True
        assert not profile.is_promotion_eligible()

    def test_redaction_flags(self):
        profile = RightsProfile(
            source_class=SourceClass.PUBLIC_WEB,
            contains_secrets=True,
            contains_pii=True,
        )
        flags = profile.redaction_required_flags()
        assert "contains_secrets" in flags
        assert "contains_pii" in flags


class TestEvidenceReceipt:
    def _make_receipt(self, **kwargs) -> EvidenceReceipt:
        defaults = dict(
            receipt_id="receipt-001",
            artifact_refs=[
                ArtifactRef(
                    artifact_id="chain-001",
                    artifact_type="chain",
                    storage_uri="local://chain.jsonl",
                    sha256="abc123",
                )
            ],
            manifest_hash="manifest-hash-abc",
            policy_decisions=[
                PolicyDecision(gate_name="rights_status", passed=True),
                PolicyDecision(gate_name="provenance_completeness", passed=True),
            ],
            approvals=["reviewer@example.com"],
            training_eligibility=TrainingEligibility.ALLOWED,
            issuer="harvest-service",
        )
        defaults.update(kwargs)
        return EvidenceReceipt(**defaults)

    def test_receipt_self_seals_on_creation(self):
        receipt = self._make_receipt()
        assert receipt.receipt_hash is not None

    def test_receipt_verify_passes(self):
        receipt = self._make_receipt()
        assert receipt.verify() is True

    def test_all_gates_passed(self):
        receipt = self._make_receipt()
        assert receipt.all_gates_passed() is True

    def test_failed_gate_detected(self):
        receipt = self._make_receipt(
            policy_decisions=[
                PolicyDecision(gate_name="rights_status", passed=False),
            ]
        )
        assert not receipt.all_gates_passed()
