"""
Rights and retention model for DANTEHARVEST.

Every artifact ingested by Harvest must carry a RightsProfile. The profile
governs retention lifetime, training eligibility, redistribution, and
what redaction is required before promotion to a pack.

Source classes and enumerations are defined in the PRD
(Rights Privacy and Repo Strategy section).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class SourceClass(str, Enum):
    """Classification of where the artifact originated."""
    OWNED_INTERNAL = "owned_internal"
    CUSTOMER_CONFIDENTIAL = "customer_confidential"
    LICENSED_REFERENCE = "licensed_reference"
    PUBLIC_WEB = "public_web"
    PERSONAL_DEVICE_MEMORY = "personal_device_memory"
    SYNTHETIC_EVAL = "synthetic_eval"
    OSS_CODE_OR_DOCS = "oss_code_or_docs"


class TrainingEligibility(str, Enum):
    """Whether the artifact may be used for model training."""
    ALLOWED = "allowed"
    ALLOWED_AFTER_REDACTION = "allowed_after_redaction"
    REFERENCE_ONLY = "reference_only"
    RESTRICTED = "restricted"
    FORBIDDEN = "forbidden"
    UNKNOWN = "unknown"


class RedistributionEligibility(str, Enum):
    ALLOWED = "allowed"
    RESTRICTED = "restricted"
    FORBIDDEN = "forbidden"
    UNKNOWN = "unknown"


class ReviewStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_REDACTION = "needs_redaction"
    OWNER_ASSERTED = "owner_asserted"
    OWNER_ASSERTED_AND_REVIEWED = "owner_asserted_and_reviewed"


class RetentionClass(str, Enum):
    SHORT = "short"          # Rolling window, default for personal/device memory
    MEDIUM = "medium"        # Public web, licensed reference
    LONG = "long"            # Owned internal, synthetic eval
    POLICY_BOUND = "policy_bound"  # Enterprise: retention set by customer policy
    LEGAL_HOLD = "legal_hold"


# ---------------------------------------------------------------------------
# Default rights profiles per source class
# ---------------------------------------------------------------------------

_SOURCE_DEFAULTS: dict[SourceClass, dict] = {
    SourceClass.OWNED_INTERNAL: {
        "training_eligibility": TrainingEligibility.ALLOWED,
        "redistribution_eligibility": RedistributionEligibility.RESTRICTED,
        "retention_class": RetentionClass.LONG,
        "review_status": ReviewStatus.OWNER_ASSERTED,
    },
    SourceClass.CUSTOMER_CONFIDENTIAL: {
        "training_eligibility": TrainingEligibility.FORBIDDEN,
        "redistribution_eligibility": RedistributionEligibility.FORBIDDEN,
        "retention_class": RetentionClass.POLICY_BOUND,
        "review_status": ReviewStatus.PENDING,
    },
    SourceClass.LICENSED_REFERENCE: {
        "training_eligibility": TrainingEligibility.UNKNOWN,
        "redistribution_eligibility": RedistributionEligibility.RESTRICTED,
        "retention_class": RetentionClass.LONG,
        "review_status": ReviewStatus.PENDING,
    },
    SourceClass.PUBLIC_WEB: {
        "training_eligibility": TrainingEligibility.REFERENCE_ONLY,
        "redistribution_eligibility": RedistributionEligibility.RESTRICTED,
        "retention_class": RetentionClass.MEDIUM,
        "review_status": ReviewStatus.PENDING,
    },
    SourceClass.PERSONAL_DEVICE_MEMORY: {
        "training_eligibility": TrainingEligibility.FORBIDDEN,
        "redistribution_eligibility": RedistributionEligibility.FORBIDDEN,
        "retention_class": RetentionClass.SHORT,
        "review_status": ReviewStatus.PENDING,
    },
    SourceClass.SYNTHETIC_EVAL: {
        "training_eligibility": TrainingEligibility.ALLOWED,
        "redistribution_eligibility": RedistributionEligibility.ALLOWED,
        "retention_class": RetentionClass.LONG,
        "review_status": ReviewStatus.APPROVED,
    },
    SourceClass.OSS_CODE_OR_DOCS: {
        "training_eligibility": TrainingEligibility.UNKNOWN,
        "redistribution_eligibility": RedistributionEligibility.UNKNOWN,
        "retention_class": RetentionClass.LONG,
        "review_status": ReviewStatus.PENDING,
    },
}


def default_rights_for(source_class: SourceClass) -> "RightsProfile":
    """Return a RightsProfile with safe defaults for the given source class."""
    defaults = _SOURCE_DEFAULTS.get(source_class, {})
    return RightsProfile(source_class=source_class, **defaults)


# ---------------------------------------------------------------------------
# RightsProfile — attached to every ingested artifact
# ---------------------------------------------------------------------------

class RightsProfile(BaseModel):
    """
    Rights and retention flags for a single artifact.

    Attach this to every raw artifact at ingest time. The profile follows
    the artifact through normalization, distillation, and pack promotion.
    No pack may be promoted without an approved RightsProfile.
    """

    source_class: SourceClass

    license_type: Optional[str] = Field(
        default=None,
        description="SPDX identifier or free-form license name (e.g. 'MIT', 'CC-BY-4.0')",
    )
    license_evidence_uri: Optional[str] = Field(
        default=None,
        description="URI pointing to the license file, page, or recorded approval",
    )
    ownership_asserted_by: Optional[str] = Field(
        default=None,
        description="Identity of person/service asserting ownership",
    )

    training_eligibility: TrainingEligibility = TrainingEligibility.UNKNOWN
    redistribution_eligibility: RedistributionEligibility = RedistributionEligibility.UNKNOWN

    contains_secrets: bool = False
    contains_pii: bool = False
    contains_credentials: bool = False
    requires_redaction: bool = False

    review_status: ReviewStatus = ReviewStatus.PENDING
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None

    retention_class: RetentionClass = RetentionClass.MEDIUM
    deletion_at: Optional[datetime] = None
    legal_hold: bool = False

    policy_version: str = "1.0"

    def is_promotion_eligible(self) -> bool:
        """
        Returns True only when the rights profile allows pack promotion.

        Conditions (all must hold):
        - review_status is APPROVED or OWNER_ASSERTED_AND_REVIEWED
        - training_eligibility is not FORBIDDEN
        - no unresolved redaction requirement
        - no legal hold blocking promotion
        """
        if self.legal_hold:
            return False
        if self.requires_redaction:
            return False
        if self.review_status not in (
            ReviewStatus.APPROVED,
            ReviewStatus.OWNER_ASSERTED_AND_REVIEWED,
        ):
            return False
        if self.training_eligibility == TrainingEligibility.FORBIDDEN:
            return False
        return True

    def redaction_required_flags(self) -> list[str]:
        """Return list of flags that require redaction attention."""
        flags = []
        if self.contains_secrets:
            flags.append("contains_secrets")
        if self.contains_pii:
            flags.append("contains_pii")
        if self.contains_credentials:
            flags.append("contains_credentials")
        if self.requires_redaction:
            flags.append("requires_redaction")
        return flags
