"""
Promotion gates and confidence bands for DANTEHARVEST.

Confidence bands and promotion requirements from PRD
§Computer Apprenticeship Design — Confidence and approval logic.

A candidate pack must pass ALL promotion gates before it can be added
to the pack registry. This module is the single enforcement point.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Confidence bands
# ---------------------------------------------------------------------------

class ConfidenceBand(str, Enum):
    """
    Four explicit bands govern what the system can do with an artifact.

    GREEN  ≥ 0.90 → replay + promotion candidate
    YELLOW 0.75–0.89 → draft pack, requires human review
    ORANGE 0.50–0.74 → evidence only, not reusable skill
    RED    < 0.50 → raw/diagnostic artifact only
    """
    GREEN = "green"
    YELLOW = "yellow"
    ORANGE = "orange"
    RED = "red"


def classify_confidence(score: float) -> ConfidenceBand:
    """Map a confidence score [0, 1] to a ConfidenceBand."""
    if score >= 0.90:
        return ConfidenceBand.GREEN
    if score >= 0.75:
        return ConfidenceBand.YELLOW
    if score >= 0.50:
        return ConfidenceBand.ORANGE
    return ConfidenceBand.RED


# ---------------------------------------------------------------------------
# Individual gate checks
# ---------------------------------------------------------------------------

@dataclass
class GateResult:
    gate_name: str
    passed: bool
    reason: Optional[str] = None
    detail: Optional[Dict[str, Any]] = None


def _gate(name: str, passed: bool, reason: Optional[str] = None, **detail) -> GateResult:
    return GateResult(gate_name=name, passed=passed, reason=reason, detail=detail or None)


def gate_provenance_completeness(provenance_score: float) -> GateResult:
    passed = provenance_score >= 1.0
    return _gate(
        "provenance_completeness",
        passed,
        reason=None if passed else f"Score {provenance_score:.2f} < 1.0",
        provenance_score=provenance_score,
    )


def gate_rights_status(rights_status: str) -> GateResult:
    allowed = {"approved", "owner_asserted_and_reviewed"}
    passed = rights_status.lower() in allowed
    return _gate(
        "rights_status",
        passed,
        reason=None if passed else f"Rights status '{rights_status}' not in {allowed}",
        rights_status=rights_status,
    )


def gate_replay_pass_rate(pass_rate: float, threshold: float = 0.85) -> GateResult:
    passed = pass_rate >= threshold
    return _gate(
        "replay_pass_rate",
        passed,
        reason=None if passed else f"Replay pass rate {pass_rate:.2%} < threshold {threshold:.2%}",
        pass_rate=pass_rate,
        threshold=threshold,
    )


def gate_deterministic_step_graph(is_deterministic: bool) -> GateResult:
    return _gate(
        "deterministic_step_graph",
        is_deterministic,
        reason=None if is_deterministic else "Step graph contains non-deterministic branches",
    )


def gate_redaction_complete(redaction_complete: bool) -> GateResult:
    return _gate(
        "redaction_complete",
        redaction_complete,
        reason=None if redaction_complete else "Redaction pass not completed",
    )


def gate_human_reviewer_signoff(
    has_signoff: bool,
    requires_signoff: bool = True,
) -> GateResult:
    passed = (not requires_signoff) or has_signoff
    return _gate(
        "human_reviewer_signoff",
        passed,
        reason=None if passed else "Human reviewer signoff required but not present",
        has_signoff=has_signoff,
        requires_signoff=requires_signoff,
    )


# ---------------------------------------------------------------------------
# Promotion gate runner
# ---------------------------------------------------------------------------

@dataclass
class PromotionResult:
    eligible: bool
    gates: List[GateResult] = field(default_factory=list)
    failing_gates: List[str] = field(default_factory=list)
    confidence_band: Optional[ConfidenceBand] = None

    def summary(self) -> str:
        if self.eligible:
            return "ELIGIBLE — all promotion gates passed."
        return (
            f"NOT ELIGIBLE — {len(self.failing_gates)} gate(s) failed: "
            + ", ".join(self.failing_gates)
        )


def evaluate_promotion(
    provenance_score: float,
    rights_status: str,
    replay_pass_rate: float,
    is_deterministic: bool,
    redaction_complete: bool,
    has_human_signoff: bool,
    confidence_score: float,
    requires_human_signoff: bool = True,
    replay_threshold: float = 0.85,
) -> PromotionResult:
    """
    Evaluate all promotion gates and return a PromotionResult.

    All gates must pass for `eligible` to be True.
    """
    gates = [
        gate_provenance_completeness(provenance_score),
        gate_rights_status(rights_status),
        gate_replay_pass_rate(replay_pass_rate, threshold=replay_threshold),
        gate_deterministic_step_graph(is_deterministic),
        gate_redaction_complete(redaction_complete),
        gate_human_reviewer_signoff(has_human_signoff, requires_signoff=requires_human_signoff),
    ]
    failing = [g.gate_name for g in gates if not g.passed]
    return PromotionResult(
        eligible=len(failing) == 0,
        gates=gates,
        failing_gates=failing,
        confidence_band=classify_confidence(confidence_score),
    )
