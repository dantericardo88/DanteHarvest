"""Unit tests for confidence bands and promotion gates."""

import pytest

from harvest_core.evaluation.gates import (
    ConfidenceBand,
    PromotionResult,
    classify_confidence,
    evaluate_promotion,
    gate_provenance_completeness,
    gate_redaction_complete,
    gate_replay_pass_rate,
    gate_rights_status,
)


class TestConfidenceBands:
    def test_green_at_0_90(self):
        assert classify_confidence(0.90) == ConfidenceBand.GREEN

    def test_green_at_1_0(self):
        assert classify_confidence(1.0) == ConfidenceBand.GREEN

    def test_yellow_at_0_80(self):
        assert classify_confidence(0.80) == ConfidenceBand.YELLOW

    def test_orange_at_0_60(self):
        assert classify_confidence(0.60) == ConfidenceBand.ORANGE

    def test_red_at_0_30(self):
        assert classify_confidence(0.30) == ConfidenceBand.RED

    def test_boundary_0_75_is_yellow(self):
        assert classify_confidence(0.75) == ConfidenceBand.YELLOW

    def test_boundary_0_50_is_orange(self):
        assert classify_confidence(0.50) == ConfidenceBand.ORANGE


class TestIndividualGates:
    def test_provenance_completeness_passes_at_1(self):
        assert gate_provenance_completeness(1.0).passed is True

    def test_provenance_completeness_fails_below_1(self):
        result = gate_provenance_completeness(0.99)
        assert result.passed is False

    def test_rights_status_approved_passes(self):
        assert gate_rights_status("approved").passed is True

    def test_rights_status_pending_fails(self):
        assert gate_rights_status("pending").passed is False

    def test_replay_pass_rate_passes_at_threshold(self):
        assert gate_replay_pass_rate(0.85, threshold=0.85).passed is True

    def test_replay_pass_rate_fails_below_threshold(self):
        assert gate_replay_pass_rate(0.84, threshold=0.85).passed is False

    def test_redaction_complete_passes(self):
        assert gate_redaction_complete(True).passed is True

    def test_redaction_incomplete_fails(self):
        assert gate_redaction_complete(False).passed is False


class TestEvaluatePromotion:
    def _passing_args(self, **overrides) -> dict:
        args = dict(
            provenance_score=1.0,
            rights_status="approved",
            replay_pass_rate=0.90,
            is_deterministic=True,
            redaction_complete=True,
            has_human_signoff=True,
            confidence_score=0.92,
        )
        args.update(overrides)
        return args

    def test_all_passing_is_eligible(self):
        result = evaluate_promotion(**self._passing_args())
        assert result.eligible is True
        assert result.confidence_band == ConfidenceBand.GREEN

    def test_single_failing_gate_makes_ineligible(self):
        result = evaluate_promotion(**self._passing_args(provenance_score=0.5))
        assert result.eligible is False
        assert "provenance_completeness" in result.failing_gates

    def test_summary_mentions_failing_gates(self):
        result = evaluate_promotion(**self._passing_args(rights_status="pending"))
        assert "rights_status" in result.summary()
