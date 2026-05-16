"""Tests for PackQualityScorer and PackQualityReport."""
import time
import pytest

from harvest_distill.packs.pack_quality_scorer import (
    PackQualityScorer,
    PackQualityReport,
    REQUIRED_PACK_FIELDS,
    OPTIONAL_PACK_FIELDS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def scorer():
    return PackQualityScorer()


def _full_pack(num_artifacts: int = 3) -> dict:
    """A pack with all required + optional fields and fresh artifacts."""
    now = time.time()
    return {
        "id": "pack-001",
        "name": "Test Pack",
        "version": "1.0",
        "artifacts": [
            {"id": f"art-{i}", "content_type": ct, "created_at": now - i * 3600}
            for i, ct in enumerate(["text", "image", "audio", "video", "code"][:num_artifacts])
        ],
        "description": "A test pack",
        "created_at": now,
        "tags": ["a", "b"],
        "schema": "v1",
        "source_url": "https://example.com",
    }


def _minimal_pack() -> dict:
    """Pack with only required fields and fresh artifacts."""
    now = time.time()
    return {
        "id": "pack-min",
        "name": "Minimal",
        "version": "0.1",
        "artifacts": [{"id": "a1", "content_type": "text", "created_at": now}],
    }


def _empty_pack() -> dict:
    """Pack with no fields at all."""
    return {}


# ---------------------------------------------------------------------------
# score() — returns PackQualityReport
# ---------------------------------------------------------------------------

class TestScoreReturnsReport:
    def test_returns_pack_quality_report(self, scorer):
        report = scorer.score(_minimal_pack())
        assert isinstance(report, PackQualityReport)

    def test_pack_id_propagated(self, scorer):
        report = scorer.score(_minimal_pack())
        assert report.pack_id == "pack-min"

    def test_unknown_pack_id_for_empty(self, scorer):
        report = scorer.score(_empty_pack())
        assert report.pack_id == "unknown"

    def test_artifact_count_correct(self, scorer):
        pack = _full_pack(num_artifacts=3)
        report = scorer.score(pack)
        assert report.artifact_count == 3

    def test_zero_artifacts_when_empty(self, scorer):
        report = scorer.score(_empty_pack())
        assert report.artifact_count == 0

    def test_overall_score_in_range(self, scorer):
        report = scorer.score(_full_pack())
        assert 0.0 <= report.overall_score <= 1.0

    def test_completeness_score_in_range(self, scorer):
        report = scorer.score(_full_pack())
        assert 0.0 <= report.completeness_score <= 1.0

    def test_diversity_score_in_range(self, scorer):
        report = scorer.score(_full_pack())
        assert 0.0 <= report.diversity_score <= 1.0

    def test_freshness_score_in_range(self, scorer):
        report = scorer.score(_full_pack())
        assert 0.0 <= report.freshness_score <= 1.0


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_full_pack_schema_valid(self, scorer):
        report = scorer.score(_full_pack())
        assert report.schema_valid is True
        assert report.schema_errors == []

    def test_missing_all_required_fields_invalid(self, scorer):
        report = scorer.score(_empty_pack())
        assert report.schema_valid is False
        assert len(report.schema_errors) == len(REQUIRED_PACK_FIELDS)

    def test_missing_one_required_field(self, scorer):
        pack = _minimal_pack()
        del pack["version"]
        report = scorer.score(pack)
        assert report.schema_valid is False
        assert any("version" in e for e in report.schema_errors)

    def test_missing_id_reported(self, scorer):
        pack = _minimal_pack()
        del pack["id"]
        report = scorer.score(pack)
        assert report.schema_valid is False
        assert any("id" in e for e in report.schema_errors)

    def test_optional_fields_missing_still_valid(self, scorer):
        # minimal pack has all required fields, just no optional ones
        report = scorer.score(_minimal_pack())
        assert report.schema_valid is True

    def test_error_messages_mention_field_name(self, scorer):
        report = scorer.score(_empty_pack())
        all_text = " ".join(report.schema_errors)
        for f in REQUIRED_PACK_FIELDS:
            assert f in all_text


# ---------------------------------------------------------------------------
# passes_threshold
# ---------------------------------------------------------------------------

class TestPassesThreshold:
    def test_high_score_passes_default_threshold(self, scorer):
        pack = _full_pack(num_artifacts=5)
        report = scorer.score(pack)
        # Full pack with 5 diverse fresh artifacts should score well
        # completeness=1.0, diversity=1.0, freshness≈1.0 → overall≈1.0
        assert report.passes_threshold(0.7)

    def test_empty_pack_fails_default_threshold(self, scorer):
        report = scorer.score(_empty_pack())
        assert not report.passes_threshold(0.7)

    def test_custom_threshold_zero_always_passes(self, scorer):
        report = scorer.score(_empty_pack())
        assert report.passes_threshold(0.0)

    def test_custom_threshold_one_fails_unless_perfect(self, scorer):
        # minimal pack won't be perfect
        report = scorer.score(_minimal_pack())
        assert not report.passes_threshold(1.0)

    def test_default_threshold_is_0_7(self, scorer):
        # PackQualityReport.passes_threshold() default should be 0.7
        import inspect
        sig = inspect.signature(PackQualityReport.passes_threshold)
        assert sig.parameters["threshold"].default == 0.7


# ---------------------------------------------------------------------------
# score_batch
# ---------------------------------------------------------------------------

class TestScoreBatch:
    def test_returns_list(self, scorer):
        result = scorer.score_batch([_minimal_pack(), _full_pack()])
        assert isinstance(result, list)

    def test_length_matches_input(self, scorer):
        packs = [_minimal_pack(), _full_pack(), _empty_pack()]
        result = scorer.score_batch(packs)
        assert len(result) == 3

    def test_each_element_is_report(self, scorer):
        result = scorer.score_batch([_minimal_pack(), _full_pack()])
        for r in result:
            assert isinstance(r, PackQualityReport)

    def test_empty_list(self, scorer):
        result = scorer.score_batch([])
        assert result == []

    def test_correct_pack_ids_preserved(self, scorer):
        packs = [_minimal_pack(), _full_pack()]
        reports = scorer.score_batch(packs)
        assert reports[0].pack_id == "pack-min"
        assert reports[1].pack_id == "pack-001"


# ---------------------------------------------------------------------------
# get_failing_packs
# ---------------------------------------------------------------------------

class TestGetFailingPacks:
    def test_empty_pack_is_failing(self, scorer):
        failing = scorer.get_failing_packs([_empty_pack()], threshold=0.7)
        assert len(failing) == 1

    def test_full_fresh_pack_not_failing(self, scorer):
        failing = scorer.get_failing_packs([_full_pack(5)], threshold=0.7)
        assert len(failing) == 0

    def test_filters_only_failing(self, scorer):
        packs = [_full_pack(5), _empty_pack(), _minimal_pack()]
        failing = scorer.get_failing_packs(packs, threshold=0.7)
        # empty pack definitely fails; full pack definitely passes
        failing_ids = [p.get("id", "unknown") for p in failing]
        assert "unknown" in failing_ids   # empty pack
        assert "pack-001" not in failing_ids  # full pack

    def test_returns_original_dicts(self, scorer):
        pack = _empty_pack()
        failing = scorer.get_failing_packs([pack], threshold=0.7)
        assert failing[0] is pack

    def test_threshold_zero_returns_none_failing(self, scorer):
        packs = [_empty_pack(), _minimal_pack()]
        failing = scorer.get_failing_packs(packs, threshold=0.0)
        assert len(failing) == 0

    def test_threshold_one_returns_all_failing(self, scorer):
        packs = [_minimal_pack(), _full_pack()]
        failing = scorer.get_failing_packs(packs, threshold=1.0)
        assert len(failing) == len(packs)


# ---------------------------------------------------------------------------
# summary()
# ---------------------------------------------------------------------------

class TestSummary:
    def test_summary_returns_dict(self, scorer):
        report = scorer.score(_minimal_pack())
        s = report.summary()
        assert isinstance(s, dict)

    def test_summary_contains_required_keys(self, scorer):
        report = scorer.score(_minimal_pack())
        s = report.summary()
        for key in ("pack_id", "schema_valid", "overall_score", "artifact_count", "passes_threshold"):
            assert key in s

    def test_summary_overall_score_rounded(self, scorer):
        report = scorer.score(_full_pack())
        s = report.summary()
        # Should be rounded to 3 decimal places max
        assert s["overall_score"] == round(s["overall_score"], 3)
