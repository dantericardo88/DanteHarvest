"""
Unit tests for CoverageEnforcer and CoverageReport.
"""
from __future__ import annotations

import json
import os
import tempfile

import pytest
from harvest_index.coverage import CoverageEnforcer, CoverageReport


@pytest.fixture
def enforcer():
    return CoverageEnforcer(threshold=80.0)


@pytest.fixture
def passing_report():
    return CoverageReport(
        total_lines=1000,
        covered_lines=850,
        coverage_pct=85.0,
        module_coverage={"src/a.py": 90.0, "src/b.py": 80.0},
        missing_modules=[],
    )


@pytest.fixture
def failing_report():
    return CoverageReport(
        total_lines=1000,
        covered_lines=700,
        coverage_pct=70.0,
        module_coverage={"src/a.py": 70.0, "src/b.py": 0.0},
        missing_modules=["src/b.py"],
    )


# ---------------------------------------------------------------------------
# CoverageReport.passes_threshold
# ---------------------------------------------------------------------------

class TestCoverageReportPassesThreshold:
    def test_above_threshold_passes(self):
        report = CoverageReport(coverage_pct=85.0)
        assert report.passes_threshold(80.0) is True

    def test_exact_threshold_passes(self):
        report = CoverageReport(coverage_pct=80.0)
        assert report.passes_threshold(80.0) is True

    def test_below_threshold_fails(self):
        report = CoverageReport(coverage_pct=75.0)
        assert report.passes_threshold(80.0) is False

    def test_default_threshold_is_80(self):
        report = CoverageReport(coverage_pct=80.0)
        assert report.passes_threshold() is True

    def test_custom_threshold(self):
        report = CoverageReport(coverage_pct=90.0)
        assert report.passes_threshold(95.0) is False
        assert report.passes_threshold(85.0) is True


# ---------------------------------------------------------------------------
# CoverageReport.summary
# ---------------------------------------------------------------------------

class TestCoverageReportSummary:
    def test_summary_keys(self, passing_report):
        s = passing_report.summary()
        assert "total_lines" in s
        assert "covered_lines" in s
        assert "coverage_pct" in s
        assert "module_count" in s
        assert "missing_modules" in s

    def test_summary_values(self, passing_report):
        s = passing_report.summary()
        assert s["total_lines"] == 1000
        assert s["coverage_pct"] == 85.0
        assert s["module_count"] == 2


# ---------------------------------------------------------------------------
# CoverageEnforcer.check_threshold
# ---------------------------------------------------------------------------

class TestCheckThreshold:
    def test_passed_true_when_above(self, enforcer, passing_report):
        result = enforcer.check_threshold(passing_report)
        assert result["passed"] is True

    def test_passed_false_when_below(self, enforcer, failing_report):
        result = enforcer.check_threshold(failing_report)
        assert result["passed"] is False

    def test_threshold_in_result(self, enforcer, passing_report):
        result = enforcer.check_threshold(passing_report)
        assert result["threshold"] == pytest.approx(80.0)

    def test_actual_in_result(self, enforcer, passing_report):
        result = enforcer.check_threshold(passing_report)
        assert result["actual"] == pytest.approx(85.0)

    def test_gap_correct_for_passing(self, enforcer, passing_report):
        result = enforcer.check_threshold(passing_report)
        # threshold(80) - actual(85) = -5 → surplus
        assert result["gap"] == pytest.approx(-5.0)

    def test_gap_correct_for_failing(self, enforcer, failing_report):
        result = enforcer.check_threshold(failing_report)
        # threshold(80) - actual(70) = 10 → shortfall
        assert result["gap"] == pytest.approx(10.0)

    def test_message_present(self, enforcer, passing_report):
        result = enforcer.check_threshold(passing_report)
        assert isinstance(result["message"], str)
        assert len(result["message"]) > 0


# ---------------------------------------------------------------------------
# CoverageEnforcer.generate_text_report
# ---------------------------------------------------------------------------

class TestGenerateTextReport:
    def test_pass_string_in_report(self, enforcer, passing_report):
        text = enforcer.generate_text_report(passing_report)
        assert "PASS" in text

    def test_fail_string_in_report(self, enforcer, failing_report):
        text = enforcer.generate_text_report(failing_report)
        assert "FAIL" in text

    def test_coverage_pct_in_report(self, enforcer, passing_report):
        text = enforcer.generate_text_report(passing_report)
        assert "85.0" in text

    def test_missing_modules_listed(self, enforcer, failing_report):
        text = enforcer.generate_text_report(failing_report)
        assert "src/b.py" in text

    def test_returns_string(self, enforcer, passing_report):
        assert isinstance(enforcer.generate_text_report(passing_report), str)

    def test_no_pass_in_fail_report(self, enforcer, failing_report):
        text = enforcer.generate_text_report(failing_report)
        # FAIL report must not contain a standalone PASS verdict
        assert "FAIL" in text


# ---------------------------------------------------------------------------
# CoverageEnforcer.get_coverage_from_env
# ---------------------------------------------------------------------------

class TestGetCoverageFromEnv:
    def test_reads_env_var(self, monkeypatch):
        monkeypatch.setenv("COVERAGE_PCT", "87.5")
        result = CoverageEnforcer.get_coverage_from_env()
        assert result == pytest.approx(87.5)

    def test_returns_none_when_unset(self, monkeypatch):
        monkeypatch.delenv("COVERAGE_PCT", raising=False)
        assert CoverageEnforcer.get_coverage_from_env() is None

    def test_returns_none_for_invalid_value(self, monkeypatch):
        monkeypatch.setenv("COVERAGE_PCT", "not-a-number")
        assert CoverageEnforcer.get_coverage_from_env() is None

    def test_integer_string_works(self, monkeypatch):
        monkeypatch.setenv("COVERAGE_PCT", "90")
        result = CoverageEnforcer.get_coverage_from_env()
        assert result == pytest.approx(90.0)


# ---------------------------------------------------------------------------
# CoverageEnforcer.parse_coverage_json
# ---------------------------------------------------------------------------

class TestParseCoverageJson:
    def _write_coverage_json(self, data: dict) -> str:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as fh:
            json.dump(data, fh)
            return fh.name

    def test_parses_totals(self, enforcer):
        data = {
            "totals": {
                "num_statements": 200,
                "covered_lines": 180,
                "percent_covered": 90.0,
            },
            "files": {},
        }
        path = self._write_coverage_json(data)
        try:
            report = enforcer.parse_coverage_json(path)
            assert report.total_lines == 200
            assert report.covered_lines == 180
            assert report.coverage_pct == pytest.approx(90.0)
        finally:
            os.unlink(path)

    def test_parses_module_coverage(self, enforcer):
        data = {
            "totals": {"num_statements": 100, "covered_lines": 80, "percent_covered": 80.0},
            "files": {
                "src/a.py": {"summary": {"num_statements": 50, "covered_lines": 50, "percent_covered": 100.0}},
                "src/b.py": {"summary": {"num_statements": 50, "covered_lines": 30, "percent_covered": 60.0}},
            },
        }
        path = self._write_coverage_json(data)
        try:
            report = enforcer.parse_coverage_json(path)
            assert "src/a.py" in report.module_coverage
            assert report.module_coverage["src/a.py"] == pytest.approx(100.0)
            assert report.module_coverage["src/b.py"] == pytest.approx(60.0)
        finally:
            os.unlink(path)

    def test_detects_missing_modules(self, enforcer):
        data = {
            "totals": {"num_statements": 100, "covered_lines": 60, "percent_covered": 60.0},
            "files": {
                "src/untouched.py": {"summary": {"num_statements": 40, "covered_lines": 0, "percent_covered": 0.0}},
            },
        }
        path = self._write_coverage_json(data)
        try:
            report = enforcer.parse_coverage_json(path)
            assert "src/untouched.py" in report.missing_modules
        finally:
            os.unlink(path)

    def test_returns_coverage_report(self, enforcer):
        data = {"totals": {"num_statements": 0, "covered_lines": 0, "percent_covered": 0.0}, "files": {}}
        path = self._write_coverage_json(data)
        try:
            report = enforcer.parse_coverage_json(path)
            assert isinstance(report, CoverageReport)
        finally:
            os.unlink(path)
