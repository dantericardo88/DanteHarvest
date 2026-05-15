"""Unit tests for harvest_core.rights.agpl_quarantine."""

import pytest
from pathlib import Path

from harvest_core.rights.agpl_quarantine import (
    KNOWN_AGPL_PACKAGES,
    AGPLPackageScanner,
    AGPLImportGuard,
    AGPLQuarantineError,
    QuarantineViolation,
    get_quarantine_report,
)


class TestKnownAgplPackages:
    def test_known_agpl_packages_in_set(self):
        # Core packages that must be present
        expected = {"networkx", "pymupdf", "pyaudio", "pynput", "gnupg", "python-gnupg"}
        for pkg in expected:
            assert pkg in KNOWN_AGPL_PACKAGES, f"Expected '{pkg}' in KNOWN_AGPL_PACKAGES"

    def test_is_frozenset(self):
        assert isinstance(KNOWN_AGPL_PACKAGES, frozenset)

    def test_nonempty(self):
        assert len(KNOWN_AGPL_PACKAGES) > 0


class TestAGPLPackageScannerRequirements:
    def test_scan_requirements_flags_agpl(self, tmp_path: Path):
        req = tmp_path / "requirements.txt"
        req.write_text("requests==2.31.0\npymupdf==1.23.0\nnumpy>=1.24\n")
        scanner = AGPLPackageScanner()
        violations = scanner.scan_requirements(req)
        pkg_names = [v.package_name.lower() for v in violations]
        assert any("pymupdf" in n for n in pkg_names), (
            f"Expected pymupdf violation, got: {pkg_names}"
        )

    def test_clean_requirements_passes(self, tmp_path: Path):
        req = tmp_path / "requirements.txt"
        req.write_text("requests==2.31.0\nnumpy>=1.24\npydantic>=2.0\n")
        scanner = AGPLPackageScanner()
        violations = scanner.scan_requirements(req)
        assert violations == [], f"Expected no violations, got: {violations}"

    def test_scan_requirements_multiple_agpl(self, tmp_path: Path):
        req = tmp_path / "requirements.txt"
        req.write_text("pymupdf==1.23.0\npyaudio==0.2.13\nrequests\n")
        scanner = AGPLPackageScanner()
        violations = scanner.scan_requirements(req)
        assert len(violations) >= 2

    def test_scan_requirements_nonexistent_file(self, tmp_path: Path):
        scanner = AGPLPackageScanner()
        violations = scanner.scan_requirements(tmp_path / "missing.txt")
        assert violations == []

    def test_scan_requirements_inline_comment_ignored(self, tmp_path: Path):
        req = tmp_path / "requirements.txt"
        # pymupdf appears only in a comment — should not flag
        req.write_text("requests==2.31.0  # not pymupdf\nnumpy\n")
        scanner = AGPLPackageScanner()
        violations = scanner.scan_requirements(req)
        assert violations == []

    def test_scan_requirements_pyproject_toml(self, tmp_path: Path):
        toml = tmp_path / "pyproject.toml"
        toml.write_text(
            '[project]\nname = "myapp"\n[project.dependencies]\npymupdf = ">=1.0"\nrequests = "*"\n'
        )
        scanner = AGPLPackageScanner()
        violations = scanner.scan_requirements(toml)
        pkg_names = [v.package_name.lower() for v in violations]
        assert any("pymupdf" in n for n in pkg_names)


class TestAGPLPackageScannerCheckImport:
    def test_check_import_known_agpl(self):
        scanner = AGPLPackageScanner()
        assert scanner.check_import("pymupdf") is True

    def test_check_import_case_insensitive_normalization(self):
        scanner = AGPLPackageScanner()
        # python-gnupg normalizes to python_gnupg
        assert scanner.check_import("python-gnupg") is True

    def test_check_import_clean_package(self):
        scanner = AGPLPackageScanner()
        assert scanner.check_import("requests") is False

    def test_check_import_pydantic_clean(self):
        scanner = AGPLPackageScanner()
        assert scanner.check_import("pydantic") is False


class TestAGPLPackageScannerInstalled:
    def test_scan_installed_returns_list(self):
        scanner = AGPLPackageScanner()
        result = scanner.scan_installed()
        assert isinstance(result, list)

    def test_scan_installed_violations_are_typed(self):
        scanner = AGPLPackageScanner()
        for v in scanner.scan_installed():
            assert isinstance(v, QuarantineViolation)
            assert v.package_name
            assert v.severity in ("high", "medium", "low")


class TestAGPLImportGuard:
    def test_guard_import_strict_raises(self):
        guard = AGPLImportGuard(strict=True)
        with pytest.raises(AGPLQuarantineError):
            guard.guard_import("pymupdf")

    def test_guard_import_non_strict_no_raise(self):
        guard = AGPLImportGuard(strict=False)
        # Must not raise — logs a warning instead
        guard.guard_import("pymupdf")

    def test_guard_import_clean_package_strict_no_raise(self):
        guard = AGPLImportGuard(strict=True)
        guard.guard_import("requests")  # clean — should not raise

    def test_guard_import_strict_property(self):
        assert AGPLImportGuard(strict=True).strict is True
        assert AGPLImportGuard(strict=False).strict is False

    def test_guard_import_strict_error_message(self):
        guard = AGPLImportGuard(strict=True)
        with pytest.raises(AGPLQuarantineError, match="pymupdf"):
            guard.guard_import("pymupdf")


class TestGetQuarantineReport:
    def test_get_quarantine_report_returns_dict(self):
        report = get_quarantine_report()
        assert isinstance(report, dict)

    def test_report_has_expected_keys(self):
        report = get_quarantine_report()
        assert "quarantine_active" in report
        assert "known_packages_count" in report
        assert "installed_violations" in report
        assert "installed_violation_count" in report

    def test_report_quarantine_active_true_by_default(self):
        report = get_quarantine_report()
        assert report["quarantine_active"] is True

    def test_report_known_packages_count_positive(self):
        report = get_quarantine_report()
        assert report["known_packages_count"] > 0

    def test_report_installed_violations_is_list(self):
        report = get_quarantine_report()
        assert isinstance(report["installed_violations"], list)

    def test_report_violation_count_matches_list(self):
        report = get_quarantine_report()
        assert report["installed_violation_count"] == len(report["installed_violations"])
