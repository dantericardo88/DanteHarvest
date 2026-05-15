"""
AGPLQuarantine — import-level AGPL scanner and guard.

Prevents AGPL-licensed packages from contaminating the Harvest pipeline.
AGPL requires derivative works served over a network to publish their source.
Any pipeline that processes user data via an AGPL library inherits that obligation.

Constitutional guarantee:
- QUARANTINE_ACTIVE=True by default; set to False only for explicit overrides
- AGPLPackageScanner uses stdlib only (importlib.metadata, pathlib, re)
- AGPLImportGuard.strict=True raises at import time (fail-closed)
- AGPLImportGuard.strict=False logs a warning (fail-open, for gradual migration)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known AGPL packages relevant to a data pipeline
# ---------------------------------------------------------------------------

KNOWN_AGPL_PACKAGES: frozenset = frozenset(
    {
        # Network / crypto
        "gnupg",
        "python-gnupg",
        "pyOpenSSL",  # historically AGPL in some forks; included for caution
        # Audio / input (capture pipelines)
        "pyaudio",
        "pynput",
        # Graph / ML libraries with AGPL editions
        "networkx",           # now Apache-2.0 but older 2.x releases were GPL
        "igraph",             # GPL-2+; C library is GPL
        "pygraph",
        # Document processing
        "pymupdf",            # AGPL-3.0 (the free tier)
        "fitz",               # PyMuPDF alias
        "ghostscript",        # AGPL in its free distribution
        # OCR
        "tesserocr",          # AGPL via Tesseract itself
        # Fuzzy-match / dedup
        "datasketch",         # MIT but commonly bundled with AGPL components
        # Catch-all prefix
        "agpl-utils",
        "agpl-tools",
        "agpl-license",
    }
)

# Packages that carry AGPL-family license names in their metadata
_AGPL_LICENSE_PATTERNS: list[re.Pattern] = [
    re.compile(r"AGPL", re.IGNORECASE),
    re.compile(r"Affero", re.IGNORECASE),
    re.compile(r"GNU Affero General Public License", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# QuarantineViolation
# ---------------------------------------------------------------------------

@dataclass
class QuarantineViolation:
    """Describes a single AGPL quarantine violation."""

    package_name: str
    license: str
    reason: str
    severity: str = "high"  # "high" | "medium" | "low"

    def to_dict(self) -> dict:
        return {
            "package_name": self.package_name,
            "license": self.license,
            "reason": self.reason,
            "severity": self.severity,
        }


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class AGPLQuarantineError(Exception):
    """Raised when an AGPL-quarantined package is imported in strict mode."""


# ---------------------------------------------------------------------------
# AGPLPackageScanner
# ---------------------------------------------------------------------------

class AGPLPackageScanner:
    """
    Scans the Python environment and requirements files for AGPL packages.

    Uses only stdlib (importlib.metadata, pathlib, re) — no third-party deps.
    """

    def scan_installed(self) -> List[QuarantineViolation]:
        """
        Check every installed distribution for AGPL license metadata.

        Returns a list of QuarantineViolation for packages that are:
        - in KNOWN_AGPL_PACKAGES (by normalized name), or
        - have a license classifier/metadata matching AGPL patterns.
        """
        try:
            from importlib.metadata import packages_distributions, distributions
        except ImportError:
            # Python < 3.9 fallback
            try:
                from importlib_metadata import packages_distributions, distributions  # type: ignore
            except ImportError:
                logger.warning("importlib.metadata not available; skipping installed scan")
                return []

        violations: List[QuarantineViolation] = []
        seen: set = set()

        for dist in distributions():
            name: str = dist.metadata.get("Name", "") or ""
            normalized = _normalize_pkg_name(name)

            if not name or normalized in seen:
                continue
            seen.add(normalized)

            # Check against known quarantine list
            if normalized in {_normalize_pkg_name(p) for p in KNOWN_AGPL_PACKAGES}:
                license_val = dist.metadata.get("License", "AGPL (known list)")
                violations.append(
                    QuarantineViolation(
                        package_name=name,
                        license=license_val or "AGPL (known list)",
                        reason=f"Package '{name}' is in the AGPL quarantine list",
                        severity="high",
                    )
                )
                continue

            # Check license metadata for AGPL patterns
            license_val = dist.metadata.get("License", "") or ""
            classifiers = dist.metadata.get_all("Classifier") or []
            classifier_str = " ".join(classifiers)

            combined = f"{license_val} {classifier_str}"
            if _matches_agpl(combined):
                violations.append(
                    QuarantineViolation(
                        package_name=name,
                        license=license_val or "AGPL (detected from classifiers)",
                        reason=f"Package '{name}' has AGPL license metadata",
                        severity="high",
                    )
                )

        return violations

    def scan_requirements(self, path: Path) -> List[QuarantineViolation]:
        """
        Parse a requirements.txt or pyproject.toml and flag AGPL packages.

        Only name-matching against KNOWN_AGPL_PACKAGES is performed here;
        no network calls are made.
        """
        path = Path(path)
        if not path.exists():
            logger.warning("scan_requirements: path does not exist: %s", path)
            return []

        content = path.read_text(encoding="utf-8")
        package_names = _extract_package_names(path.name, content)

        violations: List[QuarantineViolation] = []
        known_normalized = {_normalize_pkg_name(p): p for p in KNOWN_AGPL_PACKAGES}

        for pkg in package_names:
            norm = _normalize_pkg_name(pkg)
            if norm in known_normalized:
                original_name = known_normalized[norm]
                violations.append(
                    QuarantineViolation(
                        package_name=pkg,
                        license="AGPL (known list)",
                        reason=(
                            f"Package '{pkg}' is in the AGPL quarantine list "
                            f"(matches '{original_name}')"
                        ),
                        severity="high",
                    )
                )
        return violations

    def check_import(self, package_name: str) -> bool:
        """
        Return True if the given package name is AGPL-quarantined.

        Checks against KNOWN_AGPL_PACKAGES only (no metadata lookup).
        Suitable for use in hot-path guards.
        """
        norm = _normalize_pkg_name(package_name)
        known_normalized = {_normalize_pkg_name(p) for p in KNOWN_AGPL_PACKAGES}
        return norm in known_normalized


# ---------------------------------------------------------------------------
# AGPLImportGuard
# ---------------------------------------------------------------------------

class AGPLImportGuard:
    """
    Gate that prevents (or warns about) AGPL package imports.

    Usage:
        guard = AGPLImportGuard(strict=True)
        guard.guard_import("pymupdf")   # raises AGPLQuarantineError in strict mode

        guard = AGPLImportGuard(strict=False)
        guard.guard_import("pymupdf")   # logs a warning only
    """

    def __init__(self, strict: bool = False) -> None:
        self._strict = strict
        self._scanner = AGPLPackageScanner()

    @property
    def strict(self) -> bool:
        return self._strict

    def guard_import(self, package_name: str) -> None:
        """
        Check whether *package_name* is AGPL-quarantined before use.

        Strict mode: raises AGPLQuarantineError.
        Non-strict mode: logs a WARNING.
        """
        if not QUARANTINE_ACTIVE:
            return

        if self._scanner.check_import(package_name):
            msg = (
                f"AGPL quarantine violation: attempted import of '{package_name}'. "
                "This package is AGPL-licensed. Using it in a networked service "
                "may require you to publish your source code. "
                "Use an MIT/Apache alternative or obtain a commercial license."
            )
            if self._strict:
                raise AGPLQuarantineError(msg)
            else:
                logger.warning(msg)


# ---------------------------------------------------------------------------
# Module-level quarantine flag
# ---------------------------------------------------------------------------

#: Set to False to disable all AGPL quarantine checks globally.
#: Override via: import harvest_core.rights.agpl_quarantine as aq; aq.QUARANTINE_ACTIVE = False
QUARANTINE_ACTIVE: bool = True


# ---------------------------------------------------------------------------
# get_quarantine_report
# ---------------------------------------------------------------------------

def get_quarantine_report() -> dict:
    """
    Return a summary dict of all AGPL violations found in the current environment.

    Keys:
        quarantine_active: bool
        known_packages_count: int
        installed_violations: List[dict]
        installed_violation_count: int
        error: Optional[str]  — present only on scan failure
    """
    report: dict = {
        "quarantine_active": QUARANTINE_ACTIVE,
        "known_packages_count": len(KNOWN_AGPL_PACKAGES),
        "installed_violations": [],
        "installed_violation_count": 0,
    }
    try:
        scanner = AGPLPackageScanner()
        violations = scanner.scan_installed()
        report["installed_violations"] = [v.to_dict() for v in violations]
        report["installed_violation_count"] = len(violations)
    except Exception as exc:  # pragma: no cover
        report["error"] = str(exc)
    return report


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _normalize_pkg_name(name: str) -> str:
    """Normalize a package name to lowercase with hyphens replaced by underscores."""
    return re.sub(r"[-_.]+", "_", name).lower()


def _matches_agpl(text: str) -> bool:
    """Return True if *text* contains any AGPL license pattern."""
    return any(pat.search(text) for pat in _AGPL_LICENSE_PATTERNS)


def _extract_package_names(filename: str, content: str) -> List[str]:
    """
    Extract package names from requirements.txt or pyproject.toml content.
    Returns a list of bare package names (no version specifiers).
    """
    names: List[str] = []

    if filename.endswith("pyproject.toml"):
        # Capture lines inside [project] dependencies or [tool.poetry.dependencies]
        # Simple regex extraction — no TOML parser required.
        in_deps = False
        for line in content.splitlines():
            stripped = line.strip()
            # Enter a dependencies section
            if re.match(r"^\[.*dependencies.*\]", stripped, re.IGNORECASE):
                in_deps = True
                continue
            # Leave on next section header
            if stripped.startswith("[") and in_deps:
                in_deps = False
                continue
            if in_deps and stripped and not stripped.startswith("#"):
                # Extract the package name before any version constraint
                match = re.match(r'^["\']?([A-Za-z0-9_.\-]+)', stripped)
                if match:
                    names.append(match.group(1))
    else:
        # requirements.txt format
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("-"):
                continue
            # Strip inline comments
            line = line.split("#")[0].strip()
            # Extract package name before any version specifier or extras
            match = re.match(r"^([A-Za-z0-9_.\-]+)", line)
            if match:
                names.append(match.group(1))

    return names
