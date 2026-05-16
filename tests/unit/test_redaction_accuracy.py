"""
Tests for redaction_accuracy improvements:
- Stricter email pattern (RFC 5322 compliant, no version-string false positives)
- Configurable context_window
- resolve_overlaps keeps longest/highest-priority match
- scan_with_context returns context_before / context_after
- get_scanner_config returns pattern_count and context_window
"""

from __future__ import annotations

from harvest_core.rights.redaction_scanner import RedactionScanner, PATTERN_PRIORITY


# ---------------------------------------------------------------------------
# Email pattern — stricter RFC 5322 compliance
# ---------------------------------------------------------------------------

class TestEmailPattern:
    """The email regex must not match version strings like '1.0@2.0'."""

    def _scanner(self) -> RedactionScanner:
        return RedactionScanner(scan_pii=True, scan_secrets=False)

    def test_valid_email_detected(self):
        scanner = self._scanner()
        result = scanner.scan("Contact us at hello@example.com for support.")
        names = [f.pattern_name for f in result.findings]
        assert "email_address" in names

    def test_version_string_not_matched(self):
        """'1.0@2.0' must NOT be detected as an email address."""
        scanner = self._scanner()
        result = scanner.scan("Running version 1.0@2.0 of the software.")
        email_findings = [f for f in result.findings if f.pattern_name == "email_address"]
        assert email_findings == [], (
            f"Email pattern wrongly matched version string: {email_findings}"
        )

    def test_version_at_version_not_matched(self):
        """'2.0@3.5' (digit.digit@digit.digit) must NOT be detected as email."""
        scanner = self._scanner()
        result = scanner.scan("version 2.0@3.5 released")
        email_findings = [f for f in result.findings if f.pattern_name == "email_address"]
        assert email_findings == [], (
            f"Email pattern wrongly matched numeric version string: {email_findings}"
        )

    def test_valid_subdomain_email_detected(self):
        scanner = self._scanner()
        result = scanner.scan("Send reports to ops+alerts@mail.example.co.uk")
        names = [f.pattern_name for f in result.findings]
        assert "email_address" in names

    def test_pure_numeric_version_string_not_matched(self):
        """'1.0@2.0' (the canonical version-string false-positive) must not match."""
        scanner = self._scanner()
        result = scanner.scan("upgrade from 1.0@2.0 to 3.0")
        email_findings = [f for f in result.findings if f.pattern_name == "email_address"]
        assert email_findings == [], (
            f"Email pattern wrongly matched '1.0@2.0': {email_findings}"
        )


# ---------------------------------------------------------------------------
# scan_with_context
# ---------------------------------------------------------------------------

class TestScanWithContext:
    def test_returns_context_before_and_after(self):
        scanner = RedactionScanner(scan_pii=True, scan_secrets=False, context_window=20)
        text = "Hello world, reach me at user@example.com and say hi"
        matches = scanner.scan_with_context(text)
        assert len(matches) >= 1
        email_matches = [m for m in matches if m["pattern_name"] == "email_address"]
        assert len(email_matches) >= 1
        m = email_matches[0]
        assert "context_before" in m
        assert "context_after" in m
        assert "user@example.com" not in m["context_before"]
        assert "user@example.com" not in m["context_after"]

    def test_context_window_respected(self):
        scanner = RedactionScanner(scan_pii=True, scan_secrets=False, context_window=5)
        text = "X" * 50 + "user@example.com" + "Y" * 50
        matches = scanner.scan_with_context(text)
        email_matches = [m for m in matches if m["pattern_name"] == "email_address"]
        assert len(email_matches) >= 1
        m = email_matches[0]
        assert len(m["context_before"]) <= 5
        assert len(m["context_after"]) <= 5

    def test_empty_text_returns_empty_list(self):
        scanner = RedactionScanner(scan_pii=True, scan_secrets=False)
        assert scanner.scan_with_context("") == []

    def test_clean_text_returns_empty_list(self):
        scanner = RedactionScanner(scan_pii=True, scan_secrets=False)
        assert scanner.scan_with_context("No sensitive data here.") == []


# ---------------------------------------------------------------------------
# resolve_overlaps
# ---------------------------------------------------------------------------

class TestResolveOverlaps:
    def _make_match(self, name: str, start: int, end: int) -> dict:
        return {
            "pattern_name": name,
            "category": "pii",
            "start": start,
            "end": end,
            "matched_text": "x" * (end - start),
            "context_before": "",
            "context_after": "",
        }

    def test_non_overlapping_all_kept(self):
        scanner = RedactionScanner()
        m1 = self._make_match("email_address", 0, 10)
        m2 = self._make_match("us_phone", 20, 30)
        result = scanner.resolve_overlaps([m1, m2])
        assert len(result) == 2

    def test_overlapping_keeps_longer_span(self):
        scanner = RedactionScanner()
        short = self._make_match("email_address", 5, 10)   # span 5
        long_ = self._make_match("us_ssn", 5, 15)          # span 10 — should win
        result = scanner.resolve_overlaps([short, long_])
        assert len(result) == 1
        assert result[0]["pattern_name"] == "us_ssn"

    def test_overlapping_same_span_higher_priority_wins(self):
        scanner = RedactionScanner()
        low  = self._make_match("email_address", 0, 10)   # priority 6
        high = self._make_match("credit_card", 0, 10)     # priority 10
        result = scanner.resolve_overlaps([low, high])
        assert len(result) == 1
        assert result[0]["pattern_name"] == "credit_card"

    def test_empty_input_returns_empty(self):
        scanner = RedactionScanner()
        assert scanner.resolve_overlaps([]) == []

    def test_single_item_returned_unchanged(self):
        scanner = RedactionScanner()
        m = self._make_match("us_ssn", 0, 11)
        result = scanner.resolve_overlaps([m])
        assert result == [m]

    def test_adjacent_non_overlapping_both_kept(self):
        """Matches that share a boundary (end == start) are NOT overlapping."""
        scanner = RedactionScanner()
        m1 = self._make_match("email_address", 0, 10)
        m2 = self._make_match("us_phone", 10, 20)
        result = scanner.resolve_overlaps([m1, m2])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# get_scanner_config
# ---------------------------------------------------------------------------

class TestGetScannerConfig:
    def test_returns_pattern_count(self):
        scanner = RedactionScanner(scan_pii=True, scan_secrets=True)
        config = scanner.get_scanner_config()
        assert "pattern_count" in config
        assert isinstance(config["pattern_count"], int)
        assert config["pattern_count"] > 0

    def test_returns_context_window(self):
        scanner = RedactionScanner(context_window=99)
        config = scanner.get_scanner_config()
        assert config["context_window"] == 99

    def test_returns_patterns_list(self):
        scanner = RedactionScanner(scan_pii=True, scan_secrets=True)
        config = scanner.get_scanner_config()
        assert "patterns" in config
        assert isinstance(config["patterns"], list)
        assert len(config["patterns"]) == config["pattern_count"]

    def test_pii_only_scanner_has_fewer_patterns(self):
        full = RedactionScanner(scan_pii=True, scan_secrets=True)
        pii_only = RedactionScanner(scan_pii=True, scan_secrets=False)
        assert pii_only.get_scanner_config()["pattern_count"] < full.get_scanner_config()["pattern_count"]

    def test_default_context_window_is_50(self):
        scanner = RedactionScanner()
        assert scanner.get_scanner_config()["context_window"] == 50


# ---------------------------------------------------------------------------
# PATTERN_PRIORITY export
# ---------------------------------------------------------------------------

class TestPatternPriority:
    def test_priority_dict_exported(self):
        assert isinstance(PATTERN_PRIORITY, dict)
        assert "credit_card" in PATTERN_PRIORITY
        assert "email_address" in PATTERN_PRIORITY

    def test_credit_card_higher_than_email(self):
        assert PATTERN_PRIORITY["credit_card"] > PATTERN_PRIORITY["email_address"]
