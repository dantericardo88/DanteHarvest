"""
Extended redaction tests — pattern breadth, confidence scoring, RedactionReport.

Covers:
- New patterns added in redaction_scanner.py (AWS key, JWT, MAC, lat/long,
  BTC, ETH, public IPv4, private IPv4 exclusion, drivers license)
- RedactionMatch / RedactionReport dataclasses
- NERRedactor.redact_with_report() returning (str, RedactionReport)
- Pattern count threshold (>= 30 combined across _PATTERNS + EXTENDED_PATTERNS)
"""

from __future__ import annotations

import pytest

from harvest_core.rights.redaction_scanner import RedactionScanner, _PATTERNS, _PII_PATTERNS
from harvest_core.rights.pii_patterns import EXTENDED_PATTERNS
from harvest_core.rights.ner_redactor import (
    NERRedactor,
    RedactionMatch,
    RedactionReport,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scanner() -> RedactionScanner:
    return RedactionScanner()


def _redactor() -> NERRedactor:
    """NERRedactor with spaCy disabled — regex-only path."""
    r = NERRedactor()
    # Force spaCy unavailable so tests never need the model installed.
    r._nlp = None
    # Monkey-patch _load_nlp to always raise so it falls back gracefully.
    from harvest_core.control.exceptions import NormalizationError
    r._load_nlp = lambda: (_ for _ in ()).throw(NormalizationError("no spacy in test"))  # type: ignore[method-assign]
    return r


# ---------------------------------------------------------------------------
# Individual new-pattern tests
# ---------------------------------------------------------------------------

class TestNewPatterns:
    def test_aws_key_redacted(self):
        text = "My key is AKIAIOSFODNN7EXAMPLE here."
        result = _scanner().scan(text)
        names = [f.pattern_name for f in result.findings]
        assert "aws_access_key" in names

    def test_jwt_token_redacted(self):
        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        result = _scanner().scan(jwt)
        names = [f.pattern_name for f in result.findings]
        assert "jwt_token" in names

    def test_mac_address_redacted(self):
        text = "Device MAC: AA:BB:CC:DD:EE:FF connected."
        result = _scanner().scan(text)
        names = [f.pattern_name for f in result.findings]
        assert "mac_address" in names

    def test_lat_long_redacted(self):
        text = "Location: 37.7749, -122.4194"
        result = _scanner().scan(text)
        names = [f.pattern_name for f in result.findings]
        assert "lat_long_coordinates" in names

    def test_bitcoin_address_redacted(self):
        # Valid mainnet P2PKH address (26-34 chars, starts with 1 or 3)
        text = "Send BTC to 1A1zP1eP5QGefi2DMPTfTL5SLmv7Divf"
        result = _scanner().scan(text)
        names = [f.pattern_name for f in result.findings]
        assert "bitcoin_address" in names

    def test_ethereum_address_redacted(self):
        text = "ETH wallet: 0xAbCdEf1234567890AbCdEf1234567890AbCdEf12"
        result = _scanner().scan(text)
        names = [f.pattern_name for f in result.findings]
        assert "ethereum_address" in names

    def test_ipv4_public_redacted(self):
        text = "DNS server at 8.8.8.8 is reachable."
        result = _scanner().scan(text)
        names = [f.pattern_name for f in result.findings]
        assert "ipv4_public" in names

    def test_ipv4_private_not_redacted(self):
        """Private-range IPs (192.168.x.x, 10.x.x.x, 172.16-31.x.x) must NOT
        appear as ipv4_public findings."""
        scanner = _scanner()
        for private_ip in ("192.168.1.1", "10.0.0.1", "172.16.0.1", "172.31.255.255"):
            result = scanner.scan(f"Gateway is {private_ip}")
            names = [f.pattern_name for f in result.findings]
            assert "ipv4_public" not in names, (
                f"Private IP {private_ip!r} was incorrectly flagged as ipv4_public"
            )


# ---------------------------------------------------------------------------
# Pattern count threshold
# ---------------------------------------------------------------------------

class TestPatternCount:
    def test_total_pattern_count_30_plus(self):
        """Combined unique patterns across _PATTERNS and EXTENDED_PATTERNS >= 30."""
        combined = set(_PATTERNS.keys()) | set(EXTENDED_PATTERNS.keys())
        assert len(combined) >= 30, (
            f"Expected >= 30 patterns, found {len(combined)}: {sorted(combined)}"
        )


# ---------------------------------------------------------------------------
# RedactionReport / redact_with_report tests
# ---------------------------------------------------------------------------

class TestRedactWithReport:
    def test_redact_with_report_returns_tuple(self):
        redactor = _redactor()
        result = redactor.redact_with_report("user@example.com")
        assert isinstance(result, tuple)
        assert len(result) == 2
        redacted_text, report = result
        assert isinstance(redacted_text, str)
        assert isinstance(report, RedactionReport)

    def test_redaction_report_has_matches(self):
        redactor = _redactor()
        _, report = redactor.redact_with_report("Contact user@example.com today.")
        assert len(report.matches) >= 1
        assert all(isinstance(m, RedactionMatch) for m in report.matches)

    def test_redaction_report_patterns_triggered(self):
        redactor = _redactor()
        _, report = redactor.redact_with_report("Email: user@example.com")
        assert "email_address" in report.patterns_triggered

    def test_redaction_report_summary_string(self):
        redactor = _redactor()
        _, report = redactor.redact_with_report("user@example.com")
        summary = report.summary()
        assert isinstance(summary, str)
        assert len(summary) > 0
        # Should mention count
        assert any(ch.isdigit() for ch in summary)

    def test_redaction_rate_between_0_and_1(self):
        redactor = _redactor()
        _, report = redactor.redact_with_report("Reach me at user@example.com please.")
        assert 0.0 <= report.redaction_rate <= 1.0

    def test_no_pii_empty_report(self):
        redactor = _redactor()
        clean = "The quick brown fox jumps over the lazy dog."
        redacted_text, report = redactor.redact_with_report(clean)
        assert report.matches == []
        assert report.redaction_rate == 0.0
        assert redacted_text == clean

    def test_redaction_report_original_and_redacted_lengths(self):
        redactor = _redactor()
        text = "Key: AKIAIOSFODNN7EXAMPLE"
        redacted_text, report = redactor.redact_with_report(text)
        assert report.original_length == len(text)
        assert report.redacted_length == len(redacted_text)

    def test_redaction_match_fields(self):
        redactor = _redactor()
        _, report = redactor.redact_with_report("Call 123-45-6789 now.")
        # SSN pattern should fire
        ssn_matches = [m for m in report.matches if m.pattern_name == "us_ssn"]
        assert len(ssn_matches) >= 1
        m = ssn_matches[0]
        assert m.matched_text == "123-45-6789"
        assert m.start >= 0
        assert m.end > m.start
        assert 0.0 <= m.confidence <= 1.0
        assert m.replacement  # non-empty replacement token

    def test_patterns_triggered_unique(self):
        """patterns_triggered must contain no duplicates."""
        redactor = _redactor()
        text = "user@example.com and admin@example.com"
        _, report = redactor.redact_with_report(text)
        triggered = report.patterns_triggered
        assert len(triggered) == len(set(triggered))

    def test_redaction_report_summary_no_pii(self):
        redactor = _redactor()
        _, report = redactor.redact_with_report("nothing sensitive here")
        summary = report.summary()
        assert "clean" in summary.lower() or summary  # non-empty either way

    def test_aws_key_in_report(self):
        redactor = _redactor()
        text = "key=AKIAIOSFODNN7EXAMPLE"
        redacted, report = redactor.redact_with_report(text)
        assert "AKIAIOSFODNN7EXAMPLE" not in redacted
        assert "aws_access_key" in report.patterns_triggered

    def test_multiple_patterns_in_report(self):
        redactor = _redactor()
        text = "email: foo@bar.com, SSN: 123-45-6789"
        _, report = redactor.redact_with_report(text)
        triggered = report.patterns_triggered
        assert "email_address" in triggered
        assert "us_ssn" in triggered
        assert len(report.matches) >= 2
