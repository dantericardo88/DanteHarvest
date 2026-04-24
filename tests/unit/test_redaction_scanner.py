"""Unit tests for RedactionScanner."""

import pytest
from harvest_core.rights.redaction_scanner import RedactionScanner, ScanResult


class TestRedactionScanner:
    def setup_method(self):
        self.scanner = RedactionScanner()

    def test_clean_text_has_no_findings(self):
        result = self.scanner.scan("Hello, world. This is clean text.")
        assert result.is_clean
        assert result.findings == []
        assert result.secret_count == 0
        assert result.pii_count == 0

    def test_detects_aws_access_key(self):
        text = "export AWS_KEY=AKIAIOSFODNN7EXAMPLE and more"
        result = self.scanner.scan(text)
        assert not result.is_clean
        names = [f.pattern_name for f in result.findings]
        assert "aws_access_key" in names

    def test_detects_github_token(self):
        text = "token = ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh12"
        result = self.scanner.scan(text)
        assert not result.is_clean

    def test_detects_email(self):
        text = "Contact john.doe@example.com for details"
        result = self.scanner.scan(text)
        assert not result.is_clean
        names = [f.pattern_name for f in result.findings]
        assert "email_address" in names
        assert result.pii_count >= 1

    def test_detects_ssn(self):
        text = "SSN: 123-45-6789"
        result = self.scanner.scan(text)
        assert not result.is_clean
        names = [f.pattern_name for f in result.findings]
        assert "us_ssn" in names

    def test_detects_private_key_header(self):
        text = "-----BEGIN RSA PRIVATE KEY-----\nMIIEo..."
        result = self.scanner.scan(text)
        assert not result.is_clean
        names = [f.pattern_name for f in result.findings]
        assert "private_key_header" in names

    def test_redact_replaces_findings(self):
        text = "Contact admin@secret.com for the AWS key AKIAIOSFODNN7EXAMPLE"
        redacted = self.scanner.redact(text)
        assert "admin@secret.com" not in redacted
        assert "AKIAIOSFODNN7EXAMPLE" not in redacted
        assert "[REDACTED]" in redacted

    def test_redact_clean_text_unchanged(self):
        text = "No sensitive data here."
        assert self.scanner.redact(text) == text

    def test_scan_raises_on_non_string(self):
        from harvest_core.control.exceptions import HarvestError
        with pytest.raises(HarvestError):
            self.scanner.scan(12345)

    def test_scan_secrets_only_mode(self):
        scanner = RedactionScanner(scan_pii=False, scan_secrets=True)
        email_only = "user@example.com"
        result = scanner.scan(email_only)
        assert result.is_clean  # PII disabled, email not flagged as secret

    def test_summary_reports_correctly(self):
        text = "user@example.com and ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh12"
        result = self.scanner.scan(text)
        summary = result.summary()
        assert "redaction_required" in summary
