"""
RedactionScanner — detect secrets, PII, and credentials in text content.

Scans text blobs for patterns that must be redacted before training
data promotion.  All detection is local (regex + heuristics).  No
network calls, no external APIs — local-first guarantee.

Emits redaction.required or redaction.clean chain entries.
Fail-closed: scanner errors are raised, not silently swallowed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

from harvest_core.control.exceptions import HarvestError


# ---------------------------------------------------------------------------
# Pattern priority levels — higher means more specific / higher confidence.
# Used by resolve_overlaps to prefer more-specific matches.
# ---------------------------------------------------------------------------

PATTERN_PRIORITY: dict[str, int] = {
    "credit_card": 10,
    "us_ssn": 10,
    "aws_access_key": 9,
    "azure_connection_string": 9,
    "jwt_token": 8,
    "us_phone": 7,
    "email_address": 6,  # lower priority due to false positives
    "ipv4_public": 5,
}


# ---------------------------------------------------------------------------
# Detection patterns
# ---------------------------------------------------------------------------

_PATTERNS: dict[str, re.Pattern] = {
    # Credentials and secrets
    "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}", re.ASCII),
    "aws_secret_key": re.compile(r"(?i)aws.{0,20}secret.{0,20}['\"][0-9a-zA-Z/+]{40}['\"]"),
    "azure_connection_string": re.compile(
        r"DefaultEndpointsProtocol=https;AccountName=[^;]{1,63};AccountKey=[A-Za-z0-9+/]{86}==;[^\s\"']{0,200}"
    ),
    "github_token": re.compile(r"ghp_[0-9a-zA-Z]{36}", re.ASCII),
    "github_oauth": re.compile(r"gho_[0-9a-zA-Z]{36}", re.ASCII),
    "slack_token": re.compile(r"xox[baprs]-[0-9a-zA-Z\-]+"),
    "generic_api_key": re.compile(
        r"(?i)(api[_\-]?key|apikey|api[_\-]?secret)['\"\s:=]+[0-9a-zA-Z\-_]{16,64}"
    ),
    "generic_secret": re.compile(
        r"(?i)(secret|password|passwd|pwd|token)['\"\s:=]+[^\s'\",;]{8,128}"
    ),
    "bearer_token": re.compile(r"(?i)bearer\s+[0-9a-zA-Z\-._~+/]+=*"),
    "private_key_header": re.compile(r"-----BEGIN\s+(RSA|EC|DSA|OPENSSH|PGP)\s+PRIVATE KEY-----"),
    "jwt_token": re.compile(r"eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+"),
    "anthropic_api_key": re.compile(r"sk-ant-[a-zA-Z0-9\-_]{20,}"),
    "openai_api_key": re.compile(r"sk-[a-zA-Z0-9]{20,}"),
    "stripe_key": re.compile(r"(?:sk|pk)_(?:live|test)_[a-zA-Z0-9]{24,}"),
    "google_api_key": re.compile(r"AIza[0-9A-Za-z\-_]{35}"),
    "npm_token": re.compile(r"npm_[a-zA-Z0-9]{36}"),
    "discord_token": re.compile(r"[MN][a-zA-Z0-9]{23}\.[a-zA-Z0-9\-_]{6}\.[a-zA-Z0-9\-_]{27}"),

    # Crypto / blockchain secrets
    "bitcoin_address": re.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b"),
    "ethereum_address": re.compile(r"\b0x[a-fA-F0-9]{40}\b"),

    # PII
    # RFC 5322-compliant email — negative lookbehind prevents matching
    # version-string patterns like "1.0@2.0" where the local-part is pure digits.
    "email_address": re.compile(
        r"(?<!\d)(?<!\d\.)[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,253}\.[a-zA-Z]{2,63}(?!\d)"
    ),
    "us_phone": re.compile(
        r"\b(?:\+1[\s\-]?)?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4}\b"
    ),
    "us_ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "credit_card": re.compile(
        r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6011[0-9]{12})\b"
    ),
    # Public IPv4 only — private ranges (10.x, 172.16-31.x, 192.168.x) excluded via negative lookahead
    "ipv4_public": re.compile(
        r"\b(?!10\.\d{1,3}\.\d{1,3}\.\d{1,3}\b)"
        r"(?!172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}\b)"
        r"(?!192\.168\.\d{1,3}\.\d{1,3}\b)"
        r"(?!127\.0\.0\.1\b)"
        r"(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    ),
    "ip_address_private": re.compile(
        r"\b(10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})\b"
    ),
    "mac_address": re.compile(r"\b([0-9A-Fa-f]{2}[:\-]){5}([0-9A-Fa-f]{2})\b"),
    "lat_long_coordinates": re.compile(
        r"-?\d{1,3}\.\d{4,},\s*-?\d{1,3}\.\d{4,}"
    ),
    "uk_nino": re.compile(r"\b[A-Z]{2}\s?\d{2}\s?\d{2}\s?\d{2}\s?[A-D]\b"),
    "iban": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}(?:[A-Z0-9]?){0,16}\b"),
    "passport_number": re.compile(r"\b[A-Z]{1,2}\d{6,9}\b"),
    "us_drivers_license": re.compile(r"\b[A-Z]\d{7}\b"),
    "date_of_birth": re.compile(r"(?i)\b(?:dob|date\s+of\s+birth)[:\s]+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b"),
    "medical_record": re.compile(r"(?i)\bMRN[:\s]+\d{5,10}\b"),
    "vehicle_vin": re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b"),
}

# PII patterns that need review but don't always require redaction
_PII_PATTERNS = {
    "email_address", "us_phone", "us_ssn", "credit_card",
    "uk_nino", "iban", "passport_number", "date_of_birth", "medical_record", "vehicle_vin",
    "ipv4_public", "mac_address", "lat_long_coordinates", "us_drivers_license",
    # ip_address_private is informational — private IPs are not redacted by default
}
# Credential patterns that always require redaction
_SECRET_PATTERNS = set(_PATTERNS.keys()) - _PII_PATTERNS - {"ip_address_private"}


@dataclass
class Finding:
    pattern_name: str
    category: str  # "secret" | "pii"
    match_start: int
    match_end: int
    excerpt: str  # first 40 chars, redacted in output


@dataclass
class ScanResult:
    text_length: int
    findings: List[Finding] = field(default_factory=list)
    redaction_required: bool = False
    secret_count: int = 0
    pii_count: int = 0

    @property
    def is_clean(self) -> bool:
        return not self.redaction_required

    def summary(self) -> str:
        if self.is_clean:
            return f"clean — no findings in {self.text_length} chars"
        return (
            f"redaction_required — {self.secret_count} secret(s), "
            f"{self.pii_count} PII hit(s) in {self.text_length} chars"
        )


class RedactionScanner:
    """
    Scan text content for secrets and PII.

    Usage:
        scanner = RedactionScanner()
        result = scanner.scan(text)
        if result.redaction_required:
            redacted = scanner.redact(text)
    """

    def __init__(
        self,
        scan_pii: bool = True,
        scan_secrets: bool = True,
        context_window: int = 50,
    ):
        self.scan_pii = scan_pii
        self.scan_secrets = scan_secrets
        self.context_window = context_window  # chars before/after match to include in report
        active_patterns: dict[str, re.Pattern] = {}
        if scan_secrets:
            active_patterns.update({k: _PATTERNS[k] for k in _SECRET_PATTERNS})
        if scan_pii:
            active_patterns.update({k: _PATTERNS[k] for k in _PII_PATTERNS})
        self._active = active_patterns

    def scan(self, text: str) -> ScanResult:
        """
        Scan text for secrets and PII.  Returns ScanResult with all findings.
        Never raises — all detection errors are treated as 'clean' for that pattern.
        """
        if not isinstance(text, str):
            raise HarvestError(f"RedactionScanner.scan() expects str, got {type(text).__name__}")

        result = ScanResult(text_length=len(text))

        for name, pattern in self._active.items():
            try:
                for m in pattern.finditer(text):
                    raw = m.group()
                    excerpt = (raw[:37] + "...") if len(raw) > 40 else raw
                    category = "secret" if name in _SECRET_PATTERNS else "pii"
                    result.findings.append(Finding(
                        pattern_name=name,
                        category=category,
                        match_start=m.start(),
                        match_end=m.end(),
                        excerpt=excerpt,
                    ))
            except Exception:
                pass  # pattern match errors are non-fatal

        result.secret_count = sum(1 for f in result.findings if f.category == "secret")
        result.pii_count = sum(1 for f in result.findings if f.category == "pii")
        result.redaction_required = result.secret_count > 0 or result.pii_count > 0
        return result

    def redact(self, text: str, replacement: str = "[REDACTED]") -> str:
        """
        Return a copy of text with all findings replaced by the replacement string.
        Replacements applied right-to-left so offsets remain valid.
        """
        scan = self.scan(text)
        if not scan.findings:
            return text

        # sort descending by start so right-to-left replacement is safe
        findings = sorted(scan.findings, key=lambda f: f.match_start, reverse=True)
        chars = list(text)
        for f in findings:
            chars[f.match_start:f.match_end] = list(replacement)
        return "".join(chars)

    def scan_with_context(self, text: str) -> list:
        """Return matches with surrounding context for human review.

        Each entry is a dict with all Finding fields plus ``context_before``
        and ``context_after`` — the ``context_window`` characters on either
        side of the match.
        """
        scan = self.scan(text)
        result = []
        for f in scan.findings:
            start = max(0, f.match_start - self.context_window)
            end = min(len(text), f.match_end + self.context_window)
            result.append({
                "pattern_name": f.pattern_name,
                "category": f.category,
                "start": f.match_start,
                "end": f.match_end,
                "matched_text": f.excerpt,
                "context_before": text[start:f.match_start],
                "context_after": text[f.match_end:end],
            })
        return result

    def resolve_overlaps(self, matches: list) -> list:
        """Given overlapping matches, keep the most specific (longest span).

        Accepts a list of dicts as produced by ``scan_with_context``.
        When spans overlap, the entry with the larger span wins; ties are
        broken by higher ``PATTERN_PRIORITY``.
        """
        if not matches:
            return matches

        def _priority(m: dict) -> int:
            return PATTERN_PRIORITY.get(m.get("pattern_name", ""), 0)

        # Sort by start, then by descending span length, then by descending priority
        sorted_m = sorted(
            matches,
            key=lambda m: (
                m.get("start", 0),
                -(m.get("end", m.get("start", 0)) - m.get("start", 0)),
                -_priority(m),
            ),
        )
        resolved: list = []
        last_end: int = -1
        for m in sorted_m:
            start = m.get("start", 0)
            end = m.get("end", start + len(m.get("matched_text", "")))
            if start >= last_end:
                resolved.append(m)
                last_end = end
            elif end > last_end:
                # Current extends further — replace last entry
                resolved[-1] = m
                last_end = end
        return resolved

    def get_scanner_config(self) -> dict:
        """Return the current scanner configuration as a dict."""
        return {
            "pattern_count": len(self._active),
            "context_window": self.context_window,
            "patterns": sorted(self._active.keys()),
        }

    def scan_file(self, path) -> ScanResult:
        """Read a text file and scan its contents."""
        from pathlib import Path
        p = Path(path)
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            raise HarvestError(f"RedactionScanner cannot read {path}: {e}") from e
        return self.scan(text)
