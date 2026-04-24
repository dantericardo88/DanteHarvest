"""
pii_patterns — enriched PII and credential detection patterns.

Extends RedactionScanner._PATTERNS with:
- International phone variants (+44, +91, +33, etc.)
- EIN / Tax ID (US Federal Employer Identification Number)
- Passport numbers (US, UK, generic)
- IBAN bank account numbers
- IPv6 addresses
- Driver's license patterns (US generic)
- Date of birth patterns
- Medicare / Medicaid beneficiary IDs
- Vehicle Identification Numbers (VIN)
- Discord tokens, Stripe keys, Twilio SIDs (common dev secrets)
- Hex color lookalike false-positive suppression built into tests

Constitutional guarantee: these patterns are additive — they never replace the
base _PATTERNS in redaction_scanner.py. Import this module and call
register_extended_patterns() to activate.
"""

from __future__ import annotations

import re
from typing import Dict


EXTENDED_PATTERNS: Dict[str, re.Pattern] = {
    # ------------------------------------------------------------------
    # PII — identity documents
    # ------------------------------------------------------------------
    "us_ein": re.compile(
        r"\b\d{2}-\d{7}\b"
    ),
    "us_passport": re.compile(
        r"\b[A-Z]{1,2}[0-9]{6,9}\b"
    ),
    "us_drivers_license": re.compile(
        r"\b[A-Z]\d{7}\b|\b\d{9}\b"  # common US DL formats
    ),
    "us_medicare_id": re.compile(
        r"\b[1-9][A-Z]{2}\d[A-Z]{2}\d[A-Z]{2}\d\b"  # MBI format
    ),
    "vin": re.compile(
        r"\b[A-HJ-NPR-Z0-9]{17}\b"
    ),
    "date_of_birth": re.compile(
        r"\b(?:dob|date[\s_-]?of[\s_-]?birth|born[\s_-]?on)[\s:]+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        re.IGNORECASE,
    ),

    # ------------------------------------------------------------------
    # PII — financial
    # ------------------------------------------------------------------
    "iban": re.compile(
        r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7,19}\b"
    ),
    "amex_card": re.compile(
        r"\b3[47][0-9]{13}\b"
    ),
    "discover_card": re.compile(
        r"\b6(?:011|5[0-9]{2})[0-9]{12}\b"
    ),
    "swift_bic": re.compile(
        r"\b[A-Z]{6}[A-Z2-9][A-NP-Z0-9](?:[A-Z0-9]{3})?\b"
    ),

    # ------------------------------------------------------------------
    # PII — contact
    # ------------------------------------------------------------------
    "intl_phone": re.compile(
        r"\+(?:1|7|20|27|30|31|32|33|34|36|39|40|41|43|44|45|46|47|48|49|51|52|53|54|55|56|57|58|60|61|62|63|64|65|66|81|82|84|86|90|91|92|93|94|95|98)"
        r"[\s\-]?(?:\(?\d{1,4}\)?[\s\-]?)?\d{3,4}[\s\-]?\d{3,4}(?:[\s\-]?\d{1,4})?\b"
    ),
    "email_with_tag": re.compile(
        r"\b[a-zA-Z0-9._%+\-]+\+[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
    ),
    "ipv6_address": re.compile(
        r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b"
        r"|\b(?:[0-9a-fA-F]{1,4}:){1,7}:\b"
        r"|\b::(?:[0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}\b"
    ),

    # ------------------------------------------------------------------
    # Secrets — developer credentials
    # ------------------------------------------------------------------
    "stripe_key": re.compile(
        r"\b(?:sk|pk)_(?:live|test)_[0-9a-zA-Z]{24,}\b"
    ),
    "twilio_sid": re.compile(
        r"\bAC[0-9a-fA-F]{32}\b"
    ),
    "twilio_auth_token": re.compile(
        r"\b[0-9a-fA-F]{32}\b"
    ),
    "discord_token": re.compile(
        r"\b[MN][A-Za-z\d]{23}\.[\w-]{6}\.[\w-]{27}\b"
    ),
    "sendgrid_key": re.compile(
        r"\bSG\.[a-zA-Z0-9\-_]{22}\.[a-zA-Z0-9\-_]{43}\b"
    ),
    "npm_token": re.compile(
        r"\bnpm_[a-zA-Z0-9]{36}\b"
    ),
    "google_api_key": re.compile(
        r"\bAIza[0-9A-Za-z\-_]{35}\b"
    ),
    "heroku_api_key": re.compile(
        r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
    ),
}

# Patterns that are always PII (need review before promotion)
EXTENDED_PII_PATTERNS = {
    "us_ein", "us_passport", "us_drivers_license", "us_medicare_id", "vin",
    "date_of_birth", "iban", "amex_card", "discover_card", "swift_bic",
    "intl_phone", "email_with_tag", "ipv6_address",
}

# Patterns that are always secrets (must be redacted before any promotion)
EXTENDED_SECRET_PATTERNS = {
    "stripe_key", "twilio_sid", "discord_token", "sendgrid_key",
    "npm_token", "google_api_key", "heroku_api_key",
    # twilio_auth_token is intentionally excluded from auto-secrets —
    # 32-char hex strings are too common (MD5 hashes, etc.)
}


def register_extended_patterns() -> None:
    """
    Register extended PII and credential patterns into RedactionScanner.

    Call once at startup (e.g., in cli.py main() or server startup).
    Idempotent — safe to call multiple times.
    """
    from harvest_core.rights.redaction_scanner import _PATTERNS, _PII_PATTERNS, _SECRET_PATTERNS

    for name, pattern in EXTENDED_PATTERNS.items():
        if name not in _PATTERNS:
            _PATTERNS[name] = pattern

    for name in EXTENDED_PII_PATTERNS:
        _PII_PATTERNS.add(name)

    for name in EXTENDED_SECRET_PATTERNS:
        _SECRET_PATTERNS.add(name)
