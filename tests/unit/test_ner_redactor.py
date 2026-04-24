"""Tests for NERRedactor — context-aware NER + regex PII detection."""

import sys
from unittest.mock import patch, MagicMock

from harvest_core.rights.ner_redactor import (
    NERRedactor,
    NERFinding,
    NERRedactorResult,
)
from harvest_core.rights.redaction_scanner import ScanResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_scan_result(redaction_required=False) -> ScanResult:
    result = ScanResult(text_length=100)
    result.redaction_required = redaction_required
    return result


def make_ner_result(ner_findings=None, regex_required=False) -> NERRedactorResult:
    return NERRedactorResult(
        regex_result=make_scan_result(redaction_required=regex_required),
        ner_findings=ner_findings or [],
    )


# ---------------------------------------------------------------------------
# NERRedactorResult logic
# ---------------------------------------------------------------------------

def test_result_redaction_required_from_regex():
    result = make_ner_result(regex_required=True)
    assert result.redaction_required is True


def test_result_redaction_required_from_person_entity():
    finding = NERFinding(
        entity_type="PERSON",
        text="John Smith",
        start=0,
        end=10,
        context_pii=True,
        confidence=1.0,
    )
    result = make_ner_result(ner_findings=[finding])
    assert result.redaction_required is True


def test_result_not_required_when_clean():
    result = make_ner_result()
    assert result.redaction_required is False


def test_result_all_findings_count():
    result = make_ner_result(ner_findings=[
        NERFinding("PERSON", "Alice", 0, 5, True, 1.0),
        NERFinding("ORG", "Acme", 10, 14, False, 0.3),
    ])
    assert result.all_findings_count == 2


def test_result_summary_clean():
    result = make_ner_result()
    assert "clean" in result.summary()


def test_result_summary_with_findings():
    finding = NERFinding("PERSON", "Bob", 0, 3, True, 1.0)
    result = make_ner_result(ner_findings=[finding])
    assert "NER" in result.summary()


# ---------------------------------------------------------------------------
# NERRedactor.is_available()
# ---------------------------------------------------------------------------

def test_is_available_returns_bool():
    redactor = NERRedactor()
    result = redactor.is_available()
    assert isinstance(result, bool)


def test_is_available_false_when_spacy_missing():
    redactor = NERRedactor()
    redactor._nlp = None
    with patch.dict(sys.modules, {"spacy": None}):
        result = redactor.is_available()
    assert result is False


# ---------------------------------------------------------------------------
# NERRedactor.scan() — spaCy mocked
# ---------------------------------------------------------------------------

def _make_mock_ent(label, text, start_char, end_char):
    ent = MagicMock()
    ent.label_ = label
    ent.text = text
    ent.start_char = start_char
    ent.end_char = end_char
    return ent


def test_scan_person_entity_flagged():
    redactor = NERRedactor()

    mock_ent = _make_mock_ent("PERSON", "Alice", 20, 25)
    mock_doc = MagicMock()
    mock_doc.ents = [mock_ent]
    mock_nlp = MagicMock(return_value=mock_doc)

    with patch.object(redactor, "_load_nlp", return_value=mock_nlp):
        text = "Please contact Alice about your account."
        result = redactor.scan(text)

    assert len(result.ner_findings) == 1
    assert result.ner_findings[0].entity_type == "PERSON"
    assert result.ner_findings[0].context_pii is True
    assert result.redaction_required is True


def test_scan_org_without_context_not_flagged():
    redactor = NERRedactor()

    mock_ent = _make_mock_ent("ORG", "NASA", 4, 8)
    mock_doc = MagicMock()
    mock_doc.ents = [mock_ent]
    mock_nlp = MagicMock(return_value=mock_doc)

    with patch.object(redactor, "_load_nlp", return_value=mock_nlp):
        text = "The NASA rocket launched successfully."
        result = redactor.scan(text)

    # ORG without personal context → context_pii=False → not redaction_required
    assert result.ner_findings[0].context_pii is False
    assert result.redaction_required is False


def test_scan_falls_back_to_regex_when_spacy_unavailable():
    from harvest_core.rights.ner_redactor import NormalizationError
    redactor = NERRedactor()

    with patch.object(redactor, "_load_nlp", side_effect=NormalizationError("no spacy")):
        result = redactor.scan("test text with nothing")

    assert result.spacy_available is False
    assert isinstance(result.redaction_required, bool)


# ---------------------------------------------------------------------------
# NERRedactor.redact()
# ---------------------------------------------------------------------------

def test_redact_replaces_person_entity():
    redactor = NERRedactor()

    mock_ent = _make_mock_ent("PERSON", "John Doe", 17, 25)
    mock_doc = MagicMock()
    mock_doc.ents = [mock_ent]
    mock_nlp = MagicMock(return_value=mock_doc)

    with patch.object(redactor, "_load_nlp", return_value=mock_nlp):
        text = "Please contact John Doe today."
        result = redactor.redact(text)

    assert "John Doe" not in result
    assert "[REDACTED]" in result


def test_redact_merges_overlapping_intervals():
    redactor = NERRedactor()

    # Two overlapping entities
    ent1 = _make_mock_ent("PERSON", "Jo", 8, 10)
    ent2 = _make_mock_ent("PERSON", "John Smith", 8, 18)
    mock_doc = MagicMock()
    mock_doc.ents = [ent1, ent2]
    mock_nlp = MagicMock(return_value=mock_doc)

    with patch.object(redactor, "_load_nlp", return_value=mock_nlp):
        text = "Contact John Smith now."
        result = redactor.redact(text)

    assert result.count("[REDACTED]") == 1  # merged, not doubled


def test_redact_no_findings_returns_unchanged():
    redactor = NERRedactor()

    mock_doc = MagicMock()
    mock_doc.ents = []
    mock_nlp = MagicMock(return_value=mock_doc)

    with patch.object(redactor, "_load_nlp", return_value=mock_nlp):
        text = "The quick brown fox."
        result = redactor.redact(text)

    assert result == text
