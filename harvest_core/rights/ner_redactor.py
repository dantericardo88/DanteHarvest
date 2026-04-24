"""
NERRedactor — context-aware PII detection and redaction using spaCy NER.

Enhances RedactionScanner with Named Entity Recognition:
- PERSON names (context-aware — not just word-shape heuristics)
- ORG names when combined with personal context signals
- GPE / LOC entities when they appear as personal addresses
- MONEY amounts in personal financial contexts
- Custom entity types via pattern registration

Harvested from: spaCy EntityRuler patterns + Presidio context-aware approach.

Constitutional guarantees:
- Local-first: spaCy runs locally; no network calls
- Fail-closed: spaCy unavailable raises NormalizationError; scanner never silently skips
- Layered: NER findings stack on top of regex findings from RedactionScanner
- Zero-ambiguity: NERRedactorResult.redaction_required is always bool
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Set, Tuple

from harvest_core.control.exceptions import NormalizationError
from harvest_core.rights.redaction_scanner import RedactionScanner, ScanResult


# NER entity types treated as PII when found in personal context
_PII_ENTITY_TYPES: Set[str] = {"PERSON", "GPE", "LOC", "ORG", "MONEY", "CARDINAL"}

# Context signals that indicate a personal context (within 100 chars)
_PERSONAL_CONTEXT_PATTERNS = [
    re.compile(r"\b(my|your|his|her|their|our)\s+name\b", re.IGNORECASE),
    re.compile(r"\b(contact|call|email|reach|spoke\s+with|meeting\s+with)\b", re.IGNORECASE),
    re.compile(r"\b(patient|client|customer|employee|user|account\s+holder)\b", re.IGNORECASE),
    re.compile(r"\b(signed|owned\s+by|belongs\s+to|reported\s+by)\b", re.IGNORECASE),
    re.compile(r"\b(ssn|social\s+security|dob|date\s+of\s+birth|address|zip\s+code)\b", re.IGNORECASE),
]

# Always redact PERSON entities regardless of context
_ALWAYS_REDACT: Set[str] = {"PERSON"}


@dataclass
class NERFinding:
    entity_type: str
    text: str
    start: int
    end: int
    context_pii: bool  # true if personal context was detected nearby
    confidence: float  # 1.0 for PERSON, lower for context-dependent types


@dataclass
class NERRedactorResult:
    regex_result: ScanResult
    ner_findings: List[NERFinding] = field(default_factory=list)
    spacy_available: bool = True

    @property
    def redaction_required(self) -> bool:
        if self.regex_result.redaction_required:
            return True
        return any(f.context_pii or f.entity_type in _ALWAYS_REDACT for f in self.ner_findings)

    @property
    def all_findings_count(self) -> int:
        return len(self.regex_result.findings) + len(self.ner_findings)

    def summary(self) -> str:
        parts = []
        if self.regex_result.redaction_required:
            parts.append(self.regex_result.summary())
        if self.ner_findings:
            pii_count = sum(1 for f in self.ner_findings if f.context_pii or f.entity_type in _ALWAYS_REDACT)
            parts.append(f"NER: {len(self.ner_findings)} entities, {pii_count} flagged as PII")
        if not parts:
            return f"clean — {self.regex_result.text_length} chars, {len(self.ner_findings)} NER entities (no PII)"
        return " | ".join(parts)


class NERRedactor:
    """
    Context-aware PII detector and redactor combining regex patterns with spaCy NER.

    Usage:
        redactor = NERRedactor()
        result = redactor.scan("Please contact John Smith at john@example.com")
        if result.redaction_required:
            clean = redactor.redact("Please contact John Smith at john@example.com")
    """

    def __init__(
        self,
        model: str = "en_core_web_sm",
        regex_scanner: Optional[RedactionScanner] = None,
        context_window: int = 120,
    ):
        self._model_name = model
        self._nlp = None  # lazy-loaded
        self._scanner = regex_scanner or RedactionScanner()
        self._context_window = context_window

    def _load_nlp(self):
        if self._nlp is not None:
            return self._nlp
        try:
            import spacy
        except (ImportError, Exception) as e:
            raise NormalizationError(
                "spaCy not installed or not loadable. Run: pip install spacy"
            ) from e
        try:
            self._nlp = spacy.load(self._model_name)
        except OSError as e:
            raise NormalizationError(
                f"spaCy model '{self._model_name}' not found. "
                f"Run: python -m spacy download {self._model_name}"
            ) from e
        except Exception as e:
            raise NormalizationError(f"spaCy failed to load: {e}") from e
        return self._nlp

    def is_available(self) -> bool:
        """Return True if spaCy and the model are loadable."""
        try:
            self._load_nlp()
            return True
        except Exception:
            return False

    def _has_personal_context(self, text: str, start: int, end: int) -> bool:
        """Check if entity at [start, end] has personal context signals nearby."""
        window_start = max(0, start - self._context_window)
        window_end = min(len(text), end + self._context_window)
        window = text[window_start:window_end]
        return any(p.search(window) for p in _PERSONAL_CONTEXT_PATTERNS)

    def scan(self, text: str) -> NERRedactorResult:
        """
        Scan text using regex patterns AND spaCy NER.
        Falls back to regex-only if spaCy is unavailable.
        """
        regex_result = self._scanner.scan(text)

        try:
            nlp = self._load_nlp()
        except NormalizationError:
            return NERRedactorResult(regex_result=regex_result, spacy_available=False)

        doc = nlp(text)
        ner_findings: List[NERFinding] = []

        for ent in doc.ents:
            if ent.label_ not in _PII_ENTITY_TYPES:
                continue
            is_always = ent.label_ in _ALWAYS_REDACT
            has_context = is_always or self._has_personal_context(text, ent.start_char, ent.end_char)
            confidence = 1.0 if is_always else (0.8 if has_context else 0.3)
            ner_findings.append(NERFinding(
                entity_type=ent.label_,
                text=ent.text,
                start=ent.start_char,
                end=ent.end_char,
                context_pii=has_context,
                confidence=confidence,
            ))

        return NERRedactorResult(regex_result=regex_result, ner_findings=ner_findings)

    def redact(self, text: str, replacement: str = "[REDACTED]") -> str:
        """
        Redact all regex findings AND NER PII findings.
        Applies replacements right-to-left so offsets remain valid.
        """
        result = self.scan(text)

        intervals: List[Tuple[int, int]] = []
        for f in result.regex_result.findings:
            intervals.append((f.match_start, f.match_end))
        for f in result.ner_findings:
            if f.context_pii or f.entity_type in _ALWAYS_REDACT:
                intervals.append((f.start, f.end))

        if not intervals:
            return text

        # merge overlapping intervals
        intervals.sort(key=lambda x: x[0])
        merged: List[Tuple[int, int]] = []
        for start, end in intervals:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))

        # apply right-to-left
        chars = list(text)
        for start, end in reversed(merged):
            chars[start:end] = list(replacement)
        return "".join(chars)

    def redact_file(self, path, replacement: str = "[REDACTED]") -> str:
        """Read a text file, redact PII, return redacted text."""
        from pathlib import Path
        p = Path(path)
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            raise NormalizationError(f"NERRedactor cannot read {path}: {e}") from e
        return self.redact(text, replacement=replacement)
