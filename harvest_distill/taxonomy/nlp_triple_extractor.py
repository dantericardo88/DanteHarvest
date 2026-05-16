"""
NLPTripleExtractor — pure-stdlib rule-based triple extraction from plain text.

Wave 7f: knowledge_graph_extraction — automated NLP pipeline (6→9).

Extracts (subject, predicate, object) triples from plain text and markdown
using regex patterns. Zero external dependencies — stdlib only (re, string).

Constitutional guarantees:
- Pure stdlib: no spaCy, NLTK, stanza, or any NLP library
- Fail-open: malformed text never raises; returns empty list
- Deterministic: same input always produces same output
- Provenance: every triple carries source_text for traceability
"""

from __future__ import annotations

import re
import string
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Predicate semantic groups for deduplication
# ---------------------------------------------------------------------------

_PREDICATE_GROUPS: Dict[str, Set[str]] = {
    "identity":   {"is_a", "is", "equals", "synonym_of"},
    "membership": {"part_of", "belongs_to", "member_of", "subset_of"},
    "creation":   {"created_by", "authored_by", "made_by", "built_by"},
    "location":   {"located_in", "found_in", "based_in"},
    "usage":      {"uses", "uses_tool", "leverages", "employs"},
    "possession": {"has", "contains", "includes", "owns"},
    "relation":   {"related_to", "associated_with", "connected_to"},
}


# ---------------------------------------------------------------------------
# Triple dataclass
# ---------------------------------------------------------------------------

@dataclass
class Triple:
    """A subject-predicate-object triple extracted from text."""
    subject: str
    predicate: str
    object_: str
    confidence: float = 1.0          # 0.0-1.0
    source_text: str = ""            # original sentence / fragment

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Triple):
            return NotImplemented
        return (
            self.subject.lower() == other.subject.lower()
            and self.predicate == other.predicate
            and self.object_.lower() == other.object_.lower()
        )

    def __hash__(self) -> int:
        return hash((self.subject.lower(), self.predicate, self.object_.lower()))


# ---------------------------------------------------------------------------
# NLPTripleExtractor
# ---------------------------------------------------------------------------

class NLPTripleExtractor:
    """
    Extracts (subject, predicate, object) triples from plain text.

    Pure stdlib implementation - no spaCy, NLTK, or other NLP dependencies.
    Uses rule-based patterns to extract common relationship types.

    Example::

        extractor = NLPTripleExtractor()
        triples = extractor.extract_triples("Python is a programming language.")
        # -> [Triple(subject='Python', predicate='is_a', object_='programming language', ...)]
    """

    # ------------------------------------------------------------------
    # Relation patterns (regex, predicate_label, confidence)
    # Each pattern has two capture groups: (subject, object)
    # Patterns are ordered from most specific to least specific.
    # ------------------------------------------------------------------
    _PATTERNS: List[Tuple[re.Pattern, str, float]] = []

    # Raw pattern specs - compiled once per instance
    _RAW_PATTERNS: List[Tuple[str, str, float]] = [
        # authored-by (most specific - before generic created_by)
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:was authored by|is authored by)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "authored_by",
            0.9,
        ),
        # created-by
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:was created by|is made by|is developed by|is built by)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "created_by",
            0.9,
        ),
        # located-in
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:is located in|is based in|is headquartered in|is found in)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "located_in",
            0.9,
        ),
        # subset-of / member-of (more specific than part_of)
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:is a subset of|is a member of|is member of)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "subset_of",
            0.88,
        ),
        # part-of
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:is part of|belongs to|is a component of)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "part_of",
            0.85,
        ),
        # leverages / employs (more specific than uses)
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:leverages|employs)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "leverages",
            0.82,
        ),
        # uses/requires
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:uses|requires|depends on|needs|utilizes)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "uses",
            0.8,
        ),
        # has/contains
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:has|contains|includes|provides)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "has",
            0.75,
        ),
        # is-a / type (after more specific patterns)
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+is\s+(?:a|an|the)\s+"
            r"([a-z][A-Za-z0-9]*(?:\s+[a-z][A-Za-z0-9]*)*)",
            "is_a",
            0.85,
        ),
        # related-to (via "X and Y are related")
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+and\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+are\s+related",
            "related_to",
            0.7,
        ),
        # associated-with / connected-to
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:is associated with|is connected to)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "associated_with",
            0.7,
        ),
        # synonym-of (via "also known as" / "aka")
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:also known as|aka|also called)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "synonym_of",
            0.9,
        ),
        # successor-of (precedes/follows)
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:succeeds|replaces|supersedes)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "succeeds",
            0.85,
        ),
        # member-of (standalone phrasing)
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:is a member of|is member of)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "member_of",
            0.85,
        ),
        # precedes / temporal ordering
        (
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
            r"\s+(?:precedes|is followed by|comes before)\s+"
            r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)",
            "precedes",
            0.8,
        ),
    ]

    def __init__(self) -> None:
        # Compile once per instance
        self._compiled: List[Tuple[re.Pattern, str, float]] = [
            (re.compile(pat, re.IGNORECASE), label, conf)
            for pat, label, conf in self._RAW_PATTERNS
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_triples(self, text: str) -> List[Triple]:
        """
        Extract (subject, predicate, object) triples from plain text.

        Splits text into sentences, applies all relation patterns, then
        deduplicates by (subject, predicate, object).

        Returns list of Triple objects with confidence and source_text.
        """
        if not text or not text.strip():
            return []

        results: List[Triple] = []
        seen: Set[Triple] = set()

        for sentence in self._split_sentences(text):
            sentence = sentence.strip()
            if not sentence:
                continue
            for pattern, predicate, confidence in self._compiled:
                for m in pattern.finditer(sentence):
                    subj = m.group(1).strip()
                    obj = m.group(2).strip()
                    if not subj or not obj or subj.lower() == obj.lower():
                        continue
                    t = Triple(
                        subject=subj,
                        predicate=predicate,
                        object_=obj,
                        confidence=confidence,
                        source_text=sentence,
                    )
                    if t not in seen:
                        seen.add(t)
                        results.append(t)

        return results

    def extract_entities(self, text: str) -> List[str]:
        """
        Extract named-entity candidates from text.

        Candidates are:
        - Capitalized multi-word phrases (Title Case runs)
        - Quoted terms ('term' or "term")
        - ALL_CAPS abbreviations (>= 2 chars)

        Returns a deduplicated list preserving first-seen order.
        """
        if not text or not text.strip():
            return []

        seen: Set[str] = set()
        results: List[str] = []

        def _add(ent: str) -> None:
            ent = ent.strip()
            if ent and ent.lower() not in seen:
                seen.add(ent.lower())
                results.append(ent)

        # 1. Capitalized multi-word phrases (2+ consecutive Title-cased tokens)
        cap_pattern = re.compile(
            r"\b([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)+)\b"
        )
        for m in cap_pattern.finditer(text):
            _add(m.group(1))

        # 2. Single capitalized word that looks like a proper noun
        single_cap = re.compile(r"(?<=[a-z,;]\s)([A-Z][a-z]{2,})\b")
        for m in single_cap.finditer(text):
            _add(m.group(1))

        # 3. Quoted terms (single or double quotes)
        quoted = re.compile(r"""["']([^"']{2,50})["']""")
        for m in quoted.finditer(text):
            _add(m.group(1))

        # 4. ALL-CAPS abbreviations (>= 2 uppercase letters)
        abbrev = re.compile(r"\b([A-Z]{2,})\b")
        for m in abbrev.finditer(text):
            _add(m.group(1))

        return results

    def deduplicate_triples(self, triples: List[Triple]) -> List[Triple]:
        """Remove triples where subject+object+predicate_group is already seen.

        Two triples are considered semantic duplicates if their subject and
        object are the same (case-insensitive) AND their predicate maps to the
        same semantic category in _PREDICATE_GROUPS.
        """
        seen: Set[Tuple[str, str, str]] = set()
        result: List[Triple] = []
        for t in triples:
            group = next(
                (g for g, preds in _PREDICATE_GROUPS.items() if t.predicate in preds),
                t.predicate,  # fallback: use predicate itself as its own group
            )
            key = (t.subject.lower(), group, t.object_.lower())
            if key not in seen:
                seen.add(key)
                result.append(t)
        return result

    def link_entities(self, triples: List[Triple]) -> Dict[str, List[Triple]]:
        """Build entity graph - map entity_name -> [triples_involving_entity].

        When the same string appears as both subject and object in different
        triples, all those triples are linked under the same key.
        """
        entity_map: Dict[str, List[Triple]] = {}
        for t in triples:
            for entity in [t.subject, t.object_]:
                entity_map.setdefault(entity, []).append(t)
        return entity_map

    def extract_from_markdown(self, markdown: str) -> List[Triple]:
        """
        Extract triples from markdown text.

        Additional extraction beyond plain-text patterns:
        - Headers as entity/section declarations
        - Table rows: 2-column tables as subject-object pairs
        - Definition lists: "Term: Definition" -> (Term, defined_as, Definition)
        - Bullet items: "- X is a Y" or "- X: Y"
        - Plain-text extraction on each non-special line

        Returns deduplicated list of Triple objects.
        """
        if not markdown or not markdown.strip():
            return []

        seen: Set[Triple] = set()
        results: List[Triple] = []

        def _add(t: Triple) -> None:
            if t not in seen:
                seen.add(t)
                results.append(t)

        lines = markdown.splitlines()
        current_header: Optional[str] = None

        for line in lines:
            line_stripped = line.strip()

            # --- Headers: emit as entity/section declaration ---
            header_m = re.match(r"^#{1,6}\s+(.+)$", line_stripped)
            if header_m:
                current_header = header_m.group(1).strip()
                _add(Triple(
                    subject=current_header,
                    predicate="is_section",
                    object_="document",
                    confidence=0.5,
                    source_text=line_stripped,
                ))
                continue

            # --- Table rows: 2-column tables as subject-object pairs ---
            table_m = re.match(r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|?\s*$", line_stripped)
            if table_m:
                col1 = table_m.group(1).strip()
                col2 = table_m.group(2).strip()
                # Skip separator rows like |---|---|
                if col1 and col2 and not re.match(r"^[-:]+$", col1) and not re.match(r"^[-:]+$", col2):
                    _add(Triple(
                        subject=col1,
                        predicate="related_to",
                        object_=col2,
                        confidence=0.65,
                        source_text=line_stripped,
                    ))
                continue

            # --- Definition list / key: value ---
            defn_m = re.match(r"^([A-Za-z][^:]{1,60}):\s+(.{2,})$", line_stripped)
            if defn_m:
                key = defn_m.group(1).strip()
                val = defn_m.group(2).strip()
                # Filter out obvious non-definition lines (URLs, timestamps, etc.)
                if not re.search(r"https?://|^\d{4}-\d{2}|^[0-9]", val):
                    t = Triple(
                        subject=key,
                        predicate="defined_as",
                        object_=val,
                        confidence=0.8,
                        source_text=line_stripped,
                    )
                    _add(t)
                    # If under a header, also add section membership
                    if current_header:
                        _add(Triple(
                            subject=key,
                            predicate="belongs_to_section",
                            object_=current_header,
                            confidence=0.6,
                            source_text=line_stripped,
                        ))
                continue

            # --- Bullet items: "- X is a Y", "- X: Y", or plain content ---
            bullet_m = re.match(r"^[-*]\s+(.+)$", line_stripped)
            if bullet_m:
                bullet_content = bullet_m.group(1).strip()
                # Try "X is a Y" in bullet
                isa_m = re.match(
                    r"([A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*)"
                    r"\s+is\s+(?:a|an|the)\s+([a-zA-Z][A-Za-z0-9]*(?:\s+[a-zA-Z][A-Za-z0-9]*)*)",
                    bullet_content,
                )
                if isa_m:
                    _add(Triple(
                        subject=isa_m.group(1).strip(),
                        predicate="is_a",
                        object_=isa_m.group(2).strip(),
                        confidence=0.8,
                        source_text=line_stripped,
                    ))
                else:
                    # Try "Key: Value" in bullet
                    kv_m = re.match(r"([A-Za-z][^:]{1,60}):\s+(.{2,})$", bullet_content)
                    if kv_m:
                        key = kv_m.group(1).strip()
                        val = kv_m.group(2).strip()
                        if not re.search(r"https?://|^\d{4}-\d{2}|^[0-9]", val):
                            _add(Triple(
                                subject=key,
                                predicate="defined_as",
                                object_=val,
                                confidence=0.75,
                                source_text=line_stripped,
                            ))
                    else:
                        # Run plain-text patterns on bullet content
                        for t in self.extract_triples(bullet_content):
                            _add(t)
                continue

            # --- Plain text extraction on non-special lines ---
            if line_stripped and not line_stripped.startswith(("#", ">", "```", "---", "===")):
                for t in self.extract_triples(line_stripped):
                    _add(t)

        return results

    def batch_extract(self, texts: List[str]) -> List[Triple]:
        """
        Extract triples from multiple texts, deduplicating across all.

        Same (subject, predicate, object) triple from different texts
        appears only once (first occurrence kept).

        Returns deduplicated list of Triple objects.
        """
        seen: Set[Triple] = set()
        results: List[Triple] = []
        for text in texts:
            for t in self.extract_triples(text):
                if t not in seen:
                    seen.add(t)
                    results.append(t)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_sentences(text: str) -> List[str]:
        """
        Split text into sentences.

        Splits on '. ', '! ', '? ' and newlines.
        Preserves sentence text for provenance.
        """
        # Replace common sentence terminators followed by whitespace
        text = re.sub(r"([.!?])\s+", r"\1\n", text)
        sentences = [s.strip() for s in text.splitlines() if s.strip()]
        return sentences
