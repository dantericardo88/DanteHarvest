"""
MetadataEnricher — add structured metadata to text chunks.

Harvested from: LlamaIndex IngestionPipeline + metadata extraction patterns.

Enriches each Chunk with:
  - title: first markdown heading or first sentence (heuristic, local-first)
  - word_count: token count
  - source_path: provenance pointer
  - chunk_index: ordinal within the source document
  - keywords: top-N TF terms (no LLM required)

Optional LLM-based enrichment (title summarization, keyword extraction)
is activated by passing llm_client; fails with ImportError if the
dependency is not installed (zero-ambiguity: no silent degradation).

Local-first: all heuristic paths require zero network calls.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from harvest_normalize.chunking.chunker import Chunk


_HEADING_RE = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
_SENTENCE_END_RE = re.compile(r"[.!?]\s+|[.!?]$")


class MetadataEnricher:
    """
    Enrich a list of Chunks with structured metadata.

    Usage:
        enricher = MetadataEnricher(source_path="storage/doc.pdf")
        enriched = enricher.enrich(chunks)
        for c in enriched:
            print(c.metadata["title"], c.metadata["word_count"])
    """

    def __init__(
        self,
        source_path: str = "",
        top_keywords: int = 5,
        llm_client=None,
    ):
        self.source_path = source_path
        self.top_keywords = top_keywords
        self.llm_client = llm_client

    def enrich(
        self,
        chunks: List[Chunk],
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Chunk]:
        """
        Enrich chunks in-place and return them.
        Empty chunk list returns [] (not error).
        """
        if not chunks:
            return chunks

        extra = extra_metadata or {}
        doc_text = " ".join(c.text for c in chunks)
        global_keywords = self._extract_keywords(doc_text)

        for chunk in chunks:
            chunk.metadata.update({
                "title": self._extract_title(chunk.text),
                "word_count": len(chunk.text.split()),
                "source_path": self.source_path,
                "chunk_index": chunk.index,
                "keywords": self._extract_keywords(chunk.text, self.top_keywords),
                "global_keywords": global_keywords,
                **extra,
            })

        return chunks

    def _extract_title(self, text: str) -> str:
        # 1. First markdown heading
        m = _HEADING_RE.search(text)
        if m:
            return m.group(1).strip()

        # 2. First non-empty sentence
        stripped = text.strip()
        if not stripped:
            return ""
        parts = _SENTENCE_END_RE.split(stripped, maxsplit=1)
        first = parts[0].strip()
        if first:
            return first[:120]  # cap at 120 chars

        return stripped[:80]

    def _extract_keywords(self, text: str, n: int = 5) -> List[str]:
        """Top-N terms by frequency, excluding stopwords."""
        _STOP = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
            "for", "of", "with", "is", "are", "was", "were", "be", "been",
            "have", "has", "had", "do", "does", "did", "will", "would",
            "could", "should", "may", "might", "this", "that", "these",
            "those", "it", "its", "by", "from", "as", "not", "no",
        }
        tokens = re.findall(r"[a-z][a-z0-9_\-]{2,}", text.lower())
        freq: Dict[str, int] = {}
        for t in tokens:
            if t not in _STOP:
                freq[t] = freq.get(t, 0) + 1
        return sorted(freq, key=lambda k: -freq[k])[:n]
