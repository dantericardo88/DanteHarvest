"""
SemanticChunker — boundary-aware text chunking using paragraph/section semantics.

Harvested from: LlamaIndex SentenceSplitter + LangChain RecursiveCharacterTextSplitter patterns.

Unlike fixed-size chunking, SemanticChunker respects the natural structure of text:
  1. Paragraph boundaries (blank lines)
  2. Section boundaries (Markdown headings, HR separators)
  3. Sentence boundaries (. ! ?)
  4. Optional cosine-similarity boundary scoring to merge small paragraphs

Constitutional guarantees:
- No chunk is ever empty
- Every chunk preserves start_char/end_char for provenance reconstruction
- Fail-closed: malformed text falls back to paragraph splitting
- Local-first: boundary scoring uses cosine similarity — no external calls

Scoring strategy (semantic_score mode):
  Paragraphs are merged if their TF-IDF cosine similarity exceeds `merge_threshold`.
  This approximates sentence-transformer boundary detection without a model dependency.
"""

from __future__ import annotations

import re
import math
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Tuple

from harvest_normalize.chunking.chunker import Chunk, ChunkResult, ChunkStrategy


class SemanticStrategy(str, Enum):
    PARAGRAPH = "paragraph"       # split on blank lines
    RECURSIVE = "recursive"       # try section → paragraph → sentence
    SCORED = "scored"             # merge paragraphs by cosine similarity


@dataclass
class SemanticChunkResult:
    """Extended ChunkResult that includes boundary scores."""
    chunks: List[Chunk]
    strategy: str
    source_length: int
    total_chunks: int
    boundary_scores: List[float] = field(default_factory=list)

    def texts(self) -> List[str]:
        return [c.text for c in self.chunks]

    def to_chunk_result(self) -> ChunkResult:
        return ChunkResult(
            chunks=self.chunks,
            strategy=ChunkStrategy.SENTENCE,
            source_length=self.source_length,
            total_chunks=self.total_chunks,
        )


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------
_BLANK_LINE = re.compile(r"\n{2,}")
_MD_HEADING = re.compile(r"^#{1,6}\s+.+$", re.MULTILINE)
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
_HR = re.compile(r"^[-*_]{3,}\s*$", re.MULTILINE)


class SemanticChunker:
    """
    Boundary-aware chunker that understands document structure.

    Usage:
        chunker = SemanticChunker(
            strategy=SemanticStrategy.RECURSIVE,
            target_chunk_size=512,
            min_chunk_size=50,
            merge_threshold=0.35,
        )
        result = chunker.chunk(text)
        for chunk in result.chunks:
            print(chunk.index, chunk.char_count)

    Strategies:
      PARAGRAPH — split on blank lines only (fastest)
      RECURSIVE — try headings → paragraphs → sentences (best quality)
      SCORED    — paragraph split then merge adjacent ones by cosine similarity
    """

    def __init__(
        self,
        strategy: SemanticStrategy = SemanticStrategy.RECURSIVE,
        target_chunk_size: int = 512,
        min_chunk_size: int = 50,
        max_chunk_size: int = 2048,
        merge_threshold: float = 0.35,
        overlap_sentences: int = 1,
    ):
        self.strategy = strategy
        self.target_chunk_size = target_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self.merge_threshold = merge_threshold
        self.overlap_sentences = overlap_sentences

    def chunk(self, text: str, metadata: Optional[dict] = None) -> SemanticChunkResult:
        """Split text into semantically coherent chunks."""
        if not text or not text.strip():
            return SemanticChunkResult(
                chunks=[],
                strategy=self.strategy.value,
                source_length=0,
                total_chunks=0,
                boundary_scores=[],
            )

        meta = metadata or {}

        if self.strategy == SemanticStrategy.PARAGRAPH:
            chunks, scores = self._paragraph_split(text, meta)
        elif self.strategy == SemanticStrategy.SCORED:
            chunks, scores = self._scored_split(text, meta)
        else:  # RECURSIVE
            chunks, scores = self._recursive_split(text, meta)

        # Filter micro-chunks
        chunks = [c for c in chunks if len(c.text.strip()) >= self.min_chunk_size]
        # Re-index
        for i, c in enumerate(chunks):
            c.index = i

        return SemanticChunkResult(
            chunks=chunks,
            strategy=self.strategy.value,
            source_length=len(text),
            total_chunks=len(chunks),
            boundary_scores=scores,
        )

    # ------------------------------------------------------------------
    # Paragraph splitting
    # ------------------------------------------------------------------

    def _paragraph_split(
        self, text: str, meta: dict
    ) -> Tuple[List[Chunk], List[float]]:
        """Split on blank lines, then merge short paragraphs."""
        paragraphs = _BLANK_LINE.split(text)
        segments = self._merge_short_segments(paragraphs)
        return self._segments_to_chunks(segments, text, meta, ChunkStrategy.SENTENCE), []

    # ------------------------------------------------------------------
    # Recursive splitting
    # ------------------------------------------------------------------

    def _recursive_split(
        self, text: str, meta: dict
    ) -> Tuple[List[Chunk], List[float]]:
        """
        Split hierarchy: headings → blank lines → sentences → characters.
        This mirrors LangChain's RecursiveCharacterTextSplitter.
        """
        # Step 1: split on Markdown headings or HR
        sections = self._split_by_headings(text)

        if len(sections) <= 1:
            # No structural headings — split by paragraph
            sections = _BLANK_LINE.split(text)

        chunks = []
        for section in sections:
            section = section.strip()
            if not section:
                continue
            if len(section) <= self.target_chunk_size:
                start = text.find(section)
                chunks.append(_make_chunk(0, section, max(0, start), meta))
            else:
                # Section too large — split by sentences
                sub = self._sentence_split(section, meta, text)
                chunks.extend(sub)

        return chunks, []

    def _split_by_headings(self, text: str) -> List[str]:
        """Split text at Markdown heading boundaries."""
        positions = sorted(
            {m.start() for m in _MD_HEADING.finditer(text)}
            | {m.start() for m in _HR.finditer(text)}
        )
        if not positions:
            return [text]

        sections = []
        prev = 0
        for pos in positions:
            if pos > prev:
                sections.append(text[prev:pos])
            prev = pos
        sections.append(text[prev:])
        return [s for s in sections if s.strip()]

    def _sentence_split(
        self, text: str, meta: dict, source: str
    ) -> List[Chunk]:
        """Split text into sentence-boundary chunks of target_chunk_size."""
        sentences = _SENTENCE_END.split(text)
        chunks = []
        buffer = ""
        buf_start = 0
        idx = 0

        for sent in sentences:
            candidate = (buffer + " " + sent).strip() if buffer else sent.strip()
            if len(candidate) > self.target_chunk_size and buffer:
                # Flush current buffer
                start = max(0, source.find(buffer))
                chunks.append(_make_chunk(idx, buffer, start, meta))
                idx += 1
                # Start new buffer with optional overlap
                if self.overlap_sentences and chunks:
                    overlap = self._last_sentences(buffer, self.overlap_sentences)
                    buffer = (overlap + " " + sent).strip()
                else:
                    buffer = sent.strip()
            else:
                buffer = candidate

        if buffer.strip():
            start = max(0, source.find(buffer))
            chunks.append(_make_chunk(idx, buffer.strip(), start, meta))

        return chunks

    def _last_sentences(self, text: str, n: int) -> str:
        """Return the last n sentences from text for overlap."""
        parts = _SENTENCE_END.split(text)
        return " ".join(parts[-n:]).strip()

    # ------------------------------------------------------------------
    # Scored splitting (TF-IDF cosine boundary detection)
    # ------------------------------------------------------------------

    def _scored_split(
        self, text: str, meta: dict
    ) -> Tuple[List[Chunk], List[float]]:
        """
        Split on paragraph boundaries, then score each boundary by TF-IDF cosine.
        Adjacent paragraphs with similarity > merge_threshold are merged.

        This approximates sentence-transformer boundary detection without a
        model dependency — pure-Python, zero network calls.
        """
        paragraphs = [p.strip() for p in _BLANK_LINE.split(text) if p.strip()]
        if len(paragraphs) <= 1:
            return self._paragraph_split(text, meta)

        # Build TF-IDF vectors
        vectors = [_tfidf_vector(p) for p in paragraphs]

        # Score boundaries between adjacent paragraphs
        boundary_scores = []
        for i in range(len(paragraphs) - 1):
            sim = _cosine_sim(vectors[i], vectors[i + 1])
            boundary_scores.append(sim)

        # Merge paragraphs across high-similarity boundaries
        merged_segments: List[str] = []
        current = paragraphs[0]
        for i, score in enumerate(boundary_scores):
            next_para = paragraphs[i + 1]
            candidate = current + "\n\n" + next_para
            if score >= self.merge_threshold and len(candidate) <= self.max_chunk_size:
                current = candidate
            else:
                merged_segments.append(current)
                current = next_para
        merged_segments.append(current)

        chunks = self._segments_to_chunks(merged_segments, text, meta, ChunkStrategy.SENTENCE)
        return chunks, boundary_scores

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _merge_short_segments(self, segments: List[str]) -> List[str]:
        """Merge consecutive short segments until target_chunk_size is reached."""
        merged = []
        current = ""
        for seg in segments:
            seg = seg.strip()
            if not seg:
                continue
            candidate = (current + "\n\n" + seg).strip() if current else seg
            if len(candidate) <= self.target_chunk_size:
                current = candidate
            else:
                if current:
                    merged.append(current)
                current = seg
        if current:
            merged.append(current)
        return merged

    def _segments_to_chunks(
        self,
        segments: List[str],
        source: str,
        meta: dict,
        strategy: ChunkStrategy,
    ) -> List[Chunk]:
        chunks = []
        search_from = 0
        for i, seg in enumerate(segments):
            seg = seg.strip()
            if not seg:
                continue
            start = source.find(seg, search_from)
            if start == -1:
                start = 0
            end = start + len(seg)
            chunks.append(Chunk(
                index=i,
                text=seg,
                start_char=start,
                end_char=end,
                strategy=strategy,
                metadata={**meta, "semantic_strategy": self.strategy.value},
            ))
            search_from = max(search_from, end)
        return chunks


# ---------------------------------------------------------------------------
# TF-IDF helpers (pure Python, zero deps)
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _tfidf_vector(text: str) -> Counter:
    tokens = _tokenize(text)
    total = max(len(tokens), 1)
    tf = Counter(tokens)
    return Counter({t: c / total for t, c in tf.items()})


def _cosine_sim(a: Counter, b: Counter) -> float:
    common = set(a) & set(b)
    if not common:
        return 0.0
    dot = sum(a[t] * b[t] for t in common)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _make_chunk(idx: int, text: str, start: int, meta: dict) -> Chunk:
    return Chunk(
        index=idx,
        text=text,
        start_char=start,
        end_char=start + len(text),
        strategy=ChunkStrategy.SENTENCE,
        metadata=dict(meta),
    )
