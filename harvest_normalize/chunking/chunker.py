"""
Chunker — split text into semantically coherent chunks.

Three strategies:
  - fixed:    equal character windows with overlap
  - sentence: split on sentence boundaries (no mid-sentence breaks)
  - topic:    split on Markdown heading boundaries

Constitutional guarantee: no chunk is ever empty; every chunk carries
its source position so provenance can be reconstructed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class ChunkStrategy(str, Enum):
    FIXED = "fixed"
    SENTENCE = "sentence"
    TOPIC = "topic"


@dataclass
class Chunk:
    index: int
    text: str
    start_char: int
    end_char: int
    strategy: ChunkStrategy
    metadata: dict = field(default_factory=dict)

    @property
    def char_count(self) -> int:
        return len(self.text)

    @property
    def word_count(self) -> int:
        return len(self.text.split())


@dataclass
class ChunkResult:
    chunks: List[Chunk]
    strategy: ChunkStrategy
    source_length: int
    total_chunks: int

    def texts(self) -> List[str]:
        return [c.text for c in self.chunks]


# ---------------------------------------------------------------------------
# Sentence boundary pattern (simplified; handles ., !, ?)
# ---------------------------------------------------------------------------
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
_MARKDOWN_HEADING = re.compile(r"^#{1,6}\s+.+$", re.MULTILINE)


class Chunker:
    """
    Split text into chunks using one of three strategies.

    Usage:
        chunker = Chunker(strategy=ChunkStrategy.FIXED, chunk_size=512, overlap=64)
        result = chunker.chunk(text)
        for chunk in result.chunks:
            print(chunk.index, chunk.char_count)
    """

    def __init__(
        self,
        strategy: ChunkStrategy = ChunkStrategy.FIXED,
        chunk_size: int = 512,
        overlap: int = 64,
        min_chunk_size: int = 10,
    ):
        self.strategy = strategy
        self.chunk_size = chunk_size
        self.overlap = max(0, min(overlap, chunk_size - 1))
        self.min_chunk_size = min_chunk_size

    def chunk(self, text: str, metadata: Optional[dict] = None) -> ChunkResult:
        """Split text into chunks according to the configured strategy."""
        if not text:
            return ChunkResult(
                chunks=[],
                strategy=self.strategy,
                source_length=0,
                total_chunks=0,
            )

        extra_meta = metadata or {}
        if self.strategy == ChunkStrategy.FIXED:
            chunks = self._fixed(text, extra_meta)
        elif self.strategy == ChunkStrategy.SENTENCE:
            chunks = self._sentence(text, extra_meta)
        elif self.strategy == ChunkStrategy.TOPIC:
            chunks = self._topic(text, extra_meta)
        else:
            chunks = self._fixed(text, extra_meta)

        # Filter out micro-chunks below min_chunk_size
        chunks = [c for c in chunks if c.char_count >= self.min_chunk_size]
        # Re-index after filtering
        for i, c in enumerate(chunks):
            c.index = i

        return ChunkResult(
            chunks=chunks,
            strategy=self.strategy,
            source_length=len(text),
            total_chunks=len(chunks),
        )

    def _fixed(self, text: str, meta: dict) -> List[Chunk]:
        chunks = []
        start = 0
        idx = 0
        size = self.chunk_size
        step = size - self.overlap

        while start < len(text):
            end = min(start + size, len(text))
            chunk_text = text[start:end]
            chunks.append(Chunk(
                index=idx,
                text=chunk_text,
                start_char=start,
                end_char=end,
                strategy=ChunkStrategy.FIXED,
                metadata={**meta},
            ))
            idx += 1
            if end == len(text):
                break
            start += step

        return chunks

    def _sentence(self, text: str, meta: dict) -> List[Chunk]:
        # Split on sentence endings, then accumulate into windows
        sentences = _SENTENCE_END.split(text)
        chunks = []
        buffer = ""
        buf_start = 0
        idx = 0
        char_pos = 0

        for sentence in sentences:
            if not buffer:
                buf_start = char_pos
            candidate = (buffer + " " + sentence).strip() if buffer else sentence
            if len(candidate) >= self.chunk_size and buffer:
                chunks.append(Chunk(
                    index=idx,
                    text=buffer,
                    start_char=buf_start,
                    end_char=buf_start + len(buffer),
                    strategy=ChunkStrategy.SENTENCE,
                    metadata={**meta},
                ))
                idx += 1
                # Overlap: carry last overlap chars into new buffer
                buffer = buffer[-self.overlap:] + " " + sentence if self.overlap else sentence
                buf_start = char_pos
            else:
                buffer = candidate
            char_pos += len(sentence) + 1  # +1 for the space that was split on

        if buffer:
            chunks.append(Chunk(
                index=idx,
                text=buffer,
                start_char=buf_start,
                end_char=buf_start + len(buffer),
                strategy=ChunkStrategy.SENTENCE,
                metadata={**meta},
            ))

        return chunks

    def _topic(self, text: str, meta: dict) -> List[Chunk]:
        """Split on Markdown heading boundaries (## Heading, # Title, etc.)"""
        heading_positions = [m.start() for m in _MARKDOWN_HEADING.finditer(text)]

        if not heading_positions:
            # No headings — fall back to sentence chunking
            return self._sentence(text, meta)

        chunks = []
        idx = 0
        boundaries = heading_positions + [len(text)]

        for i, start in enumerate(boundaries[:-1]):
            end = boundaries[i + 1]
            segment = text[start:end].strip()
            if segment:
                chunks.append(Chunk(
                    index=idx,
                    text=segment,
                    start_char=start,
                    end_char=end,
                    strategy=ChunkStrategy.TOPIC,
                    metadata={**meta},
                ))
                idx += 1

        # Handle text before first heading
        if heading_positions[0] > 0:
            preamble = text[:heading_positions[0]].strip()
            if preamble:
                preamble_chunk = Chunk(
                    index=-1,
                    text=preamble,
                    start_char=0,
                    end_char=heading_positions[0],
                    strategy=ChunkStrategy.TOPIC,
                    metadata={**meta},
                )
                chunks.insert(0, preamble_chunk)

        return chunks
