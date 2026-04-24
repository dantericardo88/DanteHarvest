"""Unit tests for Chunker."""

import pytest
from harvest_normalize.chunking.chunker import Chunker, ChunkStrategy, ChunkResult


class TestFixedChunker:
    def setup_method(self):
        self.chunker = Chunker(strategy=ChunkStrategy.FIXED, chunk_size=100, overlap=20)

    def test_single_chunk_for_short_text(self):
        text = "Hello world"
        result = self.chunker.chunk(text)
        assert result.total_chunks == 1
        assert result.chunks[0].text == text

    def test_multiple_chunks_for_long_text(self):
        text = "A" * 250
        result = self.chunker.chunk(text)
        assert result.total_chunks > 1

    def test_chunks_cover_all_text(self):
        text = "word " * 60  # 300 chars
        result = self.chunker.chunk(text)
        # Each character from original must appear in at least one chunk
        reconstructed = set()
        for c in result.chunks:
            for i, ch in enumerate(c.text):
                reconstructed.add(c.start_char + i)
        assert len(text) <= max(reconstructed) + 1

    def test_empty_text_returns_no_chunks(self):
        result = self.chunker.chunk("")
        assert result.total_chunks == 0
        assert result.chunks == []

    def test_chunk_indices_are_sequential(self):
        text = "x" * 500
        result = self.chunker.chunk(text)
        indices = [c.index for c in result.chunks]
        assert indices == list(range(len(indices)))

    def test_strategy_recorded_on_chunks(self):
        result = self.chunker.chunk("hello world")
        assert all(c.strategy == ChunkStrategy.FIXED for c in result.chunks)


class TestSentenceChunker:
    def setup_method(self):
        self.chunker = Chunker(strategy=ChunkStrategy.SENTENCE, chunk_size=80, overlap=0)

    def test_splits_on_sentence_boundaries(self):
        text = "First sentence. Second sentence. Third sentence."
        result = self.chunker.chunk(text)
        assert result.total_chunks >= 1
        # No chunk should be empty
        assert all(c.char_count > 0 for c in result.chunks)

    def test_single_sentence_no_split(self):
        text = "Just one sentence here."
        result = self.chunker.chunk(text)
        assert result.total_chunks == 1


class TestTopicChunker:
    def setup_method(self):
        self.chunker = Chunker(strategy=ChunkStrategy.TOPIC, chunk_size=200, overlap=0)

    def test_splits_on_markdown_headings(self):
        text = "# Chapter 1\nContent for chapter one.\n## Section 1.1\nMore content here."
        result = self.chunker.chunk(text)
        assert result.total_chunks >= 2

    def test_fallback_to_sentence_when_no_headings(self):
        text = "No headings here. Just plain text content that goes on and on."
        result = self.chunker.chunk(text)
        assert result.total_chunks >= 1

    def test_chunk_texts_non_empty(self):
        text = "# A\ncontent a\n# B\ncontent b\n# C\ncontent c"
        result = self.chunker.chunk(text)
        assert all(c.text.strip() for c in result.chunks)


class TestChunkerTexts:
    def test_texts_method_returns_list_of_strings(self):
        chunker = Chunker(strategy=ChunkStrategy.FIXED, chunk_size=50, overlap=0)
        result = chunker.chunk("hello " * 30)
        texts = result.texts()
        assert isinstance(texts, list)
        assert all(isinstance(t, str) for t in texts)
        assert len(texts) == result.total_chunks
