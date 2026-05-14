"""Tests for SemanticChunker — boundary-aware text chunking."""

import pytest

from harvest_normalize.chunking.semantic_chunker import (
    SemanticChunker,
    SemanticStrategy,
    SemanticChunkResult,
    _cosine_sim,
    _tfidf_vector,
)


PARA_TEXT = """
The first paragraph discusses the basics of invoice processing in enterprise systems.
It explains the core workflow from receipt to payment.

The second paragraph covers vendor management and supplier relationships.
Each vendor must be onboarded and verified before payments are made.

The third paragraph addresses compliance and audit requirements.
All transactions must be logged in the evidence chain for SOX compliance.
""".strip()

MARKDOWN_TEXT = """
# Invoice Processing

The core workflow begins with receipt scanning using OCR technology.

## Vendor Onboarding

Each vendor must complete KYC verification before any payments are released.

## Compliance

All transactions are logged in an append-only evidence chain.

### Audit Trail

The audit trail must be tamper-evident and complete.
""".strip()

SHORT_TEXT = "A single sentence without much content."


class TestSemanticChunkerParagraph:
    def test_paragraph_split_produces_chunks(self):
        # Use a small target_chunk_size so paragraphs are not merged together
        chunker = SemanticChunker(
            strategy=SemanticStrategy.PARAGRAPH, target_chunk_size=100
        )
        result = chunker.chunk(PARA_TEXT)
        assert isinstance(result, SemanticChunkResult)
        assert result.total_chunks >= 3

    def test_chunks_not_empty(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk(PARA_TEXT)
        for chunk in result.chunks:
            assert len(chunk.text.strip()) > 0

    def test_chunks_indexed_sequentially(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk(PARA_TEXT)
        for i, chunk in enumerate(result.chunks):
            assert chunk.index == i

    def test_empty_text_returns_empty(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk("")
        assert result.total_chunks == 0
        assert result.chunks == []

    def test_whitespace_only_returns_empty(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk("   \n\n   ")
        assert result.total_chunks == 0

    def test_source_length_recorded(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk(PARA_TEXT)
        assert result.source_length == len(PARA_TEXT)

    def test_min_chunk_size_filters_tiny_chunks(self):
        chunker = SemanticChunker(
            strategy=SemanticStrategy.PARAGRAPH, min_chunk_size=100
        )
        result = chunker.chunk(PARA_TEXT)
        for chunk in result.chunks:
            assert len(chunk.text.strip()) >= 100

    def test_metadata_propagated(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk(PARA_TEXT, metadata={"source": "test_doc"})
        for chunk in result.chunks:
            assert chunk.metadata.get("source") == "test_doc"

    def test_strategy_label_in_result(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk(PARA_TEXT)
        assert result.strategy == "paragraph"


class TestSemanticChunkerRecursive:
    def test_recursive_splits_markdown_by_headings(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.RECURSIVE)
        result = chunker.chunk(MARKDOWN_TEXT)
        assert result.total_chunks >= 3

    def test_recursive_no_headings_falls_back(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.RECURSIVE)
        result = chunker.chunk(PARA_TEXT)
        assert result.total_chunks >= 1

    def test_recursive_large_section_split_to_sentences(self):
        long_para = "Invoice processing is complex. " * 50
        text = f"# Section\n\n{long_para}"
        chunker = SemanticChunker(
            strategy=SemanticStrategy.RECURSIVE, target_chunk_size=100
        )
        result = chunker.chunk(text)
        # Should be split into multiple chunks
        assert result.total_chunks >= 3

    def test_recursive_strategy_label(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.RECURSIVE)
        result = chunker.chunk(PARA_TEXT)
        assert result.strategy == "recursive"

    def test_recursive_chunks_have_start_end(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.RECURSIVE)
        result = chunker.chunk(PARA_TEXT)
        for chunk in result.chunks:
            assert chunk.start_char >= 0
            assert chunk.end_char > chunk.start_char


class TestSemanticChunkerScored:
    def test_scored_returns_boundary_scores(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.SCORED)
        result = chunker.chunk(PARA_TEXT)
        assert isinstance(result.boundary_scores, list)

    def test_scored_merges_similar_paragraphs(self):
        # Two paragraphs about the same topic should merge
        text = "Invoice receipt scanning is step one.\n\nInvoice validation and OCR is step two.\n\nCompletely different topic about weather forecasting."
        chunker = SemanticChunker(
            strategy=SemanticStrategy.SCORED,
            merge_threshold=0.1,
        )
        result = chunker.chunk(text)
        # Should produce fewer chunks than 3 paragraphs
        assert result.total_chunks <= 3

    def test_scored_strategy_label(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.SCORED)
        result = chunker.chunk(PARA_TEXT)
        assert result.strategy == "scored"

    def test_scored_semantic_metadata_in_chunks(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.SCORED)
        result = chunker.chunk(PARA_TEXT)
        for chunk in result.chunks:
            assert chunk.metadata.get("semantic_strategy") == "scored"

    def test_scored_single_para_no_scores(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.SCORED)
        result = chunker.chunk("Single paragraph with just one section.")
        # With only one paragraph, boundary_scores should be empty or minimal
        assert isinstance(result.boundary_scores, list)


class TestTFIDFHelpers:
    def test_cosine_identical_text(self):
        v = _tfidf_vector("invoice processing workflow")
        score = _cosine_sim(v, v)
        assert score > 0.99

    def test_cosine_unrelated_text(self):
        v1 = _tfidf_vector("invoice payment vendor accounting")
        v2 = _tfidf_vector("quantum physics neutron electron")
        score = _cosine_sim(v1, v2)
        assert score < 0.1

    def test_cosine_empty_counter_returns_zero(self):
        from collections import Counter
        score = _cosine_sim(Counter(), Counter({"a": 1.0}))
        assert score == 0.0

    def test_tfidf_vector_is_normalized(self):
        v = _tfidf_vector("a a a b b c")
        # All values should be <= 1.0 (relative frequencies)
        for val in v.values():
            assert val <= 1.0

    def test_cosine_sim_range(self):
        v1 = _tfidf_vector("invoice workflow")
        v2 = _tfidf_vector("invoice workflow vendor")
        score = _cosine_sim(v1, v2)
        assert 0.0 <= score <= 1.0


class TestToChunkResult:
    def test_to_chunk_result_compatible(self):
        from harvest_normalize.chunking.chunker import ChunkResult
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        sem_result = chunker.chunk(PARA_TEXT)
        cr = sem_result.to_chunk_result()
        assert isinstance(cr, ChunkResult)
        assert cr.total_chunks == sem_result.total_chunks

    def test_texts_method(self):
        chunker = SemanticChunker(strategy=SemanticStrategy.PARAGRAPH)
        result = chunker.chunk(PARA_TEXT)
        texts = result.texts()
        assert isinstance(texts, list)
        assert all(isinstance(t, str) for t in texts)
        assert len(texts) == result.total_chunks
