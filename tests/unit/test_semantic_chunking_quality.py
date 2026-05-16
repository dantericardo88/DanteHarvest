"""Tests for SemanticChunker quality scoring methods."""

from harvest_normalize.chunking.semantic_chunker import SemanticChunker, SemanticStrategy


PROSE_TEXT = """
The first paragraph discusses the basics of invoice processing in enterprise systems.
It explains the core workflow from receipt to payment.

The second paragraph covers vendor management and supplier relationships.
Each vendor must be onboarded and verified before payments are made.

The third paragraph addresses compliance and audit requirements.
All transactions must be logged in the evidence chain for SOX compliance.
""".strip()


class TestScoreChunkCoherence:
    def setup_method(self):
        self.chunker = SemanticChunker()

    def test_moderate_diversity_prose_returns_1(self):
        # Each long word appears exactly twice → TTR = 0.5, which is in the [0.3, 0.7] window → score = 1.0
        words = ["invoice", "payment", "vendor", "system", "audit",
                 "review", "check", "report", "record", "entry"]
        text = " ".join(words * 2)  # 20 words, 10 unique → TTR = 0.5
        score = self.chunker.score_chunk_coherence(text)
        assert score == 1.0

    def test_very_short_text_returns_1(self):
        score = self.chunker.score_chunk_coherence("Hi")
        assert score == 1.0

    def test_short_text_under_4_long_words_returns_1(self):
        # Words all <= 3 chars — filtered out, count < 4 → returns 1.0
        score = self.chunker.score_chunk_coherence("yes no ok")
        assert score == 1.0

    def test_score_in_range(self):
        text = "word " * 20
        score = self.chunker.score_chunk_coherence(text)
        assert 0.0 <= score <= 1.0

    def test_highly_repetitive_text_lower_score(self):
        # same long word repeated many times → TTR very low → score < 1.0
        text = "invoice " * 30
        score = self.chunker.score_chunk_coherence(text)
        assert score < 1.0

    def test_result_is_float(self):
        score = self.chunker.score_chunk_coherence("Some reasonable text here today.")
        assert isinstance(score, float)


class TestScoreBoundaryQuality:
    def setup_method(self):
        self.chunker = SemanticChunker()

    def test_sentence_ending_before_gives_higher_score(self):
        before = "This is the end of a sentence."
        after = "This starts a new thought."
        score = self.chunker.score_boundary_quality(before, after)
        # Sentence-ending before gives a score >= 0.5 + base 0.2 = 0.7
        assert score >= 0.7

    def test_heading_start_after_adds_bonus(self):
        before = "This is a paragraph ending."
        after = "## New Section"
        score = self.chunker.score_boundary_quality(before, after)
        assert score >= 0.7

    def test_mid_sentence_cut_lower_score(self):
        before = "This sentence was cut in the"
        after = "middle of nothing"
        score_mid = self.chunker.score_boundary_quality(before, after)
        before_clean = "This is a complete sentence."
        score_clean = self.chunker.score_boundary_quality(before_clean, after)
        assert score_mid < score_clean

    def test_score_in_range(self):
        score = self.chunker.score_boundary_quality("some text", "more text")
        assert 0.0 <= score <= 1.0

    def test_exclamation_mark_ending_scores_high(self):
        before = "Wow, this is amazing!"
        after = "Now onto the next point."
        score = self.chunker.score_boundary_quality(before, after)
        assert score >= 0.7

    def test_question_mark_ending_scores_high(self):
        before = "Is this the right approach?"
        after = "Let us explore alternatives."
        score = self.chunker.score_boundary_quality(before, after)
        assert score >= 0.7


class TestScoreOverlapQuality:
    def setup_method(self):
        self.chunker = SemanticChunker()

    def test_empty_overlap_returns_zero(self):
        score = self.chunker.score_overlap_quality("chunk a text", "chunk b text", "")
        assert score == 0.0

    def test_ideal_overlap_10_percent_returns_1(self):
        # chunk_a is 100 chars, overlap is 10 chars (10%) — ideal
        chunk_a = "a" * 100
        chunk_b = "b" * 100
        overlap = "a" * 10  # 10% of chunk_a
        score = self.chunker.score_overlap_quality(chunk_a, chunk_b, overlap)
        assert score == 1.0

    def test_ideal_overlap_5_percent_returns_1(self):
        chunk_a = "a" * 100
        chunk_b = "b" * 100
        overlap = "a" * 5  # exactly 5% — boundary of ideal range
        score = self.chunker.score_overlap_quality(chunk_a, chunk_b, overlap)
        assert score == 1.0

    def test_ideal_overlap_20_percent_returns_1(self):
        chunk_a = "a" * 100
        chunk_b = "b" * 100
        overlap = "a" * 20  # exactly 20% — boundary of ideal range
        score = self.chunker.score_overlap_quality(chunk_a, chunk_b, overlap)
        assert score == 1.0

    def test_tiny_overlap_lower_score(self):
        chunk_a = "a" * 200
        chunk_b = "b" * 200
        overlap = "a" * 1  # 0.5% — below ideal range
        score = self.chunker.score_overlap_quality(chunk_a, chunk_b, overlap)
        assert score < 1.0

    def test_huge_overlap_lower_score(self):
        chunk_a = "a" * 100
        chunk_b = "b" * 100
        overlap = "a" * 80  # 80% — way above ideal range
        score = self.chunker.score_overlap_quality(chunk_a, chunk_b, overlap)
        assert score < 1.0

    def test_score_in_range(self):
        score = self.chunker.score_overlap_quality("abc" * 50, "def" * 50, "abc" * 5)
        assert 0.0 <= score <= 1.0


class TestChunkWithQuality:
    def setup_method(self):
        self.chunker = SemanticChunker(
            strategy=SemanticStrategy.PARAGRAPH,
            target_chunk_size=200,
            min_chunk_size=10,
        )

    def test_returns_list(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        assert isinstance(result, list)

    def test_each_item_is_dict(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        assert len(result) > 0
        for item in result:
            assert isinstance(item, dict)

    def test_quality_score_key_present(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        for item in result:
            assert "quality_score" in item

    def test_all_required_keys_present(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        required_keys = {"text", "index", "length", "coherence_score", "overlap_quality", "quality_score"}
        for item in result:
            assert required_keys.issubset(item.keys())

    def test_index_is_sequential(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        for i, item in enumerate(result):
            assert item["index"] == i

    def test_length_matches_text(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        for item in result:
            assert item["length"] == len(item["text"])

    def test_quality_score_in_range(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        for item in result:
            assert 0.0 <= item["quality_score"] <= 1.0

    def test_coherence_score_in_range(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        for item in result:
            assert 0.0 <= item["coherence_score"] <= 1.0

    def test_overlap_quality_in_range(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        for item in result:
            assert 0.0 <= item["overlap_quality"] <= 1.0

    def test_empty_text_returns_empty_list(self):
        result = self.chunker.chunk_with_quality("")
        assert result == []

    def test_first_chunk_overlap_quality_is_1(self):
        result = self.chunker.chunk_with_quality(PROSE_TEXT)
        if result:
            # First chunk has no previous chunk, so overlap_quality defaults to 1.0
            assert result[0]["overlap_quality"] == 1.0
