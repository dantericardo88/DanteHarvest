"""
Tests for TranscriptMerger — merges diarization segments with Whisper word timestamps.

All tests use in-memory mocks. CI-safe: no audio, no models.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pytest

from harvest_normalize.transcribe.transcript_merger import (
    SpeakerTurn,
    TranscriptMerger,
)
from harvest_normalize.transcribe.diarization_adapter import SpeakerSegment


# ---------------------------------------------------------------------------
# Minimal TranscriptWord stub (avoids importing whisper_adapter in every test)
# ---------------------------------------------------------------------------

@dataclass
class W:
    """Lightweight stand-in for TranscriptWord."""
    word: str
    start: float
    end: float
    speaker_id: str = ""


def words(*specs) -> list:
    """specs: (word, start, end) tuples → List[W]"""
    return [W(word=w, start=s, end=e) for w, s, e in specs]


def segs(*specs) -> List[SpeakerSegment]:
    """specs: (speaker_id, start, end) tuples → List[SpeakerSegment]"""
    return [SpeakerSegment(speaker_id=sp, start=s, end=e) for sp, s, e in specs]


# ---------------------------------------------------------------------------
# SpeakerTurn dataclass
# ---------------------------------------------------------------------------

class TestSpeakerTurn:
    def test_duration(self):
        turn = SpeakerTurn(speaker="S0", start=1.0, end=4.0, text="hello")
        assert abs(turn.duration - 3.0) < 0.001

    def test_speaker_never_none(self):
        turn = SpeakerTurn(speaker=None, start=0.0, end=1.0, text="hi")
        assert turn.speaker == "UNKNOWN"

    def test_text_never_none(self):
        turn = SpeakerTurn(speaker="S0", start=0.0, end=1.0, text=None)
        assert turn.text == ""

    def test_word_count_defaults_zero(self):
        turn = SpeakerTurn(speaker="S0", start=0.0, end=1.0, text="x")
        assert turn.word_count == 0


# ---------------------------------------------------------------------------
# merge — empty inputs
# ---------------------------------------------------------------------------

class TestMergeEmpty:
    def test_no_words_returns_empty(self):
        merger = TranscriptMerger()
        result = merger.merge([], segs(("SPEAKER_00", 0.0, 5.0)))
        assert result == []

    def test_no_diarization_single_speaker_fallback(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.0, 0.5), ("world", 0.6, 1.0))
        result = merger.merge(ws, [])
        assert len(result) == 1
        assert result[0].speaker == "SPEAKER_00"

    def test_no_words_no_diar_returns_empty(self):
        merger = TranscriptMerger()
        result = merger.merge([], [])
        assert result == []


# ---------------------------------------------------------------------------
# merge — single speaker
# ---------------------------------------------------------------------------

class TestMergeSingleSpeaker:
    def test_all_words_in_one_turn(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.0, 0.5), ("world", 0.6, 1.0), ("foo", 1.1, 1.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0))
        result = merger.merge(ws, ds)
        assert len(result) == 1
        assert result[0].speaker == "SPEAKER_00"

    def test_text_contains_all_words(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.0, 0.5), ("world", 0.6, 1.0))
        ds = segs(("SPEAKER_00", 0.0, 5.0))
        result = merger.merge(ws, ds)
        assert "hello" in result[0].text
        assert "world" in result[0].text

    def test_turn_timestamps(self):
        merger = TranscriptMerger()
        ws = words(("hello", 1.0, 1.5), ("world", 2.0, 2.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0))
        result = merger.merge(ws, ds)
        assert result[0].start == 1.0
        assert result[0].end == 2.5

    def test_word_count(self):
        merger = TranscriptMerger()
        ws = words(("a", 0.0, 0.5), ("b", 0.6, 1.0), ("c", 1.1, 1.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0))
        result = merger.merge(ws, ds)
        assert result[0].word_count == 3


# ---------------------------------------------------------------------------
# merge — two speakers, clean boundaries
# ---------------------------------------------------------------------------

class TestMergeTwoSpeakers:
    def test_two_turns_produced(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.5, 1.0), ("world", 6.0, 6.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 5.1, 10.0))
        result = merger.merge(ws, ds)
        assert len(result) == 2

    def test_correct_speaker_assignment(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.5, 1.0), ("world", 6.0, 6.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 5.1, 10.0))
        result = merger.merge(ws, ds)
        assert result[0].speaker == "SPEAKER_00"
        assert result[1].speaker == "SPEAKER_01"

    def test_turn_text_correct(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.5, 1.0), ("world", 6.0, 6.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 5.1, 10.0))
        result = merger.merge(ws, ds)
        assert "hello" in result[0].text
        assert "world" in result[1].text

    def test_turns_sorted_by_start(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.5, 1.0), ("world", 6.0, 6.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 5.1, 10.0))
        result = merger.merge(ws, ds)
        starts = [t.start for t in result]
        assert starts == sorted(starts)


# ---------------------------------------------------------------------------
# merge — gap filling
# ---------------------------------------------------------------------------

class TestMergeGapFilling:
    def test_gap_fills_with_previous_speaker(self):
        """Word between two segments → inherits previous speaker."""
        merger = TranscriptMerger(gap_fill=True)
        # Word at 5.05 falls in the gap between segments (5.0, 5.1)
        ws = words(("hello", 0.5, 1.0), ("gap_word", 5.05, 5.08), ("world", 6.0, 6.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 5.1, 10.0))
        result = merger.merge(ws, ds)
        # gap_word should be SPEAKER_00 (previous) when gap_fill=True
        gap_turn = next((t for t in result if "gap_word" in t.text), None)
        assert gap_turn is not None
        assert gap_turn.speaker == "SPEAKER_00"

    def test_gap_fill_disabled_uses_unknown(self):
        """With gap_fill=False, gap words get UNKNOWN."""
        merger = TranscriptMerger(gap_fill=False)
        ws = words(("gap_word", 5.05, 5.08))
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 5.1, 10.0))
        result = merger.merge(ws, ds)
        assert result[0].speaker == "UNKNOWN"


# ---------------------------------------------------------------------------
# merge — overlap handling
# ---------------------------------------------------------------------------

class TestMergeOverlap:
    def test_word_with_longer_overlap_wins(self):
        """Word 0.0-2.0: overlaps S0 (0-1.5) by 1.5s, overlaps S1 (1.0-5.0) by 1.0s → S0."""
        merger = TranscriptMerger()
        ws = [W("test", 0.0, 2.0)]
        ds = segs(("SPEAKER_00", 0.0, 1.5), ("SPEAKER_01", 1.0, 5.0))
        result = merger.merge(ws, ds)
        assert result[0].speaker == "SPEAKER_00"

    def test_word_entirely_in_one_segment(self):
        merger = TranscriptMerger()
        ws = [W("hi", 2.0, 3.0)]
        ds = segs(("SPEAKER_00", 0.0, 5.0), ("SPEAKER_01", 6.0, 10.0))
        result = merger.merge(ws, ds)
        assert result[0].speaker == "SPEAKER_00"


# ---------------------------------------------------------------------------
# merge — adjacent same-speaker merging
# ---------------------------------------------------------------------------

class TestMergeAdjacentSameSpeaker:
    def test_adjacent_same_speaker_merged(self):
        """Two non-consecutive words attributed to same speaker → one turn."""
        merger = TranscriptMerger(gap_fill=True)
        ws = words(
            ("a", 0.5, 1.0),   # SPEAKER_00
            ("b", 5.5, 6.0),   # SPEAKER_01
            ("c", 11.0, 11.5), # SPEAKER_00 again
        )
        ds = segs(
            ("SPEAKER_00", 0.0, 2.0),
            ("SPEAKER_01", 5.0, 7.0),
            ("SPEAKER_00", 10.0, 12.0),
        )
        result = merger.merge(ws, ds)
        # Words a and c both go to SPEAKER_00, b goes to SPEAKER_01
        speakers = [t.speaker for t in result]
        assert "SPEAKER_01" in speakers
        assert "SPEAKER_00" in speakers


# ---------------------------------------------------------------------------
# merge_with_text
# ---------------------------------------------------------------------------

class TestMergeWithText:
    def test_empty_words_uses_full_text(self):
        merger = TranscriptMerger()
        result = merger.merge_with_text("hello world", [], [])
        assert len(result) == 1
        assert result[0].text == "hello world"
        assert result[0].speaker == "SPEAKER_00"

    def test_empty_words_empty_text_returns_empty(self):
        merger = TranscriptMerger()
        result = merger.merge_with_text("", [], [])
        assert result == []

    def test_with_words_delegates_to_merge(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.0, 0.5))
        ds = segs(("SPEAKER_00", 0.0, 5.0))
        result = merger.merge_with_text("ignored_text", ws, ds)
        assert len(result) >= 1
        assert "hello" in result[0].text


# ---------------------------------------------------------------------------
# single-speaker fallback detail
# ---------------------------------------------------------------------------

class TestSingleSpeakerFallback:
    def test_fallback_speaker_is_speaker_00(self):
        merger = TranscriptMerger()
        ws = words(("a", 0.0, 1.0), ("b", 1.1, 2.0))
        result = merger.merge(ws, [])
        assert result[0].speaker == "SPEAKER_00"

    def test_fallback_text_has_all_words(self):
        merger = TranscriptMerger()
        ws = words(("hello", 0.0, 0.5), ("world", 0.6, 1.0))
        result = merger.merge(ws, [])
        assert "hello" in result[0].text
        assert "world" in result[0].text

    def test_fallback_timestamps(self):
        merger = TranscriptMerger()
        ws = words(("a", 1.0, 1.5), ("b", 2.0, 2.5))
        result = merger.merge(ws, [])
        assert result[0].start == 1.0
        assert result[0].end == 2.5

    def test_fallback_word_count(self):
        merger = TranscriptMerger()
        ws = words(("x", 0.0, 1.0), ("y", 1.1, 2.0), ("z", 2.1, 3.0))
        result = merger.merge(ws, [])
        assert result[0].word_count == 3
