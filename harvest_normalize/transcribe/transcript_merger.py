"""
TranscriptMerger — merge diarization segments with Whisper word timestamps.

Produces a deduplicated, speaker-attributed List[SpeakerTurn] from:
  - WhisperAdapter TranscriptWord list (fine-grained word timestamps)
  - DiarizationAdapter SpeakerSegment list (coarse speaker boundaries)

Handles:
- Overlap: a word at the boundary of two speaker segments goes to whichever
  speaker has the longer overlap with the word's time range.
- Gap-filling: silent gaps between words are attributed to the previous speaker.
- Single-speaker fallback: when diarization is empty or unavailable, all words
  are attributed to "SPEAKER_00".

Constitutional guarantees:
- Zero-ambiguity: SpeakerTurn.speaker is always str, never None
- Fail-safe: never raises on empty input; returns [] for no words, ["SPEAKER_00"]
  turn for single-speaker fallback
- Deterministic: given same inputs, always returns same outputs (no random choice)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from harvest_normalize.transcribe.diarization_adapter import SpeakerSegment


@dataclass
class SpeakerTurn:
    """A contiguous block of speech attributed to one speaker."""
    speaker: str
    start: float
    end: float
    text: str
    word_count: int = 0

    @property
    def duration(self) -> float:
        return self.end - self.start

    def __post_init__(self) -> None:
        if self.speaker is None:
            self.speaker = "UNKNOWN"
        if self.text is None:
            self.text = ""


class TranscriptMerger:
    """
    Merges diarization segments with Whisper word timestamps.

    Usage:
        merger = TranscriptMerger()
        turns = merger.merge(words, diar_segments)
        for turn in turns:
            print(f"{turn.speaker}: {turn.text}")

    Single-speaker fallback (empty diarization):
        turns = merger.merge(words, [])
        # → all words in one SPEAKER_00 SpeakerTurn
    """

    def __init__(self, gap_fill: bool = True):
        """
        Args:
            gap_fill: if True, silent gaps between words inherit the previous
                      speaker rather than being attributed to UNKNOWN.
        """
        self.gap_fill = gap_fill

    def merge(
        self,
        words: list,
        diar_segments: List[SpeakerSegment],
    ) -> List[SpeakerTurn]:
        """
        Merge word-timestamp list with diarization segments.

        Args:
            words: list of TranscriptWord objects (.word, .start, .end attributes)
            diar_segments: list of SpeakerSegment from DiarizationAdapter

        Returns:
            List[SpeakerTurn] sorted by start time, consecutive same-speaker
            turns merged into one.
        """
        if not words:
            return []

        # Single-speaker fallback when no diarization available
        if not diar_segments:
            return self._single_speaker_fallback(words)

        # Sort diarization segments by start time for binary search
        sorted_segs = sorted(diar_segments, key=lambda s: s.start)

        # Assign each word a speaker
        assigned: List[tuple[str, object]] = []  # (speaker_id, word)
        last_speaker = sorted_segs[0].speaker_id if sorted_segs else "SPEAKER_00"

        for word in words:
            speaker = self._assign_speaker(word, sorted_segs)
            if speaker == "UNKNOWN" and self.gap_fill:
                speaker = last_speaker
            else:
                last_speaker = speaker
            assigned.append((speaker, word))

        # Group consecutive same-speaker words into SpeakerTurn
        return self._group_into_turns(assigned)

    def merge_with_text(
        self,
        full_text: str,
        words: list,
        diar_segments: List[SpeakerSegment],
    ) -> List[SpeakerTurn]:
        """
        Convenience method: merge words + diarization; attaches full_text to
        the first turn when words is empty (single-turn fallback).
        """
        if not words and full_text:
            return [SpeakerTurn(
                speaker="SPEAKER_00",
                start=0.0,
                end=0.0,
                text=full_text,
                word_count=len(full_text.split()),
            )]
        return self.merge(words, diar_segments)

    def _assign_speaker(self, word, sorted_segs: List[SpeakerSegment]) -> str:
        """
        Assign a speaker to a word using overlap scoring.

        For each diarization segment that overlaps the word's time range,
        compute the overlap duration. Return the speaker with the max overlap.
        Ties broken by segment order (first one wins).
        """
        word_start = float(word.start)
        word_end = float(word.end)
        word_mid = (word_start + word_end) / 2.0

        best_speaker = "UNKNOWN"
        best_overlap = 0.0

        for seg in sorted_segs:
            # Early termination: segments are sorted, skip past-end ones
            if seg.start > word_end:
                break

            overlap = self._overlap(word_start, word_end, seg.start, seg.end)
            if overlap > best_overlap:
                best_overlap = overlap
                best_speaker = seg.speaker_id
            elif overlap == 0.0 and best_overlap == 0.0:
                # Midpoint fallback: no overlap but check if word midpoint is in segment
                if seg.start <= word_mid <= seg.end:
                    best_speaker = seg.speaker_id

        return best_speaker

    @staticmethod
    def _overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
        """Compute overlap duration between two intervals."""
        return max(0.0, min(a_end, b_end) - max(a_start, b_start))

    def _single_speaker_fallback(self, words: list) -> List[SpeakerTurn]:
        """All words attributed to SPEAKER_00."""
        text = " ".join(w.word for w in words)
        return [SpeakerTurn(
            speaker="SPEAKER_00",
            start=float(words[0].start),
            end=float(words[-1].end),
            text=text,
            word_count=len(words),
        )]

    def _group_into_turns(self, assigned: list) -> List[SpeakerTurn]:
        """Group (speaker, word) pairs into consecutive SpeakerTurn objects."""
        if not assigned:
            return []

        turns: List[SpeakerTurn] = []
        current_speaker, current_word = assigned[0]
        current_words = [current_word]

        for speaker, word in assigned[1:]:
            if speaker == current_speaker:
                current_words.append(word)
            else:
                turns.append(self._make_turn(current_speaker, current_words))
                current_speaker = speaker
                current_words = [word]

        turns.append(self._make_turn(current_speaker, current_words))

        # Merge adjacent same-speaker turns (can arise from gap-filling)
        return self._merge_adjacent_turns(turns)

    @staticmethod
    def _make_turn(speaker: str, words: list) -> SpeakerTurn:
        return SpeakerTurn(
            speaker=speaker,
            start=float(words[0].start),
            end=float(words[-1].end),
            text=" ".join(w.word for w in words),
            word_count=len(words),
        )

    @staticmethod
    def _merge_adjacent_turns(turns: List[SpeakerTurn]) -> List[SpeakerTurn]:
        """Merge consecutive turns with the same speaker into one."""
        if not turns:
            return []
        merged: List[SpeakerTurn] = [turns[0]]
        for turn in turns[1:]:
            if turn.speaker == merged[-1].speaker:
                prev = merged[-1]
                merged[-1] = SpeakerTurn(
                    speaker=prev.speaker,
                    start=prev.start,
                    end=turn.end,
                    text=prev.text + " " + turn.text,
                    word_count=prev.word_count + turn.word_count,
                )
            else:
                merged.append(turn)
        return merged
