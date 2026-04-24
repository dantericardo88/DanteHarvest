"""
TranscriptAligner — align audio transcripts with UI action events.

Correlates transcript segments (word-level timestamps) with browser
action events to produce AlignedSegments for procedure inference.

Algorithm:
1. For each action event, find transcript words within ±window_seconds
2. Build AlignedSegment with matched words and confidence score
3. Confidence = overlap ratio × speaker_confidence

Fail-closed: unaligned actions produce AlignedSegments with confidence=0.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TranscriptWord:
    word: str
    start_time: float
    end_time: float
    confidence: float = 1.0
    speaker_id: Optional[str] = None


@dataclass
class TranscriptSegment:
    text: str
    start_time: float
    end_time: float
    words: List[TranscriptWord] = field(default_factory=list)
    speaker_id: Optional[str] = None
    confidence: float = 1.0

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TranscriptSegment":
        words = [TranscriptWord(**w) for w in d.get("words", [])]
        return cls(
            text=d.get("text", ""),
            start_time=d.get("start_time", 0.0),
            end_time=d.get("end_time", 0.0),
            words=words,
            speaker_id=d.get("speaker_id"),
            confidence=d.get("confidence", 1.0),
        )


@dataclass
class AlignedSegment:
    action_type: str
    action_timestamp: float
    action_target: Optional[str]
    transcript_text: str
    transcript_start: float
    transcript_end: float
    confidence: float
    words: List[TranscriptWord] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "action_type": self.action_type,
            "action_timestamp": self.action_timestamp,
            "action_target": self.action_target,
            "transcript_text": self.transcript_text,
            "transcript_start": self.transcript_start,
            "transcript_end": self.transcript_end,
            "confidence": self.confidence,
            "word_count": len(self.words),
            "metadata": self.metadata,
        }


@dataclass
class AlignmentResult:
    aligned: List[AlignedSegment]
    total_actions: int
    aligned_count: int
    unaligned_count: int

    @property
    def alignment_rate(self) -> float:
        if self.total_actions == 0:
            return 0.0
        return self.aligned_count / self.total_actions

    def to_dict(self) -> dict:
        return {
            "total_actions": self.total_actions,
            "aligned_count": self.aligned_count,
            "unaligned_count": self.unaligned_count,
            "alignment_rate": self.alignment_rate,
            "segments": [s.to_dict() for s in self.aligned],
        }


class TranscriptAligner:
    """
    Align audio transcript segments with browser/UI action events.

    Usage:
        aligner = TranscriptAligner(window_seconds=5.0)
        result = aligner.align(actions=action_list, segments=transcript_segments)
        for seg in result.aligned:
            print(seg.action_type, seg.transcript_text, seg.confidence)
    """

    def __init__(self, window_seconds: float = 5.0, min_confidence: float = 0.0):
        self.window_seconds = window_seconds
        self.min_confidence = min_confidence

    def align(
        self,
        actions: List[Dict[str, Any]],
        segments: List[TranscriptSegment],
    ) -> AlignmentResult:
        """
        Align action events with transcript segments.

        actions: list of dicts with keys action_type, timestamp, target_selector
        segments: list of TranscriptSegment
        """
        aligned = []
        unaligned_count = 0

        for action in actions:
            ts = action.get("timestamp", 0.0)
            action_type = action.get("action_type", "unknown")
            target = action.get("target_selector")

            # Find segments whose time range overlaps [ts - window, ts + window]
            lo = ts - self.window_seconds
            hi = ts + self.window_seconds
            matching = [
                s for s in segments
                if not (s.end_time < lo or s.start_time > hi)
            ]

            if matching:
                # Merge words from all matching segments
                all_words: List[TranscriptWord] = []
                for seg in matching:
                    all_words.extend(seg.words)
                combined_text = " ".join(s.text for s in matching)
                avg_confidence = sum(s.confidence for s in matching) / len(matching)

                segment = AlignedSegment(
                    action_type=action_type,
                    action_timestamp=ts,
                    action_target=target,
                    transcript_text=combined_text,
                    transcript_start=min(s.start_time for s in matching),
                    transcript_end=max(s.end_time for s in matching),
                    confidence=avg_confidence,
                    words=all_words,
                    metadata={k: v for k, v in action.items()
                               if k not in ("timestamp", "action_type", "target_selector")},
                )
            else:
                unaligned_count += 1
                segment = AlignedSegment(
                    action_type=action_type,
                    action_timestamp=ts,
                    action_target=target,
                    transcript_text="",
                    transcript_start=ts,
                    transcript_end=ts,
                    confidence=0.0,
                    metadata={},
                )

            if segment.confidence >= self.min_confidence:
                aligned.append(segment)

        return AlignmentResult(
            aligned=aligned,
            total_actions=len(actions),
            aligned_count=len(actions) - unaligned_count,
            unaligned_count=unaligned_count,
        )

    def align_from_dict(
        self,
        actions: List[Dict[str, Any]],
        raw_segments: List[Dict[str, Any]],
    ) -> AlignmentResult:
        segments = [TranscriptSegment.from_dict(s) for s in raw_segments]
        return self.align(actions, segments)
