"""Unit tests for TranscriptAligner."""

from harvest_normalize.align.transcript_aligner import (
    TranscriptAligner,
    TranscriptSegment,
)


def _make_segment(text: str, start: float, end: float) -> TranscriptSegment:
    return TranscriptSegment(text=text, start_time=start, end_time=end, confidence=0.9)


def _make_action(action_type: str, timestamp: float, target: str = "#el") -> dict:
    return {"action_type": action_type, "timestamp": timestamp, "target_selector": target}


class TestTranscriptAligner:
    def setup_method(self):
        self.aligner = TranscriptAligner(window_seconds=3.0)

    def test_aligns_action_with_overlapping_segment(self):
        actions = [_make_action("click", timestamp=5.0)]
        segments = [_make_segment("click the button", start=3.0, end=7.0)]
        result = self.aligner.align(actions, segments)

        assert result.aligned_count == 1
        assert result.unaligned_count == 0
        assert "click" in result.aligned[0].transcript_text

    def test_no_overlap_produces_unaligned_segment(self):
        actions = [_make_action("click", timestamp=20.0)]
        segments = [_make_segment("some audio", start=0.0, end=5.0)]
        result = self.aligner.align(actions, segments)

        assert result.unaligned_count == 1
        assert result.aligned[0].confidence == 0.0

    def test_empty_actions_returns_empty_result(self):
        result = self.aligner.align([], [_make_segment("hello", 0, 5)])
        assert result.total_actions == 0
        assert result.aligned == []

    def test_empty_segments_all_unaligned(self):
        actions = [_make_action("click", 5.0), _make_action("type", 10.0)]
        result = self.aligner.align(actions, [])
        assert result.unaligned_count == 2
        assert all(s.confidence == 0.0 for s in result.aligned)

    def test_alignment_rate_computed_correctly(self):
        actions = [_make_action("click", 5.0), _make_action("type", 100.0)]
        segments = [_make_segment("click it", 3.0, 7.0)]
        result = self.aligner.align(actions, segments)
        assert result.alignment_rate == 0.5

    def test_multiple_segments_merged_for_single_action(self):
        actions = [_make_action("scroll", 5.0)]
        segments = [
            _make_segment("first part", 3.0, 5.0),
            _make_segment("second part", 5.0, 7.0),
        ]
        result = self.aligner.align(actions, segments)
        assert len(result.aligned) == 1
        combined = result.aligned[0].transcript_text
        assert "first part" in combined and "second part" in combined

    def test_align_from_dict(self):
        actions = [{"action_type": "click", "timestamp": 2.0, "target_selector": "#ok"}]
        raw_segs = [{"text": "press ok", "start_time": 0.5, "end_time": 3.5,
                     "words": [], "confidence": 0.85}]
        result = self.aligner.align_from_dict(actions, raw_segs)
        assert result.aligned_count == 1

    def test_to_dict_structure(self):
        actions = [_make_action("click", 1.0)]
        segs = [_make_segment("hello", 0.0, 2.0)]
        result = self.aligner.align(actions, segs)
        d = result.to_dict()
        assert "total_actions" in d
        assert "alignment_rate" in d
        assert "segments" in d
