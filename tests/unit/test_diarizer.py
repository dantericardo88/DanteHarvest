"""Tests for SpeakerDiarizer — multi-speaker attribution."""

import pytest
from harvest_normalize.transcribe.diarizer import (
    DiarizationResult,
    DiarizationSegment,
    SpeakerDiarizer,
    annotate_words_with_speakers,
)
from harvest_core.control.exceptions import NormalizationError


def make_result(segments=None) -> DiarizationResult:
    segs = segments or [
        DiarizationSegment("SPEAKER_00", 0.0, 5.0),
        DiarizationSegment("SPEAKER_01", 5.1, 10.0),
        DiarizationSegment("SPEAKER_00", 10.1, 15.0),
    ]
    speakers = {s.speaker for s in segs}
    return DiarizationResult(
        segments=segs,
        speaker_count=len(speakers),
        audio_path="test.wav",
    )


def test_speaker_at_within_segment():
    result = make_result()
    assert result.speaker_at(2.5) == "SPEAKER_00"
    assert result.speaker_at(7.0) == "SPEAKER_01"
    assert result.speaker_at(12.0) == "SPEAKER_00"


def test_speaker_at_no_match_returns_unknown():
    result = make_result()
    assert result.speaker_at(5.05) == "UNKNOWN"


def test_to_speaker_map():
    result = make_result()
    speaker_map = result.to_speaker_map()
    assert "SPEAKER_00" in speaker_map
    assert "SPEAKER_01" in speaker_map
    assert len(speaker_map["SPEAKER_00"]) == 2


def test_missing_file_raises():
    diarizer = SpeakerDiarizer()
    with pytest.raises(NormalizationError, match="not found"):
        diarizer.diarize("/nonexistent/audio.wav")


def test_pyannote_not_installed_raises(tmp_path):
    import sys
    import unittest.mock as mock
    wav_path = tmp_path / "test.wav"
    wav_path.write_bytes(b"RIFF" + b"\x00" * 36)
    diarizer = SpeakerDiarizer()
    with mock.patch.dict(sys.modules, {"pyannote": None, "pyannote.audio": None}):
        diarizer._pipeline = None
        with pytest.raises(NormalizationError, match="pyannote"):
            diarizer._load_pipeline()


def test_annotate_words_with_speakers():
    from harvest_normalize.transcribe.whisper_adapter import TranscriptWord
    words = [
        TranscriptWord("hello", 0.5, 1.0),
        TranscriptWord("world", 6.0, 6.5),
    ]
    diarization = make_result()
    annotated = annotate_words_with_speakers(words, diarization)
    assert annotated[0].speaker_id == "SPEAKER_00"
    assert annotated[1].speaker_id == "SPEAKER_01"


def test_segment_duration():
    seg = DiarizationSegment("SPEAKER_00", 2.0, 5.5)
    assert abs(seg.duration - 3.5) < 0.001
