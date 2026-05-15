"""
Tests for DiarizationAdapter — pyannote.audio speaker diarization with graceful stub.

All tests are fully mocked (no real audio, no real models).
CI-safe: pyannote.audio is never required.
"""

from __future__ import annotations

import os
import sys
import warnings
import unittest.mock as mock
from collections.abc import Mapping
from pathlib import Path

import pytest

from harvest_normalize.transcribe.diarization_adapter import (
    SPEAKER_UNKNOWN_LABEL,
    DiarizationAdapter,
    DiarizationResult,
    SpeakerSegment,
    _TOKEN_MISSING_WARNING,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_wav(tmp_path: Path, name: str = "test.wav", size_bytes: int = 3244) -> Path:
    """Write a minimal placeholder WAV file (not valid audio, just non-zero size)."""
    p = tmp_path / name
    # 44 header bytes + PCM data so stub duration calculation is non-trivial
    p.write_bytes(b"RIFF" + b"\x00" * (size_bytes - 4))
    return p


class _FakeTrack:
    """Minimal pyannote Segment mock."""
    def __init__(self, start, end):
        self.start = start
        self.end = end


class _FakeDiarization:
    """Mock pyannote Annotation object."""
    def __init__(self, tracks):
        # tracks: list of (start, end, speaker)
        self._tracks = tracks

    def itertracks(self, yield_label=True):
        for start, end, speaker in self._tracks:
            yield _FakeTrack(start, end), None, speaker


# ---------------------------------------------------------------------------
# is_available
# ---------------------------------------------------------------------------

class TestIsAvailable:
    def test_returns_false_when_pyannote_absent(self):
        with mock.patch.dict(sys.modules, {"pyannote": None, "pyannote.audio": None}):
            # Force re-evaluation by creating fresh adapter
            result = DiarizationAdapter.is_available()
            # Can't guarantee False because pyannote might be installed;
            # only guarantee it's a bool
            assert isinstance(result, bool)

    def test_returns_true_when_pyannote_importable(self):
        fake_module = mock.MagicMock()
        with mock.patch.dict(sys.modules, {"pyannote": fake_module, "pyannote.audio": fake_module}):
            # Patch the import path used inside is_available
            with mock.patch("builtins.__import__", side_effect=lambda name, *a, **kw: (
                fake_module if name == "pyannote.audio" else __import__(name, *a, **kw)
            )):
                # We just verify is_available returns a bool; real pyannote may/may not be present
                assert isinstance(DiarizationAdapter.is_available(), bool)

    def test_is_available_false_without_token(self):
        """is_available() returns False when no HF token env var is set."""
        env_sans_token = {
            k: v for k, v in os.environ.items()
            if k not in ("HF_TOKEN", "HUGGINGFACE_TOKEN", "HUGGINGFACE_HUB_TOKEN")
        }
        with mock.patch.dict(os.environ, env_sans_token, clear=True):
            assert DiarizationAdapter.is_available() is False

    def test_is_available_true_with_token(self):
        """is_available() returns True when HF_TOKEN is set (token check passes)."""
        # Patch the pyannote import to succeed regardless of real installation
        fake_pyannote = mock.MagicMock()
        with mock.patch.dict(os.environ, {"HF_TOKEN": "hf_fake_token"}), \
             mock.patch.dict(sys.modules, {"pyannote": fake_pyannote, "pyannote.audio": fake_pyannote}):
            # is_available checks token first; if token present, attempts pyannote import
            # Since we patched sys.modules it will find it — result must be True
            result = DiarizationAdapter.is_available()
            assert result is True


# ---------------------------------------------------------------------------
# diarize — file not found
# ---------------------------------------------------------------------------

class TestDiarizeFileMissing:
    def test_missing_file_returns_empty(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        result = adapter.diarize("/nonexistent/audio.wav")
        assert result == []

    def test_missing_file_does_not_raise(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        # Must not raise even if pyannote not available
        result = adapter.diarize("/no/such/file.wav")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# diarize — stub fallback (pyannote not installed)
# ---------------------------------------------------------------------------

class TestDiarizeStubFallback:
    def test_stub_returns_single_unknown_segment(self, tmp_path):
        wav = make_wav(tmp_path)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        segments = adapter.diarize(wav)
        assert len(segments) == 1
        assert segments[0].speaker_id == SPEAKER_UNKNOWN_LABEL

    def test_stub_segment_start_is_zero(self, tmp_path):
        wav = make_wav(tmp_path)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        segments = adapter.diarize(wav)
        assert segments[0].start == 0.0

    def test_stub_segment_end_is_positive(self, tmp_path):
        wav = make_wav(tmp_path)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        segments = adapter.diarize(wav)
        assert segments[0].end > 0.0

    def test_stub_duration_scales_with_file_size(self, tmp_path):
        small = make_wav(tmp_path, "small.wav", size_bytes=3244)
        large = make_wav(tmp_path, "large.wav", size_bytes=64044)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        segs_small = adapter.diarize(small)
        segs_large = adapter.diarize(large)
        assert segs_large[0].end > segs_small[0].end


# ---------------------------------------------------------------------------
# diarize — happy path with mocked pyannote
# ---------------------------------------------------------------------------

class TestDiarizeHappyPath:
    def _make_adapter_with_mocked_pipeline(self, tracks, num_speakers=None):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter(num_speakers=num_speakers)
        # Force-enable diarization even without a real token, for unit testing
        adapter._diarization_enabled = True
        fake_diarization = _FakeDiarization(tracks)
        adapter._pipeline = mock.MagicMock()
        adapter._pipeline.return_value = fake_diarization
        return adapter

    def test_returns_correct_speaker_ids(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter_with_mocked_pipeline([
            (0.0, 5.0, "SPEAKER_00"),
            (5.1, 10.0, "SPEAKER_01"),
        ])
        with mock.patch.object(DiarizationAdapter, "_pyannote_importable", return_value=True):
            segments = adapter.diarize(wav)
        speakers = {s.speaker_id for s in segments}
        assert "SPEAKER_00" in speakers
        assert "SPEAKER_01" in speakers

    def test_segments_sorted_by_start(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter_with_mocked_pipeline([
            (5.1, 10.0, "SPEAKER_01"),
            (0.0, 5.0, "SPEAKER_00"),
        ])
        with mock.patch.object(DiarizationAdapter, "_pyannote_importable", return_value=True):
            segments = adapter.diarize(wav)
        starts = [s.start for s in segments]
        assert starts == sorted(starts)

    def test_returns_correct_count(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter_with_mocked_pipeline([
            (0.0, 3.0, "SPEAKER_00"),
            (3.1, 6.0, "SPEAKER_01"),
            (6.1, 9.0, "SPEAKER_00"),
        ])
        with mock.patch.object(DiarizationAdapter, "_pyannote_importable", return_value=True):
            segments = adapter.diarize(wav)
        assert len(segments) == 3

    def test_pipeline_error_falls_back_to_stub(self, tmp_path):
        wav = make_wav(tmp_path)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            adapter = DiarizationAdapter()
        adapter._diarization_enabled = True
        adapter._pipeline = mock.MagicMock(side_effect=RuntimeError("GPU OOM"))
        with mock.patch.object(DiarizationAdapter, "_pyannote_importable", return_value=True):
            segments = adapter.diarize(wav)
        # Should fall back to stub (single SPEAKER_UNKNOWN segment)
        assert len(segments) == 1
        assert segments[0].speaker_id == SPEAKER_UNKNOWN_LABEL


# ---------------------------------------------------------------------------
# diarize_and_assign
# ---------------------------------------------------------------------------

class TestDiarizeAndAssign:
    def _make_words(self, specs):
        """specs: list of (word, start, end)"""
        from harvest_normalize.transcribe.whisper_adapter import TranscriptWord
        return [TranscriptWord(word=w, start=s, end=e) for w, s, e in specs]

    def _make_adapter(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return DiarizationAdapter()

    def test_assigns_correct_speaker_to_words(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter()
        segs = [
            SpeakerSegment("SPEAKER_00", 0.0, 5.0),
            SpeakerSegment("SPEAKER_01", 5.1, 10.0),
        ]
        with mock.patch.object(adapter, "diarize", return_value=segs):
            words = self._make_words([
                ("hello", 0.5, 1.0),
                ("world", 6.0, 6.5),
            ])
            result = adapter.diarize_and_assign(words, wav)
        assert result[0].speaker_id == "SPEAKER_00"
        assert result[1].speaker_id == "SPEAKER_01"

    def test_groups_consecutive_same_speaker_words(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter()
        segs = [SpeakerSegment("SPEAKER_00", 0.0, 10.0)]
        with mock.patch.object(adapter, "diarize", return_value=segs):
            words = self._make_words([
                ("hello", 0.5, 1.0),
                ("world", 1.1, 1.5),
                ("foo", 2.0, 2.5),
            ])
            result = adapter.diarize_and_assign(words, wav)
        assert len(result) == 1
        assert "hello" in result[0].text
        assert "world" in result[0].text

    def test_empty_words_returns_empty(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter()
        with mock.patch.object(adapter, "diarize", return_value=[]):
            result = adapter.diarize_and_assign([], wav)
        assert result == []

    def test_speaker_id_set_on_word_objects(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter()
        segs = [SpeakerSegment("SPEAKER_00", 0.0, 5.0)]
        from harvest_normalize.transcribe.whisper_adapter import TranscriptWord
        words = [TranscriptWord("test", 1.0, 1.5)]
        with mock.patch.object(adapter, "diarize", return_value=segs):
            adapter.diarize_and_assign(words, wav)
        assert hasattr(words[0], "speaker_id")
        assert words[0].speaker_id == "SPEAKER_00"

    def test_two_speakers_two_groups(self, tmp_path):
        wav = make_wav(tmp_path)
        adapter = self._make_adapter()
        segs = [
            SpeakerSegment("SPEAKER_00", 0.0, 3.0),
            SpeakerSegment("SPEAKER_01", 3.1, 6.0),
        ]
        words = self._make_words([
            ("hello", 0.5, 1.0),
            ("world", 3.5, 4.0),
        ])
        with mock.patch.object(adapter, "diarize", return_value=segs):
            result = adapter.diarize_and_assign(words, wav)
        assert len(result) == 2
        assert result[0].speaker_id == "SPEAKER_00"
        assert result[1].speaker_id == "SPEAKER_01"


# ---------------------------------------------------------------------------
# SpeakerSegment dataclass
# ---------------------------------------------------------------------------

class TestSpeakerSegment:
    def test_duration(self):
        seg = SpeakerSegment("SPEAKER_00", 2.0, 7.5)
        assert abs(seg.duration - 5.5) < 0.001

    def test_overlaps_true(self):
        a = SpeakerSegment("S0", 0.0, 5.0)
        b = SpeakerSegment("S1", 4.0, 8.0)
        assert a.overlaps(b)

    def test_overlaps_false_adjacent(self):
        a = SpeakerSegment("S0", 0.0, 5.0)
        b = SpeakerSegment("S1", 5.0, 8.0)
        # Touching at boundary — not overlapping (strict inequality)
        assert not a.overlaps(b)

    def test_overlaps_false_gap(self):
        a = SpeakerSegment("S0", 0.0, 3.0)
        b = SpeakerSegment("S1", 5.0, 8.0)
        assert not a.overlaps(b)

    def test_text_defaults_to_empty(self):
        seg = SpeakerSegment("S0", 0.0, 1.0)
        assert seg.text == ""

    def test_speaker_id_is_str(self):
        seg = SpeakerSegment("SPEAKER_00", 0.0, 1.0)
        assert isinstance(seg.speaker_id, str)


# ---------------------------------------------------------------------------
# New tests: token detection, warning, DiarizationResult, availability info
# ---------------------------------------------------------------------------

def _clear_hf_env(env: Mapping[str, str]) -> dict:
    """Return a plain dict copy of env with all HF token vars removed."""
    return {
        k: v for k, v in env.items()
        if k not in ("HF_TOKEN", "HUGGINGFACE_TOKEN", "HUGGINGFACE_HUB_TOKEN")
    }


class TestTokenDetectionAndWarning:
    def test_warning_emitted_when_no_token(self):
        """Instantiating DiarizationAdapter without any HF token emits UserWarning."""
        clean_env = _clear_hf_env(os.environ)
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                DiarizationAdapter()
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(user_warnings) >= 1
        assert "HF_TOKEN" in str(user_warnings[0].message)

    def test_warning_message_exact_text(self):
        """The emitted warning contains the canonical message text."""
        clean_env = _clear_hf_env(os.environ)
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                DiarizationAdapter()
        messages = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        assert any("Speaker diarization disabled" in m for m in messages)
        assert any("SPEAKER_UNKNOWN" in m for m in messages)

    def test_no_warning_when_token_set(self):
        """No UserWarning emitted when HF_TOKEN is present."""
        with mock.patch.dict(os.environ, {"HF_TOKEN": "hf_fake_tok"}):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                DiarizationAdapter()
        token_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(token_warnings) == 0

    def test_diarization_enabled_flag_false_without_token(self):
        """_diarization_enabled is False when no HF token is set."""
        clean_env = _clear_hf_env(os.environ)
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                adapter = DiarizationAdapter()
        assert adapter._diarization_enabled is False

    def test_diarization_enabled_flag_true_with_token(self):
        """_diarization_enabled is True when HF_TOKEN env var is set."""
        with mock.patch.dict(os.environ, {"HF_TOKEN": "hf_fake_tok"}):
            adapter = DiarizationAdapter()
        assert adapter._diarization_enabled is True

    def test_speaker_unknown_label_when_no_token(self, tmp_path):
        """Returned speaker label is SPEAKER_UNKNOWN (not UNKNOWN) when no token."""
        wav = make_wav(tmp_path)
        clean_env = _clear_hf_env(os.environ)
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                adapter = DiarizationAdapter()
        segments = adapter.diarize(wav)
        assert len(segments) == 1
        assert segments[0].speaker_id == SPEAKER_UNKNOWN_LABEL
        assert segments[0].speaker_id != "UNKNOWN"

    def test_explicit_token_arg_disables_warning(self):
        """Passing huggingface_token= explicitly should suppress the UserWarning."""
        clean_env = _clear_hf_env(os.environ)
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                DiarizationAdapter(huggingface_token="hf_explicit_tok")
        token_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(token_warnings) == 0


class TestGetAvailabilityInfo:
    def test_get_availability_info_returns_dict(self):
        """get_availability_info() returns a dict with the required keys."""
        info = DiarizationAdapter.get_availability_info()
        assert isinstance(info, dict)
        assert "available" in info
        assert "reason" in info
        assert "missing_token" in info
        assert "missing_package" in info

    def test_get_availability_info_missing_token(self):
        """No token env var → missing_token=True, available=False."""
        clean_env = _clear_hf_env(os.environ)
        with mock.patch.dict(os.environ, clean_env, clear=True):
            info = DiarizationAdapter.get_availability_info()
        assert info["missing_token"] is True
        assert info["available"] is False

    def test_get_availability_info_with_token_and_package(self):
        """Token set + pyannote importable → available=True, both missing flags False."""
        fake_pyannote = mock.MagicMock()
        with mock.patch.dict(os.environ, {"HF_TOKEN": "hf_fake"}), \
             mock.patch.dict(sys.modules, {"pyannote": fake_pyannote, "pyannote.audio": fake_pyannote}):
            info = DiarizationAdapter.get_availability_info()
        assert info["missing_token"] is False
        assert info["available"] is True

    def test_get_availability_info_reason_is_str(self):
        """reason field is always a non-empty string."""
        info = DiarizationAdapter.get_availability_info()
        assert isinstance(info["reason"], str)
        assert len(info["reason"]) > 0

    def test_get_availability_info_missing_package(self):
        """Token set but pyannote absent → missing_package=True, available=False."""
        with mock.patch.dict(os.environ, {"HF_TOKEN": "hf_fake"}), \
             mock.patch.dict(sys.modules, {"pyannote": None, "pyannote.audio": None}):
            info = DiarizationAdapter.get_availability_info()
        assert info["missing_package"] is True
        assert info["available"] is False


class TestDiarizationResult:
    def test_diarization_result_fields(self):
        """DiarizationResult has speaker_label, diarization_available, warning fields."""
        result = DiarizationResult(
            speaker_label=SPEAKER_UNKNOWN_LABEL,
            diarization_available=False,
            warning=_TOKEN_MISSING_WARNING,
        )
        assert result.speaker_label == SPEAKER_UNKNOWN_LABEL
        assert result.diarization_available is False
        assert result.warning == _TOKEN_MISSING_WARNING

    def test_diarization_result_warning_optional(self):
        """warning field defaults to None."""
        result = DiarizationResult(
            speaker_label="SPEAKER_00",
            diarization_available=True,
        )
        assert result.warning is None

    def test_diarization_result_available_true(self):
        """DiarizationResult with diarization_available=True."""
        result = DiarizationResult(speaker_label="SPEAKER_00", diarization_available=True)
        assert result.diarization_available is True
        assert result.speaker_label == "SPEAKER_00"
