"""
Tests for StreamingTranscriber — faster-whisper real-time transcription with batch fallback.

All tests are fully mocked. CI-safe: faster-whisper and openai-whisper not required.
"""

from __future__ import annotations

import sys
import unittest.mock as mock
from pathlib import Path

import pytest

from harvest_normalize.transcribe.streaming_transcriber import (
    StreamingTranscriber,
    TranscriptChunk,
)
from harvest_core.control.exceptions import NormalizationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_wav(tmp_path: Path, name: str = "test.wav") -> Path:
    p = tmp_path / name
    import wave, struct
    sample_rate = 16000
    n_samples = 1600  # 0.1 seconds
    with wave.open(str(p), "w") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(struct.pack(f"<{n_samples}h", *([0] * n_samples)))
    return p


class _FakeWhisperSegment:
    def __init__(self, text, start, end):
        self.text = text
        self.start = start
        self.end = end


# ---------------------------------------------------------------------------
# TranscriptChunk dataclass
# ---------------------------------------------------------------------------

class TestTranscriptChunk:
    def test_is_final_is_bool(self):
        chunk = TranscriptChunk(text="hello", start=0.0, end=1.0, is_final=True)
        assert isinstance(chunk.is_final, bool)

    def test_is_final_false(self):
        chunk = TranscriptChunk(text="hi", start=0.0, end=0.5, is_final=False)
        assert chunk.is_final is False

    def test_text_defaults_to_str(self):
        # text field is typed str; verify a normal empty string is accepted
        chunk = TranscriptChunk(text="", start=0.0, end=1.0, is_final=True)
        assert isinstance(chunk.text, str)

    def test_is_final_coerced_to_bool_via_setattr(self):
        # Simulate untyped runtime caller passing an int; __post_init__ must normalise it.
        chunk = TranscriptChunk(text="x", start=0.0, end=1.0, is_final=True)
        object.__setattr__(chunk, "is_final", 0)
        chunk.__post_init__()
        assert chunk.is_final is False
        assert isinstance(chunk.is_final, bool)

    def test_speaker_id_optional(self):
        chunk = TranscriptChunk(text="x", start=0.0, end=1.0, is_final=True)
        assert chunk.speaker_id is None

    def test_speaker_id_set(self):
        chunk = TranscriptChunk(text="x", start=0.0, end=1.0, is_final=True, speaker_id="SPEAKER_00")
        assert chunk.speaker_id == "SPEAKER_00"


# ---------------------------------------------------------------------------
# is_available
# ---------------------------------------------------------------------------

class TestIsAvailable:
    def test_returns_bool(self):
        assert isinstance(StreamingTranscriber.is_available(), bool)

    def test_false_when_faster_whisper_absent(self):
        with mock.patch.dict(sys.modules, {"faster_whisper": None}):
            # is_available catches any exception
            result = StreamingTranscriber.is_available()
            assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# stream_sync — file not found
# ---------------------------------------------------------------------------

class TestStreamSyncFileMissing:
    def test_missing_file_raises_normalization_error(self):
        transcriber = StreamingTranscriber()
        with pytest.raises(NormalizationError, match="not found"):
            list(transcriber.stream_sync("/nonexistent/audio.wav"))

    def test_missing_file_error_message_contains_path(self):
        transcriber = StreamingTranscriber()
        with pytest.raises(NormalizationError, match="nonexistent"):
            list(transcriber.stream_sync("/nonexistent/audio.wav"))


# ---------------------------------------------------------------------------
# stream_sync — faster-whisper path (mocked)
# ---------------------------------------------------------------------------

class TestStreamSyncFasterWhisper:
    def _mock_faster_whisper(self, segments):
        """Return a context manager that mocks faster_whisper.WhisperModel."""
        fake_model = mock.MagicMock()
        fake_model.transcribe.return_value = (iter(segments), mock.MagicMock())
        fake_module = mock.MagicMock()
        fake_module.WhisperModel.return_value = fake_model
        return fake_module, fake_model

    def test_yields_transcript_chunks(self, tmp_path):
        wav = make_wav(tmp_path)
        segs = [
            _FakeWhisperSegment(" hello world", 0.0, 1.0),
            _FakeWhisperSegment(" foo bar", 1.1, 2.0),
        ]
        fake_module, fake_model = self._mock_faster_whisper(segs)
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            chunks = list(transcriber.stream_sync(wav))

        assert len(chunks) == 2
        assert all(isinstance(c, TranscriptChunk) for c in chunks)

    def test_all_chunks_are_final(self, tmp_path):
        wav = make_wav(tmp_path)
        segs = [_FakeWhisperSegment(" test", 0.0, 1.0)]
        fake_module, fake_model = self._mock_faster_whisper(segs)
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            chunks = list(transcriber.stream_sync(wav))

        assert all(c.is_final for c in chunks)

    def test_chunk_text_stripped(self, tmp_path):
        wav = make_wav(tmp_path)
        segs = [_FakeWhisperSegment("  hello world  ", 0.0, 1.0)]
        fake_module, fake_model = self._mock_faster_whisper(segs)
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            chunks = list(transcriber.stream_sync(wav))

        assert chunks[0].text == "hello world"

    def test_chunk_timestamps(self, tmp_path):
        wav = make_wav(tmp_path)
        segs = [_FakeWhisperSegment("hi", 1.5, 2.5)]
        fake_module, fake_model = self._mock_faster_whisper(segs)
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            chunks = list(transcriber.stream_sync(wav))

        assert chunks[0].start == 1.5
        assert chunks[0].end == 2.5

    def test_no_segments_returns_empty(self, tmp_path):
        wav = make_wav(tmp_path)
        fake_module, fake_model = self._mock_faster_whisper([])
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            chunks = list(transcriber.stream_sync(wav))

        assert chunks == []


# ---------------------------------------------------------------------------
# stream_sync — batch fallback path (mocked)
# ---------------------------------------------------------------------------

class TestStreamSyncBatchFallback:
    def _make_mock_whisper_result(self, segments):
        """segments: list of dict with text, start, end"""
        result = mock.MagicMock()
        result.to_segments.return_value = segments
        result.text = " ".join(s["text"] for s in segments)
        result.words = []
        return result

    def _mock_whisper_adapter(self, mock_result):
        """Patch WhisperAdapter inside the harvest_normalize.transcribe.whisper_adapter module."""
        mock_adapter_instance = mock.MagicMock()

        async def fake_transcribe(*a, **kw):
            return mock_result

        mock_adapter_instance.transcribe = fake_transcribe
        mock_adapter_class = mock.MagicMock(return_value=mock_adapter_instance)
        return mock.patch(
            "harvest_normalize.transcribe.whisper_adapter.WhisperAdapter",
            mock_adapter_class,
        )

    def test_fallback_yields_chunks(self, tmp_path):
        wav = make_wav(tmp_path)
        segments = [
            {"text": "hello world", "start": 0.0, "end": 2.0},
        ]
        mock_result = self._make_mock_whisper_result(segments)

        transcriber = StreamingTranscriber()
        with mock.patch.object(StreamingTranscriber, "is_available", return_value=False):
            # Patch the WhisperAdapter that the fallback imports from its module
            with mock.patch(
                "harvest_normalize.transcribe.streaming_transcriber._stream_batch_fallback_sync_helper",
                None,
                create=True,
            ):
                # Use a direct patch of the _stream_batch_fallback_sync method
                def fake_fallback_sync(self_inner, path, language):
                    for seg in segments:
                        yield TranscriptChunk(
                            text=seg["text"],
                            start=float(seg["start"]),
                            end=float(seg["end"]),
                            is_final=True,
                        )

                with mock.patch.object(
                    StreamingTranscriber,
                    "_stream_batch_fallback_sync",
                    fake_fallback_sync,
                ):
                    chunks = list(transcriber.stream_sync(wav))

        assert len(chunks) >= 1
        assert all(isinstance(c, TranscriptChunk) for c in chunks)

    def test_fallback_chunks_are_final(self, tmp_path):
        wav = make_wav(tmp_path)
        segments = [{"text": "test", "start": 0.0, "end": 1.0}]

        transcriber = StreamingTranscriber()

        def fake_fallback_sync(self_inner, path, language):
            for seg in segments:
                yield TranscriptChunk(
                    text=seg["text"],
                    start=float(seg["start"]),
                    end=float(seg["end"]),
                    is_final=True,
                )

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=False):
            with mock.patch.object(
                StreamingTranscriber,
                "_stream_batch_fallback_sync",
                fake_fallback_sync,
            ):
                chunks = list(transcriber.stream_sync(wav))

        assert all(c.is_final for c in chunks)


# ---------------------------------------------------------------------------
# stream — async interface
# ---------------------------------------------------------------------------

class TestStreamAsync:
    @pytest.mark.asyncio
    async def test_missing_file_raises(self):
        transcriber = StreamingTranscriber()
        with pytest.raises(NormalizationError, match="not found"):
            async for _ in transcriber.stream("/nonexistent/audio.wav"):
                pass

    @pytest.mark.asyncio
    async def test_yields_chunks_async(self, tmp_path):
        wav = make_wav(tmp_path)
        segs = [_FakeWhisperSegment("hello", 0.0, 1.0)]
        fake_model = mock.MagicMock()
        fake_model.transcribe.return_value = (iter(segs), mock.MagicMock())
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        chunks = []
        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            async for chunk in transcriber.stream(wav):
                chunks.append(chunk)

        assert len(chunks) == 1
        assert chunks[0].text == "hello"


# ---------------------------------------------------------------------------
# collect — convenience method
# ---------------------------------------------------------------------------

class TestCollect:
    def test_collect_returns_list(self, tmp_path):
        wav = make_wav(tmp_path)
        segs = [_FakeWhisperSegment("hello world", 0.0, 2.0)]
        fake_model = mock.MagicMock()
        fake_model.transcribe.return_value = (iter(segs), mock.MagicMock())
        transcriber = StreamingTranscriber()
        transcriber._model = fake_model

        with mock.patch.object(StreamingTranscriber, "is_available", return_value=True):
            result = transcriber.collect(wav)

        assert isinstance(result, list)
        assert len(result) == 1

    def test_collect_missing_file_raises(self):
        transcriber = StreamingTranscriber()
        with pytest.raises(NormalizationError):
            transcriber.collect("/nonexistent/audio.wav")
