"""Tests for WhisperAdapter — audio transcription normalization."""

import sys
import wave
import struct
import unittest.mock as mock
import pytest
from pathlib import Path

from harvest_normalize.transcribe.whisper_adapter import (
    WhisperAdapter,
    TranscriptResult,
    TranscriptWord,
)
from harvest_core.control.exceptions import NormalizationError
from harvest_core.provenance.chain_writer import ChainWriter


def make_wav(path: Path, duration_seconds: float = 1.0) -> Path:
    """Create a minimal valid WAV file."""
    sample_rate = 16000
    n_samples = int(sample_rate * duration_seconds)
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "w") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(struct.pack(f"<{n_samples}h", *([0] * n_samples)))
    return path


def test_missing_file_raises():
    adapter = WhisperAdapter()
    with pytest.raises(NormalizationError, match="not found"):
        import asyncio
        asyncio.run(adapter.transcribe("/nonexistent/audio.wav", run_id="r1"))


@pytest.mark.asyncio
async def test_missing_file_raises_async():
    adapter = WhisperAdapter()
    with pytest.raises(NormalizationError, match="not found"):
        await adapter.transcribe("/nonexistent/audio.wav", run_id="r1")


@pytest.mark.asyncio
async def test_chain_started_signal_on_missing_file(tmp_path):
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    adapter = WhisperAdapter(chain_writer=writer)
    with pytest.raises(NormalizationError):
        await adapter.transcribe("/nonexistent/audio.wav", run_id="r1")
    entries = writer.read_all()
    signals = [e.signal for e in entries]
    assert "transcribe.started" in signals


@pytest.mark.asyncio
async def test_whisper_not_installed_raises_normalization_error(tmp_path):
    """When whisper is not installed, NormalizationError must be raised (fail-closed)."""
    import sys
    import unittest.mock as mock

    wav_path = make_wav(tmp_path / "test.wav")
    adapter = WhisperAdapter(model_name="base")

    with mock.patch.dict(sys.modules, {"whisper": None, "faster_whisper": None}):
        with pytest.raises(NormalizationError):
            await adapter.transcribe(wav_path, run_id="r1")


def test_transcript_result_to_segments_no_words():
    result = TranscriptResult(
        text="hello world",
        words=[],
        language="en",
        duration_seconds=5.0,
        model="base",
    )
    segments = result.to_segments()
    assert len(segments) == 1
    assert segments[0]["text"] == "hello world"


def test_transcript_result_to_segments_with_words():
    words = [
        TranscriptWord("hello", 0.0, 0.5),
        TranscriptWord("world", 0.6, 1.0),
        TranscriptWord("invoice", 6.0, 6.5),
    ]
    result = TranscriptResult(
        text="hello world invoice",
        words=words,
        language="en",
        duration_seconds=7.0,
        model="base",
    )
    segments = result.to_segments(window_seconds=5.0)
    assert len(segments) == 2
    assert "hello" in segments[0]["text"]
    assert "invoice" in segments[1]["text"]


def test_available_backends_returns_list():
    backends = WhisperAdapter.available_backends()
    assert isinstance(backends, list)
    for name in backends:
        assert isinstance(name, str)


def test_is_any_backend_available_returns_bool():
    result = WhisperAdapter.is_any_backend_available()
    assert isinstance(result, bool)


def test_available_backends_includes_faster_whisper_when_importable():
    fake_fw = mock.MagicMock()
    with mock.patch.dict(sys.modules, {"faster_whisper": fake_fw, "whisper": None, "openai": None}):
        backends = WhisperAdapter.available_backends()
    assert "faster-whisper" in backends
    assert "openai-whisper" not in backends


def test_available_backends_includes_openai_whisper_when_importable():
    fake_whisper = mock.MagicMock()
    with mock.patch.dict(sys.modules, {"faster_whisper": None, "whisper": fake_whisper, "openai": None}):
        backends = WhisperAdapter.available_backends()
    assert "openai-whisper" in backends
    assert "faster-whisper" not in backends


def test_transcribe_local_tries_faster_whisper_first(tmp_path):
    wav_path = make_wav(tmp_path / "test.wav")
    adapter = WhisperAdapter(model_name="base")

    fake_word = mock.MagicMock()
    fake_word.word = "hello"
    fake_word.start = 0.0
    fake_word.end = 0.5
    fake_word.probability = 0.99

    fake_seg = mock.MagicMock()
    fake_seg.text = "hello"
    fake_seg.words = [fake_word]

    fake_info = mock.MagicMock()
    fake_info.language = "en"
    fake_info.duration = 1.0

    fake_model_instance = mock.MagicMock()
    fake_model_instance.transcribe.return_value = ([fake_seg], fake_info)

    fake_fw_module = mock.MagicMock()
    fake_fw_module.WhisperModel.return_value = fake_model_instance

    with mock.patch.dict(sys.modules, {"faster_whisper": fake_fw_module}):
        result = adapter._transcribe_local(wav_path, language=None)

    assert result.text == "hello"
    assert result.model.startswith("faster-whisper/")
    fake_fw_module.WhisperModel.assert_called_once()


def test_transcribe_local_falls_back_to_openai_whisper_when_faster_whisper_missing(tmp_path):
    wav_path = make_wav(tmp_path / "test.wav")
    adapter = WhisperAdapter(model_name="base")

    fake_whisper = mock.MagicMock()
    fake_whisper.load_model.return_value.transcribe.return_value = {
        "text": "world",
        "segments": [],
        "language": "en",
        "duration": 1.0,
    }

    with mock.patch.dict(sys.modules, {"faster_whisper": None, "whisper": fake_whisper}):
        result = adapter._transcribe_local(wav_path, language=None)

    assert result.text == "world"
    assert result.model == "base"


def test_transcribe_local_raises_when_no_backend_available(tmp_path):
    wav_path = make_wav(tmp_path / "test.wav")
    adapter = WhisperAdapter(model_name="base")

    with mock.patch.dict(sys.modules, {"faster_whisper": None, "whisper": None}):
        with pytest.raises(NormalizationError, match="No local transcription backend"):
            adapter._transcribe_local(wav_path, language=None)
