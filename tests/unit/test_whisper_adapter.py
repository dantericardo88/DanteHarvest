"""Tests for WhisperAdapter — audio transcription normalization."""

import wave
import struct
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

    with mock.patch.dict(sys.modules, {"whisper": None}):
        with pytest.raises(NormalizationError, match="openai-whisper"):
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
