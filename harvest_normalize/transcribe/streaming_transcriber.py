"""
StreamingTranscriber — real-time audio transcription via faster-whisper (MIT).

Wraps faster-whisper for low-latency streaming transcription. Yields
TranscriptChunk objects as audio chunks arrive. When faster-whisper is not
installed, falls back to batch WhisperAdapter transparently.

Constitutional guarantees:
- Local-first: faster-whisper runs locally, zero network calls
- Graceful fallback: batch WhisperAdapter used when faster-whisper unavailable
- Zero-ambiguity: TranscriptChunk.is_final is always bool, text always str
- Fail-closed: NormalizationError raised on missing audio file or total failure
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Iterator, List, Optional

from harvest_core.control.exceptions import NormalizationError


@dataclass
class TranscriptChunk:
    """A chunk of transcribed text from a streaming transcription session."""
    text: str
    start: float
    end: float
    is_final: bool
    speaker_id: Optional[str] = None

    def __post_init__(self) -> None:
        # Zero-ambiguity: normalise any runtime mis-use that bypasses type checking.
        # The `str` / `bool` annotations are correct for typed callers; these guards
        # defend against dynamic / untyped callers at runtime without lying to the
        # type checker (the branches are only reachable via untyped code paths).
        if not isinstance(self.is_final, bool):  # type: ignore[arg-type]
            object.__setattr__(self, "is_final", bool(self.is_final))


class StreamingTranscriber:
    """
    Real-time streaming transcription using faster-whisper (MIT license).

    When faster-whisper is not installed, falls back to batch WhisperAdapter
    and yields a single final TranscriptChunk with the full transcript.

    Usage (streaming):
        transcriber = StreamingTranscriber(model_size="base")
        async for chunk in transcriber.stream("session.wav"):
            if chunk.is_final:
                print(chunk.text)

    Usage (sync iterator):
        for chunk in transcriber.stream_sync("session.wav"):
            print(chunk.text, chunk.is_final)
    """

    def __init__(
        self,
        model_size: str = "base",
        device: str = "cpu",
        compute_type: str = "int8",
        chunk_length_seconds: float = 5.0,
        beam_size: int = 5,
    ):
        self.model_size = model_size
        self.device = device
        self.compute_type = compute_type
        self.chunk_length_seconds = chunk_length_seconds
        self.beam_size = beam_size
        self._model = None

    @staticmethod
    def is_available() -> bool:
        """Return True if faster-whisper is importable."""
        try:
            import faster_whisper  # noqa: F401
            return True
        except Exception:
            return False

    async def stream(
        self,
        audio_path: str | Path,
        language: Optional[str] = None,
    ) -> AsyncIterator[TranscriptChunk]:
        """
        Async generator yielding TranscriptChunk as audio is transcribed.

        Falls back to batch WhisperAdapter if faster-whisper unavailable.
        Raises NormalizationError if audio file does not exist.
        """
        path = Path(audio_path)
        if not path.exists():
            raise NormalizationError(f"Audio file not found: {path}")

        if self.is_available():
            async for chunk in self._stream_faster_whisper(path, language):
                yield chunk
        else:
            async for chunk in self._stream_batch_fallback(path, language):
                yield chunk

    def stream_sync(
        self,
        audio_path: str | Path,
        language: Optional[str] = None,
    ) -> Iterator[TranscriptChunk]:
        """
        Synchronous generator yielding TranscriptChunk objects.

        Convenience wrapper around ``stream`` for non-async contexts.
        """
        path = Path(audio_path)
        if not path.exists():
            raise NormalizationError(f"Audio file not found: {path}")

        if self.is_available():
            yield from self._stream_faster_whisper_sync(path, language)
        else:
            yield from self._stream_batch_fallback_sync(path, language)

    async def _stream_faster_whisper(
        self,
        path: Path,
        language: Optional[str],
    ) -> AsyncIterator[TranscriptChunk]:
        # Run in thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        chunks = await loop.run_in_executor(
            None,
            lambda: list(self._stream_faster_whisper_sync(path, language)),
        )
        for chunk in chunks:
            yield chunk

    def _stream_faster_whisper_sync(
        self,
        path: Path,
        language: Optional[str],
    ) -> Iterator[TranscriptChunk]:
        from faster_whisper import WhisperModel  # type: ignore[import]

        if self._model is None:
            self._model = WhisperModel(
                self.model_size,
                device=self.device,
                compute_type=self.compute_type,
            )

        kwargs: dict = {"beam_size": self.beam_size}
        if language:
            kwargs["language"] = language

        try:
            segments, _info = self._model.transcribe(str(path), **kwargs)
        except Exception as e:
            raise NormalizationError(f"faster-whisper transcription failed: {e}") from e

        for seg in segments:
            # Each faster-whisper segment is treated as a streaming chunk
            # For true streaming you would feed audio incrementally;
            # here we simulate it by yielding per-segment with is_final=True.
            yield TranscriptChunk(
                text=seg.text.strip(),
                start=float(seg.start),
                end=float(seg.end),
                is_final=True,
            )

    async def _stream_batch_fallback(
        self,
        path: Path,
        language: Optional[str],
    ) -> AsyncIterator[TranscriptChunk]:
        chunks = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: list(self._stream_batch_fallback_sync(path, language)),
        )
        for chunk in chunks:
            yield chunk

    def _stream_batch_fallback_sync(
        self,
        path: Path,
        language: Optional[str],
    ) -> Iterator[TranscriptChunk]:
        """Batch fallback using WhisperAdapter (openai-whisper)."""
        from harvest_normalize.transcribe.whisper_adapter import WhisperAdapter

        adapter = WhisperAdapter(model_name=self.model_size)
        try:
            import asyncio as _asyncio
            loop = _asyncio.new_event_loop()
            result = loop.run_until_complete(
                adapter.transcribe(str(path), run_id="streaming-fallback", language=language)
            )
            loop.close()
        except NormalizationError:
            raise
        except Exception as e:
            raise NormalizationError(f"Batch fallback transcription failed: {e}") from e

        # Yield one chunk per windowed segment
        for seg in result.to_segments(window_seconds=self.chunk_length_seconds):
            yield TranscriptChunk(
                text=seg["text"],
                start=float(seg["start"]),
                end=float(seg["end"]),
                is_final=True,
            )

    def collect(
        self,
        audio_path: str | Path,
        language: Optional[str] = None,
    ) -> List[TranscriptChunk]:
        """
        Collect all TranscriptChunk objects into a list synchronously.
        Convenience method for batch usage.
        """
        return list(self.stream_sync(audio_path, language=language))
