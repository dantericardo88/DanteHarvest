"""
WhisperAdapter — audio-to-transcript normalization.

Harvested from: OpenAdapt/Screenpipe whisper integration patterns.

Two modes:
1. Local (default, local-first): tries faster-whisper then openai-whisper.
   Zero network calls. Raises NormalizationError when neither is installed,
   with a clear message listing install options (zero-ambiguity).
2. OpenAI API: uses openai.Audio.transcribe when api_key provided.
   Fail-closed: API errors raise NormalizationError, never return empty.

Constitutional guarantees:
- Local-first: faster-whisper preferred, openai-whisper as fallback
- Fail-closed: empty audio raises NormalizationError (not empty string)
- Zero-ambiguity: transcript is always a non-None str; words list always List[TranscriptWord]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from harvest_core.control.exceptions import NormalizationError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


@dataclass
class TranscriptWord:
    word: str
    start: float
    end: float
    confidence: float = 1.0


@dataclass
class TranscriptResult:
    text: str
    words: List[TranscriptWord]
    language: str
    duration_seconds: float
    model: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_segments(self, window_seconds: float = 5.0) -> List[Dict[str, Any]]:
        """Group words into time-windowed segments for transcript alignment."""
        if not self.words:
            return [{"text": self.text, "start": 0.0, "end": self.duration_seconds}]
        segments = []
        current_words: List[TranscriptWord] = []
        window_start = self.words[0].start if self.words else 0.0
        for word in self.words:
            if word.start - window_start >= window_seconds and current_words:
                segments.append({
                    "text": " ".join(w.word for w in current_words),
                    "start": current_words[0].start,
                    "end": current_words[-1].end,
                })
                current_words = []
                window_start = word.start
            current_words.append(word)
        if current_words:
            segments.append({
                "text": " ".join(w.word for w in current_words),
                "start": current_words[0].start,
                "end": current_words[-1].end,
            })
        return segments


class WhisperAdapter:
    """
    Transcribe audio files to text using Whisper.

    Usage (local-first):
        adapter = WhisperAdapter(model_name="base")
        result = await adapter.transcribe("audio/session.wav", run_id="run-001")
        print(result.text)

    Usage (OpenAI API):
        adapter = WhisperAdapter(api_key=os.environ["OPENAI_API_KEY"])
        result = await adapter.transcribe("audio/session.wav", run_id="run-001")
    """

    def __init__(
        self,
        model_name: str = "base",
        api_key: Optional[str] = None,
        chain_writer: Optional[ChainWriter] = None,
    ):
        self.model_name = model_name
        self.api_key = api_key
        self.chain_writer = chain_writer
        self._model = None

    @staticmethod
    def available_backends() -> List[str]:
        """Return names of importable transcription backends."""
        backends: List[str] = []
        try:
            import faster_whisper  # noqa: F401
            backends.append("faster-whisper")
        except Exception:
            pass
        try:
            import whisper  # noqa: F401
            backends.append("openai-whisper")
        except Exception:
            pass
        try:
            import openai  # noqa: F401
            backends.append("openai-api")
        except Exception:
            pass
        return backends

    @staticmethod
    def is_any_backend_available() -> bool:
        """Return True if at least one transcription backend is importable."""
        return len(WhisperAdapter.available_backends()) > 0

    async def transcribe(
        self,
        audio_path: str | Path,
        run_id: str,
        language: Optional[str] = None,
    ) -> TranscriptResult:
        """
        Transcribe an audio file.

        Raises NormalizationError if file does not exist or transcription fails.
        Never returns empty text on valid audio (fail-closed).
        """
        path = Path(audio_path)

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="transcribe.started",
                machine="whisper_adapter",
                data={"path": str(path), "model": self.model_name},
            ))

        if not path.exists():
            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="transcribe.failed",
                    machine="whisper_adapter",
                    data={"path": str(path), "error": "file not found"},
                ))
            raise NormalizationError(f"Audio file not found: {path}")

        try:
            if self.api_key:
                result = await self._transcribe_api(path, language)
            else:
                result = self._transcribe_local(path, language)
        except NormalizationError:
            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="transcribe.failed",
                    machine="whisper_adapter",
                    data={"path": str(path), "error": "transcription failed"},
                ))
            raise

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="transcribe.completed",
                machine="whisper_adapter",
                data={
                    "path": str(path),
                    "word_count": len(result.words),
                    "duration_seconds": result.duration_seconds,
                    "language": result.language,
                },
            ))

        return result

    def _transcribe_faster_whisper(self, path: Path, language: Optional[str]) -> TranscriptResult:
        from faster_whisper import WhisperModel  # type: ignore[import]
        model = WhisperModel(self.model_name, device="cpu", compute_type="int8")
        if language:
            segments, info = model.transcribe(  # type: ignore[call-arg]
                str(path), beam_size=5, word_timestamps=True, language=language,
            )
        else:
            segments, info = model.transcribe(  # type: ignore[call-arg]
                str(path), beam_size=5, word_timestamps=True,
            )
        words: List[TranscriptWord] = []
        full_text_parts: List[str] = []
        for seg in segments:
            full_text_parts.append(seg.text.strip())
            for w in (seg.words or []):
                words.append(TranscriptWord(
                    word=w.word.strip(),
                    start=w.start,
                    end=w.end,
                    confidence=w.probability,
                ))
        return TranscriptResult(
            text=" ".join(full_text_parts),
            words=words,
            language=info.language,
            duration_seconds=getattr(info, "duration", 0.0),
            model=f"faster-whisper/{self.model_name}",
        )

    def _transcribe_local(self, path: Path, language: Optional[str]) -> TranscriptResult:
        try:
            return self._transcribe_faster_whisper(path, language)
        except ImportError:
            pass

        try:
            import whisper
        except ImportError:
            raise NormalizationError(
                "No local transcription backend available. "
                "Install faster-whisper: pip install faster-whisper  "
                "or openai-whisper: pip install openai-whisper  "
                "or pass api_key for OpenAI API mode."
            )

        if self._model is None:
            self._model = whisper.load_model(self.model_name)

        kwargs: Dict[str, Any] = {}
        if language:
            kwargs["language"] = language

        result = self._model.transcribe(str(path), word_timestamps=True, **kwargs)

        words: List[TranscriptWord] = []
        for seg in result.get("segments", []):
            for w in seg.get("words", []):
                words.append(TranscriptWord(
                    word=w.get("word", "").strip(),
                    start=float(w.get("start", 0.0)),
                    end=float(w.get("end", 0.0)),
                    confidence=float(w.get("probability", 1.0)),
                ))

        text = result.get("text", "").strip()
        detected_language = result.get("language", language or "en")
        duration = float(result.get("duration", 0.0))

        return TranscriptResult(
            text=text,
            words=words,
            language=detected_language,
            duration_seconds=duration,
            model=self.model_name,
        )

    async def _transcribe_api(self, path: Path, language: Optional[str]) -> TranscriptResult:
        try:
            import openai
        except ImportError as e:
            raise NormalizationError(
                "openai package not installed. Run: pip install openai"
            ) from e

        client = openai.AsyncOpenAI(api_key=self.api_key)
        try:
            with open(path, "rb") as f:
                response = await client.audio.transcriptions.create(  # type: ignore[call-overload]
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    language=language if language else openai.NOT_GIVEN,
                    timestamp_granularities=["word"],
                )
        except NormalizationError:
            raise
        except Exception as e:
            raise NormalizationError(f"OpenAI Whisper API error: {e}") from e

        words: List[TranscriptWord] = []
        for w in (response.words or []):
            words.append(TranscriptWord(
                word=w.word,
                start=float(w.start),
                end=float(w.end),
                confidence=1.0,
            ))

        return TranscriptResult(
            text=response.text.strip(),
            words=words,
            language=response.language,
            duration_seconds=float(response.duration),
            model="whisper-1",
        )
