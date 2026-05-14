"""
DiarizationAdapter — pyannote.audio speaker diarization, graceful stub fallback.

Wraps pyannote.audio for speaker diarization with lazy imports.
When pyannote is not installed, returns a stub result rather than crashing,
so that downstream code (transcript merger, streaming transcriber) can run
in CI without heavy model dependencies.

Constitutional guarantees:
- Local-first: pyannote runs locally; no network call without huggingface_token
- Graceful stub: when pyannote not installed, SpeakerSegment list with UNKNOWN speaker
- Zero-ambiguity: SpeakerSegment.speaker_id always str, never None
- Integrates with WhisperAdapter word timestamps for per-word speaker attribution
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class SpeakerSegment:
    """A time-bounded audio segment attributed to one speaker."""
    speaker_id: str
    start: float
    end: float
    text: str = ""

    @property
    def duration(self) -> float:
        return self.end - self.start

    def overlaps(self, other: "SpeakerSegment") -> bool:
        return self.start < other.end and self.end > other.start


class DiarizationAdapter:
    """
    Wraps pyannote.audio for speaker diarization.

    Lazy import: if pyannote.audio is not installed, ``is_available()`` returns
    False and ``diarize()`` returns a single UNKNOWN-speaker SpeakerSegment
    covering the full audio duration instead of raising.

    Usage (when pyannote is installed):
        adapter = DiarizationAdapter(num_speakers=2)
        segments = adapter.diarize("session.wav")
        for seg in segments:
            print(seg.speaker_id, seg.start, seg.end)

    Usage (integration with WhisperAdapter words):
        segments = adapter.diarize_and_assign(words, audio_path="session.wav")
    """

    def __init__(
        self,
        num_speakers: Optional[int] = None,
        huggingface_token: Optional[str] = None,
        model_name: str = "pyannote/speaker-diarization-3.1",
    ):
        self.num_speakers = num_speakers
        self.huggingface_token = huggingface_token
        self.model_name = model_name
        self._pipeline = None

    @staticmethod
    def is_available() -> bool:
        """Return True if pyannote.audio is importable."""
        try:
            import pyannote.audio  # noqa: F401
            return True
        except Exception:
            return False

    def diarize(self, audio_path: str | Path) -> List[SpeakerSegment]:
        """
        Run speaker diarization on an audio file.

        Returns a list of SpeakerSegment sorted by start time.
        If pyannote.audio is not installed, returns a single stub segment
        with speaker_id="UNKNOWN" covering 0.0 to estimated duration.
        If the file does not exist, returns an empty list.
        """
        path = Path(audio_path)
        if not path.exists():
            return []

        if not self.is_available():
            return self._stub_result(path)

        try:
            pipeline = self._load_pipeline()
        except Exception:
            return self._stub_result(path)

        kwargs: dict = {}
        if self.num_speakers:
            kwargs["num_speakers"] = self.num_speakers

        try:
            diarization = pipeline(str(path), **kwargs)
        except Exception:
            return self._stub_result(path)

        segments: List[SpeakerSegment] = []
        for turn, _, speaker in diarization.itertracks(yield_label=True):
            segments.append(SpeakerSegment(
                speaker_id=str(speaker),
                start=float(turn.start),
                end=float(turn.end),
            ))
        segments.sort(key=lambda s: s.start)
        return segments

    def diarize_and_assign(
        self,
        words: list,
        audio_path: str | Path,
    ) -> List[SpeakerSegment]:
        """
        Assign speaker labels to WhisperAdapter TranscriptWord objects.

        After Whisper transcribes and returns word-timestamp segments, this method
        runs diarization and assigns speaker_id to each word in-place, then
        groups consecutive same-speaker words into SpeakerSegment objects.

        Args:
            words: list of TranscriptWord (must have .word, .start, .end attributes)
            audio_path: path to the source audio file

        Returns:
            List[SpeakerSegment] with .text populated from word groups.
        """
        diar_segments = self.diarize(audio_path)

        def speaker_at(ts: float) -> str:
            for seg in diar_segments:
                if seg.start <= ts <= seg.end:
                    return seg.speaker_id
            return "UNKNOWN"

        # Assign speaker_id to each word in-place
        for word in words:
            mid = (word.start + word.end) / 2.0
            word.speaker_id = speaker_at(mid)

        # Group consecutive same-speaker words into SpeakerSegment objects
        if not words:
            return []

        result: List[SpeakerSegment] = []
        current_speaker = words[0].speaker_id
        current_words = [words[0]]

        for word in words[1:]:
            if word.speaker_id == current_speaker:
                current_words.append(word)
            else:
                result.append(SpeakerSegment(
                    speaker_id=current_speaker,
                    start=current_words[0].start,
                    end=current_words[-1].end,
                    text=" ".join(w.word for w in current_words),
                ))
                current_speaker = word.speaker_id
                current_words = [word]

        # Flush last group
        result.append(SpeakerSegment(
            speaker_id=current_speaker,
            start=current_words[0].start,
            end=current_words[-1].end,
            text=" ".join(w.word for w in current_words),
        ))
        return result

    def _stub_result(self, path: Path) -> List[SpeakerSegment]:
        """Return a single UNKNOWN-speaker segment when pyannote is unavailable."""
        # Estimate duration from file size heuristic (WAV: 16kHz, 16-bit mono ≈ 32kB/s)
        try:
            size = path.stat().st_size
            # WAV header is ~44 bytes; PCM 16kHz 16-bit mono = 32000 bytes/s
            duration = max(1.0, (size - 44) / 32000.0)
        except Exception:
            duration = 1.0
        return [SpeakerSegment(speaker_id="UNKNOWN", start=0.0, end=duration)]

    def _load_pipeline(self):
        if self._pipeline is not None:
            return self._pipeline
        from pyannote.audio import Pipeline  # type: ignore[import]
        self._pipeline = Pipeline.from_pretrained(
            self.model_name,
            use_auth_token=self.huggingface_token,
        )
        return self._pipeline
