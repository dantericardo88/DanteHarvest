"""
SpeakerDiarizer — multi-speaker attribution for audio transcripts.

Sprint 2 target: close transcription_quality gap (DH: 6 → 9 vs Screenpipe: 9).

Harvested from: Screenpipe pyannote.audio integration + OpenAdapt speaker patterns.

Assigns speaker labels (SPEAKER_00, SPEAKER_01, ...) to time segments.
Integrates with WhisperAdapter to produce per-speaker TranscriptWord sequences.

Constitutional guarantees:
- Local-first: pyannote runs locally; no API call without huggingface_token
- Fail-closed: missing pyannote raises NormalizationError with install instructions
- Zero-ambiguity: DiarizationSegment.speaker always str, never None
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from harvest_core.control.exceptions import NormalizationError


@dataclass
class DiarizationSegment:
    speaker: str
    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start


@dataclass
class DiarizationResult:
    segments: List[DiarizationSegment]
    speaker_count: int
    audio_path: str

    def speaker_at(self, timestamp: float) -> str:
        """Return speaker label for a given timestamp. Returns 'UNKNOWN' if no match."""
        for seg in self.segments:
            if seg.start <= timestamp <= seg.end:
                return seg.speaker
        return "UNKNOWN"

    def to_speaker_map(self) -> Dict[str, List[Tuple[float, float]]]:
        """Return dict of speaker → list of (start, end) intervals."""
        result: Dict[str, List[Tuple[float, float]]] = {}
        for seg in self.segments:
            result.setdefault(seg.speaker, []).append((seg.start, seg.end))
        return result


class SpeakerDiarizer:
    """
    Assign speaker labels to audio segments using pyannote.audio.

    Usage (local-first, requires pyannote.audio + model download):
        diarizer = SpeakerDiarizer(num_speakers=2)
        result = diarizer.diarize("session.wav")
        print(result.speaker_at(5.3))  # → "SPEAKER_00"

    Usage (with HuggingFace token for gated models):
        diarizer = SpeakerDiarizer(huggingface_token=os.environ["HF_TOKEN"])
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

    def diarize(self, audio_path: str | Path) -> DiarizationResult:
        """
        Run speaker diarization on an audio file.
        Raises NormalizationError if pyannote is not installed or file is missing.
        Fail-closed: never returns empty result on valid audio without attempting diarization.
        """
        path = Path(audio_path)
        if not path.exists():
            raise NormalizationError(f"Audio file not found: {path}")

        pipeline = self._load_pipeline()

        kwargs = {}
        if self.num_speakers:
            kwargs["num_speakers"] = self.num_speakers

        try:
            diarization = pipeline(str(path), **kwargs)
        except Exception as e:
            raise NormalizationError(f"Diarization failed: {e}") from e

        segments: List[DiarizationSegment] = []
        speakers = set()
        for turn, _, speaker in diarization.itertracks(yield_label=True):
            seg = DiarizationSegment(
                speaker=speaker,
                start=float(turn.start),
                end=float(turn.end),
            )
            segments.append(seg)
            speakers.add(speaker)

        segments.sort(key=lambda s: s.start)

        return DiarizationResult(
            segments=segments,
            speaker_count=len(speakers),
            audio_path=str(path),
        )

    def _load_pipeline(self):
        if self._pipeline is not None:
            return self._pipeline
        try:
            from pyannote.audio import Pipeline
        except ImportError as e:
            raise NormalizationError(
                "pyannote.audio not installed. Run: pip install pyannote.audio"
            ) from e
        try:
            self._pipeline = Pipeline.from_pretrained(
                self.model_name,
                use_auth_token=self.huggingface_token,
            )
        except Exception as e:
            raise NormalizationError(
                f"Failed to load diarization model '{self.model_name}': {e}. "
                "You may need a HuggingFace token for gated models: "
                "SpeakerDiarizer(huggingface_token='hf_...')"
            ) from e
        return self._pipeline


def annotate_words_with_speakers(
    words,
    diarization: DiarizationResult,
) -> list:
    """
    Annotate a list of TranscriptWord objects with speaker labels from diarization.
    Returns the same list (mutated in-place) with speaker_id set.
    Zero-ambiguity: speaker_id is always str ('UNKNOWN' when no match).
    """
    for word in words:
        mid = (word.start + word.end) / 2
        word.speaker_id = diarization.speaker_at(mid)
    return words
