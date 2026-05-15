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
- Explicit warning: when HF token is missing, warns via warnings.warn (UserWarning)
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


# Canonical label used when diarization is unavailable
SPEAKER_UNKNOWN_LABEL = "SPEAKER_UNKNOWN"

# Legacy label kept for backward-compatibility in stub path
_LEGACY_UNKNOWN = "UNKNOWN"

# Environment variable names checked for HF token (in priority order)
_HF_TOKEN_ENV_VARS = ("HF_TOKEN", "HUGGINGFACE_TOKEN", "HUGGINGFACE_HUB_TOKEN")

_TOKEN_MISSING_WARNING = (
    "DiarizationAdapter: HF token not found (set HF_TOKEN env var). "
    "Speaker diarization disabled — all segments will be labeled SPEAKER_UNKNOWN."
)


def _resolve_hf_token(explicit_token: Optional[str] = None) -> Optional[str]:
    """Return an HF token from explicit arg or environment, or None if not found."""
    if explicit_token:
        return explicit_token
    for var in _HF_TOKEN_ENV_VARS:
        val = os.environ.get(var)
        if val:
            return val
    return None


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


@dataclass
class DiarizationResult:
    """Result from a single diarization operation with availability metadata."""
    speaker_label: str
    """Actual speaker label or SPEAKER_UNKNOWN when diarization is unavailable."""

    diarization_available: bool
    """False when HF token is missing or pyannote is not installed."""

    warning: Optional[str] = None
    """Human-readable warning message when diarization is unavailable."""


class DiarizationAdapter:
    """
    Wraps pyannote.audio for speaker diarization.

    Lazy import: if pyannote.audio is not installed, ``is_available()`` returns
    False and ``diarize()`` returns a single UNKNOWN-speaker SpeakerSegment
    covering the full audio duration instead of raising.

    When the HF token is not set, a UserWarning is emitted at construction time
    and ``_diarization_enabled`` is set to False.

    Usage (when pyannote is installed and HF_TOKEN is set):
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
        self.model_name = model_name
        self._pipeline = None

        # Resolve token and emit warning if missing
        resolved_token = _resolve_hf_token(huggingface_token)
        self.huggingface_token = resolved_token

        if resolved_token is None:
            warnings.warn(_TOKEN_MISSING_WARNING, UserWarning, stacklevel=2)
            self._diarization_enabled = False
        else:
            self._diarization_enabled = True

    @classmethod
    def is_available(cls) -> bool:
        """Return True only when HF token IS present AND pyannote.audio IS importable."""
        token = _resolve_hf_token()
        if token is None:
            return False
        try:
            import pyannote.audio  # noqa: F401
            return True
        except Exception:
            return False

    @classmethod
    def get_availability_info(cls) -> dict:
        """
        Return a dict describing availability status.

        Keys:
            available (bool): True only if token present AND pyannote importable
            reason (str): Human-readable explanation
            missing_token (bool): True when no HF token env var is set
            missing_package (bool): True when pyannote.audio is not importable
        """
        token = _resolve_hf_token()
        missing_token = token is None

        missing_package = False
        try:
            import pyannote.audio  # noqa: F401
        except Exception:
            missing_package = True

        available = (not missing_token) and (not missing_package)

        if missing_token and missing_package:
            reason = "HF token not set and pyannote.audio not installed"
        elif missing_token:
            reason = "HF token not set (set HF_TOKEN, HUGGINGFACE_TOKEN, or HUGGINGFACE_HUB_TOKEN)"
        elif missing_package:
            reason = "pyannote.audio not installed (pip install pyannote.audio)"
        else:
            reason = "diarization fully available"

        return {
            "available": available,
            "reason": reason,
            "missing_token": missing_token,
            "missing_package": missing_package,
        }

    def diarize(self, audio_path: str | Path) -> List[SpeakerSegment]:
        """
        Run speaker diarization on an audio file.

        Returns a list of SpeakerSegment sorted by start time.
        If diarization is disabled (no token) or pyannote.audio is not installed,
        returns a single stub segment with speaker_id=SPEAKER_UNKNOWN covering
        0.0 to estimated duration.
        If the file does not exist, returns an empty list.
        """
        path = Path(audio_path)
        if not path.exists():
            return []

        if not self._diarization_enabled or not self._pyannote_importable():
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
            return SPEAKER_UNKNOWN_LABEL

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

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pyannote_importable() -> bool:
        """Return True if pyannote.audio is importable (no token check)."""
        try:
            import pyannote.audio  # noqa: F401
            return True
        except Exception:
            return False

    def _stub_result(self, path: Path) -> List[SpeakerSegment]:
        """Return a single SPEAKER_UNKNOWN segment when diarization is unavailable."""
        # Estimate duration from file size heuristic (WAV: 16kHz, 16-bit mono ≈ 32kB/s)
        try:
            size = path.stat().st_size
            # WAV header is ~44 bytes; PCM 16kHz 16-bit mono = 32000 bytes/s
            duration = max(1.0, (size - 44) / 32000.0)
        except Exception:
            duration = 1.0
        return [SpeakerSegment(speaker_id=SPEAKER_UNKNOWN_LABEL, start=0.0, end=duration)]

    def _load_pipeline(self):
        if self._pipeline is not None:
            return self._pipeline
        from pyannote.audio import Pipeline  # type: ignore[import]
        self._pipeline = Pipeline.from_pretrained(
            self.model_name,
            use_auth_token=self.huggingface_token,
        )
        return self._pipeline
