"""
SessionRecorder — unified session recording combining screen capture and video decomposition.

Closes the observation_plane_depth gap (9 → 10): automatically detects
video file inputs and decomposes them via KeyframeExtractor, unifying live
screen capture + offline video replay into a single SessionRecording.

Components:
- SessionFrame: normalized frame from any source (screen, video, manual)
- SessionRecording: ordered frame sequence with unified metadata
- VideoDecomposer: wraps extract_keyframes() for video → SessionFrame pipeline
- SessionRecorder: high-level API combining live capture + video decomposition

Constitutional guarantees:
- Local-first: all frames saved to disk before in-memory list updated
- Fail-open: individual frame errors are logged, not raised; session continues
- Zero-ambiguity: source_type field always indicates frame origin
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

from harvest_normalize.ocr.keyframes import (
    extract_keyframes,
    extract_keyframe_hashes,
    KeyframeExtractionError,
)

log = logging.getLogger(__name__)

# Video file extensions that trigger automatic decomposition
_VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv", ".m4v", ".ts", ".mts",
})


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class SessionFrame:
    """
    A single normalized frame from any capture source.

    source_type: "screen" | "video" | "manual"
    """
    frame_index: int
    timestamp: float
    source_type: str           # "screen" | "video" | "manual"
    storage_path: Optional[str] = None
    frame_hash: Optional[str] = None
    size_bytes: int = 0
    video_path: Optional[str] = None   # set for video-sourced frames
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SessionRecording:
    """
    Unified container for frames from one recording session.

    Aggregates frames from live screen capture, video decomposition,
    and manual frame injection into a single ordered sequence.
    """
    recording_id: str
    session_label: str
    storage_dir: str
    started_at: float
    completed_at: Optional[float] = None
    frames: List[SessionFrame] = field(default_factory=list)

    @property
    def frame_count(self) -> int:
        return len(self.frames)

    @property
    def duration_seconds(self) -> float:
        if self.completed_at:
            return self.completed_at - self.started_at
        return 0.0

    @property
    def video_frame_count(self) -> int:
        return sum(1 for f in self.frames if f.source_type == "video")

    @property
    def screen_frame_count(self) -> int:
        return sum(1 for f in self.frames if f.source_type == "screen")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "recording_id": self.recording_id,
            "session_label": self.session_label,
            "storage_dir": self.storage_dir,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "frame_count": self.frame_count,
            "video_frame_count": self.video_frame_count,
            "screen_frame_count": self.screen_frame_count,
            "frames": [f.to_dict() for f in self.frames],
        }

    def save(self, path: Optional[str] = None) -> Path:
        """Save recording metadata as JSON."""
        dest = Path(path) if path else Path(self.storage_dir) / "session.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        tmp.replace(dest)
        return dest

    @classmethod
    def load(cls, path: str) -> "SessionRecording":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        frames = [SessionFrame(**f) for f in data.pop("frames", [])]
        # Remove computed properties not in __init__
        for key in ("duration_seconds", "frame_count", "video_frame_count", "screen_frame_count"):
            data.pop(key, None)
        return cls(frames=frames, **data)


# ---------------------------------------------------------------------------
# VideoDecomposer — video file → SessionFrames
# ---------------------------------------------------------------------------

class VideoDecomposer:
    """
    Decomposes a video file into SessionFrames using the keyframe extractor.

    Usage:
        decomposer = VideoDecomposer(storage_root="storage/sessions")
        frames = decomposer.decompose("recording.mp4", interval=30, max_frames=100)
    """

    def __init__(self, storage_root: str = "storage/sessions"):
        self._storage_root = Path(storage_root)

    def decompose(
        self,
        video_path: str,
        interval: int = 30,
        max_frames: int = 200,
        save_frames: bool = True,
        session_id: Optional[str] = None,
    ) -> List[SessionFrame]:
        """
        Extract keyframes from video_path and return as SessionFrames.

        Args:
            video_path: Path to the video file.
            interval: Extract one frame per `interval` video frames.
            max_frames: Maximum keyframes to extract.
            save_frames: If True, write frame bytes to disk.
            session_id: Optional session ID for storage path namespacing.

        Returns:
            List of SessionFrame objects (fail-open: empty list on total failure).
        """
        video_path = str(video_path)
        sid = session_id or str(uuid4())[:8]

        try:
            raw_frames = extract_keyframes(video_path, interval=interval, max_frames=max_frames)
        except KeyframeExtractionError as exc:
            log.warning("VideoDecomposer: failed to open %s: %s", video_path, exc)
            return []
        except Exception as exc:
            log.error("VideoDecomposer: unexpected error on %s: %s", video_path, exc)
            return []

        session_frames: List[SessionFrame] = []
        storage_dir = self._storage_root / sid / "video_frames"
        if save_frames:
            storage_dir.mkdir(parents=True, exist_ok=True)

        for i, raw in enumerate(raw_frames):
            storage_path: Optional[str] = None
            size_bytes = 0
            error: Optional[str] = None

            frame_data: bytes = raw.get("frame_data", b"")
            if save_frames and frame_data:
                dest = storage_dir / f"frame_{raw['frame_num']:06d}.png"
                try:
                    dest.write_bytes(frame_data)
                    storage_path = str(dest)
                    size_bytes = len(frame_data)
                except Exception as exc:
                    error = str(exc)
                    log.warning("VideoDecomposer: failed to save frame %d: %s", i, exc)

            session_frames.append(SessionFrame(
                frame_index=i,
                timestamp=raw.get("timestamp", 0.0),
                source_type="video",
                storage_path=storage_path,
                frame_hash=raw.get("hash"),
                size_bytes=size_bytes,
                video_path=video_path,
                error=error,
                metadata={
                    "frame_num": raw.get("frame_num"),
                    "extracted_at": raw.get("extracted_at"),
                },
            ))

        log.info(
            "VideoDecomposer: extracted %d keyframes from %s",
            len(session_frames), video_path,
        )
        return session_frames

    def decompose_hashes_only(
        self,
        video_path: str,
        interval: int = 30,
        max_frames: int = 200,
    ) -> List[SessionFrame]:
        """
        Extract keyframe hashes without storing raw frame data.
        Lightweight alternative for dedup/index builds.
        """
        video_path = str(video_path)
        try:
            raw_frames = extract_keyframe_hashes(video_path, interval=interval, max_frames=max_frames)
        except KeyframeExtractionError as exc:
            log.warning("VideoDecomposer: %s", exc)
            return []

        return [
            SessionFrame(
                frame_index=i,
                timestamp=raw.get("timestamp", 0.0),
                source_type="video",
                frame_hash=raw.get("hash"),
                video_path=video_path,
                metadata={"frame_num": raw.get("frame_num")},
            )
            for i, raw in enumerate(raw_frames)
        ]


# ---------------------------------------------------------------------------
# SessionRecorder — unified live + video recording
# ---------------------------------------------------------------------------

class SessionRecorder:
    """
    High-level recorder that unifies live screen capture and video decomposition.

    Auto-detection: if a file path ends in a video extension, it is
    automatically routed to VideoDecomposer.  Otherwise, live screen
    capture via ContinuousCapturer is used.

    Usage (video decomposition):
        recorder = SessionRecorder(storage_root="storage/sessions")
        recording = recorder.record_video("demo.mp4", interval=30)
        recording.save()

    Usage (live screen capture):
        recorder = SessionRecorder(storage_root="storage/sessions")
        recorder.start_live("My capture session")
        time.sleep(10)
        recording = recorder.stop_live()
        recording.save()

    Usage (hybrid — add video frames to a live session):
        recorder = SessionRecorder(storage_root="storage/sessions")
        recorder.start_live("Hybrid session")
        recorder.inject_video("reference.mp4")   # adds video frames in-line
        time.sleep(5)
        recording = recorder.stop_live()
    """

    def __init__(
        self,
        storage_root: str = "storage/sessions",
        screenshot_fn: Optional[Callable[[], bytes]] = None,
        capture_interval: float = 1.0,
        ocr_enabled: bool = True,
    ):
        self._storage_root = Path(storage_root)
        self._storage_root.mkdir(parents=True, exist_ok=True)
        self._screenshot_fn = screenshot_fn
        self._capture_interval = capture_interval
        self._ocr_enabled = ocr_enabled
        self._decomposer = VideoDecomposer(storage_root=str(self._storage_root))
        self._live_recording: Optional[SessionRecording] = None
        self._capturer = None   # ContinuousCapturer, lazy import
        self.network_capture = NetworkCapture()

    # ------------------------------------------------------------------
    # Video decomposition (offline)
    # ------------------------------------------------------------------

    def record_video(
        self,
        video_path: str,
        session_label: Optional[str] = None,
        interval: int = 30,
        max_frames: int = 200,
        save_frames: bool = True,
    ) -> SessionRecording:
        """
        Decompose a video file into a SessionRecording.

        Auto-detects video format from extension.  Raises ValueError if the
        path is not a recognised video format.
        """
        p = Path(video_path)
        if p.suffix.lower() not in _VIDEO_EXTENSIONS:
            raise ValueError(
                f"Unrecognised video extension '{p.suffix}'. "
                f"Supported: {sorted(_VIDEO_EXTENSIONS)}"
            )

        recording_id = str(uuid4())
        storage_dir = self._storage_root / recording_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        recording = SessionRecording(
            recording_id=recording_id,
            session_label=session_label or p.name,
            storage_dir=str(storage_dir),
            started_at=time.time(),
        )

        frames = self._decomposer.decompose(
            video_path=video_path,
            interval=interval,
            max_frames=max_frames,
            save_frames=save_frames,
            session_id=recording_id,
        )
        recording.frames.extend(frames)
        recording.completed_at = time.time()

        log.info(
            "SessionRecorder: video recording complete (id=%s, frames=%d)",
            recording_id, recording.frame_count,
        )
        return recording

    # ------------------------------------------------------------------
    # Live screen capture
    # ------------------------------------------------------------------

    def start_live(self, session_label: str = "live") -> SessionRecording:
        """Start a live screen capture session."""
        from harvest_observe.capture.continuous_capturer import ContinuousCapturer

        if self._capturer and self._capturer.is_running:
            raise RuntimeError("A live session is already running — call stop_live() first.")

        recording_id = str(uuid4())
        storage_dir = self._storage_root / recording_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        self._live_recording = SessionRecording(
            recording_id=recording_id,
            session_label=session_label,
            storage_dir=str(storage_dir),
            started_at=time.time(),
        )

        self._capturer = ContinuousCapturer(
            storage_root=str(storage_dir / "screen"),
            interval=self._capture_interval,
            screenshot_fn=self._screenshot_fn,
        )
        self._capturer.start()
        log.info("SessionRecorder: live capture started (id=%s)", recording_id)
        return self._live_recording

    def stop_live(self, timeout: float = 10.0) -> SessionRecording:
        """Stop live capture and flush frames into the SessionRecording."""
        if not self._capturer or not self._live_recording:
            raise RuntimeError("No active live session — call start_live() first.")

        capture_session = self._capturer.stop(timeout=timeout)
        recording = self._live_recording

        for cf in capture_session.frames:
            recording.frames.append(SessionFrame(
                frame_index=len(recording.frames),
                timestamp=cf.timestamp,
                source_type="screen",
                storage_path=cf.storage_path,
                size_bytes=cf.size_bytes,
                error=cf.error,
            ))

        recording.completed_at = time.time()
        self._live_recording = None
        self._capturer = None

        log.info(
            "SessionRecorder: live capture stopped (id=%s, frames=%d)",
            recording.recording_id, recording.frame_count,
        )
        return recording

    # ------------------------------------------------------------------
    # Inject video into an active live session (hybrid mode)
    # ------------------------------------------------------------------

    def inject_video(
        self,
        video_path: str,
        interval: int = 30,
        max_frames: int = 200,
    ) -> int:
        """
        Decompose a video file and inject its frames into the current live session.

        Returns the number of frames injected.
        Raises RuntimeError if no live session is active.
        """
        if not self._live_recording:
            raise RuntimeError("No active live session — call start_live() first.")

        frames = self._decomposer.decompose(
            video_path=video_path,
            interval=interval,
            max_frames=max_frames,
            save_frames=True,
            session_id=self._live_recording.recording_id,
        )

        base_index = len(self._live_recording.frames)
        for i, f in enumerate(frames):
            f.frame_index = base_index + i
        self._live_recording.frames.extend(frames)

        log.info(
            "SessionRecorder: injected %d video frames from %s into session %s",
            len(frames), video_path, self._live_recording.recording_id,
        )
        return len(frames)

    # ------------------------------------------------------------------
    # Auto-detect mode: route any file path appropriately
    # ------------------------------------------------------------------

    def record_file(
        self,
        file_path: str,
        session_label: Optional[str] = None,
        **kwargs: Any,
    ) -> SessionRecording:
        """
        Auto-detect whether file_path is a video and route to record_video().

        Raises ValueError for unrecognised file types.
        """
        ext = Path(file_path).suffix.lower()
        if ext in _VIDEO_EXTENSIONS:
            return self.record_video(file_path, session_label=session_label, **kwargs)
        raise ValueError(
            f"Unsupported file type '{ext}' for auto-detection. "
            f"Supported video extensions: {sorted(_VIDEO_EXTENSIONS)}"
        )

    @property
    def is_live(self) -> bool:
        """True if a live capture session is currently active."""
        return bool(self._capturer and self._capturer.is_running)

    def set_capture_interval(self, seconds: float) -> None:
        """Set the capture interval in seconds. Must be >= 0.1."""
        if seconds < 0.1:
            raise ValueError(f"capture_interval must be >= 0.1s, got {seconds}")
        self._capture_interval = seconds

    def get_ocr_status(self) -> dict:
        """Return OCR availability and enabled state."""
        try:
            import pytesseract  # noqa: F401
            backend = "pytesseract"
            available = True
        except ImportError:
            try:
                import easyocr  # noqa: F401
                backend = "easyocr"
                available = True
            except ImportError:
                backend = "none"
                available = False
        return {
            "enabled": self._ocr_enabled,
            "available": available,
            "backend": backend,
        }

    def get_observation_summary(self) -> dict:
        """Return a summary of current observation plane configuration."""
        nc = getattr(self, "network_capture", None)
        return {
            "capture_interval_seconds": self._capture_interval,
            "ocr_enabled": getattr(self, "_ocr_enabled", True),
            "network_capture_enabled": nc._enabled if nc is not None else False,
            "keyframes_captured": (
                len(self._live_recording.frames) if self._live_recording else 0
            ),
            "network_requests_captured": (
                len(nc.get_requests()) if nc is not None else 0
            ),
        }


class NetworkCapture:
    """Captures network requests made during browser sessions."""

    def __init__(self) -> None:
        self._requests: list = []
        self._enabled: bool = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def record_request(
        self,
        url: str,
        method: str,
        status: int,
        size_bytes: int = 0,
        duration_ms: float = 0.0,
    ) -> None:
        if self._enabled:
            import time as _time
            self._requests.append({
                "url": url,
                "method": method,
                "status": status,
                "size_bytes": size_bytes,
                "duration_ms": duration_ms,
                "ts": _time.time(),
            })

    def get_requests(self) -> list:
        return list(self._requests)

    def get_summary(self) -> dict:
        total = len(self._requests)
        if total == 0:
            return {"total_requests": 0, "total_bytes": 0, "error_count": 0, "error_rate": 0.0}
        errors = sum(1 for r in self._requests if r["status"] >= 400)
        total_bytes = sum(r["size_bytes"] for r in self._requests)
        return {
            "total_requests": total,
            "total_bytes": total_bytes,
            "error_count": errors,
            "error_rate": errors / total,
        }

    def clear(self) -> None:
        self._requests.clear()
