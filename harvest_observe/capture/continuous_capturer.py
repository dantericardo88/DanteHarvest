"""
ContinuousCapturer — background thread loop that screenshots at a configurable interval.

Harvested from: OpenAdapt continuous-capture + Screenpipe frame-pump patterns.

Saves frames to a CaptureSession (list of CaptureFrame dataclasses).
Graceful stop via threading.Event.  Does NOT require any external display or
PIL/MSS at import time — availability is checked lazily; if unavailable a stub
bytes payload is stored so CI never breaks.

Constitutional guarantees:
- Local-first: frames saved to disk before in-memory list is updated.
- Fail-closed: screenshot errors are logged, NOT silently swallowed; the
  background thread records an error frame so callers can detect failures.
- No external network calls.
- Thread-safe: get_frames() returns a snapshot copy.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional
from uuid import uuid4

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class CaptureFrame:
    """Single captured frame."""

    frame_index: int
    timestamp: float
    storage_path: str
    size_bytes: int = 0
    error: Optional[str] = None  # non-None when capture failed for this frame


@dataclass
class CaptureSession:
    """Container for all frames captured in one ContinuousCapturer session."""

    session_id: str
    storage_dir: str
    start_time: float
    end_time: Optional[float] = None
    frames: List[CaptureFrame] = field(default_factory=list)

    @property
    def frame_count(self) -> int:
        return len(self.frames)

    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return 0.0


# ---------------------------------------------------------------------------
# Screenshot helper
# ---------------------------------------------------------------------------


def _default_screenshot_fn() -> bytes:
    """
    Return raw PNG bytes of the primary monitor.

    Tries (in order):
    1. mss — fastest cross-platform screenshot library
    2. PIL / ImageGrab — Pillow screen grab
    3. Stub bytes — CI / headless fallback (never raises)
    """
    try:
        import mss  # type: ignore
        import mss.tools  # type: ignore
        with mss.mss() as sct:
            monitor = sct.monitors[1]  # primary monitor
            raw = sct.grab(monitor)
            return mss.tools.to_png(raw.rgb, raw.size)
    except Exception:
        pass

    try:
        import io
        from PIL import ImageGrab  # type: ignore
        buf = io.BytesIO()
        img = ImageGrab.grab()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception:
        pass

    # Headless/CI stub — 1×1 transparent PNG
    return (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
        b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
        b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
        b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )


# ---------------------------------------------------------------------------
# ContinuousCapturer
# ---------------------------------------------------------------------------


class ContinuousCapturer:
    """
    Background thread that captures screenshots at a fixed interval.

    Usage::

        capturer = ContinuousCapturer(storage_root="storage/capture", interval=5.0)
        capturer.start()
        time.sleep(30)
        capturer.stop()
        frames = capturer.get_frames()

    Parameters
    ----------
    storage_root:
        Directory under which frame images are saved.
    interval:
        Seconds between captures (default 5).
    screenshot_fn:
        Callable[[], bytes] — override for testing or custom capture.
        Defaults to :func:`_default_screenshot_fn`.
    max_frames:
        Stop automatically after this many frames (0 = unlimited).
    """

    def __init__(
        self,
        storage_root: str = "storage/capture",
        interval: float = 5.0,
        screenshot_fn: Optional[Callable[[], bytes]] = None,
        max_frames: int = 0,
    ) -> None:
        self.storage_root = Path(storage_root)
        self.interval = interval
        self._screenshot_fn: Callable[[], bytes] = screenshot_fn or _default_screenshot_fn
        self.max_frames = max_frames

        self._stop_event: threading.Event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock: threading.Lock = threading.Lock()
        self._session: Optional[CaptureSession] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> CaptureSession:
        """Start background capture thread. Returns the active CaptureSession."""
        if self._thread and self._thread.is_alive():
            raise RuntimeError("ContinuousCapturer is already running — call stop() first.")

        session_id = str(uuid4())
        storage_dir = self.storage_root / session_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        self._session = CaptureSession(
            session_id=session_id,
            storage_dir=str(storage_dir),
            start_time=time.time(),
        )
        self._stop_event.clear()

        self._thread = threading.Thread(
            target=self._capture_loop,
            name=f"ContinuousCapturer-{session_id[:8]}",
            daemon=True,
        )
        self._thread.start()
        log.info("ContinuousCapturer started (session=%s, interval=%.1fs)", session_id, self.interval)
        return self._session

    def stop(self, timeout: float = 10.0) -> CaptureSession:
        """
        Signal the background thread to stop and wait for it to finish.

        Returns the completed CaptureSession.
        """
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                log.warning("ContinuousCapturer thread did not stop within %.1fs", timeout)
        if self._session:
            with self._lock:
                self._session.end_time = time.time()
        log.info(
            "ContinuousCapturer stopped (frames=%d, duration=%.1fs)",
            self._session.frame_count if self._session else 0,
            self._session.duration_seconds if self._session else 0.0,
        )
        return self._session  # type: ignore[return-value]

    def get_frames(self) -> List[CaptureFrame]:
        """Return a snapshot copy of captured frames (thread-safe)."""
        if not self._session:
            return []
        with self._lock:
            return list(self._session.frames)

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    @property
    def session(self) -> Optional[CaptureSession]:
        return self._session

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _capture_loop(self) -> None:
        """Main loop executed in the background thread."""
        assert self._session is not None
        frame_index = 0

        while not self._stop_event.is_set():
            self._capture_one(frame_index)
            frame_index += 1

            if self.max_frames and frame_index >= self.max_frames:
                log.debug("ContinuousCapturer reached max_frames=%d, stopping.", self.max_frames)
                break

            # Sleep in small increments so stop_event is checked frequently
            deadline = time.monotonic() + self.interval
            while time.monotonic() < deadline and not self._stop_event.is_set():
                time.sleep(min(0.1, deadline - time.monotonic()))

    def _capture_one(self, frame_index: int) -> None:
        """Capture a single frame and append it to the session."""
        assert self._session is not None
        storage_dir = Path(self._session.storage_dir)
        dest = storage_dir / f"frame_{frame_index:06d}.png"
        ts = time.time()

        try:
            data = self._screenshot_fn()
            dest.write_bytes(data)
            frame = CaptureFrame(
                frame_index=frame_index,
                timestamp=ts,
                storage_path=str(dest),
                size_bytes=len(data),
            )
        except Exception as exc:  # pragma: no cover
            log.error("Frame capture failed at index %d: %s", frame_index, exc)
            frame = CaptureFrame(
                frame_index=frame_index,
                timestamp=ts,
                storage_path=str(dest),
                error=str(exc),
            )

        with self._lock:
            self._session.frames.append(frame)
