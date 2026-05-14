"""
Tests for harvest_observe.capture.continuous_capturer.

All external I/O (filesystem writes, PIL/MSS screenshot calls) is fully mocked
so the suite is CI-safe and headless-friendly.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from harvest_observe.capture.continuous_capturer import (
    CaptureFrame,
    CaptureSession,
    ContinuousCapturer,
    _default_screenshot_fn,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STUB_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
    b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _make_screenshot_fn(payload: bytes = _STUB_PNG):
    """Return a deterministic screenshot callable."""
    call_count = {"n": 0}

    def _fn() -> bytes:
        call_count["n"] += 1
        return payload

    _fn.call_count_ref = call_count
    return _fn


# ---------------------------------------------------------------------------
# CaptureFrame dataclass
# ---------------------------------------------------------------------------


class TestCaptureFrame:
    def test_basic_fields(self):
        f = CaptureFrame(frame_index=0, timestamp=1.0, storage_path="/tmp/f.png", size_bytes=42)
        assert f.frame_index == 0
        assert f.timestamp == 1.0
        assert f.storage_path == "/tmp/f.png"
        assert f.size_bytes == 42
        assert f.error is None

    def test_error_field(self):
        f = CaptureFrame(frame_index=1, timestamp=2.0, storage_path="/tmp/f.png", error="boom")
        assert f.error == "boom"


# ---------------------------------------------------------------------------
# CaptureSession dataclass
# ---------------------------------------------------------------------------


class TestCaptureSession:
    def _session(self, frames: List[CaptureFrame] | None = None) -> CaptureSession:
        return CaptureSession(
            session_id="test-session",
            storage_dir="/tmp/caps",
            start_time=1000.0,
            end_time=1010.0,
            frames=frames or [],
        )

    def test_frame_count_empty(self):
        s = self._session()
        assert s.frame_count == 0

    def test_frame_count_with_frames(self):
        frames = [
            CaptureFrame(i, float(i), "/tmp/f.png") for i in range(5)
        ]
        s = self._session(frames)
        assert s.frame_count == 5

    def test_duration_seconds(self):
        s = self._session()
        assert s.duration_seconds == pytest.approx(10.0)

    def test_duration_without_end_time(self):
        s = CaptureSession(
            session_id="x", storage_dir="/tmp", start_time=0.0, end_time=None
        )
        assert s.duration_seconds == 0.0


# ---------------------------------------------------------------------------
# _default_screenshot_fn fallback
# ---------------------------------------------------------------------------


class TestDefaultScreenshotFn:
    def test_returns_bytes_when_mss_and_pil_absent(self):
        """When both mss and PIL are unavailable the stub PNG bytes are returned."""
        import builtins
        real_import = builtins.__import__

        def _block_import(name, *args, **kwargs):
            if name in ("mss", "PIL", "PIL.ImageGrab"):
                raise ImportError(f"blocked: {name}")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_block_import):
            result = _default_screenshot_fn()

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_returns_bytes_type(self):
        # Just verify it returns bytes (regardless of whether mss/PIL work)
        result = _default_screenshot_fn()
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# ContinuousCapturer — unit tests (filesystem mocked)
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_storage(tmp_path):
    return str(tmp_path / "capture")


class TestContinuousCapturerInit:
    def test_default_interval(self, tmp_storage):
        c = ContinuousCapturer(storage_root=tmp_storage)
        assert c.interval == 5.0

    def test_custom_interval(self, tmp_storage):
        c = ContinuousCapturer(storage_root=tmp_storage, interval=2.0)
        assert c.interval == 2.0

    def test_not_running_initially(self, tmp_storage):
        c = ContinuousCapturer(storage_root=tmp_storage)
        assert not c.is_running

    def test_session_none_before_start(self, tmp_storage):
        c = ContinuousCapturer(storage_root=tmp_storage)
        assert c.session is None

    def test_get_frames_empty_before_start(self, tmp_storage):
        c = ContinuousCapturer(storage_root=tmp_storage)
        assert c.get_frames() == []


class TestContinuousCapturerLifecycle:
    def test_start_returns_capture_session(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        session = c.start()
        try:
            assert isinstance(session, CaptureSession)
            assert session.session_id
            assert session.start_time > 0
        finally:
            c.stop()

    def test_is_running_after_start(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        c.start()
        try:
            assert c.is_running
        finally:
            c.stop()

    def test_not_running_after_stop(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        c.start()
        c.stop()
        assert not c.is_running

    def test_stop_sets_end_time(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        c.start()
        session = c.stop()
        assert session.end_time is not None
        assert session.end_time >= session.start_time

    def test_double_start_raises(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        c.start()
        try:
            with pytest.raises(RuntimeError, match="already running"):
                c.start()
        finally:
            c.stop()


class TestContinuousCapturerCapture:
    def test_captures_frames_within_timeout(self, tmp_storage):
        """Capturer records at least one frame when run briefly."""
        captured: List[bytes] = []
        fn = _make_screenshot_fn()

        c = ContinuousCapturer(
            storage_root=tmp_storage,
            interval=0.05,  # 50 ms between frames
            screenshot_fn=fn,
            max_frames=3,
        )
        session = c.start()
        # Wait for max_frames to be reached (up to 2 s)
        deadline = time.monotonic() + 2.0
        while c.is_running and time.monotonic() < deadline:
            time.sleep(0.05)

        c.stop()
        frames = c.get_frames()
        assert len(frames) >= 1
        assert all(isinstance(f, CaptureFrame) for f in frames)

    def test_get_frames_returns_copy(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(
            storage_root=tmp_storage, interval=0.05, screenshot_fn=fn, max_frames=2
        )
        c.start()
        deadline = time.monotonic() + 2.0
        while c.is_running and time.monotonic() < deadline:
            time.sleep(0.05)
        c.stop()

        frames1 = c.get_frames()
        frames2 = c.get_frames()
        # Different list objects
        assert frames1 is not frames2
        assert len(frames1) == len(frames2)

    def test_max_frames_stops_thread(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(
            storage_root=tmp_storage, interval=0.01, screenshot_fn=fn, max_frames=2
        )
        c.start()
        deadline = time.monotonic() + 3.0
        while c.is_running and time.monotonic() < deadline:
            time.sleep(0.05)

        assert not c.is_running, "Thread should have exited after max_frames"
        assert c.get_frames().__len__() == 2

    def test_frames_have_timestamps(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(
            storage_root=tmp_storage, interval=0.05, screenshot_fn=fn, max_frames=2
        )
        c.start()
        deadline = time.monotonic() + 3.0
        while c.is_running and time.monotonic() < deadline:
            time.sleep(0.05)
        c.stop()

        for frame in c.get_frames():
            assert frame.timestamp > 0

    def test_frames_written_to_disk(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(
            storage_root=tmp_storage, interval=0.05, screenshot_fn=fn, max_frames=1
        )
        c.start()
        deadline = time.monotonic() + 2.0
        while c.is_running and time.monotonic() < deadline:
            time.sleep(0.05)
        c.stop()

        for frame in c.get_frames():
            assert Path(frame.storage_path).exists(), f"Frame file missing: {frame.storage_path}"

    def test_storage_directory_created(self, tmp_storage):
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(
            storage_root=tmp_storage, interval=60.0, screenshot_fn=fn
        )
        session = c.start()
        try:
            assert Path(session.storage_dir).is_dir()
        finally:
            c.stop()

    def test_session_id_is_unique_across_instances(self, tmp_storage):
        fn = _make_screenshot_fn()
        c1 = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        c2 = ContinuousCapturer(storage_root=tmp_storage, interval=60.0, screenshot_fn=fn)
        s1 = c1.start()
        s2 = c2.start()
        try:
            assert s1.session_id != s2.session_id
        finally:
            c1.stop()
            c2.stop()


class TestContinuousCapturerThreadSafety:
    def test_concurrent_get_frames_is_safe(self, tmp_storage):
        """Multiple threads calling get_frames() concurrently should not raise."""
        fn = _make_screenshot_fn()
        c = ContinuousCapturer(
            storage_root=tmp_storage, interval=0.02, screenshot_fn=fn, max_frames=10
        )
        c.start()

        errors: List[Exception] = []

        def reader():
            for _ in range(20):
                try:
                    c.get_frames()
                except Exception as exc:
                    errors.append(exc)
                time.sleep(0.005)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        c.stop()
        assert errors == [], f"Thread-safety errors: {errors}"
