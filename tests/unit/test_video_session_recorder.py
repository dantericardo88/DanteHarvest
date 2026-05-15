"""Tests for harvest_observe.capture.session_recorder (video decomposition + live)."""
import time
import pytest
from pathlib import Path
from unittest.mock import patch


def _make_recorder(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecorder
    return SessionRecorder(storage_root=str(tmp_path / "sessions"))


def _fake_screenshot() -> bytes:
    return b"\x89PNG\r\n\x1a\n" + b"\x00" * 50


def _make_fake_keyframes(n=3):
    return [
        {
            "frame_num": i * 30,
            "frame_data": b"\x89PNG" + bytes([i]) * 100,
            "hash": f"hash{i:04x}",
            "timestamp": float(i),
            "video_path": "test.mp4",
            "extracted_at": "2026-01-01T00:00:00",
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# SessionFrame
# ---------------------------------------------------------------------------

def test_session_frame_to_dict():
    from harvest_observe.capture.session_recorder import SessionFrame
    f = SessionFrame(frame_index=0, timestamp=1.0, source_type="video", frame_hash="abc")
    d = f.to_dict()
    assert d["frame_index"] == 0
    assert d["source_type"] == "video"
    assert d["frame_hash"] == "abc"


# ---------------------------------------------------------------------------
# SessionRecording
# ---------------------------------------------------------------------------

def test_session_recording_counts(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecording, SessionFrame
    rec = SessionRecording("r1", "test", str(tmp_path), started_at=time.time())
    rec.frames.append(SessionFrame(0, 1.0, "video"))
    rec.frames.append(SessionFrame(1, 2.0, "screen"))
    rec.frames.append(SessionFrame(2, 3.0, "video"))
    assert rec.frame_count == 3
    assert rec.video_frame_count == 2
    assert rec.screen_frame_count == 1


def test_session_recording_save_load(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecording, SessionFrame
    rec = SessionRecording("r1", "test session", str(tmp_path), started_at=1000.0, completed_at=1060.0)
    rec.frames.append(SessionFrame(0, 1000.0, "video", frame_hash="deadbeef"))
    saved = rec.save(str(tmp_path / "session.json"))
    assert saved.exists()

    restored = SessionRecording.load(str(saved))
    assert restored.recording_id == "r1"
    assert len(restored.frames) == 1
    assert restored.frames[0].frame_hash == "deadbeef"


def test_session_recording_duration(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecording
    rec = SessionRecording("r1", "t", str(tmp_path), started_at=100.0, completed_at=160.0)
    assert rec.duration_seconds == pytest.approx(60.0)


# ---------------------------------------------------------------------------
# VideoDecomposer
# ---------------------------------------------------------------------------

def test_video_decomposer_missing_file(tmp_path):
    from harvest_observe.capture.session_recorder import VideoDecomposer
    decomp = VideoDecomposer(storage_root=str(tmp_path))
    frames = decomp.decompose("nonexistent.mp4", save_frames=False)
    assert frames == []


def test_video_decomposer_with_mocked_extractor(tmp_path):
    from harvest_observe.capture.session_recorder import VideoDecomposer
    decomp = VideoDecomposer(storage_root=str(tmp_path))

    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(4)):
        frames = decomp.decompose("fake.mp4", save_frames=True)

    assert len(frames) == 4
    assert all(f.source_type == "video" for f in frames)
    assert frames[0].frame_hash == "hash0000"


def test_video_decomposer_saves_frames(tmp_path):
    from harvest_observe.capture.session_recorder import VideoDecomposer
    decomp = VideoDecomposer(storage_root=str(tmp_path))

    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(2)):
        frames = decomp.decompose("fake.mp4", save_frames=True, session_id="sess-001")

    assert all(f.storage_path is not None for f in frames)
    assert all(Path(f.storage_path).exists() for f in frames)


def test_video_decomposer_no_save_no_disk(tmp_path):
    from harvest_observe.capture.session_recorder import VideoDecomposer
    decomp = VideoDecomposer(storage_root=str(tmp_path))

    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(2)):
        frames = decomp.decompose("fake.mp4", save_frames=False)

    assert all(f.storage_path is None for f in frames)


def test_video_decomposer_hashes_only(tmp_path):
    from harvest_observe.capture.session_recorder import VideoDecomposer
    decomp = VideoDecomposer(storage_root=str(tmp_path))

    fake_hashes = [{"frame_num": i, "hash": f"h{i}", "timestamp": float(i), "video_path": "x"} for i in range(3)]
    with patch("harvest_observe.capture.session_recorder.extract_keyframe_hashes", return_value=fake_hashes):
        frames = decomp.decompose_hashes_only("fake.mp4")

    assert len(frames) == 3
    assert all(f.storage_path is None for f in frames)
    assert frames[0].frame_hash == "h0"


# ---------------------------------------------------------------------------
# SessionRecorder — video
# ---------------------------------------------------------------------------

def test_recorder_record_video(tmp_path):
    rec = _make_recorder(tmp_path)
    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(5)):
        recording = rec.record_video("demo.mp4")
    assert recording.frame_count == 5
    assert recording.video_frame_count == 5
    assert recording.completed_at is not None


def test_recorder_record_video_label(tmp_path):
    rec = _make_recorder(tmp_path)
    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(1)):
        recording = rec.record_video("demo.mp4", session_label="Login flow")
    assert recording.session_label == "Login flow"


def test_recorder_record_video_bad_extension(tmp_path):
    rec = _make_recorder(tmp_path)
    with pytest.raises(ValueError, match="Unrecognised video extension"):
        rec.record_video("document.pdf")


def test_recorder_record_file_routes_video(tmp_path):
    rec = _make_recorder(tmp_path)
    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(3)):
        recording = rec.record_file("demo.mp4")
    assert recording.frame_count == 3


def test_recorder_record_file_unsupported(tmp_path):
    rec = _make_recorder(tmp_path)
    with pytest.raises(ValueError, match="Unsupported file type"):
        rec.record_file("notes.txt")


def test_recorder_all_video_extensions(tmp_path):
    from harvest_observe.capture.session_recorder import _VIDEO_EXTENSIONS
    rec = _make_recorder(tmp_path)
    for ext in list(_VIDEO_EXTENSIONS)[:4]:
        with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(1)):
            recording = rec.record_video(f"file{ext}")
        assert recording.frame_count == 1


def test_recorder_recording_save_roundtrip(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecording
    rec = _make_recorder(tmp_path)
    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(2)):
        recording = rec.record_video("demo.mp4")
    saved = recording.save()
    restored = SessionRecording.load(str(saved))
    assert restored.frame_count == 2
    assert restored.video_frame_count == 2


# ---------------------------------------------------------------------------
# SessionRecorder — live capture
# ---------------------------------------------------------------------------

def test_recorder_live_start_stop(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecorder
    rec = SessionRecorder(
        storage_root=str(tmp_path / "sessions"),
        screenshot_fn=_fake_screenshot,
        capture_interval=0.05,
    )
    rec.start_live("test session")
    assert rec.is_live
    time.sleep(0.15)
    result = rec.stop_live()
    assert not rec.is_live
    assert result.screen_frame_count >= 1


def test_recorder_live_double_start_raises(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecorder
    rec = SessionRecorder(
        storage_root=str(tmp_path / "sessions"),
        screenshot_fn=_fake_screenshot,
        capture_interval=1.0,
    )
    rec.start_live("first")
    try:
        with pytest.raises(RuntimeError, match="already running"):
            rec.start_live("second")
    finally:
        rec.stop_live()


def test_recorder_stop_without_start_raises(tmp_path):
    rec = _make_recorder(tmp_path)
    with pytest.raises(RuntimeError, match="No active live session"):
        rec.stop_live()


def test_recorder_inject_video_into_live(tmp_path):
    from harvest_observe.capture.session_recorder import SessionRecorder
    rec = SessionRecorder(
        storage_root=str(tmp_path / "sessions"),
        screenshot_fn=_fake_screenshot,
        capture_interval=1.0,
    )
    rec.start_live("hybrid")
    with patch("harvest_observe.capture.session_recorder.extract_keyframes", return_value=_make_fake_keyframes(4)):
        injected = rec.inject_video("ref.mp4")
    assert injected == 4
    result = rec.stop_live()
    assert result.video_frame_count == 4


def test_recorder_inject_without_live_raises(tmp_path):
    rec = _make_recorder(tmp_path)
    with pytest.raises(RuntimeError, match="No active live session"):
        rec.inject_video("ref.mp4")
