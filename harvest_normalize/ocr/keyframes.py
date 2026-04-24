"""
Keyframe extraction helpers for video assets.

Transplanted from DanteDistillerV2/backend/transcribe/keyframes.py.
Import paths updated for DANTEHARVEST package layout.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List

try:
    import cv2  # type: ignore
except ImportError:
    class _UnavailableVideoCapture:
        def __init__(self, *_args, **_kwargs):
            self._opened = False
        def isOpened(self) -> bool:
            return False
        def read(self):
            return False, None
        def get(self, *_args, **_kwargs):
            return 0.0
        def release(self) -> None:
            return None
    cv2 = SimpleNamespace(CAP_PROP_FPS=5, VideoCapture=_UnavailableVideoCapture)  # type: ignore


class KeyframeExtractionError(Exception):
    """Raised when keyframe extraction fails."""


def compute_frame_hash(frame_data: bytes) -> str:
    return hashlib.sha256(frame_data).hexdigest()


def verify_keyframe_integrity(frame_data: bytes, expected_hash: str) -> bool:
    return compute_frame_hash(frame_data) == expected_hash


def _frame_bytes(frame: Any) -> bytes:
    if isinstance(frame, bytes):
        return frame
    if hasattr(frame, "tobytes"):
        return frame.tobytes()
    raise KeyframeExtractionError("Unsupported frame payload type")


def _open_capture(video_path: str):
    capture = cv2.VideoCapture(video_path)
    if not capture or not capture.isOpened():
        raise KeyframeExtractionError(f"Video file not found or unreadable: {video_path}")
    return capture


def extract_keyframes(
    video_path: str, interval: int = 10, max_frames: int = 50
) -> List[Dict[str, Any]]:
    """
    Extract keyframes from a video at the given frame interval.

    Args:
        video_path: Path to video file.
        interval: Extract 1 frame per `interval` frames.
        max_frames: Maximum frames to extract.

    Returns:
        List of dicts with frame_num, frame_data (bytes), hash, timestamp, etc.
    """
    if interval <= 0:
        raise KeyframeExtractionError("interval must be positive")
    if max_frames <= 0:
        return []

    capture = _open_capture(video_path)
    frames: List[Dict[str, Any]] = []
    fps = float(capture.get(getattr(cv2, "CAP_PROP_FPS", 5)) or 0.0)
    frame_num = 0

    try:
        while len(frames) < max_frames:
            success, frame = capture.read()
            if not success:
                break
            if frame_num % interval == 0:
                data = _frame_bytes(frame)
                frames.append({
                    "frame_num": frame_num,
                    "frame_data": data,
                    "hash": compute_frame_hash(data),
                    "timestamp": (frame_num / fps) if fps > 0 else 0.0,
                    "video_path": video_path,
                    "extracted_at": datetime.utcnow().isoformat(),
                })
            frame_num += 1
    finally:
        capture.release()

    return frames


def extract_keyframe_hashes(
    video_path: str, interval: int = 10, max_frames: int = 50
) -> List[Dict[str, Any]]:
    """Like extract_keyframes but omits raw frame_data bytes."""
    frames = extract_keyframes(video_path=video_path, interval=interval, max_frames=max_frames)
    return [
        {
            "frame_num": f["frame_num"],
            "hash": f["hash"],
            "timestamp": f["timestamp"],
            "video_path": f["video_path"],
            "extracted_at": f["extracted_at"],
        }
        for f in frames
    ]
