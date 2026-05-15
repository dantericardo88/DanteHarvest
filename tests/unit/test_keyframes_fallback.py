"""Tests for imageio fallback path in harvest_normalize.ocr.keyframes."""

from __future__ import annotations

import hashlib
from unittest.mock import MagicMock, patch

import pytest

import harvest_normalize.ocr.keyframes as kf
from harvest_normalize.ocr.keyframes import (
    KeyframeExtractionError,
    _extract_keyframes_imageio,
    _ImageIOVideoReader,
    backend_available,
    extract_keyframe_hashes,
    extract_keyframes,
)


def _make_png_bytes(pixel: int = 128) -> bytes:
    from PIL import Image
    import io
    img = Image.new("RGB", (4, 4), color=(pixel, pixel, pixel))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class TestBackendAvailable:
    def test_returns_cv2_when_cv2_available(self):
        with patch.object(kf, "_CV2_AVAILABLE", True):
            assert backend_available() == "cv2"

    def test_returns_imageio_when_cv2_absent_imageio_present(self):
        with patch.object(kf, "_CV2_AVAILABLE", False), \
             patch.object(kf, "_imageio_available", return_value=True):
            assert backend_available() == "imageio"

    def test_returns_none_when_nothing_installed(self):
        with patch.object(kf, "_CV2_AVAILABLE", False), \
             patch.object(kf, "_imageio_available", return_value=False):
            assert backend_available() == "none"


class TestExtractKeyframesNoBackend:
    def test_raises_when_no_backend_available(self):
        with patch.object(kf, "_CV2_AVAILABLE", False), \
             patch.object(kf, "_imageio_available", return_value=False):
            with pytest.raises(KeyframeExtractionError, match="No video backend"):
                extract_keyframes("fake.mp4")

    def test_raises_on_nonpositive_interval(self):
        with pytest.raises(KeyframeExtractionError, match="interval must be positive"):
            extract_keyframes("fake.mp4", interval=0)

    def test_returns_empty_list_on_zero_max_frames(self):
        result = extract_keyframes("fake.mp4", max_frames=0)
        assert result == []


class TestExtractKeyframesImageioPath:
    def _make_mock_reader(self, frame_count: int = 30, fps: float = 25.0):
        frames = [_make_png_bytes(i % 256) for i in range(frame_count)]
        mock_reader = MagicMock()
        mock_reader.get_meta_data.return_value = {"fps": fps}
        mock_reader.__next__ = MagicMock(
            side_effect=frames + [StopIteration()]
        )
        return mock_reader, frames

    def test_extracts_correct_frame_count(self):
        mock_reader, raw_frames = self._make_mock_reader(frame_count=30)
        mock_imageio_reader = MagicMock()
        mock_imageio_reader.get_meta_data.return_value = {"fps": 25.0}

        call_count = 0
        png_frames = [_make_png_bytes(i % 256) for i in range(30)]

        def fake_next(reader):
            nonlocal call_count
            if call_count >= 30:
                raise StopIteration
            val = png_frames[call_count]
            call_count += 1
            return val

        with patch.object(kf, "_CV2_AVAILABLE", False), \
             patch.object(kf, "_imageio_available", return_value=True), \
             patch("harvest_normalize.ocr.keyframes._ImageIOVideoReader") as MockReader:
            instance = MockReader.return_value
            instance.fps = 25.0
            read_call_count = [0]
            def fake_read_frame():
                if read_call_count[0] >= 30:
                    return None
                data = _make_png_bytes(read_call_count[0] % 256)
                read_call_count[0] += 1
                return data
            instance.read_frame.side_effect = fake_read_frame
            instance.release = MagicMock()

            results = extract_keyframes("fake.mp4", interval=5, max_frames=6)

        assert len(results) == 6
        assert all("frame_data" in r for r in results)
        assert all("hash" in r for r in results)
        assert results[0]["frame_num"] == 0
        assert results[1]["frame_num"] == 5

    def test_frame_hashes_match_frame_data(self):
        with patch.object(kf, "_CV2_AVAILABLE", False), \
             patch.object(kf, "_imageio_available", return_value=True), \
             patch("harvest_normalize.ocr.keyframes._ImageIOVideoReader") as MockReader:
            instance = MockReader.return_value
            instance.fps = 10.0
            png = _make_png_bytes(200)
            call_count = [0]
            def fake_read():
                if call_count[0] >= 10:
                    return None
                call_count[0] += 1
                return png
            instance.read_frame.side_effect = fake_read
            instance.release = MagicMock()

            results = extract_keyframes("fake.mp4", interval=1, max_frames=3)

        for r in results:
            assert r["hash"] == hashlib.sha256(r["frame_data"]).hexdigest()

    def test_extract_keyframe_hashes_omits_frame_data(self):
        with patch.object(kf, "_CV2_AVAILABLE", False), \
             patch.object(kf, "_imageio_available", return_value=True), \
             patch("harvest_normalize.ocr.keyframes._ImageIOVideoReader") as MockReader:
            instance = MockReader.return_value
            instance.fps = 10.0
            png = _make_png_bytes(100)
            call_count = [0]
            def fake_read():
                if call_count[0] >= 5:
                    return None
                call_count[0] += 1
                return png
            instance.read_frame.side_effect = fake_read
            instance.release = MagicMock()

            results = extract_keyframe_hashes("fake.mp4", interval=1, max_frames=3)

        assert len(results) == 3
        assert all("frame_data" not in r for r in results)
        assert all("hash" in r for r in results)
        assert all("frame_num" in r for r in results)
        assert all("timestamp" in r for r in results)
        assert all("video_path" in r for r in results)
        assert all("extracted_at" in r for r in results)
