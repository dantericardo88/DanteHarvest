"""Tests for harvest_normalize.ocr.streaming_ocr."""
import asyncio
import os
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch


def _make_processor(ocr_text="hello world"):
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor
    mock_engine = MagicMock()
    mock_engine.extract_text.return_value = ocr_text
    return StreamingOCRProcessor(ocr_engine=mock_engine)


def test_streaming_ocr_result_has_text():
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRResult
    r = StreamingOCRResult(frame_index=0, timestamp_s=0.0, text="  hello  ")
    assert r.has_text is True


def test_streaming_ocr_result_no_text():
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRResult
    r = StreamingOCRResult(frame_index=0, timestamp_s=0.0, text="   ")
    assert r.has_text is False


def test_streaming_ocr_result_error_field():
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRResult
    r = StreamingOCRResult(frame_index=1, timestamp_s=1.0, text="", error="OCR failed")
    assert r.error == "OCR failed"
    assert r.has_text is False


def test_ocr_transcript_full_text():
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRResult, OCRTranscript
    results = [
        StreamingOCRResult(frame_index=0, timestamp_s=0.0, text="hello"),
        StreamingOCRResult(frame_index=1, timestamp_s=1.0, text="world"),
        StreamingOCRResult(frame_index=2, timestamp_s=2.0, text=""),  # blank, skipped
    ]
    t = OCRTranscript(source_path="test.mp4", results=results)
    text = t.full_text
    assert "hello" in text
    assert "world" in text
    assert "[0.0s]" in text
    assert "[1.0s]" in text


def test_ocr_transcript_frame_count():
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRResult, OCRTranscript
    results = [
        StreamingOCRResult(frame_index=i, timestamp_s=float(i), text=f"frame {i}")
        for i in range(5)
    ]
    t = OCRTranscript(source_path="test.mp4", results=results)
    assert t.frame_count == 5


def test_ocr_transcript_duration():
    from harvest_normalize.ocr.streaming_ocr import OCRTranscript
    import time
    t = OCRTranscript(source_path="test.mp4")
    t.started_at = 100.0
    t.completed_at = 105.0
    assert t.duration_s == pytest.approx(5.0)


def test_ocr_transcript_duration_none_before_complete():
    from harvest_normalize.ocr.streaming_ocr import OCRTranscript
    t = OCRTranscript(source_path="test.mp4")
    t.completed_at = None
    assert t.duration_s is None


@pytest.mark.asyncio
async def test_stream_images_processes_files(tmp_path):
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor

    # Create dummy image files
    img1 = tmp_path / "frame_001.png"
    img2 = tmp_path / "frame_002.png"
    img1.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
    img2.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

    mock_engine = MagicMock()
    mock_engine.extract_text.return_value = "sample text"

    processor = StreamingOCRProcessor(ocr_engine=mock_engine)
    results = []
    async for r in processor.stream_images(tmp_path, glob_pattern="*.png"):
        results.append(r)

    assert len(results) == 2
    assert all(r.text == "sample text" for r in results)


@pytest.mark.asyncio
async def test_stream_images_max_images(tmp_path):
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor

    for i in range(5):
        (tmp_path / f"frame_{i:03d}.png").write_bytes(b"\x89PNG" + b"\x00" * 20)

    mock_engine = MagicMock()
    mock_engine.extract_text.return_value = "text"

    processor = StreamingOCRProcessor(ocr_engine=mock_engine)
    results = []
    async for r in processor.stream_images(tmp_path, glob_pattern="*.png", max_images=2):
        results.append(r)

    assert len(results) == 2


@pytest.mark.asyncio
async def test_stream_images_ocr_error_still_yields(tmp_path):
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor

    img = tmp_path / "bad.png"
    img.write_bytes(b"not a real image")

    mock_engine = MagicMock()
    mock_engine.extract_text.side_effect = RuntimeError("tesseract failure")

    processor = StreamingOCRProcessor(ocr_engine=mock_engine)
    results = []
    async for r in processor.stream_images(tmp_path, glob_pattern="*.png"):
        results.append(r)

    assert len(results) == 1
    assert results[0].error is not None


@pytest.mark.asyncio
async def test_transcript_after_stream_images(tmp_path):
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor

    (tmp_path / "f1.png").write_bytes(b"\x89PNG" + b"\x00" * 20)

    mock_engine = MagicMock()
    mock_engine.extract_text.return_value = "transcript text"

    processor = StreamingOCRProcessor(ocr_engine=mock_engine)
    async for _ in processor.stream_images(tmp_path, glob_pattern="*.png"):
        pass

    t = processor.transcript()
    assert t.frame_count == 1
    assert "transcript text" in t.full_text
    assert t.completed_at is not None


@pytest.mark.asyncio
async def test_progress_callback_called(tmp_path):
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor

    (tmp_path / "img.png").write_bytes(b"\x89PNG" + b"\x00" * 20)

    mock_engine = MagicMock()
    mock_engine.extract_text.return_value = "cb text"

    calls = []
    processor = StreamingOCRProcessor(
        ocr_engine=mock_engine,
        progress_callback=lambda r: calls.append(r),
    )
    async for _ in processor.stream_images(tmp_path, glob_pattern="*.png"):
        pass

    assert len(calls) == 1
    assert calls[0].text == "cb text"


@pytest.mark.asyncio
async def test_streaming_ocr_no_cv2_raises_on_video():
    from harvest_normalize.ocr.streaming_ocr import StreamingOCRProcessor
    mock_engine = MagicMock()
    processor = StreamingOCRProcessor(ocr_engine=mock_engine)

    import sys
    import types
    # Simulate cv2 not being installed by inserting a broken module
    real_cv2 = sys.modules.pop("cv2", None)
    broken = types.ModuleType("cv2")

    def _raise(*a, **kw):
        raise ImportError("No module named 'cv2'")

    broken.VideoCapture = _raise
    sys.modules["cv2"] = broken
    try:
        with pytest.raises((ImportError, Exception)):
            async for _ in processor.stream("fake_video.mp4"):
                pass
    finally:
        if real_cv2 is not None:
            sys.modules["cv2"] = real_cv2
        else:
            sys.modules.pop("cv2", None)
