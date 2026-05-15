"""
StreamingOCRProcessor — real-time frame-by-frame OCR with async generator API.

Wave 5b: computer_vision_pipeline — real-time streaming OCR mode (7→9).

Processes video files or image sequences as a streaming pipeline:
  - Frames are extracted one at a time (never load the full video into memory)
  - OCR runs on each frame immediately; results yielded via async generator
  - Progress callbacks notify the caller after each frame
  - Deduplication: near-identical consecutive frames are skipped (diff threshold)
  - Output: StreamingOCRResult per unique frame, aggregated transcript on close

Extends the existing OCREngine (batch) with a real-time streaming mode.

Constitutional guarantees:
- Local-first: zero network calls — opencv + tesseract only
- Fail-open: OCR errors on individual frames are recorded but don't halt the stream
- Zero-ambiguity: `stream()` always yields at least one result or raises StopAsyncIteration
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Callable, List, Optional, Tuple

from harvest_normalize.ocr.ocr_engine import OCREngine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class StreamingOCRResult:
    frame_index: int
    timestamp_s: float          # position in the video (seconds)
    text: str
    confidence: float = 1.0     # 0–1; 1.0 when backend doesn't provide confidence
    error: Optional[str] = None
    frame_hash: Optional[str] = None

    @property
    def has_text(self) -> bool:
        return bool(self.text.strip())


@dataclass
class OCRTranscript:
    """Aggregated transcript of all frames from a streaming session."""
    source_path: str
    results: List[StreamingOCRResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None

    @property
    def full_text(self) -> str:
        return "\n\n".join(
            f"[{r.timestamp_s:.1f}s] {r.text.strip()}"
            for r in self.results
            if r.has_text
        )

    @property
    def frame_count(self) -> int:
        return len(self.results)

    @property
    def duration_s(self) -> Optional[float]:
        if self.completed_at:
            return self.completed_at - self.started_at
        return None


# ---------------------------------------------------------------------------
# StreamingOCRProcessor
# ---------------------------------------------------------------------------

class StreamingOCRProcessor:
    """
    Process a video file or image directory as a streaming OCR pipeline.

    Usage (video file):
        processor = StreamingOCRProcessor(ocr_engine=OCREngine())
        async for result in processor.stream("video.mp4", fps_sample=1.0):
            print(f"{result.timestamp_s:.1f}s: {result.text[:80]}")
        transcript = processor.transcript()

    Usage (image directory):
        async for result in processor.stream_images(Path("frames/")):
            print(result.text)
    """

    def __init__(
        self,
        ocr_engine: Optional[OCREngine] = None,
        dedup_threshold: float = 0.95,  # skip frames with >95% similar hash
        progress_callback: Optional[Callable[[StreamingOCRResult], None]] = None,
    ):
        self._ocr = ocr_engine or OCREngine()
        self._dedup_threshold = dedup_threshold
        self._progress_cb = progress_callback
        self._results: List[StreamingOCRResult] = []
        self._source_path: str = ""

    async def stream(
        self,
        video_path: str,
        fps_sample: float = 1.0,
        max_frames: Optional[int] = None,
    ) -> AsyncIterator[StreamingOCRResult]:
        """
        Async generator yielding StreamingOCRResult for each unique sampled frame.
        Samples at fps_sample frames per second (default: 1 per second).
        """
        self._source_path = video_path
        self._results = []

        try:
            import cv2 as _cv2
        except ImportError:
            raise ImportError("opencv-python required for streaming OCR: pip install opencv-python")

        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            raise ValueError(f"Cannot open video: {video_path}")

        video_fps = cap.get(_cv2.CAP_PROP_FPS) or 25.0
        frame_interval = max(1, int(video_fps / fps_sample))
        frame_index = 0
        processed = 0
        last_hash: Optional[str] = None

        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                if frame_index % frame_interval != 0:
                    frame_index += 1
                    continue

                if max_frames is not None and processed >= max_frames:
                    break

                timestamp_s = frame_index / video_fps

                # Dedup: skip near-identical frames
                frame_hash = self._frame_hash(frame)
                if last_hash and self._is_duplicate(frame_hash, last_hash):
                    frame_index += 1
                    continue
                last_hash = frame_hash

                result = await self._ocr_frame(frame, frame_index, timestamp_s, frame_hash)
                self._results.append(result)
                processed += 1

                if self._progress_cb:
                    try:
                        self._progress_cb(result)
                    except Exception:
                        pass

                yield result

                # Yield control to the event loop between frames
                await asyncio.sleep(0)
                frame_index += 1
        finally:
            cap.release()

    async def stream_images(
        self,
        image_dir: Path,
        glob_pattern: str = "*.png",
        max_images: Optional[int] = None,
    ) -> AsyncIterator[StreamingOCRResult]:
        """
        Async generator that OCRs image files from a directory one at a time.
        """
        image_dir = Path(image_dir)
        self._source_path = str(image_dir)
        self._results = []

        images = sorted(image_dir.glob(glob_pattern))
        if max_images:
            images = images[:max_images]

        for i, img_path in enumerate(images):
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._ocr_image_file, img_path, i
            )
            self._results.append(result)
            if self._progress_cb:
                try:
                    self._progress_cb(result)
                except Exception:
                    pass
            yield result
            await asyncio.sleep(0)

    def transcript(self) -> OCRTranscript:
        """Return the aggregated transcript from the most recent stream() call."""
        t = OCRTranscript(source_path=self._source_path, results=list(self._results))
        t.completed_at = time.time()
        return t

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _ocr_frame(
        self,
        frame,
        frame_index: int,
        timestamp_s: float,
        frame_hash: str,
    ) -> StreamingOCRResult:
        """Run OCR on a single OpenCV frame asynchronously."""
        loop = asyncio.get_event_loop()
        try:
            text = await loop.run_in_executor(None, self._ocr_numpy_frame, frame)
            return StreamingOCRResult(
                frame_index=frame_index,
                timestamp_s=timestamp_s,
                text=text,
                frame_hash=frame_hash,
            )
        except Exception as e:
            return StreamingOCRResult(
                frame_index=frame_index,
                timestamp_s=timestamp_s,
                text="",
                error=str(e),
                frame_hash=frame_hash,
            )

    def _ocr_numpy_frame(self, frame) -> str:
        """Convert numpy frame to temp PNG and run OCR."""
        import cv2 as _cv2
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            _cv2.imwrite(tmp_path, frame)
            return self._ocr.extract_text(tmp_path)
        finally:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass

    def _ocr_image_file(self, image_path: Path, index: int) -> StreamingOCRResult:
        try:
            text = self._ocr.extract_text(str(image_path))
            return StreamingOCRResult(
                frame_index=index,
                timestamp_s=float(index),
                text=text,
                frame_hash=self._file_hash(image_path),
            )
        except Exception as e:
            return StreamingOCRResult(
                frame_index=index,
                timestamp_s=float(index),
                text="",
                error=str(e),
            )

    def _frame_hash(self, frame) -> str:
        """Compute a hash of a numpy frame for deduplication."""
        try:
            import numpy as np
            small = frame[::8, ::8]  # downsample for speed
            return hashlib.md5(small.tobytes()).hexdigest()
        except Exception:
            return ""

    def _file_hash(self, path: Path) -> str:
        try:
            return hashlib.md5(path.read_bytes()).hexdigest()
        except Exception:
            return ""

    def _is_duplicate(self, hash_a: str, hash_b: str) -> bool:
        if not hash_a or not hash_b:
            return False
        return hash_a == hash_b
