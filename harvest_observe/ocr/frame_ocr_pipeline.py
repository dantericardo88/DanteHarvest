"""
FrameOCRPipeline — batch OCR over a list of CaptureFrame objects.

Harvested from: OpenAdapt OCR stage + Screenpipe frame-text-extraction patterns.

Runs OCR on each frame using pytesseract when available, falls back to a
placeholder so CI / headless environments never fail.

Supports configurable concurrency via a ThreadPoolExecutor.

Constitutional guarantees:
- Fail-closed on import: missing pytesseract/PIL is handled gracefully with a
  descriptive placeholder, never silent None.
- Local-first: reads frame data from disk paths stored in CaptureFrame.
- No external network calls.
- Thread-safe: each worker operates on an independent frame; shared state is
  only the output list assembled after all futures complete.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from harvest_observe.capture.continuous_capturer import CaptureFrame

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class OcrFrame:
    """OCR result for a single captured frame."""

    timestamp: float
    text: str
    confidence: float  # 0.0–1.0; 0.0 when OCR unavailable or frame errored
    frame_index: int = -1
    storage_path: str = ""
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# OCR helpers
# ---------------------------------------------------------------------------

_PYTESSERACT_AVAILABLE: Optional[bool] = None


def _check_pytesseract() -> bool:
    global _PYTESSERACT_AVAILABLE
    if _PYTESSERACT_AVAILABLE is None:
        try:
            import pytesseract  # noqa: F401  # type: ignore
            from PIL import Image  # noqa: F401  # type: ignore
            _PYTESSERACT_AVAILABLE = True
        except ImportError:
            _PYTESSERACT_AVAILABLE = False
    return _PYTESSERACT_AVAILABLE  # type: ignore[return-value]


def _ocr_path(path: str) -> tuple[str, float]:
    """
    Run OCR on a single image file.

    Returns (text, confidence).  If pytesseract or PIL are unavailable returns
    a placeholder string with confidence 0.0.
    """
    if not _check_pytesseract():
        return ("[ocr-unavailable]", 0.0)

    try:
        import pytesseract  # type: ignore
        from PIL import Image  # type: ignore

        img = Image.open(path)
        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
        words = [w for w in data["text"] if w.strip()]
        confs = [
            c for c, w in zip(data["conf"], data["text"])
            if w.strip() and isinstance(c, (int, float)) and int(c) >= 0
        ]
        text = " ".join(words)
        confidence = (sum(confs) / len(confs) / 100.0) if confs else 0.0
        return (text, float(min(max(confidence, 0.0), 1.0)))
    except Exception as exc:
        log.warning("OCR failed for %s: %s", path, exc)
        return (f"[ocr-error: {exc}]", 0.0)


# ---------------------------------------------------------------------------
# FrameOCRPipeline
# ---------------------------------------------------------------------------


class FrameOCRPipeline:
    """
    Batch OCR pipeline over a list of CaptureFrame objects.

    Usage::

        pipeline = FrameOCRPipeline(max_workers=4)
        frames: List[CaptureFrame] = capturer.get_frames()
        results: List[OcrFrame] = pipeline.run(frames)

    Parameters
    ----------
    max_workers:
        Thread-pool concurrency (default 4).  Set to 1 for sequential.
    """

    def __init__(self, max_workers: int = 4) -> None:
        if max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {max_workers}")
        self.max_workers = max_workers

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, frames: List[CaptureFrame]) -> List[OcrFrame]:
        """
        Run OCR on every frame in *frames*.

        Returns a list of OcrFrame objects in the same order as the input.
        """
        if not frames:
            return []

        results: List[Optional[OcrFrame]] = [None] * len(frames)

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ocr-worker") as pool:
            future_to_idx = {
                pool.submit(self._process_frame, frame): idx
                for idx, frame in enumerate(frames)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:  # pragma: no cover
                    frame = frames[idx]
                    results[idx] = OcrFrame(
                        timestamp=frame.timestamp,
                        text="[pipeline-error]",
                        confidence=0.0,
                        frame_index=frame.frame_index,
                        storage_path=frame.storage_path,
                        error=str(exc),
                    )

        # filter out any Nones (should not happen, but be safe)
        return [r for r in results if r is not None]

    def run_single(self, frame: CaptureFrame) -> OcrFrame:
        """Convenience: OCR a single frame synchronously."""
        return self._process_frame(frame)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _process_frame(self, frame: CaptureFrame) -> OcrFrame:
        if frame.error:
            return OcrFrame(
                timestamp=frame.timestamp,
                text="[capture-error]",
                confidence=0.0,
                frame_index=frame.frame_index,
                storage_path=frame.storage_path,
                error=frame.error,
            )

        path = frame.storage_path
        if not Path(path).exists():
            return OcrFrame(
                timestamp=frame.timestamp,
                text="[frame-not-found]",
                confidence=0.0,
                frame_index=frame.frame_index,
                storage_path=path,
                error=f"File not found: {path}",
            )

        text, confidence = _ocr_path(path)
        return OcrFrame(
            timestamp=frame.timestamp,
            text=text,
            confidence=confidence,
            frame_index=frame.frame_index,
            storage_path=path,
        )
