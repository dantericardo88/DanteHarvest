"""
OCREngine — text extraction from images and scanned PDFs.

Transplanted from DanteDistillerV2/backend/machines/normalize/ocr_engine.py.
Extended with abstract engine interface so Harvest can swap OCR backends.
"""

import logging
from pathlib import Path
from typing import List, Optional, Protocol

logger = logging.getLogger(__name__)


class OCRBackend(Protocol):
    """Abstract OCR backend interface for swappable engines."""

    def extract_text(self, image_path: str) -> str: ...


class TesseractBackend:
    """Default backend using pytesseract + Pillow."""

    def __init__(self, language: str = "eng"):
        self.language = language

    def extract_text(self, image_path: str) -> str:
        import pytesseract
        from PIL import Image
        img = Image.open(image_path)
        if img.mode != "RGB":
            img = img.convert("RGB")
        return pytesseract.image_to_string(img, lang=self.language)


class OCREngine:
    """
    OCR engine for Harvest normalization plane.

    Accepts an abstract backend so callers can inject alternative OCR
    implementations (EasyOCR, PaddleOCR, etc.) without changing the interface.
    """

    def __init__(self, backend: Optional[OCRBackend] = None, language: str = "eng"):
        self._backend: OCRBackend = backend or TesseractBackend(language=language)

    def extract_text(self, image_path: str) -> str:
        """Extract text from a single image file."""
        try:
            text = self._backend.extract_text(image_path)
            logger.debug("Extracted %d chars from %s", len(text), image_path)
            return text
        except Exception as e:
            logger.error("OCR failed for %s: %s", image_path, e)
            raise

    def extract_text_batch(self, image_paths: List[str]) -> List[str]:
        """Extract text from multiple images; failed paths return empty string."""
        results = []
        for path in image_paths:
            try:
                results.append(self.extract_text(path))
            except Exception as e:
                logger.warning("OCR failed for %s: %s", path, e)
                results.append("")
        return results

    def extract_with_metadata(self, image_path: str) -> dict:
        """Extract text and return structured result with path and char count."""
        text = self.extract_text(image_path)
        return {
            "image_path": image_path,
            "text": text,
            "char_count": len(text),
            "line_count": text.count("\n"),
        }
