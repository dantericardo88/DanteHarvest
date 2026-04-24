"""
EPUBAdapter — EPUB to markdown conversion.

Harvested from: ebooklib patterns + Calibre open-source extraction logic.

Converts EPUB books to markdown. Each chapter becomes a level-2 heading
followed by the chapter body with HTML stripped.

Constitutional guarantees:
- Local-first: ebooklib runs locally; no network calls
- Fail-closed: missing file or import failure raises NormalizationError
- Zero-ambiguity: convert() always returns str (never None)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List


from harvest_core.control.exceptions import NormalizationError


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"&#39;", "'", text)
    text = re.sub(r"\s{3,}", "\n\n", text)
    return text.strip()


class EPUBAdapter:
    """
    Convert EPUB files to markdown.

    Usage:
        adapter = EPUBAdapter()
        markdown = adapter.convert(Path("book.epub"))
    """

    def convert(self, path: Path) -> str:
        """
        Convert EPUB to markdown string.
        Fail-closed: raises NormalizationError if file not found or ebooklib missing.
        """
        if not path.exists():
            raise NormalizationError(f"EPUB file not found: {path}")

        if path.suffix.lower() != ".epub":
            raise NormalizationError(
                f"Unsupported format: {path.suffix}. EPUBAdapter only handles .epub"
            )

        try:
            import ebooklib
            from ebooklib import epub
        except ImportError as e:
            raise NormalizationError(
                "ebooklib not installed. Run: pip install ebooklib"
            ) from e

        try:
            book = epub.read_epub(str(path))
        except Exception as e:
            raise NormalizationError(f"Failed to open EPUB {path}: {e}") from e

        title = book.get_metadata("DC", "title")
        book_title = title[0][0] if title else path.stem

        sections: List[str] = [f"# {book_title}\n"]

        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            content = item.get_content()
            if isinstance(content, bytes):
                content = content.decode("utf-8", errors="replace")

            chapter_name = item.get_name() or "Chapter"
            chapter_name = chapter_name.replace(".xhtml", "").replace(".html", "")
            chapter_name = chapter_name.split("/")[-1].replace("_", " ").title()

            body_match = re.search(r"<body[^>]*>(.*?)</body>", content, re.DOTALL | re.IGNORECASE)
            body = body_match.group(1) if body_match else content
            text = _strip_html(body)

            if text.strip():
                sections.append(f"## {chapter_name}\n\n{text}")

        return "\n\n".join(sections) if len(sections) > 1 else sections[0] if sections else ""
