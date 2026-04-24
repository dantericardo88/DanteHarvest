"""
MarkItDown adapter — file-to-Markdown normalization.

Wraps Microsoft's MarkItDown library for converting PDF, DOCX, PPTX,
images, and other office formats to clean Markdown text.

Emits normalize.started and normalize.completed chain entries.
Fail-closed: unsupported or unreadable files raise NormalizationError.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from harvest_core.control.exceptions import NormalizationError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


_SUPPORTED_SUFFIXES = {
    ".pdf", ".docx", ".pptx", ".xlsx", ".xls", ".doc",
    ".html", ".htm", ".xml", ".csv", ".json",
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp",
    ".mp3", ".wav", ".m4a",
    ".txt", ".md", ".rst",
    ".zip",
}


class MarkdownResult:
    __slots__ = ("markdown", "char_count", "line_count", "source_path")

    def __init__(self, markdown: str, source_path: str):
        self.markdown = markdown
        self.char_count = len(markdown)
        self.line_count = markdown.count("\n")
        self.source_path = source_path


class MarkItDownAdapter:
    """
    Normalize files to Markdown using MarkItDown.

    Usage:
        adapter = MarkItDownAdapter(chain_writer)
        result = await adapter.convert(path=Path("report.pdf"), run_id="run-001")
        print(result.markdown)
    """

    def __init__(self, chain_writer: Optional[ChainWriter] = None):
        self.chain_writer = chain_writer
        self._md = None  # lazy-loaded

    def _get_converter(self):
        if self._md is None:
            try:
                from markitdown import MarkItDown
                self._md = MarkItDown()
            except ImportError as e:
                raise NormalizationError(
                    "MarkItDown not installed. Run: pip install markitdown"
                ) from e
        return self._md

    async def convert(
        self,
        path: Path,
        run_id: str,
        artifact_id: Optional[str] = None,
    ) -> MarkdownResult:
        """
        Convert a file to Markdown. Fail-closed on unsupported or unreadable files.
        Emits normalize.started → normalize.completed | normalize.failed.
        """
        path = Path(path)

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="normalize.started",
                machine="markitdown_adapter",
                data={"path": str(path), "artifact_id": artifact_id or ""},
            ))

        try:
            suffix = path.suffix.lower()
            if suffix not in _SUPPORTED_SUFFIXES:
                raise NormalizationError(
                    f"Unsupported file type '{suffix}' for MarkItDown conversion. "
                    f"Supported: {sorted(_SUPPORTED_SUFFIXES)}"
                )

            if not path.exists():
                raise NormalizationError(f"File not found: {path}")

            converter = self._get_converter()
            result = converter.convert(str(path))
            markdown = result.text_content or ""

            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="normalize.completed",
                    machine="markitdown_adapter",
                    data={
                        "path": str(path),
                        "artifact_id": artifact_id or "",
                        "char_count": len(markdown),
                        "line_count": markdown.count("\n"),
                    },
                ))

            return MarkdownResult(markdown=markdown, source_path=str(path))

        except NormalizationError:
            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="normalize.failed",
                    machine="markitdown_adapter",
                    data={"path": str(path), "error": "normalization_error"},
                ))
            raise
        except Exception as e:
            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="normalize.failed",
                    machine="markitdown_adapter",
                    data={"path": str(path), "error": str(e)},
                ))
            raise NormalizationError(f"MarkItDown conversion failed for {path}: {e}") from e

    def convert_sync(self, path: Path) -> str:
        """Synchronous conversion without chain entries — for use in pipelines."""
        path = Path(path)
        suffix = path.suffix.lower()
        if suffix not in _SUPPORTED_SUFFIXES:
            raise NormalizationError(f"Unsupported file type '{suffix}'")
        converter = self._get_converter()
        result = converter.convert(str(path))
        return result.text_content or ""
