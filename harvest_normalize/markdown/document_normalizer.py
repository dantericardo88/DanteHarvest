"""
DocumentNormalizer — unified dispatcher for file-to-markdown normalization.

Routes files to the best available adapter by suffix:
  .xlsx / .xls / .xlsm / .csv → XLSXAdapter  (openpyxl, structured tables)
  .epub                        → EPUBAdapter   (ebooklib, chapter extraction)
  everything else              → MarkItDownAdapter

Constitutional guarantees:
- Fail-closed: unsupported or unreadable files raise NormalizationError
- Zero-ambiguity: always returns str on valid input
- Local-first: all adapters run locally with no network calls
- Chain entries emitted for every dispatch path
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from harvest_core.control.exceptions import NormalizationError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_normalize.markdown.markitdown_adapter import MarkItDownAdapter, MarkdownResult
from harvest_normalize.markdown.xlsx_adapter import XLSXAdapter


_XLSX_SUFFIXES = {".xlsx", ".xls", ".xlsm", ".csv"}
_EPUB_SUFFIXES = {".epub"}


class DocumentNormalizer:
    """
    Unified file-to-markdown normalizer.

    Usage:
        normalizer = DocumentNormalizer(chain_writer)
        result = await normalizer.convert(Path("report.xlsx"), run_id="run-001")
        print(result.markdown)
    """

    def __init__(self, chain_writer: Optional[ChainWriter] = None):
        self.chain_writer = chain_writer
        self._xlsx = XLSXAdapter()
        self._markitdown: Optional[MarkItDownAdapter] = None
        self._epub: Optional[object] = None  # lazy — ebooklib optional

    def _get_markitdown(self) -> MarkItDownAdapter:
        if self._markitdown is None:
            self._markitdown = MarkItDownAdapter(chain_writer=None)
        return self._markitdown

    async def convert(
        self,
        path: Path,
        run_id: str,
        artifact_id: Optional[str] = None,
    ) -> MarkdownResult:
        """
        Convert any supported file to Markdown.
        Dispatches to the best adapter for the suffix.
        Emits normalize.started → normalize.completed | normalize.failed.
        """
        path = Path(path)

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="normalize.started",
                machine="document_normalizer",
                data={"path": str(path), "artifact_id": artifact_id or ""},
            ))

        try:
            suffix = path.suffix.lower()

            if suffix in _XLSX_SUFFIXES:
                markdown = self._xlsx.convert(path)
                adapter_used = "xlsx_adapter"
            elif suffix in _EPUB_SUFFIXES:
                markdown = self._convert_epub(path)
                adapter_used = "epub_adapter"
            else:
                md = self._get_markitdown()
                result = await md.convert(path, run_id="__inner__", artifact_id=artifact_id)
                markdown = result.markdown
                adapter_used = "markitdown_adapter"

            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="normalize.completed",
                    machine="document_normalizer",
                    data={
                        "path": str(path),
                        "artifact_id": artifact_id or "",
                        "adapter": adapter_used,
                        "char_count": len(markdown),
                    },
                ))

            return MarkdownResult(markdown=markdown, source_path=str(path))

        except NormalizationError:
            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="normalize.failed",
                    machine="document_normalizer",
                    data={"path": str(path), "error": "normalization_error"},
                ))
            raise
        except Exception as e:
            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="normalize.failed",
                    machine="document_normalizer",
                    data={"path": str(path), "error": str(e)},
                ))
            raise NormalizationError(f"Normalization failed for {path}: {e}") from e

    def _convert_epub(self, path: Path) -> str:
        try:
            from harvest_normalize.markdown.epub_adapter import EPUBAdapter
        except ImportError as e:
            raise NormalizationError("EPUBAdapter not available") from e
        adapter = EPUBAdapter()
        return adapter.convert(path)

    def convert_sync(self, path: Path) -> str:
        """Synchronous conversion without chain entries — for pipeline use."""
        path = Path(path)
        suffix = path.suffix.lower()
        if suffix in _XLSX_SUFFIXES:
            return self._xlsx.convert(path)
        if suffix in _EPUB_SUFFIXES:
            return self._convert_epub(path)
        md = self._get_markitdown()
        return md.convert_sync(path)
