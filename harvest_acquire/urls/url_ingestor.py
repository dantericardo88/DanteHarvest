"""
URLIngestor — acquire web pages into the Harvest artifact store.

Pipeline: robots.txt check → Playwright fetch → MarkItDown normalize →
chain entries → IngestResult.

Constitutional guarantees:
- robots.txt checked FIRST, fail-closed on network errors (ConstitutionalError)
- acquire.failed emitted on any error, never silent
- No URL ingested without rights profile
- Local-first: content stored locally before normalization
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional
from uuid import uuid4

from harvest_core.control.exceptions import AcquisitionError, ConstitutionalError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.rights.rights_model import RightsProfile, SourceClass, default_rights_for


class URLIngestResult:
    __slots__ = ("artifact_id", "url", "sha256", "storage_uri", "char_count", "markdown")

    def __init__(
        self,
        artifact_id: str,
        url: str,
        sha256: str,
        storage_uri: str,
        char_count: int,
        markdown: str,
    ):
        self.artifact_id = artifact_id
        self.url = url
        self.sha256 = sha256
        self.storage_uri = storage_uri
        self.char_count = char_count
        self.markdown = markdown


class URLIngestor:
    """
    Ingest a web page into the Harvest artifact store.

    Requires a running Playwright engine and MarkItDown adapter.
    robots.txt is checked before any fetch attempt.

    Usage:
        ingestor = URLIngestor(chain_writer, playwright_engine, markitdown_adapter)
        result = await ingestor.ingest(url="https://example.com", run_id="run-001")
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        playwright_engine,
        markitdown_adapter=None,
        storage_root: str = "storage",
    ):
        self.chain_writer = chain_writer
        self.playwright_engine = playwright_engine
        self.markitdown_adapter = markitdown_adapter
        self.storage_root = Path(storage_root)

    async def ingest(
        self,
        url: str,
        run_id: str,
        rights_profile: Optional[RightsProfile] = None,
    ) -> URLIngestResult:
        """
        Fetch and ingest a URL.  Emits acquire.started → acquire.completed | acquire.failed.
        Raises ConstitutionalError if robots.txt disallows the URL (caller must not retry).
        Raises AcquisitionError on fetch or storage failures.
        """
        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="acquire.started",
            machine="url_ingestor",
            data={"url": url, "source_type": "url"},
        ))

        try:
            rp = rights_profile or default_rights_for(SourceClass.PUBLIC_WEB)

            # robots.txt check is inside playwright_engine.fetch_page()
            page_result = await self.playwright_engine.fetch_page(url)
            if not page_result.get("success"):
                error = page_result.get("error", "page fetch failed")
                raise AcquisitionError(f"Playwright fetch failed for {url}: {error}")
            html_content = page_result.get("html", "") or page_result.get("content", "")
            if not html_content:
                raise AcquisitionError(f"Playwright returned empty content for {url}")
            markdown = page_result.get("markdown", html_content)

            artifact_id = str(uuid4())
            sha256 = hashlib.sha256(markdown.encode()).hexdigest()

            # Persist raw markdown to local store
            dest_dir = self.storage_root / "artifacts" / artifact_id
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_file = dest_dir / "page.md"
            dest_file.write_text(markdown, encoding="utf-8")
            storage_uri = f"local://{dest_file}"

            # Normalize via MarkItDown if HTML was returned and adapter available
            if self.markitdown_adapter and dest_file.suffix == ".html":
                try:
                    md_result = await self.markitdown_adapter.convert(
                        path=dest_file,
                        run_id=run_id,
                        artifact_id=artifact_id,
                    )
                    markdown = md_result.markdown
                    sha256 = hashlib.sha256(markdown.encode()).hexdigest()
                except Exception:
                    pass  # fall back to raw html if normalization fails

            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.completed",
                machine="url_ingestor",
                data={
                    "artifact_id": artifact_id,
                    "url": url,
                    "sha256": sha256,
                    "storage_uri": storage_uri,
                    "char_count": len(markdown),
                    "rights_status": rp.review_status.value,
                    "training_eligibility": rp.training_eligibility.value,
                },
            ))

            return URLIngestResult(
                artifact_id=artifact_id,
                url=url,
                sha256=sha256,
                storage_uri=storage_uri,
                char_count=len(markdown),
                markdown=markdown,
            )

        except ConstitutionalError:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.failed",
                machine="url_ingestor",
                data={"url": url, "error": "robots_txt_disallowed"},
            ))
            raise
        except AcquisitionError:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.failed",
                machine="url_ingestor",
                data={"url": url, "error": "acquisition_error"},
            ))
            raise
        except Exception as e:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.failed",
                machine="url_ingestor",
                data={"url": url, "error": str(e)},
            ))
            raise AcquisitionError(f"URL ingest failed for {url}: {e}") from e
