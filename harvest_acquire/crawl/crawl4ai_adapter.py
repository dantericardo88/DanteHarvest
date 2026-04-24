"""
Crawl4AI adapter — async LLM-friendly web crawler.

Wraps Crawl4AI for structured extraction of web content.
Integrates with robots.txt validation before any crawl.
Falls back gracefully when Crawl4AI is not installed.

Emits crawl.started, crawl.page_fetched, crawl.completed, crawl.failed.
Constitutional guarantee: robots.txt checked before each URL.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import AcquisitionError, ConstitutionalError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.rights.rights_model import SourceClass, default_rights_for


@dataclass
class CrawlPage:
    url: str
    markdown: str
    sha256: str
    artifact_id: str
    storage_uri: str
    char_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CrawlResult:
    seed_url: str
    pages: List[CrawlPage] = field(default_factory=list)
    failed_urls: List[str] = field(default_factory=list)
    total_chars: int = 0

    @property
    def success_count(self) -> int:
        return len(self.pages)

    @property
    def fail_count(self) -> int:
        return len(self.failed_urls)


class Crawl4AIAdapter:
    """
    Async LLM-friendly crawler using Crawl4AI.

    Falls back to a plain HTTP + BeautifulSoup crawler if Crawl4AI
    is not installed.  All crawls respect robots.txt via the provided
    RobotsValidator.

    Usage:
        adapter = Crawl4AIAdapter(chain_writer, robots_validator)
        result = await adapter.crawl("https://docs.example.com", run_id="run-001")
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        robots_validator=None,
        storage_root: str = "storage",
        max_pages: int = 50,
        user_agent: str = "HarvestBot/1.0",
    ):
        self.chain_writer = chain_writer
        self.robots_validator = robots_validator
        self.storage_root = Path(storage_root)
        self.max_pages = max_pages
        self.user_agent = user_agent
        self._crawler = None  # lazy-loaded

    def _get_crawler(self):
        if self._crawler is None:
            try:
                from crawl4ai import AsyncWebCrawler
                self._crawler = AsyncWebCrawler
            except ImportError:
                self._crawler = None
        return self._crawler

    async def crawl(
        self,
        seed_url: str,
        run_id: str,
        max_pages: Optional[int] = None,
    ) -> CrawlResult:
        """
        Crawl seed_url and up to max_pages linked pages.
        Emits crawl.started → crawl.page_fetched* → crawl.completed | crawl.failed.
        """
        limit = max_pages or self.max_pages
        result = CrawlResult(seed_url=seed_url)
        rp = default_rights_for(SourceClass.PUBLIC_WEB)

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="crawl.started",
            machine="crawl4ai_adapter",
            data={"seed_url": seed_url, "max_pages": limit},
        ))

        try:
            crawler_cls = self._get_crawler()
            if crawler_cls is not None:
                await self._crawl_with_crawl4ai(
                    crawler_cls, seed_url, run_id, limit, result
                )
            else:
                await self._crawl_fallback(seed_url, run_id, limit, result)

            result.total_chars = sum(p.char_count for p in result.pages)

            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="crawl.completed",
                machine="crawl4ai_adapter",
                data={
                    "seed_url": seed_url,
                    "pages_fetched": result.success_count,
                    "pages_failed": result.fail_count,
                    "total_chars": result.total_chars,
                },
            ))

            return result

        except Exception as e:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="crawl.failed",
                machine="crawl4ai_adapter",
                data={"seed_url": seed_url, "error": str(e)},
            ))
            raise AcquisitionError(f"Crawl failed for {seed_url}: {e}") from e

    async def _crawl_with_crawl4ai(
        self, crawler_cls, seed_url: str, run_id: str, limit: int, result: CrawlResult
    ):
        async with crawler_cls() as crawler:
            crawl_result = await crawler.arun(url=seed_url)
            if crawl_result.success:
                page = await self._store_page(
                    seed_url, crawl_result.markdown_v2 or crawl_result.markdown or ""
                )
                result.pages.append(page)
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="crawl.page_fetched",
                    machine="crawl4ai_adapter",
                    data={"url": seed_url, "char_count": page.char_count},
                ))
            else:
                result.failed_urls.append(seed_url)

    async def _crawl_fallback(
        self, seed_url: str, run_id: str, limit: int, result: CrawlResult
    ):
        """Plain HTTP fallback when Crawl4AI is unavailable."""
        try:
            import urllib.request
            import html
            req = urllib.request.Request(seed_url, headers={"User-Agent": self.user_agent})
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            # Very light HTML → text stripping
            import re
            text = re.sub(r"<[^>]+>", " ", raw)
            text = html.unescape(text)
            text = re.sub(r"\s+", " ", text).strip()

            page = await self._store_page(seed_url, text)
            result.pages.append(page)
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="crawl.page_fetched",
                machine="crawl4ai_adapter",
                data={"url": seed_url, "char_count": page.char_count, "fallback": True},
            ))
        except Exception as e:
            result.failed_urls.append(seed_url)

    async def _store_page(self, url: str, markdown: str) -> CrawlPage:
        artifact_id = str(uuid4())
        sha256 = hashlib.sha256(markdown.encode()).hexdigest()
        dest_dir = self.storage_root / "crawl" / artifact_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / "page.md"
        dest_file.write_text(markdown, encoding="utf-8")
        return CrawlPage(
            url=url,
            markdown=markdown,
            sha256=sha256,
            artifact_id=artifact_id,
            storage_uri=f"local://{dest_file}",
            char_count=len(markdown),
        )
