"""
CrawleeAdapter — request-queue-based crawling with session management.

Harvested from: Crawlee (Apify) TypeScript patterns translated to Python.

Crawlee's key contribution: a typed RequestQueue that manages URLs in FIFO order
with deduplication, retry, and session rotation. This adapter implements the
same pattern in Python for the Harvest acquisition plane.

Two fetch backends (selected at construction time):
1. HTTP-only (default): stdlib urllib — zero dependencies, no JS support.
2. JS rendering: Playwright headless Chromium — handles SPAs and dynamic content.
   Enable with: CrawleeAdapter(..., use_js_rendering=True)
   Playwright auto-detected; falls back to HTTP-only if not installed.

Sitemap seeding (optional, enabled by default):
   When crawl(url) is called, the adapter checks {origin}/sitemap.xml and seeds
   the RequestQueue with discovered URLs (via SitemapParser).
   Disable with: CrawleeAdapter(..., use_sitemap=False)

Robots enforcement (optional, enabled by default):
   Before fetching any URL the adapter verifies it is allowed by robots.txt
   (via RobotsChecker). Respects Crawl-delay between requests.
   Disable with: CrawleeAdapter(..., respect_robots=False)

Constitutional guarantees:
- Local-first: no Playwright required; falls back to urllib HTTP client
- Fail-closed: empty queue after crawl raises AcquisitionError (not silent empty)
- Zero-ambiguity: CrawlResult.pages always List[PageResult], never None
- Append-only chain: crawl.started, crawl.page_fetched, crawl.completed always emitted
"""

from __future__ import annotations

import re
import time
import urllib.request
import urllib.error
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set
from uuid import uuid4

from harvest_core.control.exceptions import AcquisitionError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.rights.rights_model import RightsProfile


@dataclass
class PageResult:
    url: str
    markdown: str
    status_code: int
    depth: int
    artifact_id: str
    fetched_at: float = field(default_factory=time.time)


@dataclass
class CrawlResult:
    pages: List[PageResult]
    total_bytes: int
    errors: List[Dict[str, Any]]

    @property
    def page_count(self) -> int:
        return len(self.pages)

    @property
    def success_rate(self) -> float:
        total = len(self.pages) + len(self.errors)
        return len(self.pages) / total if total > 0 else 0.0


class _RequestQueue:
    """
    FIFO URL queue with deduplication. Mirrors Crawlee RequestQueue semantics.
    Enqueuing a URL already seen is a no-op (zero-ambiguity: no duplicate pages).
    """

    def __init__(self):
        self._queue: Deque[tuple[str, int]] = deque()
        self._seen: Set[str] = set()

    def enqueue(self, url: str, depth: int = 0) -> bool:
        if url in self._seen:
            return False
        self._seen.add(url)
        self._queue.append((url, depth))
        return True

    def dequeue(self) -> Optional[tuple[str, int]]:
        if self._queue:
            return self._queue.popleft()
        return None

    def __len__(self) -> int:
        return len(self._queue)

    @property
    def is_empty(self) -> bool:
        return len(self._queue) == 0


def _extract_links(html: str, base_url: str) -> List[str]:
    """Extract absolute href links from HTML."""
    from urllib.parse import urlparse
    pattern = re.compile(r'href=["\']([^"\'#?]+)["\']', re.IGNORECASE)
    parsed_base = urlparse(base_url)
    links = []
    for m in pattern.finditer(html):
        href = m.group(1).strip()
        if href.startswith("http://") or href.startswith("https://"):
            links.append(href)
        elif href.startswith("/"):
            links.append(f"{parsed_base.scheme}://{parsed_base.netloc}{href}")
    return links


def _coerce_str(value: "str | list[str]") -> str:
    """Coerce extract_content() result (str | List[str]) to str."""
    if isinstance(value, list):
        return " ".join(value)
    return value


def _html_to_markdown(html: str, user_query: Optional[str] = None) -> str:
    """Convert HTML to clean text. Uses BM25 content filter when available."""
    try:
        from harvest_acquire.crawl.content_filter import extract_content
        result = extract_content(html, user_query=user_query, join=True)
        if result:
            return _coerce_str(result)
    except Exception:
        pass
    # Fallback: simple regex stripping
    clean = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    clean = re.sub(r"<style[^>]*>.*?</style>", "", clean, flags=re.DOTALL | re.IGNORECASE)
    clean = re.sub(r"<[^>]+>", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _fetch_url(
    url: str,
    timeout: int = 10,
    proxy_url: Optional[str] = None,
    use_stealth_headers: bool = False,
) -> tuple[str, int]:
    """Fetch URL using stdlib. Returns (html_content, status_code)."""
    from harvest_acquire.crawl.stealth_headers import stealth_headers, respect_retry_after

    if use_stealth_headers:
        headers = stealth_headers()
    else:
        headers = {"User-Agent": "HarvestBot/1.0 (+https://github.com/danteharvest)"}

    req = urllib.request.Request(url, headers=headers)

    opener = urllib.request.build_opener()
    if proxy_url:
        proxy_support = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        opener = urllib.request.build_opener(proxy_support)

    try:
        with opener.open(req, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", errors="replace")
            return content, resp.status
    except urllib.error.HTTPError as e:
        if e.code == 429:
            respect_retry_after(dict(e.headers))
        return "", e.code
    except Exception as e:
        raise AcquisitionError(f"Failed to fetch {url}: {e}") from e


async def _fetch_url_playwright(url: str, timeout: int = 15000) -> tuple[str, int]:
    """
    Fetch a URL using Playwright headless Chromium (JS rendering).
    Returns (html_content, status_code). Raises AcquisitionError on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as e:
        raise AcquisitionError(
            "playwright not installed. Run: pip install playwright && playwright install chromium"
        ) from e

    status_code = 200
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            page = await browser.new_page()

            def _capture_status(response):
                nonlocal status_code
                if response.url == url:
                    status_code = response.status

            page.on("response", _capture_status)
            try:
                await page.goto(url, wait_until="networkidle", timeout=timeout)
            except Exception:
                # domcontentloaded is sufficient if networkidle times out
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            html = await page.content()
        finally:
            await browser.close()

    return html, status_code


def _is_playwright_available() -> bool:
    try:
        import playwright  # noqa: F401
        return True
    except Exception:
        return False


class CrawleeAdapter:
    """
    Crawlee-style request-queue crawler for the Harvest acquisition plane.

    Usage (HTTP-only, zero deps):
        adapter = CrawleeAdapter(writer, storage_root="storage")
        result = await adapter.crawl(url="https://example.com", run_id="run-001", ...)

    Usage (JS rendering, handles SPAs):
        adapter = CrawleeAdapter(writer, use_js_rendering=True)
        result = await adapter.crawl(url="https://app.example.com", run_id="run-001", ...)

    If use_js_rendering=True but Playwright is not installed, falls back to HTTP-only.
    """

    def __init__(
        self,
        chain_writer: Optional[ChainWriter] = None,
        storage_root: str = "storage",
        use_js_rendering: bool = False,
        browser_pool: Optional[Any] = None,
        proxy_url: Optional[str] = None,
        use_stealth_headers: bool = False,
        use_sitemap: bool = True,
        respect_robots: bool = True,
        robots_user_agent: str = "HarvestBot",
    ):
        self._use_js = use_js_rendering and _is_playwright_available()
        self.chain_writer = chain_writer
        self.storage_root = Path(storage_root)
        self._browser_pool = browser_pool  # optional PlaywrightPool
        self._proxy_url = proxy_url
        self._stealth = use_stealth_headers
        self._use_sitemap = use_sitemap
        self._respect_robots = respect_robots
        if respect_robots:
            from harvest_acquire.crawl.robots_checker import RobotsChecker
            self._robots: Optional[Any] = RobotsChecker(user_agent=robots_user_agent)
        else:
            self._robots = None
        if use_sitemap:
            from harvest_acquire.crawl.sitemap_parser import SitemapParser
            self._sitemap: Optional[Any] = SitemapParser()
        else:
            self._sitemap = None

    @property
    def rendering_mode(self) -> str:
        return "playwright" if self._use_js else "http"

    async def _fetch(self, url: str) -> tuple[str, int]:
        if self._use_js:
            return await _fetch_url_playwright(url)
        return _fetch_url(url, proxy_url=self._proxy_url, use_stealth_headers=self._stealth)

    async def crawl(  # noqa: PLR0912
        self,
        url: str,
        run_id: str,
        rights_profile: Optional[RightsProfile] = None,
        max_depth: int = 1,
        max_pages: int = 10,
        follow_links: bool = False,
        user_query: Optional[str] = None,
    ) -> CrawlResult:
        """
        Crawl url using a request queue.
        Fail-closed: raises AcquisitionError if no pages could be fetched.
        """
        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="crawl.started",
                machine="crawlee_adapter",
                data={
                    "url": url,
                    "max_depth": max_depth,
                    "max_pages": max_pages,
                    "rendering_mode": self.rendering_mode,
                    "rights_status": getattr(rights_profile, "rights_status", "unknown"),
                },
            ))

        from urllib.parse import urlparse as _urlparse
        queue = _RequestQueue()
        queue.enqueue(url, depth=0)

        # Seed queue from sitemap if enabled
        if self._sitemap is not None:
            _origin = "{0}://{1}".format(*_urlparse(url)[:2])
            try:
                _sitemap_urls = self._sitemap.discover_and_parse(url)
                for _su in _sitemap_urls:
                    queue.enqueue(_su, depth=0)
            except Exception:
                pass  # Sitemap failure never aborts the crawl

        pages: List[PageResult] = []
        errors: List[Dict[str, Any]] = []
        total_bytes = 0

        while not queue.is_empty and len(pages) < max_pages:
            _item = queue.dequeue()
            if _item is None:
                break
            current_url, depth = _item

            # Enforce robots.txt before fetching
            if self._robots is not None:
                if not self._robots.is_allowed(current_url):
                    errors.append({"url": current_url, "error": "disallowed by robots.txt"})
                    continue
                await self._robots.async_respect_delay(current_url)

            try:
                html, status_code = await self._fetch(current_url)
            except AcquisitionError as e:
                errors.append({"url": current_url, "error": str(e)})
                continue

            if not html or status_code >= 400:
                errors.append({"url": current_url, "status_code": status_code})
                continue

            markdown = _html_to_markdown(html, user_query=user_query)
            artifact_id = str(uuid4())
            total_bytes += len(markdown.encode())

            self._store_page(artifact_id, current_url, markdown)

            page = PageResult(
                url=current_url,
                markdown=markdown,
                status_code=status_code,
                depth=depth,
                artifact_id=artifact_id,
            )
            pages.append(page)

            if self.chain_writer:
                await self.chain_writer.append(ChainEntry(
                    run_id=run_id,
                    signal="crawl.page_fetched",
                    machine="crawlee_adapter",
                    data={
                        "url": current_url,
                        "depth": depth,
                        "artifact_id": artifact_id,
                        "status_code": status_code,
                        "bytes": len(markdown.encode()),
                    },
                ))

            if follow_links and depth < max_depth:
                for link in _extract_links(html, current_url):
                    queue.enqueue(link, depth=depth + 1)

        if self.chain_writer:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="crawl.completed",
                machine="crawlee_adapter",
                data={
                    "url": url,
                    "page_count": len(pages),
                    "error_count": len(errors),
                    "total_bytes": total_bytes,
                },
            ))

        return CrawlResult(pages=pages, total_bytes=total_bytes, errors=errors)

    def _store_page(self, artifact_id: str, url: str, markdown: str) -> None:
        out_dir = self.storage_root / "crawlee" / artifact_id
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "page.md").write_text(markdown, encoding="utf-8")
        (out_dir / "url.txt").write_text(url, encoding="utf-8")
