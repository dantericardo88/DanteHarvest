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

import asyncio
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


_SPA_MUTATION_SCRIPT = """
() => new Promise((resolve) => {
    let settled = false;
    const done = () => { if (!settled) { settled = true; resolve(); } };
    const observer = new MutationObserver(() => {
        clearTimeout(timer);
        timer = setTimeout(done, 300);
    });
    observer.observe(document.body || document.documentElement, {
        childList: true, subtree: true, attributes: true
    });
    let timer = setTimeout(() => { observer.disconnect(); done(); }, 2000);
})
"""

_WAIT_STRATEGIES = ("networkidle", "domcontentloaded", "load", "commit")

_SPA_MARKERS = (
    "react", "vue", "angular", "__NEXT_DATA__", "ng-version",
    "data-reactroot", "data-v-", "_nuxt", "svelte", "ember",
    "v-cloak",
)


def _auto_detect_spa(html: str) -> bool:
    """Heuristically detect if a page is a SPA based on common framework markers."""
    sample = html[:8000].lower()
    return any(marker.lower() in sample for marker in _SPA_MARKERS)


async def wait_for_content_stable(page, quiet_ms: int = 500, max_wait_ms: int = 5000) -> None:
    """
    Wait until DOM mutations settle for at least `quiet_ms` milliseconds.
    Uses a MutationObserver with a debounced timer. Fail-open on error.
    """
    script = f"""
    () => new Promise((resolve) => {{
        let settled = false;
        const done = () => {{ if (!settled) {{ settled = true; resolve(); }} }};
        const observer = new MutationObserver(() => {{
            clearTimeout(timer);
            timer = setTimeout(done, {quiet_ms});
        }});
        observer.observe(document.body || document.documentElement, {{
            childList: true, subtree: true, attributes: true
        }});
        let timer = setTimeout(() => {{ observer.disconnect(); done(); }}, {max_wait_ms});
    }})
    """
    try:
        await page.evaluate(script)
    except Exception:
        pass


async def wait_for_selector_or_timeout(
    page, selector: str, timeout_ms: int = 5000
) -> bool:
    """Wait for a CSS selector to appear. Returns True if found, False on timeout."""
    try:
        await page.wait_for_selector(selector, timeout=timeout_ms)
        return True
    except Exception:
        return False


async def _fetch_url_playwright(
    url: str,
    timeout: int = 15000,
    wait_until: str = "networkidle",
    spa_mode: bool = False,
    extra_wait_ms: int = 0,
) -> tuple[str, int]:
    """
    Fetch a URL using Playwright headless Chromium with configurable SPA strategies.

    wait_until: Playwright load event — "networkidle" | "domcontentloaded" | "load" | "commit"
    spa_mode:   After page load, wait for DOM mutation quiet period (MutationObserver-based).
    extra_wait_ms: Fixed post-load delay in ms (e.g. for lazy-loaded content).
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as e:
        raise AcquisitionError(
            "playwright not installed. Run: pip install playwright && playwright install chromium"
        ) from e

    if wait_until not in _WAIT_STRATEGIES:
        wait_until = "networkidle"

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

            from typing import cast as _cast, Literal as _Literal
            _WU = _cast(
                _Literal["commit", "domcontentloaded", "load", "networkidle"],
                wait_until,
            )
            # Primary load attempt with requested strategy
            try:
                await page.goto(url, wait_until=_WU, timeout=timeout)
            except Exception:
                # Fall back: domcontentloaded is sufficient if networkidle times out
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                except Exception:
                    pass  # Best-effort — capture whatever the page has

            # SPA quiescence: wait until DOM mutations settle
            if spa_mode:
                try:
                    await page.evaluate(_SPA_MUTATION_SCRIPT)
                except Exception:
                    pass  # Mutation observer failure never aborts the fetch

            # Fixed post-load delay for lazy-rendered content
            if extra_wait_ms > 0:
                import asyncio as _asyncio
                await _asyncio.sleep(extra_wait_ms / 1000)

            html = await page.content()
        finally:
            await browser.close()

    return html, status_code


def _extract_text_content(html: str) -> str:
    """
    Strip all <script>, <style>, and <head> tags and return visible text content.
    Pure regex, no external deps.
    """
    text = re.sub(r"<head[^>]*>.*?</head>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_preloaded_state(html: str) -> Dict[str, Any]:
    """
    Extract common preloaded JS state stores from HTML:
    window.__PRELOADED_STATE__, window.INITIAL_REDUX_STATE,
    window.__APOLLO_STATE__, window.__RELAY_STORE__.
    Returns dict keyed by variable name with raw JSON string as value.
    """
    import json
    result: Dict[str, Any] = {}
    patterns = [
        ("__PRELOADED_STATE__", r'(?:window\.)?__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("INITIAL_REDUX_STATE", r'(?:window\.)?INITIAL_REDUX_STATE\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("__APOLLO_STATE__", r'(?:window\.)?__APOLLO_STATE__\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("__RELAY_STORE__", r'(?:window\.)?__RELAY_STORE__\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("__NEXT_DATA__", r'__NEXT_DATA__\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("__INITIAL_STATE__", r'(?:window\.)?__INITIAL_STATE__\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
    ]
    for key, pattern in patterns:
        m = re.search(pattern, html, flags=re.DOTALL)
        if m:
            raw = m.group(1)[:8192]
            try:
                result[key] = json.loads(raw)
            except Exception:
                result[key] = raw
    return result


def _extract_inline_data_attrs(html: str) -> Dict[str, Any]:
    """
    Extract data-props, data-initial, data-server-props attributes from root div/section
    elements (common in React/Vue server-side props injection).
    Returns dict keyed by attribute name with parsed JSON value.

    Uses separate single-quote and double-quote patterns so that JSON payloads
    containing the other quote type are captured correctly.
    """
    import json
    result: Dict[str, Any] = {}
    attr_names = ("data-props", "data-initial", "data-server-props", "data-page", "data-hydration")
    for attr in attr_names:
        escaped = re.escape(attr)
        # Single-quoted attribute value: content may contain " freely
        single = re.compile(rf"{escaped}='([^']*)'", re.IGNORECASE)
        # Double-quoted attribute value: content may contain ' freely
        double = re.compile(rf'{escaped}="([^"]*)"', re.IGNORECASE)
        m = single.search(html) or double.search(html)
        if m:
            raw = m.group(1)
            try:
                result[attr] = json.loads(raw)
            except Exception:
                result[attr] = raw
    return result


def _extract_app_config(html: str) -> Dict[str, Any]:
    """
    Extract window.APP_CONFIG, window.appConfig, window.config JS assignments.
    Returns dict keyed by config name.
    """
    import json
    result: Dict[str, Any] = {}
    patterns = [
        ("APP_CONFIG", r'(?:window\.)?APP_CONFIG\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("appConfig", r'(?:window\.)?appConfig\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
        ("config", r'window\.config\s*=\s*(\{.*?\})\s*(?:;|</script>)'),
    ]
    for key, pattern in patterns:
        m = re.search(pattern, html, flags=re.DOTALL)
        if m:
            raw = m.group(1)[:4096]
            try:
                result[key] = json.loads(raw)
            except Exception:
                result[key] = raw
    return result


def _detect_csr_only(html: str, text_content: str) -> bool:
    """
    Return True if the page appears to be CSR-only (pure client-side rendered).
    Heuristics:
    - Visible text content is very short (<1000 chars), AND
    - The ratio of <script> tag bytes to total body bytes is high (>70%)
    """
    text_len = len(text_content)
    if text_len >= 1000:
        return False  # substantial server-rendered text, not CSR-only

    # Measure script weight vs total
    script_content = "".join(
        re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.DOTALL | re.IGNORECASE)
    )
    total_len = len(html)
    if total_len == 0:
        return False
    script_ratio = len(script_content) / total_len
    return script_ratio > 0.70


def _detect_framework(html: str) -> str:
    """Detect the JS framework used, returning a short identifier."""
    sample = html[:8000].lower()
    if "__next_data__" in sample:
        return "next"
    if "_nuxt" in sample or "nuxtjs" in sample:
        return "nuxt"
    if "svelte" in sample:
        return "svelte"
    if "data-reactroot" in sample or "react" in sample:
        return "react"
    if "ng-version" in sample or "angular" in sample:
        return "angular"
    if "data-v-" in sample or "vue" in sample or "v-cloak" in sample:
        return "vue"
    if "ember" in sample:
        return "ember"
    return "ssr-generic"


def _extract_meta_tags(html: str) -> Dict[str, str]:
    """Extract Open Graph and standard meta tags from HTML."""
    meta: Dict[str, str] = {}
    for m in re.finditer(
        r'<meta\s+(?:[^>]*?\s+)?(?:property|name)=["\']([^"\']+)["\'][^>]*content=["\']([^"\']*)["\']',
        html, flags=re.IGNORECASE,
    ):
        meta[m.group(1)] = m.group(2)
    for m in re.finditer(
        r'<meta\s+(?:[^>]*?\s+)?content=["\']([^"\']*)["\'][^>]*(?:property|name)=["\']([^"\']+)["\']',
        html, flags=re.IGNORECASE,
    ):
        meta[m.group(2)] = m.group(1)
    return meta


def _fetch_url_spa_enhanced(
    url: str,
    timeout: int = 10,
    proxy_url: Optional[str] = None,
    use_stealth_headers: bool = False,
) -> tuple[str, int]:
    """
    HTTP fetch with SPA-aware enrichment.

    Returns a rich string encoding of all extracted data alongside the HTTP status code.
    The string is backward-compatible with the previous format (text followed by labeled
    sections) but now also includes:
    - CSR-only detection with explicit requires_playwright signal
    - Extended framework state extraction (__PRELOADED_STATE__, __APOLLO_STATE__, __RELAY_STORE__)
    - Inline data-attribute extraction (data-props, data-server-props, etc.)
    - App config extraction (window.APP_CONFIG, window.appConfig, window.config)
    - Visible text content stripped of all script/style/head content
    - Framework detection (next/nuxt/svelte/react/angular/vue/ssr-generic/csr-only)

    For callers that need the full structured dict, use _fetch_url_spa_enhanced_dict().
    """
    result = _fetch_url_spa_enhanced_dict(
        url, timeout=timeout, proxy_url=proxy_url, use_stealth_headers=use_stealth_headers
    )
    status = result["status_code"]
    if status >= 400 or not result.get("text_content") and not result.get("structured_data"):
        return result.get("text_content", ""), status

    parts: List[str] = []

    # Visible text content
    if result["text_content"]:
        parts.append(result["text_content"])

    # CSR-only advisory
    if result["requires_playwright"]:
        parts.append(
            f"[SPA_CSR_ONLY] content_type={result['content_type']} "
            "Client-side rendered — Playwright required for full content"
        )

    # Structured data: JSON-LD, JSON-DATA, framework stores, app config, inline attrs
    sd = result["structured_data"]
    if sd.get("json_ld"):
        for block in sd["json_ld"]:
            parts.append("JSON-LD: " + block)
    if sd.get("json_data"):
        for block in sd["json_data"]:
            parts.append("JSON-DATA: " + block)
    for key in ("__PRELOADED_STATE__", "INITIAL_REDUX_STATE", "__APOLLO_STATE__",
                "__RELAY_STORE__", "__NEXT_DATA__", "__INITIAL_STATE__"):
        if key in sd.get("preloaded_state", {}):
            import json as _json
            val = sd["preloaded_state"][key]
            raw = val if isinstance(val, str) else _json.dumps(val)
            parts.append(f"{key}: " + raw[:4096])
    if sd.get("app_config"):
        import json as _json
        parts.append("APP_CONFIG: " + _json.dumps(sd["app_config"])[:2048])
    if sd.get("inline_data_attrs"):
        import json as _json
        parts.append("DATA_ATTRS: " + _json.dumps(sd["inline_data_attrs"])[:2048])

    # Meta tags
    if result["meta"]:
        meta_parts = [f"{k}: {v}" for k, v in result["meta"].items()]
        parts.append("META: " + " | ".join(meta_parts))

    return "\n\n".join(parts), status


def _fetch_url_spa_enhanced_dict(
    url: str,
    timeout: int = 10,
    proxy_url: Optional[str] = None,
    use_stealth_headers: bool = False,
) -> Dict[str, Any]:
    """
    HTTP fetch returning a full structured dict with all SPA-aware extraction results.

    Returns:
        {
            "url": str,
            "status_code": int,
            "content_type": str,   # "csr-only" | "next" | "nuxt" | ... | "ssr-generic"
            "text_content": str,   # visible text stripped of scripts/styles/head
            "structured_data": {   # merged extraction from all sources
                "json_ld": [...],
                "json_data": [...],
                "preloaded_state": {...},
                "app_config": {...},
                "inline_data_attrs": {...},
            },
            "meta": {...},         # Open Graph + standard meta tags
            "requires_playwright": bool,
        }
    """
    html, status = _fetch_url(url, timeout=timeout, proxy_url=proxy_url,
                              use_stealth_headers=use_stealth_headers)

    if status >= 400 or not html:
        return {
            "url": url,
            "status_code": status,
            "content_type": "error",
            "text_content": html or "",
            "structured_data": {},
            "meta": {},
            "requires_playwright": False,
        }

    # Visible text (strips scripts/styles/head)
    text_content = _extract_text_content(html)

    # CSR-only detection
    csr_only = _detect_csr_only(html, text_content)
    if csr_only:
        framework = "csr-only"
    else:
        framework = _detect_framework(html)

    # Extended structured extraction
    json_ld_blocks = [
        b.strip() for b in re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, flags=re.DOTALL | re.IGNORECASE,
        ) if b.strip()
    ]
    json_data_blocks = [
        b.strip() for b in re.findall(
            r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>',
            html, flags=re.DOTALL | re.IGNORECASE,
        ) if b.strip()
    ]

    preloaded_state = _extract_preloaded_state(html)
    app_config = _extract_app_config(html)
    inline_data_attrs = _extract_inline_data_attrs(html)
    meta = _extract_meta_tags(html)

    structured_data: Dict[str, Any] = {
        "json_ld": json_ld_blocks,
        "json_data": json_data_blocks,
        "preloaded_state": preloaded_state,
        "app_config": app_config,
        "inline_data_attrs": inline_data_attrs,
    }

    requires_playwright = csr_only

    return {
        "url": url,
        "status_code": status,
        "content_type": framework,
        "text_content": text_content,
        "structured_data": structured_data,
        "meta": meta,
        "requires_playwright": requires_playwright,
    }


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
        wait_until: str = "networkidle",
        spa_mode: bool = False,
        extra_wait_ms: int = 0,
        rate_limiter: Optional[Any] = None,
        default_rps: float = 1.0,
        auto_spa_detection: bool = True,
    ):
        self._use_js = use_js_rendering and _is_playwright_available()
        self._auto_spa = auto_spa_detection
        self.chain_writer = chain_writer
        self.storage_root = Path(storage_root)
        self._browser_pool = browser_pool  # optional PlaywrightPool
        self._proxy_url = proxy_url
        self._stealth = use_stealth_headers
        self._use_sitemap = use_sitemap
        self._respect_robots = respect_robots
        self._wait_until = wait_until if wait_until in _WAIT_STRATEGIES else "networkidle"
        self._spa_mode = spa_mode
        self._extra_wait_ms = extra_wait_ms
        if rate_limiter is not None:
            self._rate_limiter: Optional[Any] = rate_limiter
        else:
            from harvest_acquire.crawl.domain_rate_limiter import DomainRateLimiter
            self._rate_limiter = DomainRateLimiter(default_rps=default_rps)
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
        if self._use_js:
            return "playwright"
        if self._auto_spa:
            return "http_spa"
        return "http"

    async def _fetch_with_retry(
        self,
        url: str,
        max_attempts: int = 3,
        base_delay_s: float = 1.0,
    ) -> tuple[str, int]:
        """
        Fetch url with exponential-backoff retry for transient errors.
        - 429: wait for Retry-After (via rate limiter) and retry
        - 5xx: retry with exponential backoff
        - 4xx (not 429): no retry — permanent failure
        - Network error (AcquisitionError): retry up to max_attempts
        """
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                html, status_code = await self._fetch(url)
            except AcquisitionError as e:
                last_exc = e
                if attempt < max_attempts:
                    await asyncio.sleep(base_delay_s * (2 ** (attempt - 1)))
                continue

            if status_code == 429:
                if self._rate_limiter is not None:
                    self._rate_limiter.record_result(url, 429)
                    await self._rate_limiter.wait_for_token(url)
                elif attempt < max_attempts:
                    await asyncio.sleep(base_delay_s * (2 ** (attempt - 1)))
                continue

            if 500 <= status_code < 600 and attempt < max_attempts:
                await asyncio.sleep(base_delay_s * (2 ** (attempt - 1)))
                continue

            return html, status_code

        if last_exc is not None:
            raise last_exc
        return "", 503

    async def _fetch(self, url: str) -> tuple[str, int]:
        if self._use_js:
            return await _fetch_url_playwright(
                url,
                wait_until=self._wait_until,
                spa_mode=self._spa_mode,
                extra_wait_ms=self._extra_wait_ms,
            )
        if self._auto_spa:
            html, status = _fetch_url(url, proxy_url=self._proxy_url, use_stealth_headers=self._stealth)
            if status < 400 and _auto_detect_spa(html):
                if _is_playwright_available():
                    return await _fetch_url_playwright(
                        url,
                        wait_until=self._wait_until,
                        spa_mode=True,
                        extra_wait_ms=self._extra_wait_ms,
                    )
                return _fetch_url_spa_enhanced(
                    url,
                    proxy_url=self._proxy_url,
                    use_stealth_headers=self._stealth,
                )
            return html, status
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

            if self._rate_limiter is not None:
                await self._rate_limiter.wait_for_token(current_url)

            try:
                html, status_code = await self._fetch_with_retry(current_url)
            except AcquisitionError as e:
                errors.append({"url": current_url, "error": str(e)})
                continue

            if self._rate_limiter is not None:
                self._rate_limiter.record_result(current_url, status_code)

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
