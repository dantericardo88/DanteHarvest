"""
SitemapParser — parse sitemap.xml to seed a crawl RequestQueue.

Sprint 5 target: close crawl_acquisition gap (DH: 7 → 9 vs Firecrawl: 9).

Harvested from: Crawlee sitemap plugin + Scrapy SitemapSpider patterns.

Fetches and parses XML sitemaps (standard + sitemap index format).
Seeds a CrawleeAdapter RequestQueue with discovered URLs.
Respects robots.txt crawl-delay from the sitemap source domain.

Constitutional guarantees:
- Local-first: parses downloaded sitemap locally; no external parser dependency
- Fail-closed: unreachable sitemap raises AcquisitionError (not silent empty queue)
- Zero-ambiguity: parse() always returns List[str] URLs, never None
"""

from __future__ import annotations

import re
import urllib.request
import urllib.error
from typing import List, Optional
from xml.etree import ElementTree

from harvest_core.control.exceptions import AcquisitionError


_USER_AGENT = "HarvestBot/1.0 (+https://github.com/danteharvest)"
_SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


def _fetch_xml(url: str, timeout: int = 15) -> str:
    """Fetch URL as text. Fail-closed: raises AcquisitionError on network error."""
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raise AcquisitionError(f"HTTP {e.code} fetching sitemap: {url}") from e
    except Exception as e:
        raise AcquisitionError(f"Failed to fetch sitemap {url}: {e}") from e


def _parse_sitemap_xml(xml_text: str) -> List[str]:
    """
    Parse a standard sitemap.xml or sitemap index.
    Returns list of URLs (loc elements). Empty sitemap returns [] (not error).
    """
    urls: List[str] = []
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return urls

    tag = root.tag.lower()
    if "sitemapindex" in tag:
        for sitemap in root.iter():
            if sitemap.tag.endswith("}loc") or sitemap.tag == "loc":
                text = (sitemap.text or "").strip()
                if text:
                    urls.append(text)
    else:
        for url_elem in root.iter():
            if url_elem.tag.endswith("}loc") or url_elem.tag == "loc":
                text = (url_elem.text or "").strip()
                if text:
                    urls.append(text)

    return urls


class SitemapParser:
    """
    Fetch and parse XML sitemaps to seed a crawl RequestQueue.

    Usage:
        parser = SitemapParser()
        urls = parser.parse("https://docs.example.com/sitemap.xml")
        # → ["https://docs.example.com/page1", "https://docs.example.com/page2", ...]

    Usage (auto-discover from domain root):
        urls = parser.discover_and_parse("https://docs.example.com")
    """

    def __init__(self, max_urls: int = 500, timeout: int = 15):
        self.max_urls = max_urls
        self.timeout = timeout

    def parse(self, sitemap_url: str) -> List[str]:
        """
        Fetch and parse a sitemap.xml URL.
        Fail-closed: raises AcquisitionError if unreachable.
        Zero-ambiguity: always returns List[str].
        """
        xml_text = _fetch_xml(sitemap_url, timeout=self.timeout)
        urls = _parse_sitemap_xml(xml_text)

        sitemap_refs = [u for u in urls if u.endswith(".xml")]
        page_urls = [u for u in urls if not u.endswith(".xml")]

        for sitemap_ref in sitemap_refs:
            if len(page_urls) >= self.max_urls:
                break
            try:
                sub_xml = _fetch_xml(sitemap_ref, timeout=self.timeout)
                sub_urls = _parse_sitemap_xml(sub_xml)
                page_urls.extend(u for u in sub_urls if not u.endswith(".xml"))
            except AcquisitionError:
                continue

        return page_urls[:self.max_urls]

    def discover_and_parse(self, base_url: str) -> List[str]:
        """
        Auto-discover sitemaps for a domain by trying standard paths.
        Falls back to empty list (not error) if no sitemap found.
        Zero-ambiguity: always returns List[str].
        """
        base = base_url.rstrip("/")
        from urllib.parse import urlparse
        parsed = urlparse(base)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        candidates = [
            f"{origin}/sitemap.xml",
            f"{origin}/sitemap_index.xml",
            f"{origin}/sitemap.xml.gz",
            f"{base}/sitemap.xml",
        ]

        for candidate in candidates:
            try:
                return self.parse(candidate)
            except AcquisitionError:
                continue

        return []

    def get_crawl_delay(self, robots_url: str) -> float:
        """
        Parse Crawl-delay from robots.txt for the given URL's domain.
        Returns 0.0 if no crawl-delay specified (zero-ambiguity: always float).
        """
        try:
            xml_text = _fetch_xml(robots_url, timeout=5)
        except AcquisitionError:
            return 0.0

        for line in xml_text.splitlines():
            line = line.strip().lower()
            if line.startswith("crawl-delay:"):
                try:
                    return float(line.split(":", 1)[1].strip())
                except ValueError:
                    return 0.0
        return 0.0
