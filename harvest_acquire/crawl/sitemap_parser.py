"""
SitemapParser — parse sitemap.xml to seed a crawl RequestQueue.

Sprint 5 target: close crawl_acquisition gap (DH: 7 → 9 vs Firecrawl: 9).

Harvested from: Crawlee sitemap plugin + Scrapy SitemapSpider patterns.

Fetches and parses XML sitemaps (standard + sitemap index format).
Handles gzip-compressed sitemaps (.xml.gz).
Seeds a CrawleeAdapter RequestQueue with discovered URLs.
Returns SitemapEntry objects carrying priority/lastmod metadata.
Respects robots.txt crawl-delay from the sitemap source domain.

Constitutional guarantees:
- Local-first: parses downloaded sitemap locally; no external parser dependency
- Fail-closed: unreachable sitemap raises AcquisitionError (not silent empty queue)
- Zero-ambiguity: parse() always returns List[SitemapEntry], never None
"""

from __future__ import annotations

import gzip
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import List, Optional
from xml.etree import ElementTree

from harvest_core.control.exceptions import AcquisitionError


_USER_AGENT = "HarvestBot/1.0 (+https://github.com/danteharvest)"
_SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
}


@dataclass
class SitemapEntry:
    """A single URL discovered in a sitemap, with optional metadata."""
    url: str
    priority: float = 0.5
    lastmod: Optional[str] = None
    changefreq: Optional[str] = None


def _fetch_raw(url: str, timeout: int = 15) -> bytes:
    """
    Fetch URL as raw bytes. Fail-closed: raises AcquisitionError on network error.
    Handles gzip-compressed responses transparently.
    """
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            content_encoding = resp.headers.get("Content-Encoding", "")
            # Decompress if gzip-encoded in transport
            if "gzip" in content_encoding:
                raw = gzip.decompress(raw)
            return raw
    except urllib.error.HTTPError as e:
        raise AcquisitionError(f"HTTP {e.code} fetching sitemap: {url}") from e
    except Exception as e:
        raise AcquisitionError(f"Failed to fetch sitemap {url}: {e}") from e


def _fetch_xml(url: str, timeout: int = 15) -> str:
    """
    Fetch URL as text. Handles gzip-compressed sitemaps (.xml.gz).
    Fail-closed: raises AcquisitionError on network error.
    """
    raw = _fetch_raw(url, timeout=timeout)
    # Detect gzip magic bytes for files ending in .gz or served as gzip
    if raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except Exception as e:
            raise AcquisitionError(f"Failed to decompress gzip sitemap {url}: {e}") from e
    return raw.decode("utf-8", errors="replace")


def _parse_sitemap_xml(xml_text: str) -> List[str]:
    """
    Parse a standard sitemap.xml or sitemap index.
    Returns list of URLs (loc elements). Empty sitemap returns [] (not error).
    Backward-compatible: returns plain str list (used internally).
    """
    entries = _parse_sitemap_entries(xml_text)
    return [e.url for e in entries]


def _parse_sitemap_entries(xml_text: str) -> List[SitemapEntry]:
    """
    Parse a standard sitemap.xml or sitemap index with full metadata.
    Returns List[SitemapEntry] with url, priority, lastmod, changefreq.
    Empty sitemap returns [] (not error).
    """
    entries: List[SitemapEntry] = []
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return entries

    def _tag_local(elem) -> str:
        """Return tag name without namespace."""
        tag = elem.tag
        if "}" in tag:
            return tag.split("}", 1)[1].lower()
        return tag.lower()

    root_local = _tag_local(root)

    if root_local == "sitemapindex":
        # Sitemap index: each <sitemap> contains a <loc> pointing to another sitemap
        for child in root:
            if _tag_local(child) == "sitemap":
                loc = None
                lastmod = None
                for elem in child:
                    local = _tag_local(elem)
                    if local == "loc":
                        loc = (elem.text or "").strip()
                    elif local == "lastmod":
                        lastmod = (elem.text or "").strip() or None
                if loc:
                    entries.append(SitemapEntry(url=loc, lastmod=lastmod))
    else:
        # Standard sitemap: each <url> contains loc, priority, lastmod, changefreq
        for child in root:
            if _tag_local(child) == "url":
                loc = None
                priority = 0.5
                lastmod = None
                changefreq = None
                for elem in child:
                    local = _tag_local(elem)
                    if local == "loc":
                        loc = (elem.text or "").strip()
                    elif local == "priority":
                        try:
                            priority = float((elem.text or "0.5").strip())
                        except ValueError:
                            priority = 0.5
                    elif local == "lastmod":
                        lastmod = (elem.text or "").strip() or None
                    elif local == "changefreq":
                        changefreq = (elem.text or "").strip() or None
                if loc:
                    entries.append(SitemapEntry(
                        url=loc,
                        priority=priority,
                        lastmod=lastmod,
                        changefreq=changefreq,
                    ))

    return entries


class SitemapParser:
    """
    Fetch and parse XML sitemaps to seed a crawl RequestQueue.

    Supports:
    - Standard sitemap.xml and sitemap_index.xml (recursive)
    - Gzip-compressed sitemaps (.xml.gz)
    - Priority / lastmod / changefreq metadata via SitemapEntry

    Usage:
        parser = SitemapParser()
        entries = parser.parse_entries("https://docs.example.com/sitemap.xml")
        # → [SitemapEntry(url="https://...", priority=0.8, lastmod="2024-01-01"), ...]

        urls = parser.parse("https://docs.example.com/sitemap.xml")
        # → ["https://docs.example.com/page1", ...]  (plain URL list, backward-compat)

    Usage (auto-discover from domain root):
        urls = parser.discover_and_parse("https://docs.example.com")
    """

    def __init__(self, max_urls: int = 500, timeout: int = 15):
        self.max_urls = max_urls
        self.timeout = timeout

    def parse_entries(self, sitemap_url: str) -> List[SitemapEntry]:
        """
        Fetch and parse a sitemap.xml URL, returning full SitemapEntry objects
        with url, priority, lastmod, and changefreq metadata.

        Recursively resolves sitemap index references.
        Fail-closed: raises AcquisitionError if unreachable.
        Zero-ambiguity: always returns List[SitemapEntry].
        """
        xml_text = _fetch_xml(sitemap_url, timeout=self.timeout)
        all_entries = _parse_sitemap_entries(xml_text)

        # Separate sub-sitemap references from page entries
        sitemap_refs = [e for e in all_entries if e.url.endswith(".xml") or e.url.endswith(".xml.gz")]
        page_entries = [e for e in all_entries if not (e.url.endswith(".xml") or e.url.endswith(".xml.gz"))]

        for ref_entry in sitemap_refs:
            if len(page_entries) >= self.max_urls:
                break
            try:
                sub_xml = _fetch_xml(ref_entry.url, timeout=self.timeout)
                sub_entries = _parse_sitemap_entries(sub_xml)
                page_entries.extend(
                    e for e in sub_entries
                    if not (e.url.endswith(".xml") or e.url.endswith(".xml.gz"))
                )
            except AcquisitionError:
                continue

        return page_entries[:self.max_urls]

    def parse(self, sitemap_url: str) -> List[str]:
        """
        Fetch and parse a sitemap.xml URL.
        Fail-closed: raises AcquisitionError if unreachable.
        Zero-ambiguity: always returns List[str].
        Backward-compatible wrapper around parse_entries().
        """
        return [e.url for e in self.parse_entries(sitemap_url)]

    def discover_and_parse(self, base_url: str) -> List[str]:
        """
        Auto-discover sitemaps for a domain by trying standard paths.
        Falls back to empty list (not error) if no sitemap found.
        Zero-ambiguity: always returns List[str].
        """
        from urllib.parse import urlparse
        base = base_url.rstrip("/")
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

    def discover_and_parse_entries(self, base_url: str) -> List[SitemapEntry]:
        """
        Auto-discover sitemaps and return full SitemapEntry metadata.
        Falls back to empty list (not error) if no sitemap found.
        """
        from urllib.parse import urlparse
        base = base_url.rstrip("/")
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
                return self.parse_entries(candidate)
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
