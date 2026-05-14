"""Tests for SitemapParser — sitemap.xml URL discovery."""

import gzip
import pytest
from unittest.mock import patch
from harvest_acquire.crawl.sitemap_parser import (
    SitemapParser,
    SitemapEntry,
    _parse_sitemap_xml,
    _parse_sitemap_entries,
    _fetch_xml,
)
from harvest_core.control.exceptions import AcquisitionError


_SIMPLE_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/page1</loc></url>
  <url><loc>https://example.com/page2</loc></url>
  <url><loc>https://example.com/about</loc></url>
</urlset>"""

_INDEX_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap-pages.xml</loc></sitemap>
  <sitemap><loc>https://example.com/sitemap-blog.xml</loc></sitemap>
</sitemapindex>"""


def test_parse_simple_sitemap():
    urls = _parse_sitemap_xml(_SIMPLE_SITEMAP)
    assert len(urls) == 3
    assert "https://example.com/page1" in urls


def test_parse_sitemap_index():
    urls = _parse_sitemap_xml(_INDEX_SITEMAP)
    assert len(urls) == 2
    assert all(u.endswith(".xml") for u in urls)


def test_parse_empty_sitemap():
    urls = _parse_sitemap_xml("<urlset></urlset>")
    assert urls == []


def test_parse_malformed_xml():
    urls = _parse_sitemap_xml("not xml at all <<<")
    assert urls == []


def test_sitemap_parser_fetch_and_parse():
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml", return_value=_SIMPLE_SITEMAP):
        urls = parser.parse("https://example.com/sitemap.xml")
    assert len(urls) == 3
    assert "https://example.com/about" in urls


def test_sitemap_parser_unreachable_raises():
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml",
               side_effect=AcquisitionError("HTTP 404")):
        with pytest.raises(AcquisitionError):
            parser.parse("https://example.com/sitemap.xml")


def test_discover_and_parse_falls_back_gracefully():
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml",
               side_effect=AcquisitionError("unreachable")):
        urls = parser.discover_and_parse("https://example.com")
    assert urls == []


def test_max_urls_respected():
    big_sitemap = "<urlset>" + "".join(
        f"<url><loc>https://example.com/p{i}</loc></url>" for i in range(100)
    ) + "</urlset>"
    parser = SitemapParser(max_urls=10)
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml", return_value=big_sitemap):
        urls = parser.parse("https://example.com/sitemap.xml")
    assert len(urls) <= 10


def test_get_crawl_delay_parses():
    robots_txt = "User-agent: *\nCrawl-delay: 2\nDisallow: /private"
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml", return_value=robots_txt):
        delay = parser.get_crawl_delay("https://example.com/robots.txt")
    assert delay == 2.0


def test_get_crawl_delay_missing_returns_zero():
    robots_txt = "User-agent: *\nDisallow: /private"
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml", return_value=robots_txt):
        delay = parser.get_crawl_delay("https://example.com/robots.txt")
    assert delay == 0.0


# ---------------------------------------------------------------------------
# SitemapEntry metadata tests
# ---------------------------------------------------------------------------

_METADATA_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/high-priority</loc>
    <priority>0.9</priority>
    <lastmod>2024-03-01</lastmod>
    <changefreq>daily</changefreq>
  </url>
  <url>
    <loc>https://example.com/low-priority</loc>
    <priority>0.3</priority>
    <lastmod>2023-01-15</lastmod>
    <changefreq>monthly</changefreq>
  </url>
  <url>
    <loc>https://example.com/no-meta</loc>
  </url>
</urlset>"""


def test_parse_sitemap_entries_returns_sitemap_entry_objects():
    entries = _parse_sitemap_entries(_METADATA_SITEMAP)
    assert len(entries) == 3
    assert all(isinstance(e, SitemapEntry) for e in entries)


def test_parse_sitemap_entries_priority():
    entries = _parse_sitemap_entries(_METADATA_SITEMAP)
    high = next(e for e in entries if "high-priority" in e.url)
    low = next(e for e in entries if "low-priority" in e.url)
    assert high.priority == 0.9
    assert low.priority == 0.3


def test_parse_sitemap_entries_lastmod():
    entries = _parse_sitemap_entries(_METADATA_SITEMAP)
    high = next(e for e in entries if "high-priority" in e.url)
    assert high.lastmod == "2024-03-01"


def test_parse_sitemap_entries_changefreq():
    entries = _parse_sitemap_entries(_METADATA_SITEMAP)
    high = next(e for e in entries if "high-priority" in e.url)
    assert high.changefreq == "daily"


def test_parse_sitemap_entries_defaults_for_missing_meta():
    entries = _parse_sitemap_entries(_METADATA_SITEMAP)
    no_meta = next(e for e in entries if "no-meta" in e.url)
    assert no_meta.priority == 0.5
    assert no_meta.lastmod is None
    assert no_meta.changefreq is None


def test_parse_entries_via_parser():
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml", return_value=_METADATA_SITEMAP):
        entries = parser.parse_entries("https://example.com/sitemap.xml")
    assert len(entries) == 3
    assert entries[0].url == "https://example.com/high-priority"


# ---------------------------------------------------------------------------
# Gzip sitemap tests
# ---------------------------------------------------------------------------

def test_fetch_xml_decompresses_gzip(tmp_path):
    """_fetch_xml transparently decompresses gzip-encoded sitemap bytes."""
    xml_content = _METADATA_SITEMAP.encode("utf-8")
    gzip_bytes = gzip.compress(xml_content)

    import urllib.response
    import io

    class _FakeResp:
        def read(self):
            return gzip_bytes
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        headers = {"Content-Encoding": ""}  # no transport encoding — raw gzip file

    with patch("urllib.request.urlopen", return_value=_FakeResp()):
        result = _fetch_xml("https://example.com/sitemap.xml.gz")

    assert "high-priority" in result
    assert result.startswith("<?xml")


def test_parse_entries_gzip_sitemap():
    """parse_entries works end-to-end with a gzip-compressed sitemap."""
    xml_bytes = _METADATA_SITEMAP.encode("utf-8")
    gzip_bytes = gzip.compress(xml_bytes)

    class _FakeResp:
        def read(self):
            return gzip_bytes
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        headers = {"Content-Encoding": ""}

    parser = SitemapParser()
    with patch("urllib.request.urlopen", return_value=_FakeResp()):
        entries = parser.parse_entries("https://example.com/sitemap.xml.gz")

    assert len(entries) == 3
    assert entries[0].priority == 0.9


# ---------------------------------------------------------------------------
# discover_and_parse_entries tests
# ---------------------------------------------------------------------------

def test_discover_and_parse_entries_returns_entries():
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml", return_value=_METADATA_SITEMAP):
        entries = parser.discover_and_parse_entries("https://example.com")
    assert len(entries) == 3
    assert all(isinstance(e, SitemapEntry) for e in entries)


def test_discover_and_parse_entries_fallback_empty():
    parser = SitemapParser()
    with patch("harvest_acquire.crawl.sitemap_parser._fetch_xml",
               side_effect=AcquisitionError("unreachable")):
        entries = parser.discover_and_parse_entries("https://example.com")
    assert entries == []
