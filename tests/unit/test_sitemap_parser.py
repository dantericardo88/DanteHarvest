"""Tests for SitemapParser — sitemap.xml URL discovery."""

import pytest
from unittest.mock import patch
from harvest_acquire.crawl.sitemap_parser import SitemapParser, _parse_sitemap_xml
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
