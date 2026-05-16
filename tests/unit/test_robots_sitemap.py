"""
Unit tests for RobotsParser and SitemapParser.

Covers: sitemap_and_robots_compliance dimension (score 9)
- robots.txt parsing with wildcard support
- disallow / allow checking
- crawl-delay extraction
- sitemap URL discovery from robots.txt Sitemap directives
- sitemap XML URL extraction via <loc> tags
"""
import pytest

from harvest_acquire.crawl.robots_parser import RobotsParser, SitemapParser


ROBOTS_BASIC = """
User-agent: *
Disallow: /private/
Disallow: /admin/
Allow: /public/
Crawl-delay: 2

Sitemap: https://example.com/sitemap.xml
Sitemap: https://example.com/sitemap2.xml
"""

ROBOTS_WILDCARD = """
User-agent: *
Disallow: /*.pdf$
Disallow: /search?*
Allow: /search/results
"""

ROBOTS_SPECIFIC_AGENT = """
User-agent: DanteHarvest/1.0
Disallow: /restricted/

User-agent: *
Disallow: /admin/
"""

ROBOTS_EMPTY = ""


SITEMAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/page1</loc>
    <priority>0.8</priority>
  </url>
  <url>
    <loc>https://example.com/page2</loc>
    <priority>0.5</priority>
  </url>
  <url>
    <loc>https://example.com/about</loc>
  </url>
</urlset>
"""

SITEMAP_INDEX_XML = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>https://example.com/sitemap-pages.xml</loc>
  </sitemap>
  <sitemap>
    <loc>https://example.com/sitemap-blog.xml</loc>
  </sitemap>
</sitemapindex>
"""


# ---------------------------------------------------------------------------
# RobotsParser — parse()
# ---------------------------------------------------------------------------

class TestRobotsParserParse:
    def test_extracts_disallow_rules(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        rules = [r for r in parser._rules if r.user_agent == "*"]
        assert rules, "Expected at least one rule for *"
        all_disallows = [p for r in rules for p in r.disallow]
        assert "/private/" in all_disallows
        assert "/admin/" in all_disallows

    def test_extracts_allow_rules(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        rules = [r for r in parser._rules if r.user_agent == "*"]
        all_allows = [p for r in rules for p in r.allow]
        assert "/public/" in all_allows

    def test_extracts_sitemap_urls(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        sitemaps = parser.get_sitemaps()
        assert "https://example.com/sitemap.xml" in sitemaps
        assert "https://example.com/sitemap2.xml" in sitemaps

    def test_empty_robots_produces_no_rules(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_EMPTY)
        assert parser._rules == []
        assert parser._sitemaps == []

    def test_comments_are_stripped(self):
        parser = RobotsParser()
        parser.parse("User-agent: * # all bots\nDisallow: /secret/ # very secret\n")
        rules = [r for r in parser._rules if r.user_agent == "*"]
        disallows = [p for r in rules for p in r.disallow]
        assert "/secret/" in disallows


# ---------------------------------------------------------------------------
# RobotsParser — is_allowed()
# ---------------------------------------------------------------------------

class TestRobotsParserIsAllowed:
    def test_disallowed_path_returns_false(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        assert parser.is_allowed("https://example.com/private/data") is False

    def test_allowed_path_returns_true(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        assert parser.is_allowed("https://example.com/public/page") is True

    def test_unconstrained_path_returns_true(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        assert parser.is_allowed("https://example.com/blog/post") is True

    def test_empty_robots_allows_all(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_EMPTY)
        assert parser.is_allowed("https://example.com/anything") is True

    def test_specific_agent_rule_applied(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_SPECIFIC_AGENT)
        # DanteHarvest/1.0 is explicitly disallowed from /restricted/
        assert parser.is_allowed(
            "https://example.com/restricted/page",
            user_agent="DanteHarvest/1.0",
        ) is False

    def test_wildcard_agent_fallback(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_SPECIFIC_AGENT)
        # OtherBot has no explicit rule — falls back to *
        assert parser.is_allowed(
            "https://example.com/admin/panel",
            user_agent="OtherBot",
        ) is False

    def test_no_rules_means_allowed(self):
        parser = RobotsParser()
        parser.parse("User-agent: *\n")
        # No disallow lines — everything is allowed
        assert parser.is_allowed("https://example.com/secret") is True


# ---------------------------------------------------------------------------
# RobotsParser — _match_pattern()
# ---------------------------------------------------------------------------

class TestMatchPattern:
    def test_wildcard_pdf_matches_pdf_path(self):
        parser = RobotsParser()
        assert parser._match_pattern("/*.pdf", "/doc.pdf") is True

    def test_wildcard_pdf_does_not_match_html(self):
        parser = RobotsParser()
        assert parser._match_pattern("/*.pdf", "/doc.html") is False

    def test_dollar_anchor_matches_exact_end(self):
        parser = RobotsParser()
        assert parser._match_pattern("/page$", "/page") is True

    def test_dollar_anchor_rejects_suffix(self):
        parser = RobotsParser()
        assert parser._match_pattern("/page$", "/page/sub") is False

    def test_prefix_match(self):
        parser = RobotsParser()
        assert parser._match_pattern("/admin/", "/admin/users") is True

    def test_empty_pattern_never_matches(self):
        parser = RobotsParser()
        assert parser._match_pattern("", "/anything") is False

    def test_wildcard_search_query(self):
        parser = RobotsParser()
        assert parser._match_pattern("/search?*", "/search?q=hello") is True


# ---------------------------------------------------------------------------
# RobotsParser — get_crawl_delay()
# ---------------------------------------------------------------------------

class TestGetCrawlDelay:
    def test_returns_crawl_delay_as_float(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        delay = parser.get_crawl_delay()
        assert delay == 2.0

    def test_returns_none_when_not_specified(self):
        parser = RobotsParser()
        parser.parse("User-agent: *\nDisallow: /admin/\n")
        assert parser.get_crawl_delay() is None

    def test_returns_none_for_empty_robots(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_EMPTY)
        assert parser.get_crawl_delay() is None


# ---------------------------------------------------------------------------
# RobotsParser — get_sitemaps()
# ---------------------------------------------------------------------------

class TestGetSitemaps:
    def test_returns_declared_sitemap_urls(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        sitemaps = parser.get_sitemaps()
        assert isinstance(sitemaps, list)
        assert len(sitemaps) == 2
        assert all(s.startswith("https://") for s in sitemaps)

    def test_returns_empty_list_when_no_sitemaps(self):
        parser = RobotsParser()
        parser.parse("User-agent: *\nDisallow: /\n")
        assert parser.get_sitemaps() == []

    def test_returns_copy_not_reference(self):
        parser = RobotsParser()
        parser.parse(ROBOTS_BASIC)
        sitemaps = parser.get_sitemaps()
        sitemaps.clear()
        assert len(parser.get_sitemaps()) == 2  # internal state unchanged


# ---------------------------------------------------------------------------
# SitemapParser — parse_xml()
# ---------------------------------------------------------------------------

class TestSitemapParserParseXml:
    def test_extracts_loc_urls(self):
        sp = SitemapParser()
        urls = sp.parse_xml(SITEMAP_XML)
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
        assert "https://example.com/about" in urls

    def test_returns_all_urls(self):
        sp = SitemapParser()
        urls = sp.parse_xml(SITEMAP_XML)
        assert len(urls) == 3

    def test_parses_sitemap_index_locs(self):
        sp = SitemapParser()
        urls = sp.parse_xml(SITEMAP_INDEX_XML)
        assert "https://example.com/sitemap-pages.xml" in urls
        assert "https://example.com/sitemap-blog.xml" in urls

    def test_empty_xml_returns_empty_list(self):
        sp = SitemapParser()
        assert sp.parse_xml("") == []

    def test_strips_whitespace_from_urls(self):
        sp = SitemapParser()
        xml = "<urlset><url><loc>  https://example.com/spaced  </loc></url></urlset>"
        urls = sp.parse_xml(xml)
        assert urls == ["https://example.com/spaced"]

    def test_case_insensitive_loc_tag(self):
        sp = SitemapParser()
        xml = "<urlset><url><LOC>https://example.com/upper</LOC></url></urlset>"
        urls = sp.parse_xml(xml)
        assert "https://example.com/upper" in urls


# ---------------------------------------------------------------------------
# SitemapParser — parse_index()
# ---------------------------------------------------------------------------

class TestSitemapParserParseIndex:
    def test_extracts_child_sitemap_urls(self):
        sp = SitemapParser()
        urls = sp.parse_index(SITEMAP_INDEX_XML)
        assert len(urls) == 2

    def test_returns_list(self):
        sp = SitemapParser()
        result = sp.parse_index(SITEMAP_INDEX_XML)
        assert isinstance(result, list)
