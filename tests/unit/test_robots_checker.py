"""Tests for RobotsChecker — robots.txt parsing and crawl-delay enforcement."""

import asyncio
import time
import pytest
from unittest.mock import patch

from harvest_acquire.crawl.robots_checker import (
    RobotsChecker,
    _parse_robots,
    _path_matches,
    _fetch_robots,
)


# ---------------------------------------------------------------------------
# Robots.txt fixtures
# ---------------------------------------------------------------------------

_SIMPLE_ROBOTS = """\
User-agent: *
Disallow: /private
Disallow: /admin/
Allow: /admin/public
Crawl-delay: 1
"""

_MULTI_AGENT_ROBOTS = """\
User-agent: Googlebot
Disallow: /nogoogle

User-agent: HarvestBot
Disallow: /noharvest
Crawl-delay: 2

User-agent: *
Disallow: /noone
"""

_EMPTY_ROBOTS = ""

_WILDCARD_ROBOTS = """\
User-agent: *
Disallow: /secret*
Allow: /secret/ok
"""

_ANCHORED_ROBOTS = """\
User-agent: *
Disallow: /page$
"""


# ---------------------------------------------------------------------------
# _path_matches unit tests
# ---------------------------------------------------------------------------

def test_path_matches_simple_prefix():
    assert _path_matches("/private/page", "/private") is True


def test_path_matches_no_match():
    assert _path_matches("/public/page", "/private") is False


def test_path_matches_wildcard():
    assert _path_matches("/secret/anything", "/secret*") is True


def test_path_matches_wildcard_no_match():
    assert _path_matches("/other/path", "/secret*") is False


def test_path_matches_end_anchor():
    assert _path_matches("/page", "/page$") is True


def test_path_matches_end_anchor_no_match():
    assert _path_matches("/page/sub", "/page$") is False


def test_path_matches_root_disallow():
    # Disallow: / means everything is blocked
    assert _path_matches("/anything", "/") is True


# ---------------------------------------------------------------------------
# _parse_robots unit tests
# ---------------------------------------------------------------------------

def test_parse_robots_wildcard_disallow():
    rules = _parse_robots(_SIMPLE_ROBOTS, "SomeBot")
    assert "/private" in rules.disallow


def test_parse_robots_crawl_delay():
    rules = _parse_robots(_SIMPLE_ROBOTS, "SomeBot")
    assert rules.crawl_delay == 1.0


def test_parse_robots_exact_agent_wins():
    rules = _parse_robots(_MULTI_AGENT_ROBOTS, "harvestbot")
    assert "/noharvest" in rules.disallow
    assert "/nogoogle" not in rules.disallow


def test_parse_robots_exact_agent_crawl_delay():
    rules = _parse_robots(_MULTI_AGENT_ROBOTS, "harvestbot")
    assert rules.crawl_delay == 2.0


def test_parse_robots_falls_back_to_wildcard():
    rules = _parse_robots(_MULTI_AGENT_ROBOTS, "unknownbot")
    assert "/noone" in rules.disallow


def test_parse_robots_empty_returns_empty_rules():
    rules = _parse_robots(_EMPTY_ROBOTS, "AnyBot")
    assert rules.disallow == []
    assert rules.crawl_delay == 0.0


def test_parse_robots_allow_listed():
    rules = _parse_robots(_SIMPLE_ROBOTS, "SomeBot")
    assert "/admin/public" in rules.allow


# ---------------------------------------------------------------------------
# RobotsChecker.is_allowed tests (mocked HTTP)
# ---------------------------------------------------------------------------

def test_is_allowed_disallowed_path():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS):
        assert checker.is_allowed("https://example.com/private/page") is False


def test_is_allowed_allowed_path():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS):
        assert checker.is_allowed("https://example.com/public/page") is True


def test_is_allowed_allow_overrides_disallow():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS):
        # /admin/ is disallowed but /admin/public is explicitly allowed
        assert checker.is_allowed("https://example.com/admin/public") is True


def test_is_allowed_unreachable_robots_allows_all():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=None):
        assert checker.is_allowed("https://example.com/anything") is True


def test_is_allowed_caches_per_domain():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS) as mock_fetch:
        checker.is_allowed("https://example.com/page1")
        checker.is_allowed("https://example.com/page2")
    # robots.txt fetched only once for the domain
    assert mock_fetch.call_count == 1


def test_is_allowed_separate_domains_fetch_separately():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS) as mock_fetch:
        checker.is_allowed("https://example.com/page")
        checker.is_allowed("https://other.com/page")
    assert mock_fetch.call_count == 2


def test_clear_cache_forces_refetch():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS) as mock_fetch:
        checker.is_allowed("https://example.com/page")
        checker.clear_cache()
        checker.is_allowed("https://example.com/page")
    assert mock_fetch.call_count == 2


# ---------------------------------------------------------------------------
# RobotsChecker.crawl_delay tests
# ---------------------------------------------------------------------------

def test_crawl_delay_returns_value():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_SIMPLE_ROBOTS):
        assert checker.crawl_delay("https://example.com/") == 1.0


def test_crawl_delay_returns_zero_when_absent():
    robots = "User-agent: *\nDisallow: /private\n"
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=robots):
        assert checker.crawl_delay("https://example.com/") == 0.0


def test_crawl_delay_unreachable_returns_zero():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=None):
        assert checker.crawl_delay("https://example.com/") == 0.0


# ---------------------------------------------------------------------------
# RobotsChecker.async_respect_delay tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_respect_delay_no_delay():
    """Zero crawl-delay: async_respect_delay returns immediately."""
    robots = "User-agent: *\nDisallow:\n"
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=robots):
        start = time.monotonic()
        await checker.async_respect_delay("https://example.com/")
        elapsed = time.monotonic() - start
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_async_respect_delay_second_call_is_immediate():
    """After waiting once, immediate second call within delay window respects remaining time."""
    robots = "User-agent: *\nCrawl-delay: 0.05\n"
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=robots):
        await checker.async_respect_delay("https://example.com/")
        # Second call right after — should sleep ~0.05s but not error
        await checker.async_respect_delay("https://example.com/")


# ---------------------------------------------------------------------------
# Wildcard and anchor pattern integration
# ---------------------------------------------------------------------------

def test_wildcard_disallow_blocks_matching_paths():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_WILDCARD_ROBOTS):
        assert checker.is_allowed("https://example.com/secret/data") is False


def test_allow_overrides_wildcard_disallow():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_WILDCARD_ROBOTS):
        assert checker.is_allowed("https://example.com/secret/ok") is True


def test_anchored_disallow_exact_only():
    checker = RobotsChecker(user_agent="SomeBot")
    with patch("harvest_acquire.crawl.robots_checker._fetch_robots", return_value=_ANCHORED_ROBOTS):
        assert checker.is_allowed("https://example.com/page") is False
        assert checker.is_allowed("https://example.com/page/sub") is True
