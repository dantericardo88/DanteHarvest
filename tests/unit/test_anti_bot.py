"""
Phase 9 — Anti-bot basics tests.

Verifies:
1. stealth_headers() returns 20+ UAs and realistic headers
2. random_ua() returns different values
3. respect_retry_after() honours Retry-After header
4. CrawleeAdapter accepts proxy_url and use_stealth_headers params
5. _fetch_url uses stealth headers when enabled
"""

import time
from unittest.mock import patch, MagicMock
import pytest

from harvest_acquire.crawl.stealth_headers import (
    stealth_headers, random_ua, respect_retry_after, USER_AGENTS
)
from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter


# ---------------------------------------------------------------------------
# User-agent pool
# ---------------------------------------------------------------------------

def test_ua_pool_has_20_plus():
    assert len(USER_AGENTS) >= 20


def test_random_ua_returns_string():
    ua = random_ua()
    assert isinstance(ua, str)
    assert len(ua) > 20


def test_random_ua_rotates():
    seen = set(random_ua() for _ in range(100))
    assert len(seen) > 1  # Not always the same


# ---------------------------------------------------------------------------
# stealth_headers()
# ---------------------------------------------------------------------------

def test_stealth_headers_contains_required_keys():
    h = stealth_headers()
    assert "User-Agent" in h
    assert "Accept" in h
    assert "Accept-Language" in h


def test_stealth_headers_chrome_includes_sec_headers():
    chrome_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    h = stealth_headers(user_agent=chrome_ua)
    assert "Sec-Ch-Ua" in h
    assert "Sec-Fetch-Mode" in h
    assert "Sec-Fetch-Dest" in h


def test_stealth_headers_firefox_no_sec_ch():
    firefox_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
    h = stealth_headers(user_agent=firefox_ua)
    assert "Sec-Ch-Ua" not in h


def test_stealth_headers_custom_ua():
    custom = "MyCustomAgent/1.0"
    h = stealth_headers(user_agent=custom)
    assert h["User-Agent"] == custom


def test_stealth_headers_vary_across_calls():
    headers1 = stealth_headers()
    headers2 = stealth_headers()
    # User-Agent may differ (random selection)
    uas = {stealth_headers()["User-Agent"] for _ in range(50)}
    assert len(uas) > 1


# ---------------------------------------------------------------------------
# respect_retry_after()
# ---------------------------------------------------------------------------

def test_respect_retry_after_numeric():
    sleep_calls = []
    with patch("harvest_acquire.crawl.stealth_headers.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        respect_retry_after({"Retry-After": "2"})
    assert sleep_calls == [2.0]


def test_respect_retry_after_caps_at_60():
    sleep_calls = []
    with patch("harvest_acquire.crawl.stealth_headers.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        respect_retry_after({"Retry-After": "9999"})
    assert sleep_calls == [60.0]


def test_respect_retry_after_missing_header():
    with patch("harvest_acquire.crawl.stealth_headers.time.sleep") as mock_sleep:
        respect_retry_after({})
        mock_sleep.assert_not_called()


def test_respect_retry_after_invalid_value():
    with patch("harvest_acquire.crawl.stealth_headers.time.sleep") as mock_sleep:
        respect_retry_after({"Retry-After": "not-a-number"})
        mock_sleep.assert_not_called()


def test_respect_retry_after_lowercase_header():
    sleep_calls = []
    with patch("harvest_acquire.crawl.stealth_headers.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        respect_retry_after({"retry-after": "5"})
    assert sleep_calls == [5.0]


# ---------------------------------------------------------------------------
# CrawleeAdapter accepts new params
# ---------------------------------------------------------------------------

def test_crawlee_adapter_accepts_proxy_url():
    adapter = CrawleeAdapter(proxy_url="http://proxy:8080")
    assert adapter._proxy_url == "http://proxy:8080"


def test_crawlee_adapter_accepts_stealth():
    adapter = CrawleeAdapter(use_stealth_headers=True)
    assert adapter._stealth is True


def test_crawlee_adapter_stealth_false_by_default():
    adapter = CrawleeAdapter()
    assert adapter._stealth is False


def test_crawlee_adapter_proxy_none_by_default():
    adapter = CrawleeAdapter()
    assert adapter._proxy_url is None


# ---------------------------------------------------------------------------
# _fetch_url passes stealth headers
# ---------------------------------------------------------------------------

def test_fetch_url_uses_stealth_headers_when_enabled():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url

    captured_headers = {}

    class MockResp:
        status = 200
        def read(self): return b"<html></html>"
        def __enter__(self): return self
        def __exit__(self, *a): pass

    def fake_open(req, timeout=None):
        captured_headers.update(req.headers)
        return MockResp()

    opener = MagicMock()
    opener.open = fake_open

    with patch("urllib.request.build_opener", return_value=opener):
        _fetch_url("https://example.com", use_stealth_headers=True)

    ua_header = captured_headers.get("User-agent", "")
    assert "HarvestBot" not in ua_header


def test_fetch_url_uses_default_ua_when_stealth_disabled():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url

    captured_headers = {}

    class MockResp:
        status = 200
        def read(self): return b"<html></html>"
        def __enter__(self): return self
        def __exit__(self, *a): pass

    def fake_open(req, timeout=None):
        captured_headers.update(req.headers)
        return MockResp()

    opener = MagicMock()
    opener.open = fake_open

    with patch("urllib.request.build_opener", return_value=opener):
        _fetch_url("https://example.com", use_stealth_headers=False)

    assert "HarvestBot" in captured_headers.get("User-agent", "")
