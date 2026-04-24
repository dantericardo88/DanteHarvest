"""
Phase 3 — PlaywrightPool browser infrastructure tests.

Tests pool lifecycle, hook invocation, stats, and CrawleeAdapter integration
without launching a real browser (all Playwright calls are mocked).
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from harvest_acquire.browser.playwright_pool import PlaywrightPool, _BrowserSlot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_browser():
    browser = MagicMock()
    browser.close = AsyncMock()
    page = MagicMock()
    page.close = AsyncMock()
    browser.new_page = AsyncMock(return_value=page)
    return browser, page


def _make_mock_playwright(browser):
    pw = MagicMock()
    pw.chromium.launch = AsyncMock(return_value=browser)
    pw.stop = AsyncMock()
    return pw


# ---------------------------------------------------------------------------
# Module existence
# ---------------------------------------------------------------------------

def test_playwright_pool_importable():
    from harvest_acquire.browser.playwright_pool import PlaywrightPool
    assert PlaywrightPool


def test_playwright_pool_has_required_attrs():
    pool = PlaywrightPool()
    assert pool.max_browsers == 3
    assert pool.retire_after_pages == 50
    assert pool.idle_close_secs == 300
    assert pool.proxy_url is None


def test_playwright_pool_accepts_all_hooks():
    hook = AsyncMock()
    pool = PlaywrightPool(
        pre_launch=hook,
        post_launch=hook,
        pre_page_create=hook,
        post_page_create=hook,
        pre_page_close=hook,
        post_page_close=hook,
    )
    assert pool._hooks["pre_launch"] is hook
    assert pool._hooks["post_launch"] is hook


def test_playwright_pool_accepts_proxy():
    pool = PlaywrightPool(proxy_url="http://proxy:8080")
    assert pool.proxy_url == "http://proxy:8080"


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def test_stats_initial():
    pool = PlaywrightPool()
    s = pool.stats()
    assert s["open"] is False
    assert s["active_browsers"] == 0
    assert s["max_browsers"] == 3


def test_stats_reflects_slots():
    pool = PlaywrightPool()
    browser = MagicMock()
    pool._slots.append(_BrowserSlot(browser=browser))
    pool._open = True
    s = pool.stats()
    assert s["active_browsers"] == 1
    assert s["open"] is True


# ---------------------------------------------------------------------------
# Lifecycle: open / close
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_open_sets_flag():
    browser, page = _make_mock_browser()
    pw = _make_mock_playwright(browser)
    pool = PlaywrightPool()
    with patch("playwright.async_api.async_playwright") as mock_ap:
        ctx = MagicMock()
        ctx.start = AsyncMock(return_value=pw)
        mock_ap.return_value = ctx
        await pool.open()
    assert pool._open is True
    pool._playwright = pw
    await pool.close()
    assert pool._open is False


@pytest.mark.asyncio
async def test_close_clears_slots():
    browser, page = _make_mock_browser()
    pool = PlaywrightPool()
    pool._open = True
    pool._playwright = _make_mock_playwright(browser)
    pool._slots.append(_BrowserSlot(browser=browser))
    await pool.close()
    assert pool._slots == []
    browser.close.assert_called_once()


# ---------------------------------------------------------------------------
# Hook invocation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hooks_called_on_page_lifecycle():
    browser, page = _make_mock_browser()
    pw = _make_mock_playwright(browser)

    pre_page_create = AsyncMock()
    post_page_create = AsyncMock()
    pre_page_close = AsyncMock()
    post_page_close = AsyncMock()
    pre_launch = AsyncMock()
    post_launch = AsyncMock()

    pool = PlaywrightPool(
        pre_launch=pre_launch,
        post_launch=post_launch,
        pre_page_create=pre_page_create,
        post_page_create=post_page_create,
        pre_page_close=pre_page_close,
        post_page_close=post_page_close,
    )
    pool._playwright = pw
    pool._open = True

    async with pool.acquire_page() as p:
        assert p is page

    pre_launch.assert_called_once()
    post_launch.assert_called_once()
    pre_page_create.assert_called_once()
    post_page_create.assert_called_once()
    pre_page_close.assert_called_once()
    post_page_close.assert_called_once()


# ---------------------------------------------------------------------------
# Retirement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_browser_retired_after_page_limit():
    browser, page = _make_mock_browser()
    pw = _make_mock_playwright(browser)

    pool = PlaywrightPool(retire_after_pages=2)
    pool._playwright = pw
    pool._open = True

    async with pool.acquire_page():
        pass
    async with pool.acquire_page():
        pass

    # After 2 page closes, retire_stale_slots is called; browser should be retired
    assert all(s.state == "retired" or s not in pool._slots for s in pool._slots)


@pytest.mark.asyncio
async def test_pool_exhaustion_raises():
    browser, _ = _make_mock_browser()
    pw = _make_mock_playwright(browser)

    pool = PlaywrightPool(max_browsers=1, retire_after_pages=1000)
    pool._playwright = pw
    pool._open = True

    # Fill pool to capacity: one active browser already at retire limit
    # Slot is active but exhausted its page budget — no headroom left
    slot = _BrowserSlot(browser=browser)
    slot.pages_served = 1000  # == retire_after_pages, so no headroom
    pool._slots.append(slot)

    # Pool has 1 active browser at capacity — launch would exceed max_browsers=1
    from harvest_core.control.exceptions import AcquisitionError
    with pytest.raises(AcquisitionError, match="exhausted"):
        await pool._find_or_launch_slot()


# ---------------------------------------------------------------------------
# CrawleeAdapter accepts browser_pool param
# ---------------------------------------------------------------------------

def test_crawlee_adapter_accepts_pool():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    pool = PlaywrightPool()
    adapter = CrawleeAdapter(browser_pool=pool)
    assert adapter._browser_pool is pool


def test_crawlee_adapter_pool_none_by_default():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter()
    assert adapter._browser_pool is None
