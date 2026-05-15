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


# ---------------------------------------------------------------------------
# error_threshold parameter and health_report()
# ---------------------------------------------------------------------------

def test_error_threshold_default():
    pool = PlaywrightPool()
    assert pool.error_threshold == 5


def test_error_threshold_custom():
    pool = PlaywrightPool(error_threshold=3)
    assert pool.error_threshold == 3


def test_health_report_healthy_no_errors():
    pool = PlaywrightPool(error_threshold=5)
    browser = MagicMock()
    pool._slots.append(_BrowserSlot(browser=browser))
    pool._open = True
    report = pool.health_report()
    assert report["status"] == "healthy"
    assert report["active_browsers"] == 1
    assert report["total_errors"] == 0
    assert report["error_threshold"] == 5
    assert report["recommendations"] == []


def test_health_report_critical_no_active_browsers():
    pool = PlaywrightPool(error_threshold=5)
    pool._open = True
    report = pool.health_report()
    assert report["status"] == "critical"
    assert report["active_browsers"] == 0
    assert "Pool exhausted — increase max_browsers" in report["recommendations"]


def test_health_report_degraded_when_errors_at_half_threshold():
    pool = PlaywrightPool(error_threshold=4)
    browser = MagicMock()
    slot = _BrowserSlot(browser=browser)
    slot.error_count = 2  # == threshold / 2
    pool._slots.append(slot)
    pool._open = True
    report = pool.health_report()
    assert report["status"] == "degraded"


def test_health_report_includes_browser_error_recommendation():
    pool = PlaywrightPool(error_threshold=5)
    browser = MagicMock()
    slot = _BrowserSlot(browser=browser)
    slot.error_count = 2
    pool._slots.append(slot)
    pool._open = True
    report = pool.health_report()
    assert any("Browser #0" in r and "2 errors" in r for r in report["recommendations"])


def test_health_report_healthy_below_half_threshold():
    pool = PlaywrightPool(error_threshold=10)
    browser = MagicMock()
    slot = _BrowserSlot(browser=browser)
    slot.error_count = 4  # < 10/2 = 5
    pool._slots.append(slot)
    pool._open = True
    report = pool.health_report()
    assert report["status"] == "healthy"


def test_health_report_browsers_list_structure():
    pool = PlaywrightPool(error_threshold=5)
    browser = MagicMock()
    pool._slots.append(_BrowserSlot(browser=browser))
    pool._open = True
    report = pool.health_report()
    assert len(report["browsers"]) == 1
    b = report["browsers"][0]
    assert "index" in b
    assert "state" in b
    assert "pages_served" in b
    assert "error_count" in b


@pytest.mark.asyncio
async def test_release_page_increments_error_count_on_close_failure():
    browser, page = _make_mock_browser()
    page.close = AsyncMock(side_effect=RuntimeError("crash"))
    pw = _make_mock_playwright(browser)

    pool = PlaywrightPool(error_threshold=5)
    pool._playwright = pw
    pool._open = True

    slot = _BrowserSlot(browser=browser)
    pool._slots.append(slot)
    page._harvest_slot = slot

    await pool._release_page(page)
    assert slot.error_count == 1


@pytest.mark.asyncio
async def test_release_page_retires_slot_at_error_threshold():
    browser, page = _make_mock_browser()
    page.close = AsyncMock(side_effect=RuntimeError("crash"))
    pw = _make_mock_playwright(browser)

    pool = PlaywrightPool(error_threshold=3)
    pool._playwright = pw
    pool._open = True

    slot = _BrowserSlot(browser=browser)
    slot.error_count = 2  # one more will hit threshold
    pool._slots.append(slot)
    page._harvest_slot = slot

    await pool._release_page(page)
    assert slot.state == "retired"
