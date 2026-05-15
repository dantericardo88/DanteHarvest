"""Tests for Wave 2a — SPA wait strategies and network idle detection in CrawleeAdapter."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# _fetch_url_playwright signature and constants
# ---------------------------------------------------------------------------

def test_fetch_playwright_accepts_wait_until_param():
    import inspect
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url_playwright
    sig = inspect.signature(_fetch_url_playwright)
    assert "wait_until" in sig.parameters
    assert "spa_mode" in sig.parameters
    assert "extra_wait_ms" in sig.parameters


def test_wait_strategies_constant():
    from harvest_acquire.crawl.crawlee_adapter import _WAIT_STRATEGIES
    assert "networkidle" in _WAIT_STRATEGIES
    assert "domcontentloaded" in _WAIT_STRATEGIES
    assert "load" in _WAIT_STRATEGIES
    assert "commit" in _WAIT_STRATEGIES


def test_spa_mutation_script_defined():
    from harvest_acquire.crawl.crawlee_adapter import _SPA_MUTATION_SCRIPT
    assert "MutationObserver" in _SPA_MUTATION_SCRIPT
    assert "resolve" in _SPA_MUTATION_SCRIPT


# ---------------------------------------------------------------------------
# CrawleeAdapter SPA parameters
# ---------------------------------------------------------------------------

def test_adapter_stores_wait_until():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(wait_until="domcontentloaded")
    assert adapter._wait_until == "domcontentloaded"


def test_adapter_stores_spa_mode():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(spa_mode=True)
    assert adapter._spa_mode is True


def test_adapter_stores_extra_wait_ms():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(extra_wait_ms=500)
    assert adapter._extra_wait_ms == 500


def test_adapter_invalid_wait_until_falls_back_to_networkidle():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(wait_until="invalid_strategy")
    assert adapter._wait_until == "networkidle"


def test_adapter_defaults_no_spa():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter()
    assert adapter._spa_mode is False
    assert adapter._extra_wait_ms == 0
    assert adapter._wait_until == "networkidle"


def test_adapter_all_valid_strategies_accepted():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter, _WAIT_STRATEGIES
    for strategy in _WAIT_STRATEGIES:
        adapter = CrawleeAdapter(wait_until=strategy)
        assert adapter._wait_until == strategy


# ---------------------------------------------------------------------------
# _fetch routes SPA params to playwright
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_passes_spa_params_to_playwright():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter

    adapter = CrawleeAdapter(
        use_js_rendering=True,
        wait_until="domcontentloaded",
        spa_mode=True,
        extra_wait_ms=100,
        use_sitemap=False,
        respect_robots=False,
    )
    adapter._use_js = True  # force JS mode without Playwright check

    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url_playwright",
        new_callable=AsyncMock,
    ) as mock_fetch:
        mock_fetch.return_value = ("<html>ok</html>", 200)
        await adapter._fetch("https://spa.example.com")

    mock_fetch.assert_called_once_with(
        "https://spa.example.com",
        wait_until="domcontentloaded",
        spa_mode=True,
        extra_wait_ms=100,
    )


@pytest.mark.asyncio
async def test_fetch_http_mode_ignores_spa_params():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter

    adapter = CrawleeAdapter(
        use_js_rendering=False,
        spa_mode=True,
        extra_wait_ms=500,
        use_sitemap=False,
        respect_robots=False,
    )

    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url",
        return_value=("<html>ok</html>", 200),
    ) as mock_http:
        await adapter._fetch("https://example.com")

    mock_http.assert_called_once()
    # spa_mode and extra_wait_ms must NOT be in the call
    call_kwargs = mock_http.call_args[1]
    assert "spa_mode" not in call_kwargs
    assert "extra_wait_ms" not in call_kwargs
