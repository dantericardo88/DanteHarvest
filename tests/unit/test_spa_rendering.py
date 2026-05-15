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


# ---------------------------------------------------------------------------
# _fetch_url_spa_enhanced
# ---------------------------------------------------------------------------

def test_spa_enhanced_extracts_json_ld():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url_spa_enhanced

    html = (
        '<html><head>'
        '<script type="application/ld+json">{"@type":"Product","name":"Widget"}</script>'
        '</head><body><p>Hello</p></body></html>'
    )
    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url",
        return_value=(html, 200),
    ):
        result, status = _fetch_url_spa_enhanced("https://example.com")

    assert status == 200
    assert "JSON-LD:" in result
    assert '"@type":"Product"' in result


def test_spa_enhanced_extracts_next_data():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url_spa_enhanced

    html = (
        '<html><body>'
        '<script>window.__NEXT_DATA__ = {"props":{"title":"Hello Next"}};</script>'
        '</body></html>'
    )
    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url",
        return_value=(html, 200),
    ):
        result, status = _fetch_url_spa_enhanced("https://next.example.com")

    assert status == 200
    assert "__NEXT_DATA__" in result
    assert "Hello Next" in result


def test_spa_enhanced_extracts_application_json_blocks():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url_spa_enhanced

    html = (
        '<html><body>'
        '<script type="application/json">{"items":[1,2,3]}</script>'
        '</body></html>'
    )
    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url",
        return_value=(html, 200),
    ):
        result, status = _fetch_url_spa_enhanced("https://example.com")

    assert "JSON-DATA:" in result
    assert '"items"' in result


def test_spa_enhanced_extracts_meta_tags():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url_spa_enhanced

    html = (
        '<html><head>'
        '<meta property="og:title" content="My SPA Page">'
        '<meta name="description" content="A great page">'
        '</head><body></body></html>'
    )
    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url",
        return_value=(html, 200),
    ):
        result, status = _fetch_url_spa_enhanced("https://example.com")

    assert "META:" in result
    assert "og:title" in result
    assert "My SPA Page" in result


def test_spa_enhanced_passes_through_error_status():
    from harvest_acquire.crawl.crawlee_adapter import _fetch_url_spa_enhanced

    with patch(
        "harvest_acquire.crawl.crawlee_adapter._fetch_url",
        return_value=("", 404),
    ):
        result, status = _fetch_url_spa_enhanced("https://example.com/missing")

    assert status == 404
    assert result == ""


# ---------------------------------------------------------------------------
# auto_spa_detection in CrawleeAdapter
# ---------------------------------------------------------------------------

def test_adapter_stores_auto_spa_detection():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(auto_spa_detection=False)
    assert adapter._auto_spa is False


def test_adapter_auto_spa_detection_default_true():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter()
    assert adapter._auto_spa is True


@pytest.mark.asyncio
async def test_auto_spa_triggers_enhanced_path_when_spa_detected():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter

    spa_html = '<html><body data-reactroot=""><div id="root"></div></body></html>'

    adapter = CrawleeAdapter(
        use_js_rendering=False,
        auto_spa_detection=True,
        use_sitemap=False,
        respect_robots=False,
    )

    enriched = "base text\n\nJSON-LD: {}"

    with patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=(spa_html, 200)), \
         patch("harvest_acquire.crawl.crawlee_adapter._is_playwright_available", return_value=False), \
         patch("harvest_acquire.crawl.crawlee_adapter._fetch_url_spa_enhanced", return_value=(enriched, 200)) as mock_enhanced:
        result, status = await adapter._fetch("https://react.example.com")

    mock_enhanced.assert_called_once()
    assert result == enriched
    assert status == 200


@pytest.mark.asyncio
async def test_auto_spa_skips_enhanced_when_not_spa():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter

    plain_html = "<html><body><p>Plain static content</p></body></html>"

    adapter = CrawleeAdapter(
        use_js_rendering=False,
        auto_spa_detection=True,
        use_sitemap=False,
        respect_robots=False,
    )

    with patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=(plain_html, 200)) as mock_fetch, \
         patch("harvest_acquire.crawl.crawlee_adapter._fetch_url_spa_enhanced") as mock_enhanced:
        result, status = await adapter._fetch("https://static.example.com")

    mock_enhanced.assert_not_called()
    assert result == plain_html


# ---------------------------------------------------------------------------
# rendering_mode property
# ---------------------------------------------------------------------------

def test_rendering_mode_playwright():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter()
    adapter._use_js = True
    assert adapter.rendering_mode == "playwright"


def test_rendering_mode_http_spa():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(auto_spa_detection=True)
    adapter._use_js = False
    assert adapter.rendering_mode == "http_spa"


def test_rendering_mode_http():
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    adapter = CrawleeAdapter(auto_spa_detection=False)
    adapter._use_js = False
    assert adapter.rendering_mode == "http"


# ---------------------------------------------------------------------------
# _auto_detect_spa framework markers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("marker,html_snippet", [
    ("react", '<div data-reactroot="">'),
    ("vue", '<div id="app" v-cloak>'),
    ("angular", '<app-root ng-version="12.0.0">'),
    ("__NEXT_DATA__", '<script id="__NEXT_DATA__" type="application/json">'),
    ("svelte", '<!-- Svelte component -->'),
    ("_nuxt", '<div id="__nuxt">'),
    ("ember", '<div class="ember-view">'),
    ("data-reactroot", '<div data-reactroot="">'),
    ("data-v-", '<div data-v-abc123>'),
])
def test_auto_detect_spa_recognizes_framework_markers(marker, html_snippet):
    from harvest_acquire.crawl.crawlee_adapter import _auto_detect_spa
    html = f"<html><body>{html_snippet}</body></html>"
    assert _auto_detect_spa(html) is True


def test_auto_detect_spa_returns_false_for_plain_html():
    from harvest_acquire.crawl.crawlee_adapter import _auto_detect_spa
    html = "<html><body><h1>Hello World</h1><p>Static content only.</p></body></html>"
    assert _auto_detect_spa(html) is False
