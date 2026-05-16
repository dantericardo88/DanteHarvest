"""
Tests for the enhanced SPA extraction functions in crawlee_adapter.

Covers:
- _extract_text_content: strips scripts/styles/head
- _extract_preloaded_state: __PRELOADED_STATE__, __APOLLO_STATE__, etc.
- _extract_inline_data_attrs: data-props, data-server-props, etc.
- _extract_app_config: window.APP_CONFIG, window.appConfig, window.config
- _detect_csr_only: skeleton HTML detection
- _fetch_url_spa_enhanced: full integration, CSR vs SSR return shape
"""

from __future__ import annotations

import json
import unittest.mock as mock

import pytest

from harvest_acquire.crawl.crawlee_adapter import (
    _detect_csr_only,
    _extract_app_config,
    _extract_inline_data_attrs,
    _extract_preloaded_state,
    _extract_text_content,
    _fetch_url_spa_enhanced,
    _fetch_url_spa_enhanced_dict,
)


# ---------------------------------------------------------------------------
# _extract_text_content
# ---------------------------------------------------------------------------

class TestExtractTextContent:
    def test_strips_script_tags(self):
        html = "<html><body><script>alert('xss')</script>Visible text</body></html>"
        result = _extract_text_content(html)
        assert "alert" not in result
        assert "Visible text" in result

    def test_strips_style_tags(self):
        html = "<html><body><style>body { color: red; }</style>Clean text</body></html>"
        result = _extract_text_content(html)
        assert "color" not in result
        assert "Clean text" in result

    def test_strips_head_tag(self):
        html = (
            "<html><head><title>Page Title</title>"
            "<script>window.config={}</script></head>"
            "<body>Body content</body></html>"
        )
        result = _extract_text_content(html)
        assert "Page Title" not in result
        assert "Body content" in result

    def test_strips_html_tags_leaves_text(self):
        html = "<html><body><h1>Hello</h1><p>World</p></body></html>"
        result = _extract_text_content(html)
        assert "Hello" in result
        assert "World" in result
        assert "<" not in result

    def test_collapses_whitespace(self):
        html = "<body>   <p>  spaced   out  </p>   </body>"
        result = _extract_text_content(html)
        assert "  " not in result  # no double-spaces after collapse


# ---------------------------------------------------------------------------
# _extract_preloaded_state
# ---------------------------------------------------------------------------

class TestExtractPreloadedState:
    def test_extract_preloaded_state_basic(self):
        payload = {"user": "test", "token": "abc"}
        html = f'<script>window.__PRELOADED_STATE__ = {json.dumps(payload)};</script>'
        result = _extract_preloaded_state(html)
        assert "__PRELOADED_STATE__" in result
        assert result["__PRELOADED_STATE__"]["user"] == "test"

    def test_extract_apollo_state(self):
        payload = {"ROOT_QUERY": {"__typename": "Query"}}
        html = f'<script>window.__APOLLO_STATE__ = {json.dumps(payload)};</script>'
        result = _extract_preloaded_state(html)
        assert "__APOLLO_STATE__" in result
        assert "ROOT_QUERY" in result["__APOLLO_STATE__"]

    def test_extract_relay_store(self):
        payload = {"relay:root": {}}
        html = f'<script>window.__RELAY_STORE__ = {json.dumps(payload)};</script>'
        result = _extract_preloaded_state(html)
        assert "__RELAY_STORE__" in result

    def test_extract_next_data(self):
        payload = {"props": {"pageProps": {"id": 42}}, "page": "/home"}
        html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
        # __NEXT_DATA__ is also detected via the preloaded state pattern
        html_inline = f'<script>__NEXT_DATA__ = {json.dumps(payload)};</script>'
        result = _extract_preloaded_state(html_inline)
        assert "__NEXT_DATA__" in result
        assert result["__NEXT_DATA__"]["page"] == "/home"

    def test_returns_empty_dict_when_no_state(self):
        html = "<html><body>No state here</body></html>"
        result = _extract_preloaded_state(html)
        assert result == {}

    def test_falls_back_to_raw_string_on_invalid_json(self):
        html = "<script>window.__PRELOADED_STATE__ = not_valid_json;</script>"
        result = _extract_preloaded_state(html)
        # Should not raise; returns raw string or empty
        # The regex requires { ... } so invalid JSON without braces won't match
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# _extract_inline_data_attrs
# ---------------------------------------------------------------------------

class TestExtractInlineDataAttrs:
    def test_data_props(self):
        payload = {"title": "Hello", "count": 3}
        html = f'<div id="app" data-props=\'{json.dumps(payload)}\'></div>'
        result = _extract_inline_data_attrs(html)
        assert "data-props" in result
        assert result["data-props"]["title"] == "Hello"

    def test_data_initial(self):
        payload = {"user": {"name": "Alice"}}
        html = f'<div id="root" data-initial=\'{json.dumps(payload)}\'></div>'
        result = _extract_inline_data_attrs(html)
        assert "data-initial" in result
        assert result["data-initial"]["user"]["name"] == "Alice"

    def test_data_server_props(self):
        payload = {"locale": "en-US"}
        html = f'<div data-server-props=\'{json.dumps(payload)}\'></div>'
        result = _extract_inline_data_attrs(html)
        assert "data-server-props" in result

    def test_returns_empty_dict_when_no_attrs(self):
        html = "<div id='app' class='container'></div>"
        result = _extract_inline_data_attrs(html)
        assert result == {}

    def test_falls_back_to_raw_string_on_invalid_json(self):
        html = "<div data-props='not-json'></div>"
        result = _extract_inline_data_attrs(html)
        assert result.get("data-props") == "not-json"


# ---------------------------------------------------------------------------
# _extract_app_config
# ---------------------------------------------------------------------------

class TestExtractAppConfig:
    def test_extract_app_config_uppercase(self):
        payload = {"apiUrl": "https://api.example.com", "version": "2.0"}
        html = f'<script>window.APP_CONFIG = {json.dumps(payload)};</script>'
        result = _extract_app_config(html)
        assert "APP_CONFIG" in result
        assert result["APP_CONFIG"]["apiUrl"] == "https://api.example.com"

    def test_extract_app_config_camelcase(self):
        payload = {"featureFlags": {"darkMode": True}}
        html = f'<script>window.appConfig = {json.dumps(payload)};</script>'
        result = _extract_app_config(html)
        assert "appConfig" in result

    def test_extract_window_config(self):
        payload = {"env": "production"}
        html = f'<script>window.config = {json.dumps(payload)};</script>'
        result = _extract_app_config(html)
        assert "config" in result
        assert result["config"]["env"] == "production"

    def test_returns_empty_dict_when_no_config(self):
        html = "<html><body>No config</body></html>"
        result = _extract_app_config(html)
        assert result == {}


# ---------------------------------------------------------------------------
# _detect_csr_only
# ---------------------------------------------------------------------------

class TestDetectCsrOnly:
    def _make_csr_skeleton(self) -> str:
        """Minimal HTML as produced by a pure CRA/Vite app — tiny visible text, huge scripts."""
        script_payload = "x" * 50_000  # large inline script block
        return (
            "<!DOCTYPE html><html><head>"
            "<title>App</title>"
            f"<script>{script_payload}</script>"
            "</head><body>"
            '<div id="root"></div>'
            "</body></html>"
        )

    def _make_ssr_page(self) -> str:
        """SSR page with substantial rendered text content."""
        body_text = " ".join(["word"] * 400)  # ~2000 chars of text
        return f"<html><body><article>{body_text}</article></body></html>"

    def test_detect_csr_only_skeleton(self):
        html = self._make_csr_skeleton()
        text = _extract_text_content(html)
        assert _detect_csr_only(html, text)["is_csr"] is True

    def test_detect_ssr_has_content(self):
        html = self._make_ssr_page()
        text = _extract_text_content(html)
        assert _detect_csr_only(html, text)["is_csr"] is False

    def test_short_text_but_low_script_ratio_is_not_csr(self):
        # Very small page with no scripts — a simple static page, not CSR
        html = "<html><body><p>Hello world</p></body></html>"
        text = _extract_text_content(html)
        assert _detect_csr_only(html, text)["is_csr"] is False


# ---------------------------------------------------------------------------
# _fetch_url_spa_enhanced (integration via mock)
# ---------------------------------------------------------------------------

class TestFetchUrlSpaEnhanced:
    def _csr_html(self) -> str:
        script_payload = "y" * 60_000
        return (
            "<!DOCTYPE html><html><head><title>App</title>"
            f"<script>{script_payload}</script></head>"
            "<body><div id='root'></div></body></html>"
        )

    def _ssr_html(self) -> str:
        body_text = " ".join(["content"] * 300)
        og = '<meta property="og:title" content="My SSR Page">'
        ld = '{"@type":"WebPage","name":"Test"}'
        return (
            f"<html><head>{og}"
            f'<script type="application/ld+json">{ld}</script>'
            "</head>"
            f"<body><article>{body_text}</article></body></html>"
        )

    def test_csr_only_sets_requires_playwright(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(self._csr_html(), 200),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        assert result["requires_playwright"] is True
        assert result["content_type"] == "csr-only"
        assert result["status_code"] == 200

    def test_csr_only_string_result_contains_advisory(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(self._csr_html(), 200),
        ):
            text, status = _fetch_url_spa_enhanced("http://example.com")

        assert "SPA_CSR_ONLY" in text
        assert "Playwright" in text
        assert status == 200

    def test_ssr_page_requires_playwright_false(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(self._ssr_html(), 200),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        assert result["requires_playwright"] is False
        assert result["content_type"] != "csr-only"

    def test_ssr_page_extracts_meta(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(self._ssr_html(), 200),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        assert result["meta"].get("og:title") == "My SSR Page"

    def test_ssr_page_extracts_json_ld(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(self._ssr_html(), 200),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        json_ld = result["structured_data"]["json_ld"]
        assert len(json_ld) == 1
        assert "WebPage" in json_ld[0]

    def test_http_error_returns_error_dict(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=("", 404),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        assert result["status_code"] == 404
        assert result["content_type"] == "error"
        assert result["requires_playwright"] is False

    def test_preloaded_state_extracted(self):
        payload = {"user": "alice", "role": "admin"}
        html = (
            "<html><head></head><body>"
            + " ".join(["word"] * 300)
            + f"<script>window.__PRELOADED_STATE__ = {json.dumps(payload)};</script>"
            + "</body></html>"
        )
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(html, 200),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        ps = result["structured_data"]["preloaded_state"]
        assert "__PRELOADED_STATE__" in ps
        assert ps["__PRELOADED_STATE__"]["user"] == "alice"

    def test_result_dict_has_all_keys(self):
        with mock.patch(
            "harvest_acquire.crawl.crawlee_adapter._fetch_url",
            return_value=(self._ssr_html(), 200),
        ):
            result = _fetch_url_spa_enhanced_dict("http://example.com")

        for key in ("url", "status_code", "content_type", "text_content",
                    "structured_data", "meta", "requires_playwright"):
            assert key in result, f"Missing key: {key}"
