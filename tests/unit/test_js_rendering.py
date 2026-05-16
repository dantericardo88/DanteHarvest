"""Tests for javascript_rendering_fidelity improvements in crawlee_adapter."""
import pytest


# ---------------------------------------------------------------------------
# _detect_framework — structured dict output
# ---------------------------------------------------------------------------

def test_detect_framework_returns_dict():
    from harvest_acquire.crawl.crawlee_adapter import _detect_framework
    result = _detect_framework("<html><head></head><body></body></html>")
    assert isinstance(result, dict)
    assert "frameworks" in result
    assert "primary" in result
    assert "is_spa" in result


def test_detect_framework_next_js():
    from harvest_acquire.crawl.crawlee_adapter import _detect_framework
    html = '<script id="__NEXT_DATA__" type="application/json">{}</script><link href="/_next/static/x.js">'
    result = _detect_framework(html)
    assert result["is_spa"] is True
    assert result["primary"] == "next.js"
    assert len(result["frameworks"]) >= 1
    fw = result["frameworks"][0]
    assert "name" in fw
    assert "confidence" in fw
    assert "indicators_found" in fw


def test_detect_framework_react():
    from harvest_acquire.crawl.crawlee_adapter import _detect_framework
    html = '<div data-reactroot id="react-root"></div>'
    result = _detect_framework(html)
    assert result["is_spa"] is True
    assert result["primary"] == "react"


def test_detect_framework_no_spa():
    from harvest_acquire.crawl.crawlee_adapter import _detect_framework
    html = "<html><body><p>Plain server-rendered content.</p></body></html>"
    result = _detect_framework(html)
    assert result["is_spa"] is False
    assert result["primary"] is None
    assert result["frameworks"] == []


def test_detect_framework_ssr_likely_next():
    from harvest_acquire.crawl.crawlee_adapter import _detect_framework
    html = '<script id="__NEXT_DATA__">{"props":{}}</script><link href="/_next/static/a.js">'
    result = _detect_framework(html)
    assert result["ssr_likely"] is True


def test_detect_framework_confidence_bounded():
    from harvest_acquire.crawl.crawlee_adapter import _detect_framework
    html = "<div ng-app ng-version='15' _nghost-abc></div>"
    result = _detect_framework(html)
    for fw in result["frameworks"]:
        assert 0.0 <= fw["confidence"] <= 1.0


# ---------------------------------------------------------------------------
# _detect_csr_only — dict with confidence score
# ---------------------------------------------------------------------------

def test_detect_csr_only_returns_dict():
    from harvest_acquire.crawl.crawlee_adapter import _detect_csr_only
    result = _detect_csr_only("<html></html>", "")
    assert isinstance(result, dict)
    assert "is_csr" in result
    assert "confidence" in result
    assert "signals" in result


def test_detect_csr_only_high_confidence_for_csr_page():
    from harvest_acquire.crawl.crawlee_adapter import _detect_csr_only
    # Short body text + mostly script content
    big_script = "<script>" + "var x=1;" * 500 + "</script>"
    html = f"<html><body>{big_script}</body></html>"
    text = "Hi"
    result = _detect_csr_only(html, text)
    assert result["confidence"] >= 0.5
    assert result["is_csr"] is True


def test_detect_csr_only_low_confidence_for_ssr():
    from harvest_acquire.crawl.crawlee_adapter import _detect_csr_only
    html = "<html><body>" + "<p>word </p>" * 200 + "</body></html>"
    text = " ".join(["word"] * 300)
    result = _detect_csr_only(html, text)
    # Long pre-rendered text should produce low CSR confidence
    assert result["confidence"] < 0.5
    assert result["is_csr"] is False


def test_detect_csr_only_signals_is_list():
    from harvest_acquire.crawl.crawlee_adapter import _detect_csr_only
    result = _detect_csr_only("<html></html>", "x" * 2000)
    assert isinstance(result["signals"], list)


def test_detect_csr_only_confidence_bounded():
    from harvest_acquire.crawl.crawlee_adapter import _detect_csr_only
    result = _detect_csr_only("", "")
    assert 0.0 <= result["confidence"] <= 1.0


# ---------------------------------------------------------------------------
# get_rendering_summary — combined analysis
# ---------------------------------------------------------------------------

def test_get_rendering_summary_returns_required_keys():
    from harvest_acquire.crawl.crawlee_adapter import get_rendering_summary
    html = "<html><body><p>Hello world</p></body></html>"
    summary = get_rendering_summary(html)
    assert "framework" in summary
    assert "csr_detection" in summary
    assert "preloaded_state_keys" in summary
    assert "inline_data_attrs" in summary
    assert "app_config_keys" in summary


def test_get_rendering_summary_preloaded_state_keys():
    from harvest_acquire.crawl.crawlee_adapter import get_rendering_summary
    html = '<script>window.__PRELOADED_STATE__ = {"a":1};</script>'
    summary = get_rendering_summary(html)
    assert "__PRELOADED_STATE__" in summary["preloaded_state_keys"]


def test_get_rendering_summary_inline_data_attrs_count():
    from harvest_acquire.crawl.crawlee_adapter import get_rendering_summary
    html = '<div data-props=\'{"x":1}\'></div><div data-page=\'{"y":2}\'></div>'
    summary = get_rendering_summary(html)
    assert isinstance(summary["inline_data_attrs"], int)
    assert summary["inline_data_attrs"] >= 1


def test_get_rendering_summary_framework_is_dict():
    from harvest_acquire.crawl.crawlee_adapter import get_rendering_summary
    summary = get_rendering_summary("<html></html>")
    assert isinstance(summary["framework"], dict)
    assert "is_spa" in summary["framework"]


def test_get_rendering_summary_csr_detection_is_dict():
    from harvest_acquire.crawl.crawlee_adapter import get_rendering_summary
    summary = get_rendering_summary("<html></html>")
    assert isinstance(summary["csr_detection"], dict)
    assert "confidence" in summary["csr_detection"]


# ---------------------------------------------------------------------------
# MUTATION_OBSERVER_JS
# ---------------------------------------------------------------------------

def test_mutation_observer_js_is_non_empty_string():
    from harvest_acquire.crawl.crawlee_adapter import MUTATION_OBSERVER_JS
    assert isinstance(MUTATION_OBSERVER_JS, str)
    assert len(MUTATION_OBSERVER_JS.strip()) > 0


def test_mutation_observer_js_contains_key_identifiers():
    from harvest_acquire.crawl.crawlee_adapter import MUTATION_OBSERVER_JS
    assert "__harvestMutationCount" in MUTATION_OBSERVER_JS
    assert "MutationObserver" in MUTATION_OBSERVER_JS
    assert "document.body" in MUTATION_OBSERVER_JS


# ---------------------------------------------------------------------------
# _inject_mutation_observer / _get_mutation_count stubs
# ---------------------------------------------------------------------------

def test_inject_mutation_observer_noop_without_evaluate():
    from harvest_acquire.crawl.crawlee_adapter import _inject_mutation_observer

    class NoEvalPage:
        pass

    # Should not raise
    _inject_mutation_observer(NoEvalPage())


def test_get_mutation_count_returns_zero_without_evaluate():
    from harvest_acquire.crawl.crawlee_adapter import _get_mutation_count

    class NoEvalPage:
        pass

    assert _get_mutation_count(NoEvalPage()) == 0


def test_get_mutation_count_calls_evaluate():
    from harvest_acquire.crawl.crawlee_adapter import _get_mutation_count

    class FakePage:
        def evaluate(self, script):
            return 42

    assert _get_mutation_count(FakePage()) == 42
