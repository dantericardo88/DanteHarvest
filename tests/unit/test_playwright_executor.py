"""Unit tests for playwright_executor — all mocked (Playwright not required in CI)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from harvest_index.registry.playwright_executor import (
    PlaywrightReplaySession,
    _dispatch_action,
    playwright_step_executor,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_page(url="https://example.com", content="<html><body>Hello World</body></html>"):
    page = AsyncMock()
    page.url = url
    page.content = AsyncMock(return_value=content)
    page.goto = AsyncMock(return_value=MagicMock(status=200))
    page.click = AsyncMock()
    page.fill = AsyncMock()
    page.press = AsyncMock()
    page.wait_for_selector = AsyncMock()
    page.screenshot = AsyncMock()
    page.evaluate = AsyncMock(return_value=42)
    return page


# ---------------------------------------------------------------------------
# playwright_step_executor — no page in context
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_page_returns_failed():
    result = await playwright_step_executor("navigate:https://example.com", "s1", context={})
    assert result["passed"] is False
    assert "page" in result["error"].lower() or "playwright" in result["error"].lower()


@pytest.mark.asyncio
async def test_playwright_not_installed_returns_failed():
    with patch.dict("sys.modules", {"playwright": None, "playwright.async_api": None}):
        result = await playwright_step_executor("navigate:https://example.com", "s1", context={})
        assert result["passed"] is False


# ---------------------------------------------------------------------------
# _dispatch_action — navigate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_navigate_success():
    page = _mock_page()
    result = await _dispatch_action(page, "navigate:https://example.com")
    assert result["passed"] is True
    page.goto.assert_called_once_with("https://example.com", wait_until="domcontentloaded", timeout=15000)


@pytest.mark.asyncio
async def test_navigate_failure():
    page = _mock_page()
    page.goto = AsyncMock(side_effect=Exception("net::ERR_NAME_NOT_RESOLVED"))
    result = await _dispatch_action(page, "navigate:https://notasite.invalid")
    assert result["passed"] is False
    assert "ERR_NAME_NOT_RESOLVED" in result["error"]


# ---------------------------------------------------------------------------
# _dispatch_action — click
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_click_success():
    page = _mock_page()
    result = await _dispatch_action(page, "click:#submit-btn")
    assert result["passed"] is True
    page.click.assert_called_once_with("#submit-btn", timeout=10000)


@pytest.mark.asyncio
async def test_click_failure():
    page = _mock_page()
    page.click = AsyncMock(side_effect=Exception("Timeout waiting for selector"))
    result = await _dispatch_action(page, "click:#missing")
    assert result["passed"] is False


# ---------------------------------------------------------------------------
# _dispatch_action — fill
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fill_success():
    page = _mock_page()
    result = await _dispatch_action(page, "fill:#email:user@example.com")
    assert result["passed"] is True
    page.fill.assert_called_once_with("#email", "user@example.com", timeout=10000)


@pytest.mark.asyncio
async def test_fill_malformed():
    page = _mock_page()
    result = await _dispatch_action(page, "fill:#email")
    assert result["passed"] is False
    assert "malformed" in result["error"]


# ---------------------------------------------------------------------------
# _dispatch_action — press
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_press_success():
    page = _mock_page()
    result = await _dispatch_action(page, "press:#search:Enter")
    assert result["passed"] is True
    page.press.assert_called_once_with("#search", "Enter", timeout=10000)


# ---------------------------------------------------------------------------
# _dispatch_action — wait
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wait_success():
    page = _mock_page()
    result = await _dispatch_action(page, "wait:.results-container")
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_wait_timeout():
    page = _mock_page()
    page.wait_for_selector = AsyncMock(side_effect=Exception("Timeout"))
    result = await _dispatch_action(page, "wait:.never-appears")
    assert result["passed"] is False


# ---------------------------------------------------------------------------
# _dispatch_action — expect_text
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expect_text_found():
    page = _mock_page(content="<html><body>Hello World</body></html>")
    result = await _dispatch_action(page, "expect_text:Hello World")
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_expect_text_not_found():
    page = _mock_page(content="<html><body>Goodbye</body></html>")
    result = await _dispatch_action(page, "expect_text:Hello World")
    assert result["passed"] is False
    assert "Hello World" in result["error"]


# ---------------------------------------------------------------------------
# _dispatch_action — expect_url
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_expect_url_match():
    page = _mock_page(url="https://app.example.com/dashboard")
    result = await _dispatch_action(page, "expect_url:dashboard")
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_expect_url_no_match():
    page = _mock_page(url="https://app.example.com/login")
    result = await _dispatch_action(page, "expect_url:dashboard")
    assert result["passed"] is False


# ---------------------------------------------------------------------------
# _dispatch_action — screenshot
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_screenshot_success():
    page = _mock_page()
    result = await _dispatch_action(page, "screenshot:/tmp/step.png")
    assert result["passed"] is True
    assert result["output"]["path"] == "/tmp/step.png"


# ---------------------------------------------------------------------------
# _dispatch_action — eval
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_eval_success():
    page = _mock_page()
    page.evaluate = AsyncMock(return_value={"count": 5})
    result = await _dispatch_action(page, "eval:document.querySelectorAll('li').length")
    assert result["passed"] is True
    assert result["output"] == {"count": 5}


# ---------------------------------------------------------------------------
# _dispatch_action — unknown action
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_action_fails_closed():
    page = _mock_page()
    result = await _dispatch_action(page, "hover:#some-element")
    assert result["passed"] is False
    assert "Unrecognized" in result["error"]


# ---------------------------------------------------------------------------
# playwright_step_executor — with page in context
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_executor_with_page_in_context():
    page = _mock_page()
    result = await playwright_step_executor(
        "navigate:https://example.com", "s1", context={"page": page}
    )
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_executor_page_kwarg_takes_precedence():
    page1 = _mock_page(url="https://one.com")
    page2 = _mock_page(url="https://two.com")
    page2.goto = AsyncMock(return_value=MagicMock(status=200))
    result = await playwright_step_executor(
        "navigate:https://two.com", "s1", context={"page": page1}, page=page2
    )
    assert result["passed"] is True
    page2.goto.assert_called_once()


# ---------------------------------------------------------------------------
# PlaywrightReplaySession — import error path
# ---------------------------------------------------------------------------

def test_session_repr_has_headless():
    session = PlaywrightReplaySession(headless=True)
    assert session._headless is True
    assert session.context == {}
