"""Tests for HttpStepExecutor and the real default executor in ReplayHarness."""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from harvest_index.registry.replay_harness import (
    HttpStepExecutor,
    ReplayHarness,
    _noop_executor,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_urlopen(content: bytes = b"hello", status: int = 200):
    """Return a context-manager mock that looks like urllib.request.urlopen."""
    resp = MagicMock()
    resp.read.return_value = content
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
# Fix 2: default executor is not the noop
# ---------------------------------------------------------------------------

def test_default_executor_is_not_noop():
    """ReplayHarness() with no executor must NOT use the bare noop.

    The default is HttpStepExecutor (when Playwright is unavailable) or
    the PlaywrightStepExecutor (when Playwright is installed).  Either way,
    it must not be the trivial _noop_executor function.
    """
    harness = ReplayHarness()
    assert harness.step_executor is not _noop_executor
    # Must be a real executor: either HttpStepExecutor instance or a callable
    # from the playwright_executor module — not the noop stub.
    is_http = isinstance(harness.step_executor, HttpStepExecutor)
    is_playwright = callable(harness.step_executor) and not isinstance(harness.step_executor, HttpStepExecutor)
    assert is_http or is_playwright, (
        f"Expected HttpStepExecutor or playwright executor, got {harness.step_executor!r}"
    )


# ---------------------------------------------------------------------------
# HttpStepExecutor — navigate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_executor_navigate_step():
    """navigate step returns success=True when urlopen succeeds."""
    executor = HttpStepExecutor()
    fake_resp = _fake_urlopen(b"page content", status=200)
    with patch("urllib.request.urlopen", return_value=fake_resp):
        result = await executor(action="navigate https://example.com", step_id="s1")
    assert result["passed"] is True
    assert result["output"]["status_code"] == 200
    assert result["output"]["content_length"] == len(b"page content")
    assert result["output"]["simulated"] is False


@pytest.mark.asyncio
async def test_http_executor_navigate_failure():
    """navigate step returns passed=False when urlopen raises."""
    executor = HttpStepExecutor()
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        result = await executor(action="navigate https://bad.example", step_id="s1")
    assert result["passed"] is False
    assert "connection refused" in result["error"]


# ---------------------------------------------------------------------------
# HttpStepExecutor — click / type (simulated)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_executor_click_simulated():
    """click step returns success=True with simulated=True (no real browser)."""
    executor = HttpStepExecutor()
    result = await executor(action="click #submit-btn", step_id="s2")
    assert result["passed"] is True
    assert result["output"]["simulated"] is True
    assert result["output"]["step_type"] == "click"


@pytest.mark.asyncio
async def test_http_executor_type_simulated():
    """type/fill step is simulated."""
    executor = HttpStepExecutor()
    result = await executor(action="type hello world", step_id="s3")
    assert result["passed"] is True
    assert result["output"]["simulated"] is True


@pytest.mark.asyncio
async def test_http_executor_fill_simulated():
    executor = HttpStepExecutor()
    result = await executor(action="fill #email user@example.com", step_id="s4")
    assert result["passed"] is True
    assert result["output"]["simulated"] is True


# ---------------------------------------------------------------------------
# HttpStepExecutor — wait
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_executor_wait_step():
    """wait step sleeps and returns success without error."""
    executor = HttpStepExecutor()
    with patch("time.sleep") as mock_sleep:
        result = await executor(action="wait", step_id="s5", duration=1.5)
    assert result["passed"] is True
    mock_sleep.assert_called_once_with(1.5)


@pytest.mark.asyncio
async def test_http_executor_wait_default_duration():
    """wait step uses 0.5 s default when no duration provided."""
    executor = HttpStepExecutor()
    with patch("time.sleep") as mock_sleep:
        result = await executor(action="wait", step_id="s6")
    assert result["passed"] is True
    mock_sleep.assert_called_once_with(0.5)


# ---------------------------------------------------------------------------
# HttpStepExecutor — unknown step type
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_executor_unknown_step_simulated():
    executor = HttpStepExecutor()
    result = await executor(action="frobnicate something", step_id="s7")
    assert result["passed"] is True
    assert result["output"]["simulated"] is True
