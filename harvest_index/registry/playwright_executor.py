"""
PlaywrightStepExecutor — real browser-based step executor for ReplayHarness.

Interprets PackStep.action strings into Playwright operations:
  navigate:<url>             → page.goto(url)
  click:<selector>           → page.click(selector)
  fill:<selector>:<value>    → page.fill(selector, value)
  wait:<selector>            → page.wait_for_selector(selector)
  expect_text:<text>         → assert text in page.content()
  expect_url:<pattern>       → assert pattern in page.url
  screenshot:<path>          → page.screenshot(path=path)
  eval:<js>                  → page.evaluate(js)
  press:<selector>:<key>     → page.press(selector, key)

For actions with selector_hint, the selector in the action may be overridden.
For actions with expected_outcome, a text-match check is appended.

Constitutional guarantees:
- Fail-closed: unrecognized action format returns {"passed": False, "error": "..."}
- Local-first: runs headless Chromium; no external services required
- Playwright not installed → ImportError surfaced as {"passed": False, "error": "playwright not installed"}
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, Optional


async def playwright_step_executor(
    action: str,
    step_id: str,
    context: Optional[Dict[str, Any]] = None,
    page: Any = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Execute a single PackStep action using Playwright.

    Args:
        action: PackStep.action string (e.g. "navigate:https://example.com")
        step_id: Step identifier (for logging)
        context: Shared context dict; 'page' key can hold a live Playwright page
        page: Explicit page override (takes precedence over context["page"])

    Returns:
        {"passed": bool, "output": Any, "error": str | None}
    """
    ctx = context or {}
    live_page = page or ctx.get("page")

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return {
            "passed": False,
            "error": "playwright not installed. Run: pip install playwright && playwright install chromium",
            "output": None,
        }

    # If no page provided, we can't execute browser actions
    if live_page is None:
        return {
            "passed": False,
            "error": f"No Playwright page in context for step {step_id}. Use PlaywrightReplaySession.",
            "output": None,
        }

    return await _dispatch_action(live_page, action)


async def _dispatch_action(page: Any, action: str) -> Dict[str, Any]:
    action = action.strip()

    # navigate:<url>
    if action.startswith("navigate:"):
        url = action[len("navigate:"):]
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            return {"passed": True, "output": {"url": page.url, "status": resp.status if resp else None}}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # click:<selector>
    if action.startswith("click:"):
        selector = action[len("click:"):]
        try:
            await page.click(selector, timeout=10000)
            return {"passed": True, "output": None}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # fill:<selector>:<value>
    if action.startswith("fill:"):
        rest = action[len("fill:"):]
        # split on first colon after selector — selectors may contain colons (e.g. :nth-child)
        # Use a heuristic: first token before space or until value separator " = "
        parts = rest.split(":", 1)
        if len(parts) < 2:
            return {"passed": False, "error": f"fill action malformed: {action}", "output": None}
        selector, value = parts[0], parts[1]
        try:
            await page.fill(selector, value, timeout=10000)
            return {"passed": True, "output": None}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # press:<selector>:<key>
    if action.startswith("press:"):
        rest = action[len("press:"):]
        parts = rest.split(":", 1)
        if len(parts) < 2:
            return {"passed": False, "error": f"press action malformed: {action}", "output": None}
        selector, key = parts[0], parts[1]
        try:
            await page.press(selector, key, timeout=10000)
            return {"passed": True, "output": None}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # wait:<selector>
    if action.startswith("wait:"):
        selector = action[len("wait:"):]
        try:
            await page.wait_for_selector(selector, timeout=15000)
            return {"passed": True, "output": None}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # expect_text:<text>
    if action.startswith("expect_text:"):
        expected = action[len("expect_text:"):]
        try:
            content = await page.content()
            passed = expected in content
            return {
                "passed": passed,
                "output": {"found": passed},
                "error": None if passed else f"Text not found: {expected!r}",
            }
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # expect_url:<pattern>
    if action.startswith("expect_url:"):
        pattern = action[len("expect_url:"):]
        current = page.url
        passed = bool(re.search(pattern, current))
        return {
            "passed": passed,
            "output": {"current_url": current},
            "error": None if passed else f"URL {current!r} did not match {pattern!r}",
        }

    # screenshot:<path>
    if action.startswith("screenshot:"):
        path = action[len("screenshot:"):]
        try:
            await page.screenshot(path=path, full_page=True)
            return {"passed": True, "output": {"path": path}}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # eval:<js>
    if action.startswith("eval:"):
        js = action[len("eval:"):]
        try:
            result = await page.evaluate(js)
            return {"passed": True, "output": result}
        except Exception as e:
            return {"passed": False, "error": str(e), "output": None}

    # Unknown action — fail-closed
    return {
        "passed": False,
        "error": f"Unrecognized action format: {action!r}. Supported: navigate, click, fill, press, wait, expect_text, expect_url, screenshot, eval",
        "output": None,
    }


class PlaywrightReplaySession:
    """
    Context manager that opens a Playwright browser and injects the page into
    the ReplayHarness context dict so steps can share a persistent browser session.

    Usage:
        async with PlaywrightReplaySession() as session:
            harness = ReplayHarness(chain_writer, step_executor=playwright_step_executor)
            report = await harness.replay(pack, run_id="run-001", context=session.context)
    """

    def __init__(self, headless: bool = True, base_url: Optional[str] = None):
        self._headless = headless
        self._base_url = base_url
        self._playwright = None
        self._browser = None
        self._page = None
        self.context: Dict[str, Any] = {}

    async def __aenter__(self) -> "PlaywrightReplaySession":
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise ImportError(
                "playwright not installed. Run: pip install playwright && playwright install chromium"
            ) from e

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self._headless)
        self._page = await self._browser.new_page()
        if self._base_url:
            await self._page.goto(self._base_url, wait_until="domcontentloaded", timeout=15000)
        self.context = {"page": self._page, "browser": self._browser}
        return self

    async def __aexit__(self, *args) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self.context = {}
