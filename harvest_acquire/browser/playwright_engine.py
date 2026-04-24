"""
PlaywrightEngine — headless browser automation for DANTEHARVEST.

Transplanted from DanteDistillerV2/backend/machines/harvester/playwright_engine.py.
Import paths updated. robots.txt check wired to Harvest's robots_validator.

Constitutional guarantees:
- robots.txt FIRST on every fetch (fail-closed)
- Always close pages/contexts on error
- No anti-bot circumvention: stealth mode only suppresses automation flags
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from harvest_acquire.browser.robots_validator import check_robots_txt
from harvest_core.control.exceptions import AcquisitionError, ConstitutionalError

logger = logging.getLogger(__name__)


class PlaywrightEngine:
    """
    Playwright-based web page fetcher for Harvest acquisition.

    Usage:
        engine = await create_playwright_engine()
        result = await engine.fetch_page("https://example.com")
        await engine.close()
    """

    def __init__(
        self,
        browser_type: str = "chromium",
        headless: bool = True,
        timeout: float = 30_000,
        user_agent: Optional[str] = None,
    ):
        self.browser_type = browser_type
        self.headless = headless
        self.timeout = timeout
        self.user_agent = user_agent
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._pages_fetched = 0
        self._errors = 0

    async def initialize(self, stealth_mode: bool = False) -> None:
        try:
            self._playwright = await async_playwright().start()
            launcher = getattr(self._playwright, self.browser_type)
            args = []
            if stealth_mode and self.browser_type == "chromium":
                args = [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ]
            self._browser = await launcher.launch(headless=self.headless, args=args)
            ctx_opts: Dict[str, Any] = {
                "viewport": {"width": 1920, "height": 1080},
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }
            if self.user_agent:
                ctx_opts["user_agent"] = self.user_agent
            self._context = await self._browser.new_context(**ctx_opts)
            self._context.set_default_timeout(self.timeout)
            logger.info("PlaywrightEngine initialized: %s headless=%s", self.browser_type, self.headless)
        except Exception as e:
            await self.close()
            raise AcquisitionError(f"PlaywrightEngine init failed: {e}") from e

    async def close(self) -> None:
        for obj in (self._context, self._browser, self._playwright):
            if obj:
                try:
                    await obj.close()
                except Exception:
                    pass
        self._context = self._browser = self._playwright = None

    async def fetch_page(
        self,
        url: str,
        wait_for: str = "networkidle",
        wait_selector: Optional[str] = None,
        screenshot: bool = False,
        screenshot_path: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """Fetch a page. Checks robots.txt before proceeding (constitutional)."""
        await check_robots_txt(url)
        if not self._context:
            raise AcquisitionError("PlaywrightEngine not initialized")

        page: Optional[Page] = None
        try:
            page = await self._context.new_page()
            response = await page.goto(url, wait_until=wait_for, timeout=self.timeout)
            if wait_selector:
                try:
                    await page.wait_for_selector(wait_selector, timeout=self.timeout)
                except PlaywrightTimeoutError:
                    logger.warning("Selector '%s' not found on %s", wait_selector, url)

            html = await page.content()
            title = await page.title()
            screenshot_uri = None
            if screenshot:
                if not screenshot_path:
                    screenshot_path = Path(f"screenshot_{datetime.utcnow().timestamp()}.png")
                await page.screenshot(path=str(screenshot_path), full_page=True)
                screenshot_uri = str(screenshot_path)

            self._pages_fetched += 1
            return {
                "url": url,
                "html": html,
                "title": title,
                "status_code": response.status if response else None,
                "screenshot": screenshot_uri,
                "timestamp": datetime.utcnow().isoformat(),
                "success": True,
            }

        except ConstitutionalError:
            raise
        except PlaywrightTimeoutError as e:
            self._errors += 1
            return {"url": url, "html": None, "error": f"Timeout: {e}", "success": False}
        except Exception as e:
            self._errors += 1
            return {"url": url, "html": None, "error": str(e), "success": False}
        finally:
            if page:
                await page.close()

    async def fetch_with_interaction(
        self,
        url: str,
        interactions: List[Dict[str, Any]],
        wait_for: str = "networkidle",
    ) -> Dict[str, Any]:
        await check_robots_txt(url)
        if not self._context:
            raise AcquisitionError("PlaywrightEngine not initialized")

        page: Optional[Page] = None
        try:
            page = await self._context.new_page()
            await page.goto(url, wait_until=wait_for, timeout=self.timeout)
            for interaction in interactions:
                kind = interaction.get("type")
                if kind == "click":
                    await page.click(interaction["selector"])
                    await page.wait_for_load_state(wait_for)
                elif kind == "fill":
                    await page.fill(interaction["selector"], interaction["value"])
                elif kind == "wait":
                    await asyncio.sleep(interaction.get("milliseconds", 1000) / 1000.0)
                elif kind == "scroll":
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)
                else:
                    logger.warning("Unknown interaction type: %s", kind)
            html = await page.content()
            self._pages_fetched += 1
            return {"url": url, "html": html, "title": await page.title(), "success": True}
        except ConstitutionalError:
            raise
        except Exception as e:
            self._errors += 1
            return {"url": url, "html": None, "error": str(e), "success": False}
        finally:
            if page:
                await page.close()

    def get_stats(self) -> Dict[str, Any]:
        total = self._pages_fetched + self._errors
        return {
            "browser_type": self.browser_type,
            "headless": self.headless,
            "pages_fetched": self._pages_fetched,
            "errors": self._errors,
            "success_rate": self._pages_fetched / total if total > 0 else 0.0,
        }


async def create_playwright_engine(
    browser_type: str = "chromium",
    headless: bool = True,
    stealth: bool = False,
    user_agent: Optional[str] = None,
) -> PlaywrightEngine:
    engine = PlaywrightEngine(browser_type=browser_type, headless=headless, user_agent=user_agent)
    await engine.initialize(stealth_mode=stealth)
    return engine
