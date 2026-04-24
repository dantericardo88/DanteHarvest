"""
PlaywrightPool — reusable Playwright browser pool.

Harvested from: Crawlee (Apify) BrowserPool pattern (Apache-2.0).
Translated from TypeScript to Python; refit for Harvest's local-first model.

Three-state browser lifecycle:
  starting → active → retired

A browser is retired after `retire_after_pages` page closes, or after
`idle_close_secs` of inactivity. Retired browsers are closed async.

Constitutional guarantees:
- Local-first: no remote browser cloud required
- Fail-closed: pool exhausted → AcquisitionError, not silent block
- Zero-ambiguity: acquire_page() always returns a live Page or raises
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

from harvest_core.control.exceptions import AcquisitionError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifecycle hook type
# ---------------------------------------------------------------------------

Hook = Callable[..., Any]  # async def hook(**kwargs) -> None


@dataclass
class _BrowserSlot:
    browser: Any                    # playwright Browser
    pages_served: int = 0
    last_active: float = field(default_factory=time.time)
    state: str = "active"           # active | retired
    error_count: int = 0
    launched_at: float = field(default_factory=time.time)

    def uptime_secs(self) -> float:
        return time.time() - self.launched_at

    def idle_secs(self) -> float:
        return time.time() - self.last_active


# ---------------------------------------------------------------------------
# Device fingerprint profiles (UA + viewport + timezone)
# ---------------------------------------------------------------------------

DEVICE_PROFILES: Dict[str, Dict[str, Any]] = {
    "chrome_windows": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1920, "height": 1080},
        "timezone_id": "America/New_York",
    },
    "chrome_mac": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1440, "height": 900},
        "timezone_id": "America/Los_Angeles",
    },
    "firefox_windows": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "viewport": {"width": 1366, "height": 768},
        "timezone_id": "Europe/London",
    },
    "chrome_linux": {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1280, "height": 800},
        "timezone_id": "Europe/Berlin",
    },
    "mobile_safari": {
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 390, "height": 844},
        "timezone_id": "America/Chicago",
        "is_mobile": True,
    },
}


class PlaywrightPool:
    """
    Pool of reusable Playwright browser instances.

    Usage:
        async with PlaywrightPool(max_browsers=3) as pool:
            async with pool.acquire_page() as page:
                await page.goto("https://example.com")
                html = await page.content()

    Lifecycle hooks (all optional, called with keyword args):
        pre_launch(pool)               — before browser launch
        post_launch(pool, browser)     — after browser launch
        pre_page_create(pool, browser) — before page creation
        post_page_create(pool, page)   — after page creation
        pre_page_close(pool, page)     — before page close
        post_page_close(pool)          — after page close
    """

    def __init__(
        self,
        max_browsers: int = 3,
        max_pages_per_browser: int = 10,
        retire_after_pages: int = 50,
        idle_close_secs: int = 300,
        headless: bool = True,
        proxy_url: Optional[str] = None,
        fingerprint_profile: Optional[str] = None,  # key from DEVICE_PROFILES, or None for random
        pre_launch: Optional[Hook] = None,
        post_launch: Optional[Hook] = None,
        pre_page_create: Optional[Hook] = None,
        post_page_create: Optional[Hook] = None,
        pre_page_close: Optional[Hook] = None,
        post_page_close: Optional[Hook] = None,
    ):
        self.max_browsers = max_browsers
        self.max_pages_per_browser = max_pages_per_browser
        self.retire_after_pages = retire_after_pages
        self.idle_close_secs = idle_close_secs
        self.headless = headless
        self.proxy_url = proxy_url
        self.fingerprint_profile = fingerprint_profile  # None = rotate randomly per browser

        self._hooks: Dict[str, Optional[Hook]] = {
            "pre_launch": pre_launch,
            "post_launch": post_launch,
            "pre_page_create": pre_page_create,
            "post_page_create": post_page_create,
            "pre_page_close": pre_page_close,
            "post_page_close": post_page_close,
        }

        self._playwright: Any = None
        self._slots: List[_BrowserSlot] = []
        self._lock = asyncio.Lock()
        self._open = False

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "PlaywrightPool":
        await self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def open(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise AcquisitionError("playwright not installed: pip install playwright") from e
        self._playwright = await async_playwright().start()
        self._open = True

    async def close(self) -> None:
        self._open = False
        for slot in self._slots:
            try:
                await slot.browser.close()
            except Exception:
                pass
        self._slots.clear()
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Page acquisition
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire_page(self) -> AsyncIterator[Any]:
        """Yield a live Playwright Page. Closes the page on exit."""
        page = await self._get_page()
        try:
            yield page
        finally:
            await self._release_page(page)

    async def _get_page(self) -> Any:
        async with self._lock:
            slot = await self._find_or_launch_slot()
            await self._call_hook("pre_page_create", pool=self, browser=slot.browser)
            profile = getattr(slot, "_profile", {})
            context_opts: Dict[str, Any] = {}
            if profile.get("user_agent"):
                context_opts["user_agent"] = profile["user_agent"]
            if profile.get("viewport"):
                context_opts["viewport"] = profile["viewport"]
            if profile.get("timezone_id"):
                context_opts["timezone_id"] = profile["timezone_id"]
            if profile.get("is_mobile"):
                context_opts["is_mobile"] = True
            if context_opts:
                context = await slot.browser.new_context(**context_opts)
                page = await context.new_page()
            else:
                page = await slot.browser.new_page()
            slot.pages_served += 1
            slot.last_active = time.time()
            await self._call_hook("post_page_create", pool=self, page=page)
            return page

    async def _release_page(self, page: Any) -> None:
        await self._call_hook("pre_page_close", pool=self, page=page)
        try:
            await page.close()
        except Exception:
            pass
        await self._call_hook("post_page_close", pool=self)
        await self._retire_stale_slots()

    async def _find_or_launch_slot(self) -> _BrowserSlot:
        # Prefer an active slot with headroom
        for slot in self._slots:
            if slot.state == "active" and slot.pages_served < self.retire_after_pages:
                return slot
        # Launch a new browser if pool not full
        active = [s for s in self._slots if s.state == "active"]
        if len(active) < self.max_browsers:
            return await self._launch_slot()
        raise AcquisitionError(
            f"PlaywrightPool exhausted: all {self.max_browsers} browsers at capacity"
        )

    def _pick_profile(self) -> Dict[str, Any]:
        """Return a device fingerprint profile dict (UA, viewport, timezone).

        Returns empty dict when fingerprint_profile is None (no fingerprinting —
        preserves original new_page() behaviour for tests and minimal deployments).
        Use fingerprint_profile="random" to rotate across all profiles.
        """
        import random
        if self.fingerprint_profile is None:
            return {}
        if self.fingerprint_profile == "random":
            return random.choice(list(DEVICE_PROFILES.values()))
        return DEVICE_PROFILES.get(self.fingerprint_profile, {})

    async def _launch_slot(self) -> _BrowserSlot:
        await self._call_hook("pre_launch", pool=self)
        launch_opts: Dict[str, Any] = {"headless": self.headless}
        if self.proxy_url:
            launch_opts["proxy"] = {"server": self.proxy_url}
        browser = await self._playwright.chromium.launch(**launch_opts)
        profile = self._pick_profile()
        slot = _BrowserSlot(browser=browser)
        slot._profile = profile  # stored for context creation in _get_page
        self._slots.append(slot)
        await self._call_hook("post_launch", pool=self, browser=browser)
        logger.debug("PlaywrightPool: launched browser (total=%d)", len(self._slots))
        return slot

    async def _retire_stale_slots(self) -> None:
        now = time.time()
        for slot in list(self._slots):
            if slot.state != "active":
                continue
            if slot.pages_served >= self.retire_after_pages:
                slot.state = "retired"
            elif now - slot.last_active > self.idle_close_secs:
                slot.state = "retired"
        for slot in [s for s in self._slots if s.state == "retired"]:
            try:
                await slot.browser.close()
            except Exception:
                pass
            self._slots.remove(slot)
            logger.debug("PlaywrightPool: retired browser (remaining=%d)", len(self._slots))

    async def _call_hook(self, name: str, **kwargs: Any) -> None:
        hook = self._hooks.get(name)
        if hook is None:
            return
        try:
            result = hook(**kwargs)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.warning("PlaywrightPool hook '%s' raised: %s", name, e)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        active = [s for s in self._slots if s.state == "active"]
        return {
            "open": self._open,
            "active_browsers": len(active),
            "total_slots": len(self._slots),
            "max_browsers": self.max_browsers,
            "proxy_url": self.proxy_url,
            "fingerprint_profile": self.fingerprint_profile,
        }

    def health(self) -> Dict[str, Any]:
        """Per-browser health metrics for monitoring integrations."""
        browsers = []
        for i, slot in enumerate(self._slots):
            profile = getattr(slot, "_profile", {})
            browsers.append({
                "index": i,
                "state": slot.state,
                "pages_served": slot.pages_served,
                "error_count": slot.error_count,
                "uptime_secs": slot.uptime_secs(),
                "idle_secs": slot.idle_secs(),
                "user_agent": profile.get("user_agent", "default")[:60],
            })
        return {
            "open": self._open,
            "browser_count": len(self._slots),
            "active_count": sum(1 for s in self._slots if s.state == "active"),
            "total_errors": sum(s.error_count for s in self._slots),
            "browsers": browsers,
        }
