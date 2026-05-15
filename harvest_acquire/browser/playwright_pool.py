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
    profile: Dict[str, Any] = field(default_factory=dict)  # device fingerprint profile

    def uptime_secs(self) -> float:
        return time.time() - self.launched_at

    def idle_secs(self) -> float:
        return time.time() - self.last_active


# ---------------------------------------------------------------------------
# Device fingerprint profiles (UA + viewport + timezone)
# ---------------------------------------------------------------------------

DEVICE_PROFILES: Dict[str, Dict[str, Any]] = {
    # ── Desktop: Chrome ──────────────────────────────────────────────────
    "chrome_windows_1080p": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1920, "height": 1080},
        "timezone_id": "America/New_York",
    },
    "chrome_windows_1366": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "viewport": {"width": 1366, "height": 768},
        "timezone_id": "America/Chicago",
    },
    "chrome_windows_4k": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 3840, "height": 2160},
        "timezone_id": "America/Los_Angeles",
    },
    "chrome_mac_m1": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1440, "height": 900},
        "timezone_id": "America/Los_Angeles",
    },
    "chrome_mac_1280": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1280, "height": 800},
        "timezone_id": "America/Denver",
    },
    "chrome_linux_1080p": {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1920, "height": 1080},
        "timezone_id": "Europe/Berlin",
    },
    "chrome_linux_1280": {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "viewport": {"width": 1280, "height": 800},
        "timezone_id": "Europe/Paris",
    },
    # ── Desktop: Firefox ─────────────────────────────────────────────────
    "firefox_windows_1080p": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "viewport": {"width": 1920, "height": 1080},
        "timezone_id": "Europe/London",
    },
    "firefox_windows_768": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "viewport": {"width": 1366, "height": 768},
        "timezone_id": "America/Toronto",
    },
    "firefox_mac": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
        "viewport": {"width": 1440, "height": 900},
        "timezone_id": "America/Vancouver",
    },
    "firefox_linux": {
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "viewport": {"width": 1280, "height": 1024},
        "timezone_id": "Europe/Amsterdam",
    },
    # ── Desktop: Safari ──────────────────────────────────────────────────
    "safari_mac_1440": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "viewport": {"width": 1440, "height": 900},
        "timezone_id": "America/Los_Angeles",
    },
    "safari_mac_1280": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        "viewport": {"width": 1280, "height": 800},
        "timezone_id": "America/New_York",
    },
    # ── Mobile: iOS / iPhone ─────────────────────────────────────────────
    "mobile_iphone_15": {
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 390, "height": 844},
        "timezone_id": "America/Chicago",
        "is_mobile": True,
    },
    "mobile_iphone_se": {
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 375, "height": 667},
        "timezone_id": "America/New_York",
        "is_mobile": True,
    },
    "mobile_ipad_pro": {
        "user_agent": "Mozilla/5.0 (iPad; CPU OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 1024, "height": 1366},
        "timezone_id": "America/Chicago",
        "is_mobile": True,
    },
    # ── Mobile: Android ──────────────────────────────────────────────────
    "mobile_android_chrome": {
        "user_agent": "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
        "viewport": {"width": 412, "height": 915},
        "timezone_id": "America/Los_Angeles",
        "is_mobile": True,
    },
    "mobile_android_samsung": {
        "user_agent": "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/24.0 Chrome/117.0.0.0 Mobile Safari/537.36",
        "viewport": {"width": 360, "height": 780},
        "timezone_id": "Europe/Berlin",
        "is_mobile": True,
    },
    "mobile_android_tablet": {
        "user_agent": "Mozilla/5.0 (Linux; Android 13; Pixel Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1280, "height": 800},
        "timezone_id": "America/New_York",
        "is_mobile": False,
    },
    # ── Regional / locale ─────────────────────────────────────────────────
    "chrome_jp": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1366, "height": 768},
        "timezone_id": "Asia/Tokyo",
    },
    "chrome_br": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1366, "height": 768},
        "timezone_id": "America/Sao_Paulo",
    },
    "chrome_in": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1280, "height": 720},
        "timezone_id": "Asia/Kolkata",
    },
}

# Alias backward-compatible names
DEVICE_PROFILES["chrome_windows"] = DEVICE_PROFILES["chrome_windows_1080p"]
DEVICE_PROFILES["chrome_mac"] = DEVICE_PROFILES["chrome_mac_m1"]
DEVICE_PROFILES["firefox_windows"] = DEVICE_PROFILES["firefox_windows_1080p"]
DEVICE_PROFILES["chrome_linux"] = DEVICE_PROFILES["chrome_linux_1080p"]
DEVICE_PROFILES["mobile_safari"] = DEVICE_PROFILES["mobile_iphone_15"]


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
        error_threshold: int = 5,
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
        self.error_threshold = error_threshold

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
            profile = slot.profile
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
            page._harvest_slot = slot
            await self._call_hook("post_page_create", pool=self, page=page)
            return page

    async def _release_page(self, page: Any) -> None:
        await self._call_hook("pre_page_close", pool=self, page=page)
        slot: Optional[_BrowserSlot] = getattr(page, "_harvest_slot", None)
        try:
            await page.close()
        except Exception as exc:
            logger.warning("PlaywrightPool: page.close() failed: %s", exc)
            if slot is not None:
                slot.error_count += 1
                if slot.error_count >= self.error_threshold:
                    slot.state = "retired"
                    logger.warning(
                        "PlaywrightPool: browser retired after %d errors (threshold=%d)",
                        slot.error_count,
                        self.error_threshold,
                    )
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
        slot = _BrowserSlot(browser=browser, profile=profile)
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
            elif slot.error_count >= self.error_threshold:
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
            profile = slot.profile
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

    def health_report(self) -> Dict[str, Any]:
        """Structured health report with status, per-browser detail, and recommendations."""
        active_slots = [s for s in self._slots if s.state == "active"]
        active_count = len(active_slots)
        total_errors = sum(s.error_count for s in self._slots)
        half_threshold = self.error_threshold / 2

        browsers = []
        for i, slot in enumerate(self._slots):
            browsers.append({
                "index": i,
                "state": slot.state,
                "pages_served": slot.pages_served,
                "error_count": slot.error_count,
                "uptime_secs": slot.uptime_secs(),
                "idle_secs": slot.idle_secs(),
            })

        recommendations: List[str] = []
        for i, slot in enumerate(self._slots):
            if slot.error_count > 0:
                recommendations.append(
                    f"Browser #{i} has high error rate ({slot.error_count} errors) — consider restart"
                )
        if active_count == 0:
            recommendations.append("Pool exhausted — increase max_browsers")

        if active_count == 0:
            status = "critical"
        elif any(s.error_count >= half_threshold for s in active_slots):
            status = "degraded"
        else:
            status = "healthy"

        return {
            "status": status,
            "active_browsers": active_count,
            "total_errors": total_errors,
            "error_threshold": self.error_threshold,
            "browsers": browsers,
            "recommendations": recommendations,
        }

    async def warmup(self, n: int = 1) -> int:
        """
        Pre-launch up to `n` browsers so they are ready before the first request.
        Returns the number of browsers successfully launched.
        Call after open() to warm the pool.
        Fail-open: browser launch errors are swallowed.
        """
        if not self._open:
            return 0
        launched = 0
        async with self._lock:
            current = len([s for s in self._slots if s.state == "active"])
            to_launch = min(n, self.max_browsers - current)
            for _ in range(to_launch):
                try:
                    slot = await self._launch_slot()
                    self._slots.append(slot)
                    launched += 1
                except Exception as e:
                    logger.warning("PlaywrightPool.warmup: launch failed: %s", e)
        return launched

    def quota_stats(self) -> Dict[str, Any]:
        """Per-browser quota usage — pages served vs retire threshold."""
        return {
            "retire_after_pages": self.retire_after_pages,
            "browsers": [
                {
                    "index": i,
                    "pages_served": s.pages_served,
                    "quota_pct": round(s.pages_served / max(self.retire_after_pages, 1) * 100, 1),
                    "state": s.state,
                }
                for i, s in enumerate(self._slots)
            ],
        }
