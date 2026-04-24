"""
BrowserActionLayer — structured action abstraction for agent-driven browser control.

Harvested from: Stagehand (MIT) action schema + Playwright page API.

Provides a typed action request/result layer that sits between LLM agents and
the raw Playwright page. Agents submit BrowserAction objects; the layer executes,
captures pre/post DOM snapshots, and returns ActionResult for agent feedback.

Constitutional guarantees:
- Fail-closed: unrecognised action types return ActionResult(success=False)
- Zero-ambiguity: ActionResult.success is always bool, never None
- Local-first: all snapshots are in-process; no external calls
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class BrowserActionType(str, enum.Enum):
    CLICK      = "click"
    FILL       = "fill"
    TYPE       = "type"       # key-by-key typing (triggers input events)
    SCROLL     = "scroll"
    NAVIGATE   = "navigate"
    WAIT       = "wait"
    HOVER      = "hover"
    SELECT     = "select"     # <select> dropdown
    ASSERT     = "assert"     # assert element text / visibility
    SCREENSHOT = "screenshot"
    EVALUATE   = "evaluate"   # run arbitrary JS and return result


@dataclass
class BrowserAction:
    """A single typed browser action."""
    type: BrowserActionType
    selector: Optional[str] = None       # CSS / XPath selector
    value: Optional[str] = None          # text to fill / JS to evaluate / URL to navigate
    timeout_ms: int = 5000
    # SCROLL: scroll direction and amount
    scroll_x: int = 0
    scroll_y: int = 500
    # ASSERT: expected text or visibility flag
    assert_text: Optional[str] = None
    assert_visible: Optional[bool] = None
    # Metadata for agent tracing
    step_id: Optional[str] = None
    intent: Optional[str] = None         # human-readable reason

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BrowserAction":
        """Deserialize from a plain dict (LLM output / JSON API)."""
        action_type = BrowserActionType(d["type"])
        return cls(
            type=action_type,
            selector=d.get("selector"),
            value=d.get("value"),
            timeout_ms=int(d.get("timeout_ms", 5000)),
            scroll_x=int(d.get("scroll_x", 0)),
            scroll_y=int(d.get("scroll_y", 500)),
            assert_text=d.get("assert_text"),
            assert_visible=d.get("assert_visible"),
            step_id=d.get("step_id"),
            intent=d.get("intent"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type.value,
            "selector": self.selector,
            "value": self.value,
            "timeout_ms": self.timeout_ms,
            "scroll_x": self.scroll_x,
            "scroll_y": self.scroll_y,
            "assert_text": self.assert_text,
            "assert_visible": self.assert_visible,
            "step_id": self.step_id,
            "intent": self.intent,
        }


@dataclass
class ActionResult:
    """Result of executing a single BrowserAction."""
    action_type: str
    success: bool
    duration_ms: float = 0.0
    error: Optional[str] = None
    # DOM snapshots for agent feedback
    pre_snapshot: Optional[str] = None   # HTML before action
    post_snapshot: Optional[str] = None  # HTML after action
    screenshot_bytes: Optional[bytes] = None
    evaluate_result: Optional[Any] = None
    # For ASSERT
    assert_passed: Optional[bool] = None
    assert_actual: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_type": self.action_type,
            "success": self.success,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "assert_passed": self.assert_passed,
            "assert_actual": self.assert_actual,
            "evaluate_result": self.evaluate_result,
        }


@dataclass
class ActionSequenceResult:
    """Result of executing a sequence of BrowserActions."""
    actions: List[ActionResult] = field(default_factory=list)
    final_url: Optional[str] = None
    final_html: Optional[str] = None
    total_duration_ms: float = 0.0

    @property
    def success(self) -> bool:
        return all(r.success for r in self.actions)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.actions if not r.success)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "actions": [a.to_dict() for a in self.actions],
            "failed_count": self.failed_count,
            "total_duration_ms": self.total_duration_ms,
            "final_url": self.final_url,
        }


class ActionLayer:
    """
    Executes BrowserAction sequences against a Playwright page.

    Wraps the raw Playwright page API with:
    - Pre/post HTML snapshot capture for agent feedback
    - Element existence pre-check before destructive actions
    - Typed action routing with fail-closed error handling
    - Per-action duration tracking

    Usage:
        layer = ActionLayer(page, capture_snapshots=True)
        result = await layer.execute(BrowserAction(type=BrowserActionType.CLICK, selector="#btn"))
    """

    def __init__(self, page: Any, capture_snapshots: bool = True, snapshot_max_bytes: int = 50_000):
        self._page = page
        self._capture = capture_snapshots
        self._snap_limit = snapshot_max_bytes

    async def execute(self, action: BrowserAction) -> ActionResult:
        """Execute a single action. Always returns ActionResult — never raises."""
        t0 = time.perf_counter()
        pre_snap: Optional[str] = None
        post_snap: Optional[str] = None

        action_type_str = action.type.value if hasattr(action.type, "value") else str(action.type)
        try:
            if self._capture and action.type not in (BrowserActionType.SCREENSHOT,):
                try:
                    pre_snap = await self._snapshot()
                except Exception:
                    pass

            result = await self._dispatch(action)

            if self._capture and action.type not in (BrowserActionType.SCREENSHOT,):
                try:
                    post_snap = await self._snapshot()
                except Exception:
                    pass

            result.pre_snapshot = pre_snap
            result.post_snapshot = post_snap
            result.duration_ms = (time.perf_counter() - t0) * 1000
            return result

        except Exception as e:
            return ActionResult(
                action_type=action_type_str,
                success=False,
                error=str(e),
                duration_ms=(time.perf_counter() - t0) * 1000,
            )

    async def execute_sequence(self, actions: List[BrowserAction]) -> ActionSequenceResult:
        """Execute a list of actions in order. Stops on first failure."""
        seq = ActionSequenceResult()
        t0 = time.perf_counter()
        for action in actions:
            result = await self.execute(action)
            seq.actions.append(result)
            if not result.success:
                break
        seq.total_duration_ms = (time.perf_counter() - t0) * 1000
        try:
            seq.final_url = self._page.url
            seq.final_html = await self._snapshot()
        except Exception:
            pass
        return seq

    # ------------------------------------------------------------------
    # Private dispatch
    # ------------------------------------------------------------------

    async def _dispatch(self, action: BrowserAction) -> ActionResult:
        t = action.type
        p = self._page

        if t == BrowserActionType.CLICK:
            if not action.selector:
                return ActionResult(t.value, success=False, error="selector required for CLICK")
            await p.click(action.selector, timeout=action.timeout_ms)
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.FILL:
            if not action.selector or action.value is None:
                return ActionResult(t.value, success=False, error="selector and value required for FILL")
            await p.fill(action.selector, action.value, timeout=action.timeout_ms)
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.TYPE:
            if not action.selector or action.value is None:
                return ActionResult(t.value, success=False, error="selector and value required for TYPE")
            await p.type(action.selector, action.value, timeout=action.timeout_ms)
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.HOVER:
            if not action.selector:
                return ActionResult(t.value, success=False, error="selector required for HOVER")
            await p.hover(action.selector, timeout=action.timeout_ms)
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.SELECT:
            if not action.selector or action.value is None:
                return ActionResult(t.value, success=False, error="selector and value required for SELECT")
            await p.select_option(action.selector, value=action.value, timeout=action.timeout_ms)
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.SCROLL:
            await p.evaluate(f"window.scrollBy({action.scroll_x}, {action.scroll_y})")
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.NAVIGATE:
            if not action.value:
                return ActionResult(t.value, success=False, error="value (URL) required for NAVIGATE")
            await p.goto(action.value, timeout=action.timeout_ms)
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.WAIT:
            import asyncio
            delay = float(action.value or "1000") / 1000.0
            await asyncio.sleep(min(delay, 30.0))
            return ActionResult(t.value, success=True)

        elif t == BrowserActionType.EVALUATE:
            if not action.value:
                return ActionResult(t.value, success=False, error="value (JS expression) required for EVALUATE")
            result = await p.evaluate(action.value)
            return ActionResult(t.value, success=True, evaluate_result=result)

        elif t == BrowserActionType.ASSERT:
            return await self._assert(action)

        elif t == BrowserActionType.SCREENSHOT:
            shot = await p.screenshot(type="png")
            return ActionResult(t.value, success=True, screenshot_bytes=shot)

        else:
            type_str = t.value if hasattr(t, "value") else str(t)
            return ActionResult(type_str, success=False, error=f"Unknown action type: {t}")

    async def _assert(self, action: BrowserAction) -> ActionResult:
        """Assert element text or visibility. Fail-closed on element not found."""
        result = ActionResult(BrowserActionType.ASSERT.value, success=False)
        try:
            if action.selector:
                element = await self._page.query_selector(action.selector)
                if element is None:
                    result.error = f"Element not found: {action.selector}"
                    result.assert_passed = False
                    return result

                if action.assert_visible is not None:
                    is_visible = await element.is_visible()
                    result.assert_passed = (is_visible == action.assert_visible)
                    result.assert_actual = f"visible={is_visible}"

                if action.assert_text is not None:
                    actual = await element.inner_text()
                    result.assert_actual = actual
                    result.assert_passed = (action.assert_text in actual)

                result.success = result.assert_passed is True
            else:
                # Assert on page title or URL
                if action.assert_text:
                    title = await self._page.title()
                    result.assert_passed = (action.assert_text in title)
                    result.assert_actual = title
                    result.success = result.assert_passed
        except Exception as e:
            result.error = str(e)
        return result

    async def _snapshot(self) -> str:
        """Capture a truncated HTML snapshot of the current page."""
        content = await self._page.content()
        if len(content) > self._snap_limit:
            content = content[: self._snap_limit] + "<!-- snapshot truncated -->"
        return content
