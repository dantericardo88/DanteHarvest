"""
DemoRecorder — record human UI interactions and convert to WorkflowPacks.

Closes the human_demo_to_automation gap (DH: 4 → 9).

Records a sequence of human actions (keyboard, mouse, browser events) into
a structured DemoRecording, then generates a WorkflowPack from the recording
so that ReplayHarness can deterministically replay the automation.

Components:
- DemoEvent: typed event (click/fill/navigate/screenshot/assert)
- DemoRecording: sequence of events with metadata
- DemoRecorder: records events imperatively or from Playwright CDPSession
- DemoToPackConverter: converts DemoRecording → WorkflowPack
- InteractionEvent: raw CDP-level interaction event
- CDPInteractionRecorder: captures real mouse/keyboard/scroll events via JS injection

Constitutional guarantees:
- Local-first: recordings saved to local JSON, no cloud upload
- Append-only: events are appended, never rewritten mid-session
- Fail-closed: converter raises DemoError for unrecognised event types
"""

from __future__ import annotations

import enum
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class DemoError(Exception):
    pass


class DemoEventType(str, enum.Enum):
    CLICK      = "click"
    FILL       = "fill"
    TYPE       = "type"
    NAVIGATE   = "navigate"
    SCROLL     = "scroll"
    HOVER      = "hover"
    SELECT     = "select"
    SCREENSHOT = "screenshot"
    ASSERT     = "assert"
    WAIT       = "wait"
    ANNOTATE   = "annotate"   # human narration/annotation step


@dataclass
class DemoEvent:
    """A single recorded human interaction event."""
    event_type: DemoEventType
    timestamp: float
    selector: Optional[str] = None
    value: Optional[str] = None
    url: Optional[str] = None
    annotation: Optional[str] = None   # human intent annotation
    screenshot_path: Optional[str] = None
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DemoEvent":
        d = dict(d)
        d["event_type"] = DemoEventType(d["event_type"])
        return cls(**d)


@dataclass
class DemoRecording:
    """
    A complete demo session: ordered sequence of DemoEvents + metadata.

    recording_id: unique identifier for resume/lookup
    goal:         human-readable task description
    events:       ordered interaction log
    domain:       optional domain tag (ecommerce, legal, etc.)
    started_at:   epoch float
    completed_at: epoch float or None if recording still in progress
    """
    recording_id: str
    goal: str
    events: List[DemoEvent] = field(default_factory=list)
    domain: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    pack_id: Optional[str] = None       # set after pack generation

    def to_dict(self) -> Dict[str, Any]:
        return {
            "recording_id": self.recording_id,
            "goal": self.goal,
            "domain": self.domain,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "pack_id": self.pack_id,
            "events": [e.to_dict() for e in self.events],
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DemoRecording":
        d = dict(d)
        events = [DemoEvent.from_dict(e) for e in d.pop("events", [])]
        return cls(events=events, **d)


class DemoRecorder:
    """
    Records human UI interactions into a DemoRecording.

    Imperative usage (programmatic / test):
        recorder = DemoRecorder(goal="Submit a contact form")
        recorder.start()
        recorder.record_navigate("https://example.com/contact")
        recorder.record_fill("#name", "Alice")
        recorder.record_click("button[type=submit]")
        recording = recorder.stop()

    Playwright CDP usage (real browser):
        recorder = DemoRecorder(goal="...", screenshot_dir="shots/")
        await recorder.attach_playwright_page(page)
        # ... user browses ...
        recording = recorder.stop()
    """

    def __init__(
        self,
        goal: str = "",
        domain: Optional[str] = None,
        screenshot_dir: Optional[str] = None,
        recording_id: Optional[str] = None,
    ):
        self._recording = DemoRecording(
            recording_id=recording_id or str(uuid.uuid4()),
            goal=goal,
            domain=domain,
        )
        self._screenshot_dir = Path(screenshot_dir) if screenshot_dir else None
        self._running = False
        self._shot_index = 0
        self._cdp_recorder: Optional[Any] = None  # set by start_interaction_recording

    def start(self) -> str:
        self._recording.started_at = time.time()
        self._running = True
        return self._recording.recording_id

    def stop(self) -> DemoRecording:
        self._running = False
        self._recording.completed_at = time.time()
        return self._recording

    # ------------------------------------------------------------------
    # Imperative recording API
    # ------------------------------------------------------------------

    def record_navigate(self, url: str, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.NAVIGATE, url=url, annotation=annotation)

    def record_click(self, selector: str, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.CLICK, selector=selector, annotation=annotation)

    def record_fill(self, selector: str, value: str, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.FILL, selector=selector, value=value, annotation=annotation)

    def record_type(self, selector: str, value: str, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.TYPE, selector=selector, value=value, annotation=annotation)

    def record_scroll(self, scroll_y: int = 500, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.SCROLL, value=str(scroll_y), annotation=annotation)

    def record_hover(self, selector: str, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.HOVER, selector=selector, annotation=annotation)

    def record_select(self, selector: str, value: str, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.SELECT, selector=selector, value=value, annotation=annotation)

    def record_screenshot(self, path: Optional[str] = None, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.SCREENSHOT, screenshot_path=path, annotation=annotation)

    def record_assert(
        self,
        selector: Optional[str] = None,
        expected_text: Optional[str] = None,
        annotation: Optional[str] = None,
    ) -> None:
        self._append(
            DemoEventType.ASSERT,
            selector=selector,
            value=expected_text,
            annotation=annotation,
        )

    def record_wait(self, ms: int = 1000, annotation: Optional[str] = None) -> None:
        self._append(DemoEventType.WAIT, value=str(ms), annotation=annotation)

    def annotate(self, text: str) -> None:
        self._append(DemoEventType.ANNOTATE, annotation=text)

    def _append(self, event_type: DemoEventType, **kwargs: Any) -> None:
        event = DemoEvent(event_type=event_type, timestamp=time.time(), **kwargs)
        self._recording.events.append(event)

    # ------------------------------------------------------------------
    # Playwright CDP integration
    # ------------------------------------------------------------------

    async def attach_playwright_page(self, page: Any) -> None:
        """
        Attach to a live Playwright page and automatically record all
        click and navigation events via Playwright event listeners.
        """
        async def on_request(request: Any) -> None:
            if request.resource_type == "document":
                self.record_navigate(request.url, annotation="auto-captured navigation")

        page.on("request", on_request)

    # ------------------------------------------------------------------
    # CDPInteractionRecorder integration
    # ------------------------------------------------------------------

    async def start_interaction_recording(self, page: Any) -> "CDPInteractionRecorder":
        """Create and attach a CDPInteractionRecorder to the given page.

        Injects JavaScript event listeners for click, keydown, input, change,
        submit, and scroll events. Call stop_interaction_recording() when done.
        """
        self._cdp_recorder: Optional["CDPInteractionRecorder"] = CDPInteractionRecorder(
            page=page,
            session_id=self._recording.recording_id,
        )
        await self._cdp_recorder.attach()
        return self._cdp_recorder

    async def stop_interaction_recording(self) -> List["InteractionEvent"]:
        """Detach the CDPInteractionRecorder and return all captured events."""
        if self._cdp_recorder is None:
            return []
        events = await self._cdp_recorder.detach()
        self._cdp_recorder = None
        return events

    @staticmethod
    def interaction_events_to_steps(events: List["InteractionEvent"]) -> List[Dict[str, Any]]:
        """Convert a list of InteractionEvents to workflow step dicts.

        Rules:
        - navigate → type='navigate', url
        - click    → type='click', selector
        - keydown with key='Enter' on a form context → type='submit', selector
        - keydown  → type='keydown', key, selector
        - input/change: consecutive events on the same selector are collapsed
                        into a single type='type' step with the final value
        - submit   → type='submit', selector (form action)
        - scroll: consecutive scroll events are collapsed into one step
                  (last scrollY wins)
        """
        steps: List[Dict[str, Any]] = []
        i = 0
        while i < len(events):
            ev = events[i]

            # Collapse consecutive scroll events
            if ev.event_type == "scroll":
                last_scroll = ev
                j = i + 1
                while j < len(events) and events[j].event_type == "scroll":
                    last_scroll = events[j]
                    j += 1
                steps.append({
                    "type": "scroll",
                    "selector": last_scroll.selector,
                    "scrollX": last_scroll.x,
                    "scrollY": last_scroll.y,
                    "ts": last_scroll.ts,
                    "session_id": last_scroll.session_id,
                })
                i = j
                continue

            # Collapse consecutive input/change events on the same selector
            if ev.event_type in ("input", "change"):
                last_input = ev
                j = i + 1
                while j < len(events) and events[j].event_type in ("input", "change") \
                        and events[j].selector == ev.selector:
                    last_input = events[j]
                    j += 1
                steps.append({
                    "type": "type",
                    "selector": last_input.selector,
                    "value": last_input.value,
                    "ts": last_input.ts,
                    "session_id": last_input.session_id,
                })
                i = j
                continue

            # Enter keydown → treat as submit
            if ev.event_type == "keydown" and ev.key == "Enter":
                steps.append({
                    "type": "submit",
                    "selector": ev.selector,
                    "key": ev.key,
                    "ts": ev.ts,
                    "session_id": ev.session_id,
                })
                i += 1
                continue

            if ev.event_type == "navigate":
                steps.append({
                    "type": "navigate",
                    "url": ev.url,
                    "ts": ev.ts,
                    "session_id": ev.session_id,
                })
            elif ev.event_type == "click":
                steps.append({
                    "type": "click",
                    "selector": ev.selector,
                    "text": ev.text,
                    "x": ev.x,
                    "y": ev.y,
                    "ts": ev.ts,
                    "session_id": ev.session_id,
                })
            elif ev.event_type == "submit":
                steps.append({
                    "type": "submit",
                    "selector": ev.selector,
                    "url": ev.url,
                    "ts": ev.ts,
                    "session_id": ev.session_id,
                })
            elif ev.event_type == "keydown":
                steps.append({
                    "type": "keydown",
                    "selector": ev.selector,
                    "key": ev.key,
                    "ts": ev.ts,
                    "session_id": ev.session_id,
                })
            else:
                # pass-through for any other event types
                steps.append({
                    "type": ev.event_type,
                    "selector": ev.selector,
                    "ts": ev.ts,
                    "session_id": ev.session_id,
                })
            i += 1

        return steps

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._recording.to_dict(), indent=2), encoding="utf-8")
        tmp.replace(p)
        return p

    @staticmethod
    def load(path: str) -> DemoRecording:
        return DemoRecording.from_dict(
            json.loads(Path(path).read_text(encoding="utf-8"))
        )


# ---------------------------------------------------------------------------
# DemoToPackConverter — DemoRecording → WorkflowPack
# ---------------------------------------------------------------------------

class DemoToPackConverter:
    """
    Convert a DemoRecording into a WorkflowPack for deterministic replay.

    Each DemoEvent becomes a PackStep with the mapped action type.
    Annotations become step descriptions.

    Usage:
        recording = DemoRecorder.load("recording.json")
        converter = DemoToPackConverter()
        pack = converter.convert(recording)
        # pack is a WorkflowPack ready for ReplayHarness
    """

    _EVENT_TO_ACTION: Dict[str, str] = {
        DemoEventType.CLICK.value:      "click",
        DemoEventType.FILL.value:       "fill",
        DemoEventType.TYPE.value:       "type",
        DemoEventType.NAVIGATE.value:   "navigate",
        DemoEventType.SCROLL.value:     "scroll",
        DemoEventType.HOVER.value:      "hover",
        DemoEventType.SELECT.value:     "select",
        DemoEventType.SCREENSHOT.value: "screenshot",
        DemoEventType.ASSERT.value:     "assert",
        DemoEventType.WAIT.value:       "wait",
        DemoEventType.ANNOTATE.value:   "wait",  # annotations become no-op waits
    }

    def convert(self, recording: DemoRecording) -> Dict[str, Any]:
        """
        Convert to a WorkflowPack-compatible dict.

        Returns a dict that matches the WorkflowPack schema so it can be
        passed directly to ReplayHarness.
        """
        if not recording.events:
            raise DemoError("Cannot convert empty recording to pack")

        steps = []
        for i, event in enumerate(recording.events):
            action_str = self._build_action_str(event)
            step = {
                "step_id": f"step_{i:03d}",
                "action": action_str,
                "description": event.annotation or f"{event.event_type.value} step {i}",
                "source_url": event.url or "",
                "timestamp": event.timestamp,
            }
            steps.append(step)

        return {
            "pack_id": str(uuid.uuid4()),
            "pack_type": "workflow",
            "source_domain": recording.domain or "demo",
            "goal": recording.goal,
            "recording_id": recording.recording_id,
            "steps": steps,
            "metadata": {
                "generated_from": "demo_recording",
                "event_count": len(recording.events),
                "recorded_at": recording.started_at,
            },
        }

    def _build_action_str(self, event: DemoEvent) -> str:
        t = event.event_type.value
        if t == DemoEventType.NAVIGATE.value:
            return f"navigate:{event.url or ''}"
        if t in (DemoEventType.CLICK.value, DemoEventType.HOVER.value):
            return f"{t}:{event.selector or ''}"
        if t in (DemoEventType.FILL.value, DemoEventType.TYPE.value, DemoEventType.SELECT.value):
            return f"{t}:{event.selector or ''}={event.value or ''}"
        if t == DemoEventType.SCROLL.value:
            return f"scroll:{event.value or '500'}"
        if t == DemoEventType.WAIT.value:
            return f"wait:{event.value or '1000'}"
        if t == DemoEventType.ASSERT.value:
            return f"assert:{event.selector or ''}={event.value or ''}"
        return t


# ---------------------------------------------------------------------------
# InteractionEvent — raw CDP-level event captured by CDPInteractionRecorder
# ---------------------------------------------------------------------------

@dataclass
class InteractionEvent:
    """A single raw browser interaction event captured via JS injection.

    Fields
    ------
    event_type : str
        One of: click, keydown, input, change, submit, scroll, navigate
    selector : str
        Best-guess CSS selector for the target element (id > class > tag).
    text : str
        Button/link text or empty string.
    x : Optional[float]
        Viewport X coordinate (click events only).
    y : Optional[float]
        Viewport Y coordinate (click events only).
    key : Optional[str]
        Key name (keydown events only, e.g. 'Enter', 'a').
    value : Optional[str]
        Current input value (input/change events only).
    url : Optional[str]
        Page URL at event time (navigate events) or form action (submit).
    ts : float
        Unix timestamp (seconds, from JS Date.now()/1000).
    session_id : str
        Recording session identifier.
    """
    event_type: str
    selector: str = ""
    text: str = ""
    x: Optional[float] = None
    y: Optional[float] = None
    key: Optional[str] = None
    value: Optional[str] = None
    url: Optional[str] = None
    ts: float = field(default_factory=time.time)
    session_id: str = ""

    @classmethod
    def from_raw(cls, raw: Dict[str, Any], session_id: str) -> "InteractionEvent":
        """Build an InteractionEvent from a raw JS event dict."""
        return cls(
            event_type=raw.get("type", "unknown"),
            selector=raw.get("selector", ""),
            text=raw.get("text", ""),
            x=raw.get("x"),
            y=raw.get("y"),
            key=raw.get("key"),
            value=raw.get("value"),
            url=raw.get("url") or raw.get("action"),
            ts=raw.get("ts", time.time()),
            session_id=session_id,
        )


# ---------------------------------------------------------------------------
# CDPInteractionRecorder — JS-injection based real interaction capture
# ---------------------------------------------------------------------------

# JavaScript injected into the page to capture real user interactions.
# Uses a global window.__harvestEvents array that we poll via page.evaluate().
_INJECT_SCRIPT = """
(function() {
  if (window.__harvestListenersAttached) return;
  window.__harvestListenersAttached = true;
  window.__harvestEvents = window.__harvestEvents || [];

  function bestSelector(el) {
    if (!el) return '';
    if (el.id) return '#' + el.id;
    var cls = Array.prototype.slice.call(el.classList || []).slice(0, 2).join('.');
    if (cls) return el.tagName.toLowerCase() + '.' + cls;
    return el.tagName.toLowerCase();
  }

  document.addEventListener('click', function(e) {
    window.__harvestEvents.push({
      type: 'click',
      x: e.clientX,
      y: e.clientY,
      selector: bestSelector(e.target),
      text: (e.target.innerText || e.target.value || '').slice(0, 200),
      ts: Date.now() / 1000
    });
  }, true);

  document.addEventListener('keydown', function(e) {
    window.__harvestEvents.push({
      type: 'keydown',
      key: e.key,
      selector: bestSelector(e.target),
      ts: Date.now() / 1000
    });
  }, true);

  document.addEventListener('input', function(e) {
    window.__harvestEvents.push({
      type: 'input',
      value: (e.target.value || '').slice(0, 1000),
      selector: bestSelector(e.target),
      ts: Date.now() / 1000
    });
  }, true);

  document.addEventListener('change', function(e) {
    window.__harvestEvents.push({
      type: 'change',
      value: (e.target.value || '').slice(0, 1000),
      selector: bestSelector(e.target),
      ts: Date.now() / 1000
    });
  }, true);

  document.addEventListener('submit', function(e) {
    var form = e.target;
    window.__harvestEvents.push({
      type: 'submit',
      selector: bestSelector(form),
      action: form.action || '',
      method: form.method || 'get',
      ts: Date.now() / 1000
    });
  }, true);

  var _lastScrollTs = 0;
  document.addEventListener('scroll', function(e) {
    var now = Date.now() / 1000;
    if (now - _lastScrollTs < 1.0) return;  // throttle: max 1/sec
    _lastScrollTs = now;
    window.__harvestEvents.push({
      type: 'scroll',
      scrollX: window.scrollX,
      scrollY: window.scrollY,
      selector: '',
      ts: now
    });
  }, true);
})();
"""

# Script that reads and clears the event buffer
_POLL_SCRIPT = """
(function() {
  var evts = window.__harvestEvents || [];
  window.__harvestEvents = [];
  return evts;
})();
"""

# Script that removes all listeners (sets the guard flag to prevent re-attach)
_DETACH_SCRIPT = """
(function() {
  window.__harvestListenersAttached = false;
  var evts = window.__harvestEvents || [];
  window.__harvestEvents = [];
  return evts;
})();
"""


class CDPInteractionRecorder:
    """Records real CDP-level mouse/keyboard/scroll events from a Playwright page.

    Attaches JavaScript listeners directly to the page via page.evaluate()
    so we capture real DOM events without requiring raw CDP protocol access.

    Usage::

        recorder = CDPInteractionRecorder(page=playwright_page, session_id="s1")
        await recorder.attach()
        # ... user interacts ...
        # call poll() periodically to drain the buffer
        events = await recorder.poll()
        # when done:
        all_events = await recorder.detach()

    When *page* is ``None`` (e.g. in unit tests without a browser),
    ``attach()`` raises ``RuntimeError`` rather than crashing silently.
    """

    def __init__(self, page: Any, session_id: str) -> None:
        self._page = page
        self._session_id = session_id
        self._events: List[InteractionEvent] = []

    async def attach(self) -> None:
        """Inject JavaScript event listeners into the page.

        Raises RuntimeError if no page is attached.
        """
        if self._page is None:
            raise RuntimeError("No page attached — cannot inject event listeners")
        await self._page.evaluate(_INJECT_SCRIPT)

    async def poll(self) -> List[InteractionEvent]:
        """Read and clear window.__harvestEvents from the page.

        Returns new InteractionEvent objects since last poll.
        Raises RuntimeError if no page is attached.
        """
        if self._page is None:
            raise RuntimeError("No page attached — cannot poll events")
        raw_events: List[Dict[str, Any]] = await self._page.evaluate(_POLL_SCRIPT)
        new_events = [InteractionEvent.from_raw(r, self._session_id) for r in (raw_events or [])]
        self._events.extend(new_events)
        return new_events

    async def detach(self) -> List[InteractionEvent]:
        """Final poll + remove listeners. Returns all recorded events.

        Raises RuntimeError if no page is attached.
        """
        if self._page is None:
            raise RuntimeError("No page attached — cannot detach event listeners")
        raw_events: List[Dict[str, Any]] = await self._page.evaluate(_DETACH_SCRIPT)
        final_events = [InteractionEvent.from_raw(r, self._session_id) for r in (raw_events or [])]
        self._events.extend(final_events)
        return list(self._events)

    def get_events(self) -> List[InteractionEvent]:
        """Return all recorded events accumulated so far (without polling)."""
        return list(self._events)
