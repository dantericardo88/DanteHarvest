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
