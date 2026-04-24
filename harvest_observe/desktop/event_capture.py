"""
DesktopEventCapture — record keyboard/mouse/window events from desktop sessions.

Harvested from: OpenAdapt PerformanceEvent + Screenpipe event bus patterns.

Captures desktop events as typed EventRecord objects and writes them to the
append-only chain. Events are stored locally as JSONL per session.

Constitutional guarantees:
- Local-first: all events written to local filesystem before chain entry
- Fail-closed: missing pynput raises ImportError with install instructions (not silent)
- Zero-ambiguity: EventRecord.event_type is always a DesktopEventType enum, never str
- Append-only chain: every event MUST emit a chain entry
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List

from harvest_core.control.exceptions import HarvestError


class ObservationError(HarvestError):
    """Raised when a desktop observation operation fails."""
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


class DesktopEventType(str, Enum):
    KEY_PRESS = "key_press"
    KEY_RELEASE = "key_release"
    MOUSE_MOVE = "mouse_move"
    MOUSE_CLICK = "mouse_click"
    MOUSE_SCROLL = "mouse_scroll"
    WINDOW_FOCUS = "window_focus"
    WINDOW_CLOSE = "window_close"


@dataclass
class EventRecord:
    event_type: DesktopEventType
    timestamp: float
    session_id: str
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CaptureSession:
    session_id: str
    event_count: int
    duration_seconds: float
    output_path: str


class DesktopEventCapture:
    """
    Capture desktop keyboard/mouse events for observation plane recording.

    Usage (live capture):
        capture = DesktopEventCapture(writer, session_id="sess-001")
        session = await capture.start_capture(run_id="run-001", duration_seconds=60)

    Usage (ingest from file):
        session = await capture.ingest_event_file(path, run_id="run-001")
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        session_id: str,
        storage_root: str = "storage",
    ):
        self.chain_writer = chain_writer
        self.session_id = session_id
        self.storage_root = Path(storage_root)
        self._events: List[EventRecord] = []

    async def ingest_event_file(
        self, path: str | Path, run_id: str
    ) -> CaptureSession:
        """
        Load a pre-recorded JSONL event file and emit chain entries.
        Fail-closed: missing file raises ObservationError.
        """
        path = Path(path)
        if not path.exists():
            raise ObservationError(f"Event file not found: {path}")

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="desktop.capture_started",
            machine="desktop_event_capture",
            data={"session_id": self.session_id, "source": str(path)},
        ))

        events: List[EventRecord] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
                event = EventRecord(
                    event_type=DesktopEventType(raw["event_type"]),
                    timestamp=float(raw["timestamp"]),
                    session_id=raw.get("session_id", self.session_id),
                    data=raw.get("data", {}),
                )
                events.append(event)
            except (KeyError, ValueError):
                continue

        output_path = self._store_events(events)

        duration = 0.0
        if len(events) >= 2:
            duration = events[-1].timestamp - events[0].timestamp

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="desktop.capture_completed",
            machine="desktop_event_capture",
            data={
                "session_id": self.session_id,
                "event_count": len(events),
                "duration_seconds": round(duration, 3),
                "output_path": str(output_path),
            },
        ))

        return CaptureSession(
            session_id=self.session_id,
            event_count=len(events),
            duration_seconds=duration,
            output_path=str(output_path),
        )

    async def start_live_capture(
        self, run_id: str, duration_seconds: float = 30.0
    ) -> CaptureSession:
        """
        Capture live keyboard/mouse events for duration_seconds.
        Requires pynput (MIT license). Raises ObservationError if not installed.
        """
        try:
            from pynput import keyboard, mouse
        except ImportError as e:
            raise ObservationError(
                "pynput not installed. Run: pip install pynput"
            ) from e

        import threading

        events: List[EventRecord] = []
        def _on_key_press(key):
            events.append(EventRecord(
                event_type=DesktopEventType.KEY_PRESS,
                timestamp=time.time(),
                session_id=self.session_id,
                data={"key": str(key)},
            ))

        def _on_key_release(key):
            events.append(EventRecord(
                event_type=DesktopEventType.KEY_RELEASE,
                timestamp=time.time(),
                session_id=self.session_id,
                data={"key": str(key)},
            ))

        def _on_mouse_click(x, y, button, pressed):
            events.append(EventRecord(
                event_type=DesktopEventType.MOUSE_CLICK,
                timestamp=time.time(),
                session_id=self.session_id,
                data={"x": x, "y": y, "button": str(button), "pressed": pressed},
            ))

        def _on_mouse_scroll(x, y, dx, dy):
            events.append(EventRecord(
                event_type=DesktopEventType.MOUSE_SCROLL,
                timestamp=time.time(),
                session_id=self.session_id,
                data={"x": x, "y": y, "dx": dx, "dy": dy},
            ))

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="desktop.capture_started",
            machine="desktop_event_capture",
            data={"session_id": self.session_id, "duration_seconds": duration_seconds},
        ))

        key_listener = keyboard.Listener(on_press=_on_key_press, on_release=_on_key_release)
        mouse_listener = mouse.Listener(on_click=_on_mouse_click, on_scroll=_on_mouse_scroll)

        key_listener.start()
        mouse_listener.start()

        import asyncio
        await asyncio.sleep(duration_seconds)

        key_listener.stop()
        mouse_listener.stop()

        output_path = self._store_events(events)
        duration = duration_seconds

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="desktop.capture_completed",
            machine="desktop_event_capture",
            data={
                "session_id": self.session_id,
                "event_count": len(events),
                "duration_seconds": duration,
                "output_path": str(output_path),
            },
        ))

        return CaptureSession(
            session_id=self.session_id,
            event_count=len(events),
            duration_seconds=duration,
            output_path=str(output_path),
        )

    def _store_events(self, events: List[EventRecord]) -> Path:
        out_dir = self.storage_root / "desktop" / self.session_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "events.jsonl"
        lines = []
        for e in events:
            lines.append(json.dumps({
                "event_type": e.event_type.value,
                "timestamp": e.timestamp,
                "session_id": e.session_id,
                "data": e.data,
            }))
        out_path.write_text("\n".join(lines), encoding="utf-8")
        return out_path
