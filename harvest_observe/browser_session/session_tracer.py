"""
SessionTracer — structured event recording + Playwright trace archive support.

Harvested from: cua-bench + Playwright trace patterns.

Two-level tracing:
1. Structured JSONL event log — always active, zero external deps
2. Playwright .zip trace archive — active when a Playwright context is provided,
   captures screenshots + DOM snapshots viewable in `playwright show-trace`.

Constitutional guarantees:
- Non-fatal: trace persistence failures never raise (fail-open for diagnostics only)
- Local-first: all artifacts written to local filesystem
- Zero-ambiguity: save() always returns a path, even when recording failed
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class TraceEvent:
    event_name: str
    timestamp: float
    data: Dict[str, Any]
    screenshot_path: Optional[str] = None


@dataclass
class TraceRecord:
    trajectory_id: str
    started_at: float
    events: List[TraceEvent] = field(default_factory=list)
    playwright_trace_path: Optional[str] = None
    finished_at: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "trajectory_id": self.trajectory_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "playwright_trace_path": self.playwright_trace_path,
            "events": [asdict(e) for e in self.events],
        }


class SessionTracer:
    """
    Records structured session events and optionally captures a Playwright trace.

    Usage (standalone):
        tracer = SessionTracer(trace_dir="storage/traces")
        tid = tracer.start()
        tracer.record("step.started", {"action": "navigate", "url": "https://x.com"})
        tracer.record("step.completed", {"result": "ok"})
        path = tracer.save()   # writes trajectory_id.jsonl

    Usage (with Playwright context for .zip trace):
        async with playwright_context.tracing as t:
            await t.start(screenshots=True, snapshots=True)
            # ... run replay steps ...
            path = tracer.save(playwright_context=playwright_context)
    """

    def __init__(self, trace_dir: str = "storage/traces"):
        self._trace_dir = Path(trace_dir)
        self._trace_dir.mkdir(parents=True, exist_ok=True)
        self._record: Optional[TraceRecord] = None

    def start(self, trajectory_id: Optional[str] = None) -> str:
        tid = trajectory_id or str(uuid4())
        self._record = TraceRecord(trajectory_id=tid, started_at=time.time())
        return tid

    @property
    def trajectory_id(self) -> Optional[str]:
        return self._record.trajectory_id if self._record else None

    @property
    def events(self) -> List[TraceEvent]:
        return self._record.events if self._record else []

    def record(
        self,
        event_name: str,
        data: Dict[str, Any],
        screenshot: Optional[bytes] = None,
    ) -> None:
        if self._record is None:
            self.start()
        screenshot_path = None
        if screenshot is not None:
            screenshot_path = str(self._save_screenshot(screenshot))
        self._record.events.append(TraceEvent(
            event_name=event_name,
            timestamp=time.time(),
            data=data,
            screenshot_path=screenshot_path,
        ))

    def save(self, playwright_context: Any = None) -> Path:
        """
        Persist the trace to disk. Returns the path to the JSONL file.
        Playwright trace zip is saved alongside if context is provided.
        Non-fatal: errors are logged, not raised.
        """
        if self._record is None:
            self.start()
        self._record.finished_at = time.time()

        if playwright_context is not None:
            try:
                zip_path = self._trace_dir / f"{self._record.trajectory_id}.zip"
                # Playwright tracing.stop() is async — caller must await separately.
                # Store the intended path so ReplayHarness can call stop(path=...).
                self._record.playwright_trace_path = str(zip_path)
            except Exception as e:
                logger.warning("SessionTracer: playwright trace path setup failed: %s", e)

        out_path = self._trace_dir / f"{self._record.trajectory_id}.jsonl"
        try:
            lines = [json.dumps(e) for e in self._record.to_dict()["events"]]
            header = json.dumps({
                "trajectory_id": self._record.trajectory_id,
                "started_at": self._record.started_at,
                "finished_at": self._record.finished_at,
                "playwright_trace_path": self._record.playwright_trace_path,
                "event_count": len(self._record.events),
            })
            out_path.write_text("\n".join([header] + lines), encoding="utf-8")
        except Exception as e:
            logger.warning("SessionTracer: failed to write trace: %s", e)
        return out_path

    def get_record(self) -> Optional[TraceRecord]:
        return self._record

    def _save_screenshot(self, data: bytes) -> Path:
        if self._record is None:
            raise RuntimeError("tracer not started")
        screenshots_dir = self._trace_dir / self._record.trajectory_id / "screenshots"
        screenshots_dir.mkdir(parents=True, exist_ok=True)
        path = screenshots_dir / f"{uuid4()}.png"
        path.write_bytes(data)
        return path


# ---------------------------------------------------------------------------
# PlaywrightTraceCapture — async helper for ReplayHarness integration
# ---------------------------------------------------------------------------

class PlaywrightTraceCapture:
    """
    Context manager that starts a Playwright tracing session and stops it on exit.
    On failure (exception), saves the trace zip. On success, saves only if requested.

    Usage:
        async with PlaywrightTraceCapture(context, tracer, save_on_success=False) as cap:
            await replay_steps(...)
        # cap.trace_path is set if a trace was saved
    """

    def __init__(
        self,
        playwright_context: Any,
        tracer: SessionTracer,
        save_on_success: bool = False,
    ):
        self._ctx = playwright_context
        self._tracer = tracer
        self._save_on_success = save_on_success
        self.trace_path: Optional[Path] = None
        self._failed = False

    async def __aenter__(self) -> "PlaywrightTraceCapture":
        try:
            await self._ctx.tracing.start(screenshots=True, snapshots=True)
        except Exception as e:
            logger.warning("PlaywrightTraceCapture: tracing.start failed: %s", e)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._failed = exc_type is not None
        should_save = self._failed or self._save_on_success
        if should_save:
            try:
                tracer_record = self._tracer.get_record()
                tid = tracer_record.trajectory_id if tracer_record else str(uuid4())
                trace_dir = Path(self._tracer._trace_dir)
                zip_path = trace_dir / f"{tid}.zip"
                await self._ctx.tracing.stop(path=str(zip_path))
                self.trace_path = zip_path
                if tracer_record:
                    tracer_record.playwright_trace_path = str(zip_path)
                logger.info("PlaywrightTraceCapture: saved trace to %s", zip_path)
            except Exception as e:
                logger.warning("PlaywrightTraceCapture: failed to stop/save trace: %s", e)
        else:
            try:
                await self._ctx.tracing.stop()
            except Exception:
                pass
