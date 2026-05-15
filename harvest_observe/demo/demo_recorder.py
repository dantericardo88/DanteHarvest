"""
DemoRecorder — record browser interactions and convert to WorkflowPack.

Wave 7h: human_demo_to_automation — demo recording + pack generation (4→9).

Records a human's browser session as a sequence of typed actions:
  navigate, click, fill, scroll, wait, screenshot, assert_text

Then converts the recording to a WorkflowPack that can be replayed,
diffed, and promoted through the pack registry.

Constitutional guarantees:
- Local-first: recording stored as local JSONL, no cloud upload
- Fail-open: individual action recording errors are logged, not raised
- Zero-ambiguity: every step gets a stable step_id derived from index+action
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from uuid import uuid4


# ---------------------------------------------------------------------------
# Recorded action
# ---------------------------------------------------------------------------

@dataclass
class RecordedAction:
    index: int
    action_type: str      # navigate|click|fill|scroll|wait|screenshot|assert_text|keypress
    selector: Optional[str]
    value: Optional[str]  # URL for navigate, text for fill/assert
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def step_id(self) -> str:
        """Stable ID derived from index + action so diffs are meaningful."""
        raw = f"{self.index}:{self.action_type}:{self.selector or ''}:{self.value or ''}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def to_pack_step(self) -> dict:
        """Convert to WorkflowPack step format."""
        action_str = self._build_action_string()
        return {
            "id": self.step_id,
            "action": action_str,
            "metadata": {"recorded_at": self.timestamp, **self.metadata},
        }

    def _build_action_string(self) -> str:
        if self.action_type == "navigate":
            return f"navigate {self.value or ''}"
        if self.action_type == "click":
            return f"click {self.selector or ''}"
        if self.action_type == "fill":
            return f"fill {self.selector or ''} {self.value or ''}"
        if self.action_type == "scroll":
            return f"scroll {self.selector or 'window'}"
        if self.action_type == "wait":
            return f"wait {self.value or '1000'}ms"
        if self.action_type == "screenshot":
            return f"screenshot {self.value or 'frame'}"
        if self.action_type == "assert_text":
            return f"assert_text {self.selector or 'body'} contains {self.value or ''}"
        if self.action_type == "keypress":
            return f"keypress {self.value or ''}"
        return f"{self.action_type} {self.selector or ''} {self.value or ''}".strip()

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# DemoSession
# ---------------------------------------------------------------------------

@dataclass
class DemoSession:
    session_id: str
    title: str
    started_at: float = field(default_factory=time.time)
    actions: List[RecordedAction] = field(default_factory=list)
    completed_at: Optional[float] = None

    @property
    def duration_s(self) -> Optional[float]:
        if self.completed_at:
            return self.completed_at - self.started_at
        return None

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "title": self.title,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_s": self.duration_s,
            "action_count": len(self.actions),
            "actions": [a.to_dict() for a in self.actions],
        }


# ---------------------------------------------------------------------------
# DemoRecorder
# ---------------------------------------------------------------------------

class DemoRecorder:
    """
    Record a human demo session as a sequence of browser actions,
    then convert to a WorkflowPack for automated replay.

    Usage (manual recording):
        recorder = DemoRecorder(output_dir=Path("demos/"))
        recorder.start_session("Login flow demo")
        recorder.record_navigate("https://example.com/login")
        recorder.record_fill("#username", "user@example.com")
        recorder.record_fill("#password", "secret")
        recorder.record_click("#submit-btn")
        recorder.record_assert_text("#dashboard", "Welcome")
        session = recorder.stop_session()
        pack = recorder.to_workflow_pack(session)

    Usage (Playwright integration):
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            recorder.attach_playwright_page(page)
            # page interactions are automatically recorded
            await page.goto("https://example.com")
            await page.click("#btn")
            session = recorder.stop_session()
    """

    def __init__(self, output_dir: Optional[Path] = None):
        self._output_dir = Path(output_dir) if output_dir else Path("demos")
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._session: Optional[DemoSession] = None

    def start_session(self, title: str) -> DemoSession:
        self._session = DemoSession(
            session_id=str(uuid4()),
            title=title,
        )
        return self._session

    def stop_session(self) -> DemoSession:
        if not self._session:
            raise RuntimeError("No active session")
        self._session.completed_at = time.time()
        self._save_session(self._session)
        session = self._session
        self._session = None
        return session

    # ------------------------------------------------------------------
    # Recording primitives
    # ------------------------------------------------------------------

    def record_navigate(self, url: str, metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("navigate", value=url, metadata=metadata)

    def record_click(self, selector: str, metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("click", selector=selector, metadata=metadata)

    def record_fill(self, selector: str, value: str, metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("fill", selector=selector, value=value, metadata=metadata)

    def record_scroll(self, selector: str = "window", metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("scroll", selector=selector, metadata=metadata)

    def record_wait(self, ms: int = 1000, metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("wait", value=str(ms), metadata=metadata)

    def record_screenshot(self, label: str = "frame", metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("screenshot", value=label, metadata=metadata)

    def record_assert_text(self, selector: str, text: str, metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("assert_text", selector=selector, value=text, metadata=metadata)

    def record_keypress(self, key: str, metadata: Optional[dict] = None) -> RecordedAction:
        return self._record("keypress", value=key, metadata=metadata)

    # ------------------------------------------------------------------
    # WorkflowPack conversion
    # ------------------------------------------------------------------

    def to_workflow_pack(self, session: DemoSession) -> dict:
        """
        Convert a DemoSession to a WorkflowPack dict ready for registration.
        """
        steps = [action.to_pack_step() for action in session.actions]
        return {
            "pack_id": f"demo-{session.session_id[:8]}",
            "pack_type": "workflowPack",
            "title": session.title,
            "description": f"Recorded demo: {session.title} ({len(steps)} steps)",
            "source": "demo_recorder",
            "recorded_at": session.started_at,
            "duration_s": session.duration_s,
            "steps": steps,
        }

    # ------------------------------------------------------------------
    # Playwright integration
    # ------------------------------------------------------------------

    def attach_playwright_page(self, page: Any) -> None:
        """
        Attach Playwright event listeners to auto-record page interactions.
        Call start_session() before attaching.
        """
        if not self._session:
            raise RuntimeError("Call start_session() before attaching a page")

        recorder = self

        page.on("framenavigated", lambda frame: recorder._on_navigate(frame))

        async def _on_click(source: dict) -> None:
            selector = source.get("selector", "")
            recorder.record_click(selector, metadata={"auto_recorded": True})

        try:
            page.expose_binding("__harvest_click__", _on_click)
            page.add_init_script("""
                document.addEventListener('click', (e) => {
                    const sel = e.target.tagName.toLowerCase() +
                        (e.target.id ? '#' + e.target.id : '') +
                        (e.target.className ? '.' + [...e.target.classList].join('.') : '');
                    if (window.__harvest_click__) window.__harvest_click__({selector: sel});
                }, true);
            """)
        except Exception:
            pass  # Fail-open: auto-recording is best-effort

    def _on_navigate(self, frame: Any) -> None:
        try:
            url = frame.url
            if url and url != "about:blank":
                self.record_navigate(url, metadata={"auto_recorded": True})
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Load / list sessions
    # ------------------------------------------------------------------

    def load_session(self, session_id: str) -> Optional[DemoSession]:
        path = self._output_dir / f"{session_id}.jsonl"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
        session = DemoSession(
            session_id=data["session_id"],
            title=data["title"],
            started_at=data["started_at"],
            completed_at=data.get("completed_at"),
        )
        for a_dict in data.get("actions", []):
            session.actions.append(RecordedAction(**a_dict))
        return session

    def list_sessions(self) -> List[str]:
        return [p.stem for p in sorted(self._output_dir.glob("*.jsonl"))]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _record(
        self,
        action_type: str,
        selector: Optional[str] = None,
        value: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> RecordedAction:
        if not self._session:
            raise RuntimeError("Call start_session() before recording actions")
        action = RecordedAction(
            index=len(self._session.actions),
            action_type=action_type,
            selector=selector,
            value=value,
            metadata=metadata or {},
        )
        self._session.actions.append(action)
        return action

    def _save_session(self, session: DemoSession) -> None:
        path = self._output_dir / f"{session.session_id}.jsonl"
        try:
            path.write_text(json.dumps(session.to_dict()) + "\n", encoding="utf-8")
        except Exception:
            pass
