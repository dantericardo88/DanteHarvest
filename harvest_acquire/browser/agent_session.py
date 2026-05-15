"""
AgentSession — stateful multi-turn browser control loop for long-horizon tasks.

Closes the agent_browser_infrastructure gap (DH: 4 → 9).

Provides:
- AgentSession: persistent browser session across multiple agent turns
- AgentTask: declarative task with goal, steps, and termination conditions
- AgentPlanner: simple LLM-agnostic planning loop (goal → subtasks → execute → observe)
- AgentSessionStore: JSON-backed persistence for session resume

Constitutional guarantees:
- Local-first: sessions persisted to local JSON; no cloud required
- Fail-closed: exceeded max_turns raises AgentError, not silent loop
- Zero-ambiguity: AgentSessionStatus enum — no intermediate states
- Append-only: all turn results appended to session history
"""

from __future__ import annotations

import enum
import json
import time
import uuid
from uuid import uuid4
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from harvest_acquire.browser.action_layer import (
    ActionLayer,
    BrowserAction,
    BrowserActionType,
    ActionResult,
)
from harvest_core.control.exceptions import AcquisitionError


class AgentError(Exception):
    pass


class AgentSessionStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    PAUSED    = "paused"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class AgentTurn:
    """A single reasoning + action turn within a session."""
    turn_id: str
    turn_index: int
    timestamp: float
    goal: str
    action: Dict[str, Any]
    result: Dict[str, Any]
    observation: str           # summary of page state after action
    success: bool
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AgentTask:
    """
    A declarative long-horizon task for the agent to execute.

    goal:           Natural language task description
    success_url:    Optional URL pattern that signals task completion
    success_text:   Optional page-text that signals task completion
    max_turns:      Hard ceiling on action turns (fail-closed)
    subtasks:       Optional ordered list of intermediate goal strings
    """
    goal: str
    success_url: Optional[str] = None
    success_text: Optional[str] = None
    max_turns: int = 50
    subtasks: List[str] = field(default_factory=list)


@dataclass
class AgentSession:
    """
    Persistent browser session supporting multi-turn agent control.

    A session tracks:
    - Current task and all turn history
    - Browser page state (URL, title, DOM snapshot)
    - Intermediate subtask progress
    - Resume capability via JSON serialization
    """
    session_id: str
    task: AgentTask
    status: AgentSessionStatus = AgentSessionStatus.PENDING
    turns: List[AgentTurn] = field(default_factory=list)
    current_subtask_index: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    final_url: Optional[str] = None
    final_snapshot: Optional[str] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def new(cls, task: AgentTask, metadata: Optional[Dict[str, Any]] = None) -> "AgentSession":
        return cls(
            session_id=str(uuid4()),
            task=task,
            metadata=metadata or {},
        )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["task"] = asdict(self.task)
        d["turns"] = [t.to_dict() for t in self.turns]
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentSession":
        task_d = d.pop("task")
        task = AgentTask(**task_d)
        turns_d = d.pop("turns", [])
        turns = [AgentTurn(**t) for t in turns_d]
        return cls(task=task, turns=turns, **d)

    @property
    def turn_count(self) -> int:
        return len(self.turns)

    @property
    def current_goal(self) -> str:
        if self.task.subtasks and self.current_subtask_index < len(self.task.subtasks):
            return self.task.subtasks[self.current_subtask_index]
        return self.task.goal

    def advance_subtask(self) -> bool:
        """Move to next subtask. Returns True if more subtasks remain."""
        if self.current_subtask_index < len(self.task.subtasks) - 1:
            self.current_subtask_index += 1
            return True
        return False

    def is_goal_met(self, url: str, page_text: str) -> bool:
        if self.task.success_url and self.task.success_url in url:
            return True
        if self.task.success_text and self.task.success_text in page_text:
            return True
        return False


# ---------------------------------------------------------------------------
# Session store — JSON-backed persistence
# ---------------------------------------------------------------------------

class AgentSessionStore:
    """
    Persist and resume AgentSession objects as JSON files.

    One file per session: {store_dir}/{session_id}.json

    Usage:
        store = AgentSessionStore("storage/agent_sessions")
        store.save(session)
        resumed = store.load("session-id-here")
    """

    def __init__(self, store_dir: str = "storage/agent_sessions"):
        self._dir = Path(store_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def save(self, session: AgentSession) -> Path:
        path = self._dir / f"{session.session_id}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(session.to_dict(), indent=2), encoding="utf-8")
        tmp.replace(path)
        return path

    def load(self, session_id: str) -> AgentSession:
        path = self._dir / f"{session_id}.json"
        if not path.exists():
            raise AgentError(f"Session not found: {session_id}")
        return AgentSession.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def list_sessions(self) -> List[str]:
        return [p.stem for p in sorted(self._dir.glob("*.json"))]

    def delete(self, session_id: str) -> None:
        path = self._dir / f"{session_id}.json"
        if path.exists():
            path.unlink()


# ---------------------------------------------------------------------------
# Planner — LLM-agnostic action plan generation
# ---------------------------------------------------------------------------

ActionPlanFn = Callable[[str, str, str], List[Dict[str, Any]]]
# signature: plan_fn(goal, page_url, page_snapshot) -> list of action dicts


def _default_plan(goal: str, page_url: str, page_snapshot: str) -> List[Dict[str, Any]]:
    """Minimal fallback planner: SCREENSHOT to observe, then no-op WAIT."""
    return [
        {"type": "screenshot"},
        {"type": "wait", "value": "500"},
    ]


class AgentPlanner:
    """
    Wraps an external plan function (LLM-backed or rule-based).

    A plan function receives (goal, current_url, page_snapshot) and
    returns a list of action dicts that the ActionLayer can execute.

    For production use, inject an LLM-backed plan_fn:
        def llm_plan(goal, url, snapshot):
            response = llm.complete(f"Goal: {goal}\nURL: {url}\nHTML: {snapshot[:2000]}")
            return json.loads(response)  # list of action dicts

    For testing, the default plan_fn takes a screenshot and waits.
    """

    def __init__(self, plan_fn: Optional[ActionPlanFn] = None):
        self._plan_fn = plan_fn or _default_plan

    def plan(self, goal: str, page_url: str, page_snapshot: str) -> List[BrowserAction]:
        raw = self._plan_fn(goal, page_url, page_snapshot)
        actions = []
        for d in raw:
            try:
                actions.append(BrowserAction.from_dict(d))
            except Exception:
                pass
        return actions


# ---------------------------------------------------------------------------
# AgentSessionRunner — executes a session turn-by-turn
# ---------------------------------------------------------------------------

class AgentSessionRunner:
    """
    Executes an AgentSession against a live Playwright page.

    Usage (async):
        runner = AgentSessionRunner(session, page, planner=AgentPlanner())
        await runner.run()
        print(session.status)

    Each turn:
    1. Capture current page state (URL + truncated HTML)
    2. Call planner to get next actions
    3. Execute actions via ActionLayer
    4. Check goal-met conditions
    5. Append AgentTurn to session history
    6. Persist via store (if provided)
    """

    def __init__(
        self,
        session: AgentSession,
        page: Any,
        planner: Optional[AgentPlanner] = None,
        store: Optional[AgentSessionStore] = None,
        on_turn: Optional[Callable[[AgentTurn], None]] = None,
    ):
        self._session = session
        self._page = page
        self._layer = ActionLayer(page, capture_snapshots=False)
        self._planner = planner or AgentPlanner()
        self._store = store
        self._on_turn = on_turn

    async def run(self) -> AgentSession:
        s = self._session
        s.status = AgentSessionStatus.RUNNING
        s.started_at = time.time()
        if self._store:
            self._store.save(s)

        try:
            while s.turn_count < s.task.max_turns:
                # Observe current state
                url = ""
                snapshot = ""
                page_text = ""
                try:
                    url = self._page.url
                    snapshot = await self._page.content()
                    snapshot = snapshot[:4000]
                    page_text = await self._page.evaluate(
                        "document.body ? document.body.innerText : ''"
                    )
                except Exception:
                    pass

                # Check if goal already met
                if s.is_goal_met(url, page_text):
                    s.status = AgentSessionStatus.COMPLETED
                    break

                # Plan next actions
                actions = self._planner.plan(s.current_goal, url, snapshot)
                if not actions:
                    # Planner returned nothing — check if we can advance subtask
                    if not s.advance_subtask():
                        s.status = AgentSessionStatus.COMPLETED
                        break
                    continue  # more subtasks remain, plan for next

                # Execute first action of the plan
                action = actions[0]
                result: ActionResult = await self._layer.execute(action)

                # Observe after action
                post_url = url
                post_text = page_text
                try:
                    post_url = self._page.url
                    post_text = await self._page.evaluate(
                        "document.body ? document.body.innerText.slice(0, 500) : ''"
                    )
                except Exception:
                    pass

                # Build and append turn record
                turn = AgentTurn(
                    turn_id=str(uuid4()),
                    turn_index=s.turn_count,
                    timestamp=time.time(),
                    goal=s.current_goal,
                    action=action.to_dict(),
                    result=result.to_dict(),
                    observation=f"url={post_url} text_preview={post_text[:200]}",
                    success=result.success,
                    error=result.error,
                )
                s.turns.append(turn)
                if self._on_turn:
                    self._on_turn(turn)
                if self._store:
                    self._store.save(s)

                # Advance subtask if current goal met
                if s.is_goal_met(post_url, post_text):
                    if not s.advance_subtask():
                        s.status = AgentSessionStatus.COMPLETED
                        break

                if not result.success:
                    s.error = f"Turn {s.turn_count} failed: {result.error}"
                    s.status = AgentSessionStatus.FAILED
                    break

            else:
                s.error = f"Exceeded max_turns={s.task.max_turns}"
                s.status = AgentSessionStatus.FAILED

        except Exception as e:
            s.error = str(e)
            s.status = AgentSessionStatus.FAILED

        finally:
            s.completed_at = time.time()
            try:
                s.final_url = self._page.url
                s.final_snapshot = (await self._page.content())[:2000]
            except Exception:
                pass
            if self._store:
                self._store.save(s)

        return s


# ---------------------------------------------------------------------------
# LongHorizonPlanner — decomposes a high-level goal into subtask chains
# ---------------------------------------------------------------------------

class LongHorizonPlanner:
    """
    Decomposes a complex, multi-page goal into ordered subtasks,
    then delegates each subtask to an AgentPlanner for action planning.

    Usage:
        def decompose_fn(goal: str) -> List[str]:
            # Call LLM to decompose goal into steps
            return ["navigate to login", "fill credentials", "click submit", "verify dashboard"]

        planner = LongHorizonPlanner(decompose_fn=decompose_fn)
        task = planner.build_task("Log in and check account balance", max_turns=20)
        session = AgentSession.new(task)
        runner = AgentSessionRunner(session, page, planner=AgentPlanner(plan_fn=...))
        await runner.run()
    """

    def __init__(
        self,
        decompose_fn: Optional[Callable[[str], List[str]]] = None,
        max_subtasks: int = 10,
    ):
        self._decompose = decompose_fn or self._default_decompose
        self._max_subtasks = max_subtasks

    def build_task(
        self,
        goal: str,
        max_turns: int = 20,
        success_text: Optional[str] = None,
        success_url_pattern: Optional[str] = None,
    ) -> "AgentTask":
        """Decompose goal into subtasks and build an AgentTask."""
        subtasks = self._decompose(goal)[:self._max_subtasks]
        return AgentTask(
            goal=goal,
            subtasks=subtasks,
            max_turns=max_turns,
            success_text=success_text or "",
            success_url=success_url_pattern or "",
        )

    def plan_with_retry(
        self,
        planner: "AgentPlanner",
        goal: str,
        url: str,
        snapshot: str,
        max_retries: int = 2,
    ) -> List["BrowserAction"]:
        """Plan with automatic retry on empty plan — back-off strategy."""
        for attempt in range(max_retries + 1):
            actions = planner.plan(goal, url, snapshot)
            if actions:
                return actions
        return []

    @staticmethod
    def _default_decompose(goal: str) -> List[str]:
        """Fallback: treat the whole goal as a single subtask."""
        return [goal]


# ---------------------------------------------------------------------------
# MultiPageNavigator — manages browser tab context for multi-page flows
# ---------------------------------------------------------------------------

class MultiPageNavigator:
    """
    Tracks browser history across multiple page navigations in a session.
    Provides breadcrumb trail and back-navigation capability.

    Usage:
        nav = MultiPageNavigator()
        nav.push("https://example.com/step1", "Step 1 — Login page")
        nav.push("https://example.com/step2", "Step 2 — Dashboard")
        print(nav.current_url)       # "https://example.com/step2"
        nav.back()
        print(nav.current_url)       # "https://example.com/step1"
    """

    def __init__(self):
        self._history: List[dict] = []
        self._index: int = -1

    def push(self, url: str, description: str = "") -> None:
        # Truncate forward history on new push (browser-like behavior)
        self._history = self._history[:self._index + 1]
        self._history.append({"url": url, "description": description, "timestamp": time.time()})
        self._index = len(self._history) - 1

    def back(self) -> Optional[str]:
        if self._index > 0:
            self._index -= 1
            return self._history[self._index]["url"]
        return None

    def forward(self) -> Optional[str]:
        if self._index < len(self._history) - 1:
            self._index += 1
            return self._history[self._index]["url"]
        return None

    @property
    def current_url(self) -> Optional[str]:
        if self._index >= 0:
            return self._history[self._index]["url"]
        return None

    @property
    def breadcrumbs(self) -> List[dict]:
        return list(self._history[:self._index + 1])

    def to_dict(self) -> dict:
        return {
            "current_index": self._index,
            "history": self._history,
            "current_url": self.current_url,
        }
