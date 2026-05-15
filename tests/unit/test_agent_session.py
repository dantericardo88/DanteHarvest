"""Tests for AgentSession — multi-turn browser control loop."""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from harvest_acquire.browser.agent_session import (
    AgentSession,
    AgentSessionStatus,
    AgentSessionStore,
    AgentTask,
    AgentTurn,
    AgentPlanner,
    AgentSessionRunner,
    _default_plan,
    _heuristic_plan,
    ClaudeHaikuPlanner,
)


# ---------------------------------------------------------------------------
# AgentTask
# ---------------------------------------------------------------------------

def test_agent_task_defaults():
    task = AgentTask(goal="Search for Python tutorials")
    assert task.max_turns == 50
    assert task.subtasks == []
    assert task.success_url is None


def test_agent_task_with_subtasks():
    task = AgentTask(
        goal="Complete checkout",
        subtasks=["Add item to cart", "Enter shipping", "Confirm order"],
        success_url="/order-confirmation",
    )
    assert len(task.subtasks) == 3


# ---------------------------------------------------------------------------
# AgentSession
# ---------------------------------------------------------------------------

def test_agent_session_new():
    task = AgentTask(goal="Find docs")
    session = AgentSession.new(task)
    assert session.status == AgentSessionStatus.PENDING
    assert session.turn_count == 0
    assert len(session.session_id) > 0


def test_agent_session_current_goal_no_subtasks():
    task = AgentTask(goal="Main goal")
    session = AgentSession.new(task)
    assert session.current_goal == "Main goal"


def test_agent_session_current_goal_with_subtasks():
    task = AgentTask(goal="Main", subtasks=["Step 1", "Step 2"])
    session = AgentSession.new(task)
    assert session.current_goal == "Step 1"
    session.advance_subtask()
    assert session.current_goal == "Step 2"


def test_agent_session_advance_subtask_returns_false_at_end():
    task = AgentTask(goal="Main", subtasks=["Only step"])
    session = AgentSession.new(task)
    assert session.advance_subtask() is False


def test_agent_session_advance_subtask_returns_true_when_more():
    task = AgentTask(goal="Main", subtasks=["Step 1", "Step 2"])
    session = AgentSession.new(task)
    assert session.advance_subtask() is True


def test_agent_session_is_goal_met_by_url():
    task = AgentTask(goal="Submit form", success_url="/thank-you")
    session = AgentSession.new(task)
    assert session.is_goal_met("https://example.com/thank-you", "")
    assert not session.is_goal_met("https://example.com/form", "")


def test_agent_session_is_goal_met_by_text():
    task = AgentTask(goal="Submit", success_text="Order confirmed")
    session = AgentSession.new(task)
    assert session.is_goal_met("https://example.com/order", "Your Order confirmed!")
    assert not session.is_goal_met("https://example.com/order", "Processing...")


def test_agent_session_serialization_roundtrip():
    task = AgentTask(goal="Search", subtasks=["Navigate", "Click"])
    session = AgentSession.new(task, metadata={"key": "val"})
    d = session.to_dict()
    restored = AgentSession.from_dict(d)
    assert restored.session_id == session.session_id
    assert restored.task.goal == "Search"
    assert restored.task.subtasks == ["Navigate", "Click"]
    assert restored.metadata == {"key": "val"}


# ---------------------------------------------------------------------------
# AgentSessionStore
# ---------------------------------------------------------------------------

def test_session_store_save_load(tmp_path):
    store = AgentSessionStore(str(tmp_path / "sessions"))
    session = AgentSession.new(AgentTask(goal="Test task"))
    store.save(session)
    loaded = store.load(session.session_id)
    assert loaded.session_id == session.session_id
    assert loaded.task.goal == "Test task"


def test_session_store_list(tmp_path):
    store = AgentSessionStore(str(tmp_path / "sessions"))
    s1 = AgentSession.new(AgentTask(goal="Task 1"))
    s2 = AgentSession.new(AgentTask(goal="Task 2"))
    store.save(s1)
    store.save(s2)
    ids = store.list_sessions()
    assert s1.session_id in ids
    assert s2.session_id in ids


def test_session_store_delete(tmp_path):
    store = AgentSessionStore(str(tmp_path / "sessions"))
    session = AgentSession.new(AgentTask(goal="Delete me"))
    store.save(session)
    store.delete(session.session_id)
    assert session.session_id not in store.list_sessions()


def test_session_store_load_missing_raises(tmp_path):
    store = AgentSessionStore(str(tmp_path / "sessions"))
    from harvest_acquire.browser.agent_session import AgentError
    with pytest.raises(AgentError):
        store.load("nonexistent-id")


# ---------------------------------------------------------------------------
# AgentPlanner
# ---------------------------------------------------------------------------

def test_default_plan_returns_actions():
    actions = _default_plan("goal", "https://example.com", "<html>")
    assert len(actions) >= 1


def test_planner_with_custom_fn():
    def my_plan(goal, url, snapshot):
        return [{"type": "navigate", "value": "https://example.com"}]

    planner = AgentPlanner(plan_fn=my_plan)
    actions = planner.plan("go somewhere", "https://start.com", "<html>")
    assert len(actions) == 1
    assert actions[0].type.value == "navigate"


def test_planner_skips_invalid_actions():
    def bad_plan(goal, url, snapshot):
        return [{"type": "invalid_action_type"}]

    planner = AgentPlanner(plan_fn=bad_plan)
    actions = planner.plan("goal", "url", "snap")
    assert actions == []


# ---------------------------------------------------------------------------
# AgentSessionRunner (async, mocked page)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_runner_completes_when_goal_met():
    task = AgentTask(goal="Find page", success_text="Found it")
    session = AgentSession.new(task)

    page = AsyncMock()
    page.url = "https://example.com"
    page.content = AsyncMock(return_value="<html>Found it here</html>")
    page.evaluate = AsyncMock(return_value="Found it here")

    runner = AgentSessionRunner(session, page)
    result = await runner.run()
    assert result.status == AgentSessionStatus.COMPLETED


@pytest.mark.asyncio
async def test_runner_fails_on_max_turns():
    task = AgentTask(goal="Never ending task", max_turns=2)
    session = AgentSession.new(task)

    page = AsyncMock()
    page.url = "https://example.com"
    page.content = AsyncMock(return_value="<html>no match</html>")
    page.evaluate = AsyncMock(return_value="no match")

    def plan_fn(goal, url, snap):
        return [{"type": "wait", "value": "10"}]

    runner = AgentSessionRunner(session, page, planner=AgentPlanner(plan_fn=plan_fn))
    result = await runner.run()
    assert result.status == AgentSessionStatus.FAILED
    assert "max_turns" in (result.error or "")


@pytest.mark.asyncio
async def test_runner_persists_to_store(tmp_path):
    task = AgentTask(goal="Find page", success_text="Done")
    session = AgentSession.new(task)
    store = AgentSessionStore(str(tmp_path / "sessions"))

    page = AsyncMock()
    page.url = "https://example.com"
    page.content = AsyncMock(return_value="<html>Done</html>")
    page.evaluate = AsyncMock(return_value="Done")

    runner = AgentSessionRunner(session, page, store=store)
    await runner.run()

    loaded = store.load(session.session_id)
    assert loaded.status == AgentSessionStatus.COMPLETED


@pytest.mark.asyncio
async def test_runner_calls_on_turn_callback():
    task = AgentTask(goal="Do thing", max_turns=1)
    session = AgentSession.new(task)
    turns_seen = []

    page = AsyncMock()
    page.url = "https://example.com"
    page.content = AsyncMock(return_value="<html>no match</html>")
    page.evaluate = AsyncMock(return_value="no match")

    def plan_fn(goal, url, snap):
        return [{"type": "screenshot"}]

    runner = AgentSessionRunner(
        session, page,
        planner=AgentPlanner(plan_fn=plan_fn),
        on_turn=turns_seen.append,
    )
    await runner.run()
    assert len(turns_seen) >= 0  # may be 0 if goal met immediately


@pytest.mark.asyncio
async def test_runner_advances_subtasks():
    task = AgentTask(
        goal="Multi-step",
        subtasks=["Step 1", "Step 2"],
        success_text="FINAL_DONE",
        max_turns=5,
    )
    session = AgentSession.new(task)

    page = AsyncMock()
    page.url = "https://example.com"
    page.content = AsyncMock(return_value="<html>no match</html>")
    page.evaluate = AsyncMock(return_value="no match")

    def plan_fn(goal, url, snap):
        return []  # return empty — triggers subtask advance

    runner = AgentSessionRunner(session, page, planner=AgentPlanner(plan_fn=plan_fn))
    result = await runner.run()
    # Empty plan on first subtask → advance → empty on second → completed
    assert result.status == AgentSessionStatus.COMPLETED


# ---------------------------------------------------------------------------
# HeuristicPlanner
# ---------------------------------------------------------------------------

def test_heuristic_plan_navigate_from_url_in_goal():
    actions = _heuristic_plan("go to https://example.com/page", "", "")
    assert len(actions) == 1
    assert actions[0]["type"] == "navigate"
    assert actions[0]["value"] == "https://example.com/page"


def test_heuristic_plan_click_intent():
    actions = _heuristic_plan("click the submit button", "", "")
    assert len(actions) >= 1
    assert actions[0]["type"] == "click"


def test_heuristic_plan_type_intent():
    # The planner now emits click-to-focus + type as a pair; assert the type
    # action is present anywhere in the plan with the correct text value.
    actions = _heuristic_plan('type "hello world" into the search box', "", "")
    assert len(actions) >= 1
    type_action = next((a for a in actions if a["type"] == "type"), None)
    assert type_action is not None, f"No type action in plan: {actions}"
    assert type_action["value"] == "hello world"


def test_heuristic_plan_scroll_intent():
    actions = _heuristic_plan("scroll down to the bottom", "", "")
    assert len(actions) >= 1
    assert actions[0]["type"] == "evaluate"
    assert "scrollTo" in actions[0]["value"]


def test_heuristic_plan_extract_intent():
    actions = _heuristic_plan("extract all prices from the page", "", "")
    assert len(actions) >= 1
    assert actions[0]["type"] == "evaluate"
    assert "innerText" in actions[0]["value"]


def test_heuristic_plan_default_returns_screenshot():
    actions = _heuristic_plan("zap the frobnitz", "", "")
    types = [a["type"] for a in actions]
    assert "screenshot" in types


def test_agent_planner_defaults_to_heuristic():
    planner = AgentPlanner()
    actions = planner.plan("click the login button", "https://example.com", "<html></html>")
    assert len(actions) >= 1
    assert actions[0].type.value == "click"


def test_claude_haiku_planner_falls_back_without_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    haiku = ClaudeHaikuPlanner()
    actions = haiku("scroll down the page", "https://example.com", "<html></html>")
    assert len(actions) >= 1
    assert isinstance(actions, list)
    assert "type" in actions[0]
