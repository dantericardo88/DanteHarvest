"""Tests for CDPInteractionRecorder and InteractionEvent.

Covers:
- InteractionEvent field structure
- interaction_events_to_steps conversion logic (click, keydown/Enter→submit,
  input collapse, navigate, scroll collapse)
- CDPInteractionRecorder behaviour with a mock page
- DemoRecorder integration (start/stop_interaction_recording methods present)
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from harvest_observe.capture.demo_recorder import (
    CDPInteractionRecorder,
    DemoRecorder,
    InteractionEvent,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ev(**kwargs) -> InteractionEvent:
    """Build an InteractionEvent with sensible defaults."""
    defaults = dict(
        event_type="click",
        selector="#btn",
        text="",
        x=None,
        y=None,
        key=None,
        value=None,
        url=None,
        ts=1000.0,
        session_id="s1",
    )
    defaults.update(kwargs)
    return InteractionEvent(**defaults)


# ---------------------------------------------------------------------------
# InteractionEvent fields
# ---------------------------------------------------------------------------

def test_interaction_event_fields():
    ev = InteractionEvent(
        event_type="click",
        selector="#btn",
        text="Submit",
        x=10.0,
        y=20.0,
        key=None,
        value=None,
        url=None,
        ts=1000.0,
        session_id="s1",
    )
    assert ev.event_type == "click"
    assert ev.selector == "#btn"
    assert ev.text == "Submit"
    assert ev.x == 10.0
    assert ev.y == 20.0
    assert ev.key is None
    assert ev.value is None
    assert ev.url is None
    assert ev.ts == 1000.0
    assert ev.session_id == "s1"


def test_interaction_event_from_raw_click():
    raw = {"type": "click", "selector": "#foo", "text": "Go", "x": 5.0, "y": 6.0, "ts": 999.0}
    ev = InteractionEvent.from_raw(raw, session_id="sess")
    assert ev.event_type == "click"
    assert ev.selector == "#foo"
    assert ev.text == "Go"
    assert ev.x == 5.0
    assert ev.ts == 999.0
    assert ev.session_id == "sess"


def test_interaction_event_from_raw_submit_uses_action():
    raw = {"type": "submit", "selector": "form", "action": "https://ex.com/go", "ts": 1.0}
    ev = InteractionEvent.from_raw(raw, session_id="s")
    assert ev.url == "https://ex.com/go"


# ---------------------------------------------------------------------------
# interaction_events_to_steps — click
# ---------------------------------------------------------------------------

def test_events_to_steps_click():
    events = [_ev(event_type="click", selector="#btn", text="OK", x=5.0, y=8.0)]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    s = steps[0]
    assert s["type"] == "click"
    assert s["selector"] == "#btn"
    assert s["text"] == "OK"
    assert s["x"] == 5.0
    assert s["y"] == 8.0


# ---------------------------------------------------------------------------
# interaction_events_to_steps — keydown Enter → submit
# ---------------------------------------------------------------------------

def test_events_to_steps_keydown_submit():
    events = [_ev(event_type="keydown", key="Enter", selector="input#q")]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    s = steps[0]
    assert s["type"] == "submit"
    assert s["selector"] == "input#q"
    assert s["key"] == "Enter"


def test_events_to_steps_keydown_non_enter():
    events = [_ev(event_type="keydown", key="Tab", selector="#field")]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert steps[0]["type"] == "keydown"
    assert steps[0]["key"] == "Tab"


# ---------------------------------------------------------------------------
# interaction_events_to_steps — input collapse → type
# ---------------------------------------------------------------------------

def test_events_to_steps_input():
    events = [_ev(event_type="input", selector="#name", value="Alice")]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    s = steps[0]
    assert s["type"] == "type"
    assert s["selector"] == "#name"
    assert s["value"] == "Alice"


def test_interaction_events_to_steps_groups_typing():
    """Consecutive input events on the same selector collapse to one type step."""
    events = [
        _ev(event_type="input", selector="#q", value="h", ts=1.0),
        _ev(event_type="input", selector="#q", value="he", ts=1.1),
        _ev(event_type="input", selector="#q", value="hel", ts=1.2),
        _ev(event_type="input", selector="#q", value="hell", ts=1.3),
        _ev(event_type="input", selector="#q", value="hello", ts=1.4),
    ]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    assert steps[0]["type"] == "type"
    assert steps[0]["value"] == "hello"


def test_interaction_events_to_steps_groups_typing_different_selectors():
    """Input events on different selectors produce separate type steps."""
    events = [
        _ev(event_type="input", selector="#first", value="Alice", ts=1.0),
        _ev(event_type="input", selector="#last", value="Smith", ts=1.1),
    ]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 2
    assert steps[0]["selector"] == "#first"
    assert steps[1]["selector"] == "#last"


# ---------------------------------------------------------------------------
# interaction_events_to_steps — navigate
# ---------------------------------------------------------------------------

def test_events_to_steps_navigate():
    events = [_ev(event_type="navigate", url="https://example.com", selector="")]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    s = steps[0]
    assert s["type"] == "navigate"
    assert s["url"] == "https://example.com"


# ---------------------------------------------------------------------------
# interaction_events_to_steps — scroll collapse
# ---------------------------------------------------------------------------

def test_events_to_steps_scroll():
    events = [_ev(event_type="scroll", x=0.0, y=500.0, selector="")]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    s = steps[0]
    assert s["type"] == "scroll"
    assert s["scrollY"] == 500.0


def test_interaction_events_to_steps_deduplicates_scroll():
    """Consecutive same-direction scroll events within thresholds collapse to one step."""
    events = [
        _ev(event_type="scroll", x=0.0, y=10.0, ts=1.0),
        _ev(event_type="scroll", x=0.0, y=20.0, ts=1.2),
        _ev(event_type="scroll", x=0.0, y=15.0, ts=1.4),
    ]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 1
    assert steps[0]["scrollY"] == 15.0


def test_scroll_not_collapsed_across_other_events():
    """Scroll events separated by another event type are NOT collapsed."""
    events = [
        _ev(event_type="scroll", x=0.0, y=100.0, ts=1.0),
        _ev(event_type="click", selector="#btn", ts=1.5),
        _ev(event_type="scroll", x=0.0, y=400.0, ts=2.0),
    ]
    steps = DemoRecorder.interaction_events_to_steps(events)
    assert len(steps) == 3
    assert steps[0]["type"] == "scroll"
    assert steps[1]["type"] == "click"
    assert steps[2]["type"] == "scroll"


# ---------------------------------------------------------------------------
# DemoRecorder integration
# ---------------------------------------------------------------------------

def test_demo_recorder_has_start_stop_interaction():
    recorder = DemoRecorder(goal="test")
    assert callable(getattr(recorder, "start_interaction_recording", None))
    assert callable(getattr(recorder, "stop_interaction_recording", None))


def test_demo_recorder_interaction_events_to_steps():
    """Mix of event types produces the correct step sequence."""
    events = [
        _ev(event_type="navigate", url="https://example.com", selector="", ts=1.0),
        _ev(event_type="click", selector="#login", text="Login", x=50.0, y=10.0, ts=2.0),
        _ev(event_type="input", selector="#user", value="alice", ts=3.0),
        _ev(event_type="keydown", key="Enter", selector="#user", ts=4.0),
        _ev(event_type="scroll", x=0.0, y=200.0, ts=5.0),
    ]
    steps = DemoRecorder.interaction_events_to_steps(events)
    types = [s["type"] for s in steps]
    assert types == ["navigate", "click", "type", "submit", "scroll"]


# ---------------------------------------------------------------------------
# CDPInteractionRecorder — no page (None)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cdp_recorder_attach_no_page_no_crash():
    """attach() with page=None raises RuntimeError (not unhandled crash)."""
    rec = CDPInteractionRecorder(page=None, session_id="s1")
    with pytest.raises(RuntimeError, match="No page attached"):
        await rec.attach()


@pytest.mark.asyncio
async def test_cdp_recorder_poll_no_page_raises():
    rec = CDPInteractionRecorder(page=None, session_id="s1")
    with pytest.raises(RuntimeError, match="No page attached"):
        await rec.poll()


@pytest.mark.asyncio
async def test_cdp_recorder_detach_no_page_raises():
    rec = CDPInteractionRecorder(page=None, session_id="s1")
    with pytest.raises(RuntimeError, match="No page attached"):
        await rec.detach()


# ---------------------------------------------------------------------------
# CDPInteractionRecorder — mock page
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cdp_recorder_poll_returns_list():
    """poll() with a mock page that returns raw events → list of InteractionEvent."""
    page = AsyncMock()
    page.evaluate.return_value = [
        {"type": "click", "selector": "#btn", "text": "Submit", "ts": 1000.0}
    ]
    rec = CDPInteractionRecorder(page=page, session_id="s1")
    # attach first (inject script)
    page.evaluate.return_value = None  # inject returns nothing
    await rec.attach()
    # now poll
    page.evaluate.return_value = [
        {"type": "click", "selector": "#btn", "text": "Submit", "ts": 1000.0}
    ]
    events = await rec.poll()
    assert isinstance(events, list)
    assert len(events) == 1
    assert isinstance(events[0], InteractionEvent)
    assert events[0].event_type == "click"
    assert events[0].selector == "#btn"


@pytest.mark.asyncio
async def test_cdp_recorder_get_events_empty_initially():
    """get_events() returns empty list before any poll."""
    rec = CDPInteractionRecorder(page=AsyncMock(), session_id="s1")
    assert rec.get_events() == []


@pytest.mark.asyncio
async def test_cdp_recorder_poll_empty_buffer():
    """poll() with empty page buffer returns empty list, no crash."""
    page = AsyncMock()
    page.evaluate.return_value = []
    rec = CDPInteractionRecorder(page=page, session_id="s1")
    events = await rec.poll()
    assert events == []


@pytest.mark.asyncio
async def test_cdp_recorder_detach_returns_all_events():
    """detach() returns cumulative events (poll + final drain)."""
    page = AsyncMock()
    rec = CDPInteractionRecorder(page=page, session_id="sess")

    # Simulate first poll accumulates one event
    page.evaluate.return_value = [
        {"type": "scroll", "scrollX": 0, "scrollY": 100, "selector": "", "ts": 1.0}
    ]
    await rec.poll()

    # detach drains one more event
    page.evaluate.return_value = [
        {"type": "click", "selector": "#x", "text": "", "ts": 2.0}
    ]
    all_events = await rec.detach()
    assert len(all_events) == 2
    assert all_events[0].event_type == "scroll"
    assert all_events[1].event_type == "click"


@pytest.mark.asyncio
async def test_cdp_recorder_get_events_reflects_polls():
    """get_events() returns all events accumulated across multiple polls."""
    page = AsyncMock()
    rec = CDPInteractionRecorder(page=page, session_id="s")

    page.evaluate.return_value = [{"type": "click", "selector": "#a", "ts": 1.0}]
    await rec.poll()
    page.evaluate.return_value = [{"type": "keydown", "key": "Enter", "selector": "#b", "ts": 2.0}]
    await rec.poll()

    evts = rec.get_events()
    assert len(evts) == 2
    assert evts[0].event_type == "click"
    assert evts[1].event_type == "keydown"


# ---------------------------------------------------------------------------
# DemoRecorder.start/stop_interaction_recording integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_demo_recorder_start_stop_returns_events():
    """start_interaction_recording attaches; stop returns event list."""
    page = AsyncMock()
    page.evaluate.return_value = None  # attach call

    recorder = DemoRecorder(goal="cdp test")
    recorder.start()
    cdp = await recorder.start_interaction_recording(page)
    assert isinstance(cdp, CDPInteractionRecorder)

    # Simulate stop draining one event
    page.evaluate.return_value = [
        {"type": "click", "selector": "#go", "text": "Go", "ts": 5.0}
    ]
    events = await recorder.stop_interaction_recording()
    assert isinstance(events, list)
    assert len(events) == 1
    assert events[0].event_type == "click"


@pytest.mark.asyncio
async def test_demo_recorder_stop_without_start_returns_empty():
    """stop_interaction_recording without prior start returns empty list."""
    recorder = DemoRecorder(goal="no cdp")
    events = await recorder.stop_interaction_recording()
    assert events == []
