"""Tests for DemoRecorder — human demo recording and WorkflowPack conversion."""
import json
import time
import pytest
from harvest_observe.capture.demo_recorder import (
    DemoRecorder,
    DemoRecording,
    DemoEvent,
    DemoEventType,
    DemoToPackConverter,
    DemoError,
)


# ---------------------------------------------------------------------------
# DemoEvent
# ---------------------------------------------------------------------------

def test_demo_event_roundtrip():
    ev = DemoEvent(
        event_type=DemoEventType.CLICK,
        timestamp=1234567890.0,
        selector="#btn",
        annotation="click submit",
    )
    d = ev.to_dict()
    assert d["event_type"] == "click"
    restored = DemoEvent.from_dict(d)
    assert restored.event_type == DemoEventType.CLICK
    assert restored.selector == "#btn"
    assert restored.annotation == "click submit"


def test_demo_event_all_types_roundtrip():
    for et in DemoEventType:
        ev = DemoEvent(event_type=et, timestamp=0.0)
        d = ev.to_dict()
        restored = DemoEvent.from_dict(d)
        assert restored.event_type == et


# ---------------------------------------------------------------------------
# DemoRecording
# ---------------------------------------------------------------------------

def test_demo_recording_roundtrip():
    rec = DemoRecording(
        recording_id="rec-001",
        goal="Submit form",
        domain="ecommerce",
    )
    rec.events.append(DemoEvent(event_type=DemoEventType.NAVIGATE, timestamp=1.0, url="https://example.com"))
    d = rec.to_dict()
    assert d["recording_id"] == "rec-001"
    assert len(d["events"]) == 1
    restored = DemoRecording.from_dict(d)
    assert restored.goal == "Submit form"
    assert restored.events[0].event_type == DemoEventType.NAVIGATE


# ---------------------------------------------------------------------------
# DemoRecorder — imperative API
# ---------------------------------------------------------------------------

def test_recorder_basic_flow():
    recorder = DemoRecorder(goal="Search for products")
    recorder.start()
    recorder.record_navigate("https://example.com")
    recorder.record_fill("#search", "python")
    recorder.record_click("button.search")
    recording = recorder.stop()

    assert recording.goal == "Search for products"
    assert len(recording.events) == 3
    assert recording.events[0].event_type == DemoEventType.NAVIGATE
    assert recording.events[1].event_type == DemoEventType.FILL
    assert recording.events[2].event_type == DemoEventType.CLICK
    assert recording.completed_at is not None


def test_recorder_all_event_types():
    recorder = DemoRecorder(goal="Full coverage")
    recorder.start()
    recorder.record_navigate("https://example.com", annotation="nav")
    recorder.record_click("#id", annotation="click")
    recorder.record_fill("#input", "value", annotation="fill")
    recorder.record_type("#ta", "typed text")
    recorder.record_scroll(300)
    recorder.record_hover(".menu")
    recorder.record_select("#dropdown", "option1")
    recorder.record_screenshot(path="/tmp/shot.png")
    recorder.record_assert("#el", expected_text="Hello")
    recorder.record_wait(500)
    recorder.annotate("step annotation")
    recording = recorder.stop()

    types = [e.event_type for e in recording.events]
    assert DemoEventType.NAVIGATE in types
    assert DemoEventType.CLICK in types
    assert DemoEventType.FILL in types
    assert DemoEventType.TYPE in types
    assert DemoEventType.SCROLL in types
    assert DemoEventType.HOVER in types
    assert DemoEventType.SELECT in types
    assert DemoEventType.SCREENSHOT in types
    assert DemoEventType.ASSERT in types
    assert DemoEventType.WAIT in types
    assert DemoEventType.ANNOTATE in types


def test_recorder_generates_unique_recording_ids():
    r1 = DemoRecorder(goal="A")
    r2 = DemoRecorder(goal="B")
    r1.start(); r2.start()
    assert r1.stop().recording_id != r2.stop().recording_id


def test_recorder_custom_recording_id():
    recorder = DemoRecorder(goal="G", recording_id="fixed-id")
    recorder.start()
    rec = recorder.stop()
    assert rec.recording_id == "fixed-id"


def test_recorder_save_load(tmp_path):
    recorder = DemoRecorder(goal="Persistence test")
    recorder.start()
    recorder.record_navigate("https://example.com")
    recorder.record_click("#btn")
    recorder.stop()

    path = str(tmp_path / "recording.json")
    recorder.save(path)

    loaded = DemoRecorder.load(path)
    assert loaded.goal == "Persistence test"
    assert len(loaded.events) == 2
    assert loaded.events[0].url == "https://example.com"


def test_recorder_save_atomic_on_tmp_path(tmp_path):
    recorder = DemoRecorder(goal="Atomic write")
    recorder.start()
    recorder.record_click("#x")
    recorder.stop()
    path = str(tmp_path / "subdir" / "rec.json")
    saved = recorder.save(path)
    assert saved.exists()
    assert not saved.with_suffix(".json.tmp").exists()


# ---------------------------------------------------------------------------
# DemoToPackConverter
# ---------------------------------------------------------------------------

def test_converter_basic():
    recorder = DemoRecorder(goal="Checkout flow", domain="ecommerce")
    recorder.start()
    recorder.record_navigate("https://shop.com", annotation="Go to shop")
    recorder.record_click("#add-to-cart", annotation="Add item")
    recorder.record_click("#checkout")
    recording = recorder.stop()

    converter = DemoToPackConverter()
    pack = converter.convert(recording)

    assert pack["goal"] == "Checkout flow"
    assert pack["source_domain"] == "ecommerce"
    assert pack["pack_type"] == "workflow"
    assert len(pack["steps"]) == 3
    assert pack["steps"][0]["action"] == "navigate:https://shop.com"
    assert pack["steps"][0]["description"] == "Go to shop"


def test_converter_action_strings():
    recorder = DemoRecorder(goal="Action coverage")
    recorder.start()
    recorder.record_navigate("https://ex.com")
    recorder.record_click("#btn")
    recorder.record_fill("#input", "value")
    recorder.record_type("#ta", "text")
    recorder.record_select("#sel", "opt")
    recorder.record_scroll(400)
    recorder.record_wait(200)
    recorder.record_assert("#el", "Hello")
    recorder.record_hover(".nav")
    recorder.record_screenshot()
    recorder.annotate("note")
    recording = recorder.stop()

    converter = DemoToPackConverter()
    pack = converter.convert(recording)
    actions = [s["action"] for s in pack["steps"]]

    assert "navigate:https://ex.com" in actions
    assert "click:#btn" in actions
    assert "fill:#input=value" in actions
    assert "type:#ta=text" in actions
    assert "select:#sel=opt" in actions
    assert "scroll:400" in actions
    assert "wait:200" in actions
    assert "assert:#el=Hello" in actions
    assert "hover:.nav" in actions
    assert "screenshot" in actions
    assert "annotate" in actions  # annotate events fall through to event type name


def test_converter_empty_recording_raises():
    recorder = DemoRecorder(goal="Empty")
    recorder.start()
    recording = recorder.stop()

    converter = DemoToPackConverter()
    with pytest.raises(DemoError):
        converter.convert(recording)


def test_converter_metadata():
    recorder = DemoRecorder(goal="Meta test")
    recorder.start()
    recorder.record_click("#x")
    recording = recorder.stop()

    converter = DemoToPackConverter()
    pack = converter.convert(recording)

    assert pack["metadata"]["generated_from"] == "demo_recording"
    assert pack["metadata"]["event_count"] == 1
    assert "recorded_at" in pack["metadata"]
    assert pack["recording_id"] == recording.recording_id


def test_converter_step_ids_sequential():
    recorder = DemoRecorder(goal="Step IDs")
    recorder.start()
    for _ in range(5):
        recorder.record_click("#x")
    recording = recorder.stop()

    converter = DemoToPackConverter()
    pack = converter.convert(recording)
    step_ids = [s["step_id"] for s in pack["steps"]]
    assert step_ids == ["step_000", "step_001", "step_002", "step_003", "step_004"]


@pytest.mark.asyncio
async def test_recorder_attach_playwright_page():
    """Verify attach_playwright_page wires request listener without error."""
    from unittest.mock import MagicMock, AsyncMock

    recorder = DemoRecorder(goal="CDP test")
    recorder.start()

    page = MagicMock()
    page.on = MagicMock()
    await recorder.attach_playwright_page(page)

    args = page.on.call_args
    assert args[0][0] == "request"
    assert callable(args[0][1])
