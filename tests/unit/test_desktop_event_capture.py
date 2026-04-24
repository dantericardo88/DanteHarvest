"""Tests for DesktopEventCapture — desktop event observation."""

import json
import pytest
from pathlib import Path

from harvest_observe.desktop.event_capture import (
    DesktopEventCapture,
    DesktopEventType,
)
from harvest_core.provenance.chain_writer import ChainWriter


def make_event_file(path: Path, events: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_ingest_event_file_basic(tmp_path):
    events = [
        {"event_type": "key_press", "timestamp": 1000.0, "session_id": "sess-1", "data": {"key": "a"}},
        {"event_type": "key_release", "timestamp": 1001.0, "session_id": "sess-1", "data": {"key": "a"}},
    ]
    event_file = make_event_file(tmp_path / "events.jsonl", events)
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    capture = DesktopEventCapture(writer, session_id="sess-1", storage_root=str(tmp_path))

    session = await capture.ingest_event_file(event_file, run_id="r1")
    assert session.event_count == 2
    assert session.session_id == "sess-1"


@pytest.mark.asyncio
async def test_chain_signals_emitted(tmp_path):
    events = [
        {"event_type": "mouse_click", "timestamp": 500.0, "session_id": "sess-2",
         "data": {"x": 100, "y": 200, "button": "Button.left", "pressed": True}},
    ]
    event_file = make_event_file(tmp_path / "events.jsonl", events)
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    capture = DesktopEventCapture(writer, session_id="sess-2", storage_root=str(tmp_path))

    await capture.ingest_event_file(event_file, run_id="r1")
    entries = writer.read_all()
    signals = [e.signal for e in entries]
    assert "desktop.capture_started" in signals
    assert "desktop.capture_completed" in signals


@pytest.mark.asyncio
async def test_missing_file_raises(tmp_path):
    from harvest_observe.desktop.event_capture import ObservationError
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    capture = DesktopEventCapture(writer, session_id="sess-3", storage_root=str(tmp_path))

    with pytest.raises(ObservationError, match="not found"):
        await capture.ingest_event_file("/nonexistent/events.jsonl", run_id="r1")


@pytest.mark.asyncio
async def test_events_stored_locally(tmp_path):
    events = [
        {"event_type": "key_press", "timestamp": 1000.0, "session_id": "sess-4", "data": {}},
    ]
    event_file = make_event_file(tmp_path / "events.jsonl", events)
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    capture = DesktopEventCapture(writer, session_id="sess-4", storage_root=str(tmp_path))

    session = await capture.ingest_event_file(event_file, run_id="r1")
    assert Path(session.output_path).exists()


@pytest.mark.asyncio
async def test_malformed_lines_skipped(tmp_path):
    content = (
        '{"event_type": "key_press", "timestamp": 1.0, "session_id": "s1", "data": {}}\n'
        'not-valid-json\n'
        '{"event_type": "key_release", "timestamp": 2.0, "session_id": "s1", "data": {}}\n'
    )
    event_file = tmp_path / "events.jsonl"
    event_file.write_text(content, encoding="utf-8")
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    capture = DesktopEventCapture(writer, session_id="s1", storage_root=str(tmp_path))

    session = await capture.ingest_event_file(event_file, run_id="r1")
    assert session.event_count == 2


def test_event_type_enum_values():
    assert DesktopEventType.KEY_PRESS == "key_press"
    assert DesktopEventType.MOUSE_CLICK == "mouse_click"
    assert DesktopEventType.WINDOW_FOCUS == "window_focus"
