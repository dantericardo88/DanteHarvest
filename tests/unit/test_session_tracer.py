"""
Phase 4 — SessionTracer and ReplayHarness trace integration tests.

Verifies:
1. SessionTracer records events and saves JSONL
2. SessionTracer handles screenshots
3. ReplayHarness.tracer param wires in correctly
4. ReplayReport.trace_path set after replay with tracer
5. PlaywrightTraceCapture context manager lifecycle
6. server.py has /api/replays/{replay_id}/trace endpoint
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
import pytest

from harvest_observe.browser_session.session_tracer import SessionTracer, PlaywrightTraceCapture


# ---------------------------------------------------------------------------
# SessionTracer unit tests
# ---------------------------------------------------------------------------

def test_session_tracer_start_returns_id(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tid = tracer.start()
    assert tid
    assert tracer.trajectory_id == tid


def test_session_tracer_start_custom_id(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tid = tracer.start("my-trajectory")
    assert tid == "my-trajectory"


def test_session_tracer_record_event(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tracer.start()
    tracer.record("step.started", {"action": "navigate", "url": "https://x.com"})
    assert len(tracer.events) == 1
    assert tracer.events[0].event_name == "step.started"


def test_session_tracer_multiple_events(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tracer.start()
    for i in range(5):
        tracer.record(f"event.{i}", {"i": i})
    assert len(tracer.events) == 5


def test_session_tracer_save_writes_jsonl(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tracer.start("t-001")
    tracer.record("step.started", {"action": "navigate"})
    tracer.record("step.done", {"result": "ok"})
    path = tracer.save()
    assert path.exists()
    lines = path.read_text(encoding="utf-8").strip().split("\n")
    # First line: header, then one line per event
    assert len(lines) == 3
    header = json.loads(lines[0])
    assert header["trajectory_id"] == "t-001"
    assert header["event_count"] == 2


def test_session_tracer_save_without_start(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    # save() must not raise even without start()
    path = tracer.save()
    assert path is not None


def test_session_tracer_record_screenshot(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tracer.start("t-screenshot")
    fake_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
    tracer.record("screenshot", {"step": "login"}, screenshot=fake_png)
    assert tracer.events[0].screenshot_path is not None
    assert Path(tracer.events[0].screenshot_path).exists()


def test_session_tracer_get_record(tmp_path):
    tracer = SessionTracer(trace_dir=str(tmp_path))
    tracer.start("t-record")
    record = tracer.get_record()
    assert record is not None
    assert record.trajectory_id == "t-record"


# ---------------------------------------------------------------------------
# ReplayHarness + tracer integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_harness_accepts_tracer(tmp_path):
    from harvest_index.registry.replay_harness import ReplayHarness
    tracer = SessionTracer(trace_dir=str(tmp_path))
    harness = ReplayHarness(tracer=tracer)
    assert harness.tracer is tracer


@pytest.mark.asyncio
async def test_replay_harness_sets_trace_path(tmp_path):
    from harvest_index.registry.replay_harness import ReplayHarness
    from harvest_distill.packs.pack_schemas import WorkflowPack, PackStep as WorkflowStep

    tracer = SessionTracer(trace_dir=str(tmp_path))
    harness = ReplayHarness(tracer=tracer)

    pack = WorkflowPack(pack_id="p1", title="T", goal="G", steps=[
        WorkflowStep(id="s1", action="navigate url=https://example.com"),
    ])
    report = await harness.replay(pack=pack, run_id="run-001")
    assert report.trace_path is not None
    assert Path(report.trace_path).exists()


@pytest.mark.asyncio
async def test_replay_harness_tracer_records_steps(tmp_path):
    from harvest_index.registry.replay_harness import ReplayHarness
    from harvest_distill.packs.pack_schemas import WorkflowPack, PackStep as WorkflowStep

    tracer = SessionTracer(trace_dir=str(tmp_path))
    harness = ReplayHarness(tracer=tracer)

    pack = WorkflowPack(pack_id="p2", title="T", goal="G", steps=[
        WorkflowStep(id="s1", action="click selector=#btn"),
        WorkflowStep(id="s2", action="navigate url=https://example.com"),
    ])
    await harness.replay(pack=pack, run_id="run-002")
    event_names = [e.event_name for e in tracer.events]
    assert "replay.started" in event_names
    assert "step.executed" in event_names
    assert "replay.completed" in event_names


# ---------------------------------------------------------------------------
# PlaywrightTraceCapture lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trace_capture_starts_and_stops():
    ctx = MagicMock()
    ctx.tracing.start = AsyncMock()
    ctx.tracing.stop = AsyncMock()

    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tracer = SessionTracer(trace_dir=td)
        tracer.start("cap-001")

        cap = PlaywrightTraceCapture(ctx, tracer, save_on_success=True)
        async with cap:
            pass  # success path

        ctx.tracing.start.assert_called_once_with(screenshots=True, snapshots=True)
        ctx.tracing.stop.assert_called_once()


@pytest.mark.asyncio
async def test_trace_capture_saves_on_failure():
    ctx = MagicMock()
    ctx.tracing.start = AsyncMock()
    ctx.tracing.stop = AsyncMock()

    import tempfile
    with tempfile.TemporaryDirectory() as td:
        tracer = SessionTracer(trace_dir=td)
        tracer.start("cap-fail")

        cap = PlaywrightTraceCapture(ctx, tracer, save_on_success=False)
        try:
            async with cap:
                raise RuntimeError("step failed")
        except RuntimeError:
            pass

        ctx.tracing.stop.assert_called_once()
        assert cap._failed is True


# ---------------------------------------------------------------------------
# server.py has trace endpoint
# ---------------------------------------------------------------------------

def test_server_has_trace_endpoint():
    server_src = Path("harvest_ui/reviewer/server.py").read_text(encoding="utf-8")
    assert "/trace" in server_src
    assert "replay_id" in server_src
    assert "FileResponse" in server_src


def test_replay_report_has_trace_path():
    from harvest_index.registry.replay_harness import ReplayReport
    report = ReplayReport(replay_id="r1", pack_id="p1")
    assert hasattr(report, "trace_path")
    assert report.trace_path is None
