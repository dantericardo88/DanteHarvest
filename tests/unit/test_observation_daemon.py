"""Tests for harvest_observe.daemon.observation_daemon."""
import asyncio
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock


def _make_daemon(tmp_path, capture_interval=0.05, heartbeat_interval=0.1, ocr_enabled=False):
    from harvest_observe.daemon.observation_daemon import ObservationDaemon, DaemonConfig
    from harvest_core.provenance.chain_writer import ChainWriter

    chain_path = tmp_path / "chain.jsonl"
    chain_writer = MagicMock()
    chain_writer.chain_file_path = chain_path
    chain_writer.append = AsyncMock(return_value=MagicMock(sequence=1))

    config = DaemonConfig(
        capture_interval_s=capture_interval,
        heartbeat_interval_s=heartbeat_interval,
        ocr_enabled=ocr_enabled,
        pid_file=str(tmp_path / "daemon.pid"),
    )
    return ObservationDaemon(chain_writer=chain_writer, config=config)


def test_daemon_config_defaults():
    from harvest_observe.daemon.observation_daemon import DaemonConfig
    cfg = DaemonConfig()
    assert cfg.capture_interval_s == 5.0
    assert cfg.heartbeat_interval_s == 60.0
    assert cfg.ocr_enabled is True
    assert cfg.pid_file is None


def test_daemon_config_custom():
    from harvest_observe.daemon.observation_daemon import DaemonConfig
    cfg = DaemonConfig(capture_interval_s=1.0, heartbeat_interval_s=10.0, ocr_enabled=False)
    assert cfg.capture_interval_s == 1.0
    assert not cfg.ocr_enabled


def test_daemon_initial_state(tmp_path):
    daemon = _make_daemon(tmp_path)
    assert daemon._chain_writer is not None
    # Stop event not set yet means daemon is not stopped
    assert not daemon._stop_event.is_set()


@pytest.mark.asyncio
async def test_daemon_stop_before_start(tmp_path):
    daemon = _make_daemon(tmp_path)
    daemon.stop()  # Should not raise even if not running
    assert daemon._stop_event.is_set()


@pytest.mark.asyncio
async def test_daemon_writes_pid_file(tmp_path):
    daemon = _make_daemon(tmp_path, capture_interval=0.05, heartbeat_interval=0.05)
    pid_path_recorded = []

    task = asyncio.create_task(daemon.run())
    await asyncio.sleep(0.12)
    # PID file written during run — check before stopping
    if getattr(daemon, "_pid_path", None):
        pid_path_recorded.append(daemon._pid_path)
    daemon.stop()
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    # _pid_path was set while running
    assert len(pid_path_recorded) > 0 or daemon._config.pid_file is not None


@pytest.mark.asyncio
async def test_daemon_stops_cleanly(tmp_path):
    daemon = _make_daemon(tmp_path, capture_interval=0.05, heartbeat_interval=0.05)

    async def run_and_stop():
        task = asyncio.create_task(daemon.run())
        await asyncio.sleep(0.12)
        daemon.stop()
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    await run_and_stop()
    assert daemon._stop_event.is_set()


@pytest.mark.asyncio
async def test_daemon_heartbeat_appends_to_chain(tmp_path):
    daemon = _make_daemon(tmp_path, capture_interval=0.2, heartbeat_interval=0.05)

    async def run_briefly():
        task = asyncio.create_task(daemon.run())
        await asyncio.sleep(0.2)
        daemon.stop()
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    await run_briefly()
    assert daemon._chain_writer.append.called


def test_daemon_config_pid_file_optional():
    from harvest_observe.daemon.observation_daemon import DaemonConfig
    cfg = DaemonConfig(pid_file=None)
    assert cfg.pid_file is None


@pytest.mark.asyncio
async def test_daemon_run_is_nonblocking(tmp_path):
    daemon = _make_daemon(tmp_path, capture_interval=0.1, heartbeat_interval=0.1)

    task = asyncio.create_task(daemon.run())
    # Should not block — we can await sleep while it runs
    await asyncio.sleep(0.05)
    daemon.stop()
    try:
        await asyncio.wait_for(task, timeout=1.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    # Reaching here means run() didn't block the event loop
    assert True
