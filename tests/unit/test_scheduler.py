"""
Phase 5 — Job scheduler tests.

Verifies:
1. ScheduleStore CRUD
2. HarvestScheduler add/list/remove (no APScheduler dependency in unit tests)
3. Cron validation
4. CLI schedule subcommands are wired
"""

import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest

from harvest_ui.scheduler.schedule_model import ScheduleStore, ScheduleEntry
from harvest_ui.scheduler.scheduler import HarvestScheduler, SchedulerError, _validate_cron


# ---------------------------------------------------------------------------
# ScheduleStore unit tests
# ---------------------------------------------------------------------------

def test_schedule_store_create(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    entry = store.create("crawl", "0 * * * *", {"url": "https://example.com"})
    assert entry.schedule_id
    assert entry.command == "crawl"
    assert entry.cron_expr == "0 * * * *"
    assert entry.status == "active"
    assert (tmp_path / "schedules" / f"{entry.schedule_id}.json").exists()


def test_schedule_store_get(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    entry = store.create("ingest", "30 6 * * *", {"path": "/data"})
    fetched = store.get(entry.schedule_id)
    assert fetched is not None
    assert fetched.schedule_id == entry.schedule_id
    assert fetched.command == "ingest"


def test_schedule_store_get_missing(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    assert store.get("missing-id") is None


def test_schedule_store_update(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    entry = store.create("crawl", "0 * * * *", {})
    updated = store.update(entry.schedule_id, status="paused", run_count=3)
    assert updated.status == "paused"
    assert updated.run_count == 3
    reloaded = store.get(entry.schedule_id)
    assert reloaded.status == "paused"


def test_schedule_store_delete(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    entry = store.create("crawl", "0 * * * *", {})
    assert store.delete(entry.schedule_id) is True
    reloaded = store.get(entry.schedule_id)
    assert reloaded.status == "deleted"


def test_schedule_store_list(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    store.create("crawl", "0 * * * *", {})
    store.create("ingest", "0 6 * * *", {})
    all_entries = store.list_entries()
    assert len(all_entries) == 2


def test_schedule_store_list_filter(tmp_path):
    store = ScheduleStore(storage_root=str(tmp_path))
    e1 = store.create("crawl", "0 * * * *", {})
    store.update(e1.schedule_id, status="paused")
    store.create("ingest", "0 6 * * *", {})
    active = store.list_entries(status="active")
    assert len(active) == 1
    assert active[0].command == "ingest"


def test_schedule_entry_roundtrip():
    entry = ScheduleEntry(
        schedule_id="abc",
        command="crawl",
        cron_expr="0 * * * *",
        args={"url": "https://x.com"},
        created_at=time.time(),
    )
    d = entry.to_dict()
    restored = ScheduleEntry.from_dict(d)
    assert restored.schedule_id == entry.schedule_id
    assert restored.args["url"] == "https://x.com"


# ---------------------------------------------------------------------------
# Cron validation
# ---------------------------------------------------------------------------

def test_valid_cron_passes():
    _validate_cron("0 * * * *")
    _validate_cron("30 6 1 * 0")
    _validate_cron("*/5 * * * *")


def test_invalid_cron_raises():
    with pytest.raises(SchedulerError, match="5 fields"):
        _validate_cron("0 *")

    with pytest.raises(SchedulerError, match="5 fields"):
        _validate_cron("0 * * * * * extra")


# ---------------------------------------------------------------------------
# HarvestScheduler (no real APScheduler)
# ---------------------------------------------------------------------------

def test_scheduler_add_without_apscheduler(tmp_path):
    scheduler = HarvestScheduler(storage_root=str(tmp_path))
    # Don't call start() — simulates no APScheduler available
    entry = scheduler.add("crawl", "0 * * * *", {"url": "https://example.com"})
    assert entry.schedule_id
    assert entry.command == "crawl"


def test_scheduler_add_invalid_cron_raises(tmp_path):
    scheduler = HarvestScheduler(storage_root=str(tmp_path))
    with pytest.raises(SchedulerError):
        scheduler.add("crawl", "bad cron", {})


def test_scheduler_list(tmp_path):
    scheduler = HarvestScheduler(storage_root=str(tmp_path))
    scheduler.add("crawl", "0 * * * *", {"url": "https://a.com"})
    scheduler.add("ingest", "0 6 * * *", {"path": "/data"})
    entries = scheduler.list()
    assert len(entries) == 2


def test_scheduler_remove(tmp_path):
    scheduler = HarvestScheduler(storage_root=str(tmp_path))
    entry = scheduler.add("crawl", "0 * * * *", {})
    removed = scheduler.remove(entry.schedule_id)
    assert removed is True
    reloaded = scheduler._store.get(entry.schedule_id)
    assert reloaded.status == "deleted"


def test_scheduler_remove_missing(tmp_path):
    scheduler = HarvestScheduler(storage_root=str(tmp_path))
    assert scheduler.remove("nonexistent") is False


# ---------------------------------------------------------------------------
# CLI schedule subcommands wired
# ---------------------------------------------------------------------------

def test_cli_schedule_parser_exists():
    from harvest_ui.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["schedule", "list"])
    assert args.command == "schedule"
    assert args.sched_cmd == "list"


def test_cli_schedule_add_parser():
    from harvest_ui.cli import build_parser
    parser = build_parser()
    args = parser.parse_args([
        "schedule", "add", "crawl",
        "--cron", "0 * * * *",
        "--url", "https://example.com",
    ])
    assert args.schedule_command == "crawl"
    assert args.cron == "0 * * * *"
    assert args.url == "https://example.com"


def test_cli_schedule_add_executes(tmp_path):
    from harvest_ui.cli import main
    result = main([
        "--storage", str(tmp_path),
        "schedule", "add", "crawl",
        "--cron", "0 * * * *",
        "--url", "https://example.com",
    ])
    assert result == 0


def test_cli_schedule_list_executes(tmp_path):
    from harvest_ui.cli import main
    result = main(["--storage", str(tmp_path), "schedule", "list"])
    assert result == 0


def test_cli_schedule_add_bad_cron(tmp_path):
    from harvest_ui.cli import main
    result = main([
        "--storage", str(tmp_path),
        "schedule", "add", "crawl",
        "--cron", "bad",
        "--url", "https://example.com",
    ])
    assert result == 1
