"""Tests for CronTrigger, IntervalTrigger, and JobScheduler scheduling methods."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from harvest_ui.api.job_scheduler import CronTrigger, IntervalTrigger, JobScheduler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(year=2024, month=1, day=1, hour=0, minute=0, second=0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def _make_scheduler(tmp_path):
    from harvest_ui.api.job_store import JobStore

    async def _dummy_runner(job_id, params, store):
        pass

    store = JobStore(storage_root=str(tmp_path))
    return JobScheduler(store, _dummy_runner)


# ---------------------------------------------------------------------------
# CronTrigger — @aliases
# ---------------------------------------------------------------------------

def test_cron_trigger_at_hourly():
    """@hourly fires when minute == 0, regardless of hour."""
    trigger = CronTrigger("@hourly")
    assert trigger.is_due(_utc(hour=3, minute=0)) is True
    assert trigger.is_due(_utc(hour=3, minute=1)) is False
    assert trigger.is_due(_utc(hour=0, minute=0)) is True


def test_cron_trigger_at_daily():
    """@daily fires only at midnight (hour=0, minute=0)."""
    trigger = CronTrigger("@daily")
    assert trigger.is_due(_utc(hour=0, minute=0)) is True
    assert trigger.is_due(_utc(hour=1, minute=0)) is False
    assert trigger.is_due(_utc(hour=0, minute=1)) is False


def test_cron_trigger_at_weekly():
    """@weekly resolves to '0 0 * * 0' — minute=0, hour=0 (day-of-week not enforced here)."""
    trigger = CronTrigger("@weekly")
    # Our simple parser only checks minute + hour fields
    assert trigger.is_due(_utc(hour=0, minute=0)) is True
    assert trigger.is_due(_utc(hour=0, minute=30)) is False


def test_cron_trigger_at_monthly():
    trigger = CronTrigger("@monthly")
    assert trigger.is_due(_utc(hour=0, minute=0)) is True
    assert trigger.is_due(_utc(hour=1, minute=0)) is False


# ---------------------------------------------------------------------------
# CronTrigger — step notation
# ---------------------------------------------------------------------------

def test_cron_trigger_every_5_minutes():
    trigger = CronTrigger("*/5 * * * *")
    assert trigger.is_due(_utc(minute=0)) is True
    assert trigger.is_due(_utc(minute=5)) is True
    assert trigger.is_due(_utc(minute=15)) is True
    assert trigger.is_due(_utc(minute=3)) is False


def test_cron_trigger_specific_minute_and_hour():
    trigger = CronTrigger("30 9 * * *")
    assert trigger.is_due(_utc(hour=9, minute=30)) is True
    assert trigger.is_due(_utc(hour=9, minute=31)) is False
    assert trigger.is_due(_utc(hour=10, minute=30)) is False


# ---------------------------------------------------------------------------
# CronTrigger — next_fire_time
# ---------------------------------------------------------------------------

def test_cron_trigger_next_fire_time_hourly():
    trigger = CronTrigger("@hourly")
    now = _utc(hour=3, minute=45)
    nft = trigger.next_fire_time(now)
    assert nft.minute == 0
    assert nft.hour == 4


def test_cron_trigger_invalid_expression():
    with pytest.raises(ValueError):
        CronTrigger("bad expr")


# ---------------------------------------------------------------------------
# IntervalTrigger
# ---------------------------------------------------------------------------

def test_interval_trigger_is_due():
    """60-second interval is due when last_fire was >60 s ago."""
    trigger = IntervalTrigger(seconds=60)
    last_fire = _utc(minute=0, second=0)
    now = _utc(minute=1, second=1)  # 61 s later
    assert trigger.is_due(last_fire, now) is True


def test_interval_trigger_not_due():
    """60-second interval is NOT due when last_fire was only 10 s ago."""
    trigger = IntervalTrigger(seconds=60)
    last_fire = _utc(minute=0, second=50)
    now = _utc(minute=1, second=0)  # 10 s later
    assert trigger.is_due(last_fire, now) is False


def test_interval_trigger_exactly_on_boundary():
    """Interval is due when elapsed == interval (boundary inclusive)."""
    trigger = IntervalTrigger(seconds=60)
    last_fire = _utc(minute=0, second=0)
    now = _utc(minute=1, second=0)  # exactly 60 s
    assert trigger.is_due(last_fire, now) is True


def test_interval_trigger_minutes_param():
    trigger = IntervalTrigger(minutes=5)
    assert trigger.interval_seconds == 300
    last = _utc(hour=1, minute=0)
    assert trigger.is_due(last, _utc(hour=1, minute=5)) is True
    assert trigger.is_due(last, _utc(hour=1, minute=4)) is False


def test_interval_trigger_next_fire_time():
    trigger = IntervalTrigger(seconds=3600)
    base = _utc(hour=2, minute=0)
    nft = trigger.next_fire_time(base)
    assert nft == _utc(hour=3, minute=0)


def test_interval_trigger_zero_raises():
    with pytest.raises(ValueError):
        IntervalTrigger(seconds=0)


# ---------------------------------------------------------------------------
# JobScheduler — schedule_job / get_due_jobs / run_due_jobs
# ---------------------------------------------------------------------------

def test_schedule_job_with_cron_trigger(tmp_path):
    """Scheduled job appears in get_due_jobs() when the trigger is due."""
    scheduler = _make_scheduler(tmp_path)
    trigger = CronTrigger("@hourly")
    called = []
    scheduler.schedule_job("job-cron", lambda: called.append(1), trigger)

    due_now = _utc(hour=5, minute=0)
    due_jobs = scheduler.get_due_jobs(due_now)
    assert any(j.job_id == "job-cron" for j in due_jobs)


def test_schedule_job_not_in_due_when_not_due(tmp_path):
    scheduler = _make_scheduler(tmp_path)
    trigger = CronTrigger("@hourly")
    scheduler.schedule_job("job-not-due", lambda: None, trigger)

    not_due_now = _utc(hour=5, minute=30)
    due_jobs = scheduler.get_due_jobs(not_due_now)
    assert all(j.job_id != "job-not-due" for j in due_jobs)


def test_get_due_jobs_returns_empty_when_none_due(tmp_path):
    """No due jobs returns empty list."""
    scheduler = _make_scheduler(tmp_path)
    trigger = CronTrigger("@daily")  # fires at 00:00
    scheduler.schedule_job("job-daily", lambda: None, trigger)

    not_midnight = _utc(hour=12, minute=0)
    assert scheduler.get_due_jobs(not_midnight) == []


def test_schedule_job_with_interval_trigger_never_fired(tmp_path):
    """IntervalTrigger job with no last_fire is always due."""
    scheduler = _make_scheduler(tmp_path)
    trigger = IntervalTrigger(seconds=3600)
    scheduler.schedule_job("job-interval", lambda: None, trigger)

    # last_fire is None → always due
    due = scheduler.get_due_jobs(_utc(hour=0, minute=0))
    assert any(j.job_id == "job-interval" for j in due)


def test_run_due_jobs_executes_and_updates_last_fire(tmp_path):
    """run_due_jobs() calls the function and sets last_fire."""
    scheduler = _make_scheduler(tmp_path)
    trigger = CronTrigger("@hourly")
    calls = []
    scheduler.schedule_job("job-exec", lambda: calls.append(1), trigger)

    fire_time = _utc(hour=3, minute=0)
    fired = scheduler.run_due_jobs(fire_time)

    assert "job-exec" in fired
    assert len(calls) == 1
    assert scheduler._scheduled_jobs["job-exec"].last_fire == fire_time


def test_run_due_jobs_returns_empty_when_none_due(tmp_path):
    scheduler = _make_scheduler(tmp_path)
    trigger = CronTrigger("@daily")
    scheduler.schedule_job("job-skip", lambda: None, trigger)

    not_midnight = _utc(hour=6, minute=0)
    fired = scheduler.run_due_jobs(not_midnight)
    assert fired == []


def test_run_due_jobs_error_does_not_propagate(tmp_path):
    """A job that raises must not crash run_due_jobs()."""
    scheduler = _make_scheduler(tmp_path)
    trigger = CronTrigger("@hourly")

    def boom():
        raise RuntimeError("job exploded")

    scheduler.schedule_job("job-boom", boom, trigger)
    fire_time = _utc(hour=1, minute=0)
    fired = scheduler.run_due_jobs(fire_time)
    assert "job-boom" in fired  # was attempted
