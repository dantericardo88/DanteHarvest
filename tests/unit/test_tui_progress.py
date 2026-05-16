"""Tests for ProgressReporter — Rich-based progress display."""

from __future__ import annotations

from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from rich.console import Console

from harvest_ui.tui_progress import ProgressReporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_reporter(capture: bool = True) -> tuple[ProgressReporter, StringIO]:
    """Return a ProgressReporter wired to an in-memory Console."""
    buf = StringIO()
    console = Console(file=buf, force_terminal=False, highlight=False)
    reporter = ProgressReporter(console=console)
    return reporter, buf


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestProgressReporterInit:
    def test_default_console_created(self):
        reporter = ProgressReporter()
        assert reporter.console is not None

    def test_custom_console_accepted(self):
        buf = StringIO()
        console = Console(file=buf)
        reporter = ProgressReporter(console=console)
        assert reporter.console is console

    def test_initial_state(self):
        reporter = ProgressReporter()
        assert reporter._progress is None
        assert reporter._task_id is None
        assert reporter._owned is False


# ---------------------------------------------------------------------------
# start_task / advance / update_description
# ---------------------------------------------------------------------------

class TestStartTask:
    def test_start_task_creates_progress(self):
        reporter, _ = make_reporter()
        reporter.start_task("Loading…")
        assert reporter._progress is not None
        assert reporter._task_id is not None
        reporter.finish()

    def test_start_task_with_total(self):
        reporter, _ = make_reporter()
        reporter.start_task("Processing", total=100)
        assert reporter._progress is not None
        reporter.finish()

    def test_start_task_indeterminate(self):
        reporter, _ = make_reporter()
        reporter.start_task("Spinning…", total=None)
        assert reporter._progress is not None
        reporter.finish()

    def test_advance_does_not_raise(self):
        reporter, _ = make_reporter()
        reporter.start_task("Task", total=10)
        reporter.advance(1)
        reporter.advance(3)
        reporter.finish()

    def test_advance_without_start_does_not_raise(self):
        """advance() before start_task() must be a no-op."""
        reporter, _ = make_reporter()
        reporter.advance(5)  # should not raise

    def test_update_description_does_not_raise(self):
        reporter, _ = make_reporter()
        reporter.start_task("Initial description", total=5)
        reporter.update_description("Updated description")
        reporter.finish()

    def test_update_description_without_start_does_not_raise(self):
        reporter, _ = make_reporter()
        reporter.update_description("No task yet")  # no-op


# ---------------------------------------------------------------------------
# finish
# ---------------------------------------------------------------------------

class TestFinish:
    def test_finish_stops_progress(self):
        reporter, _ = make_reporter()
        reporter.start_task("Work", total=3)
        reporter.finish()
        assert reporter._progress is None
        assert reporter._task_id is None

    def test_finish_prints_message(self):
        buf = StringIO()
        console = Console(file=buf, force_terminal=False, highlight=False)
        reporter = ProgressReporter(console=console)
        reporter.start_task("Work")
        reporter.finish("All done!")
        output = buf.getvalue()
        assert "All done!" in output

    def test_finish_without_message_does_not_raise(self):
        reporter, _ = make_reporter()
        reporter.start_task("Work")
        reporter.finish()  # no message

    def test_finish_without_start_does_not_raise(self):
        reporter, _ = make_reporter()
        reporter.finish("Nothing was running")  # no-op for progress


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_enter_returns_self(self):
        reporter, _ = make_reporter()
        with reporter as r:
            assert r is reporter

    def test_exit_cleans_up_progress(self):
        reporter, _ = make_reporter()
        with reporter:
            reporter.start_task("Inside", total=5)
            assert reporter._progress is not None
        # After exiting, progress is stopped
        assert reporter._progress is None

    def test_context_manager_full_workflow(self):
        reporter, buf = make_reporter()
        with reporter as r:
            r.start_task("Batch ingest", total=3)
            for _ in range(3):
                r.advance()
            r.finish("Batch done")
        output = buf.getvalue()
        assert "Batch done" in output

    def test_context_manager_exception_does_not_hang(self):
        reporter, _ = make_reporter()
        try:
            with reporter:
                reporter.start_task("Failing task")
                raise ValueError("deliberate error")
        except ValueError:
            pass
        # Progress must be cleaned up even after exception
        assert reporter._progress is None

    def test_nested_start_tasks_do_not_duplicate_progress(self):
        """Calling start_task twice reuses the same Progress instance."""
        reporter, _ = make_reporter()
        with reporter:
            reporter.start_task("First task")
            first_progress = reporter._progress
            reporter.start_task("Second task")
            assert reporter._progress is first_progress


# ---------------------------------------------------------------------------
# Integration: verify Rich output contains progress indicators
# ---------------------------------------------------------------------------

class TestRichOutput:
    def test_finish_message_visible_in_output(self):
        buf = StringIO()
        console = Console(file=buf, force_terminal=False)
        reporter = ProgressReporter(console=console)
        with reporter:
            reporter.start_task("Crawling pages", total=5)
            for _ in range(5):
                reporter.advance()
            reporter.finish("Crawl complete")
        assert "Crawl complete" in buf.getvalue()

    def test_multiple_advances_do_not_raise(self):
        reporter, _ = make_reporter()
        with reporter:
            reporter.start_task("Many steps", total=100)
            for i in range(100):
                reporter.advance()
            reporter.finish("Done")
