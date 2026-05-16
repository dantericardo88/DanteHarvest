"""
ProgressReporter — Rich-based progress display for DanteHarvest CLI operations.

Provides a unified interface for showing progress bars, spinners, and ETAs
across all long-running CLI commands.

Constitutional guarantees:
- Non-crashing: all display errors are silently swallowed; core operation continues.
- Composable: works as context manager or standalone start/finish calls.
- Thread-safe: delegates to Rich's own thread safety.
"""

from __future__ import annotations

from typing import Optional

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


class ProgressReporter:
    """Wraps Rich progress for DanteHarvest CLI operations.

    Usage as context manager::

        with ProgressReporter() as reporter:
            reporter.start_task("Crawling pages", total=50)
            for page in pages:
                process(page)
                reporter.advance()
            reporter.finish("Crawl complete")

    Usage standalone::

        reporter = ProgressReporter()
        reporter.start_task("Ingesting files", total=100)
        for f in files:
            ingest(f)
            reporter.advance()
        reporter.finish("Done!")
    """

    def __init__(self, console: Optional[Console] = None) -> None:
        self.console = console or Console(stderr=False)
        self._progress: Optional[Progress] = None
        self._task_id: Optional[TaskID] = None
        self._owned: bool = False  # True when we started the Progress ourselves

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "ProgressReporter":
        return self

    def __exit__(self, *args) -> None:
        self._stop_progress()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_task(self, description: str, total: Optional[int] = None) -> None:
        """Start a progress task.

        Args:
            description: Text shown next to the progress indicator.
            total: Known total for ETA bar. Pass ``None`` for an indeterminate spinner.
        """
        if self._progress is None:
            self._progress = self._make_progress(known_total=total is not None)
            self._progress.start()
            self._owned = True

        self._task_id = self._progress.add_task(description, total=total)

    def advance(self, amount: int = 1) -> None:
        """Advance the current task by *amount* steps."""
        if self._progress is not None and self._task_id is not None:
            try:
                self._progress.advance(self._task_id, amount)
            except Exception:
                pass  # never crash the caller

    def update_description(self, description: str) -> None:
        """Change the description text on the running task."""
        if self._progress is not None and self._task_id is not None:
            try:
                self._progress.update(self._task_id, description=description)
            except Exception:
                pass

    def finish(self, message: Optional[str] = None) -> None:
        """Complete the task and optionally print a success message."""
        self._stop_progress()
        if message:
            self.console.print(f"[green]{message}[/green]")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_progress(self, known_total: bool = True) -> Progress:
        """Build a Rich Progress with the right column set."""
        columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
        ]
        if known_total:
            columns += [
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ]
        else:
            columns += [TimeElapsedColumn()]

        return Progress(
            *columns,
            console=self.console,
            transient=True,
        )

    def _stop_progress(self) -> None:
        if self._progress is not None and self._owned:
            try:
                self._progress.stop()
            except Exception:
                pass
            self._progress = None
            self._task_id = None
            self._owned = False
