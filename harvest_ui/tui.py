"""
harvest_ui.tui — Rich-powered TUI helpers for the harvest CLI.

Provides progress bars, status spinners, formatted tables, and
syntax-highlighted output. Falls back to plain print() if Rich is not
installed (graceful degradation).

Sprint goal: replace bare print statements with Rich-powered output to
close the tui_progress_visibility gap from 2→7 in the v3 matrix.

Usage:
    from harvest_ui.tui import console, progress_context, print_status, print_table

    with progress_context("Ingesting files", total=len(files)) as prog:
        for i, f in enumerate(files):
            do_work(f)
            prog.advance(i)

    print_status("ok", "Artifact ingested", artifact_id="abc123")
    print_table("Pack Registry", rows, columns=["pack_id", "status", "title"])
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from typing import Any, Dict, Generator, Iterable, List, Optional

# ---------------------------------------------------------------------------
# Rich imports with graceful fallback
# ---------------------------------------------------------------------------

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    from rich.table import Table
    from rich.text import Text
    from rich import print as rich_print
    from rich.panel import Panel
    from rich.syntax import Syntax
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False


# ---------------------------------------------------------------------------
# Global console instance
# ---------------------------------------------------------------------------

if _HAS_RICH:
    console = Console(stderr=False)
    err_console = Console(stderr=True, style="bold red")
else:
    # Minimal shim so callers don't need to guard
    class _FallbackConsole:  # type: ignore[no-redef]
        def print(self, *args, **kwargs):
            print(*args)

        def rule(self, title: str = "", **kwargs):
            print(f"\n{'='*60} {title} {'='*60}\n")

    console = _FallbackConsole()  # type: ignore[assignment]
    err_console = _FallbackConsole()  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Status printing
# ---------------------------------------------------------------------------

_STATUS_ICONS = {
    "ok": "[bold green]✓[/bold green]",
    "warn": "[bold yellow]⚠[/bold yellow]",
    "error": "[bold red]✗[/bold red]",
    "info": "[bold blue]ℹ[/bold blue]",
    "pending": "[bold yellow]…[/bold yellow]",
}


def print_status(
    status: str,
    message: str,
    **fields: Any,
) -> None:
    """
    Print a formatted status line.

        print_status("ok", "Ingested file", artifact_id="abc", sha256="def")

    Output (Rich): ✓ Ingested file  artifact_id=abc sha256=def
    Output (fallback): [ok] Ingested file  artifact_id=abc sha256=def
    """
    if _HAS_RICH:
        icon = _STATUS_ICONS.get(status.lower(), f"[{status}]")
        parts = [icon, f" {message}"]
        if fields:
            field_str = "  " + "  ".join(f"[dim]{k}[/dim]={v}" for k, v in fields.items())
            parts.append(field_str)
        console.print("".join(parts))
    else:
        field_str = "  " + "  ".join(f"{k}={v}" for k, v in fields.items()) if fields else ""
        print(f"[{status}] {message}{field_str}")


def print_error(message: str, **fields: Any) -> None:
    """Print error to stderr."""
    if _HAS_RICH:
        icon = _STATUS_ICONS["error"]
        parts = [icon, f" {message}"]
        if fields:
            parts.append("  " + "  ".join(f"[dim]{k}[/dim]={v}" for k, v in fields.items()))
        err_console.print("".join(parts))
    else:
        field_str = "  " + "  ".join(f"{k}={v}" for k, v in fields.items()) if fields else ""
        print(f"[error] {message}{field_str}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Progress context manager
# ---------------------------------------------------------------------------

class _FallbackProgress:
    """Minimal shim when Rich is not installed."""

    def __init__(self, description: str, total: Optional[int] = None):
        self.description = description
        self.total = total
        self._current = 0
        print(f"[...] {description}" + (f" (0/{total})" if total else ""))

    def advance(self, n: int = 1) -> None:
        self._current += n
        if self.total:
            pct = int(100 * self._current / self.total)
            print(f"\r[{pct:3d}%] {self.description} ({self._current}/{self.total})", end="")

    def update(self, message: str) -> None:
        print(f"\r[...] {message}", end="")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        print()  # newline after inline progress


@contextmanager
def progress_context(
    description: str,
    total: Optional[int] = None,
    unit: str = "items",
) -> Generator:
    """
    Context manager that yields a progress tracker.

    Tracker exposes:
        .advance(n=1)    — increment by n
        .update(msg)     — update description text

    Example:
        with progress_context("Crawling pages", total=50) as prog:
            for page in pages:
                fetch(page)
                prog.advance()
    """
    if _HAS_RICH:
        columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn() if total else TaskProgressColumn(),
            TimeElapsedColumn(),
        ]
        if total:
            columns.append(TimeRemainingColumn())

        with Progress(*columns, console=console, transient=False) as progress:
            task_id = progress.add_task(description, total=total)

            class _Tracker:
                def advance(self, n: int = 1) -> None:
                    progress.advance(task_id, advance=n)

                def update(self, message: str) -> None:
                    progress.update(task_id, description=message)

            yield _Tracker()
    else:
        fb = _FallbackProgress(description, total)
        with fb:
            yield fb


# ---------------------------------------------------------------------------
# Spinner (indeterminate)
# ---------------------------------------------------------------------------

@contextmanager
def spinner_context(message: str) -> Generator:
    """
    Show a spinner for indeterminate operations.

    Example:
        with spinner_context("Connecting to browser..."):
            engine.start()
    """
    if _HAS_RICH:
        with console.status(f"[bold green]{message}[/bold green]") as status:
            class _Spinner:
                def update(self, msg: str) -> None:
                    status.update(f"[bold green]{msg}[/bold green]")
            yield _Spinner()
    else:
        print(f"[...] {message}")

        class _NoopSpinner:
            def update(self, msg: str) -> None:
                print(f"[...] {msg}")

        yield _NoopSpinner()


# ---------------------------------------------------------------------------
# Table printing
# ---------------------------------------------------------------------------

def print_table(
    title: str,
    rows: Iterable[Dict[str, Any]],
    columns: Optional[List[str]] = None,
    row_styles: Optional[List[str]] = None,
) -> None:
    """
    Print rows as a formatted table.

    Args:
        title: Table title shown in header.
        rows: Iterable of dicts.
        columns: Which keys to include (and in which order). If None, uses all keys from first row.
        row_styles: Optional list of Rich styles per row.
    """
    row_list = list(rows)
    if not row_list:
        if _HAS_RICH:
            console.print(f"[dim](no rows in {title})[/dim]")
        else:
            print(f"(no rows in {title})")
        return

    if columns is None:
        columns = list(row_list[0].keys())

    if _HAS_RICH:
        table = Table(title=title, show_header=True, header_style="bold cyan")
        for col in columns:
            table.add_column(col, overflow="fold")
        for i, row in enumerate(row_list):
            style = row_styles[i] if row_styles and i < len(row_styles) else None
            table.add_row(*[str(row.get(col, "")) for col in columns], style=style)
        console.print(table)
    else:
        print(f"\n--- {title} ---")
        header = "  ".join(f"{col:<20}" for col in columns)
        print(header)
        print("-" * len(header))
        for row in row_list:
            print("  ".join(f"{str(row.get(col, '')):<20}" for col in columns))
        print()


# ---------------------------------------------------------------------------
# JSON / syntax highlighting
# ---------------------------------------------------------------------------

def print_json(data: Any, title: Optional[str] = None) -> None:
    """
    Print JSON data with syntax highlighting (Rich) or pretty-printed (fallback).
    """
    import json as _json
    text = _json.dumps(data, indent=2, default=str)
    if _HAS_RICH:
        if title:
            console.rule(f"[bold]{title}[/bold]")
        syntax = Syntax(text, "json", theme="monokai", line_numbers=False)
        console.print(syntax)
    else:
        if title:
            print(f"\n--- {title} ---")
        print(text)


def print_panel(content: str, title: str = "", style: str = "blue") -> None:
    """Print content inside a Rich panel (or simple border fallback)."""
    if _HAS_RICH:
        console.print(Panel(content, title=title, border_style=style))
    else:
        width = 60
        print(f"┌{'─' * (width - 2)}┐")
        if title:
            print(f"│ {title:<{width - 3}}│")
            print(f"├{'─' * (width - 2)}┤")
        for line in content.split("\n"):
            print(f"│ {line:<{width - 3}}│")
        print(f"└{'─' * (width - 2)}┘")


# ---------------------------------------------------------------------------
# Convenience re-exports
# ---------------------------------------------------------------------------

def is_rich_available() -> bool:
    """Return True if the Rich library is installed and active."""
    return _HAS_RICH
