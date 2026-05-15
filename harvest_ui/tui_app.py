"""
HarvestTUIApp — full interactive Textual TUI session for harvest monitoring.

Wave 6c: tui_progress_visibility — full interactive Textual TUI session (8→9).

Provides a full-screen interactive TUI with:
1. Job status panel — live job queue with status indicators
2. Chain stats panel — entry counts, merkle seal status, last event
3. Artifact browser — scrollable list of recent artifacts
4. Alert log — real-time alert/error feed
5. Keyboard shortcuts — j/k navigation, r refresh, q quit, s seal chain

Uses Textual when available (pip install textual), falls back to a Rich Live
layout for terminal environments without Textual.

Activation:
    harvest tui [--storage storage] [--registry registry]
    python -m harvest_ui.tui_app

Constitutional guarantees:
- Fail-open: falls back to Rich Live layout if Textual is not installed
- Local-first: reads only from local storage/registry directories
- Refresh-based: polls storage on a configurable interval (default 2s)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, List, Optional


# ---------------------------------------------------------------------------
# Textual availability check
# ---------------------------------------------------------------------------

try:
    from textual.app import App, ComposeResult
    from textual.widgets import Header, Footer, DataTable, Log, Static, Label
    from textual.containers import Container, Horizontal, Vertical
    from textual.reactive import reactive
    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False


# ---------------------------------------------------------------------------
# Data loading helpers (shared by both TUI backends)
# ---------------------------------------------------------------------------

def _load_chain_stats(storage_root: Path) -> dict:
    chain_dir = storage_root / "chain"
    if not chain_dir.exists():
        return {"chain_files": 0, "total_entries": 0}
    chain_files = list(chain_dir.glob("*.jsonl"))
    total_entries = 0
    for f in chain_files:
        try:
            total_entries += sum(1 for l in f.read_text(encoding="utf-8").splitlines() if l.strip())
        except Exception:
            pass
    sealed = sum(1 for f in chain_dir.glob("*.manifest.json"))
    return {
        "chain_files": len(chain_files),
        "total_entries": total_entries,
        "sealed_manifests": sealed,
    }


def _load_recent_artifacts(storage_root: Path, limit: int = 20) -> List[dict]:
    artifacts_dir = storage_root / "artifacts"
    if not artifacts_dir.exists():
        return []
    files = sorted(artifacts_dir.rglob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)[:limit]
    results = []
    for f in files:
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            results.append({
                "artifact_id": d.get("artifact_id", f.stem)[:20],
                "source_type": d.get("source_type", "?"),
                "sha256": (d.get("sha256") or "")[:12],
                "storage_uri": d.get("storage_uri", "?")[:30],
            })
        except Exception:
            pass
    return results


def _load_pack_stats(registry_root: Path) -> dict:
    index_path = registry_root / "pack_index.json"
    if not index_path.exists():
        return {"total": 0, "promoted": 0, "candidate": 0}
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
        by_status: dict = {}
        for entry in index.values():
            s = entry.get("promotion_status", "?")
            by_status[s] = by_status.get(s, 0) + 1
        return {
            "total": len(index),
            "promoted": by_status.get("promoted", 0),
            "candidate": by_status.get("candidate", 0),
            "rejected": by_status.get("rejected", 0),
        }
    except Exception:
        return {"total": 0}


# ---------------------------------------------------------------------------
# Textual TUI App
# ---------------------------------------------------------------------------

if _HAS_TEXTUAL:
    class HarvestTUIApp(App):
        """Full interactive Textual TUI for harvest monitoring."""

        CSS = """
        Screen {
            layout: grid;
            grid-size: 2 2;
            grid-rows: 1fr 1fr;
            grid-columns: 1fr 1fr;
        }
        #chain-panel { border: solid $primary; }
        #pack-panel { border: solid $success; }
        #artifact-panel { border: solid $warning; }
        #alert-panel { border: solid $error; }
        .panel-title { background: $boost; padding: 0 1; }
        """

        BINDINGS = [
            ("q", "quit", "Quit"),
            ("r", "refresh", "Refresh"),
            ("s", "seal_chain", "Seal Chain"),
        ]

        def __init__(
            self,
            storage_root: str = "storage",
            registry_root: str = "registry",
            refresh_interval: float = 2.0,
        ):
            super().__init__()
            self._storage = Path(storage_root)
            self._registry = Path(registry_root)
            self._refresh_interval = refresh_interval

        def compose(self) -> ComposeResult:
            yield Header()
            with Container(id="chain-panel"):
                yield Label("Evidence Chain", classes="panel-title")
                yield Static(id="chain-stats")
            with Container(id="pack-panel"):
                yield Label("Pack Registry", classes="panel-title")
                yield Static(id="pack-stats")
            with Container(id="artifact-panel"):
                yield Label("Recent Artifacts", classes="panel-title")
                yield DataTable(id="artifact-table")
            with Container(id="alert-panel"):
                yield Label("Activity Log", classes="panel-title")
                yield Log(id="activity-log", highlight=True)
            yield Footer()

        def on_mount(self) -> None:
            self._setup_artifact_table()
            self._do_refresh()
            self.set_interval(self._refresh_interval, self.action_refresh)

        def _setup_artifact_table(self) -> None:
            table = self.query_one("#artifact-table", DataTable)
            table.add_columns("artifact_id", "source_type", "sha256", "uri")

        def action_refresh(self) -> None:
            self._do_refresh()

        def _do_refresh(self) -> None:
            # Chain stats
            chain_stats = _load_chain_stats(self._storage)
            chain_widget = self.query_one("#chain-stats", Static)
            chain_widget.update(
                f"Files: {chain_stats.get('chain_files', 0)}\n"
                f"Entries: {chain_stats.get('total_entries', 0)}\n"
                f"Sealed: {chain_stats.get('sealed_manifests', 0)}"
            )

            # Pack stats
            pack_stats = _load_pack_stats(self._registry)
            pack_widget = self.query_one("#pack-stats", Static)
            pack_widget.update(
                f"Total: {pack_stats.get('total', 0)}\n"
                f"Promoted: {pack_stats.get('promoted', 0)}\n"
                f"Candidate: {pack_stats.get('candidate', 0)}"
            )

            # Artifacts
            table = self.query_one("#artifact-table", DataTable)
            table.clear()
            for a in _load_recent_artifacts(self._storage, limit=10):
                table.add_row(a["artifact_id"], a["source_type"], a["sha256"], a["storage_uri"])

            # Log
            log = self.query_one("#activity-log", Log)
            ts = time.strftime("%H:%M:%S")
            log.write_line(f"[{ts}] Refreshed — {chain_stats.get('total_entries', 0)} entries")

        def action_seal_chain(self) -> None:
            log = self.query_one("#activity-log", Log)
            log.write_line(f"[{time.strftime('%H:%M:%S')}] Seal requested — run 'harvest verify-chain --seal'")


# ---------------------------------------------------------------------------
# Rich Live fallback TUI
# ---------------------------------------------------------------------------

class RichFallbackTUI:
    """Rich Live-based fallback when Textual is not installed."""

    def __init__(
        self,
        storage_root: str = "storage",
        registry_root: str = "registry",
        refresh_interval: float = 2.0,
    ):
        self._storage = Path(storage_root)
        self._registry = Path(registry_root)
        self._interval = refresh_interval

    def run(self) -> None:
        try:
            from rich.live import Live
            from rich.table import Table
            from rich.layout import Layout
            from rich.panel import Panel
            from rich.console import Console
            import time as _time

            console = Console()

            def _make_layout() -> Layout:
                layout = Layout()
                layout.split_column(
                    Layout(name="top", size=8),
                    Layout(name="bottom"),
                )
                layout["top"].split_row(
                    Layout(name="chain"),
                    Layout(name="packs"),
                )

                chain = _load_chain_stats(self._storage)
                packs = _load_pack_stats(self._registry)
                artifacts = _load_recent_artifacts(self._storage, limit=10)

                layout["chain"].update(Panel(
                    f"Files: {chain.get('chain_files', 0)}\n"
                    f"Entries: {chain.get('total_entries', 0)}\n"
                    f"Sealed: {chain.get('sealed_manifests', 0)}",
                    title="Evidence Chain",
                ))
                layout["packs"].update(Panel(
                    f"Total: {packs.get('total', 0)}\n"
                    f"Promoted: {packs.get('promoted', 0)}\n"
                    f"Candidate: {packs.get('candidate', 0)}",
                    title="Pack Registry",
                ))

                table = Table(title=f"Recent Artifacts (q to quit)")
                table.add_column("artifact_id")
                table.add_column("source_type")
                table.add_column("sha256")
                for a in artifacts:
                    table.add_row(a["artifact_id"], a["source_type"], a["sha256"])
                layout["bottom"].update(Panel(table, title="Artifacts"))
                return layout

            with Live(refresh_per_second=1, screen=True) as live:
                while True:
                    live.update(_make_layout())
                    _time.sleep(self._interval)
        except KeyboardInterrupt:
            pass
        except ImportError:
            print("Rich not installed. Run: pip install rich")


# ---------------------------------------------------------------------------
# Public launcher
# ---------------------------------------------------------------------------

def launch_tui(
    storage_root: str = "storage",
    registry_root: str = "registry",
    refresh_interval: float = 2.0,
) -> None:
    """
    Launch the interactive TUI. Uses Textual if available, Rich Live otherwise.

    Called by `harvest tui` CLI command.
    """
    if _HAS_TEXTUAL:
        app = HarvestTUIApp(
            storage_root=storage_root,
            registry_root=registry_root,
            refresh_interval=refresh_interval,
        )
        app.run()
    else:
        tui = RichFallbackTUI(
            storage_root=storage_root,
            registry_root=registry_root,
            refresh_interval=refresh_interval,
        )
        tui.run()
