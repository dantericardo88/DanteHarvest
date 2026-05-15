"""
harvest_ui.reviewer.tui_reviewer — Terminal UI reviewer for DanteHarvest packs.

Provides an interactive Rich-powered terminal workflow for reviewing promotion
candidates: view queue → inspect detail → approve/reject/skip → repeat.

Constitutional guarantees:
- Fail-closed: missing Rich raises ImportError with install hint (not silent)
- CI-safe: all I/O can be injected via constructor (mock-friendly)
- Zero-ambiguity: decision enum has no "maybe" state — only APPROVE/REJECT/SKIP
- Append-only: emits ReviewDecision records; never mutates pack data directly

Usage::

    reviewer = TUIReviewer(packs=[...])
    reviewer.run_interactive_session()
"""

from __future__ import annotations

import enum
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# Rich imports — not gracefully degraded; TUI requires Rich
# ---------------------------------------------------------------------------

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, TextColumn
    from rich.prompt import Prompt
    from rich.syntax import Syntax
    from rich.table import Table
    from rich.text import Text
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False


def _require_rich() -> None:
    if not _HAS_RICH:
        raise ImportError(
            "TUIReviewer requires the 'rich' library. Install with: pip install rich"
        )


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class Decision(str, enum.Enum):
    APPROVE = "approve"
    REJECT  = "reject"
    SKIP    = "skip"
    QUIT    = "quit"


@dataclass
class ReviewDecision:
    """Records a single reviewer decision for a pack."""
    pack_id: str
    decision: Decision
    reason: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class PackSummaryRow:
    """Minimal pack data for queue display."""
    pack_id: str
    pack_type: str = ""
    score: float = 0.0
    status: str = "pending"
    confidence: float = 0.0

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PackSummaryRow":
        return cls(
            pack_id=str(d.get("pack_id", d.get("id", "unknown"))),
            pack_type=str(d.get("pack_type", d.get("type", ""))),
            score=float(d.get("score", d.get("quality_score", 0.0))),
            status=str(d.get("status", d.get("promotion_status", "pending"))),
            confidence=float(d.get("confidence", d.get("confidence_score", 0.0))),
        )


# ---------------------------------------------------------------------------
# TUIReviewer
# ---------------------------------------------------------------------------

class TUIReviewer:
    """
    Interactive Terminal UI reviewer for DanteHarvest pack promotion decisions.

    Instantiate with a list of pack dicts, then call run_interactive_session()
    for the full loop, or call individual methods for unit testing.

    Args:
        packs: List of pack dicts with at least a "pack_id" key.
        console: Rich Console instance (defaults to stderr=False console).
        input_fn: Callable that reads user input (defaults to Prompt.ask for
                  Rich, or input() as fallback). Must be injectable for CI tests.
        on_decision: Optional callback invoked with each ReviewDecision.
    """

    _DECISION_KEYS = {
        "a": Decision.APPROVE,
        "approve": Decision.APPROVE,
        "r": Decision.REJECT,
        "reject": Decision.REJECT,
        "s": Decision.SKIP,
        "skip": Decision.SKIP,
        "q": Decision.QUIT,
        "quit": Decision.QUIT,
    }

    def __init__(
        self,
        packs: Optional[List[Dict[str, Any]]] = None,
        console: Optional[Any] = None,
        input_fn: Optional[Callable[[str], str]] = None,
        on_decision: Optional[Callable[[ReviewDecision], None]] = None,
        chain_writer: Optional[Any] = None,
        session_run_id: Optional[str] = None,
    ) -> None:
        _require_rich()
        self.packs: List[Dict[str, Any]] = packs or []
        self.console: Any = console or Console()
        self._input_fn = input_fn
        self.on_decision = on_decision
        self.decisions: List[ReviewDecision] = []
        self._chain_writer = chain_writer
        self._session_run_id = session_run_id or str(uuid.uuid4())

    # ------------------------------------------------------------------
    # Queue display
    # ------------------------------------------------------------------

    def review_queue(self, packs: Optional[List[Dict[str, Any]]] = None) -> Table:
        """
        Build a Rich Table showing packs in the review queue.

        Columns: id | type | score | status | confidence

        Args:
            packs: Pack list to display. Falls back to self.packs if None.

        Returns:
            rich.table.Table (caller may print or inspect).
        """
        pack_list = packs if packs is not None else self.packs
        rows = [PackSummaryRow.from_dict(p) for p in pack_list]

        table = Table(
            title="Review Queue",
            show_header=True,
            header_style="bold cyan",
            show_lines=False,
        )
        table.add_column("ID", style="bold", overflow="fold", min_width=12)
        table.add_column("Type", overflow="fold")
        table.add_column("Score", justify="right")
        table.add_column("Status", justify="center")
        table.add_column("Confidence", justify="right")

        for row in rows:
            # Color-code status
            status_text = self._status_text(row.status)
            # Color-code confidence
            conf_color = self._confidence_color(row.confidence)
            conf_text = Text(f"{row.confidence:.2f}", style=f"bold {conf_color}")

            table.add_row(
                row.pack_id,
                row.pack_type,
                f"{row.score:.2f}",
                status_text,
                conf_text,
            )

        return table

    # ------------------------------------------------------------------
    # Detail view
    # ------------------------------------------------------------------

    def review_pack(self, pack_id: str, pack: Optional[Dict[str, Any]] = None) -> Panel:
        """
        Build a Rich Panel showing pack detail.

        Displays:
          - Metadata section: pack_id, type, status, score
          - Content preview: first 500 chars of content field
          - Confidence band as an ASCII progress bar

        Args:
            pack_id: ID of the pack to display.
            pack: Pack dict. If None, looks up in self.packs by pack_id.

        Returns:
            rich.panel.Panel (caller may print or inspect).
        """
        if pack is None:
            pack = next(
                (p for p in self.packs if str(p.get("pack_id", p.get("id", ""))) == pack_id),
                None,
            )
        if pack is None:
            return Panel(
                Text(f"Pack '{pack_id}' not found", style="bold red"),
                title="Pack Detail",
                border_style="red",
            )

        text = Text()

        # -- Metadata --
        text.append("Metadata\n", style="bold underline cyan")
        for key in ("pack_id", "pack_type", "promotion_status", "status", "score",
                    "quality_score", "title", "source", "created_at"):
            val = pack.get(key)
            if val is not None:
                text.append(f"  {key}: ", style="bold")
                text.append(f"{val}\n")

        # -- Confidence bar --
        conf = float(pack.get("confidence_score", pack.get("confidence", 0.0)))
        color = self._confidence_color(conf)
        filled = int(conf * 20)
        bar = "█" * filled + "░" * (20 - filled)
        pct = int(conf * 100)
        text.append("\nConfidence: ", style="bold")
        text.append(f"[{bar}] {pct}%\n", style=f"bold {color}")

        # -- Content preview --
        content = pack.get("content", pack.get("text", pack.get("body", "")))
        if content:
            text.append("\nContent Preview\n", style="bold underline cyan")
            preview = str(content)[:500]
            if len(str(content)) > 500:
                preview += "\n… (truncated)"
            text.append(preview + "\n")

        # -- Steps / chain --
        steps = pack.get("steps", pack.get("chain", []))
        if steps:
            text.append(f"\nSteps: {len(steps)}\n", style="bold")

        return Panel(
            text,
            title=f"[bold]Pack Detail — {pack_id}[/bold]",
            border_style="cyan",
            padding=(1, 2),
        )

    # ------------------------------------------------------------------
    # Decision prompting
    # ------------------------------------------------------------------

    def prompt_decision(self, pack_id: str) -> Decision:
        """
        Prompt the reviewer for a decision on a pack.

        Reads from self._input_fn if injected (CI-safe), else uses
        Rich Prompt.ask for interactive use.

        Valid inputs: a/approve, r/reject, s/skip, q/quit (case-insensitive).
        Loops until a valid input is provided.

        Args:
            pack_id: Pack ID shown in prompt text.

        Returns:
            Decision enum value.
        """
        prompt_text = (
            f"\nDecision for [bold cyan]{pack_id}[/bold cyan]"
            " [[green]a[/green]pprove / [red]r[/red]eject / [yellow]s[/yellow]kip / [dim]q[/dim]uit]"
        )

        while True:
            if self._input_fn is not None:
                raw = self._input_fn(prompt_text).strip().lower()
            else:
                self.console.print(prompt_text)
                try:
                    raw = Prompt.ask("").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    raw = "quit"

            decision = self._DECISION_KEYS.get(raw)
            if decision is not None:
                return decision

            self.console.print(
                f"  [red]Invalid input '{raw}'. Choose: a, r, s, q[/red]"
            )

    # ------------------------------------------------------------------
    # Interactive session
    # ------------------------------------------------------------------

    def run_interactive_session(
        self,
        packs: Optional[List[Dict[str, Any]]] = None,
    ) -> List[ReviewDecision]:
        """
        Run the full interactive review loop.

        Flow:
          1. Display queue table
          2. For each pack:
             a. Show pack detail panel
             b. Prompt for decision
             c. Record ReviewDecision
             d. Call on_decision callback if set
             e. If QUIT, break early
          3. Print summary and return decisions

        Args:
            packs: Optional override for pack list.

        Returns:
            List of ReviewDecision records.
        """
        pack_list = packs if packs is not None else self.packs
        self.decisions = []

        # Show queue
        table = self.review_queue(pack_list)
        self.console.print(table)
        self.console.print(f"\n[dim]Reviewing {len(pack_list)} packs…[/dim]\n")

        for pack in pack_list:
            pack_id = str(pack.get("pack_id", pack.get("id", "unknown")))

            # Show detail
            detail_panel = self.review_pack(pack_id, pack)
            self.console.print(detail_panel)

            # Get decision
            decision = self.prompt_decision(pack_id)

            # Collect reason if rejecting
            reason: Optional[str] = None
            if decision == Decision.REJECT:
                if self._input_fn is not None:
                    reason = self._input_fn("Rejection reason").strip() or None
                else:
                    try:
                        reason = Prompt.ask("Rejection reason (optional)", default="").strip() or None
                    except (EOFError, KeyboardInterrupt):
                        reason = None

            record = ReviewDecision(
                pack_id=pack_id,
                decision=decision,
                reason=reason,
            )
            self.decisions.append(record)
            self._emit_chain_entry(record)

            if self.on_decision:
                self.on_decision(record)

            if decision == Decision.QUIT:
                self.console.print("[dim]Session quit by reviewer.[/dim]")
                break

            # Status feedback
            color_map = {
                Decision.APPROVE: "green",
                Decision.REJECT: "red",
                Decision.SKIP: "yellow",
            }
            color = color_map.get(decision, "white")
            self.console.print(
                f"  [{color}]→ {decision.value.upper()}[/{color}]  {pack_id}\n"
            )

        # Summary
        self._print_summary(self.decisions)
        return self.decisions

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _emit_chain_entry(self, record: ReviewDecision) -> None:
        """Emit reviewer decision to the append-only evidence chain (if wired)."""
        if self._chain_writer is None:
            return
        try:
            import asyncio
            from harvest_core.provenance.chain_entry import ChainEntry
            entry = ChainEntry(
                run_id=self._session_run_id,
                signal="review.decision",
                machine="tui_reviewer",
                data={
                    "pack_id": record.pack_id,
                    "decision": record.decision.value,
                    "reason": record.reason,
                    "notes": record.notes,
                },
            )
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._chain_writer.append(entry))
                else:
                    loop.run_until_complete(self._chain_writer.append(entry))
            except RuntimeError:
                asyncio.run(self._chain_writer.append(entry))
        except Exception:
            pass  # Chain write failure never blocks the reviewer

    def _print_summary(self, decisions: List[ReviewDecision]) -> None:
        approved = sum(1 for d in decisions if d.decision == Decision.APPROVE)
        rejected = sum(1 for d in decisions if d.decision == Decision.REJECT)
        skipped  = sum(1 for d in decisions if d.decision == Decision.SKIP)
        self.console.print(
            f"\n[bold]Session Summary[/bold]  "
            f"[green]approved={approved}[/green]  "
            f"[red]rejected={rejected}[/red]  "
            f"[yellow]skipped={skipped}[/yellow]\n"
        )

    @staticmethod
    def _confidence_color(score: float) -> str:
        clamped = max(0.0, min(1.0, float(score)))
        if clamped >= 0.80:
            return "green"
        elif clamped >= 0.50:
            return "yellow"
        return "red"

    @staticmethod
    def _status_text(status: str) -> Text:
        """Return color-coded Rich Text for a status string."""
        if not _HAS_RICH:
            return status  # type: ignore[return-value]
        color_map = {
            "approved": "green",
            "promoted": "green",
            "rejected": "red",
            "pending": "yellow",
            "candidate": "yellow",
            "deferred": "dim yellow",
            "deleted": "dim red",
        }
        color = color_map.get(status.lower(), "white")
        return Text(status, style=f"bold {color}")
