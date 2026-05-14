"""
harvest_ui.reviewer.confidence_visualizer — Rich-powered confidence band visualizer.

Renders confidence scores as color-coded progress bars and summary tables so
reviewers can quickly triage packs without reading raw floats.

Color thresholds (matching server._confidence_band):
  GREEN  ≥ 0.80  — high confidence, likely approve
  YELLOW 0.50–0.79 — moderate, needs review
  RED    < 0.50  — low confidence, likely reject

Constitutional guarantees:
- Fail-closed: all edge-case scores (0.0, 1.0, negative, >1) produce valid output
- Zero-ambiguity: color labels are exact strings ("green"/"yellow"/"red")
- No I/O side-effects: all methods return Rich renderables; caller does print()
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# Rich imports with graceful fallback
# ---------------------------------------------------------------------------

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, TaskID, TextColumn
    from rich.table import Table
    from rich.text import Text
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

# Shared console (no-op friendly)
_console: Any = None
if _HAS_RICH:
    _console = Console()


# ---------------------------------------------------------------------------
# Color thresholds
# ---------------------------------------------------------------------------

def confidence_color(score: float) -> str:
    """
    Return Rich color name for a confidence score.

    - "green"  → score >= 0.80
    - "yellow" → 0.50 <= score < 0.80
    - "red"    → score < 0.50

    Scores outside [0, 1] are clamped before comparison.
    """
    clamped = max(0.0, min(1.0, float(score)))
    if clamped >= 0.80:
        return "green"
    elif clamped >= 0.50:
        return "yellow"
    return "red"


def confidence_label(score: float) -> str:
    """
    Return band label string for a confidence score.

    - "HIGH"   → score >= 0.80
    - "MEDIUM" → 0.50 <= score < 0.80
    - "LOW"    → score < 0.50
    """
    clamped = max(0.0, min(1.0, float(score)))
    if clamped >= 0.80:
        return "HIGH"
    elif clamped >= 0.50:
        return "MEDIUM"
    return "LOW"


# ---------------------------------------------------------------------------
# ConfidenceVisualizer
# ---------------------------------------------------------------------------

class ConfidenceVisualizer:
    """
    Renders confidence scores using Rich progress bars and color-coded labels.

    Usage::

        vis = ConfidenceVisualizer()
        bar = vis.render_bar(0.72, label="pack-abc")
        console.print(bar)

        table = vis.render_table(packs)
        console.print(table)
    """

    def __init__(self, console: Optional[Any] = None) -> None:
        self._console = console or _console

    # ------------------------------------------------------------------
    # Single-bar rendering
    # ------------------------------------------------------------------

    def render_bar(self, score: float, label: str = "") -> Any:
        """
        Render a single confidence score as a Rich Text progress bar.

        Returns a rich.text.Text object with an inline ASCII bar if Rich is
        available, or a plain string otherwise.

        Args:
            score: Confidence score in [0, 1].
            label: Optional label prefix displayed before the bar.
        """
        clamped = max(0.0, min(1.0, float(score)))
        color = confidence_color(clamped)
        band = confidence_label(clamped)
        pct = int(clamped * 100)
        filled = int(clamped * 20)
        bar_chars = "█" * filled + "░" * (20 - filled)

        if _HAS_RICH:
            text = Text()
            if label:
                text.append(f"{label:<30} ", style="bold")
            text.append(f"[{bar_chars}]", style=color)
            text.append(f" {pct:3d}% ", style=f"bold {color}")
            text.append(f"({band})", style=f"dim {color}")
            return text
        else:
            prefix = f"{label:<30} " if label else ""
            return f"{prefix}[{bar_chars}] {pct:3d}% ({band})"

    def score_to_percentage(self, score: float) -> int:
        """Return integer percentage (0–100) for a score, clamped."""
        return int(max(0.0, min(1.0, float(score))) * 100)

    # ------------------------------------------------------------------
    # Table rendering
    # ------------------------------------------------------------------

    def render_table(
        self,
        packs: Sequence[Dict[str, Any]],
        id_key: str = "pack_id",
        score_key: str = "confidence_score",
        extra_columns: Optional[List[str]] = None,
    ) -> Any:
        """
        Render a Rich Table of packs with a confidence column.

        Each row shows: id | confidence bar | label | extra_columns...

        Args:
            packs: List of pack dicts (must have id_key and score_key).
            id_key: Dict key for pack identifier.
            score_key: Dict key for confidence score (float).
            extra_columns: Optional list of additional column keys to include.

        Returns:
            rich.table.Table if Rich available, else a list of plain strings.
        """
        extra_columns = extra_columns or []

        if _HAS_RICH:
            table = Table(title="Confidence Review", show_header=True, header_style="bold cyan")
            table.add_column("Pack ID", style="bold", overflow="fold")
            table.add_column("Confidence", min_width=25)
            table.add_column("Band", justify="center")
            for col in extra_columns:
                table.add_column(col.replace("_", " ").title(), overflow="fold")

            for pack in packs:
                score = float(pack.get(score_key, 0.0))
                color = confidence_color(score)
                band = confidence_label(score)
                bar = self.render_bar(score)

                extra_vals = [str(pack.get(col, "")) for col in extra_columns]
                table.add_row(
                    str(pack.get(id_key, "")),
                    bar,
                    Text(band, style=f"bold {color}"),
                    *extra_vals,
                    style=None,
                )
            return table
        else:
            lines = []
            header = f"{'Pack ID':<30}  {'Score':>6}  {'Band':<8}"
            for col in extra_columns:
                header += f"  {col:<20}"
            lines.append(header)
            lines.append("-" * len(header))
            for pack in packs:
                score = float(pack.get(score_key, 0.0))
                band = confidence_label(score)
                pct = self.score_to_percentage(score)
                row = f"{str(pack.get(id_key, '')):<30}  {pct:5d}%  {band:<8}"
                for col in extra_columns:
                    row += f"  {str(pack.get(col, '')):<20}"
                lines.append(row)
            return lines

    # ------------------------------------------------------------------
    # Batch summary
    # ------------------------------------------------------------------

    def summarize(self, packs: Sequence[Dict[str, Any]], score_key: str = "confidence_score") -> Dict[str, int]:
        """
        Return counts per band: {"HIGH": n, "MEDIUM": n, "LOW": n}.

        Args:
            packs: Sequence of pack dicts.
            score_key: Dict key for confidence score.
        """
        counts: Dict[str, int] = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for pack in packs:
            score = float(pack.get(score_key, 0.0))
            label = confidence_label(score)
            counts[label] = counts.get(label, 0) + 1
        return counts
