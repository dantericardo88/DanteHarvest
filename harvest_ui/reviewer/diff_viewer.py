"""
harvest_ui.reviewer.diff_viewer — Side-by-side diff viewer for pack versions.

Compares two pack dicts (or serialized content strings) and renders the diff
using difflib.unified_diff, displayed via Rich Syntax panels.

Constitutional guarantees:
- Fail-closed: all inputs normalized to strings before diff; never raises on
  empty or None content
- Zero-ambiguity: diff output is standard unified-diff format (patch-compatible)
- No I/O side-effects: render_rich() returns a Rich Panel; caller does print()
"""

from __future__ import annotations

import difflib
import json
from typing import Any, Dict, Optional, Union


# ---------------------------------------------------------------------------
# Rich imports with graceful fallback
# ---------------------------------------------------------------------------

try:
    from rich.panel import Panel
    from rich.syntax import Syntax
    from rich.columns import Columns
    from rich.text import Text
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False


# ---------------------------------------------------------------------------
# PackDiffViewer
# ---------------------------------------------------------------------------

class PackDiffViewer:
    """
    Generates and renders unified diffs between two pack versions.

    Usage::

        viewer = PackDiffViewer()
        diff_str = viewer.diff(pack_a, pack_b)
        panel = viewer.render_rich(diff_str, label_a="v1", label_b="v2")
        console.print(panel)

    Pack inputs can be:
      - dict  → JSON-serialized before diff
      - str   → used directly
      - None  → treated as empty string
    """

    def __init__(self, context_lines: int = 3) -> None:
        """
        Args:
            context_lines: Number of unchanged context lines shown around diffs.
        """
        self.context_lines = context_lines

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _to_lines(pack: Any) -> list[str]:
        """
        Normalize a pack to a list of newline-terminated strings.

        - dict/list → JSON pretty-printed
        - str       → split on newlines
        - None      → empty list
        - other     → str() conversion
        """
        if pack is None:
            return []
        if isinstance(pack, (dict, list)):
            text = json.dumps(pack, indent=2, sort_keys=True, default=str)
        elif isinstance(pack, str):
            text = pack
        else:
            text = str(pack)
        # Ensure every line ends with newline for proper unified diff output
        lines = text.splitlines(keepends=True)
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        return lines

    @staticmethod
    def _to_text(pack: Any) -> str:
        """Normalize pack to a single string (no trailing newline stripping)."""
        if pack is None:
            return ""
        if isinstance(pack, (dict, list)):
            return json.dumps(pack, indent=2, sort_keys=True, default=str)
        if isinstance(pack, str):
            return pack
        return str(pack)

    # ------------------------------------------------------------------
    # Diff generation
    # ------------------------------------------------------------------

    def diff(
        self,
        pack_a: Any,
        pack_b: Any,
        label_a: str = "version-a",
        label_b: str = "version-b",
    ) -> str:
        """
        Generate a unified diff string between two pack versions.

        Returns an empty string if the packs are identical.

        Args:
            pack_a: Original pack (dict, str, or None).
            pack_b: New pack (dict, str, or None).
            label_a: Header label for the original version.
            label_b: Header label for the new version.

        Returns:
            Unified diff string (may be empty if no differences).
        """
        lines_a = self._to_lines(pack_a)
        lines_b = self._to_lines(pack_b)

        diff_lines = list(difflib.unified_diff(
            lines_a,
            lines_b,
            fromfile=label_a,
            tofile=label_b,
            n=self.context_lines,
        ))
        return "".join(diff_lines)

    def has_changes(self, pack_a: Any, pack_b: Any) -> bool:
        """Return True if the two packs differ."""
        return self._to_text(pack_a) != self._to_text(pack_b)

    def changed_keys(self, pack_a: Dict, pack_b: Dict) -> list[str]:
        """
        Return list of top-level dict keys that differ between two pack dicts.

        Returns empty list if either input is not a dict.
        """
        if not isinstance(pack_a, dict) or not isinstance(pack_b, dict):
            return []
        all_keys = set(pack_a.keys()) | set(pack_b.keys())
        return sorted(k for k in all_keys if pack_a.get(k) != pack_b.get(k))

    # ------------------------------------------------------------------
    # Rich rendering
    # ------------------------------------------------------------------

    def render_rich(
        self,
        diff_str: str,
        label_a: str = "version-a",
        label_b: str = "version-b",
        title: str = "Pack Diff",
    ) -> Any:
        """
        Render a unified diff string as a Rich Syntax panel.

        Args:
            diff_str: Output from self.diff().
            label_a: Label for the original version (shown in panel title).
            label_b: Label for the new version (shown in panel title).
            title: Panel title.

        Returns:
            rich.panel.Panel if Rich available, else a plain string.
        """
        if not diff_str:
            if _HAS_RICH:
                return Panel(
                    Text("(no differences)", style="dim green"),
                    title=f"{title}: {label_a} → {label_b}",
                    border_style="green",
                )
            return f"--- {title}: {label_a} → {label_b} ---\n(no differences)\n"

        if _HAS_RICH:
            syntax = Syntax(
                diff_str,
                "diff",
                theme="monokai",
                line_numbers=True,
                word_wrap=False,
            )
            return Panel(
                syntax,
                title=f"{title}: [bold]{label_a}[/bold] → [bold]{label_b}[/bold]",
                border_style="yellow",
                padding=(0, 1),
            )
        else:
            header = f"--- {title}: {label_a} → {label_b} ---"
            return f"{header}\n{diff_str}"

    def render_side_by_side(
        self,
        pack_a: Any,
        pack_b: Any,
        label_a: str = "version-a",
        label_b: str = "version-b",
    ) -> Any:
        """
        Render two packs side-by-side as Rich Syntax panels.

        Args:
            pack_a: Original pack.
            pack_b: New pack.
            label_a: Label for original.
            label_b: Label for new.

        Returns:
            rich.columns.Columns with two panels if Rich available, else a plain string.
        """
        text_a = self._to_text(pack_a)
        text_b = self._to_text(pack_b)

        if _HAS_RICH:
            panel_a = Panel(
                Syntax(text_a or "(empty)", "json", theme="monokai"),
                title=f"[red]{label_a}[/red]",
                border_style="red",
            )
            panel_b = Panel(
                Syntax(text_b or "(empty)", "json", theme="monokai"),
                title=f"[green]{label_b}[/green]",
                border_style="green",
            )
            return Columns([panel_a, panel_b])
        else:
            sep = "-" * 40
            return (
                f"{sep}\n{label_a}\n{sep}\n{text_a}\n\n"
                f"{sep}\n{label_b}\n{sep}\n{text_b}\n"
            )
