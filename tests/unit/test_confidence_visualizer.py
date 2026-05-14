"""
Tests for harvest_ui.reviewer.confidence_visualizer — ConfidenceVisualizer.

All tests are CI-safe (no I/O, no real filesystem access, no network calls).
Rich renderables are inspected by type/content without printing.
"""

from __future__ import annotations

import pytest
from harvest_ui.reviewer.confidence_visualizer import (
    ConfidenceVisualizer,
    confidence_color,
    confidence_label,
)


# ---------------------------------------------------------------------------
# confidence_color — unit tests
# ---------------------------------------------------------------------------

class TestConfidenceColor:
    def test_high_confidence_green(self):
        assert confidence_color(0.80) == "green"

    def test_high_confidence_one(self):
        assert confidence_color(1.0) == "green"

    def test_high_confidence_above_threshold(self):
        assert confidence_color(0.95) == "green"

    def test_medium_confidence_yellow_lower_bound(self):
        assert confidence_color(0.50) == "yellow"

    def test_medium_confidence_yellow_upper_bound(self):
        assert confidence_color(0.79) == "yellow"

    def test_medium_confidence_midpoint(self):
        assert confidence_color(0.65) == "yellow"

    def test_low_confidence_red(self):
        assert confidence_color(0.0) == "red"

    def test_low_confidence_below_threshold(self):
        assert confidence_color(0.49) == "red"

    def test_clamp_above_one(self):
        # Values > 1.0 should be treated as 1.0 (green)
        assert confidence_color(1.5) == "green"

    def test_clamp_negative(self):
        # Negative values should be treated as 0.0 (red)
        assert confidence_color(-0.1) == "red"

    def test_boundary_exactly_080(self):
        assert confidence_color(0.80) == "green"

    def test_boundary_just_below_080(self):
        assert confidence_color(0.799) == "yellow"


# ---------------------------------------------------------------------------
# confidence_label — unit tests
# ---------------------------------------------------------------------------

class TestConfidenceLabel:
    def test_high_label(self):
        assert confidence_label(0.80) == "HIGH"

    def test_high_label_perfect(self):
        assert confidence_label(1.0) == "HIGH"

    def test_medium_label_lower(self):
        assert confidence_label(0.50) == "MEDIUM"

    def test_medium_label_upper(self):
        assert confidence_label(0.799) == "MEDIUM"

    def test_low_label_zero(self):
        assert confidence_label(0.0) == "LOW"

    def test_low_label_below_threshold(self):
        assert confidence_label(0.49) == "LOW"

    def test_clamp_over_one(self):
        assert confidence_label(2.0) == "HIGH"

    def test_clamp_negative(self):
        assert confidence_label(-1.0) == "LOW"


# ---------------------------------------------------------------------------
# ConfidenceVisualizer.render_bar
# ---------------------------------------------------------------------------

class TestRenderBar:
    def setup_method(self):
        self.vis = ConfidenceVisualizer()

    def test_render_bar_returns_something(self):
        result = self.vis.render_bar(0.75)
        assert result is not None

    def test_render_bar_label_included(self):
        result = self.vis.render_bar(0.75, label="my-pack")
        # For rich.text.Text, check plain text; for str, check substring
        text = result.plain if hasattr(result, "plain") else str(result)
        assert "my-pack" in text

    def test_render_bar_score_percentage_shown(self):
        result = self.vis.render_bar(0.5, label="")
        text = result.plain if hasattr(result, "plain") else str(result)
        assert "50" in text

    def test_render_bar_full_score(self):
        result = self.vis.render_bar(1.0, label="")
        text = result.plain if hasattr(result, "plain") else str(result)
        assert "100" in text

    def test_render_bar_zero_score(self):
        result = self.vis.render_bar(0.0, label="")
        text = result.plain if hasattr(result, "plain") else str(result)
        assert "0" in text

    def test_score_to_percentage_50(self):
        assert self.vis.score_to_percentage(0.5) == 50

    def test_score_to_percentage_100(self):
        assert self.vis.score_to_percentage(1.0) == 100

    def test_score_to_percentage_zero(self):
        assert self.vis.score_to_percentage(0.0) == 0

    def test_score_to_percentage_clamp_high(self):
        assert self.vis.score_to_percentage(2.0) == 100

    def test_score_to_percentage_clamp_negative(self):
        assert self.vis.score_to_percentage(-0.5) == 0


# ---------------------------------------------------------------------------
# ConfidenceVisualizer.render_table
# ---------------------------------------------------------------------------

class TestRenderTable:
    def setup_method(self):
        self.vis = ConfidenceVisualizer()
        self.packs = [
            {"pack_id": "a", "confidence_score": 0.9},
            {"pack_id": "b", "confidence_score": 0.6},
            {"pack_id": "c", "confidence_score": 0.2},
        ]

    def test_render_table_empty_list(self):
        # Should not raise for empty input
        result = self.vis.render_table([])
        assert result is not None

    def test_render_table_returns_table(self):
        result = self.vis.render_table(self.packs)
        assert result is not None

    def test_render_table_type(self):
        from rich.table import Table
        result = self.vis.render_table(self.packs)
        assert isinstance(result, Table)

    def test_render_table_row_count(self):
        from rich.table import Table
        result = self.vis.render_table(self.packs)
        assert isinstance(result, Table)
        assert result.row_count == 3

    def test_render_table_extra_columns(self):
        packs = [{"pack_id": "x", "confidence_score": 0.5, "status": "pending"}]
        result = self.vis.render_table(packs, extra_columns=["status"])
        # Should include status in the table columns
        from rich.table import Table
        assert isinstance(result, Table)

    def test_render_table_custom_id_key(self):
        packs = [{"id": "custom-id", "confidence_score": 0.7}]
        result = self.vis.render_table(packs, id_key="id")
        from rich.table import Table
        assert isinstance(result, Table)

    def test_render_table_custom_score_key(self):
        packs = [{"pack_id": "z", "my_score": 0.8}]
        result = self.vis.render_table(packs, score_key="my_score")
        from rich.table import Table
        assert isinstance(result, Table)


# ---------------------------------------------------------------------------
# ConfidenceVisualizer.summarize
# ---------------------------------------------------------------------------

class TestSummarize:
    def setup_method(self):
        self.vis = ConfidenceVisualizer()

    def test_summarize_empty(self):
        result = self.vis.summarize([])
        assert result == {"HIGH": 0, "MEDIUM": 0, "LOW": 0}

    def test_summarize_all_high(self):
        packs = [{"confidence_score": 0.9}, {"confidence_score": 1.0}]
        result = self.vis.summarize(packs)
        assert result["HIGH"] == 2
        assert result["MEDIUM"] == 0
        assert result["LOW"] == 0

    def test_summarize_mixed(self):
        packs = [
            {"confidence_score": 0.9},
            {"confidence_score": 0.6},
            {"confidence_score": 0.1},
        ]
        result = self.vis.summarize(packs)
        assert result["HIGH"] == 1
        assert result["MEDIUM"] == 1
        assert result["LOW"] == 1

    def test_summarize_all_low(self):
        packs = [{"confidence_score": 0.0}, {"confidence_score": 0.3}]
        result = self.vis.summarize(packs)
        assert result["LOW"] == 2

    def test_summarize_missing_score_defaults_to_zero(self):
        packs = [{"pack_id": "x"}]
        result = self.vis.summarize(packs)
        assert result["LOW"] == 1
