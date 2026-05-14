"""
Tests for harvest_ui.reviewer.diff_viewer — PackDiffViewer.

All tests are CI-safe (no I/O, no filesystem, no network).
"""

from __future__ import annotations

import json
import pytest

from harvest_ui.reviewer.diff_viewer import PackDiffViewer


# ---------------------------------------------------------------------------
# _to_lines / _to_text normalization
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_none_to_lines_empty(self):
        assert PackDiffViewer._to_lines(None) == []

    def test_none_to_text_empty(self):
        assert PackDiffViewer._to_text(None) == ""

    def test_string_to_text(self):
        assert PackDiffViewer._to_text("hello") == "hello"

    def test_dict_to_text_is_json(self):
        result = PackDiffViewer._to_text({"a": 1})
        parsed = json.loads(result)
        assert parsed == {"a": 1}

    def test_dict_to_text_sorted_keys(self):
        result = PackDiffViewer._to_text({"b": 2, "a": 1})
        parsed = json.loads(result)
        assert list(parsed.keys()) == sorted(parsed.keys())

    def test_list_to_text_is_json(self):
        result = PackDiffViewer._to_text([1, 2, 3])
        assert json.loads(result) == [1, 2, 3]

    def test_int_to_text(self):
        result = PackDiffViewer._to_text(42)
        assert result == "42"

    def test_string_to_lines_preserves_content(self):
        lines = PackDiffViewer._to_lines("line1\nline2\n")
        assert len(lines) == 2
        assert lines[0] == "line1\n"
        assert lines[1] == "line2\n"


# ---------------------------------------------------------------------------
# diff — string output
# ---------------------------------------------------------------------------

class TestDiff:
    def setup_method(self):
        self.viewer = PackDiffViewer(context_lines=0)

    def test_identical_strings_empty_diff(self):
        result = self.viewer.diff("hello", "hello")
        assert result == ""

    def test_identical_dicts_empty_diff(self):
        result = self.viewer.diff({"a": 1}, {"a": 1})
        assert result == ""

    def test_none_none_empty_diff(self):
        result = self.viewer.diff(None, None)
        assert result == ""

    def test_diff_detects_change(self):
        result = self.viewer.diff("hello", "world")
        assert result != ""

    def test_diff_shows_added_content(self):
        result = self.viewer.diff("", "new line\n")
        assert "+" in result
        assert "new line" in result

    def test_diff_shows_removed_content(self):
        result = self.viewer.diff("old line\n", "")
        assert "-" in result
        assert "old line" in result

    def test_diff_labels_in_header(self):
        result = self.viewer.diff("a", "b", label_a="v1", label_b="v2")
        assert "v1" in result
        assert "v2" in result

    def test_diff_is_unified_format(self):
        result = self.viewer.diff("line1\n", "line2\n")
        # Unified diff always starts with ---
        assert result.startswith("---")

    def test_diff_dict_vs_dict(self):
        a = {"pack_id": "x", "score": 0.5}
        b = {"pack_id": "x", "score": 0.9}
        result = self.viewer.diff(a, b)
        assert result != ""
        assert "0.5" in result or "0.9" in result

    def test_diff_none_vs_string(self):
        result = self.viewer.diff(None, "some content")
        assert result != ""

    def test_diff_string_vs_none(self):
        result = self.viewer.diff("some content", None)
        assert result != ""


# ---------------------------------------------------------------------------
# has_changes
# ---------------------------------------------------------------------------

class TestHasChanges:
    def setup_method(self):
        self.viewer = PackDiffViewer()

    def test_identical_strings_no_change(self):
        assert self.viewer.has_changes("same", "same") is False

    def test_different_strings_has_change(self):
        assert self.viewer.has_changes("a", "b") is True

    def test_none_and_none_no_change(self):
        assert self.viewer.has_changes(None, None) is False

    def test_identical_dicts_no_change(self):
        assert self.viewer.has_changes({"a": 1}, {"a": 1}) is False

    def test_different_dicts_has_change(self):
        assert self.viewer.has_changes({"a": 1}, {"a": 2}) is True


# ---------------------------------------------------------------------------
# changed_keys
# ---------------------------------------------------------------------------

class TestChangedKeys:
    def setup_method(self):
        self.viewer = PackDiffViewer()

    def test_no_changes(self):
        result = self.viewer.changed_keys({"a": 1}, {"a": 1})
        assert result == []

    def test_changed_value(self):
        result = self.viewer.changed_keys({"a": 1, "b": 2}, {"a": 1, "b": 3})
        assert result == ["b"]

    def test_added_key(self):
        result = self.viewer.changed_keys({"a": 1}, {"a": 1, "b": 2})
        assert "b" in result

    def test_removed_key(self):
        result = self.viewer.changed_keys({"a": 1, "b": 2}, {"a": 1})
        assert "b" in result

    def test_non_dict_inputs_return_empty(self):
        assert self.viewer.changed_keys("string", {"a": 1}) == []
        assert self.viewer.changed_keys({"a": 1}, "string") == []

    def test_empty_dicts(self):
        assert self.viewer.changed_keys({}, {}) == []

    def test_result_sorted(self):
        result = self.viewer.changed_keys({"z": 1, "a": 2}, {"z": 2, "a": 1})
        assert result == sorted(result)


# ---------------------------------------------------------------------------
# render_rich
# ---------------------------------------------------------------------------

class TestRenderRich:
    def setup_method(self):
        self.viewer = PackDiffViewer()

    def test_empty_diff_returns_panel(self):
        from rich.panel import Panel
        result = self.viewer.render_rich("")
        assert isinstance(result, Panel)

    def test_nonempty_diff_returns_panel(self):
        from rich.panel import Panel
        diff_str = self.viewer.diff("a\n", "b\n")
        result = self.viewer.render_rich(diff_str)
        assert isinstance(result, Panel)

    def test_empty_diff_panel_border_green(self):
        from rich.panel import Panel
        result = self.viewer.render_rich("")
        assert isinstance(result, Panel)
        assert result.border_style == "green"

    def test_nonempty_diff_panel_border_yellow(self):
        from rich.panel import Panel
        diff_str = self.viewer.diff("x\n", "y\n")
        result = self.viewer.render_rich(diff_str)
        assert result.border_style == "yellow"

    def test_render_side_by_side_returns_columns(self):
        from rich.columns import Columns
        result = self.viewer.render_side_by_side({"a": 1}, {"a": 2})
        assert isinstance(result, Columns)

    def test_render_side_by_side_none_inputs(self):
        # Should not raise
        result = self.viewer.render_side_by_side(None, None)
        assert result is not None

    def test_context_lines_parameter(self):
        viewer = PackDiffViewer(context_lines=5)
        diff = viewer.diff("a\n", "b\n")
        assert diff != ""
