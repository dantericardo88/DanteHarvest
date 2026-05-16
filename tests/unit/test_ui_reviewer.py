"""
Tests for ui_reviewer_workflow enhancements:
  - ReviewNavigator: keyboard navigation, decisions, summary, export
  - ReviewDiffDisplay: format_diff, format_item_for_review
"""

from __future__ import annotations

import json

import pytest

from harvest_ui.reviewer.tui_reviewer import ReviewDiffDisplay, ReviewNavigator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_items(n: int = 3):
    return [{"id": f"item-{i}", "url": f"https://example.com/{i}", "score": i * 0.1}
            for i in range(n)]


# ---------------------------------------------------------------------------
# ReviewNavigator — navigation
# ---------------------------------------------------------------------------

class TestReviewNavigatorNavigation:
    def test_next_increments_index(self):
        nav = ReviewNavigator(_make_items(3))
        assert nav.index == 0
        moved = nav.next()
        assert moved is True
        assert nav.index == 1

    def test_next_at_last_item_returns_false(self):
        nav = ReviewNavigator(_make_items(2))
        nav.next()  # index → 1 (last)
        moved = nav.next()
        assert moved is False
        assert nav.index == 1

    def test_prev_decrements_index(self):
        nav = ReviewNavigator(_make_items(3))
        nav.next()  # index → 1
        moved = nav.prev()
        assert moved is True
        assert nav.index == 0

    def test_prev_at_first_item_returns_false(self):
        nav = ReviewNavigator(_make_items(3))
        moved = nav.prev()
        assert moved is False
        assert nav.index == 0

    def test_current_returns_correct_item(self):
        items = _make_items(3)
        nav = ReviewNavigator(items)
        nav.next()
        assert nav.current() == items[1]

    def test_current_on_empty_list_returns_none(self):
        nav = ReviewNavigator([])
        assert nav.current() is None


# ---------------------------------------------------------------------------
# ReviewNavigator — decisions
# ---------------------------------------------------------------------------

class TestReviewNavigatorDecisions:
    def test_decide_accept_stores_decision(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("accept")
        assert nav.get_decisions()[0] == "accept"

    def test_decide_reject_stores_decision(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("reject")
        assert nav.get_decisions()[0] == "reject"

    def test_decide_skip_stores_decision(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("skip")
        assert nav.get_decisions()[0] == "skip"

    def test_decide_invalid_raises_value_error(self):
        nav = ReviewNavigator(_make_items(3))
        with pytest.raises(ValueError, match="Invalid decision"):
            nav.decide("invalid")

    def test_decide_approve_raises_value_error(self):
        # "approve" is not a valid decision (only accept/reject/skip)
        nav = ReviewNavigator(_make_items(3))
        with pytest.raises(ValueError):
            nav.decide("approve")

    def test_decisions_stored_per_index(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("accept")   # index 0
        nav.next()
        nav.decide("reject")   # index 1
        decisions = nav.get_decisions()
        assert decisions[0] == "accept"
        assert decisions[1] == "reject"

    def test_get_decisions_returns_copy(self):
        nav = ReviewNavigator(_make_items(2))
        nav.decide("accept")
        copy = nav.get_decisions()
        copy[0] = "reject"
        assert nav.get_decisions()[0] == "accept"


# ---------------------------------------------------------------------------
# ReviewNavigator — summary
# ---------------------------------------------------------------------------

class TestReviewNavigatorSummary:
    def test_summary_all_pending(self):
        nav = ReviewNavigator(_make_items(3))
        s = nav.get_summary()
        assert s["total"] == 3
        assert s["decided"] == 0
        assert s["pending"] == 3
        assert s["accepted"] == 0
        assert s["rejected"] == 0
        assert s["skipped"] == 0

    def test_summary_counts_accepted(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("accept")
        nav.next()
        nav.decide("accept")
        s = nav.get_summary()
        assert s["accepted"] == 2
        assert s["decided"] == 2
        assert s["pending"] == 1

    def test_summary_counts_rejected(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("reject")
        s = nav.get_summary()
        assert s["rejected"] == 1

    def test_summary_counts_skipped(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("skip")
        s = nav.get_summary()
        assert s["skipped"] == 1

    def test_summary_mixed_decisions(self):
        nav = ReviewNavigator(_make_items(3))
        nav.decide("accept")
        nav.next()
        nav.decide("reject")
        nav.next()
        nav.decide("skip")
        s = nav.get_summary()
        assert s["accepted"] == 1
        assert s["rejected"] == 1
        assert s["skipped"] == 1
        assert s["decided"] == 3
        assert s["pending"] == 0


# ---------------------------------------------------------------------------
# ReviewNavigator — export
# ---------------------------------------------------------------------------

class TestReviewNavigatorExport:
    def test_export_json_is_valid_json(self):
        nav = ReviewNavigator(_make_items(2))
        nav.decide("accept")
        output = nav.export_decisions("json")
        parsed = json.loads(output)
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    def test_export_json_contains_decisions(self):
        nav = ReviewNavigator(_make_items(2))
        nav.decide("accept")
        parsed = json.loads(nav.export_decisions("json"))
        assert parsed[0]["decision"] == "accept"
        assert parsed[1]["decision"] == "pending"

    def test_export_json_contains_item(self):
        items = _make_items(1)
        nav = ReviewNavigator(items)
        parsed = json.loads(nav.export_decisions("json"))
        assert parsed[0]["item"] == items[0]

    def test_export_csv_contains_header(self):
        nav = ReviewNavigator(_make_items(2))
        output = nav.export_decisions("csv")
        assert output.startswith("index,decision,item_id")

    def test_export_csv_has_comma_separated_data(self):
        nav = ReviewNavigator(_make_items(2))
        nav.decide("accept")
        lines = nav.export_decisions("csv").splitlines()
        assert len(lines) == 3  # header + 2 data rows
        parts = lines[1].split(",")
        assert len(parts) == 3
        assert parts[1] == "accept"

    def test_export_csv_uses_item_id(self):
        nav = ReviewNavigator([{"id": "abc-123", "score": 0.9}])
        output = nav.export_decisions("csv")
        assert "abc-123" in output


# ---------------------------------------------------------------------------
# ReviewNavigator — help text
# ---------------------------------------------------------------------------

class TestReviewNavigatorHelp:
    def test_help_text_contains_all_keys(self):
        nav = ReviewNavigator([])
        help_text = nav.get_help_text()
        for key in ReviewNavigator.KEYBINDINGS:
            assert f"[{key}]" in help_text


# ---------------------------------------------------------------------------
# ReviewDiffDisplay
# ---------------------------------------------------------------------------

class TestReviewDiffDisplay:
    def test_format_diff_added_lines_have_plus_prefix(self):
        d = ReviewDiffDisplay()
        result = d.format_diff("line1", "line1\nline2")
        assert "+ line2" in result

    def test_format_diff_removed_lines_have_minus_prefix(self):
        d = ReviewDiffDisplay()
        result = d.format_diff("line1\nline2", "line1")
        assert "- line2" in result

    def test_format_diff_common_lines_have_space_prefix(self):
        d = ReviewDiffDisplay()
        result = d.format_diff("common\nonly_a", "common\nonly_b")
        assert "  common" in result

    def test_format_diff_identical_strings(self):
        d = ReviewDiffDisplay()
        result = d.format_diff("hello", "hello")
        assert "+" not in result
        assert "-" not in result

    def test_format_item_for_review_short_value_unchanged(self):
        d = ReviewDiffDisplay()
        item = {"title": "Short title", "score": 0.9}
        result = d.format_item_for_review(item)
        assert "Short title" in result
        assert "0.9" in result

    def test_format_item_for_review_truncates_long_values(self):
        d = ReviewDiffDisplay()
        long_val = "x" * 300
        item = {"content": long_val}
        result = d.format_item_for_review(item)
        assert "..." in result
        # Should be truncated to 200 chars + "..."
        assert len(result.split("content: ")[1]) == 203  # 200 + len("...")

    def test_format_item_for_review_all_keys_present(self):
        d = ReviewDiffDisplay()
        item = {"a": "val_a", "b": "val_b"}
        result = d.format_item_for_review(item)
        assert "a: val_a" in result
        assert "b: val_b" in result
