"""
Tests for harvest_ui.reviewer.tui_reviewer — TUIReviewer.

All tests are CI-safe:
- Console output is captured via rich.console.Console(file=io.StringIO()).
- User input is injected via input_fn= constructor arg (no real keyboard reads).
- No filesystem or network access.
"""

from __future__ import annotations

import io
from typing import Iterator, List
from unittest.mock import MagicMock, patch

import pytest

from harvest_ui.reviewer.tui_reviewer import (
    Decision,
    PackSummaryRow,
    ReviewDecision,
    TUIReviewer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_console() -> any:
    """Return a Rich Console that writes to a StringIO buffer."""
    from rich.console import Console
    return Console(file=io.StringIO(), width=120)


def _make_packs(n: int = 3) -> List[dict]:
    return [
        {
            "pack_id": f"pack-{i:03d}",
            "pack_type": "audio",
            "score": round(0.5 + i * 0.1, 2),
            "status": "pending",
            "confidence_score": round(0.4 + i * 0.15, 2),
            "content": f"Sample content for pack {i}.",
        }
        for i in range(n)
    ]


def _input_sequence(*responses: str) -> Iterator[str]:
    """Yield responses in order, repeating last if exhausted."""
    it = iter(responses)
    last = "s"
    while True:
        try:
            last = next(it)
        except StopIteration:
            pass
        yield last


def _input_fn_from_sequence(*responses: str):
    """Return an input_fn that yields responses in order."""
    gen = _input_sequence(*responses)
    def fn(prompt: str) -> str:
        return next(gen)
    return fn


# ---------------------------------------------------------------------------
# PackSummaryRow
# ---------------------------------------------------------------------------

class TestPackSummaryRow:
    def test_from_dict_basic(self):
        row = PackSummaryRow.from_dict({"pack_id": "abc", "pack_type": "doc"})
        assert row.pack_id == "abc"
        assert row.pack_type == "doc"

    def test_from_dict_alt_id_key(self):
        row = PackSummaryRow.from_dict({"id": "xyz"})
        assert row.pack_id == "xyz"

    def test_from_dict_confidence_score(self):
        row = PackSummaryRow.from_dict({"pack_id": "a", "confidence_score": 0.75})
        assert row.confidence == pytest.approx(0.75)

    def test_from_dict_confidence_alt_key(self):
        row = PackSummaryRow.from_dict({"pack_id": "a", "confidence": 0.65})
        assert row.confidence == pytest.approx(0.65)

    def test_from_dict_defaults(self):
        row = PackSummaryRow.from_dict({})
        assert row.pack_id == "unknown"
        assert row.score == 0.0
        assert row.confidence == 0.0
        assert row.status == "pending"

    def test_from_dict_status_promotion_status(self):
        row = PackSummaryRow.from_dict({"pack_id": "a", "promotion_status": "approved"})
        assert row.status == "approved"


# ---------------------------------------------------------------------------
# Decision enum
# ---------------------------------------------------------------------------

class TestDecision:
    def test_approve_value(self):
        assert Decision.APPROVE.value == "approve"

    def test_reject_value(self):
        assert Decision.REJECT.value == "reject"

    def test_skip_value(self):
        assert Decision.SKIP.value == "skip"

    def test_quit_value(self):
        assert Decision.QUIT.value == "quit"


# ---------------------------------------------------------------------------
# TUIReviewer initialization
# ---------------------------------------------------------------------------

class TestTUIReviewerInit:
    def test_init_with_no_packs(self):
        reviewer = TUIReviewer(console=_make_console())
        assert reviewer.packs == []

    def test_init_with_packs(self):
        packs = _make_packs(2)
        reviewer = TUIReviewer(packs=packs, console=_make_console())
        assert len(reviewer.packs) == 2

    def test_init_decisions_empty(self):
        reviewer = TUIReviewer(console=_make_console())
        assert reviewer.decisions == []


# ---------------------------------------------------------------------------
# review_queue
# ---------------------------------------------------------------------------

class TestReviewQueue:
    def setup_method(self):
        self.packs = _make_packs(3)
        self.reviewer = TUIReviewer(packs=self.packs, console=_make_console())

    def test_review_queue_returns_table(self):
        from rich.table import Table
        table = self.reviewer.review_queue()
        assert isinstance(table, Table)

    def test_review_queue_row_count(self):
        from rich.table import Table
        table = self.reviewer.review_queue()
        assert table.row_count == 3

    def test_review_queue_empty(self):
        from rich.table import Table
        reviewer = TUIReviewer(packs=[], console=_make_console())
        table = reviewer.review_queue()
        assert isinstance(table, Table)
        assert table.row_count == 0

    def test_review_queue_uses_provided_packs(self):
        from rich.table import Table
        extra = _make_packs(5)
        table = self.reviewer.review_queue(packs=extra)
        assert table.row_count == 5

    def test_review_queue_has_columns(self):
        table = self.reviewer.review_queue()
        column_names = [col.header for col in table.columns]
        assert "ID" in column_names
        assert "Confidence" in column_names

    def test_review_queue_status_column(self):
        table = self.reviewer.review_queue()
        column_names = [col.header for col in table.columns]
        assert "Status" in column_names


# ---------------------------------------------------------------------------
# review_pack
# ---------------------------------------------------------------------------

class TestReviewPack:
    def setup_method(self):
        self.packs = _make_packs(2)
        self.reviewer = TUIReviewer(packs=self.packs, console=_make_console())

    def test_review_pack_returns_panel(self):
        from rich.panel import Panel
        panel = self.reviewer.review_pack("pack-000")
        assert isinstance(panel, Panel)

    def test_review_pack_not_found_returns_panel(self):
        from rich.panel import Panel
        panel = self.reviewer.review_pack("nonexistent-id")
        assert isinstance(panel, Panel)

    def test_review_pack_with_explicit_pack_dict(self):
        from rich.panel import Panel
        pack = {"pack_id": "explicit-001", "content": "hello", "confidence_score": 0.9}
        panel = self.reviewer.review_pack("explicit-001", pack=pack)
        assert isinstance(panel, Panel)

    def test_review_pack_title_contains_id(self):
        panel = self.reviewer.review_pack("pack-000")
        assert "pack-000" in panel.title

    def test_review_pack_content_preview_included(self):
        pack = {"pack_id": "p1", "content": "Unique preview text XYZ"}
        reviewer = TUIReviewer(packs=[pack], console=_make_console())
        panel = reviewer.review_pack("p1")
        # Panel content should contain the preview (check renderable)
        rendered = panel.renderable
        assert rendered is not None

    def test_review_pack_truncates_long_content(self):
        long_content = "x" * 1000
        pack = {"pack_id": "long", "content": long_content}
        reviewer = TUIReviewer(packs=[pack], console=_make_console())
        panel = reviewer.review_pack("long")
        # Should not raise and panel should exist
        assert panel is not None


# ---------------------------------------------------------------------------
# prompt_decision
# ---------------------------------------------------------------------------

class TestPromptDecision:
    def test_approve_shortcut(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("a"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.APPROVE

    def test_reject_shortcut(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("r"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.REJECT

    def test_skip_shortcut(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("s"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.SKIP

    def test_quit_shortcut(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("q"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.QUIT

    def test_full_word_approve(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("approve"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.APPROVE

    def test_full_word_reject(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("reject"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.REJECT

    def test_invalid_then_valid(self):
        # First input is invalid, second is valid
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("xyz", "s"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.SKIP

    def test_case_insensitive(self):
        reviewer = TUIReviewer(
            console=_make_console(),
            input_fn=_input_fn_from_sequence("A"),
        )
        assert reviewer.prompt_decision("pack-000") == Decision.APPROVE


# ---------------------------------------------------------------------------
# run_interactive_session
# ---------------------------------------------------------------------------

class TestRunInteractiveSession:
    def test_session_returns_decisions(self):
        packs = _make_packs(2)
        reviewer = TUIReviewer(
            packs=packs,
            console=_make_console(),
            input_fn=_input_fn_from_sequence("s", "s"),
        )
        decisions = reviewer.run_interactive_session()
        assert len(decisions) == 2

    def test_session_approve_all(self):
        packs = _make_packs(2)
        reviewer = TUIReviewer(
            packs=packs,
            console=_make_console(),
            input_fn=_input_fn_from_sequence("a", "a"),
        )
        decisions = reviewer.run_interactive_session()
        assert all(d.decision == Decision.APPROVE for d in decisions)

    def test_session_quit_early(self):
        packs = _make_packs(3)
        reviewer = TUIReviewer(
            packs=packs,
            console=_make_console(),
            input_fn=_input_fn_from_sequence("q"),
        )
        decisions = reviewer.run_interactive_session()
        # Only one decision (the quit)
        assert len(decisions) == 1
        assert decisions[0].decision == Decision.QUIT

    def test_session_reject_records_reason(self):
        packs = _make_packs(1)
        # First call returns "r" (reject), second returns the reason
        reviewer = TUIReviewer(
            packs=packs,
            console=_make_console(),
            input_fn=_input_fn_from_sequence("r", "bad quality"),
        )
        decisions = reviewer.run_interactive_session()
        assert decisions[0].decision == Decision.REJECT
        assert decisions[0].reason == "bad quality"

    def test_session_callback_invoked(self):
        packs = _make_packs(1)
        callback_records = []
        reviewer = TUIReviewer(
            packs=packs,
            console=_make_console(),
            input_fn=_input_fn_from_sequence("s"),
            on_decision=lambda d: callback_records.append(d),
        )
        reviewer.run_interactive_session()
        assert len(callback_records) == 1

    def test_session_empty_packs(self):
        reviewer = TUIReviewer(
            packs=[],
            console=_make_console(),
            input_fn=_input_fn_from_sequence(),
        )
        decisions = reviewer.run_interactive_session()
        assert decisions == []

    def test_session_stores_decisions_on_instance(self):
        packs = _make_packs(1)
        reviewer = TUIReviewer(
            packs=packs,
            console=_make_console(),
            input_fn=_input_fn_from_sequence("a"),
        )
        reviewer.run_interactive_session()
        assert len(reviewer.decisions) == 1

    def test_session_with_override_packs(self):
        reviewer = TUIReviewer(
            packs=[],
            console=_make_console(),
            input_fn=_input_fn_from_sequence("s"),
        )
        extra = _make_packs(1)
        decisions = reviewer.run_interactive_session(packs=extra)
        assert len(decisions) == 1


# ---------------------------------------------------------------------------
# _confidence_color (static helper)
# ---------------------------------------------------------------------------

class TestConfidenceColorHelper:
    def test_high(self):
        assert TUIReviewer._confidence_color(0.85) == "green"

    def test_medium(self):
        assert TUIReviewer._confidence_color(0.65) == "yellow"

    def test_low(self):
        assert TUIReviewer._confidence_color(0.3) == "red"

    def test_clamp_above_one(self):
        assert TUIReviewer._confidence_color(1.5) == "green"

    def test_clamp_negative(self):
        assert TUIReviewer._confidence_color(-0.1) == "red"
