"""
Tests for session_replay_debuggability enhancements:
  - ReplayBreakpoint (step_index and condition modes)
  - ReplayHarness.diff_step_results()
  - ReplayHarness.replay_with_debug()
  - ReplayHarness.set_speed()
  - ReplayDiffer.diff_sessions()
"""

from __future__ import annotations

import pytest

from harvest_index.registry.replay_harness import (
    ReplayBreakpoint,
    ReplayDiffer,
    ReplayHarness,
    StepResult,
)


# ---------------------------------------------------------------------------
# ReplayBreakpoint
# ---------------------------------------------------------------------------

class TestReplayBreakpoint:
    def test_step_index_match_returns_true(self):
        bp = ReplayBreakpoint(step_index=2)
        assert bp.should_break(2, {}) is True

    def test_step_index_no_match_returns_false(self):
        bp = ReplayBreakpoint(step_index=2)
        assert bp.should_break(1, {}) is False

    def test_condition_match_returns_true(self):
        bp = ReplayBreakpoint(condition=lambda s: s.get("action") == "click")
        assert bp.should_break(0, {"action": "click"}) is True

    def test_condition_no_match_returns_false(self):
        bp = ReplayBreakpoint(condition=lambda s: s.get("action") == "click")
        assert bp.should_break(0, {"action": "scroll"}) is False

    def test_no_criteria_always_false(self):
        bp = ReplayBreakpoint()
        assert bp.should_break(5, {"action": "anything"}) is False

    def test_step_index_and_condition_either_triggers(self):
        bp = ReplayBreakpoint(step_index=3, condition=lambda s: s.get("type") == "nav")
        # condition matches even though index doesn't
        assert bp.should_break(0, {"type": "nav"}) is True
        # index matches even though condition doesn't
        assert bp.should_break(3, {"type": "click"}) is True


# ---------------------------------------------------------------------------
# ReplayHarness.diff_step_results
# ---------------------------------------------------------------------------

class TestDiffStepResults:
    def _make_harness(self) -> ReplayHarness:
        return ReplayHarness()

    def test_identical_results_no_diff(self):
        h = self._make_harness()
        r = StepResult(step_id="s1", action="click", passed=True, output={"simulated": True})
        result = h.diff_step_results(r, r)
        assert result["has_diff"] is False
        assert result["diffs"] == {}

    def test_passed_field_difference_detected(self):
        h = self._make_harness()
        ra = StepResult(step_id="s1", action="click", passed=True)
        rb = StepResult(step_id="s1", action="click", passed=False)
        result = h.diff_step_results(ra, rb)
        assert result["has_diff"] is True
        assert "passed" in result["diffs"]
        assert result["diffs"]["passed"] == {"before": True, "after": False}

    def test_error_field_difference_detected(self):
        h = self._make_harness()
        ra = StepResult(step_id="s1", action="fill", passed=False, error=None)
        rb = StepResult(step_id="s1", action="fill", passed=False, error="timeout")
        result = h.diff_step_results(ra, rb)
        assert result["has_diff"] is True
        assert "error" in result["diffs"]

    def test_output_dict_difference_detected(self):
        h = self._make_harness()
        ra = StepResult(step_id="s1", action="nav", passed=True, output={"status_code": 200})
        rb = StepResult(step_id="s1", action="nav", passed=True, output={"status_code": 404})
        result = h.diff_step_results(ra, rb)
        assert result["has_diff"] is True
        assert "details" in result["diffs"]
        assert result["diffs"]["details"]["status_code"] == {"before": 200, "after": 404}

    def test_non_dict_output_not_compared_as_details(self):
        h = self._make_harness()
        ra = StepResult(step_id="s1", action="click", passed=True, output="ok")
        rb = StepResult(step_id="s1", action="click", passed=True, output="ok")
        result = h.diff_step_results(ra, rb)
        assert result["has_diff"] is False


# ---------------------------------------------------------------------------
# ReplayHarness.replay_with_debug
# ---------------------------------------------------------------------------

class TestReplayWithDebug:
    def _make_harness(self) -> ReplayHarness:
        return ReplayHarness()

    def test_returns_required_keys_empty_session(self):
        h = self._make_harness()
        result = h.replay_with_debug("session-xyz")
        assert "session_id" in result
        assert "steps_executed" in result
        assert "breakpoints_hit" in result
        assert "bp_details" in result
        assert "results" in result

    def test_session_id_echoed(self):
        h = self._make_harness()
        result = h.replay_with_debug("my-session-42")
        assert result["session_id"] == "my-session-42"

    def test_zero_steps_when_no_loader(self):
        h = self._make_harness()
        result = h.replay_with_debug("s1")
        assert result["steps_executed"] == 0
        assert result["breakpoints_hit"] == 0

    def test_breakpoints_not_hit_on_empty_steps(self):
        h = self._make_harness()
        bp = ReplayBreakpoint(step_index=0)
        result = h.replay_with_debug("s1", breakpoints=[bp])
        assert result["breakpoints_hit"] == 0


# ---------------------------------------------------------------------------
# ReplayHarness.set_speed
# ---------------------------------------------------------------------------

class TestSetSpeed:
    def test_default_speed_is_one(self):
        h = ReplayHarness()
        assert h.speed_multiplier == 1.0

    def test_set_speed_updates_multiplier(self):
        h = ReplayHarness()
        h.set_speed(2.5)
        assert h.speed_multiplier == 2.5

    def test_set_speed_clamps_to_minimum(self):
        h = ReplayHarness()
        h.set_speed(0.0)
        assert h.speed_multiplier > 0.0

    def test_constructor_speed_multiplier(self):
        h = ReplayHarness(speed_multiplier=3.0)
        assert h.speed_multiplier == 3.0


# ---------------------------------------------------------------------------
# ReplayDiffer.diff_sessions
# ---------------------------------------------------------------------------

class TestReplayDifferSessions:
    def _make_differ(self) -> ReplayDiffer:
        return ReplayDiffer()

    def test_identical_sessions_no_diffs(self):
        d = self._make_differ()
        session = {"results": [{"step_index": 0, "result": {"passed": True}}]}
        result = d.diff_sessions(session, session)
        assert result["diff_count"] == 0
        assert result["diverged_at"] is None

    def test_diverged_at_correct_index(self):
        d = self._make_differ()
        sa = {"results": [
            {"step_index": 0, "result": {"passed": True}},
            {"step_index": 1, "result": {"passed": True}},
        ]}
        sb = {"results": [
            {"step_index": 0, "result": {"passed": True}},
            {"step_index": 1, "result": {"passed": False}},
        ]}
        result = d.diff_sessions(sa, sb)
        assert result["diverged_at"] == 1
        assert result["diff_count"] == 1

    def test_different_lengths_handled(self):
        d = self._make_differ()
        sa = {"results": [{"step_index": 0, "result": {}}]}
        sb = {"results": [
            {"step_index": 0, "result": {}},
            {"step_index": 1, "result": {}},
        ]}
        result = d.diff_sessions(sa, sb)
        assert result["total_steps_a"] == 1
        assert result["total_steps_b"] == 2
        assert result["diff_count"] == 1

    def test_empty_sessions_no_diffs(self):
        d = self._make_differ()
        result = d.diff_sessions({"results": []}, {"results": []})
        assert result["diff_count"] == 0
        assert result["diverged_at"] is None

    def test_diffs_list_contains_step_index(self):
        d = self._make_differ()
        sa = {"results": [{"step_index": 0, "result": {"passed": True}}]}
        sb = {"results": [{"step_index": 0, "result": {"passed": False}}]}
        result = d.diff_sessions(sa, sb)
        assert result["diffs"][0]["step_index"] == 0
        assert result["diffs"][0]["session_a"] == {"step_index": 0, "result": {"passed": True}}
        assert result["diffs"][0]["session_b"] == {"step_index": 0, "result": {"passed": False}}
