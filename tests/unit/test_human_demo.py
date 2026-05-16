"""Tests for human_demo_to_automation dimension (7→9).

Covers:
- Scroll events in same direction are merged into one step with summed delta
- _to_cdp_key_event() returns correct CDP Input.dispatchKeyEvent format
- validate_interaction_sequence() catches orphaned keydowns and missing selectors
- get_automation_script() generates valid Playwright Python code
"""
import pytest
import time


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_recorder():
    from harvest_observe.capture.demo_recorder import DemoRecorder
    return DemoRecorder(goal="test")


def _make_event(event_type, *, selector="", y=None, x=None,
                key=None, value=None, url=None, text="",
                ts=None, session_id="test"):
    from harvest_observe.capture.demo_recorder import InteractionEvent
    return InteractionEvent(
        event_type=event_type,
        selector=selector,
        text=text,
        x=x,
        y=y,
        key=key,
        value=value,
        url=url,
        ts=ts if ts is not None else time.time(),
        session_id=session_id,
    )


# ---------------------------------------------------------------------------
# 1. Scroll merging
# ---------------------------------------------------------------------------

class TestScrollMerging:
    def test_two_same_direction_scrolls_merged(self):
        """Two down-scrolls close in time should be merged into one step."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("scroll", y=30.0, ts=now),
            _make_event("scroll", y=20.0, ts=now + 0.1),
        ]
        steps = recorder.interaction_events_to_steps(events)
        assert len(steps) == 1
        assert steps[0]["type"] == "scroll"

    def test_scroll_delta_summed(self):
        """Merged scrolls should have summed delta_y."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("scroll", y=30.0, ts=now),
            _make_event("scroll", y=20.0, ts=now + 0.1),
        ]
        steps = recorder.interaction_events_to_steps(events)
        assert steps[0]["delta_y"] == pytest.approx(50.0)

    def test_opposite_direction_scrolls_not_merged(self):
        """Down then up scroll should NOT be merged."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("scroll", y=100.0, ts=now),
            _make_event("scroll", y=-100.0, ts=now + 0.1),
        ]
        steps = recorder.interaction_events_to_steps(events)
        assert len(steps) == 2

    def test_scrolls_outside_time_window_not_merged(self):
        """Scrolls more than 0.5s apart should not be merged."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("scroll", y=30.0, ts=now),
            _make_event("scroll", y=20.0, ts=now + 1.0),  # 1s apart — outside window
        ]
        steps = recorder.interaction_events_to_steps(events)
        assert len(steps) == 2

    def test_three_same_direction_scrolls_merged_to_one(self):
        """Three consecutive down-scrolls within window merge to one."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("scroll", y=20.0, ts=now),
            _make_event("scroll", y=15.0, ts=now + 0.1),
            _make_event("scroll", y=10.0, ts=now + 0.2),
        ]
        steps = recorder.interaction_events_to_steps(events)
        assert len(steps) == 1
        assert steps[0]["delta_y"] == pytest.approx(45.0)

    def test_scroll_between_clicks_preserved(self):
        """Scroll step between two clicks should be preserved as its own step."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("click", selector="#btn1", ts=now),
            _make_event("scroll", y=100.0, ts=now + 0.1),
            _make_event("click", selector="#btn2", ts=now + 0.2),
        ]
        steps = recorder.interaction_events_to_steps(events)
        types = [s["type"] for s in steps]
        assert types == ["click", "scroll", "click"]

    def test_empty_events_returns_empty(self):
        recorder = _make_recorder()
        steps = recorder.interaction_events_to_steps([])
        assert steps == []

    def test_large_delta_not_merged_within_window(self):
        """If delta exceeds threshold (50px), even within time window, no merge."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("scroll", y=30.0, ts=now),
            _make_event("scroll", y=60.0, ts=now + 0.1),  # 60 > threshold 50
        ]
        steps = recorder.interaction_events_to_steps(events)
        # The second scroll's delta > 50px threshold → not merged
        assert len(steps) == 2


# ---------------------------------------------------------------------------
# 2. _to_cdp_key_event()
# ---------------------------------------------------------------------------

class TestCdpKeyEvent:
    def test_single_char_key(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("a")
        assert result["type"] == "keyDown"
        assert result["key"] == "a"
        assert result["code"] == "KeyA"
        assert result["windowsVirtualKeyCode"] == ord("a")
        assert result["nativeVirtualKeyCode"] == ord("a")
        assert result["autoRepeat"] is False
        assert result["isKeypad"] is False
        assert result["isSystemKey"] is False

    def test_multi_char_key_enter(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("Enter")
        assert result["key"] == "Enter"
        assert result["code"] == "Enter"
        assert result["windowsVirtualKeyCode"] == 0
        assert result["nativeVirtualKeyCode"] == 0

    def test_keyup_event_type(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("a", event_type="keyUp")
        assert result["type"] == "keyUp"

    def test_char_event_type(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("x", event_type="char")
        assert result["type"] == "char"

    def test_returns_dict(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("z")
        assert isinstance(result, dict)

    def test_uppercase_single_char(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("Z")
        assert result["code"] == "KeyZ"
        assert result["windowsVirtualKeyCode"] == ord("Z")

    def test_special_key_arrow_down(self):
        recorder = _make_recorder()
        result = recorder._to_cdp_key_event("ArrowDown")
        assert result["key"] == "ArrowDown"
        assert result["code"] == "ArrowDown"
        assert result["windowsVirtualKeyCode"] == 0


# ---------------------------------------------------------------------------
# 3. validate_interaction_sequence()
# ---------------------------------------------------------------------------

class TestValidateInteractionSequence:
    def test_valid_sequence(self):
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("navigate", url="https://example.com", ts=now),
            _make_event("click", selector="#btn", ts=now + 0.1),
            _make_event("keydown", selector="#input", key="a", ts=now + 0.2),
            _make_event("keyup", selector="#input", key="a", ts=now + 0.3),
        ]
        result = recorder.validate_interaction_sequence(events)
        assert result["valid"] is True
        assert result["issues"] == []
        assert result["event_count"] == 4

    def test_orphaned_keydown_detected(self):
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("keydown", selector="#input", key="Enter", ts=now),
            # No keyup for Enter
        ]
        result = recorder.validate_interaction_sequence(events)
        assert result["valid"] is False
        assert any("Orphaned" in issue for issue in result["issues"])

    def test_click_without_selector_detected(self):
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("click", selector="", ts=now),  # no selector
        ]
        result = recorder.validate_interaction_sequence(events)
        assert result["valid"] is False
        assert any("selector" in issue for issue in result["issues"])

    def test_multiple_issues_reported(self):
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("keydown", key="Tab", ts=now),       # orphaned
            _make_event("click", selector="", ts=now + 0.1), # no selector
        ]
        result = recorder.validate_interaction_sequence(events)
        assert result["valid"] is False
        assert len(result["issues"]) == 2

    def test_event_count_returned(self):
        recorder = _make_recorder()
        events = [_make_event("navigate", url="https://x.com")]
        result = recorder.validate_interaction_sequence(events)
        assert result["event_count"] == 1

    def test_empty_sequence_is_valid(self):
        recorder = _make_recorder()
        result = recorder.validate_interaction_sequence([])
        assert result["valid"] is True
        assert result["event_count"] == 0

    def test_matched_keydown_keyup_valid(self):
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("keydown", key="Shift", ts=now),
            _make_event("keydown", key="a", ts=now + 0.05),
            _make_event("keyup", key="a", ts=now + 0.1),
            _make_event("keyup", key="Shift", ts=now + 0.15),
        ]
        result = recorder.validate_interaction_sequence(events)
        assert result["valid"] is True


# ---------------------------------------------------------------------------
# 4. get_automation_script()
# ---------------------------------------------------------------------------

class TestGetAutomationScript:
    def test_contains_playwright_import(self):
        recorder = _make_recorder()
        events = [_make_event("navigate", url="https://example.com")]
        script = recorder.get_automation_script(events)
        assert "from playwright.sync_api import sync_playwright" in script

    def test_navigate_step_in_script(self):
        recorder = _make_recorder()
        events = [_make_event("navigate", url="https://example.com")]
        script = recorder.get_automation_script(events)
        assert "page.goto('https://example.com')" in script

    def test_click_step_in_script(self):
        recorder = _make_recorder()
        events = [_make_event("click", selector="#submit-btn")]
        script = recorder.get_automation_script(events)
        assert "page.click('#submit-btn')" in script

    def test_type_step_in_script(self):
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("input", selector="#name", value="Alice", ts=now),
        ]
        script = recorder.get_automation_script(events)
        assert "page.fill(" in script
        assert "Alice" in script

    def test_scroll_step_in_script(self):
        recorder = _make_recorder()
        events = [_make_event("scroll", y=300.0)]
        script = recorder.get_automation_script(events)
        assert "window.scrollBy" in script

    def test_browser_close_present(self):
        recorder = _make_recorder()
        events = [_make_event("navigate", url="https://x.com")]
        script = recorder.get_automation_script(events)
        assert "browser.close()" in script

    def test_full_workflow_script(self):
        """End-to-end: navigate → click → type → scroll."""
        recorder = _make_recorder()
        now = time.time()
        events = [
            _make_event("navigate", url="https://example.com", ts=now),
            _make_event("click", selector="#search", ts=now + 0.1),
            _make_event("input", selector="#search", value="hello", ts=now + 0.2),
            _make_event("scroll", y=200.0, ts=now + 0.3),
        ]
        script = recorder.get_automation_script(events)
        assert "page.goto" in script
        assert "page.click" in script
        assert "page.fill" in script
        assert "window.scrollBy" in script

    def test_returns_string(self):
        recorder = _make_recorder()
        events = [_make_event("navigate", url="https://x.com")]
        result = recorder.get_automation_script(events)
        assert isinstance(result, str)

    def test_default_framework_is_playwright(self):
        """Calling without framework arg should default to playwright."""
        recorder = _make_recorder()
        events = [_make_event("navigate", url="https://example.com")]
        script = recorder.get_automation_script(events)
        assert "playwright" in script.lower()
