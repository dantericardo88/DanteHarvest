"""
TaskSegmenter — identify discrete task boundaries in action event streams.

Segments a flat list of action events into TaskSpans using heuristics:
- Navigation events (navigate, page_load) open a new span
- Idle gaps > threshold_seconds open a new span
- Explicit markers (task_start, task_end) from the recorder open/close spans

Each TaskSpan has a title (inferred from navigate URL or first action),
a list of actions, and provenance back to the source session.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TaskSpan:
    span_id: str
    session_id: str
    title: str
    actions: List[Dict[str, Any]]
    start_time: float
    end_time: float
    span_index: int
    confidence: float = 1.0

    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time

    @property
    def action_count(self) -> int:
        return len(self.actions)

    def to_dict(self) -> dict:
        return {
            "span_id": self.span_id,
            "session_id": self.session_id,
            "title": self.title,
            "span_index": self.span_index,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "action_count": self.action_count,
            "confidence": self.confidence,
            "actions": self.actions,
        }


@dataclass
class SegmentationResult:
    spans: List[TaskSpan]
    total_actions: int
    session_id: str

    @property
    def span_count(self) -> int:
        return len(self.spans)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "span_count": self.span_count,
            "total_actions": self.total_actions,
            "spans": [s.to_dict() for s in self.spans],
        }


_NAV_ACTION_TYPES = {"navigate", "page_load", "url_change", "tab_open"}


class TaskSegmenter:
    """
    Segment a browser session's action list into discrete TaskSpans.

    Usage:
        segmenter = TaskSegmenter(idle_gap_seconds=30.0)
        result = segmenter.segment(actions=session.actions, session_id=session.session_id)
    """

    def __init__(
        self,
        idle_gap_seconds: float = 30.0,
        min_actions_per_span: int = 1,
    ):
        self.idle_gap_seconds = idle_gap_seconds
        self.min_actions_per_span = min_actions_per_span

    def segment(
        self,
        actions: List[Any],
        session_id: str,
    ) -> SegmentationResult:
        """
        Segment action list into TaskSpans.

        actions: list of ActionEvent objects or dicts with action_type and timestamp.
        """
        if not actions:
            return SegmentationResult(spans=[], total_actions=0, session_id=session_id)

        raw = [self._normalize_action(a) for a in actions]
        spans: List[TaskSpan] = []
        current_actions: List[Dict] = []
        current_start: float = raw[0]["timestamp"]
        span_idx = 0

        for i, action in enumerate(raw):
            ts = action["timestamp"]
            action_type = action["action_type"]

            # Flush current span on navigation or idle gap
            should_flush = (
                action_type in _NAV_ACTION_TYPES
                or (current_actions and ts - raw[i - 1]["timestamp"] > self.idle_gap_seconds)
            )

            if should_flush and current_actions:
                span = self._make_span(
                    current_actions, session_id, current_start, span_idx
                )
                if span.action_count >= self.min_actions_per_span:
                    spans.append(span)
                    span_idx += 1
                current_actions = []
                current_start = ts

            current_actions.append(action)

        # Flush final span
        if current_actions:
            span = self._make_span(current_actions, session_id, current_start, span_idx)
            if span.action_count >= self.min_actions_per_span:
                spans.append(span)

        return SegmentationResult(
            spans=spans,
            total_actions=len(raw),
            session_id=session_id,
        )

    def _make_span(
        self,
        actions: List[Dict],
        session_id: str,
        start_time: float,
        span_idx: int,
    ) -> TaskSpan:
        from uuid import uuid4
        end_time = actions[-1]["timestamp"]
        title = self._infer_title(actions)
        return TaskSpan(
            span_id=str(uuid4()),
            session_id=session_id,
            title=title,
            actions=actions,
            start_time=start_time,
            end_time=end_time,
            span_index=span_idx,
        )

    def _infer_title(self, actions: List[Dict]) -> str:
        # Use URL from first navigate, or action type of first action
        for action in actions:
            if action["action_type"] in _NAV_ACTION_TYPES:
                return action.get("value") or action.get("url") or "navigation"
        if actions:
            target = actions[0].get("target_selector", "")
            return f"{actions[0]['action_type']} {target}".strip()
        return "untitled"

    def _normalize_action(self, action: Any) -> Dict:
        if isinstance(action, dict):
            return action
        # ActionEvent dataclass → dict
        if hasattr(action, "to_dict"):
            return action.to_dict()
        return {
            "action_type": getattr(action, "action_type", "unknown"),
            "timestamp": getattr(action, "timestamp", 0.0),
            "target_selector": getattr(action, "target_selector", None),
            "value": getattr(action, "value", None),
        }
