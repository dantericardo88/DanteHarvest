"""Lightweight span-based tracing with correlation IDs."""
import time
import uuid
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_id: Optional[str]
    name: str
    start_time: float
    end_time: Optional[float] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    status: str = "ok"  # "ok", "error"
    error_message: Optional[str] = None

    @property
    def duration_ms(self) -> Optional[float]:
        if self.end_time is not None:
            return (self.end_time - self.start_time) * 1000
        return None

    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "name": self.name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "tags": self.tags,
            "status": self.status,
            "error_message": self.error_message,
        }


class Tracer:
    """Thread-local span-based tracer with correlation IDs."""

    _local = threading.local()
    _traces: Dict[str, List[Span]] = {}
    _lock = threading.Lock()

    @classmethod
    def start_trace(cls, name: str = None) -> str:
        """Start a new trace. Returns trace_id."""
        trace_id = str(uuid.uuid4())
        cls._local.trace_id = trace_id
        cls._local.span_stack = []
        with cls._lock:
            cls._traces[trace_id] = []
        if name:
            cls._start_span_internal(trace_id, name, parent_id=None)
        return trace_id

    @classmethod
    def get_current_trace_id(cls) -> Optional[str]:
        return getattr(cls._local, 'trace_id', None)

    @classmethod
    def _start_span_internal(cls, trace_id: str, name: str, parent_id: Optional[str]) -> Span:
        span = Span(
            trace_id=trace_id,
            span_id=str(uuid.uuid4()),
            parent_id=parent_id,
            name=name,
            start_time=time.time(),
        )
        with cls._lock:
            cls._traces.setdefault(trace_id, []).append(span)
        stack = getattr(cls._local, 'span_stack', [])
        stack.append(span)
        cls._local.span_stack = stack
        return span

    @classmethod
    @contextmanager
    def span(cls, name: str, tags: dict = None):
        """Context manager for a span. Automatically nests under current span."""
        trace_id = cls.get_current_trace_id() or cls.start_trace()
        stack = getattr(cls._local, 'span_stack', [])
        parent_id = stack[-1].span_id if stack else None
        s = cls._start_span_internal(trace_id, name, parent_id)
        if tags:
            s.tags.update(tags)
        try:
            yield s
            s.status = "ok"
        except Exception as e:
            s.status = "error"
            s.error_message = str(e)
            raise
        finally:
            s.end_time = time.time()
            if stack and stack[-1].span_id == s.span_id:
                stack.pop()

    @classmethod
    def get_trace(cls, trace_id: str) -> List[Span]:
        with cls._lock:
            return list(cls._traces.get(trace_id, []))

    @classmethod
    def format_trace(cls, trace_id: str) -> str:
        """Format a trace as a human-readable tree."""
        spans = cls.get_trace(trace_id)
        if not spans:
            return f"Trace {trace_id}: (empty)"
        lines = [f"Trace: {trace_id}"]
        root_spans = [s for s in spans if s.parent_id is None]

        def render(span, indent=0):
            dur = f"{span.duration_ms:.1f}ms" if span.duration_ms is not None else "running"
            status = " [ERROR]" if span.status == "error" else ""
            lines.append(f"{'  ' * indent}+- {span.name} ({dur}){status}")
            children = [s for s in spans if s.parent_id == span.span_id]
            for child in children:
                render(child, indent + 1)

        for root in root_spans:
            render(root)
        return "\n".join(lines)

    @classmethod
    def clear_trace(cls, trace_id: str) -> None:
        with cls._lock:
            cls._traces.pop(trace_id, None)

    @classmethod
    def clear_all(cls) -> None:
        with cls._lock:
            cls._traces.clear()
