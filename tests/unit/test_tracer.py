"""Tests for span-based Tracer with correlation IDs."""
import time
import pytest

from harvest_core.audit.tracer import Tracer, Span
from harvest_core.audit import Tracer as TracerFromInit, Span as SpanFromInit


@pytest.fixture(autouse=True)
def clear_tracer():
    """Clear all traces before each test to avoid cross-test contamination."""
    Tracer.clear_all()
    # Also clear thread-local state
    Tracer._local.__dict__.clear()
    yield
    Tracer.clear_all()


class TestStartTrace:
    def test_returns_string(self):
        trace_id = Tracer.start_trace()
        assert isinstance(trace_id, str)
        assert len(trace_id) > 0

    def test_returns_unique_ids(self):
        ids = {Tracer.start_trace() for _ in range(10)}
        assert len(ids) == 10

    def test_with_name_creates_root_span(self):
        trace_id = Tracer.start_trace("root-op")
        spans = Tracer.get_trace(trace_id)
        assert len(spans) == 1
        assert spans[0].name == "root-op"

    def test_without_name_creates_empty_trace(self):
        trace_id = Tracer.start_trace()
        spans = Tracer.get_trace(trace_id)
        assert len(spans) == 0

    def test_sets_current_trace_id(self):
        trace_id = Tracer.start_trace()
        assert Tracer.get_current_trace_id() == trace_id

    def test_trace_id_is_uuid_format(self):
        import uuid
        trace_id = Tracer.start_trace()
        # Should not raise
        uuid.UUID(trace_id)


class TestSpanContextManager:
    def test_records_span_in_trace(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("my-op"):
            pass
        spans = Tracer.get_trace(trace_id)
        assert any(s.name == "my-op" for s in spans)

    def test_duration_ms_is_set_after_exit(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("timed-op") as s:
            time.sleep(0.01)
        assert s.duration_ms is not None
        assert s.duration_ms >= 5  # at least 5ms for a 10ms sleep

    def test_duration_ms_is_none_while_running(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("live-op") as s:
            assert s.duration_ms is None

    def test_status_ok_on_success(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("good-op") as s:
            pass
        assert s.status == "ok"

    def test_status_error_on_exception(self):
        trace_id = Tracer.start_trace()
        with pytest.raises(ValueError):
            with Tracer.span("bad-op") as s:
                raise ValueError("oops")
        assert s.status == "error"
        assert s.error_message == "oops"

    def test_error_message_set_on_exception(self):
        trace_id = Tracer.start_trace()
        with pytest.raises(RuntimeError):
            with Tracer.span("failing") as s:
                raise RuntimeError("something broke")
        assert s.error_message == "something broke"

    def test_exception_is_reraised(self):
        trace_id = Tracer.start_trace()
        with pytest.raises(ZeroDivisionError):
            with Tracer.span("divide"):
                _ = 1 / 0

    def test_tags_are_stored(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("tagged", tags={"env": "test", "version": 2}) as s:
            pass
        assert s.tags["env"] == "test"
        assert s.tags["version"] == 2

    def test_creates_trace_if_none_active(self):
        # No start_trace called — span() should auto-create one
        Tracer._local.__dict__.clear()
        with Tracer.span("auto-trace") as s:
            pass
        assert s.trace_id is not None
        assert Tracer.get_trace(s.trace_id)


class TestNestedSpans:
    def test_nested_span_has_correct_parent_id(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("parent") as parent:
            with Tracer.span("child") as child:
                pass
        assert child.parent_id == parent.span_id

    def test_double_nested_parent_ids(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("grandparent") as gp:
            with Tracer.span("parent") as p:
                with Tracer.span("child") as c:
                    pass
        assert p.parent_id == gp.span_id
        assert c.parent_id == p.span_id

    def test_sequential_spans_have_no_parent(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("first") as s1:
            pass
        with Tracer.span("second") as s2:
            pass
        assert s1.parent_id is None
        assert s2.parent_id is None

    def test_all_spans_share_trace_id(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("a") as a:
            with Tracer.span("b") as b:
                pass
        assert a.trace_id == trace_id
        assert b.trace_id == trace_id

    def test_nested_span_count(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("p"):
            with Tracer.span("c1"):
                pass
            with Tracer.span("c2"):
                pass
        spans = Tracer.get_trace(trace_id)
        names = [s.name for s in spans]
        assert "p" in names
        assert "c1" in names
        assert "c2" in names


class TestFormatTrace:
    def test_returns_string(self):
        trace_id = Tracer.start_trace("fmt-test")
        result = Tracer.format_trace(trace_id)
        assert isinstance(result, str)

    def test_contains_trace_id(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("some-op"):
            pass
        result = Tracer.format_trace(trace_id)
        assert trace_id in result

    def test_contains_span_names(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("fetch-data"):
            with Tracer.span("parse-json"):
                pass
        result = Tracer.format_trace(trace_id)
        assert "fetch-data" in result
        assert "parse-json" in result

    def test_empty_trace_returns_message(self):
        trace_id = Tracer.start_trace()
        result = Tracer.format_trace(trace_id)
        assert "empty" in result.lower() or trace_id in result

    def test_unknown_trace_id_returns_message(self):
        result = Tracer.format_trace("nonexistent-id")
        assert "nonexistent-id" in result or "empty" in result.lower()

    def test_error_span_marked_in_output(self):
        trace_id = Tracer.start_trace()
        with pytest.raises(RuntimeError):
            with Tracer.span("broken-op"):
                raise RuntimeError("fail")
        result = Tracer.format_trace(trace_id)
        assert "broken-op" in result
        assert "error" in result.lower()


class TestSpanToDict:
    def test_returns_dict(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("op") as s:
            pass
        d = s.to_dict()
        assert isinstance(d, dict)

    def test_required_keys_present(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("op") as s:
            pass
        d = s.to_dict()
        required = {"trace_id", "span_id", "parent_id", "name", "start_time",
                    "end_time", "duration_ms", "tags", "status", "error_message"}
        assert required.issubset(d.keys())

    def test_duration_ms_computed_correctly(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("timed") as s:
            time.sleep(0.02)
        d = s.to_dict()
        assert d["duration_ms"] is not None
        assert d["duration_ms"] >= 10

    def test_error_message_in_dict(self):
        trace_id = Tracer.start_trace()
        with pytest.raises(ValueError):
            with Tracer.span("err-op") as s:
                raise ValueError("dict-error")
        d = s.to_dict()
        assert d["error_message"] == "dict-error"
        assert d["status"] == "error"

    def test_tags_in_dict(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("tagged", tags={"k": "v"}) as s:
            pass
        d = s.to_dict()
        assert d["tags"] == {"k": "v"}


class TestGetTrace:
    def test_returns_list(self):
        trace_id = Tracer.start_trace()
        result = Tracer.get_trace(trace_id)
        assert isinstance(result, list)

    def test_unknown_trace_returns_empty_list(self):
        result = Tracer.get_trace("does-not-exist")
        assert result == []

    def test_spans_accumulate(self):
        trace_id = Tracer.start_trace()
        for i in range(5):
            with Tracer.span(f"op-{i}"):
                pass
        spans = Tracer.get_trace(trace_id)
        assert len(spans) == 5


class TestClearTrace:
    def test_clear_specific_trace(self):
        trace_id = Tracer.start_trace()
        with Tracer.span("op"):
            pass
        Tracer.clear_trace(trace_id)
        assert Tracer.get_trace(trace_id) == []

    def test_clear_nonexistent_trace_no_error(self):
        # Should not raise
        Tracer.clear_trace("nonexistent")

    def test_clear_all(self):
        ids = [Tracer.start_trace() for _ in range(3)]
        for tid in ids:
            with Tracer.span("x"):
                pass
        Tracer.clear_all()
        for tid in ids:
            assert Tracer.get_trace(tid) == []


class TestImportFromInit:
    def test_tracer_importable_from_audit_init(self):
        assert TracerFromInit is Tracer

    def test_span_importable_from_audit_init(self):
        assert SpanFromInit is Span
