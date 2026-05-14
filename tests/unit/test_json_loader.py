"""Unit tests for JSONLoader — CI-safe, all I/O via tmp_path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from harvest_acquire.loaders.json_loader import (
    JSONLoader,
    JSONDocument,
    _flatten,
    _detect_uniform_schema,
    _records_to_markdown_table,
    _kv_to_markdown,
)
from harvest_core.control.exceptions import NormalizationError


# ---------------------------------------------------------------------------
# _flatten
# ---------------------------------------------------------------------------

class TestFlatten:
    def test_flat_dict(self):
        result = _flatten({"a": 1, "b": "hello"})
        assert result == {"a": "1", "b": "hello"}

    def test_nested_dict(self):
        result = _flatten({"outer": {"inner": "value"}})
        assert "outer.inner" in result
        assert result["outer.inner"] == "value"

    def test_none_becomes_empty_string(self):
        result = _flatten({"key": None})
        assert result["key"] == ""

    def test_list_with_index_notation(self):
        result = _flatten({"items": [10, 20]})
        assert "items[0]" in result
        assert result["items[0]"] == "10"

    def test_deeply_nested(self):
        result = _flatten({"a": {"b": {"c": "deep"}}})
        assert result["a.b.c"] == "deep"

    def test_scalar_root(self):
        result = _flatten("just a string", prefix="root")
        assert result["root"] == "just a string"


# ---------------------------------------------------------------------------
# _detect_uniform_schema
# ---------------------------------------------------------------------------

class TestDetectUniformSchema:
    def test_uniform_records(self):
        records = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        keys = _detect_uniform_schema(records)
        assert keys == ["a", "b"]

    def test_non_uniform_returns_none(self):
        records = [{"a": 1}, {"b": 2}]
        assert _detect_uniform_schema(records) is None

    def test_empty_list_returns_none(self):
        assert _detect_uniform_schema([]) is None

    def test_non_dict_items_returns_none(self):
        assert _detect_uniform_schema([1, 2, 3]) is None

    def test_single_record_is_uniform(self):
        keys = _detect_uniform_schema([{"x": 1, "y": 2}])
        assert keys == ["x", "y"]


# ---------------------------------------------------------------------------
# _records_to_markdown_table
# ---------------------------------------------------------------------------

class TestRecordsToMarkdownTable:
    def test_basic_table(self):
        records = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        md = _records_to_markdown_table(["age", "name"], records)
        assert "| age | name |" in md
        assert "| --- | --- |" in md
        assert "| 30 | Alice |" in md

    def test_none_value_becomes_empty(self):
        records = [{"a": None, "b": "val"}]
        md = _records_to_markdown_table(["a", "b"], records)
        assert "|  | val |" in md

    def test_missing_key_becomes_empty(self):
        records = [{"a": "x"}]
        md = _records_to_markdown_table(["a", "b"], records)
        assert "|  |" in md


# ---------------------------------------------------------------------------
# _kv_to_markdown
# ---------------------------------------------------------------------------

class TestKvToMarkdown:
    def test_renders_key_value_list(self):
        md = _kv_to_markdown({"name": "Alice", "age": "30"})
        assert "- **name**: Alice" in md
        assert "- **age**: 30" in md

    def test_empty_dict_returns_empty(self):
        assert _kv_to_markdown({}) == ""


# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

class TestJSONLoaderJSON:
    def test_load_array_of_uniform_objects(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps([
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
        ]))
        docs = JSONLoader().load(f)
        assert len(docs) == 1
        doc = docs[0]
        assert doc.format == "json"
        assert doc.record_count == 2
        assert "| name | score |" in doc.markdown
        assert "| Alice | 95 |" in doc.markdown

    def test_load_object(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text(json.dumps({"host": "localhost", "port": 5432}))
        docs = JSONLoader().load(f)
        assert docs[0].record_count == 1
        assert "**host**: localhost" in docs[0].markdown
        assert "**port**: 5432" in docs[0].markdown

    def test_load_array_of_mixed_objects(self, tmp_path):
        f = tmp_path / "mixed.json"
        f.write_text(json.dumps([{"a": 1}, {"b": 2}]))
        docs = JSONLoader().load(f)
        assert "Item 1" in docs[0].markdown
        assert "Item 2" in docs[0].markdown

    def test_load_nested_object_flattened(self, tmp_path):
        f = tmp_path / "nested.json"
        f.write_text(json.dumps({"db": {"host": "localhost", "port": 5432}}))
        docs = JSONLoader().load(f)
        assert "db.host" in docs[0].markdown
        assert "db.port" in docs[0].markdown

    def test_load_scalar_json(self, tmp_path):
        f = tmp_path / "scalar.json"
        f.write_text(json.dumps(42))
        docs = JSONLoader().load(f)
        assert "42" in docs[0].markdown

    def test_load_empty_array(self, tmp_path):
        f = tmp_path / "empty.json"
        f.write_text("[]")
        docs = JSONLoader().load(f)
        assert docs[0].record_count == 0

    def test_section_heading_uses_stem(self, tmp_path):
        f = tmp_path / "my_data.json"
        f.write_text(json.dumps({"key": "val"}))
        docs = JSONLoader().load(f)
        assert "## my_data" in docs[0].markdown

    def test_schema_keys_populated_for_uniform_array(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text(json.dumps([{"x": 1, "y": 2}]))
        docs = JSONLoader().load(f)
        assert docs[0].schema_keys == ["x", "y"]

    def test_invalid_json_raises(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("{ not valid json }")
        with pytest.raises(NormalizationError, match="Invalid JSON"):
            JSONLoader().load(f)


# ---------------------------------------------------------------------------
# JSONL loading
# ---------------------------------------------------------------------------

class TestJSONLoaderJSONL:
    def _write_jsonl(self, path: Path, records: list) -> None:
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n")

    def test_load_uniform_jsonl_renders_table(self, tmp_path):
        f = tmp_path / "data.jsonl"
        self._write_jsonl(f, [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ])
        docs = JSONLoader().load(f)
        # schema keys are sorted alphabetically: age, name
        assert "| age | name |" in docs[0].markdown
        assert "| 30 | Alice |" in docs[0].markdown
        assert docs[0].record_count == 2

    def test_load_mixed_jsonl_renders_sections(self, tmp_path):
        f = tmp_path / "mixed.jsonl"
        self._write_jsonl(f, [{"a": 1}, {"b": 2}])
        docs = JSONLoader().load(f)
        assert "Record 1" in docs[0].markdown
        assert "Record 2" in docs[0].markdown

    def test_load_empty_jsonl(self, tmp_path):
        f = tmp_path / "empty.jsonl"
        f.write_text("")
        docs = JSONLoader().load(f)
        assert docs[0].record_count == 0
        assert docs[0].markdown == ""

    def test_load_jsonl_blank_lines_skipped(self, tmp_path):
        f = tmp_path / "blanks.jsonl"
        f.write_text('{"a":1}\n\n{"a":2}\n\n')
        docs = JSONLoader().load(f)
        assert docs[0].record_count == 2

    def test_load_jsonl_invalid_line_raises(self, tmp_path):
        f = tmp_path / "bad.jsonl"
        f.write_text('{"a":1}\nnot json\n{"a":2}\n')
        with pytest.raises(NormalizationError, match="Invalid JSON on line"):
            JSONLoader().load(f)

    def test_load_jsonl_schema_keys_for_uniform(self, tmp_path):
        f = tmp_path / "data.jsonl"
        self._write_jsonl(f, [{"x": 1, "y": 2}, {"x": 3, "y": 4}])
        docs = JSONLoader().load(f)
        assert sorted(docs[0].schema_keys) == ["x", "y"]

    def test_load_jsonl_section_heading_uses_stem(self, tmp_path):
        f = tmp_path / "events.jsonl"
        self._write_jsonl(f, [{"id": 1}])
        docs = JSONLoader().load(f)
        assert "## events" in docs[0].markdown


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestJSONLoaderErrors:
    def test_missing_file_raises(self):
        with pytest.raises(NormalizationError, match="not found"):
            JSONLoader().load(Path("/nonexistent/data.json"))

    def test_unsupported_suffix_raises(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a,b\n1,2\n")
        with pytest.raises(NormalizationError, match="Unsupported"):
            JSONLoader().load(f)

    def test_directory_path_raises(self, tmp_path):
        with pytest.raises(NormalizationError, match="not a file"):
            JSONLoader().load(tmp_path)


# ---------------------------------------------------------------------------
# JSONDocument dataclass
# ---------------------------------------------------------------------------

class TestJSONDocument:
    def test_document_fields(self):
        doc = JSONDocument(
            file_path="/tmp/test.json",
            format="json",
            record_count=5,
            markdown="## test\n\nsome content",
            schema_keys=["a", "b"],
        )
        assert doc.record_count == 5
        assert doc.format == "json"
        assert doc.schema_keys == ["a", "b"]
