"""Tests for harvest_distill.export.multi_format_exporter.MultiFormatExporter."""
import csv
import io
import json

import pytest

from harvest_distill.export.multi_format_exporter import MultiFormatExporter


SAMPLE = [
    {"id": "a1", "title": "First", "score": 0.9, "tags": ["x", "y"]},
    {"id": "a2", "title": "Second", "score": 0.5, "tags": []},
]


@pytest.fixture
def exporter():
    return MultiFormatExporter()


# ------------------------------------------------------------------
# JSONL
# ------------------------------------------------------------------

def test_to_jsonl_one_object_per_line(exporter):
    data = exporter.export(SAMPLE, "jsonl")
    lines = [l for l in data.decode("utf-8").splitlines() if l.strip()]
    assert len(lines) == len(SAMPLE)
    for line in lines:
        obj = json.loads(line)
        assert isinstance(obj, dict)


def test_to_jsonl_content_matches(exporter):
    data = exporter.export(SAMPLE, "jsonl")
    lines = data.decode("utf-8").splitlines()
    first = json.loads(lines[0])
    assert first["id"] == "a1"
    assert first["title"] == "First"


def test_to_jsonl_ends_with_newline(exporter):
    data = exporter.export(SAMPLE, "jsonl")
    assert data.endswith(b"\n")


# ------------------------------------------------------------------
# JSON
# ------------------------------------------------------------------

def test_to_json_valid_array(exporter):
    data = exporter.export(SAMPLE, "json")
    parsed = json.loads(data)
    assert isinstance(parsed, list)
    assert len(parsed) == len(SAMPLE)


# ------------------------------------------------------------------
# CSV
# ------------------------------------------------------------------

def test_to_csv_has_header(exporter):
    data = exporter.export(SAMPLE, "csv")
    text = data.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    assert len(rows) == len(SAMPLE)


def test_to_csv_header_contains_all_fields(exporter):
    data = exporter.export(SAMPLE, "csv")
    text = data.decode("utf-8")
    first_line = text.splitlines()[0]
    for field in ("id", "title", "score", "tags"):
        assert field in first_line


def test_to_csv_empty_returns_empty_bytes(exporter):
    data = exporter.export([], "csv")
    assert data == b""


# ------------------------------------------------------------------
# Markdown
# ------------------------------------------------------------------

def test_to_markdown_has_pipe_separators(exporter):
    data = exporter.export(SAMPLE, "markdown")
    text = data.decode("utf-8")
    assert "|" in text


def test_to_markdown_has_header_row(exporter):
    data = exporter.export(SAMPLE, "markdown")
    lines = data.decode("utf-8").splitlines()
    # First line is header, second is separator
    assert "---" in lines[1]


def test_to_markdown_empty(exporter):
    data = exporter.export([], "markdown")
    assert data == b"(no artifacts)\n"


# ------------------------------------------------------------------
# HTML
# ------------------------------------------------------------------

def test_to_html_starts_with_table(exporter):
    data = exporter.export(SAMPLE, "html")
    text = data.decode("utf-8")
    assert text.startswith("<table>")


def test_to_html_contains_thead_tbody(exporter):
    data = exporter.export(SAMPLE, "html")
    text = data.decode("utf-8")
    assert "<thead>" in text
    assert "<tbody>" in text


def test_to_html_empty(exporter):
    data = exporter.export([], "html")
    assert b"No artifacts" in data


# ------------------------------------------------------------------
# Arrow / Parquet (smoke — may fall back to JSONL if pyarrow absent)
# ------------------------------------------------------------------

def test_to_arrow_returns_bytes(exporter):
    data = exporter.export(SAMPLE, "arrow")
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_to_parquet_returns_bytes(exporter):
    data = exporter.export(SAMPLE, "parquet")
    assert isinstance(data, bytes)
    assert len(data) > 0


# ------------------------------------------------------------------
# Error handling
# ------------------------------------------------------------------

def test_export_raises_for_unknown_format(exporter):
    with pytest.raises(ValueError, match="Unsupported format"):
        exporter.export(SAMPLE, "xlsx")


def test_export_case_insensitive(exporter):
    data = exporter.export(SAMPLE, "JSONL")
    lines = [l for l in data.decode("utf-8").splitlines() if l.strip()]
    assert len(lines) == len(SAMPLE)


# ------------------------------------------------------------------
# get_schema
# ------------------------------------------------------------------

def test_get_schema_returns_fields(exporter):
    schema = exporter.get_schema(SAMPLE)
    assert "fields" in schema
    assert "count" in schema
    assert schema["count"] == len(SAMPLE)
    field_names = [f[0] for f in schema["fields"]]
    assert "id" in field_names
    assert "title" in field_names


def test_get_schema_empty(exporter):
    schema = exporter.get_schema([])
    assert schema == {"fields": [], "count": 0}


# ------------------------------------------------------------------
# export_to_file
# ------------------------------------------------------------------

def test_export_to_file_writes_bytes(exporter, tmp_path):
    out = tmp_path / "out.jsonl"
    n = exporter.export_to_file(SAMPLE, "jsonl", str(out))
    assert out.exists()
    assert n > 0
    assert n == out.stat().st_size
