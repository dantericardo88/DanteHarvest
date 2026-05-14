"""Tests for PackExporter — multi-format export (JSON/JSONL/SQLite/CSV/HuggingFace/Parquet)."""

import json
import pytest
import sqlite3
from pathlib import Path


from harvest_distill.packs.pack_exporter import (
    PackExporter,
    ExportFormat,
    ExportResult,
    _flatten_record,
)
from harvest_core.control.exceptions import PackagingError


# ---------------------------------------------------------------------------
# Sample records
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "pack_id": "wf-001",
        "pack_type": "workflowPack",
        "title": "Invoice Processing",
        "goal": "Process invoices end to end",
        "version": "1.0.0",
        "steps": [{"id": "s1", "action": "open invoice"}],
        "metadata": {"source": "internal"},
    },
    {
        "pack_id": "sk-001",
        "pack_type": "skillPack",
        "skill_name": "OCR Extraction",
        "version": "1.0.0",
        "input_schema": {"file": "str"},
        "steps": None,
    },
]


@pytest.fixture
def exporter(tmp_path):
    return PackExporter(output_dir=tmp_path, dataset_name="test_dataset")


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

class TestJSONExport:
    def test_json_creates_file(self, exporter, tmp_path):
        result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSON)
        assert (tmp_path / "test_dataset.json").exists()

    def test_json_record_count(self, exporter):
        result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSON)
        assert result.record_count == 2

    def test_json_valid_content(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSON)
        data = json.loads((tmp_path / "test_dataset.json").read_text())
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["pack_id"] == "wf-001"

    def test_json_returns_export_result(self, exporter):
        result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSON)
        assert isinstance(result, ExportResult)
        assert result.format == ExportFormat.JSON
        assert result.size_bytes > 0

    def test_json_empty_records_raises(self, exporter):
        with pytest.raises(PackagingError):
            exporter.export([], format=ExportFormat.JSON)

    def test_json_custom_filename(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSON, filename="custom_export")
        assert (tmp_path / "custom_export.json").exists()


# ---------------------------------------------------------------------------
# JSONL
# ---------------------------------------------------------------------------

class TestJSONLExport:
    def test_jsonl_creates_file(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSONL)
        assert (tmp_path / "test_dataset.jsonl").exists()

    def test_jsonl_line_count(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSONL)
        lines = (tmp_path / "test_dataset.jsonl").read_text().strip().splitlines()
        assert len(lines) == 2

    def test_jsonl_each_line_valid_json(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSONL)
        for line in (tmp_path / "test_dataset.jsonl").read_text().strip().splitlines():
            obj = json.loads(line)
            assert "pack_id" in obj

    def test_jsonl_append_mode(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSONL)
        # Append one more batch
        exporter.export([SAMPLE_RECORDS[0]], format=ExportFormat.JSONL, append=True)
        result = exporter.export([SAMPLE_RECORDS[1]], format=ExportFormat.JSONL, append=True)
        # Total should now be 4 lines
        assert result.record_count == 4

    def test_jsonl_overwrite_default(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.JSONL)
        result = exporter.export([SAMPLE_RECORDS[0]], format=ExportFormat.JSONL)
        # Overwrite — should have only 1 record
        assert result.record_count == 1


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

class TestSQLiteExport:
    def test_sqlite_creates_file(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.SQLITE)
        assert (tmp_path / "test_dataset.sqlite").exists()

    def test_sqlite_record_count(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.SQLITE)
        con = sqlite3.connect(str(tmp_path / "test_dataset.sqlite"))
        count = con.execute("SELECT COUNT(*) FROM packs").fetchone()[0]
        con.close()
        assert count == 2

    def test_sqlite_pack_id_stored(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.SQLITE)
        con = sqlite3.connect(str(tmp_path / "test_dataset.sqlite"))
        rows = con.execute("SELECT pack_id FROM packs").fetchall()
        con.close()
        pack_ids = [r[0] for r in rows]
        assert "wf-001" in pack_ids
        assert "sk-001" in pack_ids

    def test_sqlite_fts_table_created(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.SQLITE)
        con = sqlite3.connect(str(tmp_path / "test_dataset.sqlite"))
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        con.close()
        assert "packs" in tables
        # FTS table created when text fields exist
        assert any("fts" in t for t in tables)

    def test_sqlite_returns_result(self, exporter):
        result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.SQLITE)
        assert isinstance(result, ExportResult)
        assert result.format == ExportFormat.SQLITE
        assert result.size_bytes > 0


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

class TestCSVExport:
    def test_csv_creates_file(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.CSV)
        assert (tmp_path / "test_dataset.csv").exists()

    def test_csv_has_header(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.CSV)
        content = (tmp_path / "test_dataset.csv").read_text()
        lines = content.strip().splitlines()
        assert len(lines) >= 3  # header + 2 data rows

    def test_csv_record_count(self, exporter):
        result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.CSV)
        assert result.record_count == 2

    def test_csv_pack_id_in_content(self, exporter, tmp_path):
        exporter.export(SAMPLE_RECORDS, format=ExportFormat.CSV)
        content = (tmp_path / "test_dataset.csv").read_text()
        assert "wf-001" in content
        assert "sk-001" in content


# ---------------------------------------------------------------------------
# HuggingFace
# ---------------------------------------------------------------------------

class TestHuggingFaceExport:
    def test_hf_export_requires_pyarrow(self, exporter):
        """HF export falls through to PackagingError if pyarrow unavailable."""
        try:
            import pyarrow
            result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.HUGGINGFACE)
            assert isinstance(result, ExportResult)
        except PackagingError as e:
            assert "pyarrow" in str(e)

    def test_hf_dir_structure(self, exporter, tmp_path):
        try:
            import pyarrow
            exporter.export(SAMPLE_RECORDS, format=ExportFormat.HUGGINGFACE)
            hf_dir = tmp_path / "test_dataset"
            assert hf_dir.is_dir()
            assert (hf_dir / "dataset_info.json").exists()
            assert (hf_dir / "README.md").exists()
            assert (hf_dir / "data").is_dir()
        except PackagingError:
            pytest.skip("pyarrow not installed")

    def test_hf_dataset_info_valid(self, exporter, tmp_path):
        try:
            import pyarrow
            exporter.export(SAMPLE_RECORDS, format=ExportFormat.HUGGINGFACE)
            info = json.loads((tmp_path / "test_dataset" / "dataset_info.json").read_text())
            assert info["dataset_name"] == "test_dataset"
            assert info["splits"]["train"]["num_examples"] == 2
        except PackagingError:
            pytest.skip("pyarrow not installed")


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

class TestParquetExport:
    def test_parquet_requires_pyarrow(self, exporter):
        try:
            import pyarrow
            result = exporter.export(SAMPLE_RECORDS, format=ExportFormat.PARQUET)
            assert isinstance(result, ExportResult)
        except PackagingError as e:
            assert "pyarrow" in str(e)

    def test_parquet_creates_file(self, exporter, tmp_path):
        try:
            import pyarrow
            exporter.export(SAMPLE_RECORDS, format=ExportFormat.PARQUET)
            assert (tmp_path / "test_dataset.parquet").exists()
        except PackagingError:
            pytest.skip("pyarrow not installed")


# ---------------------------------------------------------------------------
# _flatten_record
# ---------------------------------------------------------------------------

class TestFlattenRecord:
    def test_flat_record_unchanged(self):
        rec = {"pack_id": "wf-001", "title": "Test", "version": "1.0.0"}
        flat = _flatten_record(rec)
        assert flat["pack_id"] == "wf-001"
        assert flat["title"] == "Test"

    def test_nested_dict_serialized(self):
        rec = {"metadata": {"source": "internal", "tags": ["a", "b"]}}
        flat = _flatten_record(rec)
        parsed = json.loads(flat["metadata"])
        assert parsed["source"] == "internal"

    def test_list_serialized(self):
        rec = {"steps": [{"id": "s1"}, {"id": "s2"}]}
        flat = _flatten_record(rec)
        parsed = json.loads(flat["steps"])
        assert len(parsed) == 2

    def test_none_becomes_empty_string(self):
        rec = {"steps": None}
        flat = _flatten_record(rec)
        assert flat["steps"] == ""

    def test_scalar_values_preserved(self):
        rec = {"score": 0.95, "count": 42}
        flat = _flatten_record(rec)
        assert flat["score"] == 0.95
        assert flat["count"] == 42
