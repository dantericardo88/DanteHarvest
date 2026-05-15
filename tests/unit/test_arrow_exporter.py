"""Tests for harvest_distill.export.arrow_exporter."""
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock


def _make_chain_entry_dict(sequence=1, signal="screenshot"):
    return {
        "sequence": sequence,
        "run_id": "run-001",
        "signal": signal,
        "machine": "host-1",
        "timestamp": "2024-01-01T00:00:00Z",
        "content_hash": "abc123",
        "data": {"url": "https://example.com"},
    }


def _make_pack_entry(pack_id="wf-001"):
    e = MagicMock()
    e.pack_id = pack_id
    e.pack_type = "workflowPack"
    e.title = "Test Pack"
    e.promotion_status = "promoted"
    e.registered_at = "2024-01-01T00:00:00Z"
    e.receipt_id = "rcpt-1"
    e.confidence_score = 0.9
    return e


def test_available_format_ndjson_fallback():
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False
    assert exporter.available_format == "ndjson"


def test_available_format_arrow_when_pyarrow_present():
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter(prefer_format="arrow")
    exporter._has_pyarrow = True
    assert exporter.available_format == "arrow"


def test_export_chain_entries_ndjson(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    entries = [_make_chain_entry_dict(i) for i in range(3)]
    out = exporter.export_chain_entries(entries, output_path=tmp_path / "out.ndjson")
    assert out.exists()
    lines = out.read_text().splitlines()
    assert len(lines) == 3
    for line in lines:
        d = json.loads(line)
        assert "sequence" in d


def test_export_chain_entries_empty_ndjson(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    out = exporter.export_chain_entries([], output_path=tmp_path / "empty.ndjson")
    assert out.exists()
    assert out.read_bytes() == b""


def test_export_artifacts_ndjson(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    artifacts = [
        {"artifact_id": "a1", "source_type": "url", "sha256": "abc", "storage_uri": "s3://x",
         "ingested_at": "2024-01-01", "retention_class": "SHORT", "rights_status": "active"},
    ]
    out = exporter.export_artifacts(artifacts, output_path=tmp_path / "arts.ndjson")
    assert out.exists()
    d = json.loads(out.read_text().splitlines()[0])
    assert d["artifact_id"] == "a1"


def test_export_packs_ndjson(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    packs = [_make_pack_entry("p1"), _make_pack_entry("p2")]
    out = exporter.export_packs(packs, output_path=tmp_path / "packs.ndjson")
    assert out.exists()
    lines = out.read_text().splitlines()
    assert len(lines) == 2


def test_stream_chain_entries_ndjson():
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    entries = [_make_chain_entry_dict(i) for i in range(5)]
    chunks = list(exporter.stream_chain_entries(entries))
    assert len(chunks) >= 1
    all_data = b"".join(chunks)
    # Should have 5 newline-separated JSON records
    records = [json.loads(ln) for ln in all_data.splitlines() if ln.strip()]
    assert len(records) == 5


def test_export_chain_entries_with_dict_objects(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    entries = [_make_chain_entry_dict(1)]
    out = exporter.export_chain_entries(entries, output_path=tmp_path / "x.ndjson")
    data = json.loads(out.read_text().strip())
    assert data["run_id"] == "run-001"
    assert "data_json" in data


def test_export_chain_entries_creates_parent_dirs(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    deep = tmp_path / "a" / "b" / "c" / "out.ndjson"
    out = exporter.export_chain_entries([_make_chain_entry_dict()], output_path=deep)
    assert out.exists()


def test_export_packs_empty(tmp_path):
    from harvest_distill.export.arrow_exporter import ArrowExporter
    exporter = ArrowExporter()
    exporter._has_pyarrow = False

    out = exporter.export_packs([], output_path=tmp_path / "empty_packs.ndjson")
    assert out.exists()


def test_to_ndjson_helper():
    from harvest_distill.export.arrow_exporter import _to_ndjson
    records = [{"a": 1}, {"b": 2}]
    result = _to_ndjson(records)
    lines = result.split(b"\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1}
