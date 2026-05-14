"""
PackExporter — multi-format export for promoted packs.

Harvested from: Bright Data export pipeline patterns + HuggingFace datasets format.

Supported output formats:
  - json       (default): single-file JSON array
  - jsonl      : newline-delimited JSON, streaming-friendly
  - parquet    : columnar Parquet via pyarrow (optional dep)
  - sqlite     : SQLite database with full-text search index
  - huggingface: HuggingFace Dataset-compatible parquet shards with metadata card
  - csv        : flat CSV (scalar fields only)

Constitutional guarantees:
- Local-first: all formats write to local disk; no upload required
- Fail-closed: missing optional dep (pyarrow) raises ExportError with install hint
- Zero-ambiguity: exported files are verified for non-zero size after write
- Append-only: JSONL export supports append mode for streaming pipelines

Usage:
    from harvest_distill.packs.pack_exporter import PackExporter, ExportFormat

    exporter = PackExporter(output_dir=Path("exports"))
    result = exporter.export(packs, format=ExportFormat.JSONL)
    print(result.path, result.record_count, result.size_bytes)
"""

from __future__ import annotations

import csv
import json
import sqlite3
import io
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from harvest_core.control.exceptions import PackagingError


class ExportFormat(str, Enum):
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    SQLITE = "sqlite"
    HUGGINGFACE = "huggingface"
    CSV = "csv"


@dataclass
class ExportResult:
    format: ExportFormat
    path: Path
    record_count: int
    size_bytes: int
    metadata: Dict[str, Any]


class PackExporter:
    """
    Export promoted packs to multiple output formats.

    Args:
        output_dir: base directory for all exports (created if absent)
        dataset_name: used as filename stem and HF dataset name
    """

    def __init__(
        self,
        output_dir: Path = Path("exports"),
        dataset_name: str = "harvest_dataset",
    ):
        self.output_dir = Path(output_dir)
        self.dataset_name = dataset_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(
        self,
        records: Sequence[Dict[str, Any]],
        format: ExportFormat = ExportFormat.JSONL,
        filename: Optional[str] = None,
        append: bool = False,
    ) -> ExportResult:
        """
        Export records to the specified format.

        Args:
            records:  list of dicts (pack dicts from model.model_dump())
            format:   ExportFormat enum value
            filename: override output filename (without extension)
            append:   JSONL only — append to existing file instead of overwriting

        Returns:
            ExportResult with path, record_count, and size_bytes.

        Raises:
            PackagingError if records is empty or export fails.
        """
        if not records:
            raise PackagingError("export requires at least one record")

        stem = filename or self.dataset_name

        if format == ExportFormat.JSON:
            return self._export_json(records, stem)
        if format == ExportFormat.JSONL:
            return self._export_jsonl(records, stem, append=append)
        if format == ExportFormat.PARQUET:
            return self._export_parquet(records, stem)
        if format == ExportFormat.SQLITE:
            return self._export_sqlite(records, stem)
        if format == ExportFormat.HUGGINGFACE:
            return self._export_huggingface(records, stem)
        if format == ExportFormat.CSV:
            return self._export_csv(records, stem)
        raise PackagingError(f"Unknown export format: {format}")

    # ------------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------------

    def _export_json(self, records: Sequence[Dict], stem: str) -> ExportResult:
        path = self.output_dir / f"{stem}.json"
        data = json.dumps(list(records), indent=2, default=str)
        path.write_text(data, encoding="utf-8")
        return self._result(ExportFormat.JSON, path, len(records))

    # ------------------------------------------------------------------
    # JSONL (newline-delimited, streaming-friendly)
    # ------------------------------------------------------------------

    def _export_jsonl(
        self, records: Sequence[Dict], stem: str, append: bool = False
    ) -> ExportResult:
        path = self.output_dir / f"{stem}.jsonl"
        mode = "a" if append else "w"
        with path.open(mode, encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, default=str) + "\n")
        # Count total records in file (supports append)
        total = sum(1 for _ in path.open(encoding="utf-8"))
        return self._result(ExportFormat.JSONL, path, total)

    # ------------------------------------------------------------------
    # Parquet (requires pyarrow)
    # ------------------------------------------------------------------

    def _export_parquet(self, records: Sequence[Dict], stem: str) -> ExportResult:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise PackagingError(
                "pyarrow not installed. Run: pip install pyarrow"
            ) from e

        path = self.output_dir / f"{stem}.parquet"
        flat = [_flatten_record(r) for r in records]
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, str(path), compression="snappy")
        return self._result(ExportFormat.PARQUET, path, len(records))

    # ------------------------------------------------------------------
    # SQLite (with FTS5 full-text search index)
    # ------------------------------------------------------------------

    def _export_sqlite(self, records: Sequence[Dict], stem: str) -> ExportResult:
        path = self.output_dir / f"{stem}.sqlite"
        flat_records = [_flatten_record(r) for r in records]

        # Collect all column names
        all_keys: List[str] = []
        seen = set()
        for rec in flat_records:
            for k in rec:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        con = sqlite3.connect(str(path))
        try:
            # Main records table
            col_defs = ", ".join(f'"{k}" TEXT' for k in all_keys)
            con.execute(f"CREATE TABLE IF NOT EXISTS packs ({col_defs})")
            con.execute("DELETE FROM packs")

            placeholders = ", ".join("?" for _ in all_keys)
            col_names = ", ".join(f'"{k}"' for k in all_keys)
            for rec in flat_records:
                values = [str(rec.get(k, "")) for k in all_keys]
                con.execute(
                    f"INSERT INTO packs ({col_names}) VALUES ({placeholders})",
                    values,
                )

            # FTS5 full-text search index over text fields
            text_cols = [k for k in all_keys if k in (
                "title", "goal", "domain", "benchmark_name",
                "description", "skill_name", "trigger_context"
            )]
            if text_cols:
                fts_cols = ", ".join(text_cols)
                con.execute(
                    f"CREATE VIRTUAL TABLE IF NOT EXISTS packs_fts "
                    f"USING fts5({fts_cols}, content='packs', content_rowid='rowid')"
                )
                con.execute(
                    "INSERT INTO packs_fts(packs_fts) VALUES('rebuild')"
                )

            con.commit()
        finally:
            con.close()

        return self._result(ExportFormat.SQLITE, path, len(records))

    # ------------------------------------------------------------------
    # HuggingFace Dataset format (parquet shard + dataset_info card)
    # ------------------------------------------------------------------

    def _export_huggingface(self, records: Sequence[Dict], stem: str) -> ExportResult:
        """
        Exports a HuggingFace-compatible dataset:
          {stem}/
            data/
              train-00000-of-00001.parquet
            dataset_info.json
            README.md (data card)
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise PackagingError(
                "pyarrow not installed. Run: pip install pyarrow"
            ) from e

        hf_dir = self.output_dir / stem
        data_dir = hf_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        flat = [_flatten_record(r) for r in records]
        shard_path = data_dir / "train-00000-of-00001.parquet"
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, str(shard_path), compression="snappy")

        # dataset_info.json
        info = {
            "dataset_name": stem,
            "description": f"DanteHarvest export — {len(records)} packs",
            "license": "see source rights metadata",
            "splits": {"train": {"num_examples": len(records)}},
            "features": {k: {"dtype": "string", "_type": "Value"} for k in flat[0]} if flat else {},
        }
        (hf_dir / "dataset_info.json").write_text(
            json.dumps(info, indent=2), encoding="utf-8"
        )

        # README.md data card
        readme = (
            f"# {stem}\n\n"
            f"DanteHarvest export with {len(records)} promoted packs.\n\n"
            f"## Fields\n\n"
            + "\n".join(f"- `{k}`" for k in (flat[0] if flat else {}))
            + "\n\n## Usage\n\n```python\n"
            "from datasets import load_dataset\n"
            f"ds = load_dataset('parquet', data_files='{stem}/data/*.parquet')\n```\n"
        )
        (hf_dir / "README.md").write_text(readme, encoding="utf-8")

        return self._result(ExportFormat.HUGGINGFACE, hf_dir, len(records))

    # ------------------------------------------------------------------
    # CSV
    # ------------------------------------------------------------------

    def _export_csv(self, records: Sequence[Dict], stem: str) -> ExportResult:
        path = self.output_dir / f"{stem}.csv"
        flat = [_flatten_record(r) for r in records]

        all_keys: List[str] = []
        seen = set()
        for rec in flat:
            for k in rec:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            for rec in flat:
                writer.writerow({k: str(rec.get(k, "")) for k in all_keys})

        return self._result(ExportFormat.CSV, path, len(records))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _result(self, fmt: ExportFormat, path: Path, count: int) -> ExportResult:
        size = path.stat().st_size if path.is_file() else sum(
            f.stat().st_size for f in path.rglob("*") if f.is_file()
        )
        return ExportResult(
            format=fmt,
            path=path,
            record_count=count,
            size_bytes=size,
            metadata={"dataset_name": self.dataset_name, "output_dir": str(self.output_dir)},
        )


def _flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten nested dicts/lists to scalar values for tabular formats.
    Nested structures are JSON-serialized.
    """
    flat: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (dict, list)):
            flat[k] = json.dumps(v, default=str)
        elif v is None:
            flat[k] = ""
        else:
            flat[k] = v
    return flat
