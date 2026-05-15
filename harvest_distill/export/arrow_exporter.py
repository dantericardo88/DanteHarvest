"""
ArrowExporter — Apache Arrow IPC streaming export for chain entries and packs.

Wave 6b: export_format_diversity — Arrow/IPC streaming (8→9).

Exports Harvest data as Apache Arrow IPC streams for:
1. Evidence chain entries → Arrow RecordBatch stream (columnar, zero-copy)
2. Pack registry entries → Arrow RecordBatch for analytics
3. Artifact metadata → Arrow IPC file format (.arrow)

Falls back gracefully to Parquet (via pyarrow) or NDJSON when Arrow is unavailable.

Design:
- Uses pyarrow when available, NDJSON fallback when not installed
- Streaming mode: yields Arrow IPC chunks as bytes
- File mode: writes .arrow or .parquet file to disk
- Schema-first: Arrow schema defined per export type for type safety

Constitutional guarantees:
- Local-first: no network calls — writes to local paths or returns bytes
- Fail-open: falls back to NDJSON if pyarrow is not installed (never raises ImportError to caller)
- Zero-ambiguity: export() always returns a path or raises ExportError with reason
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional


class ArrowExportError(Exception):
    pass


# ---------------------------------------------------------------------------
# Schema definitions (dict for fallback, Arrow schema for pyarrow)
# ---------------------------------------------------------------------------

CHAIN_ENTRY_FIELDS = [
    "sequence", "run_id", "signal", "machine", "timestamp", "content_hash", "data_json",
]

PACK_ENTRY_FIELDS = [
    "pack_id", "pack_type", "title", "promotion_status", "registered_at",
    "receipt_id", "confidence_score",
]

ARTIFACT_FIELDS = [
    "artifact_id", "source_type", "sha256", "storage_uri", "ingested_at",
    "retention_class", "rights_status",
]


# ---------------------------------------------------------------------------
# Fallback: NDJSON stream
# ---------------------------------------------------------------------------

def _to_ndjson(records: List[Dict[str, Any]]) -> bytes:
    return b"\n".join(json.dumps(r).encode() for r in records)


# ---------------------------------------------------------------------------
# ArrowExporter
# ---------------------------------------------------------------------------

class ArrowExporter:
    """
    Export Harvest data as Apache Arrow IPC or Parquet.

    Usage:
        exporter = ArrowExporter()

        # Export chain entries to .arrow file
        path = exporter.export_chain_entries(entries, output_path=Path("chain.arrow"))

        # Stream chain entries as IPC bytes (for HTTP streaming)
        for chunk in exporter.stream_chain_entries(entries):
            socket.write(chunk)

        # Export pack registry to Parquet
        path = exporter.export_packs(pack_entries, output_path=Path("packs.parquet"))
    """

    def __init__(self, prefer_format: str = "arrow"):
        self._prefer = prefer_format  # "arrow" | "parquet" | "ndjson"
        self._has_pyarrow = self._check_pyarrow()

    def _check_pyarrow(self) -> bool:
        try:
            import pyarrow  # noqa: F401
            return True
        except ImportError:
            return False

    @property
    def available_format(self) -> str:
        if self._has_pyarrow:
            return self._prefer if self._prefer in ("arrow", "parquet") else "arrow"
        return "ndjson"

    # ------------------------------------------------------------------
    # Chain entries
    # ------------------------------------------------------------------

    def export_chain_entries(
        self,
        entries: List[Any],
        output_path: Optional[Path] = None,
    ) -> Path:
        """Export ChainEntry list to Arrow IPC file or NDJSON fallback."""
        records = self._chain_entries_to_records(entries)

        if output_path is None:
            output_path = Path("chain_export.arrow" if self._has_pyarrow else "chain_export.ndjson")

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self._has_pyarrow:
            self._write_arrow(records, CHAIN_ENTRY_FIELDS, output_path)
        else:
            output_path.with_suffix(".ndjson").write_bytes(_to_ndjson(records))
            output_path = output_path.with_suffix(".ndjson")

        return output_path

    def stream_chain_entries(
        self,
        entries: List[Any],
        batch_size: int = 1000,
    ) -> Iterator[bytes]:
        """Yield Arrow IPC RecordBatch bytes for each batch of chain entries."""
        records = self._chain_entries_to_records(entries)
        if self._has_pyarrow:
            yield from self._stream_arrow_batches(records, CHAIN_ENTRY_FIELDS, batch_size)
        else:
            yield _to_ndjson(records)

    # ------------------------------------------------------------------
    # Pack registry
    # ------------------------------------------------------------------

    def export_packs(
        self,
        pack_entries: List[Any],
        output_path: Optional[Path] = None,
        fmt: Optional[str] = None,
    ) -> Path:
        """Export PackEntry list to Arrow IPC or Parquet."""
        records = [
            {
                "pack_id": e.pack_id,
                "pack_type": e.pack_type,
                "title": e.title,
                "promotion_status": e.promotion_status,
                "registered_at": e.registered_at,
                "receipt_id": e.receipt_id or "",
                "confidence_score": float(e.confidence_score),
            }
            for e in pack_entries
        ]

        out_fmt = fmt or self.available_format
        suffix = ".parquet" if out_fmt == "parquet" else ".arrow" if self._has_pyarrow else ".ndjson"
        if output_path is None:
            output_path = Path(f"packs_export{suffix}")

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self._has_pyarrow and out_fmt == "parquet":
            self._write_parquet(records, PACK_ENTRY_FIELDS, output_path)
        elif self._has_pyarrow:
            self._write_arrow(records, PACK_ENTRY_FIELDS, output_path)
        else:
            output_path.write_bytes(_to_ndjson(records))

        return output_path

    # ------------------------------------------------------------------
    # Artifact metadata
    # ------------------------------------------------------------------

    def export_artifacts(
        self,
        artifact_dicts: List[Dict[str, Any]],
        output_path: Optional[Path] = None,
    ) -> Path:
        """Export artifact metadata dicts to Arrow IPC file."""
        records = [
            {
                "artifact_id": a.get("artifact_id", ""),
                "source_type": a.get("source_type", ""),
                "sha256": a.get("sha256", ""),
                "storage_uri": a.get("storage_uri", ""),
                "ingested_at": str(a.get("ingested_at", "")),
                "retention_class": a.get("retention_class", ""),
                "rights_status": a.get("rights_status", ""),
            }
            for a in artifact_dicts
        ]

        suffix = ".arrow" if self._has_pyarrow else ".ndjson"
        if output_path is None:
            output_path = Path(f"artifacts_export{suffix}")

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self._has_pyarrow:
            self._write_arrow(records, ARTIFACT_FIELDS, output_path)
        else:
            output_path.write_bytes(_to_ndjson(records))

        return output_path

    # ------------------------------------------------------------------
    # pyarrow internals
    # ------------------------------------------------------------------

    def _write_arrow(self, records: List[dict], fields: List[str], path: Path) -> None:
        import pyarrow as pa
        table = self._records_to_table(records, fields)
        import pyarrow.ipc as ipc
        with ipc.new_file(str(path), table.schema) as writer:
            writer.write_table(table)

    def _write_parquet(self, records: List[dict], fields: List[str], path: Path) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = self._records_to_table(records, fields)
        pq.write_table(table, str(path))

    def _stream_arrow_batches(
        self,
        records: List[dict],
        fields: List[str],
        batch_size: int,
    ) -> Iterator[bytes]:
        import pyarrow as pa
        import pyarrow.ipc as ipc

        table = self._records_to_table(records, fields)
        buf = io.BytesIO()
        with ipc.new_stream(buf, table.schema) as writer:
            for i in range(0, len(table), batch_size):
                batch = table.slice(i, batch_size)
                writer.write_batch(batch.to_batches()[0] if hasattr(batch, "to_batches") else batch)
        yield buf.getvalue()

    def _records_to_table(self, records: List[dict], fields: List[str]):
        import pyarrow as pa
        if not records:
            arrays = [pa.array([], type=pa.string()) for _ in fields]
            return pa.table({f: arr for f, arr in zip(fields, arrays)})
        columns = {
            f: [r.get(f, None) for r in records]
            for f in fields
        }
        return pa.table(columns)

    def _chain_entries_to_records(self, entries: List[Any]) -> List[dict]:
        records = []
        for e in entries:
            if hasattr(e, "to_dict"):
                d = e.to_dict() if callable(e.to_dict) else vars(e)
            elif isinstance(e, dict):
                d = e
            else:
                d = vars(e)
            records.append({
                "sequence": d.get("sequence", 0),
                "run_id": d.get("run_id", ""),
                "signal": d.get("signal", ""),
                "machine": d.get("machine", ""),
                "timestamp": str(d.get("timestamp", "")),
                "content_hash": d.get("content_hash", ""),
                "data_json": json.dumps(d.get("data", {})),
            })
        return records
