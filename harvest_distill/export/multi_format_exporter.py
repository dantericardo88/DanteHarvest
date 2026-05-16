"""Multi-format exporter for harvested artifacts."""
from __future__ import annotations

import csv
import io
import json
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional


class MultiFormatExporter:
    """Exports artifact collections to multiple formats."""

    SUPPORTED_FORMATS = ["jsonl", "json", "csv", "arrow", "parquet", "markdown", "html"]

    def export(self, artifacts: List[dict], format: str, output_path: str = None) -> bytes:
        """Export artifacts to the given format. Returns bytes."""
        fmt = format.lower()
        if fmt == "jsonl":
            return self._to_jsonl(artifacts)
        elif fmt == "json":
            return self._to_json(artifacts)
        elif fmt == "csv":
            return self._to_csv(artifacts)
        elif fmt == "arrow":
            return self._to_arrow(artifacts)
        elif fmt == "parquet":
            return self._to_parquet(artifacts)
        elif fmt == "markdown":
            return self._to_markdown(artifacts)
        elif fmt == "html":
            return self._to_html(artifacts)
        else:
            raise ValueError(
                f"Unsupported format: {format}. Choose from {self.SUPPORTED_FORMATS}"
            )

    def export_to_file(self, artifacts: List[dict], format: str, output_path: str) -> int:
        """Export to file. Returns byte count written."""
        data = self.export(artifacts, format)
        Path(output_path).write_bytes(data)
        return len(data)

    # ------------------------------------------------------------------
    # Format implementations
    # ------------------------------------------------------------------

    def _to_jsonl(self, artifacts: List[dict]) -> bytes:
        """One JSON object per line — streaming-friendly."""
        lines = [json.dumps(a, ensure_ascii=False) for a in artifacts]
        return ("\n".join(lines) + "\n").encode("utf-8")

    def _to_json(self, artifacts: List[dict]) -> bytes:
        return json.dumps(artifacts, indent=2, ensure_ascii=False).encode("utf-8")

    def _to_csv(self, artifacts: List[dict]) -> bytes:
        if not artifacts:
            return b""
        buf = io.StringIO()
        # Collect all field names (union of all keys, insertion-ordered)
        fields = list(dict.fromkeys(k for a in artifacts for k in a.keys()))
        writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for a in artifacts:
            # Flatten nested values to strings
            row = {
                k: json.dumps(v) if isinstance(v, (dict, list)) else v
                for k, v in a.items()
            }
            writer.writerow(row)
        return buf.getvalue().encode("utf-8")

    def _to_arrow(self, artifacts: List[dict]) -> bytes:
        """Arrow IPC format using pyarrow if available, else JSONL fallback."""
        try:
            import pyarrow as pa
            import pyarrow.ipc as ipc

            table = pa.Table.from_pylist(artifacts)
            buf = io.BytesIO()
            with ipc.new_file(buf, table.schema) as writer:
                writer.write_table(table)
            return buf.getvalue()
        except ImportError:
            warnings.warn(
                "pyarrow not installed. Falling back to JSONL for Arrow format. "
                "Install: pip install pyarrow"
            )
            return self._to_jsonl(artifacts)

    def _to_parquet(self, artifacts: List[dict]) -> bytes:
        """Parquet format using pyarrow if available, else JSONL fallback."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.Table.from_pylist(artifacts)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            return buf.getvalue()
        except ImportError:
            warnings.warn(
                "pyarrow not installed. Falling back to JSONL for Parquet format. "
                "Install: pip install pyarrow"
            )
            return self._to_jsonl(artifacts)

    def _to_markdown(self, artifacts: List[dict]) -> bytes:
        if not artifacts:
            return b"(no artifacts)\n"
        fields = list(dict.fromkeys(k for a in artifacts for k in a.keys()))
        lines = [
            "| " + " | ".join(fields) + " |",
            "| " + " | ".join(["---"] * len(fields)) + " |",
        ]
        for a in artifacts:
            row = [str(a.get(f, "")).replace("|", "\\|")[:80] for f in fields]
            lines.append("| " + " | ".join(row) + " |")
        return ("\n".join(lines) + "\n").encode("utf-8")

    def _to_html(self, artifacts: List[dict]) -> bytes:
        if not artifacts:
            return b"<p>No artifacts</p>"
        fields = list(dict.fromkeys(k for a in artifacts for k in a.keys()))
        rows = ""
        for a in artifacts:
            cells = "".join(
                f"<td>{str(a.get(f, ''))[:200]}</td>" for f in fields
            )
            rows += f"<tr>{cells}</tr>\n"
        headers = "".join(f"<th>{f}</th>" for f in fields)
        html = (
            f"<table><thead><tr>{headers}</tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )
        return html.encode("utf-8")

    def get_schema(self, artifacts: List[dict]) -> dict:
        """Infer schema from artifact list."""
        if not artifacts:
            return {"fields": [], "count": 0}
        fields: Dict[str, dict] = {}
        for a in artifacts:
            for k, v in a.items():
                t = type(v).__name__
                if k not in fields:
                    fields[k] = {"type": t, "nullable": False}
                elif fields[k]["type"] != t:
                    fields[k]["type"] = "any"
                if v is None:
                    fields[k]["nullable"] = True
        return {"fields": list(fields.items()), "count": len(artifacts)}
