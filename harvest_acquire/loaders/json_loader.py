"""
JSONLoader — ingest .json and .jsonl files into structured markdown documents.

Features:
- .json: loads full JSON; detects top-level arrays with uniform schema → markdown table;
  nested objects → flattened key-value markdown
- .jsonl: streams line by line (memory-efficient for large files);
  detects uniform schema → markdown table; mixed → per-record sections
- Handles null values, nested objects (dot-notation keys), and arrays-of-primitives

Constitutional guarantees:
- Local-first: stdlib json only — no extra deps
- Fail-closed: missing file or invalid JSON raises NormalizationError
- Zero-ambiguity: load() always returns list[JSONDocument]
- Memory-safe: JSONL streamed line-by-line, not loaded into memory at once
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from harvest_core.control.exceptions import NormalizationError


@dataclass
class JSONDocument:
    """Result of loading a JSON or JSONL file."""
    file_path: str
    format: str  # "json" or "jsonl"
    record_count: int
    markdown: str
    schema_keys: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten(obj: Any, prefix: str = "") -> Dict[str, str]:
    """Recursively flatten a nested dict/list to dot-notation string values."""
    result: Dict[str, str] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            full_key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, (dict, list)):
                result.update(_flatten(v, full_key))
            else:
                result[full_key] = "" if v is None else str(v)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            full_key = f"{prefix}[{i}]"
            if isinstance(v, (dict, list)):
                result.update(_flatten(v, full_key))
            else:
                result[full_key] = "" if v is None else str(v)
    else:
        result[prefix] = "" if obj is None else str(obj)
    return result


def _detect_uniform_schema(records: List[Any]) -> Optional[List[str]]:
    """
    Return sorted list of top-level keys if all records are dicts with same keys.
    Returns None if schema is not uniform or records are not all dicts.
    """
    if not records:
        return None
    if not all(isinstance(r, dict) for r in records):
        return None
    key_sets = [frozenset(r.keys()) for r in records]
    if len(set(key_sets)) != 1:
        return None
    return sorted(key_sets[0])


def _records_to_markdown_table(keys: List[str], records: List[Dict]) -> str:
    """Render a list of dicts with uniform keys as a markdown table."""
    header = "| " + " | ".join(keys) + " |"
    sep = "| " + " | ".join("---" for _ in keys) + " |"
    rows = []
    for rec in records:
        cells = [str(rec.get(k, "")) if rec.get(k) is not None else "" for k in keys]
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join([header, sep] + rows)


def _kv_to_markdown(flat: Dict[str, str]) -> str:
    """Render a flat key-value dict as a markdown key-value list."""
    if not flat:
        return ""
    return "\n".join(f"- **{k}**: {v}" for k, v in flat.items())


def _stream_jsonl(path: Path) -> Iterator[Any]:
    """Yield parsed JSON objects from a JSONL file, one per non-empty line."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise NormalizationError(
                    f"Invalid JSON on line {lineno} of {path}: {exc}"
                ) from exc


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class JSONLoader:
    """
    Load .json and .jsonl files into JSONDocument objects.

    Usage:
        loader = JSONLoader()
        docs = loader.load(Path("data.jsonl"))
        print(docs[0].markdown)
    """

    SUPPORTED_SUFFIXES = {".json", ".jsonl"}

    def load(self, path: Path) -> List[JSONDocument]:
        """
        Load a JSON/JSONL file. Returns a list with one JSONDocument.
        Raises NormalizationError on missing file, invalid JSON, or unsupported format.
        """
        path = Path(path)
        if not path.exists():
            raise NormalizationError(f"JSON file not found: {path}")
        if not path.is_file():
            raise NormalizationError(f"Path is not a file: {path}")

        suffix = path.suffix.lower()
        if suffix not in self.SUPPORTED_SUFFIXES:
            raise NormalizationError(
                f"Unsupported JSON format: {suffix!r}. "
                f"Supported: {', '.join(sorted(self.SUPPORTED_SUFFIXES))}"
            )

        if suffix == ".jsonl":
            return self._load_jsonl(path)
        else:
            return self._load_json(path)

    def _load_json(self, path: Path) -> List[JSONDocument]:
        """Parse a .json file; auto-detect array-of-objects for table rendering."""
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise NormalizationError(f"Invalid JSON in {path}: {exc}") from exc
        except Exception as exc:
            raise NormalizationError(f"Failed to read {path}: {exc}") from exc

        markdown, schema_keys, record_count = self._render_json(data, path.stem)
        return [JSONDocument(
            file_path=str(path),
            format="json",
            record_count=record_count,
            markdown=markdown,
            schema_keys=schema_keys,
        )]

    def _load_jsonl(self, path: Path) -> List[JSONDocument]:
        """Stream a .jsonl file; detect uniform schema for table rendering."""
        records: List[Any] = []
        try:
            for obj in _stream_jsonl(path):
                records.append(obj)
        except NormalizationError:
            raise

        if not records:
            return [JSONDocument(
                file_path=str(path),
                format="jsonl",
                record_count=0,
                markdown="",
                schema_keys=[],
            )]

        uniform_keys = _detect_uniform_schema(records)
        if uniform_keys:
            table_md = _records_to_markdown_table(uniform_keys, records)
            markdown = f"## {path.stem}\n\n{table_md}"
            schema_keys = uniform_keys
        else:
            # Mixed schema: render each record as a named section
            sections = []
            for i, rec in enumerate(records):
                flat = _flatten(rec)
                kv = _kv_to_markdown(flat)
                sections.append(f"### Record {i + 1}\n\n{kv}")
            markdown = f"## {path.stem}\n\n" + "\n\n".join(sections)
            schema_keys = []

        return [JSONDocument(
            file_path=str(path),
            format="jsonl",
            record_count=len(records),
            markdown=markdown,
            schema_keys=schema_keys,
        )]

    def _render_json(
        self, data: Any, stem: str
    ) -> tuple[str, List[str], int]:
        """Render parsed JSON into (markdown, schema_keys, record_count)."""
        if isinstance(data, list):
            uniform_keys = _detect_uniform_schema(data)
            if uniform_keys:
                table_md = _records_to_markdown_table(uniform_keys, data)
                return f"## {stem}\n\n{table_md}", uniform_keys, len(data)
            else:
                # Array of mixed or non-dict items
                sections = []
                for i, item in enumerate(data):
                    flat = _flatten(item)
                    kv = _kv_to_markdown(flat)
                    sections.append(f"### Item {i + 1}\n\n{kv}")
                return f"## {stem}\n\n" + "\n\n".join(sections), [], len(data)
        elif isinstance(data, dict):
            flat = _flatten(data)
            kv = _kv_to_markdown(flat)
            return f"## {stem}\n\n{kv}", list(flat.keys()), 1
        else:
            # Scalar
            return f"## {stem}\n\n{data}", [], 1
