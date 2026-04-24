"""
ArtifactStore — hash-addressed local artifact registry.

Content-addressed storage: artifacts are stored at artifacts/{sha256[:2]}/{sha256}/
so identical content is deduplicated automatically.

Provides CRUD operations and metadata index for all ingested artifacts.
Local-first: no network calls.  All writes are atomic (write-then-rename).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import StorageError


@dataclass
class ArtifactRecord:
    artifact_id: str
    sha256: str
    source_type: str
    storage_path: str
    file_size_bytes: int
    created_at: str
    run_id: str
    project_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ArtifactRecord":
        return cls(**d)


class ArtifactStore:
    """
    Hash-addressed local store for Harvest artifacts.

    All artifacts are stored at:
        <root>/objects/{sha256[:2]}/{sha256}/{original_filename}

    A flat JSON index at <root>/index.json maps artifact_id → ArtifactRecord.

    Usage:
        store = ArtifactStore(root="storage")
        record = store.put(
            source_path=Path("report.pdf"),
            run_id="run-001",
            project_id="proj-001",
            source_type="document",
        )
        same_or_new = store.get(record.artifact_id)
    """

    INDEX_FILE = "index.json"

    def __init__(self, root: str = "storage"):
        self.root = Path(root)
        self._index_path = self.root / self.INDEX_FILE
        self.root.mkdir(parents=True, exist_ok=True)
        self._index: Dict[str, dict] = self._load_index()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def put(
        self,
        source_path: Path,
        run_id: str,
        project_id: str,
        source_type: str = "unknown",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ArtifactRecord:
        """
        Store a file.  If content already exists (same SHA-256), returns
        the existing record rather than duplicating storage.
        """
        source_path = Path(source_path)
        if not source_path.exists():
            raise StorageError(f"Source file not found: {source_path}")

        sha256 = _compute_sha256(source_path)
        file_size = source_path.stat().st_size

        existing = self._find_by_sha256(sha256)
        if existing:
            return existing

        artifact_id = str(uuid4())
        dest = self._content_path(sha256, source_path.name)
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write: copy to temp, rename
        tmp = dest.parent / (dest.name + ".tmp")
        shutil.copy2(source_path, tmp)
        tmp.rename(dest)

        record = ArtifactRecord(
            artifact_id=artifact_id,
            sha256=sha256,
            source_type=source_type,
            storage_path=str(dest),
            file_size_bytes=file_size,
            created_at=datetime.utcnow().isoformat(),
            run_id=run_id,
            project_id=project_id,
            metadata=metadata or {},
        )
        self._index[artifact_id] = record.to_dict()
        self._save_index()
        return record

    def put_text(
        self,
        text: str,
        filename: str,
        run_id: str,
        project_id: str,
        source_type: str = "text",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ArtifactRecord:
        """Store a text blob."""
        sha256 = hashlib.sha256(text.encode()).hexdigest()
        existing = self._find_by_sha256(sha256)
        if existing:
            return existing

        artifact_id = str(uuid4())
        dest = self._content_path(sha256, filename)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.parent / (dest.name + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.rename(dest)

        record = ArtifactRecord(
            artifact_id=artifact_id,
            sha256=sha256,
            source_type=source_type,
            storage_path=str(dest),
            file_size_bytes=len(text.encode()),
            created_at=datetime.utcnow().isoformat(),
            run_id=run_id,
            project_id=project_id,
            metadata=metadata or {},
        )
        self._index[artifact_id] = record.to_dict()
        self._save_index()
        return record

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get(self, artifact_id: str) -> ArtifactRecord:
        """Return ArtifactRecord by artifact_id.  Raises StorageError if not found."""
        entry = self._index.get(artifact_id)
        if not entry:
            raise StorageError(f"Artifact not found: {artifact_id}")
        return ArtifactRecord.from_dict(entry)

    def get_path(self, artifact_id: str) -> Path:
        """Return the Path to the stored content file."""
        record = self.get(artifact_id)
        return Path(record.storage_path)

    def read_text(self, artifact_id: str) -> str:
        return self.get_path(artifact_id).read_text(encoding="utf-8")

    def list(
        self,
        run_id: Optional[str] = None,
        project_id: Optional[str] = None,
        source_type: Optional[str] = None,
    ) -> List[ArtifactRecord]:
        """List artifacts with optional filters."""
        records = [ArtifactRecord.from_dict(v) for v in self._index.values()]
        if run_id:
            records = [r for r in records if r.run_id == run_id]
        if project_id:
            records = [r for r in records if r.project_id == project_id]
        if source_type:
            records = [r for r in records if r.source_type == source_type]
        return records

    def delete(self, artifact_id: str) -> None:
        """Remove an artifact record (does not delete the content file)."""
        if artifact_id not in self._index:
            raise StorageError(f"Artifact not found: {artifact_id}")
        del self._index[artifact_id]
        self._save_index()

    def stats(self) -> dict:
        records = [ArtifactRecord.from_dict(v) for v in self._index.values()]
        return {
            "total_artifacts": len(records),
            "total_bytes": sum(r.file_size_bytes for r in records),
            "by_type": _count_by(records, lambda r: r.source_type),
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _content_path(self, sha256: str, filename: str) -> Path:
        return self.root / "objects" / sha256[:2] / sha256 / filename

    def _find_by_sha256(self, sha256: str) -> Optional[ArtifactRecord]:
        for entry in self._index.values():
            if entry.get("sha256") == sha256:
                return ArtifactRecord.from_dict(entry)
        return None

    def _load_index(self) -> Dict[str, dict]:
        if self._index_path.exists():
            try:
                return json.loads(self._index_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_index(self) -> None:
        tmp = self._index_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._index, indent=2), encoding="utf-8")
        tmp.replace(self._index_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _count_by(records: list, key_fn) -> dict:
    counts: Dict[str, int] = {}
    for r in records:
        k = key_fn(r)
        counts[k] = counts.get(k, 0) + 1
    return counts
