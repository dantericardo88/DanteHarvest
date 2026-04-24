"""
ExportManifestBuilder — deterministic artifact registry.

Transplanted from DanteDistillerV2/backend/export/export_manifest.py.
Import paths updated for DANTEHARVEST package layout.

Constitutional guarantees:
- Deterministic ordering (same artifacts → identical manifest)
- Stable SHA-256 hashing (reproducible verification)
- Schema versioning for backward compatibility
- Fail-closed on missing required artifacts (strict_mode=True)
"""

import hashlib
import json
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from harvest_core.control.exceptions import ManifestError


class ArtifactEntry:
    def __init__(
        self,
        artifact_id: str,
        file_path: str,
        category: str,
        description: Optional[str] = None,
        required: bool = True,
    ):
        self.artifact_id = artifact_id
        self.file_path = file_path
        self.category = category
        self.description = description
        self.required = required
        self.file_size: Optional[int] = None
        self.sha256_hash: Optional[str] = None
        self.relative_path: Optional[str] = None

    def compute_metadata(self, base_path: Optional[Path] = None) -> None:
        path = Path(self.file_path)
        if not path.exists():
            if self.required:
                raise ManifestError(f"Required artifact missing: {self.artifact_id} at {self.file_path}")
            return
        self.file_size = path.stat().st_size
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        self.sha256_hash = hasher.hexdigest()
        if base_path:
            try:
                self.relative_path = str(path.relative_to(base_path))
            except ValueError:
                self.relative_path = path.name
        else:
            self.relative_path = path.name

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "artifact_id": self.artifact_id,
            "category": self.category,
            "file_path": self.relative_path or self.file_path,
            "required": self.required,
        }
        if self.description:
            data["description"] = self.description
        if self.file_size is not None:
            data["size_bytes"] = self.file_size
        if self.sha256_hash:
            data["sha256"] = self.sha256_hash
        return data


class ExportManifestBuilder:
    """
    Canonical builder for Harvest export manifests.

    Usage:
        builder = ExportManifestBuilder()
        builder.add_artifact("chain", "/path/chain.jsonl", category="evidence")
        manifest = builder.build(run_id="run-001", project_id="harvest-001")
    """

    CURRENT_VERSION = "1.0"

    CATEGORIES = {
        "evidence": "Evidence chain and audit trail",
        "compliance": "Rights and constitutional compliance reports",
        "seal": "Cryptographic seals and signatures",
        "provenance": "Source and transformation tracking",
        "metadata": "Run configuration and statistics",
        "dataset": "Exported datasets and knowledge packs",
        "citation": "Bibliography and rights references",
        "readme": "Documentation and instructions",
    }

    def __init__(self, manifest_version: str = CURRENT_VERSION, strict_mode: bool = False):
        self.manifest_version = manifest_version
        self.strict_mode = strict_mode
        self.artifacts: OrderedDict[str, ArtifactEntry] = OrderedDict()
        self.categories_used: Set[str] = set()
        self.total_size_bytes: int = 0

    def add_artifact(
        self,
        artifact_id: str,
        file_path: str,
        category: str,
        description: Optional[str] = None,
        required: bool = True,
    ) -> "ExportManifestBuilder":
        if category not in self.CATEGORIES:
            raise ManifestError(f"Invalid category '{category}'. Valid: {list(self.CATEGORIES)}")
        if artifact_id in self.artifacts:
            raise ManifestError(f"Duplicate artifact_id: {artifact_id}")
        self.artifacts[artifact_id] = ArtifactEntry(artifact_id, file_path, category, description, required)
        self.categories_used.add(category)
        return self

    def build(
        self,
        run_id: str,
        project_id: str,
        base_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        base_path_obj = Path(base_path) if base_path else None
        for artifact in self.artifacts.values():
            try:
                artifact.compute_metadata(base_path_obj)
            except ManifestError:
                if self.strict_mode:
                    raise
            if artifact.file_size:
                self.total_size_bytes += artifact.file_size

        manifest: OrderedDict = OrderedDict()
        manifest["manifest_version"] = self.manifest_version
        manifest["run_id"] = run_id
        manifest["project_id"] = project_id
        manifest["generated_at"] = datetime.utcnow().isoformat()
        manifest["artifacts"] = [a.to_dict() for a in self.artifacts.values()]
        manifest["statistics"] = {
            "total_artifacts": len(self.artifacts),
            "total_size_bytes": self.total_size_bytes,
            "categories_used": sorted(self.categories_used),
            "required_artifacts": sum(1 for a in self.artifacts.values() if a.required),
            "optional_artifacts": sum(1 for a in self.artifacts.values() if not a.required),
        }
        manifest["manifest_hash"] = self._compute_manifest_hash(manifest)
        return dict(manifest)

    def _compute_manifest_hash(self, manifest: OrderedDict) -> str:
        copy = OrderedDict(manifest)
        copy.pop("manifest_hash", None)
        copy.pop("generated_at", None)
        canonical = json.dumps(copy, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def to_json(self, run_id: str, project_id: str, base_path: Optional[str] = None, indent: int = 2) -> str:
        return json.dumps(self.build(run_id, project_id, base_path), indent=indent)

    def to_file(self, output_path: str, run_id: str, project_id: str, base_path: Optional[str] = None) -> None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(run_id, project_id, base_path, indent=2))
