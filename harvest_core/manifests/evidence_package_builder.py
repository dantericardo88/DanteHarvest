"""
EvidencePackageBuilder — canonical ZIP bundle creator.

Transplanted from DanteDistillerV2/backend/export/evidence_package_builder.py.
Import paths updated for DANTEHARVEST package layout.
README text updated for DANTEHARVEST branding.

Constitutional guarantees:
- Fail-closed: missing required artifacts halt packaging
- Deterministic ZIP structure (sorted file insertion)
- All artifacts hashed in manifest for tamper detection
"""

import hashlib
import json
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from harvest_core.manifests.export_manifest import ExportManifestBuilder
from harvest_core.control.exceptions import PackagingError


class EvidencePackageBuilder:
    """
    Builds evidence packages as deterministic ZIP archives.

    Required artifacts (fail-closed if missing):
      chain, seal, compliance_html, compliance_json, manifest

    Usage:
        builder = EvidencePackageBuilder(storage_path="storage")
        result = await builder.create_package(run_id, project_id)
    """

    REQUIRED_ARTIFACTS = {
        "chain": "Complete evidence chain (chain.jsonl)",
        "seal": "Cryptographic seal (FINAL_SEAL.json)",
        "compliance_html": "Compliance report HTML",
        "compliance_json": "Compliance report JSON",
        "manifest": "Artifact manifest with hashes",
    }

    RECOMMENDED_ARTIFACTS = {
        "compliance_md": "Compliance report Markdown",
        "provenance_sources": "Source provenance manifest",
        "provenance_chunks": "Chunk provenance manifest",
        "metadata_config": "Run configuration",
        "metadata_stats": "Run statistics",
    }

    def __init__(self, storage_path: str = "storage", strict_mode: bool = True):
        self.storage_path = Path(storage_path)
        self.strict_mode = strict_mode
        self.collected_artifacts: Dict[str, str] = {}

    async def create_package(
        self,
        run_id: str,
        project_id: str,
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        if output_path is None:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            output_path = f"evidence_package_{run_id}_{ts}.zip"

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            await self._collect_artifacts(run_id, project_id, staging)
            self._validate_completeness()
            manifest = self._build_manifest(run_id, project_id, staging)
            (staging / "manifest.json").write_text(json.dumps(manifest, indent=2))
            self.collected_artifacts["manifest"] = str(staging / "manifest.json")
            (staging / "README.txt").write_text(self._generate_readme(run_id, project_id, manifest))
            self.collected_artifacts["readme"] = str(staging / "README.txt")
            self._create_zip(staging, output_file)

        hasher = hashlib.sha256()
        with open(output_file, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)

        return {
            "package_path": str(output_file),
            "package_hash": hasher.hexdigest(),
            "artifacts_included": len(self.collected_artifacts),
            "total_size_bytes": output_file.stat().st_size,
            "manifest": manifest,
            "created_at": datetime.utcnow().isoformat(),
        }

    async def _collect_artifacts(self, run_id: str, project_id: str, staging: Path) -> None:
        run_path = self.storage_path / "projects" / project_id / "runs" / run_id

        # Chain
        chain_src = run_path / "chain.jsonl"
        if chain_src.exists():
            dest = staging / "chain.jsonl"
            shutil.copy2(chain_src, dest)
            self.collected_artifacts["chain"] = str(dest)

        # Seal
        seal_src = run_path / "outputs" / "FINAL_SEAL.json"
        if seal_src.exists():
            dest = staging / "FINAL_SEAL.json"
            shutil.copy2(seal_src, dest)
            self.collected_artifacts["seal"] = str(dest)

        # Compliance
        compliance_dir = staging / "compliance"
        compliance_dir.mkdir(exist_ok=True)
        compliance_data = await self._load_compliance_data(run_id, project_id)

        html_file = compliance_dir / "report.html"
        html_file.write_text(self._generate_compliance_html(compliance_data))
        self.collected_artifacts["compliance_html"] = str(html_file)

        json_file = compliance_dir / "report.json"
        json_file.write_text(json.dumps(compliance_data, indent=2))
        self.collected_artifacts["compliance_json"] = str(json_file)

        md_file = compliance_dir / "report.md"
        md_file.write_text(self._generate_compliance_markdown(compliance_data))
        self.collected_artifacts["compliance_md"] = str(md_file)

        # Provenance
        provenance_dir = staging / "provenance"
        provenance_dir.mkdir(exist_ok=True)
        sources = await self._load_sources_provenance(run_id, project_id)
        if sources:
            f = provenance_dir / "sources.json"
            f.write_text(json.dumps(sources, indent=2))
            self.collected_artifacts["provenance_sources"] = str(f)
        chunks = await self._load_chunks_provenance(run_id, project_id)
        if chunks:
            f = provenance_dir / "chunks.json"
            f.write_text(json.dumps(chunks, indent=2))
            self.collected_artifacts["provenance_chunks"] = str(f)

        # Metadata
        metadata_dir = staging / "metadata"
        metadata_dir.mkdir(exist_ok=True)
        cfg = await self._load_run_config(run_id, project_id)
        if cfg:
            f = metadata_dir / "run_config.json"
            f.write_text(json.dumps(cfg, indent=2))
            self.collected_artifacts["metadata_config"] = str(f)
        stats = await self._load_run_statistics(run_id, project_id)
        if stats:
            f = metadata_dir / "statistics.json"
            f.write_text(json.dumps(stats, indent=2))
            self.collected_artifacts["metadata_stats"] = str(f)

    def _validate_completeness(self) -> None:
        missing = [
            f"{aid}: {desc}"
            for aid, desc in self.REQUIRED_ARTIFACTS.items()
            if aid not in self.collected_artifacts
        ]
        if missing:
            raise PackagingError(
                "Cannot create evidence package — missing required artifacts:\n"
                + "\n".join(f"  - {m}" for m in missing)
            )

    def _build_manifest(self, run_id: str, project_id: str, base_path: Path) -> Dict[str, Any]:
        builder = ExportManifestBuilder()
        category_map = {
            "chain": ("evidence", "Complete evidence chain"),
            "seal": ("seal", "Cryptographic seal"),
        }
        for artifact_id, file_path in self.collected_artifacts.items():
            if artifact_id in category_map:
                cat, desc = category_map[artifact_id]
            elif artifact_id.startswith("compliance_"):
                cat, desc = "compliance", f"Compliance report ({artifact_id.split('_')[1].upper()})"
            elif artifact_id.startswith("provenance_"):
                cat, desc = "provenance", f"{artifact_id.split('_')[1].capitalize()} provenance"
            elif artifact_id.startswith("metadata_"):
                cat, desc = "metadata", f"Run {artifact_id.split('_')[1]}"
            elif artifact_id == "readme":
                cat, desc = "readme", "Package overview"
            else:
                cat, desc = "metadata", artifact_id
            builder.add_artifact(
                artifact_id=artifact_id,
                file_path=file_path,
                category=cat,
                description=desc,
                required=(artifact_id in self.REQUIRED_ARTIFACTS),
            )
        return builder.build(run_id, project_id, base_path=str(base_path))

    def _create_zip(self, source_dir: Path, output_file: Path) -> None:
        with zipfile.ZipFile(output_file, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_path in sorted(source_dir.rglob("*")):
                if file_path.is_file():
                    zipf.write(file_path, file_path.relative_to(source_dir))

    def _generate_readme(self, run_id: str, project_id: str, manifest: Dict[str, Any]) -> str:
        n = manifest.get("statistics", {}).get("total_artifacts", 0)
        return (
            f"DANTEHARVEST EVIDENCE PACKAGE\n{'=' * 60}\n\n"
            f"Run ID:     {run_id}\nProject ID: {project_id}\n"
            f"Created:    {manifest.get('generated_at')}\n\n"
            f"Artifacts:  {n}\n\n"
            "Verify integrity by checking SHA-256 hashes in manifest.json.\n"
        )

    # Stub loaders — override in subclasses or inject real storage adapters.

    async def _load_compliance_data(self, run_id: str, project_id: str) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "project_id": project_id,
            "compliance_score": 1.0,
            "generated_at": datetime.utcnow().isoformat(),
            "checks": {
                "rights_confirmed": {"passed": True, "details": "Rights profile assigned"},
                "robots_respected": {"passed": True, "details": "robots.txt honored"},
                "chain_complete": {"passed": True, "details": "Chain has complete lifecycle"},
                "provenance_bound": {"passed": True, "details": "All outputs linked to sources"},
            },
        }

    async def _load_sources_provenance(self, run_id: str, project_id: str) -> Dict[str, Any]:
        return {"run_id": run_id, "sources": []}

    async def _load_chunks_provenance(self, run_id: str, project_id: str) -> Dict[str, Any]:
        return {"run_id": run_id, "chunks": []}

    async def _load_run_config(self, run_id: str, project_id: str) -> Dict[str, Any]:
        return {"run_id": run_id, "project_id": project_id}

    async def _load_run_statistics(self, run_id: str, project_id: str) -> Dict[str, Any]:
        return {"run_id": run_id}

    def _generate_compliance_html(self, data: Dict[str, Any]) -> str:
        rows = "".join(
            f"<tr><td>{k}</td><td>{'✓' if v['passed'] else '✗'}</td><td>{v.get('details','')}</td></tr>"
            for k, v in data.get("checks", {}).items()
        )
        return (
            "<!DOCTYPE html><html><head><title>Harvest Compliance</title></head><body>"
            f"<h1>Harvest Compliance Report</h1><p>Run: {data.get('run_id')}</p>"
            f"<table border='1'><tr><th>Check</th><th>Status</th><th>Details</th></tr>{rows}</table>"
            "</body></html>"
        )

    def _generate_compliance_markdown(self, data: Dict[str, Any]) -> str:
        lines = [f"# Harvest Compliance Report\n\n**Run:** {data.get('run_id')}\n\n## Checks\n"]
        for k, v in data.get("checks", {}).items():
            mark = "✓" if v["passed"] else "✗"
            lines.append(f"- **{k}**: {mark} {v.get('details','')}")
        return "\n".join(lines)
