"""
FileIngestor — local file acquisition for DANTEHARVEST.

Ingests files (PDF, DOCX, image, video, audio) into the Harvest artifact store.
Assigns RightsProfile, computes SHA-256, emits chain entries, and returns
a typed raw artifact ready for normalization.

Constitutional guarantees:
- acquire.failed emitted on any error (fail-closed, never silent)
- SHA-256 computed before storage
- RightsProfile attached at ingest, never added later
- No network calls (local-first)
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Optional
from uuid import uuid4

from harvest_core.control.artifact_schemas import RawVideoAsset, RawAudioStream
from harvest_core.control.exceptions import AcquisitionError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.rights.rights_model import RightsProfile, SourceClass, default_rights_for
from harvest_acquire.loaders.spreadsheet_loader import SpreadsheetLoader
from harvest_acquire.loaders.email_loader import EmailLoader
from harvest_acquire.loaders.json_loader import JSONLoader


_VIDEO_SUFFIXES = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
_AUDIO_SUFFIXES = {".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac"}
_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp"}
_DOCUMENT_SUFFIXES = {".pdf", ".docx", ".pptx", ".xlsx", ".txt", ".md", ".html"}
_SPREADSHEET_SUFFIXES = {".xlsx", ".xls", ".xlsm", ".csv", ".ods"}
_EMAIL_SUFFIXES = {".eml", ".mbox"}
_JSON_SUFFIXES = {".json", ".jsonl"}

_LOADERS: dict = {}  # populated lazily to keep imports fast


def _get_spreadsheet_loader() -> SpreadsheetLoader:
    if "spreadsheet" not in _LOADERS:
        _LOADERS["spreadsheet"] = SpreadsheetLoader()
    return _LOADERS["spreadsheet"]


def _get_email_loader() -> EmailLoader:
    if "email" not in _LOADERS:
        _LOADERS["email"] = EmailLoader()
    return _LOADERS["email"]


def _get_json_loader() -> JSONLoader:
    if "json" not in _LOADERS:
        _LOADERS["json"] = JSONLoader()
    return _LOADERS["json"]


def _compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _detect_source_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in _VIDEO_SUFFIXES:
        return "video"
    if suffix in _AUDIO_SUFFIXES:
        return "audio"
    if suffix in _IMAGE_SUFFIXES:
        return "image"
    if suffix in _SPREADSHEET_SUFFIXES:
        return "spreadsheet"
    if suffix in _EMAIL_SUFFIXES:
        return "email"
    if suffix in _JSON_SUFFIXES:
        return "json"
    if suffix in _DOCUMENT_SUFFIXES:
        return "document"
    return "unknown"


class IngestResult:
    __slots__ = ("artifact_id", "sha256", "storage_uri", "source_type", "artifact")

    def __init__(self, artifact_id: str, sha256: str, storage_uri: str, source_type: str, artifact):
        self.artifact_id = artifact_id
        self.sha256 = sha256
        self.storage_uri = storage_uri
        self.source_type = source_type
        self.artifact = artifact


class FileIngestor:
    """
    Ingest local files into the Harvest artifact store.

    Usage:
        ingestor = FileIngestor(chain_writer, storage_root="storage")
        result = await ingestor.ingest(
            path=Path("report.pdf"),
            run_id="run-001",
            rights_profile=default_rights_for(SourceClass.OWNED_INTERNAL),
        )
    """

    def __init__(self, chain_writer: ChainWriter, storage_root: str = "storage"):
        self.chain_writer = chain_writer
        self.storage_root = Path(storage_root)

    async def ingest(
        self,
        path: Path,
        run_id: str,
        rights_profile: Optional[RightsProfile] = None,
        owned_by: str = "system",
        title: Optional[str] = None,
    ) -> IngestResult:
        """
        Ingest a local file. Emits acquire.started and acquire.completed chain entries.
        On any error, emits acquire.failed and raises AcquisitionError (fail-closed).
        """
        path = Path(path)

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="acquire.started",
            machine="file_ingestor",
            data={"path": str(path), "source_type": "file"},
        ))

        try:
            if not path.exists():
                raise AcquisitionError(f"File not found: {path}")
            if not path.is_file():
                raise AcquisitionError(f"Path is not a file: {path}")

            source_type = _detect_source_type(path)
            sha256 = _compute_sha256(path)
            artifact_id = str(uuid4())
            rp = rights_profile or default_rights_for(SourceClass.PUBLIC_WEB)
            file_title = title or path.stem

            # Copy to artifact store (hash-addressed)
            dest_dir = self.storage_root / "artifacts" / artifact_id
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / path.name
            shutil.copy2(path, dest_path)
            storage_uri = f"local://{dest_path}"

            # Invoke format-specific loaders for structured formats.
            # record_count surfaces in the chain entry below.
            record_count: Optional[int] = None
            if source_type == "spreadsheet":
                docs = _get_spreadsheet_loader().load(path)
                record_count = sum(s.row_count for d in docs for s in d.sheets)
            elif source_type == "email":
                docs = _get_email_loader().load(path)
                record_count = len(docs)
            elif source_type == "json":
                docs = _get_json_loader().load(path)
                record_count = sum(d.record_count for d in docs)

            # Build typed artifact
            if source_type == "video":
                artifact = RawVideoAsset(
                    asset_id=artifact_id,
                    source_type="file",
                    title=file_title,
                    owned_by=owned_by,
                    training_eligibility=rp.training_eligibility.value,
                    storage_uri=storage_uri,
                    sha256=sha256,
                )
            elif source_type == "audio":
                artifact = RawAudioStream(
                    audio_id=artifact_id,
                    session_id=run_id,
                    sample_rate=0,   # populated by transcribe plane
                    channels=0,
                    storage_uri=storage_uri,
                    sha256=sha256,
                )
            else:
                # Documents, images, spreadsheets, emails, JSON use RawVideoAsset as a
                # generic raw container until format-specific artifact types are added.
                artifact = RawVideoAsset(
                    asset_id=artifact_id,
                    source_type=source_type,
                    title=file_title,
                    owned_by=owned_by,
                    training_eligibility=rp.training_eligibility.value,
                    storage_uri=storage_uri,
                    sha256=sha256,
                )

            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.completed",
                machine="file_ingestor",
                data={
                    "artifact_id": artifact_id,
                    "source_type": source_type,
                    "sha256": sha256,
                    "storage_uri": storage_uri,
                    "file_size_bytes": path.stat().st_size,
                    "rights_status": rp.review_status.value,
                    "training_eligibility": rp.training_eligibility.value,
                    "record_count": record_count,
                },
            ))

            return IngestResult(
                artifact_id=artifact_id,
                sha256=sha256,
                storage_uri=storage_uri,
                source_type=source_type,
                artifact=artifact,
            )

        except AcquisitionError:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.failed",
                machine="file_ingestor",
                data={"path": str(path), "error": "acquisition_error"},
            ))
            raise
        except Exception as e:
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="acquire.failed",
                machine="file_ingestor",
                data={"path": str(path), "error": str(e)},
            ))
            raise AcquisitionError(f"File ingest failed: {e}") from e
