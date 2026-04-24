"""Unit tests for FileIngestor."""

import pytest
from pathlib import Path

from harvest_core.control.exceptions import AcquisitionError
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.rights.rights_model import SourceClass, default_rights_for
from harvest_acquire.files.file_ingestor import FileIngestor, _detect_source_type


class TestDetectSourceType:
    def test_pdf_is_document(self):
        assert _detect_source_type(Path("report.pdf")) == "document"

    def test_mp4_is_video(self):
        assert _detect_source_type(Path("demo.mp4")) == "video"

    def test_mp3_is_audio(self):
        assert _detect_source_type(Path("recording.mp3")) == "audio"

    def test_png_is_image(self):
        assert _detect_source_type(Path("screenshot.png")) == "image"

    def test_unknown_extension(self):
        assert _detect_source_type(Path("data.xyz")) == "unknown"


class TestFileIngestor:
    def _make_ingestor(self, tmp_path) -> tuple[FileIngestor, ChainWriter]:
        chain_path = tmp_path / "chain.jsonl"
        writer = ChainWriter(chain_path, "run-001")
        ingestor = FileIngestor(writer, storage_root=str(tmp_path / "storage"))
        return ingestor, writer

    @pytest.mark.asyncio
    async def test_ingest_real_file_succeeds(self, tmp_path):
        # Create a real test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, Harvest!")

        ingestor, writer = self._make_ingestor(tmp_path)
        result = await ingestor.ingest(
            path=test_file,
            run_id="run-001",
            rights_profile=default_rights_for(SourceClass.OWNED_INTERNAL),
        )

        assert result.artifact_id
        assert len(result.sha256) == 64
        assert result.source_type == "document"
        assert result.storage_uri.startswith("local://")

    @pytest.mark.asyncio
    async def test_ingest_emits_acquire_started_and_completed(self, tmp_path):
        test_file = tmp_path / "test.pdf"
        test_file.write_bytes(b"%PDF-1.4 test content")

        ingestor, writer = self._make_ingestor(tmp_path)
        await ingestor.ingest(path=test_file, run_id="run-001")

        entries = writer.read_all()
        signals = [e.signal for e in entries]
        assert "acquire.started" in signals
        assert "acquire.completed" in signals
        assert "acquire.failed" not in signals

    @pytest.mark.asyncio
    async def test_missing_file_emits_acquire_failed(self, tmp_path):
        ingestor, writer = self._make_ingestor(tmp_path)

        with pytest.raises(AcquisitionError):
            await ingestor.ingest(
                path=tmp_path / "nonexistent.pdf",
                run_id="run-001",
            )

        entries = writer.read_all()
        signals = [e.signal for e in entries]
        assert "acquire.started" in signals
        assert "acquire.failed" in signals
        assert "acquire.completed" not in signals

    @pytest.mark.asyncio
    async def test_sha256_matches_file_content(self, tmp_path):
        import hashlib
        test_file = tmp_path / "data.txt"
        content = b"deterministic content for sha256 check"
        test_file.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()

        ingestor, writer = self._make_ingestor(tmp_path)
        result = await ingestor.ingest(path=test_file, run_id="run-001")

        assert result.sha256 == expected

    @pytest.mark.asyncio
    async def test_rights_profile_recorded_in_chain(self, tmp_path):
        test_file = tmp_path / "internal.txt"
        test_file.write_text("internal content")
        profile = default_rights_for(SourceClass.OWNED_INTERNAL)

        ingestor, writer = self._make_ingestor(tmp_path)
        await ingestor.ingest(path=test_file, run_id="run-001", rights_profile=profile)

        completed = next(e for e in writer.read_all() if e.signal == "acquire.completed")
        assert completed.data["training_eligibility"] == "allowed"
