"""
AudioRecorder — capture and store audio evidence streams.

Records audio from microphone or ingests existing audio files.
Emits audio.started, audio.chunk_written, audio.completed chain entries.
Local-first: all audio stored locally.
Fail-closed: errors emit audio.failed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import HarvestError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


class AudioObservationError(HarvestError):
    pass


@dataclass
class AudioChunk:
    chunk_index: int
    storage_path: str
    duration_seconds: float
    sample_rate: int
    channels: int
    size_bytes: int


@dataclass
class AudioSession:
    session_id: str
    run_id: str
    start_time: float
    end_time: Optional[float]
    chunks: List[AudioChunk] = field(default_factory=list)
    storage_dir: str = ""
    sample_rate: int = 16000
    channels: int = 1

    @property
    def total_duration(self) -> float:
        return sum(c.duration_seconds for c in self.chunks)

    @property
    def chunk_count(self) -> int:
        return len(self.chunks)


class AudioRecorder:
    """
    Record audio evidence or ingest existing audio files.

    When PyAudio is available, can record live from microphone.
    Otherwise, ingests pre-recorded audio files and emits the same
    chain signals.

    Usage (file ingest):
        recorder = AudioRecorder(chain_writer, storage_root="storage")
        session = await recorder.ingest_file(
            audio_path=Path("recording.wav"),
            run_id="run-001",
        )
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        storage_root: str = "storage",
        sample_rate: int = 16000,
        channels: int = 1,
        chunk_seconds: float = 30.0,
    ):
        self.chain_writer = chain_writer
        self.storage_root = Path(storage_root)
        self.sample_rate = sample_rate
        self.channels = channels
        self.chunk_seconds = chunk_seconds

    async def ingest_file(
        self, audio_path: Path, run_id: str
    ) -> AudioSession:
        """Ingest an existing audio file as an evidence stream."""
        audio_path = Path(audio_path)
        if not audio_path.exists():
            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="audio.failed",
                machine="audio_recorder",
                data={"error": f"File not found: {audio_path}"},
            ))
            raise AudioObservationError(f"Audio file not found: {audio_path}")

        session_id = str(uuid4())
        storage_dir = self.storage_root / "audio" / session_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        session = AudioSession(
            session_id=session_id,
            run_id=run_id,
            start_time=time.time(),
            end_time=None,
            storage_dir=str(storage_dir),
            sample_rate=self.sample_rate,
            channels=self.channels,
        )

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="audio.started",
            machine="audio_recorder",
            data={"session_id": session_id, "source_file": str(audio_path)},
        ))

        # Copy the source file as chunk 0
        import shutil
        dest = storage_dir / audio_path.name
        shutil.copy2(audio_path, dest)
        file_size = dest.stat().st_size
        duration = self._estimate_duration(dest)

        chunk = AudioChunk(
            chunk_index=0,
            storage_path=str(dest),
            duration_seconds=duration,
            sample_rate=self.sample_rate,
            channels=self.channels,
            size_bytes=file_size,
        )
        session.chunks.append(chunk)

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="audio.chunk_written",
            machine="audio_recorder",
            data={
                "session_id": session_id,
                "chunk_index": 0,
                "size_bytes": file_size,
                "duration_seconds": duration,
            },
        ))

        session.end_time = time.time()
        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="audio.completed",
            machine="audio_recorder",
            data={
                "session_id": session_id,
                "chunk_count": session.chunk_count,
                "total_duration": session.total_duration,
            },
        ))

        return session

    def _estimate_duration(self, path: Path) -> float:
        """Estimate duration from file size (fallback: 1 second per 32KB)."""
        try:
            import wave
            with wave.open(str(path), "rb") as wf:
                return wf.getnframes() / wf.getframerate()
        except Exception:
            size_kb = path.stat().st_size / 1024
            return size_kb / 32.0  # rough estimate
