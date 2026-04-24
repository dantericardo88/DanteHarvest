"""
ScreenRecorder — capture desktop screen activity as evidence.

Records screen frames at configurable intervals.  Persists frames to
local storage before emitting chain entries (local-first).

Can ingest pre-recorded video files or capture from a screenshot source.
Emits screen.started, screen.frame_captured, screen.completed chain entries.
Fail-closed on capture errors.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import HarvestError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


class ScreenObservationError(HarvestError):
    pass


@dataclass
class ScreenFrame:
    frame_index: int
    storage_path: str
    captured_at: float
    width: int = 0
    height: int = 0
    size_bytes: int = 0
    sha256: Optional[str] = None


@dataclass
class ScreenSession:
    session_id: str
    run_id: str
    start_time: float
    end_time: Optional[float]
    frames: List[ScreenFrame] = field(default_factory=list)
    storage_dir: str = ""
    fps: float = 1.0

    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def frame_count(self) -> int:
        return len(self.frames)


class ScreenRecorder:
    """
    Capture screen frames as evidence artifacts.

    Primary use: ingest existing screenshots or video frame dumps.
    Secondary use: live capture via a screenshot_fn callback.

    Usage (ingest folder of screenshots):
        recorder = ScreenRecorder(chain_writer, storage_root="storage")
        session = await recorder.ingest_frame_directory(
            frame_dir=Path("screenshots/"),
            run_id="run-001",
        )

    Usage (live capture with callback):
        async def take_screenshot() -> bytes:
            ...
        session = await recorder.start_capture(
            screenshot_fn=take_screenshot, run_id="run-001", fps=2.0
        )
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        storage_root: str = "storage",
        fps: float = 1.0,
    ):
        self.chain_writer = chain_writer
        self.storage_root = Path(storage_root)
        self.fps = fps

    async def ingest_frame_directory(
        self,
        frame_dir: Path,
        run_id: str,
        extensions: tuple = (".png", ".jpg", ".jpeg"),
    ) -> ScreenSession:
        """Ingest all image frames from a directory."""
        frame_dir = Path(frame_dir)
        if not frame_dir.is_dir():
            raise ScreenObservationError(f"Frame directory not found: {frame_dir}")

        frames = sorted(
            [f for f in frame_dir.iterdir() if f.suffix.lower() in extensions]
        )

        session_id = str(uuid4())
        storage_dir = self.storage_root / "screen" / session_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        session = ScreenSession(
            session_id=session_id,
            run_id=run_id,
            start_time=time.time(),
            end_time=None,
            storage_dir=str(storage_dir),
            fps=self.fps,
        )

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="screen.started",
            machine="screen_recorder",
            data={"session_id": session_id, "frame_count": len(frames)},
        ))

        for idx, frame_path in enumerate(frames):
            import shutil
            import hashlib
            dest = storage_dir / f"frame_{idx:05d}{frame_path.suffix}"
            shutil.copy2(frame_path, dest)
            sha256 = hashlib.sha256(dest.read_bytes()).hexdigest()

            frame = ScreenFrame(
                frame_index=idx,
                storage_path=str(dest),
                captured_at=session.start_time + idx / max(self.fps, 0.001),
                size_bytes=dest.stat().st_size,
                sha256=sha256,
            )
            session.frames.append(frame)

            await self.chain_writer.append(ChainEntry(
                run_id=run_id,
                signal="screen.frame_captured",
                machine="screen_recorder",
                data={
                    "session_id": session_id,
                    "frame_index": idx,
                    "sha256": sha256,
                    "size_bytes": frame.size_bytes,
                },
            ))

        session.end_time = time.time()
        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="screen.completed",
            machine="screen_recorder",
            data={
                "session_id": session_id,
                "frame_count": session.frame_count,
                "duration_seconds": session.duration_seconds,
            },
        ))

        return session

    async def capture_frame(
        self,
        session: ScreenSession,
        screenshot_fn: Callable,
    ) -> ScreenFrame:
        """Capture a single frame using screenshot_fn() → bytes."""
        import hashlib
        data = await screenshot_fn()
        storage_dir = Path(session.storage_dir)
        idx = session.frame_count
        dest = storage_dir / f"frame_{idx:05d}.png"
        dest.write_bytes(data)
        sha256 = hashlib.sha256(data).hexdigest()

        frame = ScreenFrame(
            frame_index=idx,
            storage_path=str(dest),
            captured_at=time.time(),
            size_bytes=len(data),
            sha256=sha256,
        )
        session.frames.append(frame)

        await self.chain_writer.append(ChainEntry(
            run_id=session.run_id,
            signal="screen.frame_captured",
            machine="screen_recorder",
            data={
                "session_id": session.session_id,
                "frame_index": idx,
                "sha256": sha256,
            },
        ))

        return frame
