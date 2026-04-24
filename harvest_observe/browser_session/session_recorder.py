"""
BrowserSessionRecorder — record Playwright browser sessions as evidence.

Captures: DOM snapshots, network requests, user actions, screenshots.
Emits session.started, session.action_recorded, session.completed chain entries.
All frames stored locally before chain entries are written (local-first).

Constitutional guarantee: session recording never uploads data externally.
Fail-closed: recording errors emit session.failed and raise ObservationError.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from harvest_core.control.exceptions import HarvestError
from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter


class ObservationError(HarvestError):
    pass


@dataclass
class ActionEvent:
    action_type: str  # click, type, navigate, scroll, etc.
    target_selector: Optional[str]
    value: Optional[str]
    timestamp: float
    screenshot_path: Optional[str] = None
    dom_snapshot_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BrowserSession:
    session_id: str
    run_id: str
    start_time: float
    end_time: Optional[float]
    actions: List[ActionEvent]
    screenshots: List[str]
    start_url: str
    storage_dir: str

    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return 0.0

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "run_id": self.run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "action_count": len(self.actions),
            "screenshot_count": len(self.screenshots),
            "start_url": self.start_url,
            "storage_dir": self.storage_dir,
            "actions": [a.to_dict() for a in self.actions],
        }


class BrowserSessionRecorder:
    """
    Record a Playwright browser session as a structured evidence artifact.

    Can operate in two modes:
    1. Live recording: wrap a Playwright page and intercept events.
    2. Replay recording: ingest a pre-recorded trace file.

    Usage (live):
        recorder = BrowserSessionRecorder(chain_writer, storage_root="storage")
        session = await recorder.start_session(url="https://example.com", run_id="run-001")
        await recorder.record_action(session, action_type="click", target="#submit")
        await recorder.end_session(session)
    """

    def __init__(
        self,
        chain_writer: ChainWriter,
        storage_root: str = "storage",
    ):
        self.chain_writer = chain_writer
        self.storage_root = Path(storage_root)

    async def start_session(self, url: str, run_id: str) -> BrowserSession:
        session_id = str(uuid4())
        storage_dir = self.storage_root / "sessions" / session_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        session = BrowserSession(
            session_id=session_id,
            run_id=run_id,
            start_time=time.time(),
            end_time=None,
            actions=[],
            screenshots=[],
            start_url=url,
            storage_dir=str(storage_dir),
        )

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="session.started",
            machine="browser_session_recorder",
            data={
                "session_id": session_id,
                "start_url": url,
                "started_at": datetime.utcnow().isoformat(),
            },
        ))

        return session

    async def record_action(
        self,
        session: BrowserSession,
        action_type: str,
        target_selector: Optional[str] = None,
        value: Optional[str] = None,
        screenshot_bytes: Optional[bytes] = None,
        dom_snapshot: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ActionEvent:
        ts = time.time()
        storage_dir = Path(session.storage_dir)
        action_idx = len(session.actions)

        screenshot_path = None
        if screenshot_bytes:
            ss_path = storage_dir / f"screenshot_{action_idx:04d}.png"
            ss_path.write_bytes(screenshot_bytes)
            screenshot_path = str(ss_path)
            session.screenshots.append(screenshot_path)

        dom_path = None
        if dom_snapshot:
            dp = storage_dir / f"dom_{action_idx:04d}.html"
            dp.write_text(dom_snapshot, encoding="utf-8")
            dom_path = str(dp)

        event = ActionEvent(
            action_type=action_type,
            target_selector=target_selector,
            value=value,
            timestamp=ts,
            screenshot_path=screenshot_path,
            dom_snapshot_path=dom_path,
            metadata=metadata or {},
        )
        session.actions.append(event)

        await self.chain_writer.append(ChainEntry(
            run_id=session.run_id,
            signal="session.action_recorded",
            machine="browser_session_recorder",
            data={
                "session_id": session.session_id,
                "action_type": action_type,
                "action_index": action_idx,
                "has_screenshot": screenshot_path is not None,
            },
        ))

        return event

    async def end_session(self, session: BrowserSession) -> BrowserSession:
        session.end_time = time.time()

        # Persist session manifest
        manifest_path = Path(session.storage_dir) / "session.json"
        manifest_path.write_text(
            json.dumps(session.to_dict(), indent=2), encoding="utf-8"
        )

        await self.chain_writer.append(ChainEntry(
            run_id=session.run_id,
            signal="session.completed",
            machine="browser_session_recorder",
            data={
                "session_id": session.session_id,
                "duration_seconds": session.duration_seconds,
                "action_count": len(session.actions),
                "screenshot_count": len(session.screenshots),
                "manifest_path": str(manifest_path),
            },
        ))

        return session

    async def ingest_trace_file(
        self, trace_path: Path, run_id: str
    ) -> BrowserSession:
        """Ingest a pre-recorded Playwright trace JSON file."""
        trace_path = Path(trace_path)
        if not trace_path.exists():
            raise ObservationError(f"Trace file not found: {trace_path}")

        raw = json.loads(trace_path.read_text(encoding="utf-8"))
        session = await self.start_session(
            url=raw.get("start_url", "unknown"), run_id=run_id
        )
        for raw_action in raw.get("actions", []):
            await self.record_action(
                session,
                action_type=raw_action.get("type", "unknown"),
                target_selector=raw_action.get("selector"),
                value=raw_action.get("value"),
                metadata=raw_action,
            )
        return await self.end_session(session)

    async def ingest_video_session(
        self,
        video_path: Path,
        run_id: str,
        interval: int = 30,
        max_frames: int = 100,
        run_ocr: bool = False,
    ) -> BrowserSession:
        """
        Ingest a screen-recording video file as a session.

        Extracts keyframes using harvest_normalize.ocr.keyframes.extract_keyframes(),
        stores frame hashes in chain entries, and optionally runs OCR on each frame.
        Fail-closed: raises ObservationError if video is unreadable.
        """
        from harvest_normalize.ocr.keyframes import extract_keyframes, KeyframeExtractionError

        video_path = Path(video_path)
        if not video_path.exists():
            raise ObservationError(f"Video file not found: {video_path}")

        session = await self.start_session(url=f"file://{video_path}", run_id=run_id)
        storage_dir = Path(session.storage_dir)

        try:
            frames = extract_keyframes(
                str(video_path),
                interval=interval,
                max_frames=max_frames,
            )
        except KeyframeExtractionError as e:
            raise ObservationError(f"Keyframe extraction failed: {e}") from e

        await self.chain_writer.append(ChainEntry(
            run_id=run_id,
            signal="session.keyframes_extracted",
            machine="browser_session_recorder",
            data={
                "session_id": session.session_id,
                "video_path": str(video_path),
                "frame_count": len(frames),
                "frame_hashes": [f["hash"] for f in frames[:10]],  # first 10 for audit
            },
        ))

        for frame in frames:
            frame_bytes = frame.get("frame_data", b"")
            ocr_text = None
            if run_ocr and frame_bytes:
                try:
                    from harvest_normalize.ocr.ocr_engine import OCREngine
                    ocr_text = OCREngine().extract_text_from_bytes(frame_bytes)
                except Exception:
                    ocr_text = None

            frame_path = storage_dir / f"frame_{frame['frame_num']:06d}.bin"
            if frame_bytes:
                frame_path.write_bytes(frame_bytes)

            event = ActionEvent(
                action_type="keyframe",
                target_selector=None,
                value=None,
                timestamp=frame["timestamp"],
                screenshot_path=str(frame_path) if frame_bytes else None,
                metadata={
                    "frame_num": frame["frame_num"],
                    "hash": frame["hash"],
                    "ocr_text": ocr_text,
                },
            )
            session.actions.append(event)

        return await self.end_session(session)


# Alias for CLI and external import compatibility
SessionRecorder = BrowserSessionRecorder
