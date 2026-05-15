"""
ObservationDaemon — 24/7 always-on background observation loop.

Wave 5d: observation_plane_depth — 24/7 always-on daemon mode (8→9).

Runs as a persistent background process that:
1. Captures screen frames at a configurable interval (via ContinuousCapturer)
2. Runs OCR on each new frame (via StreamingOCRProcessor / frame_ocr_pipeline)
3. Records desktop events (via EventCapture)
4. Emits health heartbeats to the evidence chain every N seconds
5. Writes a PID file so external monitors can verify the daemon is alive
6. Graceful shutdown on SIGINT/SIGTERM or stop() call

Design: asyncio event loop + background threads for CPU-bound work.

Constitutional guarantees:
- Fail-closed: component failures are logged and do NOT crash the daemon
- Local-first: all artifacts written to local disk, no network calls
- Append-only: all observations emitted as ChainEntry events
- 24/7: auto-restarts failed components (OCR, screen capture) without stopping
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Daemon config
# ---------------------------------------------------------------------------

@dataclass
class DaemonConfig:
    storage_root: str = "storage"
    capture_interval_s: float = 5.0         # screen capture every N seconds
    heartbeat_interval_s: float = 60.0      # chain heartbeat every N seconds
    ocr_enabled: bool = True
    event_capture_enabled: bool = True
    pid_file: Optional[str] = None          # write PID file at this path
    run_id: str = field(default_factory=lambda: f"daemon-{uuid4().hex[:8]}")
    max_frames_per_session: Optional[int] = None  # None = unlimited


# ---------------------------------------------------------------------------
# DaemonHealth
# ---------------------------------------------------------------------------

@dataclass
class DaemonHealth:
    started_at: float
    frames_captured: int = 0
    ocr_results: int = 0
    events_recorded: int = 0
    heartbeats_emitted: int = 0
    last_heartbeat_at: Optional[float] = None
    component_errors: Dict[str, int] = field(default_factory=dict)
    running: bool = True

    def record_error(self, component: str) -> None:
        self.component_errors[component] = self.component_errors.get(component, 0) + 1

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "uptime_s": time.time() - self.started_at,
            "frames_captured": self.frames_captured,
            "ocr_results": self.ocr_results,
            "events_recorded": self.events_recorded,
            "heartbeats_emitted": self.heartbeats_emitted,
            "last_heartbeat_at": self.last_heartbeat_at,
            "component_errors": dict(self.component_errors),
            "running": self.running,
        }


# ---------------------------------------------------------------------------
# ObservationDaemon
# ---------------------------------------------------------------------------

class ObservationDaemon:
    """
    Always-on observation daemon.

    Usage:
        config = DaemonConfig(capture_interval_s=5.0)
        daemon = ObservationDaemon(config, chain_writer=writer)
        asyncio.run(daemon.run())   # blocks until stop() or SIGTERM

    Non-blocking:
        task = asyncio.create_task(daemon.run())
        # ... later:
        daemon.stop()
        await task
    """

    def __init__(
        self,
        config: Optional[DaemonConfig] = None,
        chain_writer: Optional[Any] = None,
        on_frame: Optional[Callable[[Any], None]] = None,
        on_ocr: Optional[Callable[[Any], None]] = None,
    ):
        self._config = config or DaemonConfig()
        self._chain_writer = chain_writer
        self._on_frame = on_frame
        self._on_ocr = on_ocr
        self._health = DaemonHealth(started_at=time.time())
        self._stop_event = asyncio.Event()
        self._capturer: Optional[Any] = None
        self._event_capture: Optional[Any] = None

    async def run(self) -> None:
        """Main daemon loop. Runs until stop() is called or SIGTERM received."""
        self._install_signal_handlers()
        self._write_pid_file()

        logger.info("ObservationDaemon starting (run_id=%s)", self._config.run_id)

        await self._emit_chain("daemon.started", {
            "run_id": self._config.run_id,
            "capture_interval_s": self._config.capture_interval_s,
            "ocr_enabled": self._config.ocr_enabled,
        })

        tasks = [
            asyncio.create_task(self._capture_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]

        if self._config.event_capture_enabled:
            tasks.append(asyncio.create_task(self._event_loop()))

        try:
            await self._stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._health.running = False
            await self._emit_chain("daemon.stopped", self._health.to_dict())
            self._remove_pid_file()
            logger.info("ObservationDaemon stopped")

    def stop(self) -> None:
        """Signal the daemon to shut down gracefully."""
        self._stop_event.set()

    @property
    def health(self) -> DaemonHealth:
        return self._health

    # ------------------------------------------------------------------
    # Component loops
    # ------------------------------------------------------------------

    async def _capture_loop(self) -> None:
        """Screen capture loop — captures every config.capture_interval_s."""
        while not self._stop_event.is_set():
            try:
                frame_path = await asyncio.get_event_loop().run_in_executor(
                    None, self._capture_frame
                )
                if frame_path:
                    self._health.frames_captured += 1
                    if self._on_frame:
                        try:
                            self._on_frame(frame_path)
                        except Exception:
                            pass

                    if self._config.ocr_enabled:
                        asyncio.create_task(self._ocr_frame(frame_path))
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("capture_loop error: %s", e)
                self._health.record_error("capture")
            await asyncio.sleep(self._config.capture_interval_s)

    async def _heartbeat_loop(self) -> None:
        """Emit a health heartbeat to the chain every heartbeat_interval_s."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._config.heartbeat_interval_s)
                self._health.heartbeats_emitted += 1
                self._health.last_heartbeat_at = time.time()
                await self._emit_chain("daemon.heartbeat", self._health.to_dict())
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("heartbeat_loop error: %s", e)
                self._health.record_error("heartbeat")

    async def _event_loop(self) -> None:
        """Desktop event recording loop — polls EventCapture if available."""
        try:
            from harvest_observe.desktop.event_capture import EventCapture
            capture = EventCapture()
            capture.start()
            while not self._stop_event.is_set():
                try:
                    events = capture.flush_events() if hasattr(capture, "flush_events") else []
                    if events:
                        self._health.events_recorded += len(events)
                        await self._emit_chain("daemon.events", {
                            "count": len(events),
                            "types": [getattr(e, "event_type", "unknown") for e in events[:10]],
                        })
                except Exception as e:
                    logger.debug("event_loop flush error: %s", e)
                await asyncio.sleep(1.0)
            capture.stop()
        except asyncio.CancelledError:
            return
        except ImportError:
            logger.info("EventCapture not available — event recording disabled")
        except Exception as e:
            logger.warning("event_loop error: %s", e)
            self._health.record_error("event_capture")

    async def _ocr_frame(self, frame_path: str) -> None:
        """Run OCR on a captured frame asynchronously."""
        try:
            from harvest_normalize.ocr.ocr_engine import OCREngine
            engine = OCREngine()
            loop = asyncio.get_event_loop()
            text = await loop.run_in_executor(None, engine.extract_text, frame_path)
            if text.strip():
                self._health.ocr_results += 1
                if self._on_ocr:
                    try:
                        self._on_ocr(text)
                    except Exception:
                        pass
                await self._emit_chain("daemon.ocr_result", {
                    "frame_path": frame_path,
                    "text_length": len(text),
                    "preview": text[:100],
                })
        except Exception as e:
            logger.debug("OCR error for %s: %s", frame_path, e)
            self._health.record_error("ocr")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _capture_frame(self) -> Optional[str]:
        """Synchronous screen capture. Returns path to saved PNG or None."""
        try:
            from harvest_observe.capture.continuous_capturer import ContinuousCapturer
            storage = Path(self._config.storage_root) / "daemon_frames"
            storage.mkdir(parents=True, exist_ok=True)
            capturer = ContinuousCapturer(storage_root=str(storage))
            frame = capturer.capture_once()
            if frame and frame.storage_path:
                return frame.storage_path
        except Exception as e:
            logger.debug("capture_frame error: %s", e)
        return None

    async def _emit_chain(self, signal: str, data: dict) -> None:
        if self._chain_writer is None:
            return
        try:
            from harvest_core.provenance.chain_entry import ChainEntry
            entry = ChainEntry(
                run_id=self._config.run_id,
                signal=signal,
                machine="observation_daemon",
                data=data,
            )
            await self._chain_writer.append(entry)
        except Exception as e:
            logger.debug("chain emit error: %s", e)

    def _write_pid_file(self) -> None:
        pid_path = self._config.pid_file
        if not pid_path:
            pid_path = str(
                Path(self._config.storage_root) / "daemon" / f"{self._config.run_id}.pid"
            )
        try:
            Path(pid_path).parent.mkdir(parents=True, exist_ok=True)
            Path(pid_path).write_text(str(os.getpid()), encoding="utf-8")
            self._pid_path = pid_path
        except Exception as e:
            logger.warning("Could not write PID file: %s", e)
            self._pid_path = None

    def _remove_pid_file(self) -> None:
        if getattr(self, "_pid_path", None):
            try:
                Path(self._pid_path).unlink(missing_ok=True)
            except Exception:
                pass

    def _install_signal_handlers(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self.stop)
        except (NotImplementedError, AttributeError):
            # Windows: signal handlers not supported in asyncio event loops
            pass
