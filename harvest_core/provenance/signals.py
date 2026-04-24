"""
Signal types and Harvest event vocabulary.

Transplanted from DanteDistillerV2/backend/models/signals.py and extended
with Harvest-specific event kinds from the PRD (Computer Apprenticeship Design).
"""

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Core lifecycle signals
# ---------------------------------------------------------------------------

class SignalType(str, Enum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    PROCESSING = "processing"
    PROGRESS = "progress"
    ACQUIRED = "acquired"
    PARSED = "parsed"
    VALIDATED = "validated"
    STORED = "stored"
    FETCHED = "fetched"
    CHUNKED = "chunked"
    EMBEDDED = "embedded"
    EXPORTED = "exported"


class MachineState(str, Enum):
    IDLE = "idle"
    INITIALIZING = "initializing"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"


class RunState(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Harvest canonical event vocabulary (PRD §Computer Apprenticeship Design)
# ---------------------------------------------------------------------------

class HarvestEventKind(str, Enum):
    # Session lifecycle
    SESSION_STARTED = "session.started"
    SESSION_PAUSED = "session.paused"
    SESSION_STOPPED = "session.stopped"

    # Window / app
    WINDOW_FOCUSED = "window.focused"
    WINDOW_CREATED = "window.created"
    WINDOW_CLOSED = "window.closed"
    APP_CHANGED = "app.changed"

    # Browser
    BROWSER_NAVIGATED = "browser.navigated"
    BROWSER_DOM_CHANGED = "browser.domChanged"
    BROWSER_NETWORK_REQUEST = "browser.networkRequest"
    BROWSER_NETWORK_RESPONSE = "browser.networkResponse"

    # Input
    MOUSE_MOVED = "mouse.moved"
    MOUSE_CLICKED = "mouse.clicked"
    MOUSE_DRAGGED = "mouse.dragged"
    KEYBOARD_KEY_PRESSED = "keyboard.keyPressed"
    KEYBOARD_SHORTCUT = "keyboard.shortcut"
    TEXT_INPUT = "text.input"
    SCROLL_PERFORMED = "scroll.performed"

    # Extraction
    OCR_EXTRACTED = "ocr.extracted"
    TRANSCRIPT_SEGMENTED = "transcript.segmented"
    UI_STATE_CAPTURED = "ui.stateCaptured"

    # Task / procedure
    TASK_SEGMENT_OPENED = "task.segmentOpened"
    TASK_SEGMENT_CLOSED = "task.segmentClosed"
    PROCEDURE_INFERRED = "procedure.inferred"

    # Replay
    REPLAY_STARTED = "replay.started"
    REPLAY_STEP_PASSED = "replay.stepPassed"
    REPLAY_STEP_FAILED = "replay.stepFailed"

    # Pack promotion
    PACK_PROMOTED = "pack.promoted"
    PACK_REJECTED = "pack.rejected"

    # Human review
    HUMAN_APPROVED = "human.approved"
    HUMAN_REDACTED = "human.redacted"

    # Rights
    RIGHTS_DENIED = "rights.denied"


# ---------------------------------------------------------------------------
# Signal data payloads
# ---------------------------------------------------------------------------

class SignalData(BaseModel):
    timestamp: Optional[str] = None
    message: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StartedSignalData(SignalData):
    input_keys: list[str] = Field(default_factory=list)
    config: Dict[str, Any] = Field(default_factory=dict)


class CompletedSignalData(SignalData):
    output_keys: list[str] = Field(default_factory=list)
    duration_seconds: Optional[float] = None
    items_processed: Optional[int] = None


class FailedSignalData(SignalData):
    error: str
    error_type: str
    input_keys: list[str] = Field(default_factory=list)
    stack_trace: Optional[str] = None


class ProgressSignalData(SignalData):
    current: int = Field(ge=0)
    total: int = Field(gt=0)
    percentage: Optional[float] = Field(default=None, ge=0, le=100)

    def model_post_init(self, __context: Any) -> None:
        if self.percentage is None and self.total > 0:
            self.percentage = (self.current / self.total) * 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_signal_name(machine_name: str, signal_type: SignalType) -> str:
    return f"{machine_name}.{signal_type.value}"


def parse_signal_name(signal_name: str) -> tuple[str, str]:
    if "." not in signal_name:
        raise ValueError(f"Invalid signal name format: {signal_name}")
    parts = signal_name.split(".", 1)
    return parts[0], parts[1]


def is_terminal_signal(signal_name: str) -> bool:
    _, signal_type = parse_signal_name(signal_name)
    return signal_type in (SignalType.COMPLETED.value, SignalType.FAILED.value)
