"""
Canonical artifact layer schemas from DANTEHARVEST PRD.

§Computer Apprenticeship Design — Canonical artifact layers.

Layers (in order): Raw → Captured → Derived → Promotion → Evidence
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# RAW layer
# ---------------------------------------------------------------------------

class RawScreenSession(BaseModel):
    session_id: str
    device_id: str
    captured_at: datetime
    fps: float
    monitor_layout: Dict[str, Any]
    source_class: str
    retention_class: str
    rights_profile_id: str
    storage_uri: str
    sha256: str


class RawBrowserTrace(BaseModel):
    trace_id: str
    session_id: str
    url: str
    started_at: datetime
    playwright_trace_uri: str
    network_log_uri: str
    cookies_policy: str
    sha256: str


class RawAudioStream(BaseModel):
    audio_id: str
    session_id: str
    sample_rate: int
    channels: int
    language_hint: Optional[str] = None
    storage_uri: str
    sha256: str


class RawVideoAsset(BaseModel):
    asset_id: str
    source_type: str
    title: str
    owned_by: str
    license_evidence_uri: Optional[str] = None
    training_eligibility: str
    storage_uri: str
    sha256: str


# ---------------------------------------------------------------------------
# CAPTURED layer
# ---------------------------------------------------------------------------

class ActionEvent(BaseModel):
    event_id: str
    session_id: str
    timestamp_ms: int
    kind: str  # HarvestEventKind value
    actor: str
    x: Optional[float] = None
    y: Optional[float] = None
    button: Optional[str] = None
    key: Optional[str] = None
    text: Optional[str] = None
    window_id: Optional[str] = None
    app_id: Optional[str] = None
    url: Optional[str] = None
    dom_target_id: Optional[str] = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class OCRBlock(BaseModel):
    text: str
    bbox: Optional[List[float]] = None  # [x, y, w, h]
    confidence: float = Field(ge=0.0, le=1.0)


class UIState(BaseModel):
    state_id: str
    session_id: str
    timestamp_ms: int
    window_tree: Optional[Dict[str, Any]] = None
    dom_snapshot_uri: Optional[str] = None
    ocr_blocks: List[OCRBlock] = Field(default_factory=list)
    focused_element: Optional[str] = None
    selection: Optional[str] = None
    screenshot_uri: Optional[str] = None
    hash: str


# ---------------------------------------------------------------------------
# DERIVED layer
# ---------------------------------------------------------------------------

class AlignedSegment(BaseModel):
    segment_id: str
    session_id: str
    start_ms: int
    end_ms: int
    transcript_text: Optional[str] = None
    speaker: Optional[str] = None
    ui_state_refs: List[str] = Field(default_factory=list)
    event_refs: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


class TaskSpan(BaseModel):
    task_id: str
    session_id: str
    goal_text: str
    start_ms: int
    end_ms: int
    preconditions: List[str] = Field(default_factory=list)
    postconditions: List[str] = Field(default_factory=list)
    evidence_refs: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


class ProcedureStep(BaseModel):
    step_id: str
    action: str
    branch_points: List[str] = Field(default_factory=list)
    required_inputs: List[str] = Field(default_factory=list)


class ProcedureGraph(BaseModel):
    procedure_id: str
    steps: List[ProcedureStep] = Field(default_factory=list)
    branch_points: List[str] = Field(default_factory=list)
    required_inputs: List[str] = Field(default_factory=list)
    success_conditions: List[str] = Field(default_factory=list)
    failure_modes: List[str] = Field(default_factory=list)
    supporting_evidence: List[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# PROMOTION layer
# ---------------------------------------------------------------------------

class CandidatePack(BaseModel):
    pack_id: str
    pack_type: str
    title: str
    version: str
    source_refs: List[str] = Field(default_factory=list)
    rights_status: str = "pending"
    eval_summary: Optional[Dict[str, Any]] = None
    promotion_status: str = "candidate"
