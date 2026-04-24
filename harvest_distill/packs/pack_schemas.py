"""
Pack schemas for DANTEHARVEST.

Defines the four canonical pack types from the PRD
(§Computer Apprenticeship Design — Pack schema proposals):

  workflowPack    — full multi-step process
  skillPack       — reusable atomic capability
  specializationPack — domain bundle for downstream agent
  evalPack        — reproducible benchmark/test case set

All packs are Pydantic v2 models. The EvalSummary is shared across types.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Shared primitives
# ---------------------------------------------------------------------------

class PackType(str, Enum):
    WORKFLOW = "workflowPack"
    SKILL = "skillPack"
    SPECIALIZATION = "specializationPack"
    EVAL = "evalPack"


class PromotionStatus(str, Enum):
    CANDIDATE = "candidate"
    PROMOTED = "promoted"
    REJECTED = "rejected"
    DEPRECATED = "deprecated"


class EvalSummary(BaseModel):
    replay_pass_rate: float = Field(ge=0.0, le=1.0)
    sample_size: int = Field(gt=0)
    eval_environment: Optional[str] = None
    notes: Optional[str] = None


class PackStep(BaseModel):
    id: str
    action: str
    evidence_refs: List[str] = Field(default_factory=list)
    selector_hint: Optional[str] = None
    expected_outcome: Optional[str] = None


# ---------------------------------------------------------------------------
# workflowPack
# ---------------------------------------------------------------------------

class WorkflowPack(BaseModel):
    pack_type: PackType = PackType.WORKFLOW
    pack_id: str
    title: str
    goal: str
    steps: List[PackStep] = Field(default_factory=list)
    tools: List[str] = Field(default_factory=list)
    preconditions: List[str] = Field(default_factory=list)
    success_checks: List[str] = Field(default_factory=list)
    failure_modes: List[str] = Field(default_factory=list)
    evidence_refs: List[str] = Field(default_factory=list)
    rights_status: str = "pending"
    eval_summary: Optional[EvalSummary] = None
    promotion_status: PromotionStatus = PromotionStatus.CANDIDATE
    version: str = "1.0.0"
    source_refs: List[str] = Field(default_factory=list)

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "packType": "workflowPack",
            "packId": "wf_invoice_reconciliation_v1",
            "title": "Invoice reconciliation in QuickBooks",
            "goal": "Match downloaded invoices to ledger entries and flag mismatches",
            "steps": [
                {"id": "s1", "action": "Open QuickBooks → Expenses", "evidenceRefs": ["seg_014"]},
                {"id": "s2", "action": "Import invoice CSV", "evidenceRefs": ["seg_018"]},
                {"id": "s3", "action": "Flag mismatches above threshold", "evidenceRefs": ["seg_022"]},
            ],
            "rightsStatus": "approved",
            "evalSummary": {"replayPassRate": 0.94, "sampleSize": 18},
            "version": "1.0.0",
        }
    })


# ---------------------------------------------------------------------------
# skillPack
# ---------------------------------------------------------------------------

class SkillPack(BaseModel):
    pack_type: PackType = PackType.SKILL
    pack_id: str
    skill_name: str
    trigger_context: str
    action_template: str
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    guardrails: List[str] = Field(default_factory=list)
    examples: List[Dict[str, Any]] = Field(default_factory=list)
    evidence_refs: List[str] = Field(default_factory=list)
    eval_summary: Optional[EvalSummary] = None
    rights_status: str = "pending"
    promotion_status: PromotionStatus = PromotionStatus.CANDIDATE
    version: str = "1.0.0"


# ---------------------------------------------------------------------------
# specializationPack
# ---------------------------------------------------------------------------

class SpecializationPack(BaseModel):
    pack_type: PackType = PackType.SPECIALIZATION
    pack_id: str
    domain: str
    knowledge_refs: List[str] = Field(default_factory=list)
    workflow_refs: List[str] = Field(default_factory=list)
    skill_refs: List[str] = Field(default_factory=list)
    glossary: Dict[str, str] = Field(default_factory=dict)
    taxonomy: Dict[str, Any] = Field(default_factory=dict)
    disallowed_actions: List[str] = Field(default_factory=list)
    rights_boundary: str = "pending"
    promotion_status: PromotionStatus = PromotionStatus.CANDIDATE
    version: str = "1.0.0"


# ---------------------------------------------------------------------------
# evalPack
# ---------------------------------------------------------------------------

class TaskCase(BaseModel):
    case_id: str
    description: str
    fixture: Optional[Dict[str, Any]] = None
    expected_outputs: List[Any] = Field(default_factory=list)
    oracle_rules: List[str] = Field(default_factory=list)


class EvalPack(BaseModel):
    pack_type: PackType = PackType.EVAL
    pack_id: str
    benchmark_name: str
    task_cases: List[TaskCase] = Field(default_factory=list)
    success_metrics: Dict[str, Any] = Field(default_factory=dict)
    replay_environment: Optional[str] = None
    promotion_status: PromotionStatus = PromotionStatus.CANDIDATE
    version: str = "1.0.0"


# ---------------------------------------------------------------------------
# Union helper
# ---------------------------------------------------------------------------

AnyPack = WorkflowPack | SkillPack | SpecializationPack | EvalPack
