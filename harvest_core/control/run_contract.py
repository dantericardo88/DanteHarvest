"""
RunContract — execution contract for a single Harvest run.

Every acquisition, observation, and distillation run starts with a RunContract.
It is the one-door entry point: no artifact may be written without a run_id
bound to a valid RunContract. Fail-closed: unknown run_id raises HarvestError.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from harvest_core.rights.rights_model import SourceClass


class RunMode(str, Enum):
    FOUNDER = "founder"      # Local-first, fast approvals, owner assertion allowed
    ENTERPRISE = "enterprise"  # Mandatory reviewers, RBAC, signed receipts


class RunContract(BaseModel):
    """
    Immutable contract for a single Harvest run.

    Created once at run start. Never modified after creation.
    All plane operations receive the run_id from this contract.
    """

    run_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Auto-generated UUID for this run",
    )
    project_id: str = Field(description="Project this run belongs to")
    source_class: SourceClass = Field(description="Default source class for artifacts in this run")
    initiated_by: str = Field(description="Identity of user or service that started this run")
    mode: RunMode = Field(default=RunMode.FOUNDER)
    remote_sync: bool = Field(
        default=False,
        description="If True, artifacts may be written to remote storage. Default local-first.",
    )
    config: Dict[str, Any] = Field(default_factory=dict)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    notes: Optional[str] = None

    model_config = {"frozen": True}  # immutable after creation

    def chain_file_path(self, storage_root: str = "storage") -> str:
        """Return the canonical path for this run's chain.jsonl file."""
        return f"{storage_root}/projects/{self.project_id}/runs/{self.run_id}/chain.jsonl"
