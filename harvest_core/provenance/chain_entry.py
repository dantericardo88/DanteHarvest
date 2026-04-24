"""
ChainEntry — single entry in the append-only evidence chain (chain.jsonl).

Transplanted from DanteDistillerV2/backend/models/chain_entry.py.
Import paths updated for DANTEHARVEST package layout.
"""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ChainEntry(BaseModel):
    """
    A single record in the evidence chain.

    The chain is append-only. Each entry captures a signal emitted by a
    Harvest plane during execution, providing full auditability and
    deterministic replay capability.
    """

    run_id: str = Field(description="Unique identifier for the run that emitted this signal")

    signal: str = Field(
        description="Signal name in format: {plane}.{action} (e.g., 'acquire.started')",
        pattern=r"^[a-z0-9_]+\.[a-z0-9_]+$",
    )

    machine: str = Field(description="Name of the Harvest plane or machine that emitted this signal")

    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when signal was emitted",
    )

    data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Signal payload containing plane-specific data",
    )

    sequence: Optional[int] = Field(
        default=None,
        description="Sequence number within the run (set by ChainWriter)",
    )

    content_hash: Optional[str] = Field(
        default=None,
        description="SHA-256 hash of entry content for integrity verification",
    )

    @field_validator("signal")
    @classmethod
    def validate_signal_format(cls, v: str) -> str:
        if "." not in v:
            raise ValueError("Signal must be in format 'plane.action'")
        plane, action = v.split(".", 1)
        if not plane or not action:
            raise ValueError("Both plane and action must be non-empty")
        return v

    def to_jsonl_line(self) -> str:
        return self.model_dump_json(exclude_none=True)

    @classmethod
    def from_jsonl_line(cls, line: str) -> "ChainEntry":
        return cls.model_validate_json(line)

    def compute_hash(self) -> str:
        """SHA-256 of the entry content, excluding the hash and sequence fields."""
        data_for_hash = self.model_dump(exclude={"content_hash", "sequence"}, mode="json")
        canonical = json.dumps(data_for_hash, sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "run_id": "harvest-run-001",
                "signal": "acquire.started",
                "machine": "acquire",
                "timestamp": "2026-04-21T10:00:00Z",
                "data": {"source_type": "url", "url": "https://example.com"},
                "sequence": 1,
            }
        }
    )
