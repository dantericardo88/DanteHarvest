"""
BaseConnector — abstract base for all Harvest source connectors.

All connectors implement ingest() → List[str] (list of artifact IDs written).
Constitutional guarantees: fail-closed on auth errors, local-first storage.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


class ConnectorError(Exception):
    pass


@dataclass
class ConnectorRecord:
    """Uniform record type returned by fetch()-style connectors."""

    record_id: str
    title: str
    content: str
    source_url: str
    source_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    fetched_at: float = 0.0


class BaseConnector(ABC):
    """
    Base class for Harvest source connectors.

    Subclasses must implement ingest() and return a list of artifact IDs.
    All connectors write artifacts to storage_root/connectors/<connector_name>/.
    """

    connector_name: str = "base"
    # Subclasses declare which env-var names grant credentials.
    # An empty list means no credentials are needed (always available).
    required_env_vars: List[str] = []

    def __init__(self, storage_root: str = "storage"):
        self.storage_root = Path(storage_root)
        self._artifact_dir = self.storage_root / "connectors" / self.connector_name
        self._artifact_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def try_connect(cls) -> "ConnectorStatus":
        """Check credential availability without connecting or raising.

        Returns a ConnectorStatus with available=True when at least one of
        the declared required_env_vars is set (or none are required).
        Safe to call in any environment — never raises, never does I/O.
        """
        # Import here to avoid circular-import at module load time.
        from harvest_acquire.connectors.connector_registry import ConnectorStatus  # noqa: PLC0415

        env_vars: List[str] = cls.required_env_vars  # type: ignore[assignment]
        if not env_vars:
            return ConnectorStatus(
                name=cls.connector_name,
                available=True,
                missing_env_vars=[],
                config_hint="No credentials required.",
            )
        present = [v for v in env_vars if os.environ.get(v)]
        missing = [v for v in env_vars if not os.environ.get(v)]
        available = len(present) > 0
        hint = (
            f"Set one of: {', '.join(env_vars)}"
            if not available
            else f"Credential found via {present[0]}."
        )
        return ConnectorStatus(
            name=cls.connector_name,
            available=available,
            missing_env_vars=missing if not available else [],
            config_hint=hint,
        )

    @abstractmethod
    def ingest(self, **kwargs: Any) -> List[str]:
        """Ingest content and return list of artifact IDs."""

    def _write_artifact(self, artifact_id: str, content: str, meta: Optional[Dict[str, Any]] = None) -> str:
        """Write content to disk. Returns artifact_id."""
        import json
        out_dir = self._artifact_dir / artifact_id
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "content.md").write_text(content, encoding="utf-8")
        if meta:
            (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return artifact_id
