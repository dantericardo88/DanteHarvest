"""
BaseConnector — abstract base for all Harvest source connectors.

All connectors implement ingest() → List[str] (list of artifact IDs written).
Constitutional guarantees: fail-closed on auth errors, local-first storage.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional


class ConnectorError(Exception):
    pass


class BaseConnector(ABC):
    """
    Base class for Harvest source connectors.

    Subclasses must implement ingest() and return a list of artifact IDs.
    All connectors write artifacts to storage_root/connectors/<connector_name>/.
    """

    connector_name: str = "base"

    def __init__(self, storage_root: str = "storage"):
        self.storage_root = Path(storage_root)
        self._artifact_dir = self.storage_root / "connectors" / self.connector_name
        self._artifact_dir.mkdir(parents=True, exist_ok=True)

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
