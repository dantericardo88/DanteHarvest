"""
AgentRegistry — tracks which downstream DanteAgents are subscribed to which pack types.

In-memory by default, with optional JSON persistence.

Methods:
  register(agent_id, pack_types)   — subscribe an agent to one or more pack types
  route(pack)  -> List[agent_id]   — return all agents subscribed to a pack's type
  deregister(agent_id)             — remove an agent and all its subscriptions

Constitutional guarantees:
- Fail-closed: routing an unknown pack type returns [] (never raises).
- Idempotent: registering an agent twice merges subscriptions without duplication.
- Thread-safe: all mutations protected by threading.Lock (sync callers).
- Persistence optional: call save(path) / load(path) explicitly.

Usage:
    registry = AgentRegistry()
    registry.register("agent-alpha", ["workflowPack", "skillPack"])
    registry.register("agent-beta", ["evalPack"])

    agents = registry.route(handoff)      # returns ["agent-alpha"] for a workflowPack
    registry.deregister("agent-alpha")
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set

from harvest_distill.packs.dante_agents_contract import HarvestHandoff
from harvest_distill.packs.pack_schemas import PackType


# Valid pack type strings (from the canonical enum)
_VALID_PACK_TYPES: frozenset[str] = frozenset(p.value for p in PackType)


class AgentRegistryError(Exception):
    """Raised when a registry operation is invalid."""


class AgentRegistry:
    """
    In-memory registry mapping agent IDs to subscribed pack types.

    Args:
        persist_path: Optional path to a JSON file for persistence.
                      If provided, the registry auto-loads on construction
                      if the file exists.
    """

    def __init__(self, persist_path: Optional[Path] = None) -> None:
        self._lock = threading.Lock()
        # {agent_id: set of pack_type strings}
        self._subscriptions: Dict[str, Set[str]] = {}
        self._persist_path = persist_path
        if persist_path and Path(persist_path).exists():
            self.load(persist_path)

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def register(self, agent_id: str, pack_types: List[str]) -> None:
        """
        Subscribe an agent to one or more pack types.

        Args:
            agent_id:   Unique identifier for the downstream agent.
            pack_types: List of pack type strings (e.g. ["workflowPack", "skillPack"]).

        Raises:
            AgentRegistryError: if any pack_type is not a recognised canonical type.
        """
        if not agent_id or not agent_id.strip():
            raise AgentRegistryError("agent_id must be a non-empty string")

        invalid = [pt for pt in pack_types if pt not in _VALID_PACK_TYPES]
        if invalid:
            raise AgentRegistryError(
                f"Unknown pack type(s): {invalid}. "
                f"Valid types: {sorted(_VALID_PACK_TYPES)}"
            )

        with self._lock:
            existing = self._subscriptions.get(agent_id, set())
            existing.update(pack_types)
            self._subscriptions[agent_id] = existing

    def route(self, pack: HarvestHandoff) -> List[str]:
        """
        Return a sorted list of agent IDs subscribed to the pack's type.

        Args:
            pack: A HarvestHandoff whose pack_type determines routing.

        Returns:
            List of agent_ids (may be empty; never raises).
        """
        pack_type = pack.pack_type
        with self._lock:
            matched = [
                agent_id
                for agent_id, types in self._subscriptions.items()
                if pack_type in types
            ]
        return sorted(matched)

    def deregister(self, agent_id: str) -> bool:
        """
        Remove an agent and all its subscriptions.

        Args:
            agent_id: ID of the agent to remove.

        Returns:
            True if the agent was present and removed, False if it was not registered.
        """
        with self._lock:
            if agent_id in self._subscriptions:
                del self._subscriptions[agent_id]
                return True
            return False

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def list_agents(self) -> List[str]:
        """Return a sorted list of all registered agent IDs."""
        with self._lock:
            return sorted(self._subscriptions.keys())

    def subscriptions(self, agent_id: str) -> List[str]:
        """
        Return the pack types an agent is subscribed to.

        Returns [] if the agent is not registered.
        """
        with self._lock:
            return sorted(self._subscriptions.get(agent_id, set()))

    def agent_count(self) -> int:
        """Return the number of registered agents."""
        with self._lock:
            return len(self._subscriptions)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: Optional[Path] = None) -> Path:
        """
        Persist subscriptions to a JSON file.

        Args:
            path: Destination path.  Falls back to self._persist_path.

        Returns:
            The path written to.

        Raises:
            AgentRegistryError: if no path is configured.
        """
        dest = Path(path or self._persist_path or "")
        if not dest.name:
            raise AgentRegistryError(
                "No persist_path configured. Pass a path to save()."
            )
        dest.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            data = {k: sorted(v) for k, v in self._subscriptions.items()}
        dest.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return dest

    def load(self, path: Optional[Path] = None) -> None:
        """
        Load subscriptions from a JSON file (merges with existing).

        Args:
            path: Source path.  Falls back to self._persist_path.

        Raises:
            AgentRegistryError: if no path is configured or file is invalid.
        """
        src = Path(path or self._persist_path or "")
        if not src.name:
            raise AgentRegistryError(
                "No persist_path configured. Pass a path to load()."
            )
        try:
            raw = json.loads(src.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise AgentRegistryError(f"Failed to load registry from {src}: {exc}") from exc

        with self._lock:
            for agent_id, pack_types in raw.items():
                existing = self._subscriptions.get(agent_id, set())
                existing.update(pack_types)
                self._subscriptions[agent_id] = existing

    def clear(self) -> None:
        """Remove all registrations (useful for testing)."""
        with self._lock:
            self._subscriptions.clear()
