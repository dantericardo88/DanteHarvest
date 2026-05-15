"""DanteHarvest -> DanteForge DecisionNode recorder."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DecisionNode = dict[str, Any]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash_node(node_without_hash: DecisionNode) -> str:
    prev_hash = node_without_hash.get("prevHash") or ""
    return hashlib.sha256((prev_hash + _canonical_json(node_without_hash)).encode("utf-8")).hexdigest()


def resolve_decision_store(project_root: str | Path) -> Path:
    override = os.getenv("DANTEHARVEST_DECISION_STORE") or os.getenv("DANTEFORGE_DECISION_STORE")
    if override:
        return Path(override)
    return Path(project_root) / ".danteharvest" / "decision-nodes.jsonl"


def _read_nodes(store_path: Path) -> list[DecisionNode]:
    if not store_path.exists():
        return []
    nodes: list[DecisionNode] = []
    for line in store_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            nodes.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return nodes


def _resolve_parent(
    store_path: Path,
    session_id: str,
    timeline_id: str,
    parent_id: str | None,
) -> DecisionNode | None:
    nodes = _read_nodes(store_path)
    if parent_id:
        return next((node for node in nodes if node.get("id") == parent_id), None)
    for node in reversed(nodes):
        if node.get("sessionId") == session_id and node.get("timelineId") == timeline_id:
            return node
    return None


def record_decision_node(
    *,
    project_root: str | Path,
    session_id: str,
    actor_id: str,
    prompt: str,
    result: Any,
    success: bool,
    context: dict[str, Any] | None = None,
    cost_usd: float = 0.0,
    latency_ms: int = 0,
    timestamp: str | None = None,
    parent_id: str | None = None,
    evidence_ref: str | None = None,
) -> DecisionNode:
    store_path = resolve_decision_store(project_root)
    resolved_session_id = os.getenv("DANTEFORGE_DECISION_SESSION_ID") or session_id
    timeline_id = os.getenv("DANTEFORGE_DECISION_TIMELINE_ID") or "main"
    resolved_parent_id = parent_id if parent_id is not None else os.getenv("DANTEFORGE_DECISION_PARENT_ID")
    parent = _resolve_parent(store_path, resolved_session_id, timeline_id, resolved_parent_id)

    node_without_hash: DecisionNode = {
        "id": str(uuid.uuid4()),
        "parentId": parent.get("id") if parent else resolved_parent_id,
        "sessionId": resolved_session_id,
        "timelineId": timeline_id,
        "timestamp": timestamp or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "actor": {
            "type": "agent",
            "id": actor_id,
            "product": "danteharvest",
        },
        "input": {
            "prompt": prompt,
            **({"context": context} if context is not None else {}),
        },
        "output": {
            "result": result,
            "success": success,
            "costUsd": cost_usd,
            "latencyMs": latency_ms,
        },
        "prevHash": parent.get("hash") if parent else None,
        **({"evidenceRef": evidence_ref} if evidence_ref is not None else {}),
    }
    node = {**node_without_hash, "hash": _hash_node(node_without_hash)}

    store_path.parent.mkdir(parents=True, exist_ok=True)
    with store_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(node, ensure_ascii=False) + "\n")
    return node


def try_record_decision_node(**kwargs: Any) -> DecisionNode | None:
    try:
        return record_decision_node(**kwargs)
    except Exception:
        return None
