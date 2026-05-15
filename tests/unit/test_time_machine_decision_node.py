import json
from pathlib import Path

from harvest_core.time_machine_decision_node import record_decision_node, resolve_decision_store


def test_records_product_local_decision_node(tmp_path: Path) -> None:
    node = record_decision_node(
        project_root=tmp_path,
        session_id="run-a",
        actor_id="harvest-cli",
        prompt="ingest:file",
        result={"artifact_id": "a1"},
        success=True,
    )

    store = resolve_decision_store(tmp_path)
    line = store.read_text(encoding="utf-8").strip()
    parsed = json.loads(line)

    assert parsed["id"] == node["id"]
    assert parsed["actor"]["product"] == "danteharvest"
    assert parsed["sessionId"] == "run-a"
    assert parsed["timelineId"] == "main"


def test_honors_danteforge_replay_env_and_hash_chains(tmp_path: Path, monkeypatch) -> None:
    store = tmp_path / "shared" / "nodes.jsonl"
    monkeypatch.setenv("DANTEFORGE_DECISION_STORE", str(store))
    monkeypatch.setenv("DANTEFORGE_DECISION_SESSION_ID", "session-replay")
    monkeypatch.setenv("DANTEFORGE_DECISION_TIMELINE_ID", "timeline-1")

    first = record_decision_node(
        project_root=tmp_path,
        session_id="ignored",
        actor_id="harvest-cli",
        prompt="crawl:url",
        result={"pages": 2},
        success=True,
    )
    second = record_decision_node(
        project_root=tmp_path,
        session_id="ignored",
        actor_id="harvest-cli",
        prompt="pack:export",
        result={"pack_id": "p1"},
        success=True,
    )

    assert first["sessionId"] == "session-replay"
    assert first["timelineId"] == "timeline-1"
    assert second["parentId"] == first["id"]
    assert second["prevHash"] == first["hash"]
