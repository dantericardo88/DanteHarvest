"""Tests for Harvest Reviewer Server — FastAPI reviewer API."""

import pytest
from unittest.mock import patch, MagicMock


def test_create_app_raises_without_fastapi():
    """Fail-closed: missing fastapi raises ImportError, not silent None."""
    import sys
    with patch.dict(sys.modules, {"fastapi": None}):
        # Re-import to trigger the import-time check
        import importlib
        import harvest_ui.reviewer.server as mod
        orig = mod._FASTAPI_AVAILABLE
        mod._FASTAPI_AVAILABLE = False
        try:
            with pytest.raises(ImportError, match="fastapi"):
                mod.create_app()
        finally:
            mod._FASTAPI_AVAILABLE = orig


def test_confidence_band_green():
    from harvest_ui.reviewer.server import _confidence_band
    assert _confidence_band(0.95) == "GREEN"
    assert _confidence_band(0.90) == "GREEN"


def test_confidence_band_yellow():
    from harvest_ui.reviewer.server import _confidence_band
    assert _confidence_band(0.89) == "YELLOW"
    assert _confidence_band(0.75) == "YELLOW"


def test_confidence_band_orange():
    from harvest_ui.reviewer.server import _confidence_band
    assert _confidence_band(0.74) == "ORANGE"
    assert _confidence_band(0.50) == "ORANGE"


def test_confidence_band_red():
    from harvest_ui.reviewer.server import _confidence_band
    assert _confidence_band(0.49) == "RED"
    assert _confidence_band(0.0) == "RED"


@pytest.mark.skipif(
    not __import__("importlib").util.find_spec("fastapi"),
    reason="fastapi not installed",
)
def test_app_health_endpoint(tmp_path):
    from fastapi.testclient import TestClient
    from harvest_ui.reviewer.server import create_app
    app = create_app(registry_root=str(tmp_path / "registry"), storage_root=str(tmp_path))
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.skipif(
    not __import__("importlib").util.find_spec("fastapi"),
    reason="fastapi not installed",
)
def test_chain_endpoint_missing_run(tmp_path):
    from fastapi.testclient import TestClient
    from harvest_ui.reviewer.server import create_app
    app = create_app(registry_root=str(tmp_path / "registry"), storage_root=str(tmp_path))
    client = TestClient(app)
    resp = client.get("/api/runs/nonexistent-run/chain")
    assert resp.status_code == 404
    assert "error" in resp.json()["detail"]


@pytest.mark.skipif(
    not __import__("importlib").util.find_spec("fastapi"),
    reason="fastapi not installed",
)
def test_chain_endpoint_with_entries(tmp_path):
    from fastapi.testclient import TestClient
    from harvest_ui.reviewer.server import create_app
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.provenance.chain_entry import ChainEntry
    import asyncio

    chain_dir = tmp_path / "chain"
    chain_dir.mkdir()
    writer = ChainWriter(chain_dir / "run-001.jsonl", "run-001")
    asyncio.run(writer.append(ChainEntry(
        run_id="run-001", signal="test.signal", machine="test", data={"key": "val"}
    )))

    app = create_app(registry_root=str(tmp_path / "registry"), storage_root=str(tmp_path))
    client = TestClient(app)
    resp = client.get("/api/runs/run-001/chain")
    assert resp.status_code == 200
    entries = resp.json()
    assert len(entries) == 1
    assert entries[0]["signal"] == "test.signal"


@pytest.mark.skipif(
    not __import__("importlib").util.find_spec("fastapi"),
    reason="fastapi not installed",
)
def test_approve_endpoint_promotes_pack(tmp_path):
    from fastapi.testclient import TestClient
    from harvest_ui.reviewer.server import create_app
    from harvest_index.registry.pack_registry import PackRegistry
    from harvest_distill.packs.pack_schemas import WorkflowPack, PackStep, EvalSummary

    registry = PackRegistry(root=str(tmp_path / "registry"))
    pack = WorkflowPack(
        pack_id="wf-approve",
        title="Approve Me",
        goal="test",
        steps=[PackStep(id="s1", action="click")],
        eval_summary=EvalSummary(replay_pass_rate=0.9, sample_size=2),
    )
    registry.register(pack)

    app = create_app(registry_root=str(tmp_path / "registry"), storage_root=str(tmp_path))
    client = TestClient(app)
    resp = client.post(
        "/api/packs/wf-approve/approve",
        json={"run_id": "run-approve", "receipt_id": "receipt-approve", "notes": "looks good"},
    )

    assert resp.status_code == 200
    assert resp.json()["status"] == "promoted"
    refreshed = PackRegistry(root=str(tmp_path / "registry"))
    assert refreshed.get("wf-approve").promotion_status == "promoted"
    assert refreshed.get("wf-approve").receipt_id == "receipt-approve"


@pytest.mark.skipif(
    not __import__("importlib").util.find_spec("fastapi"),
    reason="fastapi not installed",
)
def test_reject_endpoint_updates_registry_state(tmp_path):
    from fastapi.testclient import TestClient
    from harvest_ui.reviewer.server import create_app
    from harvest_index.registry.pack_registry import PackRegistry
    from harvest_distill.packs.pack_schemas import WorkflowPack, PackStep, EvalSummary

    registry = PackRegistry(root=str(tmp_path / "registry"))
    pack = WorkflowPack(
        pack_id="wf-reject",
        title="Reject Me",
        goal="test",
        steps=[PackStep(id="s1", action="click")],
        eval_summary=EvalSummary(replay_pass_rate=0.9, sample_size=2),
    )
    registry.register(pack, receipt_id="receipt-1")

    app = create_app(registry_root=str(tmp_path / "registry"), storage_root=str(tmp_path))
    client = TestClient(app)
    resp = client.post(
        "/api/packs/wf-reject/reject",
        json={"run_id": "run-reject", "reason": "needs work"},
    )

    assert resp.status_code == 200
    assert resp.json()["status"] == "rejected"
    refreshed = PackRegistry(root=str(tmp_path / "registry"))
    assert refreshed.get("wf-reject").promotion_status == "rejected"
