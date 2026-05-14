"""
Unit tests for AgentRegistry.

Fully in-memory, no network, no external dependencies.  CI-safe.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from harvest_distill.export.agent_registry import AgentRegistry, AgentRegistryError
from harvest_distill.packs.dante_agents_contract import HarvestHandoff
from harvest_distill.packs.pack_schemas import PackType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def registry() -> AgentRegistry:
    return AgentRegistry()


def make_handoff(pack_type: str = "workflowPack") -> HarvestHandoff:
    return HarvestHandoff(
        handoff_id="hh-001",
        pack_id="wf-001",
        pack_type=pack_type,
        domain="test",
        receipt_id="rcpt-001",
        confidence_score=0.90,
        exported_at="2026-01-01T00:00:00",
        pack_json={"title": "Test Pack"},
    )


# ---------------------------------------------------------------------------
# register()
# ---------------------------------------------------------------------------

class TestRegister:
    def test_register_single_agent_single_type(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        assert "agent-alpha" in registry.list_agents()

    def test_register_agent_with_multiple_types(self, registry):
        registry.register("agent-alpha", ["workflowPack", "skillPack"])
        subs = registry.subscriptions("agent-alpha")
        assert "workflowPack" in subs
        assert "skillPack" in subs

    def test_register_is_idempotent(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.register("agent-alpha", ["workflowPack"])
        subs = registry.subscriptions("agent-alpha")
        assert subs.count("workflowPack") == 1

    def test_register_merges_new_types(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.register("agent-alpha", ["skillPack"])
        subs = registry.subscriptions("agent-alpha")
        assert "workflowPack" in subs
        assert "skillPack" in subs

    def test_register_multiple_agents(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.register("agent-beta", ["evalPack"])
        assert "agent-alpha" in registry.list_agents()
        assert "agent-beta" in registry.list_agents()

    def test_register_invalid_pack_type_raises(self, registry):
        with pytest.raises(AgentRegistryError, match="Unknown pack type"):
            registry.register("agent-alpha", ["invalidType"])

    def test_register_empty_agent_id_raises(self, registry):
        with pytest.raises(AgentRegistryError):
            registry.register("", ["workflowPack"])

    def test_register_whitespace_agent_id_raises(self, registry):
        with pytest.raises(AgentRegistryError):
            registry.register("   ", ["workflowPack"])

    def test_register_all_valid_pack_types(self, registry):
        all_types = [pt.value for pt in PackType]
        registry.register("agent-all", all_types)
        subs = set(registry.subscriptions("agent-all"))
        assert subs == set(all_types)


# ---------------------------------------------------------------------------
# route()
# ---------------------------------------------------------------------------

class TestRoute:
    def test_route_returns_subscribed_agents(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.register("agent-beta", ["evalPack"])
        handoff = make_handoff("workflowPack")
        matched = registry.route(handoff)
        assert matched == ["agent-alpha"]

    def test_route_returns_multiple_agents(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.register("agent-beta", ["workflowPack"])
        handoff = make_handoff("workflowPack")
        matched = registry.route(handoff)
        assert "agent-alpha" in matched
        assert "agent-beta" in matched

    def test_route_returns_empty_for_unsubscribed_type(self, registry):
        registry.register("agent-alpha", ["skillPack"])
        handoff = make_handoff("workflowPack")
        assert registry.route(handoff) == []

    def test_route_returns_empty_for_no_agents(self, registry):
        handoff = make_handoff("workflowPack")
        assert registry.route(handoff) == []

    def test_route_returns_sorted_list(self, registry):
        registry.register("zebra-agent", ["workflowPack"])
        registry.register("alpha-agent", ["workflowPack"])
        registry.register("middle-agent", ["workflowPack"])
        handoff = make_handoff("workflowPack")
        matched = registry.route(handoff)
        assert matched == sorted(matched)

    def test_route_skill_pack(self, registry):
        registry.register("tool-user", ["skillPack"])
        handoff = make_handoff("skillPack")
        assert registry.route(handoff) == ["tool-user"]

    def test_route_eval_pack(self, registry):
        registry.register("evaluator", ["evalPack"])
        handoff = make_handoff("evalPack")
        assert registry.route(handoff) == ["evaluator"]

    def test_route_specialization_pack(self, registry):
        registry.register("specialist", ["specializationPack"])
        handoff = make_handoff("specializationPack")
        assert registry.route(handoff) == ["specialist"]

    def test_route_does_not_raise_for_unknown_pack_type(self, registry):
        """route() should be fail-open: unknown type → empty list."""
        handoff = make_handoff("workflowPack")
        object.__setattr__(handoff, "pack_type", "unknownType")
        result = registry.route(handoff)
        assert result == []


# ---------------------------------------------------------------------------
# deregister()
# ---------------------------------------------------------------------------

class TestDeregister:
    def test_deregister_removes_agent(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        result = registry.deregister("agent-alpha")
        assert result is True
        assert "agent-alpha" not in registry.list_agents()

    def test_deregister_missing_agent_returns_false(self, registry):
        result = registry.deregister("nonexistent")
        assert result is False

    def test_deregister_removes_from_routing(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.deregister("agent-alpha")
        handoff = make_handoff("workflowPack")
        assert registry.route(handoff) == []

    def test_deregister_only_removes_target(self, registry):
        registry.register("agent-alpha", ["workflowPack"])
        registry.register("agent-beta", ["workflowPack"])
        registry.deregister("agent-alpha")
        handoff = make_handoff("workflowPack")
        assert registry.route(handoff) == ["agent-beta"]


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

class TestQueryHelpers:
    def test_list_agents_returns_sorted(self, registry):
        registry.register("zebra", ["workflowPack"])
        registry.register("alpha", ["workflowPack"])
        agents = registry.list_agents()
        assert agents == sorted(agents)

    def test_subscriptions_returns_empty_for_missing_agent(self, registry):
        assert registry.subscriptions("nonexistent") == []

    def test_agent_count_reflects_registrations(self, registry):
        assert registry.agent_count() == 0
        registry.register("a1", ["workflowPack"])
        assert registry.agent_count() == 1
        registry.register("a2", ["skillPack"])
        assert registry.agent_count() == 2
        registry.deregister("a1")
        assert registry.agent_count() == 1

    def test_clear_removes_all_agents(self, registry):
        registry.register("a1", ["workflowPack"])
        registry.register("a2", ["skillPack"])
        registry.clear()
        assert registry.agent_count() == 0
        assert registry.list_agents() == []


# ---------------------------------------------------------------------------
# JSON persistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_save_creates_json_file(self, tmp_path, registry):
        registry.register("agent-alpha", ["workflowPack"])
        path = tmp_path / "registry.json"
        result = registry.save(path)
        assert result == path
        assert path.exists()

    def test_save_and_load_roundtrip(self, tmp_path, registry):
        registry.register("agent-alpha", ["workflowPack", "skillPack"])
        registry.register("agent-beta", ["evalPack"])
        path = tmp_path / "registry.json"
        registry.save(path)

        new_registry = AgentRegistry()
        new_registry.load(path)
        assert "agent-alpha" in new_registry.list_agents()
        assert "agent-beta" in new_registry.list_agents()
        assert "workflowPack" in new_registry.subscriptions("agent-alpha")

    def test_load_merges_with_existing(self, tmp_path, registry):
        path = tmp_path / "registry.json"
        path.write_text(
            json.dumps({"agent-alpha": ["workflowPack"]}), encoding="utf-8"
        )
        registry.register("agent-beta", ["evalPack"])
        registry.load(path)
        assert "agent-alpha" in registry.list_agents()
        assert "agent-beta" in registry.list_agents()

    def test_auto_load_on_init(self, tmp_path):
        path = tmp_path / "registry.json"
        path.write_text(
            json.dumps({"agent-alpha": ["skillPack"]}), encoding="utf-8"
        )
        r = AgentRegistry(persist_path=path)
        assert "agent-alpha" in r.list_agents()

    def test_save_no_path_raises(self, registry):
        with pytest.raises(AgentRegistryError, match="No persist_path"):
            registry.save()

    def test_load_no_path_raises(self, registry):
        with pytest.raises(AgentRegistryError, match="No persist_path"):
            registry.load()

    def test_load_invalid_json_raises(self, tmp_path, registry):
        path = tmp_path / "bad.json"
        path.write_text("not valid json", encoding="utf-8")
        with pytest.raises(AgentRegistryError, match="Failed to load"):
            registry.load(path)

    def test_save_content_is_valid_json(self, tmp_path, registry):
        registry.register("agent-alpha", ["workflowPack"])
        path = tmp_path / "registry.json"
        registry.save(path)
        data = json.loads(path.read_text())
        assert isinstance(data, dict)
        assert "agent-alpha" in data
        assert "workflowPack" in data["agent-alpha"]


# ---------------------------------------------------------------------------
# Thread safety (basic smoke test)
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_registrations(self):
        registry = AgentRegistry()
        errors = []

        def worker(agent_id: str):
            try:
                registry.register(agent_id, ["workflowPack"])
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(f"agent-{i:03d}",))
            for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert registry.agent_count() == 20

    def test_concurrent_route_and_register(self):
        registry = AgentRegistry()
        registry.register("agent-base", ["workflowPack"])
        errors = []
        handoff = make_handoff("workflowPack")

        def register_worker(i):
            try:
                registry.register(f"agent-{i}", ["workflowPack"])
            except Exception as exc:
                errors.append(exc)

        def route_worker():
            try:
                registry.route(handoff)
            except Exception as exc:
                errors.append(exc)

        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=register_worker, args=(i,)))
            threads.append(threading.Thread(target=route_worker))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
