"""Unit tests for RunContract and RunRegistry."""

import pytest

from harvest_core.control.run_contract import RunContract, RunMode
from harvest_core.control.run_registry import RunRegistry, RunStatus
from harvest_core.control.exceptions import HarvestError
from harvest_core.rights.rights_model import SourceClass


def make_contract(**kwargs) -> RunContract:
    defaults = dict(
        project_id="proj-001",
        source_class=SourceClass.OWNED_INTERNAL,
        initiated_by="test@example.com",
    )
    defaults.update(kwargs)
    return RunContract(**defaults)


class TestRunContract:
    def test_auto_generates_run_id(self):
        c = make_contract()
        assert c.run_id
        assert len(c.run_id) == 36  # UUID4 format

    def test_run_ids_are_unique(self):
        c1, c2 = make_contract(), make_contract()
        assert c1.run_id != c2.run_id

    def test_defaults_to_founder_mode(self):
        c = make_contract()
        assert c.mode == RunMode.FOUNDER

    def test_defaults_remote_sync_false(self):
        c = make_contract()
        assert c.remote_sync is False

    def test_chain_file_path_includes_run_id(self):
        c = make_contract()
        path = c.chain_file_path()
        assert c.run_id in path
        assert c.project_id in path

    def test_is_immutable(self):
        c = make_contract()
        with pytest.raises(Exception):
            c.run_id = "hacked"


class TestRunRegistry:
    @pytest.mark.asyncio
    async def test_create_run_registers_and_emits_chain(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        contract = make_contract()
        record = await registry.create_run(contract)
        assert record.status == RunStatus.PENDING
        entries = record.chain_writer.read_all()
        assert any(e.signal == "run.created" for e in entries)

    @pytest.mark.asyncio
    async def test_get_run_returns_record(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        contract = make_contract()
        await registry.create_run(contract)
        record = registry.get_run(contract.run_id)
        assert record.contract.run_id == contract.run_id

    @pytest.mark.asyncio
    async def test_get_run_unknown_raises(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        with pytest.raises(HarvestError):
            registry.get_run("nonexistent-run-id")

    @pytest.mark.asyncio
    async def test_duplicate_create_raises(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        contract = make_contract()
        await registry.create_run(contract)
        with pytest.raises(HarvestError):
            await registry.create_run(contract)

    @pytest.mark.asyncio
    async def test_valid_state_transition_pending_to_running(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        contract = make_contract()
        await registry.create_run(contract)
        record = await registry.update_run_state(contract.run_id, RunStatus.RUNNING)
        assert record.status == RunStatus.RUNNING
        entries = record.chain_writer.read_all()
        assert any(e.signal == "run.running" for e in entries)

    @pytest.mark.asyncio
    async def test_invalid_transition_raises(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        contract = make_contract()
        await registry.create_run(contract)
        # Can't go PENDING → COMPLETED directly
        with pytest.raises(HarvestError):
            await registry.update_run_state(contract.run_id, RunStatus.COMPLETED)

    @pytest.mark.asyncio
    async def test_terminal_state_blocks_further_transitions(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        contract = make_contract()
        await registry.create_run(contract)
        await registry.update_run_state(contract.run_id, RunStatus.RUNNING)
        await registry.update_run_state(contract.run_id, RunStatus.COMPLETED)
        with pytest.raises(HarvestError):
            await registry.update_run_state(contract.run_id, RunStatus.RUNNING)

    @pytest.mark.asyncio
    async def test_list_runs_by_project(self, tmp_path):
        registry = RunRegistry(storage_root=str(tmp_path))
        c1 = make_contract(project_id="proj-A")
        c2 = make_contract(project_id="proj-B")
        await registry.create_run(c1)
        await registry.create_run(c2)
        assert len(registry.list_runs(project_id="proj-A")) == 1
        assert len(registry.list_runs()) == 2

    @pytest.mark.asyncio
    async def test_runs_persist_across_registry_instances(self, tmp_path):
        storage_root = str(tmp_path)
        contract = make_contract(project_id="proj-persist")

        registry1 = RunRegistry(storage_root=storage_root)
        await registry1.create_run(contract)
        await registry1.update_run_state(contract.run_id, RunStatus.RUNNING)

        registry2 = RunRegistry(storage_root=storage_root)
        record = registry2.get_run(contract.run_id)

        assert record.contract.project_id == "proj-persist"
        assert record.status == RunStatus.RUNNING
