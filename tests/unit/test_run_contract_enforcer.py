"""Unit tests for harvest_core.constitution.run_contract_enforcer."""

import asyncio
from pathlib import Path

import pytest

from harvest_core.constitution.run_contract_enforcer import (
    RunContractEnforcer,
    ConstitutionViolationError,
)
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.provenance.chain_entry import ChainEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_entry_dict() -> dict:
    return {"run_id": "run-test-001", "signal": "acquire.started"}


def _make_chain_entry(run_id: str = "run-test-001") -> ChainEntry:
    return ChainEntry(
        run_id=run_id,
        signal="acquire.started",
        machine="acquire",
    )


# ---------------------------------------------------------------------------
# validate_write — source_url
# ---------------------------------------------------------------------------

class TestValidateWriteSourceUrl:
    def test_validate_write_raises_on_empty_source(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-001",
                source_url="",
                rights={"license": "MIT"},
            )
        assert exc_info.value.rule == "source_not_empty"

    def test_validate_write_raises_on_whitespace_source(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-001",
                source_url="   ",
                rights={"license": "MIT"},
            )
        assert exc_info.value.rule == "source_not_empty"

    def test_validate_write_raises_on_none_source(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-001",
                source_url=None,
                rights={"license": "MIT"},
            )
        assert exc_info.value.rule == "source_not_empty"


# ---------------------------------------------------------------------------
# validate_write — license / rights
# ---------------------------------------------------------------------------

class TestValidateWriteLicense:
    def test_validate_write_raises_on_missing_license(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-002",
                source_url="https://example.com/doc.pdf",
                rights={},
            )
        assert exc_info.value.rule == "license_required"

    def test_validate_write_raises_on_empty_license_value(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-002",
                source_url="https://example.com/doc.pdf",
                rights={"license": ""},
            )
        assert exc_info.value.rule == "license_required"

    def test_validate_write_raises_on_non_dict_rights(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-002",
                source_url="https://example.com/doc.pdf",
                rights="MIT",  # type: ignore[arg-type]
            )
        assert exc_info.value.rule == "license_required"


# ---------------------------------------------------------------------------
# validate_write — local_only
# ---------------------------------------------------------------------------

class TestValidateWriteLocalOnly:
    def test_local_only_with_remote_destination_raises(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_write(
                artifact_id="art-003",
                source_url="https://example.com/doc.pdf",
                rights={"license": "MIT"},
                local_only=True,
                destination="s3://my-bucket/art-003",
            )
        assert exc_info.value.rule == "local_only_respected"

    def test_local_only_without_destination_passes(self):
        enforcer = RunContractEnforcer()
        # Must not raise
        enforcer.validate_write(
            artifact_id="art-003",
            source_url="https://example.com/doc.pdf",
            rights={"license": "MIT"},
            local_only=True,
        )

    def test_not_local_only_with_destination_passes(self):
        enforcer = RunContractEnforcer()
        enforcer.validate_write(
            artifact_id="art-003",
            source_url="https://example.com/doc.pdf",
            rights={"license": "MIT"},
            local_only=False,
            destination="s3://my-bucket/art-003",
        )


# ---------------------------------------------------------------------------
# validate_write — happy path
# ---------------------------------------------------------------------------

class TestValidateWritePasses:
    def test_validate_write_passes_on_valid_entry(self):
        enforcer = RunContractEnforcer()
        # Must not raise
        enforcer.validate_write(
            artifact_id="art-ok",
            source_url="https://example.com/document.pdf",
            rights={"license": "Apache-2.0", "owner": "Acme Corp"},
        )

    def test_validate_write_passes_with_extra_rights_fields(self):
        enforcer = RunContractEnforcer()
        enforcer.validate_write(
            artifact_id="art-ok2",
            source_url="https://example.com/data.json",
            rights={
                "license": "CC-BY-4.0",
                "redistribution": "allowed",
                "training_eligible": True,
            },
        )


# ---------------------------------------------------------------------------
# validate_chain_entry
# ---------------------------------------------------------------------------

class TestValidateChainEntry:
    def test_valid_entry_dict_passes(self):
        enforcer = RunContractEnforcer()
        enforcer.validate_chain_entry(_valid_entry_dict())

    def test_missing_run_id_raises(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_chain_entry({"signal": "acquire.started"})
        assert exc_info.value.rule == "chain_entry_has_run_id"

    def test_empty_run_id_raises(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_chain_entry({"run_id": "", "signal": "acquire.started"})
        assert exc_info.value.rule == "chain_entry_has_run_id"

    def test_missing_signal_raises(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_chain_entry({"run_id": "run-001"})
        assert exc_info.value.rule == "chain_entry_has_signal"

    def test_empty_signal_raises(self):
        enforcer = RunContractEnforcer()
        with pytest.raises(ConstitutionViolationError) as exc_info:
            enforcer.validate_chain_entry({"run_id": "run-001", "signal": ""})
        assert exc_info.value.rule == "chain_entry_has_signal"

    def test_chain_entry_object_passes(self):
        enforcer = RunContractEnforcer()
        entry = _make_chain_entry()
        # validate_chain_entry accepts ChainEntry objects too (via getattr fallback)
        enforcer.validate_chain_entry(entry.model_dump())


# ---------------------------------------------------------------------------
# ChainWriter integration
# ---------------------------------------------------------------------------

class TestChainWriterUsesEnforcer:
    @pytest.mark.asyncio
    async def test_chain_writer_uses_enforcer(self, tmp_path: Path):
        """ChainWriter with enforcer calls validate_chain_entry on append."""
        enforcer = RunContractEnforcer()
        run_id = "run-enforcer-test"
        writer = ChainWriter(
            chain_file_path=tmp_path / "chain.jsonl",
            run_id=run_id,
            enforcer=enforcer,
        )
        entry = _make_chain_entry(run_id=run_id)
        result = await writer.append(entry)
        assert result.sequence == 1

    @pytest.mark.asyncio
    async def test_enforcer_optional(self, tmp_path: Path):
        """ChainWriter without enforcer works fine — no regression."""
        run_id = "run-no-enforcer"
        writer = ChainWriter(
            chain_file_path=tmp_path / "chain.jsonl",
            run_id=run_id,
            # no enforcer=
        )
        entry = _make_chain_entry(run_id=run_id)
        result = await writer.append(entry)
        assert result.sequence == 1

    @pytest.mark.asyncio
    async def test_chain_writer_enforcer_blocks_bad_entry(self, tmp_path: Path):
        """
        Confirm the enforcer path is actually reached: if we subclass the
        enforcer to reject everything, append raises ConstitutionViolationError.
        """
        class AlwaysRejectEnforcer(RunContractEnforcer):
            def validate_chain_entry(self, entry):
                raise ConstitutionViolationError(
                    rule="test_reject", detail="always rejected"
                )

        run_id = "run-reject-test"
        writer = ChainWriter(
            chain_file_path=tmp_path / "chain.jsonl",
            run_id=run_id,
            enforcer=AlwaysRejectEnforcer(),
        )
        entry = _make_chain_entry(run_id=run_id)
        with pytest.raises(ConstitutionViolationError, match="test_reject"):
            await writer.append(entry)

    @pytest.mark.asyncio
    async def test_chain_writer_multiple_appends_with_enforcer(self, tmp_path: Path):
        """Enforcer is called on every append without accumulating state."""
        enforcer = RunContractEnforcer()
        run_id = "run-multi"
        writer = ChainWriter(
            chain_file_path=tmp_path / "chain.jsonl",
            run_id=run_id,
            enforcer=enforcer,
        )
        for i in range(3):
            entry = ChainEntry(
                run_id=run_id,
                signal="acquire.started",
                machine="acquire",
            )
            result = await writer.append(entry)
            assert result.sequence == i + 1


# ---------------------------------------------------------------------------
# ConstitutionViolationError attributes
# ---------------------------------------------------------------------------

class TestConstitutionViolationError:
    def test_rule_attribute(self):
        err = ConstitutionViolationError(rule="test_rule", detail="some detail")
        assert err.rule == "test_rule"

    def test_detail_attribute(self):
        err = ConstitutionViolationError(rule="test_rule", detail="some detail")
        assert err.detail == "some detail"

    def test_str_includes_rule_and_detail(self):
        err = ConstitutionViolationError(rule="my_rule", detail="bad thing happened")
        assert "my_rule" in str(err)
        assert "bad thing happened" in str(err)

    def test_is_exception(self):
        err = ConstitutionViolationError(rule="r", detail="d")
        assert isinstance(err, Exception)
