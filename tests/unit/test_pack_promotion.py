"""Tests for pack_promotion_pipeline: dry-run, rollback, audit trail."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_registry(tmp_path):
    from harvest_index.registry.pack_registry import PackRegistry
    return PackRegistry(root=tmp_path / "registry")


def _make_enforcer(tmp_path):
    from harvest_core.rights.retention_enforcer import RetentionEnforcer
    return RetentionEnforcer(store_path=tmp_path / "retention")


def _make_promoter(tmp_path, registry=None, enforcer=None):
    from harvest_index.registry.rights_aware_promoter import RightsAwarePromoter
    r = registry or _make_registry(tmp_path)
    e = enforcer or _make_enforcer(tmp_path)
    return RightsAwarePromoter(r, e, log_dir=tmp_path / "rights")


def _register_pack(registry, pack_id="wf-001", status="candidate"):
    from harvest_index.registry.pack_registry import PackEntry
    entry = PackEntry(
        pack_id=pack_id,
        pack_type="workflowPack",
        title="Test Pack",
        promotion_status=status,
        registered_at="2024-01-01T00:00:00Z",
        receipt_id="rcpt-1",
        storage_path="storage/packs",
        confidence_score=0.9,
    )
    registry._index[pack_id] = entry.to_dict()
    registry._save_index()
    return entry


def _valid_pack(pack_id: str = "wf-001", version: str = "1.0") -> dict:
    return {"id": pack_id, "name": "Test Pack", "version": version}


def _invalid_pack() -> dict:
    # Missing id, name, version
    return {"description": "incomplete"}


# ---------------------------------------------------------------------------
# promote() — dry-run mode
# ---------------------------------------------------------------------------

class TestPromoteDryRun:
    def test_dry_run_returns_success_true(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.promote(_valid_pack(), dry_run=True)
        assert result["success"] is True

    def test_dry_run_flag_in_result(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.promote(_valid_pack(), dry_run=True)
        assert result["dry_run"] is True

    def test_dry_run_contains_would_promote(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.promote(_valid_pack("wf-42"), dry_run=True)
        assert result["would_promote"] == "wf-42"

    def test_dry_run_message_present(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.promote(_valid_pack(), dry_run=True)
        assert "message" in result
        assert "Dry run" in result["message"]

    def test_dry_run_no_side_effects_on_promotion_log(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        promoter.promote(_valid_pack(), dry_run=True)
        assert promoter._promotion_log == []

    def test_dry_run_invalid_pack_returns_failure(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.promote(_invalid_pack(), dry_run=True)
        assert result["success"] is False
        assert "errors" in result

    def test_dry_run_includes_validation_info(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.promote(_valid_pack(), dry_run=True)
        assert "validation" in result
        assert result["validation"]["valid"] is True


# ---------------------------------------------------------------------------
# promote() — real promotion (dry_run=False)
# ---------------------------------------------------------------------------

class TestPromoteReal:
    def test_promote_records_to_log(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        assert len(promoter._promotion_log) == 1

    def test_promote_log_entry_has_pack_id(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        assert promoter._promotion_log[0]["pack_id"] == "wf-001"

    def test_promote_log_entry_has_version(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "2.5"), dry_run=False)
        assert promoter._promotion_log[0]["version"] == "2.5"

    def test_promote_log_entry_has_timestamp(self, tmp_path):
        import time
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        before = time.time()
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        after = time.time()
        ts = promoter._promotion_log[0]["timestamp"]
        assert before <= ts <= after

    def test_promote_dry_run_false_not_in_result(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        result = promoter.promote(_valid_pack("wf-001"), dry_run=False)
        assert result.get("dry_run") is False

    def test_promote_invalid_pack_no_log_entry(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        promoter.promote(_invalid_pack(), dry_run=False)
        assert len(promoter._promotion_log) == 0

    def test_promote_multiple_packs_all_logged(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        _register_pack(registry, "wf-002")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-002", "1.0"), dry_run=False)
        assert len(promoter._promotion_log) == 2


# ---------------------------------------------------------------------------
# rollback()
# ---------------------------------------------------------------------------

class TestRollback:
    def test_rollback_no_history_fails(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.rollback("wf-nonexistent")
        assert result["success"] is False
        assert "error" in result

    def test_rollback_no_history_error_mentions_pack_id(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        result = promoter.rollback("wf-xyz")
        assert "wf-xyz" in result["error"]

    def test_rollback_single_promotion_fails(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        # Only one history entry — no previous to roll back to
        result = promoter.rollback("wf-001")
        assert result["success"] is False

    def test_rollback_two_promotions_returns_success(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "2.0"), dry_run=False)
        result = promoter.rollback("wf-001")
        assert result["success"] is True

    def test_rollback_returns_previous_version(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "2.0"), dry_run=False)
        result = promoter.rollback("wf-001")
        assert result["rolled_back_to"] == "1.0"

    def test_rollback_to_specific_version(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "2.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "3.0"), dry_run=False)
        result = promoter.rollback("wf-001", to_version="1.0")
        assert result["success"] is True
        assert result["rolled_back_to"] == "1.0"

    def test_rollback_to_nonexistent_version_fails(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "2.0"), dry_run=False)
        result = promoter.rollback("wf-001", to_version="99.0")
        assert result["success"] is False

    def test_rollback_result_contains_pack_id(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "2.0"), dry_run=False)
        result = promoter.rollback("wf-001")
        assert result["pack_id"] == "wf-001"

    def test_rollback_result_contains_message(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-001", "2.0"), dry_run=False)
        result = promoter.rollback("wf-001")
        assert "message" in result


# ---------------------------------------------------------------------------
# get_promotion_audit_trail()
# ---------------------------------------------------------------------------

class TestGetPromotionAuditTrail:
    def test_empty_trail_returns_list(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        trail = promoter.get_promotion_audit_trail()
        assert isinstance(trail, list)

    def test_empty_trail_when_no_promotions(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        assert promoter.get_promotion_audit_trail() == []

    def test_trail_grows_with_promotions(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        _register_pack(registry, "wf-002")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-002", "1.0"), dry_run=False)
        assert len(promoter.get_promotion_audit_trail()) == 2

    def test_trail_filtered_by_pack_id(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        _register_pack(registry, "wf-002")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-002", "1.0"), dry_run=False)
        trail = promoter.get_promotion_audit_trail(pack_id="wf-001")
        assert len(trail) == 1
        assert trail[0]["pack_id"] == "wf-001"

    def test_trail_filter_unknown_pack_returns_empty(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        trail = promoter.get_promotion_audit_trail(pack_id="nonexistent")
        assert trail == []

    def test_trail_none_pack_id_returns_all(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        _register_pack(registry, "wf-002")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        promoter.promote(_valid_pack("wf-002", "1.0"), dry_run=False)
        trail = promoter.get_promotion_audit_trail(pack_id=None)
        assert len(trail) == 2

    def test_dry_run_promotions_not_in_trail(self, tmp_path):
        promoter = _make_promoter(tmp_path)
        promoter.promote(_valid_pack("wf-001"), dry_run=True)
        assert promoter.get_promotion_audit_trail() == []

    def test_trail_returns_copy_not_reference(self, tmp_path):
        registry = _make_registry(tmp_path)
        _register_pack(registry, "wf-001")
        promoter = _make_promoter(tmp_path, registry=registry)
        promoter.promote(_valid_pack("wf-001", "1.0"), dry_run=False)
        trail = promoter.get_promotion_audit_trail()
        trail.clear()
        # Internal log should be unchanged
        assert len(promoter._promotion_log) == 1
