"""Tests for harvest_index.registry.rights_aware_promoter."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock


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


def _register_pack(registry, pack_id="wf-001", status="promoted"):
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


def test_register_pack_artifacts(tmp_path):
    promoter = _make_promoter(tmp_path)
    promoter.register_pack_artifacts("wf-001", ["art-1", "art-2"])
    assert promoter._pack_artifacts["wf-001"] == ["art-1", "art-2"]


def test_register_pack_artifacts_persisted(tmp_path):
    from harvest_index.registry.rights_aware_promoter import RightsAwarePromoter
    r = _make_registry(tmp_path)
    e = _make_enforcer(tmp_path)
    promoter = RightsAwarePromoter(r, e, log_dir=tmp_path / "rights")
    promoter.register_pack_artifacts("wf-002", ["a1"])

    # Reload
    promoter2 = RightsAwarePromoter(r, e, log_dir=tmp_path / "rights")
    assert promoter2._pack_artifacts.get("wf-002") == ["a1"]


def test_run_cycle_no_packs_returns_empty(tmp_path):
    promoter = _make_promoter(tmp_path)
    events = promoter.run_cycle()
    assert events == []


def test_run_cycle_demotes_promoted_pack_with_expired_artifact(tmp_path):
    from harvest_core.rights.rights_model import RetentionClass
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)

    # Register artifact then expire it (don't register — simulates gc'd artifact)
    _register_pack(registry, "wf-001", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-001", ["expired-art"])

    events = promoter.run_cycle()
    demotions = [e for e in events if e.event_type == "demoted"]
    assert len(demotions) == 1
    assert demotions[0].pack_id == "wf-001"
    assert "expired-art" in demotions[0].artifact_ids


def test_run_cycle_skips_pack_without_registered_artifacts(tmp_path):
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-noarts", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    # Don't register any artifacts for this pack
    events = promoter.run_cycle()
    assert len(events) == 0


def test_run_cycle_restores_rights_expired_pack(tmp_path):
    from harvest_core.rights.rights_model import RetentionClass
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)

    # Register a valid artifact
    enforcer.register("art-live", RetentionClass.MEDIUM)

    # Pack is currently rights_expired but artifact is still valid
    _register_pack(registry, "wf-restore", status="rights_expired")
    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-restore", ["art-live"])

    events = promoter.run_cycle()
    restorations = [e for e in events if e.event_type == "re_promoted"]
    assert len(restorations) == 1
    assert restorations[0].pack_id == "wf-restore"


def test_audit_pack_returns_demotion_event(tmp_path):
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-audit", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-audit", ["gone-art"])

    event = promoter.audit_pack("wf-audit")
    assert event is not None
    assert event.event_type == "demoted"


def test_audit_pack_no_change_returns_none(tmp_path):
    from harvest_core.rights.rights_model import RetentionClass
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    enforcer.register("art-ok", RetentionClass.MEDIUM)
    _register_pack(registry, "wf-ok", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-ok", ["art-ok"])

    event = promoter.audit_pack("wf-ok")
    assert event is None  # artifact is valid, no change


def test_rights_event_log_written(tmp_path):
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-log", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-log", ["missing-art"])
    promoter.run_cycle()

    log = promoter.rights_event_log()
    assert len(log) >= 1
    assert log[0].pack_id == "wf-log"


def test_rights_event_log_append_only(tmp_path):
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-pack-a", status="promoted")
    _register_pack(registry, "wf-pack-b", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-pack-a", ["gone-a"])
    promoter.register_pack_artifacts("wf-pack-b", ["gone-b"])
    promoter.run_cycle()  # demotes both

    log = promoter.rights_event_log()
    assert len(log) >= 2  # both demotions appended


# ---------------------------------------------------------------------------
# Demotion failure handling (Fix 1: bare-except → specific exceptions + audit)
# ---------------------------------------------------------------------------

def test_demotion_failure_is_recorded(tmp_path):
    """Mock set_status to raise so the failure appears in get_failed_demotions()."""
    from harvest_index.registry.rights_aware_promoter import RightsAwarePromoter, PromotionError

    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-fail", status="promoted")

    promoter = RightsAwarePromoter(registry, enforcer, log_dir=tmp_path / "rights")
    promoter.register_pack_artifacts("wf-fail", ["gone-art"])

    # Patch set_status to raise a PromotionError
    registry.set_status = MagicMock(side_effect=PromotionError("registry locked"))

    promoter.run_cycle()

    failures = promoter.get_failed_demotions()
    assert len(failures) == 1
    assert failures[0]["pack_id"] == "wf-fail"


def test_demotion_failure_does_not_raise(tmp_path):
    """Demotion error is caught internally — run_cycle() must not propagate it."""
    from harvest_index.registry.rights_aware_promoter import RightsAwarePromoter, PromotionError

    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-silent", status="promoted")

    promoter = RightsAwarePromoter(registry, enforcer, log_dir=tmp_path / "rights")
    promoter.register_pack_artifacts("wf-silent", ["gone-art"])
    registry.set_status = MagicMock(side_effect=OSError("disk full"))

    # Must not raise
    events = promoter.run_cycle()
    assert isinstance(events, list)


def test_demotion_success_not_in_failed_list(tmp_path):
    """Successful demotion leaves get_failed_demotions() empty."""
    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-ok", status="promoted")

    promoter = _make_promoter(tmp_path, registry=registry, enforcer=enforcer)
    promoter.register_pack_artifacts("wf-ok", ["gone-art"])
    promoter.run_cycle()

    assert promoter.get_failed_demotions() == []


def test_failed_demotions_has_error_and_ts(tmp_path):
    """Each failure entry must contain 'error' and 'ts' keys."""
    from harvest_index.registry.rights_aware_promoter import RightsAwarePromoter, PromotionError

    registry = _make_registry(tmp_path)
    enforcer = _make_enforcer(tmp_path)
    _register_pack(registry, "wf-keys", status="promoted")

    promoter = RightsAwarePromoter(registry, enforcer, log_dir=tmp_path / "rights")
    promoter.register_pack_artifacts("wf-keys", ["gone-art"])
    registry.set_status = MagicMock(side_effect=ValueError("bad state"))

    promoter.run_cycle()

    failures = promoter.get_failed_demotions()
    assert len(failures) == 1
    assert "error" in failures[0]
    assert "ts" in failures[0]
    assert "bad state" in failures[0]["error"]
