"""Unit tests for harvest_ui/reviewer/review_states.py"""

import pytest
from unittest.mock import MagicMock

from harvest_ui.reviewer.review_states import (
    EGRESS_ALLOWED,
    VALID_TRANSITIONS,
    InvalidTransitionError,
    PackStatus,
    can_egress,
    transition,
)


# ---------------------------------------------------------------------------
# PackStatus values
# ---------------------------------------------------------------------------

def test_all_statuses_defined():
    statuses = {s.value for s in PackStatus}
    assert "pending" in statuses
    assert "approved" in statuses
    assert "rejected" in statuses
    assert "deferred" in statuses
    assert "deleted" in statuses


# ---------------------------------------------------------------------------
# VALID_TRANSITIONS table
# ---------------------------------------------------------------------------

def test_pending_can_go_to_approved_rejected_deferred():
    allowed = VALID_TRANSITIONS[PackStatus.PENDING]
    assert PackStatus.APPROVED in allowed
    assert PackStatus.REJECTED in allowed
    assert PackStatus.DEFERRED in allowed


def test_pending_cannot_go_to_deleted():
    allowed = VALID_TRANSITIONS[PackStatus.PENDING]
    assert PackStatus.DELETED not in allowed


def test_approved_cannot_go_to_rejected():
    allowed = VALID_TRANSITIONS[PackStatus.APPROVED]
    assert PackStatus.REJECTED not in allowed


def test_deferred_can_go_to_approved_or_rejected():
    allowed = VALID_TRANSITIONS[PackStatus.DEFERRED]
    assert PackStatus.APPROVED in allowed
    assert PackStatus.REJECTED in allowed


def test_deleted_has_no_transitions():
    allowed = VALID_TRANSITIONS[PackStatus.DELETED]
    assert len(allowed) == 0


# ---------------------------------------------------------------------------
# EGRESS_ALLOWED
# ---------------------------------------------------------------------------

def test_only_approved_can_egress():
    assert PackStatus.APPROVED in EGRESS_ALLOWED
    assert PackStatus.PENDING not in EGRESS_ALLOWED
    assert PackStatus.REJECTED not in EGRESS_ALLOWED
    assert PackStatus.DEFERRED not in EGRESS_ALLOWED


def test_can_egress_approved():
    assert can_egress(PackStatus.APPROVED) is True


def test_can_egress_rejected():
    assert can_egress(PackStatus.REJECTED) is False


def test_can_egress_pending():
    assert can_egress(PackStatus.PENDING) is False


# ---------------------------------------------------------------------------
# InvalidTransitionError
# ---------------------------------------------------------------------------

def test_invalid_transition_error_message():
    err = InvalidTransitionError("pack-001", PackStatus.APPROVED, PackStatus.REJECTED)
    assert "pack-001" in str(err)
    assert "approved" in str(err)
    assert "rejected" in str(err)


# ---------------------------------------------------------------------------
# transition() — happy path
# ---------------------------------------------------------------------------

def _make_registry(pack_id: str, status: str):
    entry = MagicMock()
    entry.promotion_status = status
    registry = MagicMock()
    registry.get = MagicMock(return_value=entry)
    registry.promote = MagicMock()
    registry.reject = MagicMock()
    registry.set_status = MagicMock()
    return registry


def test_transition_pending_to_approved_calls_promote():
    reg = _make_registry("p1", "pending")
    transition("p1", PackStatus.PENDING, PackStatus.APPROVED, registry=reg)
    reg.promote.assert_called_once_with("p1")


def test_transition_pending_to_rejected_calls_reject():
    reg = _make_registry("p1", "pending")
    transition("p1", PackStatus.PENDING, PackStatus.REJECTED, registry=reg, reason="bad quality")
    reg.reject.assert_called_once_with("p1", reason="bad quality")


def test_transition_pending_to_deferred_calls_set_status():
    reg = _make_registry("p1", "pending")
    transition("p1", PackStatus.PENDING, PackStatus.DEFERRED, registry=reg)
    reg.set_status.assert_called_once_with("p1", "deferred")


# ---------------------------------------------------------------------------
# transition() — error paths
# ---------------------------------------------------------------------------

def test_transition_invalid_raises():
    reg = _make_registry("p1", "pending")
    with pytest.raises(InvalidTransitionError):
        transition("p1", PackStatus.PENDING, PackStatus.DELETED, registry=reg)


def test_transition_toctou_raises():
    """If stored status differs from from_status, raise."""
    reg = _make_registry("p1", "approved")  # already approved
    with pytest.raises(InvalidTransitionError):
        transition("p1", PackStatus.PENDING, PackStatus.APPROVED, registry=reg)


def test_transition_unknown_pack_raises():
    registry = MagicMock()
    registry.get = MagicMock(return_value=None)
    with pytest.raises(KeyError):
        transition("missing", PackStatus.PENDING, PackStatus.APPROVED, registry=registry)
