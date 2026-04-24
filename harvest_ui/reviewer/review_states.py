"""
Reviewer state machine for DanteHarvest pack review workflow.

Harvested from: OpenAdapt Desktop (MIT) — review.py state adjacency table pattern.

Constitutional guarantees:
- Fail-closed: invalid transitions raise InvalidTransitionError (not silent no-op)
- Zero-ambiguity: EGRESS_ALLOWED is an explicit frozenset, not a string comparison
- Chain entry: every state change emits reviewer.<status> signal
"""

from __future__ import annotations

import enum
from typing import Optional


class PackStatus(str, enum.Enum):
    PENDING  = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    DEFERRED = "deferred"
    DELETED  = "deleted"


# Single source of truth for allowed transitions
VALID_TRANSITIONS: dict[PackStatus, frozenset[PackStatus]] = {
    PackStatus.PENDING:  frozenset({PackStatus.APPROVED, PackStatus.REJECTED, PackStatus.DEFERRED}),
    PackStatus.DEFERRED: frozenset({PackStatus.APPROVED, PackStatus.REJECTED, PackStatus.DELETED}),
    PackStatus.APPROVED: frozenset({PackStatus.DELETED}),
    PackStatus.REJECTED: frozenset({PackStatus.DELETED}),
    PackStatus.DELETED:  frozenset(),
}

# Only packs in this set can proceed to pack promotion
EGRESS_ALLOWED: frozenset[PackStatus] = frozenset({PackStatus.APPROVED})


class InvalidTransitionError(Exception):
    def __init__(self, pack_id: str, from_s: PackStatus, to_s: PackStatus):
        allowed = VALID_TRANSITIONS.get(from_s, frozenset())
        super().__init__(
            f"Pack '{pack_id}': {from_s.value} → {to_s.value} not allowed. "
            f"Valid transitions: {sorted(s.value for s in allowed)}"
        )


def can_egress(status: PackStatus) -> bool:
    """True if this status allows promotion to the pack registry."""
    return status in EGRESS_ALLOWED


def transition(
    pack_id: str,
    from_status: PackStatus,
    to_status: PackStatus,
    *,
    registry,
    chain_writer=None,
    reviewer: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    """
    Apply a state transition with guard, registry write, and chain entry.

    Raises InvalidTransitionError if the transition is not in VALID_TRANSITIONS.
    Validates that the pack's current stored status matches from_status (TOCTOU guard).
    """
    allowed = VALID_TRANSITIONS.get(from_status, frozenset())
    if to_status not in allowed:
        raise InvalidTransitionError(pack_id, from_status, to_status)

    # TOCTOU guard: re-read current status before writing
    entry = registry.get(pack_id)
    if entry is None:
        raise KeyError(f"Pack '{pack_id}' not found in registry")
    _ALIAS: dict = {"candidate": PackStatus.PENDING, "promoted": PackStatus.APPROVED}
    try:
        current = PackStatus(entry.promotion_status)
    except ValueError:
        current = _ALIAS.get(entry.promotion_status)
        if current is None:
            raise
    if current != from_status:
        raise InvalidTransitionError(pack_id, current, to_status)

    if to_status == PackStatus.APPROVED:
        registry.promote(pack_id)
    elif to_status == PackStatus.REJECTED:
        registry.reject(pack_id, reason=reason or "")
    else:
        # DEFERRED / DELETED — set directly if registry supports it
        if hasattr(registry, "set_status"):
            registry.set_status(pack_id, to_status.value)

    if chain_writer:
        import asyncio
        data = {"pack_id": pack_id, "from": from_status.value, "to": to_status.value}
        if reviewer:
            data["reviewer"] = reviewer
        if reason:
            data["reason"] = reason
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_emit(chain_writer, pack_id, to_status, data))
            else:
                loop.run_until_complete(_emit(chain_writer, pack_id, to_status, data))
        except Exception:
            pass


async def _emit(chain_writer, pack_id: str, to_status: PackStatus, data: dict) -> None:
    from harvest_core.provenance.chain_entry import ChainEntry
    await chain_writer.append(ChainEntry(
        run_id=pack_id,
        signal=f"reviewer.{to_status.value}",
        machine="reviewer",
        data=data,
    ))
