"""
RunContractEnforcer — write-time enforcement of the Harvest RunContract.

The RunContract is the one-door doctrine: no artifact may be written to the
evidence chain without satisfying all RunContract invariants. Previously the
RunContract existed only as documentation / data model. This module enforces
it at the point of write.

Constitutional guarantees enforced here:
1. source_url must be non-empty — unknown provenance is forbidden
2. rights dict must carry a ``license`` key — unspecified license is quarantined
3. local_only flag is respected — remote writes are blocked when RunContract
   has remote_sync=False (checked if contract is passed)
4. Every chain entry is validated before append — ChainWriter opt-in via
   ``enforcer=RunContractEnforcer()``

Usage (standalone):
    enforcer = RunContractEnforcer()
    enforcer.validate_write(
        artifact_id="art-001",
        source_url="https://example.com/doc.pdf",
        rights={"license": "MIT"},
    )

Usage (wired into ChainWriter):
    writer = ChainWriter(
        chain_file_path=path,
        run_id=run_id,
        enforcer=RunContractEnforcer(),
    )
    # validate_chain_entry is called automatically on every append
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class ConstitutionViolationError(Exception):
    """
    Raised when a write operation violates the RunContract constitution.

    Attributes:
        rule: short identifier of the violated rule
        detail: human-readable description
    """

    def __init__(self, rule: str, detail: str) -> None:
        self.rule = rule
        self.detail = detail
        super().__init__(f"[{rule}] {detail}")


# ---------------------------------------------------------------------------
# RunContractEnforcer
# ---------------------------------------------------------------------------

class RunContractEnforcer:
    """
    Validates artifact writes and chain entries against the RunContract doctrine.

    Attach to a ChainWriter via ``enforcer=`` parameter to enforce write-time
    validation on every append.  Can also be used standalone for ad-hoc checks.

    Rules enforced:
    - ``source_not_empty``: source_url may not be empty or None
    - ``license_required``: rights dict must contain a non-empty ``license`` key
    - ``local_only_respected``: remote writes blocked when local_only=True
    - ``chain_entry_has_run_id``: chain entry must carry a non-empty run_id
    - ``chain_entry_has_signal``: chain entry must carry a non-empty signal
    """

    # Rules that produce hard ConstitutionViolationError
    _REQUIRED_RIGHTS_FIELDS = ("license",)

    def validate_write(
        self,
        artifact_id: str,
        source_url: str,
        rights: Dict[str, Any],
        *,
        local_only: bool = False,
        destination: Optional[str] = None,
    ) -> None:
        """
        Validate an artifact write against the RunContract doctrine.

        Parameters
        ----------
        artifact_id:
            Identifier of the artifact being written.
        source_url:
            Provenance URL (or URI) for the artifact. Must not be empty.
        rights:
            Rights dictionary. Must contain at least ``{"license": "<value>"}``.
        local_only:
            If True, the write must remain local. Providing a remote
            ``destination`` alongside local_only=True raises a violation.
        destination:
            Optional remote destination. Blocked when local_only=True.

        Raises
        ------
        ConstitutionViolationError
            On any doctrine violation.
        """
        self._check_source_url(artifact_id, source_url)
        self._check_rights(artifact_id, rights)
        self._check_local_only(artifact_id, local_only, destination)

    def validate_chain_entry(self, entry: Dict[str, Any]) -> None:
        """
        Validate a chain entry dict before it is appended.

        Parameters
        ----------
        entry:
            A dict (or Pydantic model coerced to dict) representing a ChainEntry.

        Raises
        ------
        ConstitutionViolationError
            If the entry is missing required fields.
        """
        run_id = entry.get("run_id", "") if isinstance(entry, dict) else getattr(entry, "run_id", "")
        signal = entry.get("signal", "") if isinstance(entry, dict) else getattr(entry, "signal", "")

        if not run_id:
            raise ConstitutionViolationError(
                rule="chain_entry_has_run_id",
                detail="ChainEntry is missing a run_id — cannot append to evidence chain.",
            )
        if not signal:
            raise ConstitutionViolationError(
                rule="chain_entry_has_signal",
                detail="ChainEntry is missing a signal — every chain event must name its action.",
            )

    # ------------------------------------------------------------------
    # Private rule checkers
    # ------------------------------------------------------------------

    def _check_source_url(self, artifact_id: str, source_url: str) -> None:
        if not source_url or not source_url.strip():
            raise ConstitutionViolationError(
                rule="source_not_empty",
                detail=(
                    f"Artifact '{artifact_id}' has no source_url. "
                    "The one-door doctrine requires every artifact to declare its provenance."
                ),
            )

    def _check_rights(self, artifact_id: str, rights: Dict[str, Any]) -> None:
        if not isinstance(rights, dict):
            raise ConstitutionViolationError(
                rule="license_required",
                detail=(
                    f"Artifact '{artifact_id}' rights must be a dict, got {type(rights).__name__}."
                ),
            )
        for field in self._REQUIRED_RIGHTS_FIELDS:
            value = rights.get(field)
            if not value or (isinstance(value, str) and not value.strip()):
                raise ConstitutionViolationError(
                    rule="license_required",
                    detail=(
                        f"Artifact '{artifact_id}' rights dict is missing required field '{field}'. "
                        "No artifact may be written without an explicit license declaration."
                    ),
                )

    def _check_local_only(
        self,
        artifact_id: str,
        local_only: bool,
        destination: Optional[str],
    ) -> None:
        if local_only and destination:
            raise ConstitutionViolationError(
                rule="local_only_respected",
                detail=(
                    f"Artifact '{artifact_id}' is marked local_only but a remote destination "
                    f"'{destination}' was provided. RunContract remote_sync=False prohibits this."
                ),
            )
