"""
AutoSealingChainWriter — ChainWriter with automatic Merkle sealing.

Wave 6a: merkle_chain_sealing — auto-seal on every append (8→9).

Wraps ChainWriter to seal the Merkle chain manifest automatically:
- seal_every_n=1 (default): seal after every single append
- seal_every_n=N: seal after every N appends (batching for performance)
- seal_every_n=0: never auto-seal (same as plain ChainWriter)

Each seal updates the sidecar manifest.json with a fresh Merkle root,
providing a cryptographic checkpoint after each evidence chain update.

The auto-sealing pattern means:
1. Every ChainEntry is immediately covered by a Merkle root
2. `harvest verify-chain` can always verify — no manual `--seal` required
3. External monitors can poll the manifest for root changes

Constitutional guarantees:
- Fail-open: sealing errors are logged but never prevent the append from completing
- Append-only: sealing never modifies the JSONL entries, only the sidecar manifest
- Local-first: pure SHA-256 / stdlib, no network calls
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import List, Optional

import hashlib
import hmac
import json

from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.provenance.merkle_chain import MerkleChainManifest

logger = logging.getLogger(__name__)

_DEFAULT_SIGN_KEY = b"harvest-manifest-signing-key-v1"
_ENV_SIGN_KEY_VAR = "HARVEST_SIGN_KEY"


def _get_signing_key() -> bytes:
    """
    Return the signing key from HARVEST_SIGN_KEY env var, or fall back to the
    built-in default with a one-time warning.
    """
    import os
    raw = os.environ.get(_ENV_SIGN_KEY_VAR, "")
    if raw:
        return raw.encode()
    logger.warning(
        "HARVEST_SIGN_KEY not set — using built-in default signing key. "
        "Set %s to a secret value for production deployments.",
        _ENV_SIGN_KEY_VAR,
    )
    return _DEFAULT_SIGN_KEY


class ChainSealError(Exception):
    """Raised when Merkle chain sealing fails (fail-closed mode)."""


def _sign_manifest(manifest: dict, key: Optional[bytes] = None) -> str:
    """
    Return a 64-char hex HMAC-SHA256 signature of the manifest dict.

    The manifest is serialised with sorted keys to guarantee determinism
    regardless of insertion order.
    """
    signing_key: bytes = key if key is not None else _get_signing_key()
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(signing_key + payload).hexdigest()


def _verify_manifest(manifest: dict, signature: str, key: Optional[bytes] = None) -> bool:
    """Verify that *signature* matches the HMAC-SHA256 of *manifest*."""
    expected = _sign_manifest(manifest, key=key)
    # constant-time comparison to prevent timing attacks
    if len(expected) != len(signature):
        return False
    return hmac.compare_digest(expected, signature)


class AutoSealingChainWriter(ChainWriter):
    """
    ChainWriter that automatically seals the Merkle manifest on every append.

    Usage:
        writer = AutoSealingChainWriter(
            chain_file_path=Path("storage/chain/run-001.jsonl"),
            run_id="run-001",
            seal_every_n=1,  # seal after every append (default)
        )
        await writer.append(entry)  # chain is sealed immediately after
        manifest = writer.last_manifest  # access the current seal

    Batch mode (seal every 10 appends):
        writer = AutoSealingChainWriter(..., seal_every_n=10)
        # ... append many entries ...
        writer.seal_now()  # explicit flush/seal

    Manual-only mode:
        writer = AutoSealingChainWriter(..., seal_every_n=0)
        # seal only when seal_now() is called explicitly
    """

    def __init__(
        self,
        chain_file_path: Path,
        run_id: str,
        seal_every_n: int = 1,
    ):
        super().__init__(chain_file_path=chain_file_path, run_id=run_id)
        self._seal_every_n = seal_every_n
        self._appends_since_seal = 0
        self._seal_count = 0
        self._last_seal_at: Optional[float] = None
        self._merkle = MerkleChainManifest(self.chain_file_path)
        self._last_manifest = None

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    async def append(self, entry: ChainEntry) -> ChainEntry:
        result = await super().append(entry)
        self._appends_since_seal += 1
        if self._seal_every_n > 0 and self._appends_since_seal >= self._seal_every_n:
            self.seal_now()
        return result

    async def append_batch(self, entries: List[ChainEntry]) -> List[ChainEntry]:
        results = await super().append_batch(entries)
        self._appends_since_seal += len(entries)
        if self._seal_every_n > 0 and self._appends_since_seal >= self._seal_every_n:
            self.seal_now()
        return results

    # ------------------------------------------------------------------
    # Sealing
    # ------------------------------------------------------------------

    def seal_now(self) -> bool:
        """
        Immediately seal the chain and update the Merkle manifest.
        Returns True on success; raises ChainSealError on failure (fail-closed).
        """
        try:
            entries = self.read_all()
            if not entries:
                return True
            manifest_obj = self._merkle.seal(entries)
            self._last_manifest = manifest_obj
            self._seal_count += 1
            self._last_seal_at = time.time()
            self._appends_since_seal = 0
            # Write signature into the manifest sidecar file
            self._write_signature()
            logger.debug(
                "AutoSealingChainWriter: sealed chain (seq=%d, root=%s...)",
                len(entries),
                manifest_obj.merkle_root[:12],
            )
            return True
        except ChainSealError:
            raise
        except Exception as e:
            raise ChainSealError(f"Merkle seal failed: {e}") from e

    def _write_signature(self) -> None:
        """Add HMAC-SHA256 signature to the manifest sidecar JSON file."""
        mp = self._merkle.manifest_path
        if not mp.exists():
            return
        try:
            data = json.loads(mp.read_text())
            sig_payload = {k: v for k, v in data.items() if k != "_signature"}
            data["_signature"] = _sign_manifest(sig_payload)
            mp.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning("AutoSealingChainWriter: could not write signature: %s", e)

    def verify(self) -> tuple[bool, Optional[str]]:
        """
        Verify the current chain against the sealed manifest.
        Returns (ok, reason).
        """
        try:
            if not self._merkle.is_sealed():
                return False, "chain not yet sealed"
            entries = self.read_all()
            return self._merkle.verify(entries)
        except Exception as e:
            return False, str(e)

    def verify_chain_integrity(self) -> dict:
        """
        Full integrity check: Merkle verification + signature check.
        Returns {"valid": bool, "entries": int, "errors": list[str]}.
        """
        errors: List[str] = []
        entries: List = []

        try:
            entries = self.read_all()
        except Exception as e:
            errors.append(f"read_all failed: {e}")
            return {"valid": False, "entries": 0, "errors": errors}

        if not self._merkle.is_sealed():
            errors.append("chain not yet sealed")
            return {"valid": False, "entries": len(entries), "errors": errors}

        # Merkle verification
        try:
            ok, reason = self._merkle.verify(entries)
            if not ok:
                errors.append(f"Merkle verification failed: {reason}")
        except Exception as e:
            errors.append(f"Merkle verify error: {e}")
            ok = False

        # Signature verification
        mp = self._merkle.manifest_path
        if mp.exists():
            try:
                data = json.loads(mp.read_text())
                sig = data.get("_signature")
                if sig is None:
                    errors.append("manifest unsigned — _signature field missing")
                    ok = False
                else:
                    payload = {k: v for k, v in data.items() if k != "_signature"}
                    if not _verify_manifest(payload, sig):
                        errors.append("signature verification failed — manifest may be tampered")
                        ok = False
            except Exception as e:
                errors.append(f"signature check error: {e}")
                ok = False
        else:
            errors.append("manifest file not found")
            ok = False

        return {"valid": ok and not errors, "entries": len(entries), "errors": errors}

    def get_chain_manifest(self) -> dict:
        """
        Return the full manifest dict (including _signature).
        Raises ChainSealError if the chain has not been sealed.
        """
        if not self._merkle.is_sealed():
            raise ChainSealError("chain not yet sealed — call seal_now() or append an entry first")
        mp = self._merkle.manifest_path
        if not mp.exists():
            raise ChainSealError("manifest file not found")
        return json.loads(mp.read_text())

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def seal_count(self) -> int:
        return self._seal_count

    @property
    def last_seal_at(self) -> Optional[float]:
        return self._last_seal_at

    @property
    def last_manifest(self):
        return self._last_manifest

    @property
    def is_sealed(self) -> bool:
        return self._merkle.is_sealed()

    @property
    def manifest_path(self) -> Path:
        return self._merkle.manifest_path
