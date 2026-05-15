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

from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.provenance.chain_writer import ChainWriter
from harvest_core.provenance.merkle_chain import MerkleChainManifest

logger = logging.getLogger(__name__)


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
        Returns True on success, False on failure (fail-open).
        """
        try:
            entries = self.read_all()
            if not entries:
                return True
            manifest = self._merkle.seal(entries)
            self._last_manifest = manifest
            self._seal_count += 1
            self._last_seal_at = time.time()
            self._appends_since_seal = 0
            logger.debug(
                "AutoSealingChainWriter: sealed chain (seq=%d, root=%s...)",
                len(entries),
                manifest.merkle_root[:12],
            )
            return True
        except Exception as e:
            logger.warning("AutoSealingChainWriter: seal failed: %s", e)
            return False

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
