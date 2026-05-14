"""
MerkleChainManifest — cryptographic Merkle-root sealing for the evidence chain.

Sprint goal: build Merkle tree over JSONL entries using SHA-256 leaf hashes;
seal on demand; verify on open; close the evidence_chain_robustness gap.

Design:
- Leaf hashes: SHA-256 of each entry's existing content_hash (hex string)
- Internal nodes: SHA-256 of concat(left_hash + right_hash)
- Root: top-level digest — seals the full chain state at a point in time
- Manifest file: chain_file.manifest.json alongside the chain JSONL
- Seal is cheap (O(n) hash operations), verify is O(n)

Constitutional alignment:
- Local-first: no network calls, pure Python hashlib
- Append-only: sealing never modifies the JSONL, only writes a sidecar
- Fail-closed: any verification failure raises ChainError
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from harvest_core.control.exceptions import ChainError


@dataclass
class MerkleManifest:
    """Sealed manifest stored alongside the evidence chain JSONL."""
    chain_path: str
    sealed_at: str           # ISO-8601 UTC
    entry_count: int
    leaf_hashes: List[str]   # per-entry SHA-256 hashes (in order)
    merkle_root: str         # top-level Merkle root hex digest
    manifest_version: str = "1.0"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "MerkleManifest":
        return cls(**d)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode()).hexdigest()


def _build_merkle_root(leaf_hashes: List[str]) -> str:
    """
    Build Merkle root from a flat list of leaf hex digests.

    Algorithm:
    1. If empty, root = SHA-256 of the empty string.
    2. If odd number, duplicate last leaf.
    3. Pair up adjacent leaves, hash concat, repeat until one node remains.
    """
    if not leaf_hashes:
        return _sha256_hex("")

    nodes = list(leaf_hashes)
    while len(nodes) > 1:
        if len(nodes) % 2 == 1:
            nodes.append(nodes[-1])  # duplicate last to make even
        next_level: List[str] = []
        for i in range(0, len(nodes), 2):
            combined = nodes[i] + nodes[i + 1]
            next_level.append(_sha256_hex(combined))
        nodes = next_level

    return nodes[0]


def _manifest_path(chain_path: Path) -> Path:
    return chain_path.with_suffix(chain_path.suffix + ".manifest.json")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class MerkleChainManifest:
    """
    Seals and verifies the evidence chain with a Merkle root.

    Usage:
        mcm = MerkleChainManifest(chain_path)
        manifest = mcm.seal(entries)          # call after ChainWriter finishes
        ok, reason = mcm.verify(entries)      # call before trusting the chain
    """

    def __init__(self, chain_path: Path):
        self.chain_path = Path(chain_path)
        self.manifest_path = _manifest_path(self.chain_path)

    # ------------------------------------------------------------------
    # Seal
    # ------------------------------------------------------------------

    def seal(self, entries) -> MerkleManifest:
        """
        Build and persist a Merkle manifest for the given chain entries.

        ``entries`` — iterable of ChainEntry objects (from ChainWriter.read_all()).

        Returns the sealed MerkleManifest.
        Raises ChainError on write failure.
        """
        entry_list = list(entries)
        leaf_hashes = self._extract_leaf_hashes(entry_list)
        root = _build_merkle_root(leaf_hashes)

        manifest = MerkleManifest(
            chain_path=str(self.chain_path),
            sealed_at=datetime.now(timezone.utc).isoformat(),
            entry_count=len(entry_list),
            leaf_hashes=leaf_hashes,
            merkle_root=root,
        )

        self._write_manifest(manifest)
        return manifest

    # ------------------------------------------------------------------
    # Verify
    # ------------------------------------------------------------------

    def verify(self, entries=None) -> tuple[bool, Optional[str]]:
        """
        Verify the chain against the sealed manifest.

        If ``entries`` is None, the chain is read from disk by the caller
        (ChainWriter.read_all()) before passing here. Pass them in to avoid
        double-reading.

        Returns (True, None) on success, (False, reason_string) on failure.
        Raises ChainError if the manifest file does not exist.
        """
        if not self.manifest_path.exists():
            raise ChainError(
                f"No manifest file found at {self.manifest_path}. "
                "Seal the chain before verifying."
            )

        try:
            manifest = self.load_manifest()
        except Exception as e:
            return False, f"Manifest load failed: {e}"

        if entries is None:
            return False, "No entries provided for verification"

        entry_list = list(entries)

        if len(entry_list) != manifest.entry_count:
            return False, (
                f"Entry count mismatch: manifest={manifest.entry_count}, "
                f"actual={len(entry_list)}"
            )

        current_leaves = self._extract_leaf_hashes(entry_list)
        if current_leaves != manifest.leaf_hashes:
            # Identify the first diverging leaf
            for i, (expected, actual) in enumerate(zip(manifest.leaf_hashes, current_leaves)):
                if expected != actual:
                    return False, f"Leaf hash mismatch at entry index {i}"
            return False, "Leaf hash list length mismatch"

        current_root = _build_merkle_root(current_leaves)
        if current_root != manifest.merkle_root:
            return False, (
                f"Merkle root mismatch: "
                f"manifest={manifest.merkle_root[:16]}…, "
                f"computed={current_root[:16]}…"
            )

        return True, None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def load_manifest(self) -> MerkleManifest:
        if not self.manifest_path.exists():
            raise ChainError(f"Manifest not found: {self.manifest_path}")
        with open(self.manifest_path, encoding="utf-8") as f:
            data = json.load(f)
        return MerkleManifest.from_dict(data)

    def is_sealed(self) -> bool:
        """Return True if a manifest sidecar exists."""
        return self.manifest_path.exists()

    def _extract_leaf_hashes(self, entries: list) -> List[str]:
        """
        Extract per-entry leaf hashes.

        Each leaf = SHA-256 of the entry's content_hash string.
        This double-hashing means the leaf is bound to both the entry
        content AND the chain position (via the content_hash which
        includes the sequence number in its computation).
        """
        leaves = []
        for entry in entries:
            ch = entry.content_hash
            if not ch:
                # Recompute if missing (shouldn't happen in a sealed chain)
                ch = entry.compute_hash()
            leaves.append(_sha256_hex(ch))
        return leaves

    def _write_manifest(self, manifest: MerkleManifest) -> None:
        try:
            self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest.to_dict(), f, indent=2)
                f.flush()
        except Exception as e:
            raise ChainError(f"Failed to write manifest: {e}") from e
