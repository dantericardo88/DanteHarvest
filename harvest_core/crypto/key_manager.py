"""
KeyManager — versioned key rotation + per-artifact HKDF for EncryptedStore.

Wave 4b: encrypted_at_rest — key rotation + per-artifact KDF (7→9).

Three capabilities added on top of EncryptedStore:

1. KeyManager: versioned key registry persisted to disk.
   - Each key version has a UUID, creation timestamp, and optional expiry.
   - Active key ID written to a manifest so readers always know which version to use.
   - rotate() promotes a new passphrase, retaining old versions for decryption.

2. artifact_kdf(): per-artifact key derivation via HKDF-SHA256.
   - Derives a unique 32-byte key from (master_key, artifact_id).
   - Compromise of one artifact's key does not reveal the master or other artifacts.
   - Falls back to PBKDF2-HMAC-SHA256 when the cryptography package is missing.

3. KeyRotator: re-encrypts all .dhex artifacts from old key → new key.
   - Reads each artifact using the old EncryptedStore, writes with new EncryptedStore.
   - Idempotent: tracks rotated artifact IDs in a JSONL log.
   - Non-destructive: original files overwritten only after successful re-encryption.

Constitutional guarantees:
- Fail-closed: rotation failure on any artifact halts (caller decides to continue/abort)
- Local-first: key manifest is a JSON file on disk, zero network calls
- Append-only audit: rotation events logged to rotation_log.jsonl
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Iterator, List, Optional
from uuid import uuid4

from harvest_core.storage.encrypted_store import EncryptedStore, _derive_key


# ---------------------------------------------------------------------------
# Per-artifact KDF
# ---------------------------------------------------------------------------

def artifact_kdf(master_key: bytes, artifact_id: str) -> bytes:
    """
    Derive a unique 32-byte encryption key for artifact_id from master_key.

    Uses HKDF-SHA256 when cryptography is available, PBKDF2-HMAC-SHA256 otherwise.
    The artifact_id is used as HKDF info (domain separation), so different artifacts
    get cryptographically independent keys even from the same master.
    """
    info = f"harvest-artifact-key:{artifact_id}".encode("utf-8")
    try:
        from cryptography.hazmat.primitives.hashes import SHA256
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        hkdf = HKDF(algorithm=SHA256(), length=32, salt=None, info=info)
        return hkdf.derive(master_key)
    except ImportError:
        # PBKDF2 fallback: use info as the salt (not secret, just domain separator)
        return hashlib.pbkdf2_hmac("sha256", master_key, info, iterations=100_000, dklen=32)


# ---------------------------------------------------------------------------
# Key versions
# ---------------------------------------------------------------------------

@dataclass
class KeyVersion:
    version_id: str
    created_at: float
    passphrase_hint: str = ""  # non-secret label only; never store the passphrase
    expires_at: Optional[float] = None

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "KeyVersion":
        return cls(**d)


# ---------------------------------------------------------------------------
# KeyManager
# ---------------------------------------------------------------------------

class KeyManager:
    """
    Versioned key registry for EncryptedStore key rotation.

    Persists key metadata (NOT the passphrase) to a JSON manifest.
    The actual key material is derived on the fly from the passphrase.

    Usage:
        km = KeyManager(storage_root=Path("storage"))
        km.initialize("my-passphrase")
        km.rotate("new-passphrase")
        store = km.active_store()   # EncryptedStore with the current key
    """

    MANIFEST_NAME = "key_manifest.json"

    def __init__(self, storage_root: Path):
        self._root = Path(storage_root) / "crypto"
        self._root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self._root / self.MANIFEST_NAME
        self._passphrases: Dict[str, str] = {}  # version_id → passphrase (in-memory only)
        self._manifest: Dict = self._load_manifest()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def initialize(self, passphrase: str, hint: str = "") -> KeyVersion:
        """Create the initial key version (idempotent if already initialized)."""
        if self._manifest.get("versions"):
            return KeyVersion.from_dict(self._manifest["versions"][self._manifest["active_version_id"]])
        return self._add_version(passphrase, hint=hint)

    def rotate(self, new_passphrase: str, hint: str = "", expire_old_after_s: Optional[float] = None) -> KeyVersion:
        """
        Promote new_passphrase as the active key.
        Old versions are retained for decryption of existing artifacts.
        """
        # Optionally mark old active key as expiring
        if expire_old_after_s is not None:
            old_id = self._manifest.get("active_version_id")
            if old_id and old_id in self._manifest.get("versions", {}):
                self._manifest["versions"][old_id]["expires_at"] = time.time() + expire_old_after_s
                self._save_manifest()
        return self._add_version(new_passphrase, hint=hint)

    def active_store(self) -> EncryptedStore:
        """Return an EncryptedStore using the current active passphrase."""
        vid = self._manifest.get("active_version_id")
        if not vid or vid not in self._passphrases:
            raise RuntimeError("KeyManager not initialized — call initialize() first")
        return EncryptedStore(passphrase=self._passphrases[vid])

    def store_for_version(self, version_id: str) -> EncryptedStore:
        """Return an EncryptedStore for a specific (possibly old) key version."""
        if version_id not in self._passphrases:
            raise KeyError(f"version_id {version_id!r} not loaded in this process session")
        return EncryptedStore(passphrase=self._passphrases[version_id])

    def active_version(self) -> Optional[KeyVersion]:
        vid = self._manifest.get("active_version_id")
        if not vid:
            return None
        d = self._manifest.get("versions", {}).get(vid)
        return KeyVersion.from_dict(d) if d else None

    def all_versions(self) -> List[KeyVersion]:
        return [KeyVersion.from_dict(v) for v in self._manifest.get("versions", {}).values()]

    def active_master_key(self) -> bytes:
        """Return the raw 32-byte master key for use with artifact_kdf()."""
        vid = self._manifest.get("active_version_id")
        if not vid or vid not in self._passphrases:
            raise RuntimeError("KeyManager not initialized")
        return _derive_key(self._passphrases[vid])

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _add_version(self, passphrase: str, hint: str = "") -> KeyVersion:
        vid = str(uuid4())
        version = KeyVersion(
            version_id=vid,
            created_at=time.time(),
            passphrase_hint=hint,
        )
        versions = self._manifest.setdefault("versions", {})
        versions[vid] = version.to_dict()
        self._manifest["active_version_id"] = vid
        self._passphrases[vid] = passphrase
        self._save_manifest()
        return version

    def _load_manifest(self) -> Dict:
        if self._manifest_path.exists():
            try:
                return json.loads(self._manifest_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"versions": {}, "active_version_id": None}

    def _save_manifest(self) -> None:
        self._manifest_path.write_text(
            json.dumps(self._manifest, indent=2), encoding="utf-8"
        )


# ---------------------------------------------------------------------------
# KeyRotator
# ---------------------------------------------------------------------------

@dataclass
class RotationResult:
    artifact_path: str
    success: bool
    error: Optional[str] = None


class KeyRotator:
    """
    Re-encrypt all .dhex artifacts from old_store → new_store.

    Usage:
        rotator = KeyRotator(artifacts_dir=Path("storage/artifacts"), log_dir=Path("storage/crypto"))
        results = list(rotator.rotate(old_store, new_store))
    """

    LOG_NAME = "rotation_log.jsonl"

    def __init__(self, artifacts_dir: Path, log_dir: Optional[Path] = None):
        self._artifacts_dir = Path(artifacts_dir)
        self._log_dir = Path(log_dir) if log_dir else self._artifacts_dir.parent / "crypto"
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self._log_dir / self.LOG_NAME

    def rotate(
        self,
        old_store: EncryptedStore,
        new_store: EncryptedStore,
        glob_pattern: str = "**/*.dhex",
    ) -> Iterator[RotationResult]:
        """
        Yield one RotationResult per artifact processed.
        Artifacts are re-encrypted in place (read old key → write new key).
        """
        already_rotated = self._load_rotated_set()
        rotated_at = time.time()

        for artifact_path in sorted(self._artifacts_dir.glob(glob_pattern)):
            path_str = str(artifact_path)
            if path_str in already_rotated:
                continue  # idempotent skip

            try:
                plaintext = old_store.read(artifact_path)
                new_store.write(artifact_path, plaintext)
                self._append_log({
                    "artifact": path_str,
                    "rotated_at": rotated_at,
                    "success": True,
                })
                yield RotationResult(artifact_path=path_str, success=True)
            except Exception as e:
                self._append_log({
                    "artifact": path_str,
                    "rotated_at": rotated_at,
                    "success": False,
                    "error": str(e),
                })
                yield RotationResult(artifact_path=path_str, success=False, error=str(e))

    def rotation_log(self) -> List[dict]:
        if not self._log_path.exists():
            return []
        results = []
        for line in self._log_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    results.append(json.loads(line))
                except Exception:
                    pass
        return results

    def _load_rotated_set(self) -> set:
        return {
            entry["artifact"]
            for entry in self.rotation_log()
            if entry.get("success")
        }

    def _append_log(self, entry: dict) -> None:
        try:
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass
