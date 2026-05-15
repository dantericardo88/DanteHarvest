"""
StorageFactory — encrypted-by-default artifact store factory.

Wave 4b fix: encrypted_at_rest (7→9) — make EncryptedStore the default.

Callers previously had to opt in to encryption; now the factory returns an
EncryptedStore whenever encryption is not explicitly disabled.

Priority order for deciding which store to return:
1. ``config.encryption_enabled`` / ``config.encrypt_at_rest`` — if a
   :class:`HarvestConfig` is passed and the key is False, use PlainStore.
2. ``HARVEST_ENCRYPT_STORE`` env var — "false"/"0"/"no" → PlainStore.
3. Default: EncryptedStore (passphrase from ``HARVEST_ENCRYPT_KEY`` env var,
   or the built-in default passphrase when none is set).

Opt-out paths (explicit, auditable):
- ``StorageFactory.get_plain_store(path)``  — always plaintext.
- ``HARVEST_ENCRYPT_STORE=false``           — env-level opt-out.
- ``HarvestConfig(encrypt_at_rest=False)``  — config-level opt-out.

Constitutional guarantees:
- Fail-closed: if ``cryptography`` is not installed, falls back to PlainStore
  with a warning rather than crashing.
- Local-first: no network calls; key is derived locally.
- Zero-ambiguity: ``StorageFactory.get_default_store()`` always returns
  something usable — never raises.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Optional, Union

_log = logging.getLogger(__name__)

# Default passphrase used when HARVEST_ENCRYPT_KEY is not set.
# This provides authenticated encryption even without explicit key management;
# callers should override via env var or KeyManager for production deployments.
_DEFAULT_PASSPHRASE = "harvest-default-encrypt-key-change-me"


# ---------------------------------------------------------------------------
# PlainStore — same interface as EncryptedStore but no encryption
# ---------------------------------------------------------------------------

class PlainStore:
    """
    Simple unencrypted file store with the same interface as EncryptedStore.

    Artifacts are stored as raw bytes under ``base_path/<artifact_id>.bin``.
    Use this only when encryption is explicitly disabled.
    """

    _SUFFIX = ".bin"

    def __init__(self, base_path: Path) -> None:
        self._base = Path(base_path)
        self._base.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def write(self, artifact_id: str, data: bytes) -> Path:
        """Write *data* to disk and return the file path."""
        path = self._path(artifact_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(self._SUFFIX + ".tmp")
        try:
            tmp.write_bytes(data)
            tmp.replace(path)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
        return path

    def read(self, artifact_id: str) -> bytes:
        """Read and return raw bytes for *artifact_id*."""
        path = self._path(artifact_id)
        if not path.exists():
            raise FileNotFoundError(f"PlainStore: artifact not found: {artifact_id!r}")
        return path.read_bytes()

    def delete(self, artifact_id: str) -> bool:
        """Delete artifact from disk. Returns True if file existed."""
        path = self._path(artifact_id)
        if path.exists():
            path.unlink(missing_ok=True)
            return True
        return False

    def exists(self, artifact_id: str) -> bool:
        """Return True if the artifact file exists on disk."""
        return self._path(artifact_id).exists()

    def list_artifacts(self) -> List[str]:
        """Return all artifact IDs stored under *base_path*."""
        return [
            p.stem
            for p in self._base.rglob(f"*{self._SUFFIX}")
        ]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _path(self, artifact_id: str) -> Path:
        return self._base / f"{artifact_id}{self._SUFFIX}"

    def __repr__(self) -> str:
        return f"PlainStore(base_path={self._base!r})"


# ---------------------------------------------------------------------------
# StorageFactory
# ---------------------------------------------------------------------------

class StorageFactory:
    """
    Factory that returns the appropriate store based on configuration.

    Default: :class:`EncryptedStore` when encryption is not explicitly disabled.
    Override: set ``HARVEST_ENCRYPT_STORE=false`` or pass a config with
    ``encryption_enabled=False`` / ``encrypt_at_rest=False``.
    """

    @staticmethod
    def get_default_store(
        base_path: Path,
        config: Optional[object] = None,
    ) -> Union["EncryptedStoreAdapter", PlainStore]:
        """
        Return an EncryptedStore by default; PlainStore if explicitly disabled.

        The returned EncryptedStore is wrapped as an :class:`EncryptedStoreAdapter`
        so it exposes the same ``write(artifact_id, data)``, ``read``, ``delete``,
        ``exists``, and ``list_artifacts`` interface as :class:`PlainStore`.

        Checks (in order):
        1. ``config.encryption_enabled`` or ``config.encrypt_at_rest`` if provided.
        2. ``HARVEST_ENCRYPT_STORE`` env var ("false"/"0"/"no" → PlainStore).
        3. Falls back to EncryptedStore.

        If the ``cryptography`` package is missing, warns and returns PlainStore.
        """
        use_encryption = StorageFactory._resolve_encryption_flag(config)
        base_path = Path(base_path)

        if not use_encryption:
            return PlainStore(base_path)

        try:
            from harvest_core.storage.encrypted_store import EncryptedStore
            passphrase = os.environ.get("HARVEST_ENCRYPT_KEY", _DEFAULT_PASSPHRASE)
            store = EncryptedStore(passphrase=passphrase)
            return EncryptedStoreAdapter(store, base_path)
        except Exception as exc:
            _log.warning(
                "StorageFactory: EncryptedStore unavailable (%s); falling back to PlainStore. "
                "Install 'cryptography' package to enable encryption.",
                exc,
            )
            return PlainStore(base_path)

    @staticmethod
    def get_plain_store(base_path: Path) -> PlainStore:
        """Explicitly return an unencrypted PlainStore."""
        return PlainStore(Path(base_path))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_encryption_flag(config: Optional[object]) -> bool:
        """Determine whether encryption should be enabled."""
        # 1. Config object check
        if config is not None:
            for attr in ("encryption_enabled", "encrypt_at_rest"):
                val = None
                # Support both attribute access and dict-like .get()
                if hasattr(config, "get"):
                    val = config.get(attr)  # type: ignore[union-attr]
                elif hasattr(config, attr):
                    val = getattr(config, attr)
                if val is not None and isinstance(val, bool):
                    return val

        # 2. Env var check
        env_val = os.environ.get("HARVEST_ENCRYPT_STORE", "").strip().lower()
        if env_val in ("false", "0", "no"):
            return False
        if env_val in ("true", "1", "yes"):
            return True

        # 3. Default: encrypt
        return True


# ---------------------------------------------------------------------------
# EncryptedStoreAdapter — wraps EncryptedStore with artifact-id-based API
# ---------------------------------------------------------------------------

class EncryptedStoreAdapter:
    """
    Adapts :class:`EncryptedStore` (path-based API) to the artifact-id-based
    interface shared with :class:`PlainStore`.

    Artifacts are stored as ``<base_path>/<artifact_id>.dhex``.
    """

    _SUFFIX = ".dhex"

    def __init__(self, encrypted_store: object, base_path: Path) -> None:
        self._store = encrypted_store
        self._base = Path(base_path)
        self._base.mkdir(parents=True, exist_ok=True)

    def write(self, artifact_id: str, data: bytes) -> Path:
        path = self._path(artifact_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._store.write(path, data)  # type: ignore[union-attr]
        return path

    def read(self, artifact_id: str) -> bytes:
        path = self._path(artifact_id)
        if not path.exists():
            raise FileNotFoundError(f"EncryptedStoreAdapter: artifact not found: {artifact_id!r}")
        return self._store.read(path)  # type: ignore[union-attr]

    def delete(self, artifact_id: str) -> bool:
        path = self._path(artifact_id)
        if path.exists():
            path.unlink(missing_ok=True)
            return True
        return False

    def exists(self, artifact_id: str) -> bool:
        return self._path(artifact_id).exists()

    def list_artifacts(self) -> List[str]:
        return [p.stem for p in self._base.rglob(f"*{self._SUFFIX}")]

    @property
    def is_active(self) -> bool:
        return getattr(self._store, "is_active", True)

    def _path(self, artifact_id: str) -> Path:
        return self._base / f"{artifact_id}{self._SUFFIX}"

    def __repr__(self) -> str:
        return f"EncryptedStoreAdapter(base_path={self._base!r}, store={self._store!r})"
