"""
EncryptedStore — AES-256-GCM encrypted artifact storage wrapper.

Sprint goal: opt-in AES-256-GCM encryption via HARVEST_ENCRYPT_KEY env var.
All HarvestArtifact bytes go through the wrapper; decryption on read.

Design:
- Key derivation: Argon2id from user passphrase → 32-byte AES key
    argon2-cffi (pure Python, MIT), fallback to PBKDF2-HMAC-SHA256 if missing
- Cipher: AES-256-GCM (authenticated encryption — integrity + confidentiality)
    cryptography package (Apache-2.0/BSD, standard in Python ecosystem)
- Envelope format per file:
    [4 bytes magic] [1 byte version] [12 bytes nonce] [16 bytes GCM tag] [ciphertext]
- Opt-in: only activates when HARVEST_ENCRYPT_KEY env var is set
- Local-first: zero network calls, local key derivation + encryption

Constitutional alignment:
- Local-first: no key escrow, no cloud
- Fail-closed: missing key or tampered ciphertext raises StorageError
- Zero third-party API calls
"""

from __future__ import annotations

import hashlib
import os
import struct
from pathlib import Path
from typing import Optional

from harvest_core.control.exceptions import StorageError

# ---------------------------------------------------------------------------
# Optional-dependency guards
# ---------------------------------------------------------------------------

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _HAS_CRYPTOGRAPHY = True
except ImportError:  # pragma: no cover
    _HAS_CRYPTOGRAPHY = False

try:
    from argon2.low_level import hash_secret_raw, Type as Argon2Type
    _HAS_ARGON2 = True
except ImportError:
    _HAS_ARGON2 = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAGIC = b"DHEX"           # 4-byte magic prefix
VERSION = 1               # single byte envelope version
NONCE_LEN = 12            # 96-bit GCM nonce (NIST recommended)
TAG_LEN = 16              # 128-bit GCM authentication tag
HEADER_LEN = len(MAGIC) + 1 + NONCE_LEN + TAG_LEN  # 33 bytes

# Argon2id parameters (OWASP minimum for interactive login)
_ARGON2_TIME_COST = 2
_ARGON2_MEMORY_KB = 65536    # 64 MB
_ARGON2_PARALLELISM = 2
_ARGON2_HASH_LEN = 32        # 256-bit key output
_ARGON2_SALT = b"DanteHarvestKDF!"  # 16-byte fixed salt (not secret; key is)


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

def _derive_key(passphrase: str) -> bytes:
    """
    Derive a 32-byte AES key from a passphrase using Argon2id (preferred)
    or PBKDF2-HMAC-SHA256 (fallback if argon2-cffi is not installed).
    """
    pw_bytes = passphrase.encode("utf-8")

    if _HAS_ARGON2:
        return hash_secret_raw(
            secret=pw_bytes,
            salt=_ARGON2_SALT,
            time_cost=_ARGON2_TIME_COST,
            memory_cost=_ARGON2_MEMORY_KB,
            parallelism=_ARGON2_PARALLELISM,
            hash_len=_ARGON2_HASH_LEN,
            type=Argon2Type.ID,
        )

    # Fallback: PBKDF2-HMAC-SHA256 (stdlib, always available)
    return hashlib.pbkdf2_hmac(
        "sha256",
        pw_bytes,
        _ARGON2_SALT,
        iterations=600_000,   # OWASP 2023 recommendation for PBKDF2-SHA256
        dklen=32,
    )


# ---------------------------------------------------------------------------
# Envelope encode / decode
# ---------------------------------------------------------------------------

def _encode_envelope(nonce: bytes, tag: bytes, ciphertext: bytes) -> bytes:
    """Pack nonce + tag + ciphertext into a single bytes blob with magic header."""
    return MAGIC + struct.pack("B", VERSION) + nonce + tag + ciphertext


def _decode_envelope(data: bytes) -> tuple[bytes, bytes, bytes]:
    """Unpack envelope → (nonce, tag, ciphertext). Raises StorageError on bad magic."""
    if len(data) < HEADER_LEN:
        raise StorageError(
            "Encrypted data is too short to be a valid envelope",
            {"min_bytes": HEADER_LEN, "actual_bytes": len(data)},
        )
    if data[:4] != MAGIC:
        raise StorageError(
            "Invalid magic bytes — data was not encrypted by EncryptedStore or is corrupt",
            {"expected": MAGIC.hex(), "actual": data[:4].hex()},
        )
    version = data[4]
    if version != VERSION:
        raise StorageError(
            f"Unsupported envelope version: {version}",
            {"supported": VERSION},
        )
    offset = 5
    nonce = data[offset : offset + NONCE_LEN]
    offset += NONCE_LEN
    tag = data[offset : offset + TAG_LEN]
    offset += TAG_LEN
    ciphertext = data[offset:]
    return nonce, tag, ciphertext


# ---------------------------------------------------------------------------
# EncryptedStore
# ---------------------------------------------------------------------------

class EncryptedStore:
    """
    AES-256-GCM encrypted wrapper for local artifact storage.

    Instantiate once per process; reuses derived key.
    If passphrase is None and HARVEST_ENCRYPT_KEY env var is not set,
    the store operates in plaintext pass-through mode (no encryption).

    Example:
        store = EncryptedStore.from_env()
        store.write(path, data)
        data = store.read(path)
    """

    def __init__(self, passphrase: Optional[str] = None):
        """
        Args:
            passphrase: Encryption passphrase. If None, no encryption.
        """
        self._active = passphrase is not None
        self._key: Optional[bytes] = None
        if self._active:
            if not _HAS_CRYPTOGRAPHY:
                raise StorageError(
                    "cryptography package is required for EncryptedStore. "
                    "Run: pip install cryptography",
                    {},
                )
            self._key = _derive_key(passphrase)  # type: ignore[arg-type]

    @classmethod
    def from_env(cls) -> "EncryptedStore":
        """
        Construct from HARVEST_ENCRYPT_KEY environment variable.
        Returns a no-op (plaintext) store if env var is not set.
        """
        passphrase = os.environ.get("HARVEST_ENCRYPT_KEY")
        return cls(passphrase=passphrase)

    @property
    def is_active(self) -> bool:
        """True if encryption is enabled."""
        return self._active

    # ------------------------------------------------------------------
    # Core read / write
    # ------------------------------------------------------------------

    def write(self, path: Path, data: bytes) -> None:
        """
        Write data to path, encrypting if the store is active.

        Creates parent directories if needed. Atomic-ish: writes to tmp
        then renames (best-effort on Windows where rename may overwrite).
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        blob = self.encrypt(data) if self._active else data

        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            with open(tmp, "wb") as f:
                f.write(blob)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(path)
        except Exception as e:
            tmp.unlink(missing_ok=True)
            raise StorageError(f"EncryptedStore write failed: {e}", {"path": str(path)}) from e

    def read(self, path: Path) -> bytes:
        """
        Read data from path, decrypting if the store is active.

        Raises StorageError if the file does not exist or decryption fails.
        """
        path = Path(path)
        if not path.exists():
            raise StorageError(f"File not found: {path}", {"path": str(path)})
        try:
            with open(path, "rb") as f:
                blob = f.read()
        except Exception as e:
            raise StorageError(f"EncryptedStore read failed: {e}", {"path": str(path)}) from e

        return self.decrypt(blob) if self._active else blob

    # ------------------------------------------------------------------
    # Encrypt / decrypt primitives (can be used without path I/O)
    # ------------------------------------------------------------------

    def encrypt(self, plaintext: bytes) -> bytes:
        """
        Encrypt plaintext with AES-256-GCM.
        Returns: MAGIC + version + nonce + tag + ciphertext
        """
        if not self._active:
            raise StorageError("encrypt() called on inactive (no-key) EncryptedStore", {})
        if not _HAS_CRYPTOGRAPHY:  # pragma: no cover
            raise StorageError("cryptography package not installed", {})

        nonce = os.urandom(NONCE_LEN)
        aesgcm = AESGCM(self._key)
        # AESGCM.encrypt returns ciphertext + tag (appended)
        ct_with_tag = aesgcm.encrypt(nonce, plaintext, associated_data=None)
        # Split off the 16-byte tag from the end
        ciphertext = ct_with_tag[:-TAG_LEN]
        tag = ct_with_tag[-TAG_LEN:]
        return _encode_envelope(nonce, tag, ciphertext)

    def decrypt(self, blob: bytes) -> bytes:
        """
        Decrypt an encrypted envelope produced by encrypt().
        Raises StorageError on authentication failure or bad format.
        """
        if not self._active:
            raise StorageError("decrypt() called on inactive (no-key) EncryptedStore", {})
        if not _HAS_CRYPTOGRAPHY:  # pragma: no cover
            raise StorageError("cryptography package not installed", {})

        nonce, tag, ciphertext = _decode_envelope(blob)
        aesgcm = AESGCM(self._key)
        try:
            plaintext = aesgcm.decrypt(nonce, ciphertext + tag, associated_data=None)
        except Exception as e:
            raise StorageError(
                "AES-GCM authentication failed — data is corrupt or key is wrong",
                {"error": str(e)},
            ) from e
        return plaintext

    def __repr__(self) -> str:
        return f"EncryptedStore(active={self._active})"
