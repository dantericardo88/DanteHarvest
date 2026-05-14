"""Unit tests for EncryptedStore — AES-256-GCM artifact encryption."""

import os
import pytest

from harvest_core.storage.encrypted_store import (
    EncryptedStore,
    _derive_key,
    _encode_envelope,
    _decode_envelope,
    MAGIC,
    VERSION,
    NONCE_LEN,
    TAG_LEN,
    HEADER_LEN,
)
from harvest_core.control.exceptions import StorageError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASSPHRASE = "test-passphrase-for-unit-tests"
PLAINTEXT = b"Hello, DanteHarvest encrypted world!"


def active_store() -> EncryptedStore:
    """Return an encryption-active store using the test passphrase."""
    try:
        return EncryptedStore(passphrase=PASSPHRASE)
    except StorageError:
        pytest.skip("cryptography package not installed")


def inactive_store() -> EncryptedStore:
    """Return a no-op (plaintext) store."""
    return EncryptedStore(passphrase=None)


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

class TestDeriveKey:
    def test_returns_32_bytes(self):
        key = _derive_key(PASSPHRASE)
        assert len(key) == 32

    def test_deterministic(self):
        k1 = _derive_key(PASSPHRASE)
        k2 = _derive_key(PASSPHRASE)
        assert k1 == k2

    def test_different_passphrases_yield_different_keys(self):
        k1 = _derive_key("passphrase-A")
        k2 = _derive_key("passphrase-B")
        assert k1 != k2

    def test_returns_bytes(self):
        key = _derive_key("any")
        assert isinstance(key, bytes)


# ---------------------------------------------------------------------------
# Envelope encode / decode
# ---------------------------------------------------------------------------

class TestEnvelope:
    def _make_envelope(self):
        nonce = os.urandom(NONCE_LEN)
        tag = os.urandom(TAG_LEN)
        ciphertext = b"fake_ciphertext_data"
        return nonce, tag, ciphertext

    def test_encode_decode_roundtrip(self):
        nonce, tag, ciphertext = self._make_envelope()
        blob = _encode_envelope(nonce, tag, ciphertext)
        n2, t2, ct2 = _decode_envelope(blob)
        assert n2 == nonce
        assert t2 == tag
        assert ct2 == ciphertext

    def test_encoded_starts_with_magic(self):
        nonce, tag, ciphertext = self._make_envelope()
        blob = _encode_envelope(nonce, tag, ciphertext)
        assert blob[:4] == MAGIC

    def test_encoded_version_byte(self):
        nonce, tag, ciphertext = self._make_envelope()
        blob = _encode_envelope(nonce, tag, ciphertext)
        assert blob[4] == VERSION

    def test_decode_wrong_magic_raises(self):
        nonce, tag, ciphertext = self._make_envelope()
        blob = _encode_envelope(nonce, tag, ciphertext)
        bad_blob = b"XXXX" + blob[4:]
        with pytest.raises(StorageError, match="magic"):
            _decode_envelope(bad_blob)

    def test_decode_too_short_raises(self):
        with pytest.raises(StorageError):
            _decode_envelope(b"short")

    def test_header_length_constant(self):
        assert HEADER_LEN == len(MAGIC) + 1 + NONCE_LEN + TAG_LEN


# ---------------------------------------------------------------------------
# EncryptedStore — inactive (passphrase=None)
# ---------------------------------------------------------------------------

class TestEncryptedStoreInactive:
    def test_is_active_false(self):
        store = inactive_store()
        assert store.is_active is False

    def test_write_and_read_plaintext(self, tmp_path):
        store = inactive_store()
        path = tmp_path / "plain.bin"
        store.write(path, PLAINTEXT)
        result = store.read(path)
        assert result == PLAINTEXT

    def test_read_nonexistent_raises(self, tmp_path):
        store = inactive_store()
        with pytest.raises(StorageError):
            store.read(tmp_path / "ghost.bin")

    def test_encrypt_on_inactive_raises(self):
        store = inactive_store()
        with pytest.raises(StorageError):
            store.encrypt(PLAINTEXT)

    def test_decrypt_on_inactive_raises(self):
        store = inactive_store()
        with pytest.raises(StorageError):
            store.decrypt(b"anything")

    def test_from_env_without_key_is_inactive(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        store = EncryptedStore.from_env()
        assert store.is_active is False

    def test_repr_inactive(self):
        store = inactive_store()
        assert "active=False" in repr(store)


# ---------------------------------------------------------------------------
# EncryptedStore — active (with passphrase)
# ---------------------------------------------------------------------------

class TestEncryptedStoreActive:
    def test_is_active_true(self):
        store = active_store()
        assert store.is_active is True

    def test_encrypt_returns_bytes(self):
        store = active_store()
        blob = store.encrypt(PLAINTEXT)
        assert isinstance(blob, bytes)

    def test_encrypted_starts_with_magic(self):
        store = active_store()
        blob = store.encrypt(PLAINTEXT)
        assert blob[:4] == MAGIC

    def test_encrypt_decrypt_roundtrip(self):
        store = active_store()
        blob = store.encrypt(PLAINTEXT)
        result = store.decrypt(blob)
        assert result == PLAINTEXT

    def test_encrypt_produces_different_ciphertext_each_time(self):
        store = active_store()
        blob1 = store.encrypt(PLAINTEXT)
        blob2 = store.encrypt(PLAINTEXT)
        # Different nonces → different ciphertext
        assert blob1 != blob2

    def test_decrypt_both_yield_same_plaintext(self):
        store = active_store()
        b1 = store.encrypt(PLAINTEXT)
        b2 = store.encrypt(PLAINTEXT)
        assert store.decrypt(b1) == store.decrypt(b2) == PLAINTEXT

    def test_wrong_key_decrypt_raises(self):
        store1 = active_store()
        store2 = EncryptedStore(passphrase="completely-different-key")
        blob = store1.encrypt(PLAINTEXT)
        with pytest.raises(StorageError, match="authentication"):
            store2.decrypt(blob)

    def test_tampered_ciphertext_raises(self):
        store = active_store()
        blob = store.encrypt(PLAINTEXT)
        # Flip a byte in the ciphertext region
        tampered = bytearray(blob)
        tampered[-1] ^= 0xFF
        with pytest.raises(StorageError):
            store.decrypt(bytes(tampered))

    def test_write_and_read_encrypted(self, tmp_path):
        store = active_store()
        path = tmp_path / "secret.bin"
        store.write(path, PLAINTEXT)
        result = store.read(path)
        assert result == PLAINTEXT

    def test_written_file_is_not_plaintext(self, tmp_path):
        store = active_store()
        path = tmp_path / "secret.bin"
        store.write(path, PLAINTEXT)
        raw = path.read_bytes()
        assert PLAINTEXT not in raw  # ciphertext should not contain plaintext

    def test_read_nonexistent_raises(self, tmp_path):
        store = active_store()
        with pytest.raises(StorageError):
            store.read(tmp_path / "ghost.bin")

    def test_creates_parent_directories(self, tmp_path):
        store = active_store()
        deep = tmp_path / "a" / "b" / "c" / "file.bin"
        store.write(deep, PLAINTEXT)
        assert deep.exists()
        assert store.read(deep) == PLAINTEXT

    def test_empty_plaintext(self):
        store = active_store()
        blob = store.encrypt(b"")
        assert store.decrypt(blob) == b""

    def test_large_plaintext(self):
        store = active_store()
        big = os.urandom(1024 * 1024)  # 1 MB
        blob = store.encrypt(big)
        assert store.decrypt(blob) == big

    def test_from_env_with_key_is_active(self, monkeypatch):
        monkeypatch.setenv("HARVEST_ENCRYPT_KEY", PASSPHRASE)
        store = EncryptedStore.from_env()
        assert store.is_active is True

    def test_repr_active(self):
        store = active_store()
        assert "active=True" in repr(store)
