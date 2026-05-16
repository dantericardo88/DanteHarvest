"""Unit tests for encrypted_at_rest improvements — StorageFactory key management."""

import os
import warnings

import pytest

from harvest_core.crypto.storage_factory import (
    HarvestKeyError,
    StorageFactory,
    _get_encryption_key,
)


# ---------------------------------------------------------------------------
# _get_encryption_key
# ---------------------------------------------------------------------------

class TestGetEncryptionKey:
    def test_returns_HARVEST_ENCRYPTION_KEY_when_set(self, monkeypatch):
        monkeypatch.setenv("HARVEST_ENCRYPTION_KEY", "my-strong-key-abc123")
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        key = _get_encryption_key()
        assert key == "my-strong-key-abc123"

    def test_returns_HARVEST_ENCRYPT_KEY_as_fallback(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.setenv("HARVEST_ENCRYPT_KEY", "legacy-key-xyz")
        key = _get_encryption_key()
        assert key == "legacy-key-xyz"

    def test_HARVEST_ENCRYPTION_KEY_takes_priority(self, monkeypatch):
        monkeypatch.setenv("HARVEST_ENCRYPTION_KEY", "primary")
        monkeypatch.setenv("HARVEST_ENCRYPT_KEY", "secondary")
        key = _get_encryption_key()
        assert key == "primary"

    def test_emits_RuntimeWarning_when_no_env_var_set(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            key = _get_encryption_key()
        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert len(runtime_warnings) >= 1
        assert "HARVEST_ENCRYPTION_KEY" in str(runtime_warnings[0].message)

    def test_generates_ephemeral_key_when_no_env_var(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            key = _get_encryption_key()
        # Ephemeral key is a 64-char hex string (secrets.token_hex(32))
        assert isinstance(key, str)
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)

    def test_ephemeral_keys_are_unique(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            k1 = _get_encryption_key()
            k2 = _get_encryption_key()
        assert k1 != k2


# ---------------------------------------------------------------------------
# StorageFactory.validate_key_configuration
# ---------------------------------------------------------------------------

class TestValidateKeyConfiguration:
    def test_configured_via_HARVEST_ENCRYPTION_KEY(self, monkeypatch):
        monkeypatch.setenv("HARVEST_ENCRYPTION_KEY", "deadbeef" * 8)
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        result = StorageFactory.validate_key_configuration()
        assert result["key_configured"] is True
        assert result["key_source"] == "HARVEST_ENCRYPTION_KEY"
        assert result["persistent"] is True
        assert result["key_length_bits"] > 0

    def test_configured_via_HARVEST_ENCRYPT_KEY(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.setenv("HARVEST_ENCRYPT_KEY", "cafebabe" * 8)
        result = StorageFactory.validate_key_configuration()
        assert result["key_configured"] is True
        assert result["key_source"] == "HARVEST_ENCRYPT_KEY"
        assert result["persistent"] is True

    def test_ephemeral_when_no_env_var(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        result = StorageFactory.validate_key_configuration()
        assert result["key_configured"] is False
        assert result["key_source"] == "ephemeral"
        assert result["persistent"] is False

    def test_key_length_bits_is_multiple_of_4(self, monkeypatch):
        monkeypatch.setenv("HARVEST_ENCRYPTION_KEY", "abcd1234" * 4)  # 32 hex chars = 128 bits
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        result = StorageFactory.validate_key_configuration()
        assert result["key_length_bits"] == 128

    def test_returns_dict_with_all_keys(self, monkeypatch):
        monkeypatch.delenv("HARVEST_ENCRYPTION_KEY", raising=False)
        monkeypatch.delenv("HARVEST_ENCRYPT_KEY", raising=False)
        result = StorageFactory.validate_key_configuration()
        assert set(result.keys()) == {"key_configured", "key_source", "key_length_bits", "persistent"}


# ---------------------------------------------------------------------------
# StorageFactory.get_store_with_key
# ---------------------------------------------------------------------------

class TestGetStoreWithKey:
    def test_raises_HarvestKeyError_for_empty_key(self, tmp_path):
        with pytest.raises(HarvestKeyError):
            StorageFactory.get_store_with_key(tmp_path, "")

    def test_accepts_explicit_key(self, tmp_path):
        # Should not raise; returns a store object
        try:
            store = StorageFactory.get_store_with_key(tmp_path, "explicit-key-abc123")
            # If cryptography is available, we get an EncryptedStoreAdapter
            assert store is not None
        except HarvestKeyError:
            # cryptography package not installed — acceptable
            pass

    def test_different_keys_produce_different_stores(self, tmp_path):
        try:
            s1 = StorageFactory.get_store_with_key(tmp_path / "s1", "key-one")
            s2 = StorageFactory.get_store_with_key(tmp_path / "s2", "key-two")
            assert s1 is not s2
        except HarvestKeyError:
            pytest.skip("cryptography package not available")


# ---------------------------------------------------------------------------
# HarvestKeyError
# ---------------------------------------------------------------------------

class TestHarvestKeyError:
    def test_is_exception_subclass(self):
        assert issubclass(HarvestKeyError, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(HarvestKeyError, match="test message"):
            raise HarvestKeyError("test message")
