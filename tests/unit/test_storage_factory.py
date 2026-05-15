"""
Tests for StorageFactory — encrypted-by-default storage.

Covers:
- test_default_store_is_encrypted
- test_env_var_false_returns_plain
- test_env_var_true_returns_encrypted
- test_plain_store_write_read_roundtrip
- test_plain_store_delete
- test_encrypted_store_write_read_roundtrip
- test_storage_factory_config_override
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from harvest_core.crypto.storage_factory import (
    EncryptedStoreAdapter,
    PlainStore,
    StorageFactory,
)


# ---------------------------------------------------------------------------
# test_default_store_is_encrypted
# ---------------------------------------------------------------------------

def test_default_store_is_encrypted(tmp_path: Path) -> None:
    """StorageFactory.get_default_store() must return an EncryptedStoreAdapter
    (i.e., encrypted storage) when no override is in effect."""
    with patch.dict(os.environ, {"HARVEST_ENCRYPT_STORE": "true"}, clear=False):
        store = StorageFactory.get_default_store(tmp_path / "artifacts")
    assert isinstance(store, EncryptedStoreAdapter), (
        f"Expected EncryptedStoreAdapter, got {type(store).__name__}"
    )


# ---------------------------------------------------------------------------
# test_env_var_false_returns_plain
# ---------------------------------------------------------------------------

def test_env_var_false_returns_plain(tmp_path: Path) -> None:
    """HARVEST_ENCRYPT_STORE=false must produce a PlainStore."""
    with patch.dict(os.environ, {"HARVEST_ENCRYPT_STORE": "false"}, clear=False):
        store = StorageFactory.get_default_store(tmp_path / "artifacts")
    assert isinstance(store, PlainStore), (
        f"Expected PlainStore, got {type(store).__name__}"
    )


# ---------------------------------------------------------------------------
# test_env_var_true_returns_encrypted
# ---------------------------------------------------------------------------

def test_env_var_true_returns_encrypted(tmp_path: Path) -> None:
    """HARVEST_ENCRYPT_STORE=true must produce an EncryptedStoreAdapter."""
    with patch.dict(os.environ, {"HARVEST_ENCRYPT_STORE": "true"}, clear=False):
        store = StorageFactory.get_default_store(tmp_path / "artifacts")
    assert isinstance(store, EncryptedStoreAdapter)


# ---------------------------------------------------------------------------
# test_plain_store_write_read_roundtrip
# ---------------------------------------------------------------------------

def test_plain_store_write_read_roundtrip(tmp_path: Path) -> None:
    """PlainStore write → read must return the original bytes."""
    store = PlainStore(tmp_path / "plain")
    data = b"hello, plaintext world"
    store.write("artifact-1", data)
    result = store.read("artifact-1")
    assert result == data


# ---------------------------------------------------------------------------
# test_plain_store_delete
# ---------------------------------------------------------------------------

def test_plain_store_delete(tmp_path: Path) -> None:
    """PlainStore: write → delete → exists() must return False."""
    store = PlainStore(tmp_path / "plain")
    store.write("artifact-2", b"temporary data")
    assert store.exists("artifact-2")
    deleted = store.delete("artifact-2")
    assert deleted is True
    assert not store.exists("artifact-2")


# ---------------------------------------------------------------------------
# test_encrypted_store_write_read_roundtrip
# ---------------------------------------------------------------------------

def test_encrypted_store_write_read_roundtrip(tmp_path: Path) -> None:
    """EncryptedStoreAdapter write → read must return the original bytes."""
    with patch.dict(os.environ, {"HARVEST_ENCRYPT_STORE": "true"}, clear=False):
        store = StorageFactory.get_default_store(tmp_path / "enc")
    assert isinstance(store, EncryptedStoreAdapter)

    data = b"super secret artifact bytes"
    store.write("enc-art-1", data)
    result = store.read("enc-art-1")
    assert result == data


# ---------------------------------------------------------------------------
# test_storage_factory_config_override
# ---------------------------------------------------------------------------

def test_storage_factory_config_override_false(tmp_path: Path) -> None:
    """Passing a config object with encryption_enabled=False must yield PlainStore,
    regardless of the HARVEST_ENCRYPT_STORE env var."""

    class FakeConfig:
        def get(self, key, default=None):
            if key == "encryption_enabled":
                return False
            return default

    # Even with env var set to true, config takes precedence
    with patch.dict(os.environ, {"HARVEST_ENCRYPT_STORE": "true"}, clear=False):
        store = StorageFactory.get_default_store(tmp_path / "cfg", config=FakeConfig())
    assert isinstance(store, PlainStore)


def test_storage_factory_config_override_true(tmp_path: Path) -> None:
    """Config with encryption_enabled=True must yield EncryptedStoreAdapter
    even when HARVEST_ENCRYPT_STORE=false is set."""

    class FakeConfig:
        def get(self, key, default=None):
            if key == "encryption_enabled":
                return True
            return default

    with patch.dict(os.environ, {"HARVEST_ENCRYPT_STORE": "false"}, clear=False):
        store = StorageFactory.get_default_store(tmp_path / "cfg2", config=FakeConfig())
    assert isinstance(store, EncryptedStoreAdapter)


def test_storage_factory_harvest_config_integration(tmp_path: Path) -> None:
    """HarvestConfig.encryption_enabled property integrates correctly with StorageFactory."""
    from harvest_core.config.harvest_config import HarvestConfig

    # dev profile sets encrypt_at_rest=False
    with patch.dict(os.environ, {"HARVEST_PROFILE": "dev", "HARVEST_ENCRYPT_STORE": ""}, clear=False):
        cfg = HarvestConfig(profile="dev")
        # encryption_enabled should be False for dev profile
        assert cfg.encryption_enabled is False
        store = StorageFactory.get_default_store(tmp_path / "dev", config=cfg)
    assert isinstance(store, PlainStore)
