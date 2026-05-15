"""Tests for harvest_core.crypto.key_manager."""
import pytest
from pathlib import Path
from unittest.mock import MagicMock


def test_artifact_kdf_returns_32_bytes():
    from harvest_core.crypto.key_manager import artifact_kdf
    key = artifact_kdf(b"\x00" * 32, "artifact-001")
    assert isinstance(key, bytes)
    assert len(key) == 32


def test_artifact_kdf_different_ids_produce_different_keys():
    from harvest_core.crypto.key_manager import artifact_kdf
    master = b"\xab" * 32
    k1 = artifact_kdf(master, "art-1")
    k2 = artifact_kdf(master, "art-2")
    assert k1 != k2


def test_artifact_kdf_same_inputs_deterministic():
    from harvest_core.crypto.key_manager import artifact_kdf
    master = b"\x01" * 32
    k1 = artifact_kdf(master, "stable-id")
    k2 = artifact_kdf(master, "stable-id")
    assert k1 == k2


def test_key_version_not_expired_by_default():
    from harvest_core.crypto.key_manager import KeyVersion
    import time
    kv = KeyVersion(version_id="v1", created_at=time.time())
    assert not kv.is_expired()


def test_key_version_expired_when_past_deadline():
    from harvest_core.crypto.key_manager import KeyVersion
    import time
    kv = KeyVersion(version_id="v1", created_at=time.time(), expires_at=time.time() - 1)
    assert kv.is_expired()


def test_key_manager_initialize(tmp_path):
    from harvest_core.crypto.key_manager import KeyManager
    km = KeyManager(storage_root=tmp_path)
    v = km.initialize("passphrase-abc")
    assert v.version_id
    assert km.active_version() is not None


def test_key_manager_initialize_idempotent(tmp_path):
    from harvest_core.crypto.key_manager import KeyManager
    km = KeyManager(storage_root=tmp_path)
    v1 = km.initialize("pass1")
    v2 = km.initialize("pass2")  # should return existing, not create new
    assert v1.version_id == v2.version_id


def test_key_manager_rotate_creates_new_version(tmp_path):
    from harvest_core.crypto.key_manager import KeyManager
    km = KeyManager(storage_root=tmp_path)
    km.initialize("old-pass")
    new_v = km.rotate("new-pass")
    assert km.active_version().version_id == new_v.version_id
    assert len(km.all_versions()) == 2


def test_key_manager_active_store_returns_encrypted_store(tmp_path):
    from harvest_core.crypto.key_manager import KeyManager
    from harvest_core.storage.encrypted_store import EncryptedStore
    km = KeyManager(storage_root=tmp_path)
    km.initialize("test-pass")
    store = km.active_store()
    assert isinstance(store, EncryptedStore)
    assert store.is_active


def test_key_manager_active_master_key_length(tmp_path):
    from harvest_core.crypto.key_manager import KeyManager
    km = KeyManager(storage_root=tmp_path)
    km.initialize("master-pass")
    key = km.active_master_key()
    assert isinstance(key, bytes)
    assert len(key) == 32


def test_key_rotator_no_artifacts(tmp_path):
    from harvest_core.crypto.key_manager import KeyRotator
    from harvest_core.storage.encrypted_store import EncryptedStore
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    old_store = EncryptedStore(passphrase="old")
    new_store = EncryptedStore(passphrase="new")
    rotator = KeyRotator(artifacts_dir=artifacts_dir, log_dir=tmp_path / "crypto")
    results = list(rotator.rotate(old_store, new_store))
    assert results == []


def test_key_rotator_rotates_artifact(tmp_path):
    from harvest_core.crypto.key_manager import KeyRotator
    from harvest_core.storage.encrypted_store import EncryptedStore

    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    old_store = EncryptedStore(passphrase="old-pass")
    new_store = EncryptedStore(passphrase="new-pass")

    # Write an encrypted artifact
    artifact_path = artifacts_dir / "test.dhex"
    old_store.write(artifact_path, b"hello artifact")

    rotator = KeyRotator(artifacts_dir=artifacts_dir, log_dir=tmp_path / "crypto")
    results = list(rotator.rotate(old_store, new_store))

    assert len(results) == 1
    assert results[0].success
    # Should be readable with new store
    data = new_store.read(artifact_path)
    assert data == b"hello artifact"


def test_key_rotator_idempotent(tmp_path):
    from harvest_core.crypto.key_manager import KeyRotator
    from harvest_core.storage.encrypted_store import EncryptedStore

    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    old_store = EncryptedStore(passphrase="old")
    new_store = EncryptedStore(passphrase="new")
    artifact_path = artifacts_dir / "art.dhex"
    old_store.write(artifact_path, b"data")

    rotator = KeyRotator(artifacts_dir=artifacts_dir, log_dir=tmp_path / "crypto")
    results1 = list(rotator.rotate(old_store, new_store))
    results2 = list(rotator.rotate(old_store, new_store))  # idempotent: already rotated

    assert len(results1) == 1
    assert len(results2) == 0  # skipped
