"""Local-first data store — offline capable with sync metadata and conflict resolution."""
import os
import json
import time
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any


class SyncMetadata:
    """Tracks sync state for local-first operation."""

    def __init__(self, store_path: str):
        self.store_path = Path(store_path)
        self._meta_path = self.store_path / ".sync_metadata.json"
        self._meta: dict = self._load()

    def _load(self) -> dict:
        if self._meta_path.exists():
            try:
                return json.loads(self._meta_path.read_text())
            except Exception:
                pass
        return {"last_sync": None, "pending_uploads": [], "pending_downloads": [], "conflicts": []}

    def _save(self) -> None:
        self._meta_path.parent.mkdir(parents=True, exist_ok=True)
        self._meta_path.write_text(json.dumps(self._meta, indent=2))

    def mark_pending_upload(self, artifact_id: str) -> None:
        if artifact_id not in self._meta["pending_uploads"]:
            self._meta["pending_uploads"].append(artifact_id)
        self._save()

    def mark_synced(self, artifact_id: str) -> None:
        self._meta["pending_uploads"] = [x for x in self._meta["pending_uploads"] if x != artifact_id]
        self._meta["last_sync"] = time.time()
        self._save()

    def record_conflict(self, artifact_id: str, local_hash: str, remote_hash: str) -> None:
        self._meta["conflicts"].append({
            "artifact_id": artifact_id,
            "local_hash": local_hash,
            "remote_hash": remote_hash,
            "detected_at": time.time(),
            "resolved": False,
        })
        self._save()

    def get_pending_uploads(self) -> List[str]:
        return list(self._meta["pending_uploads"])

    def get_conflicts(self) -> List[dict]:
        return [c for c in self._meta["conflicts"] if not c["resolved"]]

    def resolve_conflict(self, artifact_id: str, resolution: str = "local") -> None:
        """Resolve conflict. resolution: 'local' (keep local) or 'remote' (use remote)."""
        for c in self._meta["conflicts"]:
            if c["artifact_id"] == artifact_id and not c["resolved"]:
                c["resolved"] = True
                c["resolution"] = resolution
                c["resolved_at"] = time.time()
        self._save()

    def get_sync_status(self) -> dict:
        return {
            "last_sync": self._meta["last_sync"],
            "pending_uploads": len(self._meta["pending_uploads"]),
            "unresolved_conflicts": len(self.get_conflicts()),
            "is_synced": len(self._meta["pending_uploads"]) == 0 and len(self.get_conflicts()) == 0,
        }


class LocalFirstStore:
    """Local-first artifact store. Works fully offline; syncs when connected."""

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.sync_meta = SyncMetadata(str(self.base_path))
        self._online = True

    def set_offline_mode(self, offline: bool = True) -> None:
        self._online = not offline

    def is_online(self) -> bool:
        return self._online

    def write(self, artifact_id: str, data: dict) -> str:
        """Write artifact locally. Marks as pending upload if online."""
        path = self._artifact_path(artifact_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(data, indent=2)
        path.write_text(content)
        artifact_hash = hashlib.sha256(content.encode()).hexdigest()
        self.sync_meta.mark_pending_upload(artifact_id)
        return artifact_hash

    def read(self, artifact_id: str) -> Optional[dict]:
        """Read artifact from local store (works offline)."""
        path = self._artifact_path(artifact_id)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except Exception:
            return None

    def exists(self, artifact_id: str) -> bool:
        return self._artifact_path(artifact_id).exists()

    def delete(self, artifact_id: str) -> bool:
        path = self._artifact_path(artifact_id)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_local(self) -> List[str]:
        """List all locally stored artifact IDs."""
        return [p.stem for p in self.base_path.glob("*.json") if not p.name.startswith('.')]

    def resolve_conflict(self, artifact_id: str, resolution: str = "local") -> dict:
        """Resolve a sync conflict. resolution: 'local' or 'remote'."""
        self.sync_meta.resolve_conflict(artifact_id, resolution)
        return {"resolved": True, "artifact_id": artifact_id, "resolution": resolution}

    def get_sync_status(self) -> dict:
        return self.sync_meta.get_sync_status()

    def _artifact_path(self, artifact_id: str) -> Path:
        safe_id = artifact_id.replace('/', '_').replace('\\', '_')
        return self.base_path / f"{safe_id}.json"
