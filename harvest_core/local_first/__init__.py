"""Local-first storage module — offline capable with sync metadata and conflict resolution."""
from .local_store import LocalFirstStore, SyncMetadata

__all__ = ["LocalFirstStore", "SyncMetadata"]
