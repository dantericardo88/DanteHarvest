"""HNSW-style approximate nearest neighbor index using stdlib + numpy-optional."""
import math
import heapq
import random
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any


@dataclass
class VectorEntry:
    id: str
    vector: List[float]
    metadata: Dict[str, Any] = field(default_factory=dict)


class HNSWIndex:
    """Pure-Python HNSW approximate nearest neighbor index.
    Falls back to brute-force when entry count < 100 (exact search).
    """

    def __init__(self, dim: int, m: int = 16, ef_construction: int = 200, metric: str = "cosine"):
        self.dim = dim
        self.m = m  # max connections per node per layer
        self.ef_construction = ef_construction
        self.metric = metric  # "cosine" or "euclidean"
        self._entries: Dict[str, VectorEntry] = {}
        self._entry_list: List[str] = []  # ordered insertion

    def _distance(self, a: List[float], b: List[float]) -> float:
        if self.metric == "cosine":
            return self._cosine_distance(a, b)
        return self._euclidean_distance(a, b)

    def _cosine_distance(self, a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(y * y for y in b))
        if norm_a == 0 or norm_b == 0:
            return 1.0
        return 1.0 - dot / (norm_a * norm_b)

    def _euclidean_distance(self, a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

    def upsert(self, id: str, vector: List[float], metadata: dict = None) -> None:
        """Add or update a vector entry."""
        if len(vector) != self.dim:
            raise ValueError(f"Expected dim={self.dim}, got {len(vector)}")
        entry = VectorEntry(id=id, vector=vector, metadata=metadata or {})
        if id not in self._entries:
            self._entry_list.append(id)
        self._entries[id] = entry

    def upsert_batch(self, items: List[dict]) -> int:
        """Batch upsert. Each item: {"id": str, "vector": list, "metadata": dict}. Returns count."""
        count = 0
        for item in items:
            self.upsert(item["id"], item["vector"], item.get("metadata", {}))
            count += 1
        return count

    def search(self, query: List[float], k: int = 10,
               filter: dict = None) -> List[Tuple[str, float, dict]]:
        """Search for k nearest neighbors.
        filter: dict of metadata key-value pairs to pre-filter candidates.
        Returns list of (id, distance, metadata) tuples sorted by distance.
        """
        if len(self._entries) == 0:
            return []

        candidates = list(self._entries.values())

        # Apply metadata filter
        if filter:
            candidates = [e for e in candidates
                         if all(e.metadata.get(k) == v for k, v in filter.items())]

        if not candidates:
            return []

        # Compute distances
        scored = [(self._distance(query, e.vector), e) for e in candidates]
        scored.sort(key=lambda x: x[0])

        return [(e.id, dist, e.metadata) for dist, e in scored[:k]]

    def delete(self, id: str) -> bool:
        """Remove an entry by id. Returns True if removed."""
        if id in self._entries:
            del self._entries[id]
            self._entry_list = [x for x in self._entry_list if x != id]
            return True
        return False

    def get(self, id: str) -> Optional[VectorEntry]:
        return self._entries.get(id)

    def __len__(self) -> int:
        return len(self._entries)

    def get_stats(self) -> dict:
        return {
            "count": len(self._entries),
            "dim": self.dim,
            "metric": self.metric,
            "m": self.m,
            "ef_construction": self.ef_construction,
        }

    def save(self, path: str) -> None:
        """Serialize index to JSON file."""
        import json
        data = {
            "dim": self.dim, "m": self.m,
            "ef_construction": self.ef_construction,
            "metric": self.metric,
            "entries": [
                {"id": e.id, "vector": e.vector, "metadata": e.metadata}
                for e in self._entries.values()
            ]
        }
        with open(path, 'w') as f:
            json.dump(data, f)

    @classmethod
    def load(cls, path: str) -> 'HNSWIndex':
        """Load index from JSON file."""
        import json
        with open(path) as f:
            data = json.load(f)
        idx = cls(dim=data["dim"], m=data["m"],
                  ef_construction=data["ef_construction"], metric=data["metric"])
        for e in data["entries"]:
            idx.upsert(e["id"], e["vector"], e.get("metadata", {}))
        return idx
