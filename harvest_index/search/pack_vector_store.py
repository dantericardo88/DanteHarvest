"""
PackVectorStore — semantic search over the Harvest pack corpus.

Harvested from: Qdrant Python client patterns (qdrant-client).

Three modes (in priority order):
1. Dense embeddings (default when sentence-transformers is cached):
   EmbeddingEngine produces 384-dim vectors; cosine similarity search.
2. Local TF-IDF (fallback when EmbeddingEngine is unavailable):
   Pure-Python cosine similarity over TF-IDF bag-of-words vectors.
3. Qdrant server: connects to a running Qdrant instance.

Constitutional guarantees:
- Local-first: no remote Qdrant required by default
- Fail-closed: missing embedding model raises StorageError (not silent empty)
- Zero-ambiguity: query on empty corpus returns [], not None
- Graceful degradation: automatically falls back TF-IDF when embeddings absent
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from harvest_core.control.exceptions import StorageError
from harvest_index.search.embedding_engine import EmbeddingEngine


@dataclass
class VectorSearchResult:
    pack_id: str
    score: float
    pack_type: str
    title: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class _LocalTFIDFIndex:
    """
    Minimal in-process TF-IDF vector index.
    Sufficient for corpora up to ~10k packs; swap for Qdrant at larger scale.
    """

    def __init__(self):
        self._docs: Dict[str, Dict[str, Any]] = {}  # pack_id → {text, meta, tf}
        self._df: Counter = Counter()  # term → document frequency
        self._n: int = 0

    def upsert(self, pack_id: str, text: str, metadata: Dict[str, Any]) -> None:
        terms = self._tokenize(text)
        tf = Counter(terms)
        total = max(len(terms), 1)
        tf_norm = {t: c / total for t, c in tf.items()}

        if pack_id in self._docs:
            # Remove old df contributions
            for term in self._docs[pack_id]["tf"]:
                self._df[term] -= 1
                if self._df[term] <= 0:
                    del self._df[term]
            self._n -= 1

        self._docs[pack_id] = {"text": text, "meta": metadata, "tf": tf_norm}
        for term in tf_norm:
            self._df[term] += 1
        self._n += 1

    def query(
        self,
        text: str,
        limit: int = 5,
        filter_by_type: Optional[str] = None,
    ) -> List[VectorSearchResult]:
        if not self._docs:
            return []

        q_terms = self._tokenize(text)
        q_tf = Counter(q_terms)
        q_total = max(len(q_terms), 1)

        scores: List[tuple] = []
        for pack_id, doc in self._docs.items():
            meta = doc["meta"]
            if filter_by_type and meta.get("pack_type") != filter_by_type:
                continue

            score = 0.0
            for term, qf in q_tf.items():
                if term not in doc["tf"]:
                    continue
                tf = doc["tf"][term]
                df = self._df.get(term, 1)
                idf = math.log((self._n + 1) / (df + 1)) + 1.0
                score += (qf / q_total) * tf * idf

            if score > 0:
                scores.append((score, pack_id, meta))

        scores.sort(reverse=True)
        return [
            VectorSearchResult(
                pack_id=pack_id,
                score=round(score, 6),
                pack_type=meta.get("pack_type", ""),
                title=meta.get("title", ""),
                metadata=meta,
            )
            for score, pack_id, meta in scores[:limit]
        ]

    def delete(self, pack_id: str) -> None:
        if pack_id not in self._docs:
            return
        for term in self._docs[pack_id]["tf"]:
            self._df[term] -= 1
            if self._df[term] <= 0:
                del self._df[term]
        del self._docs[pack_id]
        self._n -= 1

    def __len__(self) -> int:
        return self._n

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return re.findall(r"[a-z0-9]+", text.lower())


class _LocalDenseIndex:
    """
    In-process dense vector index backed by EmbeddingEngine.
    Cosine similarity search over 384-dim sentence-transformer vectors.
    """

    def __init__(self, engine: EmbeddingEngine):
        self._engine = engine
        self._vectors: Dict[str, List[float]] = {}  # pack_id → embedding
        self._meta: Dict[str, Dict[str, Any]] = {}   # pack_id → metadata

    def upsert(self, pack_id: str, text: str, metadata: Dict[str, Any]) -> None:
        self._vectors[pack_id] = self._engine.embed(text)
        self._meta[pack_id] = metadata

    def query(
        self,
        text: str,
        limit: int = 5,
        filter_by_type: Optional[str] = None,
    ) -> List[VectorSearchResult]:
        if not self._vectors:
            return []
        q_vec = self._engine.embed(text)
        scores: List[tuple] = []
        for pack_id, vec in self._vectors.items():
            meta = self._meta.get(pack_id, {})
            if filter_by_type and meta.get("pack_type") != filter_by_type:
                continue
            score = _cosine(q_vec, vec)
            if score > 0:
                scores.append((score, pack_id, meta))
        scores.sort(reverse=True)
        return [
            VectorSearchResult(
                pack_id=pack_id,
                score=round(score, 6),
                pack_type=meta.get("pack_type", ""),
                title=meta.get("title", ""),
                metadata=meta,
            )
            for score, pack_id, meta in scores[:limit]
        ]

    def delete(self, pack_id: str) -> None:
        self._vectors.pop(pack_id, None)
        self._meta.pop(pack_id, None)

    def __len__(self) -> int:
        return len(self._vectors)


def _cosine(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class PackVectorStore:
    """
    Semantic search over the Harvest pack corpus.

    Usage (local-first, no server required):
        store = PackVectorStore()
        store.upsert("pack-001", "submit invoice to accounting", metadata={...})
        results = store.query("invoice workflow", limit=5)

    Usage (Qdrant server):
        store = PackVectorStore(qdrant_url="http://localhost:6333")
        store.upsert(...)

    Both modes expose the same API.
    """

    def __init__(
        self,
        qdrant_url: Optional[str] = None,
        collection_name: str = "harvest_packs",
        persist_path: Optional[str] = None,
        embedding_engine: Optional[EmbeddingEngine] = None,
        use_embeddings: bool = True,
    ):
        self._qdrant_url = qdrant_url
        self._collection_name = collection_name
        self._persist_path = Path(persist_path) if persist_path else None
        self._qdrant_client = None
        self._local_index: Optional[_LocalTFIDFIndex] = None
        self._dense_index: Optional[_LocalDenseIndex] = None

        if qdrant_url:
            self._init_qdrant()
        else:
            if use_embeddings:
                engine = embedding_engine or EmbeddingEngine()
                if engine.is_available():
                    self._dense_index = _LocalDenseIndex(engine)
            if self._dense_index is None:
                self._local_index = _LocalTFIDFIndex()
            if self._persist_path:
                self._load_persisted()

    def upsert(
        self,
        pack_id: str,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Index a pack by text.  Overwrites existing entry for the same pack_id."""
        meta = metadata or {}
        if self._dense_index is not None:
            self._dense_index.upsert(pack_id, text, meta)
        elif self._local_index is not None:
            self._local_index.upsert(pack_id, text, meta)
            if self._persist_path:
                self._persist()
        else:
            self._qdrant_upsert(pack_id, text, meta)

    def query(
        self,
        text: str,
        limit: int = 5,
        filter_by_type: Optional[str] = None,
        mode: str = "auto",
        hybrid_alpha: float = 0.4,
    ) -> List[VectorSearchResult]:
        """
        Return up to `limit` packs semantically similar to `text`.
        Returns [] on empty corpus — never raises for valid input.

        mode:
          "auto"   — dense if available, tfidf fallback (default)
          "dense"  — dense embeddings only (requires EmbeddingEngine)
          "tfidf"  — BM25/TF-IDF only
          "hybrid" — weighted sum: alpha*tfidf + (1-alpha)*dense
        hybrid_alpha: weight for tfidf score in hybrid mode (0.0–1.0)
        """
        if not text:
            return []
        if mode == "hybrid":
            return self._query_hybrid(text, limit=limit, filter_by_type=filter_by_type, alpha=hybrid_alpha)
        if mode == "dense" or (mode == "auto" and self._dense_index is not None):
            if self._dense_index is not None:
                return self._dense_index.query(text, limit=limit, filter_by_type=filter_by_type)
        if mode == "tfidf" or (mode == "auto" and self._local_index is not None):
            if self._local_index is not None:
                return self._local_index.query(text, limit=limit, filter_by_type=filter_by_type)
        return self._qdrant_query(text, limit=limit, filter_by_type=filter_by_type)

    def _query_hybrid(
        self,
        text: str,
        limit: int,
        filter_by_type: Optional[str],
        alpha: float,
    ) -> List[VectorSearchResult]:
        """Weighted combination of TF-IDF and dense scores."""
        tfidf_results: List[VectorSearchResult] = []
        dense_results: List[VectorSearchResult] = []

        if self._local_index is not None:
            tfidf_results = self._local_index.query(text, limit=limit * 4, filter_by_type=filter_by_type)
        if self._dense_index is not None:
            dense_results = self._dense_index.query(text, limit=limit * 4, filter_by_type=filter_by_type)

        if not tfidf_results and not dense_results:
            return []
        if not dense_results:
            return tfidf_results[:limit]
        if not tfidf_results:
            return dense_results[:limit]

        # Normalise scores to [0, 1] then combine
        tfidf_max = max(r.score for r in tfidf_results) or 1.0
        dense_max = max(r.score for r in dense_results) or 1.0
        tfidf_map = {r.pack_id: r.score / tfidf_max for r in tfidf_results}
        dense_map = {r.pack_id: r.score / dense_max for r in dense_results}

        all_ids = set(tfidf_map) | set(dense_map)
        combined: List[tuple] = []
        # Keep a reference result for metadata
        meta_ref = {r.pack_id: r for r in tfidf_results + dense_results}
        for pid in all_ids:
            score = alpha * tfidf_map.get(pid, 0.0) + (1 - alpha) * dense_map.get(pid, 0.0)
            ref = meta_ref[pid]
            combined.append((score, pid, ref.pack_type, ref.title, ref.metadata))

        combined.sort(reverse=True)
        return [
            VectorSearchResult(
                pack_id=pid,
                score=round(score, 6),
                pack_type=ptype,
                title=title,
                metadata=meta,
            )
            for score, pid, ptype, title, meta in combined[:limit]
        ]

    def rerank(
        self,
        results: List[VectorSearchResult],
        query: str,
        model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2",
        top_k: Optional[int] = None,
    ) -> List[VectorSearchResult]:
        """
        Optional cross-encoder reranking using sentence-transformers.
        Falls back to original order if sentence-transformers is not installed.
        """
        if not results:
            return results
        try:
            from sentence_transformers import CrossEncoder
            ce = CrossEncoder(model)
            pairs = [(query, r.title + " " + r.metadata.get("goal", "")) for r in results]
            scores = ce.predict(pairs)
            ranked = sorted(zip(scores, results), key=lambda x: x[0], reverse=True)
            reranked = [r for _, r in ranked]
            return reranked[:top_k] if top_k else reranked
        except ImportError:
            return results[:top_k] if top_k else results

    def delete(self, pack_id: str) -> None:
        if self._dense_index is not None:
            self._dense_index.delete(pack_id)
        elif self._local_index is not None:
            self._local_index.delete(pack_id)
            if self._persist_path:
                self._persist()
        elif self._qdrant_client:
            from qdrant_client.models import PointIdsList
            self._qdrant_client.delete(
                collection_name=self._collection_name,
                points_selector=PointIdsList(points=[pack_id]),
            )

    def __len__(self) -> int:
        if self._dense_index is not None:
            return len(self._dense_index)
        if self._local_index is not None:
            return len(self._local_index)
        return 0

    @property
    def index_type(self) -> str:
        """Returns 'dense', 'tfidf', or 'qdrant' — for observability."""
        if self._dense_index is not None:
            return "dense"
        if self._local_index is not None:
            return "tfidf"
        return "qdrant"

    # ------------------------------------------------------------------
    # Qdrant backend
    # ------------------------------------------------------------------

    def _init_qdrant(self) -> None:
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.models import Distance, VectorParams
            self._qdrant_client = QdrantClient(url=self._qdrant_url)
            # Ensure collection exists
            collections = [c.name for c in self._qdrant_client.get_collections().collections]
            if self._collection_name not in collections:
                self._qdrant_client.create_collection(
                    collection_name=self._collection_name,
                    vectors_config=VectorParams(size=384, distance=Distance.COSINE),
                )
        except ImportError as e:
            raise StorageError(
                "qdrant-client not installed. Run: pip install qdrant-client"
            ) from e

    def _embed(self, text: str) -> List[float]:
        try:
            from sentence_transformers import SentenceTransformer
            if not hasattr(self, "_model"):
                self._model = SentenceTransformer("all-MiniLM-L6-v2")
            return self._model.encode(text).tolist()
        except ImportError as e:
            raise StorageError(
                "sentence-transformers not installed. Run: pip install sentence-transformers"
            ) from e

    def _qdrant_upsert(self, pack_id: str, text: str, meta: dict) -> None:
        from qdrant_client.models import PointStruct
        vector = self._embed(text)
        self._qdrant_client.upsert(
            collection_name=self._collection_name,
            points=[PointStruct(id=_stable_id(pack_id), vector=vector, payload={**meta, "pack_id": pack_id})],
        )

    def _qdrant_query(self, text: str, limit: int, filter_by_type: Optional[str]) -> List[VectorSearchResult]:
        from qdrant_client.models import Filter, FieldCondition, MatchValue
        vector = self._embed(text)
        query_filter = None
        if filter_by_type:
            query_filter = Filter(must=[FieldCondition(key="pack_type", match=MatchValue(value=filter_by_type))])
        hits = self._qdrant_client.query_points(
            collection_name=self._collection_name,
            query=vector,
            limit=limit,
            query_filter=query_filter,
        ).points
        return [
            VectorSearchResult(
                pack_id=h.payload.get("pack_id", str(h.id)),
                score=h.score,
                pack_type=h.payload.get("pack_type", ""),
                title=h.payload.get("title", ""),
                metadata=h.payload,
            )
            for h in hits
        ]

    # ------------------------------------------------------------------
    # Persistence for local index
    # ------------------------------------------------------------------

    def _persist(self) -> None:
        if not self._persist_path or self._local_index is None:
            return
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            pack_id: {"text": doc["text"], "meta": doc["meta"]}
            for pack_id, doc in self._local_index._docs.items()
        }
        tmp = self._persist_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self._persist_path)

    def _load_persisted(self) -> None:
        if not self._persist_path or not self._persist_path.exists():
            return
        try:
            data = json.loads(self._persist_path.read_text(encoding="utf-8"))
            for pack_id, entry in data.items():
                self._local_index.upsert(pack_id, entry["text"], entry["meta"])
        except Exception:
            pass  # corrupt persisted index → start fresh


def _stable_id(pack_id: str) -> int:
    """Convert UUID string to a stable integer for Qdrant point IDs."""
    return int(hashlib.md5(pack_id.encode()).hexdigest()[:16], 16)
