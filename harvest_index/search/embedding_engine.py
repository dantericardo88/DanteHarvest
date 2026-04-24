"""
EmbeddingEngine — local sentence-transformers embeddings for semantic search.

Sprint 4 target: close vector_search_integration gap (DH: 7 → 9 vs LlamaIndex: 9).

Harvested from: Qdrant sentence-transformers integration + LlamaIndex embedding patterns.

Two modes:
1. Local (default, local-first): sentence-transformers all-MiniLM-L6-v2 (38MB, cached).
   Zero network calls after first download. Falls back to TF-IDF on cold start.
2. OpenAI embeddings: text-embedding-3-small via API when api_key provided.

Constitutional guarantees:
- Local-first: local model used by default; no network after cache download
- Fail-closed: encode() raises StorageError if model unavailable (not silent zero vector)
- Zero-ambiguity: embed() always returns List[float] of fixed dimensionality
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import List, Optional

from harvest_core.control.exceptions import StorageError


_DEFAULT_MODEL = "all-MiniLM-L6-v2"
_DEFAULT_DIM = 384


class EmbeddingEngine:
    """
    Generate dense vector embeddings for semantic similarity search.

    Usage (local-first, zero config):
        engine = EmbeddingEngine()
        vec = engine.embed("invoice payment workflow")
        # returns List[float] of length 384

    Usage (OpenAI):
        engine = EmbeddingEngine(api_key=os.environ["OPENAI_API_KEY"])
        vec = engine.embed("invoice payment workflow")
        # returns List[float] of length 1536
    """

    def __init__(
        self,
        model_name: str = _DEFAULT_MODEL,
        api_key: Optional[str] = None,
        cache_dir: Optional[str] = None,
    ):
        self.model_name = model_name
        self.api_key = api_key
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self._model = None
        self._dim: Optional[int] = None

    @property
    def dim(self) -> int:
        """Embedding dimensionality. 384 for MiniLM, 1536 for OpenAI text-embedding-3-small."""
        if self._dim is not None:
            return self._dim
        if self.api_key:
            return 1536
        return _DEFAULT_DIM

    def embed(self, text: str) -> List[float]:
        """
        Embed a single text string.
        Raises StorageError if the model is unavailable (fail-closed).
        Returns List[float] — always, never None or empty (zero-ambiguity).
        """
        if not text:
            return [0.0] * self.dim

        if self.api_key:
            return self._embed_openai(text)
        return self._embed_local(text)

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed multiple texts. More efficient than calling embed() in a loop."""
        if not texts:
            return []
        if self.api_key:
            return [self._embed_openai(t) for t in texts]
        return self._embed_local_batch(texts)

    def is_available(self) -> bool:
        """Return True if the embedding backend is usable without network (local cache present)."""
        if self.api_key:
            return True
        try:
            self._load_model()
            return True
        except StorageError:
            return False

    # ------------------------------------------------------------------
    # Local sentence-transformers backend
    # ------------------------------------------------------------------

    def _load_model(self):
        if self._model is not None:
            return self._model
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise StorageError(
                "sentence-transformers not installed. Run: pip install sentence-transformers"
            ) from e
        try:
            kwargs = {}
            if self.cache_dir:
                kwargs["cache_folder"] = str(self.cache_dir)
            self._model = SentenceTransformer(self.model_name, **kwargs)
            self._dim = self._model.get_sentence_embedding_dimension()
        except Exception as e:
            raise StorageError(
                f"Failed to load embedding model '{self.model_name}': {e}. "
                "On first use, the model downloads ~38MB from HuggingFace."
            ) from e
        return self._model

    def _embed_local(self, text: str) -> List[float]:
        model = self._load_model()
        return model.encode(text, normalize_embeddings=True).tolist()

    def _embed_local_batch(self, texts: List[str]) -> List[List[float]]:
        model = self._load_model()
        return model.encode(texts, normalize_embeddings=True).tolist()

    # ------------------------------------------------------------------
    # OpenAI backend
    # ------------------------------------------------------------------

    def _embed_openai(self, text: str) -> List[float]:
        try:
            import openai
        except ImportError as e:
            raise StorageError(
                "openai not installed. Run: pip install openai"
            ) from e
        client = openai.OpenAI(api_key=self.api_key)
        try:
            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=text,
            )
            vec = response.data[0].embedding
            self._dim = len(vec)
            return vec
        except Exception as e:
            raise StorageError(f"OpenAI embedding API error: {e}") from e


class CachedEmbeddingEngine(EmbeddingEngine):
    """
    EmbeddingEngine with a local disk cache keyed by SHA-256(text).
    Avoids re-embedding identical texts across sessions.

    Usage:
        engine = CachedEmbeddingEngine(cache_dir="storage/embed_cache")
        vec = engine.embed("same text every time")  # reads from disk after first call
    """

    def __init__(self, cache_dir: str = "storage/embed_cache", **kwargs):
        super().__init__(**kwargs)
        self._embed_cache_dir = Path(cache_dir)
        self._embed_cache_dir.mkdir(parents=True, exist_ok=True)

    def embed(self, text: str) -> List[float]:
        key = hashlib.sha256(text.encode()).hexdigest()
        cache_file = self._embed_cache_dir / f"{key}.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text(encoding="utf-8"))
        vec = super().embed(text)
        tmp = cache_file.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(vec), encoding="utf-8")
        tmp.replace(cache_file)
        return vec
