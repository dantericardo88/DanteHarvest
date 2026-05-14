"""
FrameSearchIndex — TF-IDF search index over OCR'd screen history.

Harvested from: Screenpipe semantic search + OpenAdapt activity-search patterns.

Indexes OcrFrame text with TF-IDF and supports ranked full-text search over
captured screen history.  Uses scikit-learn's TfidfVectorizer when available,
falls back to a pure-Python cosine-over-bag-of-words implementation so the
module is usable in minimal CI environments.

Constitutional guarantees:
- Local-first: all frame data already on disk; this module never reads from
  or writes to the network.
- Fail-closed: importing scikit-learn is attempted lazily; absence is handled
  gracefully with an in-process fallback, never a silent no-op.
- Deterministic ordering: results are sorted by descending score, then by
  frame_index for stable tie-breaking.
"""

from __future__ import annotations

import logging
import math
import re
from collections import Counter
from typing import Dict, List, Optional, Tuple

from harvest_observe.ocr.frame_ocr_pipeline import OcrFrame

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scikit-learn availability check
# ---------------------------------------------------------------------------

_SKLEARN_AVAILABLE: Optional[bool] = None


def _check_sklearn() -> bool:
    global _SKLEARN_AVAILABLE
    if _SKLEARN_AVAILABLE is None:
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: F401
            from sklearn.metrics.pairwise import cosine_similarity  # noqa: F401
            _SKLEARN_AVAILABLE = True
        except ImportError:
            _SKLEARN_AVAILABLE = False
    return _SKLEARN_AVAILABLE  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Pure-Python TF-IDF fallback
# ---------------------------------------------------------------------------


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


class _PurePythonTfidf:
    """Minimal TF-IDF implementation for CI environments without scikit-learn."""

    def __init__(self) -> None:
        self._docs: List[List[str]] = []
        self._idf: Dict[str, float] = {}

    def fit(self, texts: List[str]) -> None:
        self._docs = [_tokenize(t) for t in texts]
        n = len(self._docs)
        df: Counter = Counter()
        for doc in self._docs:
            df.update(set(doc))
        self._idf = {
            term: math.log((n + 1) / (count + 1)) + 1.0
            for term, count in df.items()
        }

    def _tfidf_vec(self, tokens: List[str]) -> Dict[str, float]:
        tf = Counter(tokens)
        total = len(tokens) or 1
        return {t: (count / total) * self._idf.get(t, 1.0) for t, count in tf.items()}

    def cosine(self, vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
        dot = sum(vec_a.get(t, 0.0) * v for t, v in vec_b.items())
        norm_a = math.sqrt(sum(v * v for v in vec_a.values())) or 1e-10
        norm_b = math.sqrt(sum(v * v for v in vec_b.values())) or 1e-10
        return dot / (norm_a * norm_b)

    def query(self, query_text: str) -> List[float]:
        q_tokens = _tokenize(query_text)
        q_vec = self._tfidf_vec(q_tokens)
        scores: List[float] = []
        for doc_tokens in self._docs:
            d_vec = self._tfidf_vec(doc_tokens)
            scores.append(self.cosine(q_vec, d_vec))
        return scores


# ---------------------------------------------------------------------------
# FrameSearchIndex
# ---------------------------------------------------------------------------


class FrameSearchIndex:
    """
    TF-IDF search index over a collection of OcrFrame objects.

    Usage::

        index = FrameSearchIndex()
        index.build(ocr_frames)
        results = index.search("error message")

    The index must be rebuilt whenever new frames are added via :meth:`add_frames`.
    """

    def __init__(self) -> None:
        self._frames: List[OcrFrame] = []
        self._fitted: bool = False
        # sklearn path
        self._vectorizer = None  # TfidfVectorizer instance
        self._matrix = None      # sparse matrix (n_docs × n_features)
        # fallback path
        self._fallback: Optional[_PurePythonTfidf] = None

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------

    def build(self, frames: List[OcrFrame]) -> None:
        """Build (or rebuild) the index from *frames*."""
        self._frames = list(frames)
        self._fitted = False
        self._fit()

    def add_frames(self, frames: List[OcrFrame]) -> None:
        """
        Add more frames and rebuild the index.

        Equivalent to ``build(existing + frames)`` — the full corpus is
        re-fitted to keep IDF weights accurate.
        """
        self._frames.extend(frames)
        self._fitted = False
        self._fit()

    def clear(self) -> None:
        """Remove all frames and reset the index."""
        self._frames = []
        self._fitted = False
        self._vectorizer = None
        self._matrix = None
        self._fallback = None

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(self, query: str, top_k: int = 10) -> List[OcrFrame]:
        """
        Return up to *top_k* OcrFrame objects ranked by TF-IDF cosine similarity.

        Returns an empty list if the index is empty or *query* is blank.
        """
        if not self._frames or not query.strip():
            return []
        if not self._fitted:
            self._fit()

        scores = self._score(query)
        ranked: List[Tuple[float, int, OcrFrame]] = sorted(
            zip(scores, range(len(self._frames)), self._frames),
            key=lambda t: (-t[0], t[1]),
        )
        return [frame for score, _, frame in ranked[:top_k] if score > 0.0]

    def search_with_scores(self, query: str, top_k: int = 10) -> List[Tuple[OcrFrame, float]]:
        """Like :meth:`search` but also returns the similarity score."""
        if not self._frames or not query.strip():
            return []
        if not self._fitted:
            self._fit()

        scores = self._score(query)
        ranked = sorted(
            zip(scores, range(len(self._frames)), self._frames),
            key=lambda t: (-t[0], t[1]),
        )
        return [(frame, score) for score, _, frame in ranked[:top_k] if score > 0.0]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _corpus(self) -> List[str]:
        return [f.text for f in self._frames]

    def _fit(self) -> None:
        corpus = self._corpus()
        if not corpus:
            return

        if _check_sklearn():
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                self._vectorizer = TfidfVectorizer(
                    strip_accents="unicode",
                    lowercase=True,
                    analyzer="word",
                    ngram_range=(1, 2),
                    min_df=1,
                    sublinear_tf=True,
                )
                self._matrix = self._vectorizer.fit_transform(corpus)
                self._fallback = None
                self._fitted = True
                log.debug("FrameSearchIndex fitted with sklearn (%d docs)", len(corpus))
                return
            except Exception as exc:  # pragma: no cover
                log.warning("sklearn TF-IDF failed (%s), using fallback.", exc)

        # Pure-Python fallback
        self._fallback = _PurePythonTfidf()
        self._fallback.fit(corpus)
        self._vectorizer = None
        self._matrix = None
        self._fitted = True
        log.debug("FrameSearchIndex fitted with pure-Python fallback (%d docs)", len(corpus))

    def _score(self, query: str) -> List[float]:
        if self._vectorizer is not None and self._matrix is not None:
            from sklearn.metrics.pairwise import cosine_similarity
            q_vec = self._vectorizer.transform([query])
            sims = cosine_similarity(q_vec, self._matrix).flatten()
            return list(sims)

        if self._fallback is not None:
            return self._fallback.query(query)

        return [0.0] * len(self._frames)
