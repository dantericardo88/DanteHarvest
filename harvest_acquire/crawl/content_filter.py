"""
ContentFilter — BM25-scored content extraction replacing raw regex stripping.

Harvested from: crawl4ai (Apache-2.0) — BM25ContentFilter + PruningContentFilter patterns.

Two modes:
1. BM25Filter (preferred when a user query is known): scores HTML chunks against the
   harvest intent using BM25Okapi. Excludes boilerplate by class/id pattern first.
2. PruningFilter (fallback for blind crawls): uses tag-importance weights alone
   with a fixed threshold.

Constitutional guarantees:
- Local-first: pure-Python; rank-bm25 is optional (falls back to PruningFilter)
- Fail-closed: empty result returns [] not None
- Zero-ambiguity: every chunk is a plain str, never Optional[str]
"""

from __future__ import annotations

import re
from typing import List, Optional


# Boilerplate class/id patterns — any element matching is excluded before scoring
_NEGATIVE_PATTERNS = re.compile(
    r"nav|footer|header|sidebar|ads|comment|promo|advert|social|share|cookie|popup|modal",
    re.IGNORECASE,
)

# Tag importance weights for pruning mode
_TAG_WEIGHTS: dict[str, float] = {
    "article": 1.8,
    "main": 1.6,
    "section": 1.4,
    "h1": 1.6,
    "h2": 1.4,
    "h3": 1.3,
    "p": 1.2,
    "blockquote": 1.5,
    "code": 1.5,
    "pre": 1.5,
    "li": 1.0,
    "td": 0.9,
    "div": 0.7,
    "span": 0.5,
}

_TAG_PATTERN = re.compile(r"<(\w+)([^>]*)>(.*?)</\1>", re.DOTALL | re.IGNORECASE)
_ATTR_CLASS_ID = re.compile(r'(?:class|id)=["\']([^"\']*)["\']', re.IGNORECASE)
_STRIP_TAGS = re.compile(r"<[^>]+>")
_WHITESPACE = re.compile(r"\s+")


def _strip_boilerplate_tags(html: str) -> str:
    """Remove script, style, nav, footer, header blocks entirely."""
    for tag in ("script", "style", "noscript", "iframe"):
        html = re.sub(rf"<{tag}[^>]*>.*?</{tag}>", "", html, flags=re.DOTALL | re.IGNORECASE)
    return html


def _extract_chunks(html: str, min_len: int = 50) -> List[tuple[str, float]]:
    """
    Extract (text, base_weight) pairs from HTML.
    base_weight is from _TAG_WEIGHTS; elements matching _NEGATIVE_PATTERNS score 0.
    Chunks shorter than min_len chars are discarded.
    """
    html = _strip_boilerplate_tags(html)
    chunks: List[tuple[str, float]] = []

    for m in _TAG_PATTERN.finditer(html):
        tag = m.group(1).lower()
        attrs = m.group(2)
        inner = m.group(3)

        # Exclude boilerplate by class/id
        attrs_text = " ".join(_ATTR_CLASS_ID.findall(attrs))
        if _NEGATIVE_PATTERNS.search(attrs_text):
            continue

        text = _STRIP_TAGS.sub(" ", inner)
        text = _WHITESPACE.sub(" ", text).strip()
        if len(text) < min_len:
            continue

        weight = _TAG_WEIGHTS.get(tag, 0.6)
        chunks.append((text, weight))

    if not chunks:
        # Fallback: full text strip
        text = _STRIP_TAGS.sub(" ", html)
        text = _WHITESPACE.sub(" ", text).strip()
        if text:
            chunks.append((text, 1.0))

    return chunks


def _bm25_filter(
    chunks: List[tuple[str, float]],
    query: str,
    threshold: float = 1.0,
    use_stemming: bool = True,
) -> List[str]:
    """Score chunks with BM25Okapi × tag_weight and filter by threshold."""
    try:
        from rank_bm25 import BM25Okapi
    except ImportError:
        # Graceful fallback to pruning mode when rank-bm25 not installed
        return _prune_filter(chunks)

    tokenize = _make_tokenizer(use_stemming)
    texts = [c[0] for c in chunks]
    tokenized = [tokenize(t) for t in texts]
    bm25 = BM25Okapi(tokenized)
    q_tokens = tokenize(query)
    scores = bm25.get_scores(q_tokens)

    result = []
    for i, (text, tag_weight) in enumerate(chunks):
        final_score = scores[i] * tag_weight
        if final_score >= threshold:
            result.append(text)

    return result if result else [c[0] for c in chunks[:3]]  # always return ≥1 chunk


def _prune_filter(
    chunks: List[tuple[str, float]],
    threshold: float = 0.48,
) -> List[str]:
    """Tag-weight-only filter for queryless crawls."""
    result = [text for text, weight in chunks if weight >= threshold]
    return result if result else [c[0] for c in chunks[:3]]


def _make_tokenizer(use_stemming: bool):
    if use_stemming:
        try:
            from snowballstemmer import stemmer
            _stemmer = stemmer("english")

            def _tok(text: str) -> List[str]:
                words = re.findall(r"[a-z0-9]+", text.lower())
                return [_stemmer.stemWord(w) for w in words]

            return _tok
        except ImportError:
            pass

    def _tok_plain(text: str) -> List[str]:
        return re.findall(r"[a-z0-9]+", text.lower())

    return _tok_plain


def extract_content(
    html: str,
    user_query: Optional[str] = None,
    bm25_threshold: float = 1.0,
    pruning_threshold: float = 0.48,
    use_stemming: bool = True,
    min_chunk_len: int = 50,
    join: bool = True,
) -> str | List[str]:
    """
    Extract readable content from HTML.

    Args:
        html: Raw HTML string
        user_query: Harvest intent string (e.g., "invoice workflow steps").
                    When provided, enables BM25 scoring. When None, uses pruning.
        bm25_threshold: BM25×weight cutoff (lower = more content)
        pruning_threshold: Tag-weight cutoff for queryless mode
        use_stemming: Apply Snowball stemming to query and chunks
        min_chunk_len: Minimum chars per chunk
        join: If True, returns a single newline-joined string; else returns List[str]

    Returns:
        Cleaned text string (or list of strings if join=False)
    """
    if not html:
        return "" if join else []

    chunks = _extract_chunks(html, min_len=min_chunk_len)
    if not chunks:
        return "" if join else []

    if user_query:
        filtered = _bm25_filter(chunks, user_query, threshold=bm25_threshold, use_stemming=use_stemming)
    else:
        filtered = _prune_filter(chunks, threshold=pruning_threshold)

    if join:
        return "\n\n".join(filtered)
    return filtered
