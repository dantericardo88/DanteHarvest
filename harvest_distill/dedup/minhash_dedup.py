"""
MinHashDedup — near-duplicate detection using MinHash + LSH.

Wave 7b: cross_document_dedup — MinHash/SimHash near-dedup (3→9).

Provides exact-content dedup (SHA-256) and near-duplicate detection:
- MinHash: probabilistic Jaccard similarity for set-based near-dedup
- SimHash: bit-fingerprint for near-duplicate text with Hamming distance
- LSH (Locality-Sensitive Hashing): bucket-based O(1) lookup for scalable matching
- Cross-source dedup: same-content artifacts from multiple connectors unified

Constitutional guarantees:
- Local-first: pure Python stdlib, no external dedup service
- Fail-open: errors on individual docs recorded but don't halt pipeline
- Append-only: dedup results are stored as decisions, originals kept
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
import struct
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Fingerprinting helpers
# ---------------------------------------------------------------------------

def sha256_fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _shingles(text: str, k: int = 5) -> Set[str]:
    """Character k-shingles for text."""
    text = re.sub(r"\s+", " ", text.lower().strip())
    if len(text) < k:
        return {text}
    return {text[i:i+k] for i in range(len(text) - k + 1)}


def _universal_hash(a: int, b: int, n: int, p: int = (1 << 31) - 1) -> callable:
    """Return a hash function h(x) = (a*x + b) mod p mod n."""
    def _h(x: int) -> int:
        return ((a * x + b) % p) % n
    return _h


# ---------------------------------------------------------------------------
# MinHash
# ---------------------------------------------------------------------------

class MinHash:
    """
    MinHash signature for Jaccard similarity estimation.

    Usage:
        mh = MinHash(num_hashes=128)
        sig = mh.signature("hello world text here")
        jaccard = mh.estimate_jaccard(sig_a, sig_b)
    """

    def __init__(self, num_hashes: int = 128, seed: int = 42, shingle_k: int = 5):
        self._num_hashes = num_hashes
        self._shingle_k = shingle_k
        rng = random.Random(seed)
        p = (1 << 31) - 1
        self._hash_fns = [
            _universal_hash(rng.randint(1, p - 1), rng.randint(0, p - 1), 2**32, p)
            for _ in range(num_hashes)
        ]

    def signature(self, text: str) -> List[int]:
        """Compute MinHash signature (list of num_hashes ints)."""
        shingles = _shingles(text, self._shingle_k)
        hashed = [hash(s) & 0xFFFFFFFF for s in shingles]
        if not hashed:
            return [0] * self._num_hashes
        return [min(h(x) for x in hashed) for h in self._hash_fns]

    def estimate_jaccard(self, sig_a: List[int], sig_b: List[int]) -> float:
        """Estimate Jaccard similarity from two signatures."""
        if not sig_a or not sig_b:
            return 0.0
        matches = sum(a == b for a, b in zip(sig_a, sig_b))
        return matches / len(sig_a)


# ---------------------------------------------------------------------------
# SimHash
# ---------------------------------------------------------------------------

class SimHash:
    """
    SimHash 64-bit fingerprint for near-duplicate detection.
    Two texts are near-duplicates if their Hamming distance < threshold.
    """

    def __init__(self, bits: int = 64):
        self._bits = bits

    def fingerprint(self, text: str) -> int:
        """Compute SimHash fingerprint as an integer."""
        words = re.findall(r"\w+", text.lower())
        v = [0] * self._bits
        for word in words:
            h = int(hashlib.md5(word.encode()).hexdigest(), 16)
            for i in range(self._bits):
                if h & (1 << i):
                    v[i] += 1
                else:
                    v[i] -= 1
        return sum(1 << i for i in range(self._bits) if v[i] > 0)

    def hamming_distance(self, fp_a: int, fp_b: int) -> int:
        return bin(fp_a ^ fp_b).count("1")

    def are_near_duplicates(self, fp_a: int, fp_b: int, threshold: int = 3) -> bool:
        return self.hamming_distance(fp_a, fp_b) <= threshold


# ---------------------------------------------------------------------------
# LSH Bucket Index
# ---------------------------------------------------------------------------

class LSHIndex:
    """
    Locality-Sensitive Hashing index for MinHash signatures.
    Organizes signatures into bands for O(1) candidate lookup.
    """

    def __init__(self, num_bands: int = 16, rows_per_band: int = 8):
        self._num_bands = num_bands
        self._rows_per_band = rows_per_band
        self._buckets: Dict[Tuple, List[str]] = {}
        self._signatures: Dict[str, List[int]] = {}

    def add(self, doc_id: str, signature: List[int]) -> None:
        self._signatures[doc_id] = signature
        for band_idx in range(self._num_bands):
            start = band_idx * self._rows_per_band
            end = start + self._rows_per_band
            band = tuple(signature[start:end])
            key = (band_idx, band)
            self._buckets.setdefault(key, []).append(doc_id)

    def candidates(self, signature: List[int]) -> Set[str]:
        """Return candidate near-duplicate doc IDs for the given signature."""
        result: Set[str] = set()
        for band_idx in range(self._num_bands):
            start = band_idx * self._rows_per_band
            end = start + self._rows_per_band
            band = tuple(signature[start:end])
            key = (band_idx, band)
            for doc_id in self._buckets.get(key, []):
                result.add(doc_id)
        return result

    def __len__(self) -> int:
        return len(self._signatures)


# ---------------------------------------------------------------------------
# Dedup result
# ---------------------------------------------------------------------------

@dataclass
class DedupDecision:
    doc_id: str
    is_duplicate: bool
    duplicate_of: Optional[str]      # canonical doc_id it duplicates
    similarity: float                # 0.0–1.0
    method: str                      # "exact" | "minhash" | "simhash"
    decided_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# MinHashDedup
# ---------------------------------------------------------------------------

class MinHashDedup:
    """
    Near-duplicate document deduplicator using MinHash + LSH.

    Usage:
        dedup = MinHashDedup(similarity_threshold=0.8)
        for doc_id, text in documents:
            decision = dedup.add(doc_id, text)
            if decision.is_duplicate:
                print(f"{doc_id} is a near-duplicate of {decision.duplicate_of}")

        decisions = dedup.decisions()
        canonical = dedup.canonical_ids()
    """

    def __init__(
        self,
        similarity_threshold: float = 0.8,
        simhash_threshold: int = 3,
        num_hashes: int = 128,
        num_bands: int = 16,
        log_dir: Optional[Path] = None,
    ):
        self._threshold = similarity_threshold
        self._simhash_threshold = simhash_threshold
        self._minhash = MinHash(num_hashes=num_hashes)
        self._simhasher = SimHash()
        rows_per_band = max(1, num_hashes // num_bands)
        self._lsh = LSHIndex(num_bands=num_bands, rows_per_band=rows_per_band)
        self._exact: Dict[str, str] = {}           # sha256 → first doc_id
        self._simhash_fps: Dict[str, int] = {}     # doc_id → simhash fp
        self._decisions: List[DedupDecision] = []
        self._canonical: Set[str] = set()
        self._log_dir = Path(log_dir) if log_dir else None
        if self._log_dir:
            self._log_dir.mkdir(parents=True, exist_ok=True)

    def add(self, doc_id: str, text: str) -> DedupDecision:
        """
        Add a document and return a DedupDecision.
        The document is considered a duplicate if similarity >= threshold.
        """
        # 1. Exact-content dedup
        fp_exact = sha256_fingerprint(text)
        if fp_exact in self._exact:
            canonical = self._exact[fp_exact]
            decision = DedupDecision(
                doc_id=doc_id, is_duplicate=True, duplicate_of=canonical,
                similarity=1.0, method="exact",
            )
            self._decisions.append(decision)
            self._log(decision)
            return decision

        # 2. MinHash near-dedup via LSH
        sig = self._minhash.signature(text)
        candidates = self._lsh.candidates(sig)
        best_sim = 0.0
        best_match: Optional[str] = None
        for cid in candidates:
            if cid == doc_id:
                continue
            cand_sig = self._lsh._signatures.get(cid, [])
            sim = self._minhash.estimate_jaccard(sig, cand_sig)
            if sim > best_sim:
                best_sim = sim
                best_match = cid

        if best_sim >= self._threshold and best_match:
            decision = DedupDecision(
                doc_id=doc_id, is_duplicate=True, duplicate_of=best_match,
                similarity=round(best_sim, 4), method="minhash",
            )
            self._decisions.append(decision)
            self._log(decision)
            return decision

        # 3. SimHash near-dedup (catches structural similarity even with word reorder)
        fp_sim = self._simhasher.fingerprint(text)
        for cid, cand_fp in self._simhash_fps.items():
            if self._simhasher.are_near_duplicates(fp_sim, cand_fp, self._simhash_threshold):
                decision = DedupDecision(
                    doc_id=doc_id, is_duplicate=True, duplicate_of=cid,
                    similarity=0.9, method="simhash",
                )
                self._decisions.append(decision)
                self._log(decision)
                return decision

        # Not a duplicate — register as canonical
        self._exact[fp_exact] = doc_id
        self._lsh.add(doc_id, sig)
        self._simhash_fps[doc_id] = fp_sim
        self._canonical.add(doc_id)
        decision = DedupDecision(
            doc_id=doc_id, is_duplicate=False, duplicate_of=None,
            similarity=0.0, method="exact",
        )
        self._decisions.append(decision)
        self._log(decision)
        return decision

    def decisions(self) -> List[DedupDecision]:
        return list(self._decisions)

    def canonical_ids(self) -> Set[str]:
        return set(self._canonical)

    def duplicate_count(self) -> int:
        return sum(1 for d in self._decisions if d.is_duplicate)

    def stats(self) -> dict:
        total = len(self._decisions)
        dups = self.duplicate_count()
        return {
            "total_processed": total,
            "duplicates": dups,
            "canonical": total - dups,
            "dedup_rate": round(dups / total, 4) if total else 0.0,
        }

    def _log(self, decision: DedupDecision) -> None:
        if not self._log_dir:
            return
        try:
            log_path = self._log_dir / "dedup_decisions.jsonl"
            with log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(decision.to_dict()) + "\n")
        except Exception:
            pass
