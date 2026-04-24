"""
SimHash near-duplicate detection for Harvest artifacts.

Harvested from: Screenpipe SimHash deduplication architecture.

SimHash maps a text document to a 64-bit fingerprint such that
documents with similar content have fingerprints with low Hamming
distance.  Threshold ≤ 3 bits catches near-duplicates (typos, minor edits).

Zero-ambiguity: is_near_duplicate always returns bool, never raises
on valid string input.
Local-first: in-memory index, no external service required.
"""

from __future__ import annotations

import hashlib
import re
from typing import Dict, List, Optional, Set


def _token_hashes(text: str) -> List[int]:
    """Tokenize text and return per-token hash vectors."""
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    hashes = []
    for token in tokens:
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)
        hashes.append(h)
    return hashes


def simhash(text: str, bits: int = 64) -> int:
    """
    Compute a SimHash fingerprint for a text string.

    Returns a `bits`-bit integer fingerprint.
    Identical text → identical fingerprint.
    Near-identical text → fingerprint with low Hamming distance.
    """
    if not text:
        return 0

    v = [0] * bits
    token_hs = _token_hashes(text)

    if not token_hs:
        return 0

    for h in token_hs:
        for i in range(bits):
            if h & (1 << i):
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(bits):
        if v[i] > 0:
            fingerprint |= 1 << i
    return fingerprint


def hamming_distance(a: int, b: int, bits: int = 64) -> int:
    """
    Count the number of differing bits between two SimHash fingerprints.

    Identical fingerprints → 0.
    """
    xor = (a ^ b) & ((1 << bits) - 1)
    return bin(xor).count("1")


class DedupIndex:
    """
    In-memory near-duplicate index using SimHash fingerprints.

    Usage:
        index = DedupIndex(threshold=3)
        index.add("art-001", "hello world content")
        is_dup = index.is_near_duplicate("hello world content!")  # True
        is_dup = index.is_near_duplicate("completely different")  # False
    """

    def __init__(self, threshold: int = 3, bits: int = 64):
        self.threshold = threshold
        self.bits = bits
        self._entries: Dict[str, int] = {}  # artifact_id → fingerprint

    def add(self, artifact_id: str, text: str) -> int:
        """Compute and store fingerprint for artifact_id. Returns fingerprint."""
        fp = simhash(text, self.bits)
        self._entries[artifact_id] = fp
        return fp

    def is_near_duplicate(self, text: str, exclude_id: Optional[str] = None) -> bool:
        """
        Return True if text is a near-duplicate of any indexed document.
        Never raises on valid string input (zero-ambiguity).
        """
        fp = simhash(text, self.bits)
        for artifact_id, stored_fp in self._entries.items():
            if artifact_id == exclude_id:
                continue
            if hamming_distance(fp, stored_fp, self.bits) <= self.threshold:
                return True
        return False

    def find_near_duplicates(
        self, text: str, exclude_id: Optional[str] = None
    ) -> List[str]:
        """Return list of artifact_ids that are near-duplicates of text."""
        fp = simhash(text, self.bits)
        dups = []
        for artifact_id, stored_fp in self._entries.items():
            if artifact_id == exclude_id:
                continue
            if hamming_distance(fp, stored_fp, self.bits) <= self.threshold:
                dups.append(artifact_id)
        return dups

    def remove(self, artifact_id: str) -> None:
        self._entries.pop(artifact_id, None)

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)
