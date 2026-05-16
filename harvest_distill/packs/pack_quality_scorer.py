"""Pack quality scoring — validates schema, scores completeness and diversity."""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

import time


@dataclass
class PackQualityReport:
    pack_id: str
    schema_valid: bool
    schema_errors: List[str]
    completeness_score: float  # 0.0-1.0: required fields present
    diversity_score: float     # 0.0-1.0: variety of content types
    freshness_score: float     # 0.0-1.0: recency of artifacts
    overall_score: float       # weighted average
    artifact_count: int

    def passes_threshold(self, threshold: float = 0.7) -> bool:
        return self.overall_score >= threshold

    def summary(self) -> dict:
        return {
            "pack_id": self.pack_id,
            "schema_valid": self.schema_valid,
            "overall_score": round(self.overall_score, 3),
            "completeness": round(self.completeness_score, 3),
            "diversity": round(self.diversity_score, 3),
            "freshness": round(self.freshness_score, 3),
            "artifact_count": self.artifact_count,
            "passes_threshold": self.passes_threshold(),
        }


REQUIRED_PACK_FIELDS = {"id", "name", "version", "artifacts"}
OPTIONAL_PACK_FIELDS = {"description", "created_at", "tags", "schema", "source_url"}


class PackQualityScorer:
    """Scores pack quality across multiple dimensions."""

    def score(self, pack: dict) -> PackQualityReport:
        """Score a pack dict and return quality report."""
        pack_id = pack.get("id", "unknown")

        # Schema validation
        errors: List[str] = []
        for f in REQUIRED_PACK_FIELDS:
            if f not in pack:
                errors.append(f"Missing required field: {f}")

        # Completeness: required + optional fields present
        present = len([f for f in REQUIRED_PACK_FIELDS | OPTIONAL_PACK_FIELDS if f in pack])
        total_expected = len(REQUIRED_PACK_FIELDS | OPTIONAL_PACK_FIELDS)
        completeness = present / total_expected

        # Diversity: variety of content types in artifacts
        artifacts = pack.get("artifacts", [])
        if artifacts:
            content_types = set(
                a.get("content_type", a.get("type", "unknown")) for a in artifacts
            )
            diversity = min(1.0, len(content_types) / 5)  # 5+ types = max diversity
        else:
            diversity = 0.0

        # Freshness: based on artifact timestamps
        now = time.time()
        if artifacts:
            timestamps = [
                a.get("created_at", a.get("timestamp", 0)) for a in artifacts
            ]
            timestamps = [t for t in timestamps if isinstance(t, (int, float)) and t > 0]
            if timestamps:
                avg_age_days = (now - sum(timestamps) / len(timestamps)) / 86400
                freshness = max(0.0, 1.0 - avg_age_days / 365)  # linear decay over 1 year
            else:
                freshness = 0.5  # no timestamp info = neutral
        else:
            freshness = 0.0

        overall = completeness * 0.4 + diversity * 0.3 + freshness * 0.3

        return PackQualityReport(
            pack_id=pack_id,
            schema_valid=len(errors) == 0,
            schema_errors=errors,
            completeness_score=completeness,
            diversity_score=diversity,
            freshness_score=freshness,
            overall_score=overall,
            artifact_count=len(artifacts),
        )

    def score_batch(self, packs: List[dict]) -> List[PackQualityReport]:
        return [self.score(p) for p in packs]

    def get_failing_packs(self, packs: List[dict], threshold: float = 0.7) -> List[dict]:
        """Return packs that fail quality threshold."""
        return [
            p
            for p, r in zip(packs, self.score_batch(packs))
            if not r.passes_threshold(threshold)
        ]
