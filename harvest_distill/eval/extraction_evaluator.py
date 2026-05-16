"""
ExtractionEvaluator — token-level and structured-field evaluation for extracted content.

Provides:
- Token-level precision/recall/F1 (bag-of-words, ignoring order)
- ROUGE-L via Longest Common Subsequence (DP, capped at 500 words each for performance)
- Per-field F1 for structured dict outputs
- Batch aggregation across multiple prediction/ground-truth pairs

EvalResult dataclass surfaces: precision, recall, f1, rouge_l, exact_match, field_scores.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# EvalResult
# ---------------------------------------------------------------------------

@dataclass
class EvalResult:
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    rouge_l: float = 0.0
    exact_match: bool = False
    field_scores: Dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Tokenisation helper
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"\w+")


def _tokenize(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


# ---------------------------------------------------------------------------
# ExtractionEvaluator
# ---------------------------------------------------------------------------

class ExtractionEvaluator:
    """
    Evaluate extraction quality at the token and structural level.

    All methods are deterministic and require no external dependencies.
    """

    # ------------------------------------------------------------------
    # Public: token-level evaluation
    # ------------------------------------------------------------------

    def evaluate_tokens(self, predicted: str, ground_truth: str) -> EvalResult:
        """
        Token-level precision, recall, F1, ROUGE-L, and exact-match.

        Precision = |pred_tokens ∩ truth_tokens| / |pred_tokens|
        Recall    = |pred_tokens ∩ truth_tokens| / |truth_tokens|
        F1        = harmonic mean of precision and recall
        """
        pred_tokens = _tokenize(predicted)
        truth_tokens = _tokenize(ground_truth)

        # Exact match on raw strings (strip leading/trailing whitespace)
        exact = predicted.strip() == ground_truth.strip()

        if not pred_tokens and not truth_tokens:
            # Both empty → perfect agreement
            return EvalResult(
                precision=1.0, recall=1.0, f1=1.0,
                rouge_l=1.0, exact_match=True,
            )

        if not pred_tokens or not truth_tokens:
            return EvalResult(
                precision=0.0, recall=0.0, f1=0.0,
                rouge_l=0.0, exact_match=False,
            )

        # Build multisets as Counter-style dicts for overlap
        pred_counts: Dict[str, int] = {}
        for t in pred_tokens:
            pred_counts[t] = pred_counts.get(t, 0) + 1

        truth_counts: Dict[str, int] = {}
        for t in truth_tokens:
            truth_counts[t] = truth_counts.get(t, 0) + 1

        overlap = sum(
            min(pred_counts.get(t, 0), truth_counts.get(t, 0))
            for t in truth_counts
        )

        precision = overlap / len(pred_tokens) if pred_tokens else 0.0
        recall = overlap / len(truth_tokens) if truth_tokens else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0
            else 0.0
        )

        rouge = self._rouge_l(predicted, ground_truth)

        return EvalResult(
            precision=precision,
            recall=recall,
            f1=f1,
            rouge_l=rouge,
            exact_match=exact,
        )

    # ------------------------------------------------------------------
    # Public: structured dict evaluation
    # ------------------------------------------------------------------

    def evaluate_structured(
        self,
        predicted: dict,
        ground_truth: dict,
    ) -> EvalResult:
        """
        Per-field F1 over matching keys.

        For each key in ground_truth:
        - If key present in predicted: compute token-level F1 between
          str(predicted[key]) and str(ground_truth[key])
        - If key absent in predicted: field F1 = 0.0

        The aggregate f1 is the macro-average across all ground_truth fields.
        field_scores maps each field name to its individual F1.
        """
        if not ground_truth:
            return EvalResult(
                precision=1.0, recall=1.0, f1=1.0,
                rouge_l=1.0, exact_match=(predicted == ground_truth),
            )

        field_scores: Dict[str, float] = {}

        for key, truth_val in ground_truth.items():
            if key not in predicted:
                field_scores[key] = 0.0
                continue
            sub = self.evaluate_tokens(str(predicted[key]), str(truth_val))
            field_scores[key] = sub.f1

        macro_f1 = sum(field_scores.values()) / len(field_scores)

        return EvalResult(
            precision=macro_f1,   # symmetric under macro-avg
            recall=macro_f1,
            f1=macro_f1,
            rouge_l=0.0,          # not meaningful for dicts
            exact_match=(predicted == ground_truth),
            field_scores=field_scores,
        )

    # ------------------------------------------------------------------
    # Public: batch evaluation
    # ------------------------------------------------------------------

    def evaluate_batch(
        self,
        predictions: List[str],
        ground_truths: List[str],
    ) -> dict:
        """
        Aggregate token-level evaluation over parallel lists.

        Returns:
            avg_precision, avg_recall, avg_f1, avg_rouge_l,
            exact_match_rate, count, results (list of EvalResult)
        """
        if len(predictions) != len(ground_truths):
            raise ValueError(
                f"predictions and ground_truths must have the same length "
                f"({len(predictions)} vs {len(ground_truths)})"
            )

        results = [
            self.evaluate_tokens(p, g)
            for p, g in zip(predictions, ground_truths)
        ]

        n = len(results)
        if n == 0:
            return {
                "avg_precision": 0.0,
                "avg_recall": 0.0,
                "avg_f1": 0.0,
                "avg_rouge_l": 0.0,
                "exact_match_rate": 0.0,
                "count": 0,
                "results": [],
            }

        return {
            "avg_precision": sum(r.precision for r in results) / n,
            "avg_recall": sum(r.recall for r in results) / n,
            "avg_f1": sum(r.f1 for r in results) / n,
            "avg_rouge_l": sum(r.rouge_l for r in results) / n,
            "exact_match_rate": sum(1 for r in results if r.exact_match) / n,
            "count": n,
            "results": results,
        }

    # ------------------------------------------------------------------
    # Private: ROUGE-L via LCS
    # ------------------------------------------------------------------

    def _rouge_l(self, pred: str, truth: str) -> float:
        """
        ROUGE-L F1 score using Longest Common Subsequence.

        Words are capped at 500 each for performance.
        """
        _CAP = 500
        pred_words = _tokenize(pred)[:_CAP]
        truth_words = _tokenize(truth)[:_CAP]

        if not pred_words and not truth_words:
            return 1.0
        if not pred_words or not truth_words:
            return 0.0

        lcs = self._lcs_length(pred_words, truth_words)

        precision = lcs / len(pred_words)
        recall = lcs / len(truth_words)
        if precision + recall == 0:
            return 0.0
        return 2 * precision * recall / (precision + recall)

    def _lcs_length(self, a: List[str], b: List[str]) -> int:
        """
        DP Longest Common Subsequence length.

        O(|a| * |b|) time and space — inputs should be capped before calling.
        """
        m, n = len(a), len(b)
        # Use rolling two-row DP to keep memory bounded
        prev = [0] * (n + 1)
        curr = [0] * (n + 1)

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if a[i - 1] == b[j - 1]:
                    curr[j] = prev[j - 1] + 1
                else:
                    curr[j] = max(curr[j - 1], prev[j])
            prev, curr = curr, [0] * (n + 1)

        return prev[n]
