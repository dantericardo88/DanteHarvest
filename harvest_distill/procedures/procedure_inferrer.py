"""
ProcedureInferrer — infer reusable procedure graphs from TaskSpans.

Takes a list of TaskSpans and collapses repeated action patterns into
a ProcedureGraph: a directed graph of ProcedureSteps with typed edges.

Algorithm:
1. Normalize actions in each span to typed ProcedureSteps
2. Compute action-type n-gram frequency across spans
3. Identify repeated n-grams as candidate procedure skeletons
4. Build ProcedureGraph from the most frequent n-gram chains

Confidence = frequency / total_spans (how often the procedure appears).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4


@dataclass
class ProcedureStep:
    step_id: str
    action_type: str
    target_pattern: Optional[str]
    value_pattern: Optional[str]
    step_index: int
    evidence_refs: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "step_id": self.step_id,
            "action_type": self.action_type,
            "target_pattern": self.target_pattern,
            "value_pattern": self.value_pattern,
            "step_index": self.step_index,
            "evidence_refs": self.evidence_refs,
        }


@dataclass
class ProcedureGraph:
    graph_id: str
    title: str
    steps: List[ProcedureStep]
    frequency: int
    total_spans: int
    confidence: float
    source_span_ids: List[str] = field(default_factory=list)

    @property
    def step_count(self) -> int:
        return len(self.steps)

    def to_dict(self) -> dict:
        return {
            "graph_id": self.graph_id,
            "title": self.title,
            "step_count": self.step_count,
            "frequency": self.frequency,
            "total_spans": self.total_spans,
            "confidence": self.confidence,
            "source_span_ids": self.source_span_ids,
            "steps": [s.to_dict() for s in self.steps],
        }


@dataclass
class InferenceResult:
    graphs: List[ProcedureGraph]
    total_spans: int
    unique_action_types: int

    def best(self) -> Optional[ProcedureGraph]:
        if not self.graphs:
            return None
        return max(self.graphs, key=lambda g: g.confidence)

    def to_dict(self) -> dict:
        return {
            "graph_count": len(self.graphs),
            "total_spans": self.total_spans,
            "unique_action_types": self.unique_action_types,
            "graphs": [g.to_dict() for g in self.graphs],
        }


class ProcedureInferrer:
    """
    Infer procedure graphs from repeated action patterns in TaskSpans.

    Usage:
        inferrer = ProcedureInferrer(min_frequency=2, ngram_size=3)
        result = inferrer.infer(spans)
        best = result.best()
    """

    def __init__(
        self,
        min_frequency: int = 2,
        ngram_size: int = 3,
        min_confidence: float = 0.1,
    ):
        self.min_frequency = min_frequency
        self.ngram_size = ngram_size
        self.min_confidence = min_confidence

    def infer(self, spans: list) -> InferenceResult:
        """
        Infer ProcedureGraphs from a list of TaskSpan objects or dicts.
        """
        normalized = [self._normalize_span(s) for s in spans]
        total = len(normalized)

        if not normalized:
            return InferenceResult(graphs=[], total_spans=0, unique_action_types=0)

        all_action_types = set()
        for span in normalized:
            for action in span["actions"]:
                all_action_types.add(action.get("action_type", "unknown"))

        # Build n-gram frequency table
        ngram_counter: Counter = Counter()
        ngram_to_spans: Dict[Tuple, List[str]] = {}

        for span in normalized:
            actions = span["actions"]
            seq = tuple(a.get("action_type", "unknown") for a in actions)
            for i in range(len(seq) - self.ngram_size + 1):
                gram = seq[i:i + self.ngram_size]
                ngram_counter[gram] += 1
                ngram_to_spans.setdefault(gram, []).append(span["span_id"])

        # Build graphs from n-grams meeting frequency threshold
        graphs: List[ProcedureGraph] = []
        seen_grams = set()

        for gram, freq in ngram_counter.most_common():
            if freq < self.min_frequency:
                continue
            if gram in seen_grams:
                continue
            seen_grams.add(gram)

            confidence = min(1.0, freq / max(total, 1))
            if confidence < self.min_confidence:
                continue

            steps = [
                ProcedureStep(
                    step_id=str(uuid4()),
                    action_type=action_type,
                    target_pattern=None,
                    value_pattern=None,
                    step_index=i,
                )
                for i, action_type in enumerate(gram)
            ]

            graph = ProcedureGraph(
                graph_id=str(uuid4()),
                title=" → ".join(gram),
                steps=steps,
                frequency=freq,
                total_spans=total,
                confidence=confidence,
                source_span_ids=ngram_to_spans.get(gram, []),
            )
            graphs.append(graph)

        # If no n-grams, build one graph per unique span (frequency 1)
        if not graphs:
            for span in normalized[:3]:  # cap at 3 to avoid noise
                actions = span["actions"]
                if not actions:
                    continue
                steps = [
                    ProcedureStep(
                        step_id=str(uuid4()),
                        action_type=a.get("action_type", "unknown"),
                        target_pattern=a.get("target_selector"),
                        value_pattern=None,
                        step_index=i,
                    )
                    for i, a in enumerate(actions)
                ]
                graphs.append(ProcedureGraph(
                    graph_id=str(uuid4()),
                    title=span.get("title", "procedure"),
                    steps=steps,
                    frequency=1,
                    total_spans=total,
                    confidence=1.0 / max(total, 1),
                    source_span_ids=[span["span_id"]],
                ))

        return InferenceResult(
            graphs=sorted(graphs, key=lambda g: g.confidence, reverse=True),
            total_spans=total,
            unique_action_types=len(all_action_types),
        )

    def _normalize_span(self, span: Any) -> dict:
        if isinstance(span, dict):
            return span
        if hasattr(span, "to_dict"):
            return span.to_dict()
        return {
            "span_id": getattr(span, "span_id", str(uuid4())),
            "title": getattr(span, "title", ""),
            "actions": getattr(span, "actions", []),
        }
