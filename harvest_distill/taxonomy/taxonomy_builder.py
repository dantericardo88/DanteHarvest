"""
TaxonomyBuilder — derive domain taxonomy from SpecializationPack workflow refs.

Harvested from: LlamaIndex knowledge graph extraction + Instructor structured output patterns.

Builds a TaxonomyGraph from a set of WorkflowPacks by:
1. Extracting action types and tool names from each workflow step
2. Counting co-occurrence frequency to infer parent-child relationships
3. Returning a TaxonomyGraph with nodes (terms) and edges (relations)

Constitutional guarantees:
- Local-first: no LLM required for heuristic extraction path
- Fail-closed: empty workflow list raises PackagingError (not silent empty graph)
- Zero-ambiguity: TaxonomyGraph.nodes always List[TaxonomyNode], never None
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from harvest_core.control.exceptions import PackagingError
from harvest_distill.packs.pack_schemas import WorkflowPack


@dataclass
class TaxonomyNode:
    term: str
    frequency: int
    parent: Optional[str] = None
    children: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaxonomyGraph:
    domain: str
    nodes: List[TaxonomyNode]
    edges: List[tuple]
    source_pack_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "source_pack_count": self.source_pack_count,
            "nodes": [
                {
                    "term": n.term,
                    "frequency": n.frequency,
                    "parent": n.parent,
                    "children": n.children,
                }
                for n in self.nodes
            ],
        }

    def top_terms(self, n: int = 10) -> List[str]:
        return [node.term for node in sorted(self.nodes, key=lambda x: -x.frequency)[:n]]


class TaxonomyBuilder:
    """
    Build a domain taxonomy from WorkflowPacks.

    Usage:
        builder = TaxonomyBuilder(domain="accounting")
        graph = builder.build(workflow_packs)
        print(graph.top_terms())
    """

    _STOP_VERBS = {
        "click", "type", "press", "move", "scroll", "wait", "navigate",
        "open", "close", "select", "drag", "drop", "hover",
    }

    _TERM_RE = re.compile(r"[a-z][a-z0-9_\-]{2,}")

    def __init__(self, domain: str = "general", min_frequency: int = 2):
        self.domain = domain
        self.min_frequency = min_frequency

    def build(self, workflow_packs: List[WorkflowPack]) -> TaxonomyGraph:
        """
        Build taxonomy from a list of WorkflowPacks.
        Raises PackagingError if workflow_packs is empty (fail-closed).
        """
        if not workflow_packs:
            raise PackagingError("TaxonomyBuilder requires at least one WorkflowPack")

        term_freq: Counter = Counter()
        cooccurrence: Counter = Counter()

        for pack in workflow_packs:
            step_terms: List[Set[str]] = []
            for step in pack.steps:
                terms = self._extract_terms(step.action)
                term_freq.update(terms)
                step_terms.append(terms)

            for terms in step_terms:
                term_list = sorted(terms)
                for i, t1 in enumerate(term_list):
                    for t2 in term_list[i + 1:]:
                        cooccurrence[(t1, t2)] += 1

            goal_terms = self._extract_terms(pack.goal)
            term_freq.update(goal_terms)

        filtered = {t: f for t, f in term_freq.items() if f >= self.min_frequency}

        parent_map: Dict[str, str] = {}
        for (t1, t2), freq in cooccurrence.items():
            if freq >= self.min_frequency and t1 in filtered and t2 in filtered:
                if term_freq[t1] >= term_freq[t2]:
                    parent_map[t2] = t1
                else:
                    parent_map[t1] = t2

        children_map: Dict[str, List[str]] = defaultdict(list)
        for child, parent in parent_map.items():
            children_map[parent].append(child)

        nodes = [
            TaxonomyNode(
                term=term,
                frequency=freq,
                parent=parent_map.get(term),
                children=children_map.get(term, []),
            )
            for term, freq in sorted(filtered.items(), key=lambda x: -x[1])
        ]

        edges = [
            (child, "is_a", parent)
            for child, parent in parent_map.items()
            if child in filtered and parent in filtered
        ]

        return TaxonomyGraph(
            domain=self.domain,
            nodes=nodes,
            edges=edges,
            source_pack_count=len(workflow_packs),
        )

    def _extract_terms(self, text: str) -> Set[str]:
        tokens = self._TERM_RE.findall(text.lower())
        return {t for t in tokens if t not in self._STOP_VERBS and len(t) > 2}
