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

# Minimum PMI threshold for filtering weak term associations.
MIN_PMI_THRESHOLD: float = 0.1


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

    def to_jsonld(self, base_uri: str = "https://danteharvestai.com/taxonomy/") -> dict:
        """Export the taxonomy as a JSON-LD graph (schema.org DefinedTermSet)."""
        context = {
            "@context": {
                "@vocab": "https://schema.org/",
                "harvest": "https://danteharvestai.com/vocab#",
            }
        }
        terms = []
        for node in self.nodes:
            entry: Dict[str, Any] = {
                "@id": f"{base_uri}{node.term.replace(' ', '_')}",
                "@type": "DefinedTerm",
                "name": node.term,
                "harvest:frequency": node.frequency,
                "inDefinedTermSet": f"{base_uri}{self.domain}",
            }
            if node.parent:
                entry["broaderTransitive"] = {"@id": f"{base_uri}{node.parent.replace(' ', '_')}"}
            if node.children:
                entry["narrowerTransitive"] = [
                    {"@id": f"{base_uri}{c.replace(' ', '_')}"} for c in node.children
                ]
            terms.append(entry)
        return {
            **context,
            "@graph": [
                {
                    "@id": f"{base_uri}{self.domain}",
                    "@type": "DefinedTermSet",
                    "name": self.domain,
                    "harvest:source_pack_count": self.source_pack_count,
                },
                *terms,
            ],
        }

    def merge(self, other: "TaxonomyGraph") -> "TaxonomyGraph":
        """Merge two TaxonomyGraphs into a new combined graph."""
        node_map: Dict[str, TaxonomyNode] = {n.term: n for n in self.nodes}
        for other_node in other.nodes:
            if other_node.term in node_map:
                existing = node_map[other_node.term]
                node_map[other_node.term] = TaxonomyNode(
                    term=existing.term,
                    frequency=existing.frequency + other_node.frequency,
                    parent=existing.parent or other_node.parent,
                    children=list(set(existing.children + other_node.children)),
                )
            else:
                node_map[other_node.term] = other_node
        merged_edges = list(set(self.edges + other.edges))
        return TaxonomyGraph(
            domain=self.domain,
            nodes=list(node_map.values()),
            edges=merged_edges,
            source_pack_count=self.source_pack_count + other.source_pack_count,
        )


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
        # Per-instance term frequency counter, seeded during build / build_taxonomy_with_pmi.
        self._term_freq: Counter = Counter()

    def build(self, workflow_packs: List[WorkflowPack]) -> TaxonomyGraph:
        """
        Build taxonomy from a list of WorkflowPacks.
        Edges are PMI-filtered: only term pairs with NPMI > 0 (positive
        statistical association) become taxonomy edges.
        Raises PackagingError if workflow_packs is empty (fail-closed).
        """
        if not workflow_packs:
            raise PackagingError("TaxonomyBuilder requires at least one WorkflowPack")

        term_freq: Counter = Counter()
        cooccurrence: Counter = Counter()
        total_docs = len(workflow_packs)

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

        # Expose frequencies so compute_pmi / compute_npmi work with current data
        self._term_freq = term_freq

        filtered = {t: f for t, f in term_freq.items() if f >= self.min_frequency}

        parent_map: Dict[str, str] = {}
        for (t1, t2), freq in cooccurrence.items():
            if freq >= self.min_frequency and t1 in filtered and t2 in filtered:
                # Only include edges where terms are positively associated (NPMI > 0)
                npmi = self.compute_npmi(t1, t2, freq, max(total_docs, 1))
                if npmi > 0:
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

    # ------------------------------------------------------------------
    # PMI / NPMI scoring
    # ------------------------------------------------------------------

    def compute_pmi(
        self,
        term_a: str,
        term_b: str,
        co_occurrence_count: int,
        total_docs: int,
    ) -> float:
        """
        Compute Pointwise Mutual Information for two terms.

        PMI(A,B) = log2( P(A,B) / (P(A) * P(B)) )

        Marginal probabilities are estimated from self._term_freq.
        Missing terms default to frequency=1 to avoid division-by-zero.
        Returns 0.0 when total_docs==0 or co_occurrence_count==0.
        """
        import math
        if total_docs == 0 or co_occurrence_count == 0:
            return 0.0
        freq_a = self._term_freq.get(term_a, 1)
        freq_b = self._term_freq.get(term_b, 1)
        p_ab = co_occurrence_count / total_docs
        p_a = freq_a / total_docs
        p_b = freq_b / total_docs
        if p_a == 0 or p_b == 0:
            return 0.0
        ratio = p_ab / (p_a * p_b)
        if ratio <= 0:
            return 0.0
        return math.log2(ratio)

    def compute_npmi(
        self,
        term_a: str,
        term_b: str,
        co_occurrence_count: int,
        total_docs: int,
    ) -> float:
        """
        Compute Normalised PMI, scaled to [-1, 1].

        NPMI(A,B) = PMI(A,B) / -log2( P(A,B) )

        Returns 0.0 for degenerate inputs.
        """
        import math
        if total_docs == 0 or co_occurrence_count == 0:
            return 0.0
        p_ab = co_occurrence_count / total_docs
        if p_ab <= 0:
            return 0.0
        pmi = self.compute_pmi(term_a, term_b, co_occurrence_count, total_docs)
        normaliser = -math.log2(p_ab)
        if normaliser == 0:
            # p_ab == 1.0 means perfect co-occurrence → NPMI = 1.0
            return 1.0 if pmi >= 0 else -1.0
        return pmi / normaliser

    # ------------------------------------------------------------------
    # Stop-term computation
    # ------------------------------------------------------------------

    @staticmethod
    def compute_stop_terms(corpus: List[str], top_pct: float = 0.1) -> Set[str]:
        """
        Identify high-frequency stop terms from a text corpus.

        Terms appearing in the top *top_pct* fraction of documents by
        document-frequency are returned as stop terms.  Empty corpus
        returns an empty set.
        """
        if not corpus:
            return set()

        doc_freq: Counter = Counter()
        for doc in corpus:
            words = set(doc.lower().split())
            doc_freq.update(words)

        import math as _math
        n_terms = len(doc_freq)
        cutoff = max(1, _math.ceil(n_terms * top_pct))
        # Sort by (-count, term) for deterministic tie-breaking when counts are equal
        ranked = sorted(doc_freq.items(), key=lambda kv: (-kv[1], kv[0]))
        top_terms = {term for term, _ in ranked[:cutoff]}
        return top_terms

    # ------------------------------------------------------------------
    # PMI-based taxonomy building from raw text documents
    # ------------------------------------------------------------------

    def build_taxonomy_with_pmi(
        self,
        documents: List[str],
        min_pmi: float = MIN_PMI_THRESHOLD,
    ) -> dict:
        """
        Build a taxonomy from raw text documents using PMI scoring.

        Returns::

            {
                "terms": list[str],
                "pairs": list[{"term_a": str, "term_b": str, "pmi": float, "npmi": float}],
                "clusters": dict[str, list[str]],   # parent → [children]
            }

        Pairs are sorted by NPMI descending.
        """
        import math

        if not documents:
            return {"terms": [], "pairs": [], "clusters": {}}

        # Count per-document term sets for co-occurrence
        doc_term_sets: List[Set[str]] = []
        self._term_freq = Counter()
        for doc in documents:
            terms = self._extract_terms(doc)
            self._term_freq.update(terms)
            doc_term_sets.append(terms)

        total_docs = len(documents)

        # Filter by min_frequency
        filtered_terms = {
            t for t, f in self._term_freq.items() if f >= self.min_frequency
        }

        # Build co-occurrence counts
        cooccurrence: Counter = Counter()
        for term_set in doc_term_sets:
            term_list = sorted(term_set & filtered_terms)
            for i, t1 in enumerate(term_list):
                for t2 in term_list[i + 1:]:
                    cooccurrence[(t1, t2)] += 1

        # Score pairs
        pairs = []
        for (t1, t2), co_count in cooccurrence.items():
            pmi = self.compute_pmi(t1, t2, co_count, total_docs)
            if pmi < min_pmi:
                continue
            npmi = self.compute_npmi(t1, t2, co_count, total_docs)
            pairs.append({"term_a": t1, "term_b": t2, "pmi": pmi, "npmi": npmi})

        pairs.sort(key=lambda p: -p["npmi"])

        # Build simple clusters: group by highest-frequency partner
        clusters: Dict[str, List[str]] = {}
        parent_map: Dict[str, str] = {}
        for pair in pairs:
            ta: str = str(pair["term_a"])
            tb: str = str(pair["term_b"])
            fa = self._term_freq.get(ta, 0)
            fb = self._term_freq.get(tb, 0)
            parent: str = ta if fa >= fb else tb
            child: str = tb if fa >= fb else ta
            if child not in parent_map:
                parent_map[child] = parent
                clusters.setdefault(parent, []).append(child)

        return {
            "terms": sorted(filtered_terms),
            "pairs": pairs,
            "clusters": clusters,
        }
