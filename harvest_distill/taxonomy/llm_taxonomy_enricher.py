"""
LLMTaxonomyEnricher — enrich a TaxonomyGraph with LLM-derived SPO triples.

Harvested from: LlamaIndex knowledge graph extraction + Instructor structured output patterns.

Two modes:
1. **LLM mode** (requires OPENAI_API_KEY env var and openai package):
   Calls an OpenAI-compatible chat API to extract subject-predicate-object triples
   from each node's label. Adds enriched edges back to the TaxonomyGraph.
2. **Offline / co-occurrence mode** (default, no dependencies):
   Uses term substring and co-occurrence heuristics to infer SPO triples locally.
   Safe for CI — never makes network calls.

Constitutional guarantees:
- Graceful degradation: missing openai key or package → silent offline fallback.
- Non-destructive: never removes existing nodes/edges; only appends enriched edges.
- Idempotent: calling enrich() twice on the same graph does not duplicate edges.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from harvest_distill.taxonomy.taxonomy_builder import TaxonomyGraph, TaxonomyNode


@dataclass
class SPOTriple:
    """A subject-predicate-object triple extracted from node labels."""

    subject: str
    predicate: str
    obj: str
    confidence: float = 1.0
    source: str = "offline"  # "offline" | "llm"

    def as_edge(self) -> Tuple[str, str, str]:
        return (self.subject, self.predicate, self.obj)


class LLMTaxonomyEnricher:
    """
    Enrich a TaxonomyGraph with SPO triples.

    Usage (offline, no LLM needed):
        enricher = LLMTaxonomyEnricher()
        triples, graph = enricher.enrich(graph)

    Usage (LLM mode — requires OPENAI_API_KEY):
        enricher = LLMTaxonomyEnricher(
            api_key=os.environ["OPENAI_API_KEY"],
            model="gpt-4o-mini",
        )
        triples, graph = enricher.enrich(graph)
    """

    _PREDICATE_RE = re.compile(
        r"(?P<subj>[a-z][a-z0-9_\-]+)\s+(?P<pred>is|has|uses|contains|belongs_to|related_to|part_of)\s+(?P<obj>[a-z][a-z0-9_\-]+)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-haiku-4-5-20251001",
        base_url: Optional[str] = None,
        min_confidence: float = 0.5,
        llm_timeout: float = 30.0,
        backend: str = "anthropic",
    ) -> None:
        self.backend = backend  # "anthropic" | "openai"
        # Anthropic is the preferred default; fall back to OpenAI if configured
        if backend == "anthropic":
            self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        else:
            self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.model = model
        self.base_url = base_url
        self.min_confidence = min_confidence
        self.llm_timeout = llm_timeout
        self._llm_available: Optional[bool] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich(self, graph: TaxonomyGraph) -> Tuple[List[SPOTriple], TaxonomyGraph]:
        """
        Enrich *graph* in-place and return (triples_extracted, graph).

        Tries LLM extraction if configured; falls back to offline heuristics
        silently on any error or missing credentials.
        """
        triples: List[SPOTriple] = []

        if self._should_use_llm():
            try:
                triples = self._llm_extract(graph)
            except Exception:
                triples = self._offline_extract(graph)
        else:
            triples = self._offline_extract(graph)

        # Filter by confidence
        triples = [t for t in triples if t.confidence >= self.min_confidence]

        # Deduplicate and add to graph
        existing_edges: set = set(map(tuple, graph.edges))
        for triple in triples:
            edge = triple.as_edge()
            if edge not in existing_edges:
                graph.edges.append(edge)
                existing_edges.add(edge)

            # Add enriched metadata to matching nodes
            for node in graph.nodes:
                if node.term == triple.subject:
                    node.metadata.setdefault("spo_triples", [])
                    node.metadata["spo_triples"].append(
                        {
                            "predicate": triple.predicate,
                            "obj": triple.obj,
                            "source": triple.source,
                        }
                    )

        return triples, graph

    def extract_triples_from_text(self, text: str, source: str = "offline") -> List[SPOTriple]:
        """
        Extract SPO triples from an arbitrary text string.
        Public for testing and standalone use.
        """
        return self._parse_text_for_triples(text, source=source)

    # ------------------------------------------------------------------
    # LLM path
    # ------------------------------------------------------------------

    def _should_use_llm(self) -> bool:
        if not self.api_key:
            return False
        if self._llm_available is False:
            return False
        try:
            if self.backend == "anthropic":
                import anthropic  # noqa: F401
            else:
                import openai  # noqa: F401
            self._llm_available = True
            return True
        except ImportError:
            self._llm_available = False
            return False

    def _llm_extract(self, graph: TaxonomyGraph) -> List[SPOTriple]:
        """Extract SPO triples using the configured LLM backend (Anthropic by default)."""
        terms = [node.term for node in graph.nodes[:20]]
        prompt = self._build_prompt(terms, graph.domain)

        if self.backend == "anthropic":
            return self._llm_extract_anthropic(prompt)
        return self._llm_extract_openai(prompt)

    def _llm_extract_anthropic(self, prompt: str) -> List[SPOTriple]:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key)
        system = (
            "You are a knowledge graph extraction assistant. "
            "Extract subject-predicate-object triples from domain terms. "
            'Respond with valid JSON only: {"triples": [{"subject": "...", "predicate": "...", "object": "...", "confidence": 0.9}]}'
        )
        msg = client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[{"role": "user", "content": f"{system}\n\n{prompt}"}],
        )
        from anthropic.types import TextBlock as _TextBlock
        raw = next(
            (block.text for block in msg.content if isinstance(block, _TextBlock)),
            "{}",
        ).strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return self._parse_llm_response(raw)

    def _llm_extract_openai(self, prompt: str) -> List[SPOTriple]:
        import openai  # type: ignore[import]
        client = openai.OpenAI(
            api_key=self.api_key,
            **({"base_url": self.base_url} if self.base_url else {}),
            timeout=self.llm_timeout,
        )
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a knowledge graph extraction assistant. "
                        "Extract subject-predicate-object triples from domain terms. "
                        "Respond with JSON: {\"triples\": [{\"subject\": \"...\", \"predicate\": \"...\", \"object\": \"...\", \"confidence\": 0.9}]}"
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        raw = response.choices[0].message.content or "{}"
        return self._parse_llm_response(raw)

    def _build_prompt(self, terms: List[str], domain: str) -> str:
        term_str = ", ".join(terms)
        return (
            f"Domain: {domain}\n"
            f"Terms: {term_str}\n\n"
            "Extract subject-predicate-object triples showing relationships between these terms. "
            "Use predicates like: is_a, has, uses, contains, belongs_to, related_to, part_of. "
            'Return JSON: {"triples": [{"subject": "...", "predicate": "...", "object": "...", "confidence": 0.9}]}'
        )

    def _parse_llm_response(self, raw: str) -> List[SPOTriple]:
        try:
            data = json.loads(raw)
            triples_data = data.get("triples", [])
            if not isinstance(triples_data, list):
                return []
            result = []
            for item in triples_data:
                if not isinstance(item, dict):
                    continue
                subj = str(item.get("subject", "")).strip().lower()
                pred = str(item.get("predicate", "")).strip().lower()
                obj = str(item.get("object", "")).strip().lower()
                conf = float(item.get("confidence", 0.8))
                if subj and pred and obj:
                    result.append(
                        SPOTriple(
                            subject=subj,
                            predicate=pred,
                            obj=obj,
                            confidence=conf,
                            source="llm",
                        )
                    )
            return result
        except (json.JSONDecodeError, ValueError, TypeError):
            return []

    # ------------------------------------------------------------------
    # Offline / heuristic path
    # ------------------------------------------------------------------

    def _offline_extract(self, graph: TaxonomyGraph) -> List[SPOTriple]:
        """
        Derive SPO triples from node labels using co-occurrence heuristics.
        No network calls — safe for CI.
        """
        triples: List[SPOTriple] = []

        # Build a map for quick lookup
        node_map: Dict[str, TaxonomyNode] = {n.term: n for n in graph.nodes}

        for node in graph.nodes:
            # Pattern 1: node text itself may contain predicate structure
            text_triples = self._parse_text_for_triples(node.term, source="offline")
            triples.extend(text_triples)

            # Pattern 2: parent-child → "is_a" triple
            if node.parent and node.parent in node_map:
                triples.append(
                    SPOTriple(
                        subject=node.term,
                        predicate="is_a",
                        obj=node.parent,
                        confidence=0.9,
                        source="offline",
                    )
                )

            # Pattern 3: substring containment → "part_of" triple
            for other_term in node_map:
                if other_term == node.term:
                    continue
                if len(other_term) > len(node.term) and node.term in other_term:
                    triples.append(
                        SPOTriple(
                            subject=node.term,
                            predicate="part_of",
                            obj=other_term,
                            confidence=0.7,
                            source="offline",
                        )
                    )

            # Pattern 4: co-occurrence metadata → "related_to"
            for other in graph.edges:
                edge_tuple = tuple(other)
                if len(edge_tuple) >= 3:
                    src, _, dst = edge_tuple[0], edge_tuple[1], edge_tuple[2]
                    if src == node.term and dst in node_map and dst != node.term:
                        triples.append(
                            SPOTriple(
                                subject=src,
                                predicate="related_to",
                                obj=dst,
                                confidence=0.75,
                                source="offline",
                            )
                        )

        return triples

    def _parse_text_for_triples(self, text: str, source: str = "offline") -> List[SPOTriple]:
        """
        Parse explicit predicate keywords from text like "invoice has payment".
        """
        triples: List[SPOTriple] = []
        for match in self._PREDICATE_RE.finditer(text):
            subj = match.group("subj").lower()
            pred = match.group("pred").lower()
            obj = match.group("obj").lower()
            triples.append(
                SPOTriple(
                    subject=subj,
                    predicate=pred,
                    obj=obj,
                    confidence=0.85,
                    source=source,
                )
            )
        return triples
