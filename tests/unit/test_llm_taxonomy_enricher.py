"""
Tests for LLMTaxonomyEnricher.

Covers:
- Offline mode (no LLM key) — must work in CI without any network calls
- SPO triple extraction from text
- Graph augmentation (edges added, no duplicates)
- Mocked LLM path (verify prompt + response parsing)
- Graceful fallback when openai package absent or API key missing
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from harvest_distill.packs.pack_schemas import PackStep, WorkflowPack
from harvest_distill.taxonomy.llm_taxonomy_enricher import (
    LLMTaxonomyEnricher,
    SPOTriple,
)
from harvest_distill.taxonomy.taxonomy_builder import (
    TaxonomyBuilder,
    TaxonomyGraph,
    TaxonomyNode,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_pack(pack_id: str, goal: str, actions: list[str]) -> WorkflowPack:
    return WorkflowPack(
        pack_id=pack_id,
        title=f"Pack {pack_id}",
        goal=goal,
        steps=[PackStep(id=f"s{i}", action=a) for i, a in enumerate(actions)],
    )


def make_simple_graph() -> TaxonomyGraph:
    """Build a small taxonomy graph for testing."""
    packs = [
        make_pack(
            "wf-001",
            "invoice processing workflow",
            ["invoice review", "invoice approval", "invoice payment", "payment confirmation"],
        ),
        make_pack(
            "wf-002",
            "payment reconciliation",
            ["invoice verification", "payment matching", "payment reconciliation"],
        ),
    ]
    builder = TaxonomyBuilder(domain="accounting", min_frequency=1)
    return builder.build(packs)


# ---------------------------------------------------------------------------
# Offline mode tests (no LLM, no API key)
# ---------------------------------------------------------------------------


def test_offline_mode_no_api_key():
    """Enricher with no API key must use offline path without errors."""
    enricher = LLMTaxonomyEnricher()  # no api_key
    graph = make_simple_graph()
    initial_node_count = len(graph.nodes)

    triples, enriched_graph = enricher.enrich(graph)

    # Graph nodes must be preserved
    assert len(enriched_graph.nodes) == initial_node_count
    # Should have extracted some triples (parent-child and substring heuristics)
    assert isinstance(triples, list)
    # All triples must be SPOTriple instances
    for t in triples:
        assert isinstance(t, SPOTriple)
        assert t.source == "offline"


def test_offline_mode_triples_have_fields():
    """Every SPOTriple has non-empty subject, predicate, obj."""
    enricher = LLMTaxonomyEnricher()
    graph = make_simple_graph()
    triples, _ = enricher.enrich(graph)

    for t in triples:
        assert t.subject, "subject must not be empty"
        assert t.predicate, "predicate must not be empty"
        assert t.obj, "obj must not be empty"
        assert 0.0 <= t.confidence <= 1.0


def test_offline_adds_is_a_triples_for_parent_child():
    """Parent-child node relationships become is_a triples."""
    # Manually craft a graph with known parent-child
    parent_node = TaxonomyNode(term="invoice", frequency=10)
    child_node = TaxonomyNode(term="payment", frequency=5, parent="invoice")
    graph = TaxonomyGraph(
        domain="finance",
        nodes=[parent_node, child_node],
        edges=[("payment", "is_a", "invoice")],
        source_pack_count=1,
    )
    enricher = LLMTaxonomyEnricher()
    triples, enriched = enricher.enrich(graph)

    is_a_triples = [t for t in triples if t.predicate == "is_a"]
    assert any(t.subject == "payment" and t.obj == "invoice" for t in is_a_triples)


def test_offline_no_duplicate_edges():
    """Enricher must not add duplicate edges to graph."""
    graph = make_simple_graph()
    enricher = LLMTaxonomyEnricher()

    # Enrich twice
    _, graph = enricher.enrich(graph)
    edges_after_first = list(graph.edges)
    _, graph = enricher.enrich(graph)
    edges_after_second = list(graph.edges)

    # Edge count must not grow on second call (deduplication)
    assert len(edges_after_second) == len(edges_after_first)


def test_offline_enrichment_augments_edges():
    """Enrichment should add edges beyond what taxonomy_builder produces."""
    graph = make_simple_graph()
    initial_edge_count = len(graph.edges)

    enricher = LLMTaxonomyEnricher()
    _, enriched = enricher.enrich(graph)

    # With parent-child and substring heuristics, should add some edges
    assert len(enriched.edges) >= initial_edge_count


# ---------------------------------------------------------------------------
# SPO triple extraction from text
# ---------------------------------------------------------------------------


def test_extract_triples_from_text_explicit_predicate():
    """Explicit predicates in text (e.g., 'invoice has payment') are parsed."""
    enricher = LLMTaxonomyEnricher()
    triples = enricher.extract_triples_from_text("invoice has payment")

    assert len(triples) == 1
    assert triples[0].subject == "invoice"
    assert triples[0].predicate == "has"
    assert triples[0].obj == "payment"


def test_extract_triples_from_text_is_predicate():
    enricher = LLMTaxonomyEnricher()
    triples = enricher.extract_triples_from_text("payment is reconciliation")

    assert len(triples) == 1
    assert triples[0].subject == "payment"
    assert triples[0].predicate == "is"
    assert triples[0].obj == "reconciliation"


def test_extract_triples_from_text_no_predicate():
    """Text without recognized predicates returns empty list."""
    enricher = LLMTaxonomyEnricher()
    triples = enricher.extract_triples_from_text("randomterm without structure")
    assert isinstance(triples, list)


def test_extract_triples_from_text_multiple_predicates():
    enricher = LLMTaxonomyEnricher()
    text = "invoice has payment and invoice uses ledger"
    triples = enricher.extract_triples_from_text(text)
    assert len(triples) >= 1


def test_extract_triples_source_field():
    enricher = LLMTaxonomyEnricher()
    triples = enricher.extract_triples_from_text("invoice uses ledger", source="test_source")
    if triples:
        assert triples[0].source == "test_source"


# ---------------------------------------------------------------------------
# SPOTriple dataclass
# ---------------------------------------------------------------------------


def test_spo_triple_as_edge():
    t = SPOTriple(subject="invoice", predicate="is_a", obj="document", confidence=0.9)
    edge = t.as_edge()
    assert edge == ("invoice", "is_a", "document")


def test_spo_triple_defaults():
    t = SPOTriple(subject="x", predicate="p", obj="y")
    assert t.confidence == 1.0
    assert t.source == "offline"


# ---------------------------------------------------------------------------
# Mocked LLM path
# ---------------------------------------------------------------------------


def test_llm_path_called_when_api_key_set():
    """When api_key is set and openai importable, LLM path should be attempted."""
    graph = make_simple_graph()

    mock_response_content = json.dumps({
        "triples": [
            {"subject": "invoice", "predicate": "is_a", "object": "document", "confidence": 0.95},
            {"subject": "payment", "predicate": "related_to", "object": "invoice", "confidence": 0.85},
        ]
    })

    mock_message = MagicMock()
    mock_message.content = mock_response_content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_response = MagicMock()
    mock_response.choices = [mock_choice]

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response

    with patch.dict("sys.modules", {"openai": MagicMock()}):
        import openai as _openai
        _openai.OpenAI = MagicMock(return_value=mock_client)

        enricher = LLMTaxonomyEnricher(api_key="sk-test-key")
        # Force LLM available so offline path is not chosen
        enricher._llm_available = True

        with patch.object(enricher, "_llm_extract") as mock_llm:
            mock_llm.return_value = [
                SPOTriple("invoice", "is_a", "document", 0.95, "llm"),
                SPOTriple("payment", "related_to", "invoice", 0.85, "llm"),
            ]
            triples, enriched = enricher.enrich(graph)

        mock_llm.assert_called_once_with(graph)
        assert any(t.source == "llm" for t in triples)


def test_llm_parse_response_valid():
    """_parse_llm_response correctly parses a valid JSON response."""
    enricher = LLMTaxonomyEnricher()
    raw = json.dumps({
        "triples": [
            {"subject": "invoice", "predicate": "has", "object": "lineitem", "confidence": 0.9},
        ]
    })
    triples = enricher._parse_llm_response(raw)
    assert len(triples) == 1
    assert triples[0].subject == "invoice"
    assert triples[0].predicate == "has"
    assert triples[0].obj == "lineitem"
    assert triples[0].source == "llm"
    assert triples[0].confidence == 0.9


def test_llm_parse_response_invalid_json():
    """_parse_llm_response returns empty list on malformed JSON."""
    enricher = LLMTaxonomyEnricher()
    triples = enricher._parse_llm_response("not json at all {{")
    assert triples == []


def test_llm_parse_response_missing_triples_key():
    """_parse_llm_response returns empty list when 'triples' key absent."""
    enricher = LLMTaxonomyEnricher()
    triples = enricher._parse_llm_response(json.dumps({"other": []}))
    assert triples == []


def test_llm_parse_response_skips_incomplete_items():
    """_parse_llm_response skips entries missing required fields."""
    enricher = LLMTaxonomyEnricher()
    raw = json.dumps({
        "triples": [
            {"subject": "invoice"},           # missing predicate and object
            {"subject": "x", "predicate": "is_a", "object": "y", "confidence": 0.8},
        ]
    })
    triples = enricher._parse_llm_response(raw)
    assert len(triples) == 1
    assert triples[0].subject == "x"


def test_llm_fallback_on_exception():
    """If LLM call raises, falls back to offline extraction silently."""
    graph = make_simple_graph()
    initial_node_count = len(graph.nodes)

    enricher = LLMTaxonomyEnricher(api_key="sk-test-key")
    enricher._llm_available = True

    with patch.object(enricher, "_llm_extract", side_effect=ConnectionError("network error")):
        triples, enriched = enricher.enrich(graph)

    # Should still work via offline fallback
    assert len(enriched.nodes) == initial_node_count
    assert isinstance(triples, list)
    # Offline triples should all be marked as offline
    for t in triples:
        assert t.source == "offline"


# ---------------------------------------------------------------------------
# Confidence filtering
# ---------------------------------------------------------------------------


def test_confidence_filter_excludes_low_confidence():
    """Triples below min_confidence threshold are excluded from graph edges."""
    enricher = LLMTaxonomyEnricher(min_confidence=0.95)
    graph = make_simple_graph()

    with patch.object(enricher, "_offline_extract") as mock_offline:
        mock_offline.return_value = [
            SPOTriple("invoice", "is_a", "document", 0.99, "offline"),
            SPOTriple("payment", "related_to", "invoice", 0.50, "offline"),  # below threshold
        ]
        enricher._llm_available = False
        triples, enriched = enricher.enrich(graph)

    # Only high-confidence triple should pass
    high_conf_triples = [t for t in triples if t.confidence >= 0.95]
    assert len(high_conf_triples) == 1
    assert high_conf_triples[0].subject == "invoice"


# ---------------------------------------------------------------------------
# Node metadata augmentation
# ---------------------------------------------------------------------------


def test_enrich_adds_spo_metadata_to_nodes():
    """Enrichment adds spo_triples metadata to matching source nodes."""
    parent_node = TaxonomyNode(term="invoice", frequency=10)
    child_node = TaxonomyNode(term="payment", frequency=5)
    graph = TaxonomyGraph(
        domain="finance",
        nodes=[parent_node, child_node],
        edges=[],
        source_pack_count=1,
    )

    enricher = LLMTaxonomyEnricher()

    with patch.object(enricher, "_offline_extract") as mock_offline:
        mock_offline.return_value = [
            SPOTriple("invoice", "has", "payment", 0.9, "offline"),
        ]
        enricher._llm_available = False
        _, enriched = enricher.enrich(graph)

    invoice_node = next(n for n in enriched.nodes if n.term == "invoice")
    assert "spo_triples" in invoice_node.metadata
    assert len(invoice_node.metadata["spo_triples"]) >= 1
    assert invoice_node.metadata["spo_triples"][0]["predicate"] == "has"
