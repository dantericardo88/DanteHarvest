"""Tests for TaxonomyBuilder — domain taxonomy from WorkflowPacks."""

import pytest
from harvest_core.control.exceptions import PackagingError
from harvest_distill.packs.pack_schemas import PackStep, WorkflowPack
from harvest_distill.taxonomy.taxonomy_builder import TaxonomyBuilder, TaxonomyGraph


def make_pack(pack_id: str, goal: str, actions: list[str]) -> WorkflowPack:
    return WorkflowPack(
        pack_id=pack_id,
        title=f"Pack {pack_id}",
        goal=goal,
        steps=[PackStep(id=f"s{i}", action=a) for i, a in enumerate(actions)],
    )


def test_empty_packs_raises():
    builder = TaxonomyBuilder(domain="accounting")
    with pytest.raises(PackagingError, match="at least one WorkflowPack"):
        builder.build([])


def test_single_pack_builds_graph():
    pack = make_pack(
        "wf-001",
        "reconcile invoices with payment records",
        ["open invoice dashboard", "filter by payment status", "export reconciliation report"],
    )
    builder = TaxonomyBuilder(domain="accounting", min_frequency=1)
    graph = builder.build([pack])
    assert isinstance(graph, TaxonomyGraph)
    assert graph.source_pack_count == 1
    assert len(graph.nodes) > 0


def test_top_terms_returns_list():
    pack = make_pack(
        "wf-001",
        "invoice processing workflow",
        ["invoice review", "invoice approval", "invoice payment", "payment confirmation"],
    )
    builder = TaxonomyBuilder(min_frequency=1)
    graph = builder.build([pack])
    top = graph.top_terms(5)
    assert isinstance(top, list)
    assert len(top) <= 5
    assert "invoice" in top


def test_frequency_threshold_filters():
    pack = make_pack(
        "wf-001",
        "accounting workflow",
        ["reconcile invoices", "reconcile payments"],
    )
    builder = TaxonomyBuilder(min_frequency=3)
    graph = builder.build([pack])
    terms = [n.term for n in graph.nodes]
    assert "reconcile" not in terms


def test_to_dict_serializable():
    pack = make_pack(
        "wf-001",
        "invoice workflow",
        ["invoice review", "invoice approval"],
    )
    builder = TaxonomyBuilder(min_frequency=1)
    graph = builder.build([pack])
    data = graph.to_dict()
    assert data["domain"] == "general"
    assert "nodes" in data
    assert isinstance(data["node_count"], int)


def test_multiple_packs_increases_frequency():
    packs = [
        make_pack("wf-001", "invoice processing", ["invoice submission", "invoice review"]),
        make_pack("wf-002", "invoice management", ["invoice approval", "invoice export"]),
        make_pack("wf-003", "invoice workflow", ["invoice tracking", "invoice archive"]),
    ]
    builder = TaxonomyBuilder(min_frequency=2)
    graph = builder.build(packs)
    top = graph.top_terms(3)
    assert "invoice" in top


def test_graph_node_fields():
    pack = make_pack("wf-001", "test workflow", ["test action here", "test another action"])
    builder = TaxonomyBuilder(min_frequency=1)
    graph = builder.build([pack])
    for node in graph.nodes:
        assert isinstance(node.term, str)
        assert isinstance(node.frequency, int)
        assert node.frequency >= 1
