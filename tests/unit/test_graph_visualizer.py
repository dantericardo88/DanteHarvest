"""
Tests for GraphVisualizer — TaxonomyGraph → DOT / Mermaid string rendering.

No graphviz package required — pure string output only.
"""

from __future__ import annotations

import re

import pytest

from harvest_distill.packs.pack_schemas import PackStep, WorkflowPack
from harvest_distill.taxonomy.graph_visualizer import (
    GraphVisualizer,
    _dot_escape,
    _mermaid_escape,
    _term_to_dot_id,
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


def make_graph() -> TaxonomyGraph:
    packs = [
        make_pack(
            "wf-001",
            "invoice processing workflow",
            ["invoice review", "invoice approval", "invoice payment", "payment confirmation"],
        ),
        make_pack(
            "wf-002",
            "payment reconciliation process",
            ["invoice verification", "payment matching", "payment reconciliation"],
        ),
    ]
    builder = TaxonomyBuilder(domain="accounting", min_frequency=1)
    return builder.build(packs)


def make_minimal_graph() -> TaxonomyGraph:
    nodes = [
        TaxonomyNode(term="invoice", frequency=5, parent=None, children=["payment"]),
        TaxonomyNode(term="payment", frequency=3, parent="invoice"),
    ]
    edges = [("payment", "is_a", "invoice")]
    return TaxonomyGraph(domain="finance", nodes=nodes, edges=edges, source_pack_count=1)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


def test_dot_escape_quotes():
    result = _dot_escape('say "hello"')
    assert '\\"' in result
    assert '"hello"' not in result


def test_dot_escape_backslash():
    result = _dot_escape("path\\file")
    assert "\\\\" in result


def test_dot_escape_newline():
    result = _dot_escape("line1\nline2")
    assert "\\n" in result


def test_mermaid_escape_special_chars():
    result = _mermaid_escape("node[label]")
    assert "[" not in result
    assert "]" not in result


def test_term_to_dot_id_clean():
    assert _term_to_dot_id("invoice") == "invoice"


def test_term_to_dot_id_hyphen():
    result = _term_to_dot_id("invoice-payment")
    assert re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", result)


def test_term_to_dot_id_digit_prefix():
    result = _term_to_dot_id("123term")
    assert result[0].isalpha() or result[0] == "_"


# ---------------------------------------------------------------------------
# DOT output tests
# ---------------------------------------------------------------------------


def test_to_dot_returns_string():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert isinstance(result, str)


def test_to_dot_starts_with_digraph():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert result.strip().startswith("digraph")


def test_to_dot_contains_opening_and_closing_brace():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "{" in result
    assert result.strip().endswith("}")


def test_to_dot_contains_all_nodes():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "invoice" in result
    assert "payment" in result


def test_to_dot_contains_edge():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    # Edge should be represented as a -> b
    assert "->" in result


def test_to_dot_edge_has_label():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "is_a" in result


def test_to_dot_rankdir_applied():
    viz = GraphVisualizer(rankdir="LR")
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "rankdir=LR" in result


def test_to_dot_custom_graph_name():
    viz = GraphVisualizer(graph_name="MyTaxonomy")
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "MyTaxonomy" in result


def test_to_dot_uses_domain_as_name_when_no_graph_name():
    viz = GraphVisualizer()
    graph = make_minimal_graph()  # domain = "finance"
    result = viz.to_dot(graph)
    assert "finance" in result


def test_to_dot_no_duplicate_edges():
    """Multiple same edges in graph.edges should not produce duplicate DOT edges."""
    graph = TaxonomyGraph(
        domain="test",
        nodes=[
            TaxonomyNode(term="foo", frequency=2),
            TaxonomyNode(term="bar", frequency=1),
        ],
        edges=[("foo", "is_a", "bar"), ("foo", "is_a", "bar")],  # duplicate
        source_pack_count=1,
    )
    viz = GraphVisualizer()
    result = viz.to_dot(graph)
    # Count occurrences of the edge arrow between foo and bar
    edge_count = result.count("foo -> bar")
    assert edge_count == 1


def test_to_dot_node_shape_applied():
    viz = GraphVisualizer(node_shape="box")
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "shape=box" in result


def test_to_dot_frequency_in_label():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    # Frequency label in DOT uses f= notation
    assert "f=" in result


def test_to_dot_highlight_top_n():
    """Top N nodes should have 'filled' + fillcolor styling."""
    viz = GraphVisualizer(highlight_top_n=1)
    graph = make_minimal_graph()
    result = viz.to_dot(graph)
    assert "filled" in result
    assert "fillcolor" in result


def test_to_dot_larger_graph():
    viz = GraphVisualizer()
    graph = make_graph()
    result = viz.to_dot(graph)
    assert isinstance(result, str)
    assert "digraph" in result
    # All node terms should appear somewhere in the DOT output
    for node in graph.nodes:
        assert node.term in result


def test_to_dot_empty_edges():
    viz = GraphVisualizer()
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="alpha", frequency=3)],
        edges=[],
        source_pack_count=1,
    )
    result = viz.to_dot(graph)
    assert "->" not in result


def test_to_dot_malformed_edge_skipped():
    """Malformed edges (not 3-tuples) must not raise."""
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="foo", frequency=1), TaxonomyNode(term="bar", frequency=1)],
        edges=[("only_one",), ("valid", "is_a", "bar")],
        source_pack_count=1,
    )
    viz = GraphVisualizer()
    result = viz.to_dot(graph)
    assert isinstance(result, str)


def test_to_dot_label_truncated():
    viz = GraphVisualizer(max_label_length=10)
    long_term = "averylongterminvoice"
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term=long_term, frequency=1)],
        edges=[],
        source_pack_count=1,
    )
    result = viz.to_dot(graph)
    assert "..." in result


# ---------------------------------------------------------------------------
# Mermaid output tests
# ---------------------------------------------------------------------------


def test_to_mermaid_returns_string():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert isinstance(result, str)


def test_to_mermaid_starts_with_flowchart():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert result.strip().startswith("flowchart")


def test_to_mermaid_contains_all_nodes():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert "invoice" in result
    assert "payment" in result


def test_to_mermaid_contains_edge_arrow():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert "-->" in result


def test_to_mermaid_edge_label():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert "is_a" in result


def test_to_mermaid_classDef_present():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert "classDef highlight" in result


def test_to_mermaid_no_duplicate_edges():
    graph = TaxonomyGraph(
        domain="test",
        nodes=[
            TaxonomyNode(term="foo", frequency=2),
            TaxonomyNode(term="bar", frequency=1),
        ],
        edges=[("foo", "is_a", "bar"), ("foo", "is_a", "bar")],
        source_pack_count=1,
    )
    viz = GraphVisualizer()
    result = viz.to_mermaid(graph)
    # Count edge occurrences
    matches = re.findall(r"foo\s*-->", result)
    assert len(matches) == 1


def test_to_mermaid_domain_in_comment():
    viz = GraphVisualizer()
    graph = make_minimal_graph()  # domain = "finance"
    result = viz.to_mermaid(graph)
    assert "finance" in result


def test_to_mermaid_highlight_class_applied():
    viz = GraphVisualizer(highlight_top_n=1)
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert ":::highlight" in result


def test_to_mermaid_larger_graph():
    viz = GraphVisualizer()
    graph = make_graph()
    result = viz.to_mermaid(graph)
    assert isinstance(result, str)
    assert "flowchart" in result


def test_to_mermaid_empty_edges():
    viz = GraphVisualizer()
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="alpha", frequency=3)],
        edges=[],
        source_pack_count=1,
    )
    result = viz.to_mermaid(graph)
    assert "-->" not in result


def test_to_mermaid_malformed_edge_skipped():
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="foo", frequency=1), TaxonomyNode(term="bar", frequency=1)],
        edges=[("only_one",), ("valid", "is_a", "bar")],
        source_pack_count=1,
    )
    viz = GraphVisualizer()
    result = viz.to_mermaid(graph)
    assert isinstance(result, str)


def test_to_mermaid_frequency_in_label():
    viz = GraphVisualizer()
    graph = make_minimal_graph()
    result = viz.to_mermaid(graph)
    assert "f=" in result


# ---------------------------------------------------------------------------
# Cross-format consistency
# ---------------------------------------------------------------------------


def test_dot_and_mermaid_both_contain_same_nodes():
    viz = GraphVisualizer()
    graph = make_graph()
    dot = viz.to_dot(graph)
    mermaid = viz.to_mermaid(graph)
    for node in graph.nodes:
        assert node.term in dot, f"Node '{node.term}' missing from DOT output"
        assert node.term in mermaid, f"Node '{node.term}' missing from Mermaid output"


def test_visualizer_default_config():
    """Default configuration produces valid output without errors."""
    viz = GraphVisualizer()
    graph = make_graph()
    dot = viz.to_dot(graph)
    mermaid = viz.to_mermaid(graph)
    assert len(dot) > 50
    assert len(mermaid) > 50
