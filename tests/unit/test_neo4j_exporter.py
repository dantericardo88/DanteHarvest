"""
Tests for Neo4jExporter — TaxonomyGraph → Cypher statement generation.

Covers:
- Pure Cypher string output (no neo4j driver required)
- MERGE node statement syntax
- MERGE edge statement syntax
- File write round-trip
- Lazy driver import / graceful failure without neo4j installed
- Export to Neo4j live connection (mocked driver)
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from harvest_distill.packs.pack_schemas import PackStep, WorkflowPack
from harvest_distill.taxonomy.neo4j_exporter import Neo4jExporter, _escape_cypher_string, _term_to_id
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
    """Graph with manually specified nodes and edges for predictable testing."""
    nodes = [
        TaxonomyNode(term="invoice", frequency=5, parent=None, children=["payment"]),
        TaxonomyNode(term="payment", frequency=3, parent="invoice"),
    ]
    edges = [("payment", "is_a", "invoice")]
    return TaxonomyGraph(domain="finance", nodes=nodes, edges=edges, source_pack_count=1)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


def test_escape_cypher_string_apostrophe():
    result = _escape_cypher_string("o'reilly")
    assert "\\'" in result
    assert "o'reilly" not in result


def test_escape_cypher_string_backslash():
    result = _escape_cypher_string("path\\to\\file")
    assert "\\\\" in result


def test_escape_cypher_string_clean():
    assert _escape_cypher_string("invoice") == "invoice"


def test_term_to_id_clean():
    assert _term_to_id("invoice") == "invoice"


def test_term_to_id_special_chars():
    result = _term_to_id("invoice-payment")
    assert re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", result), f"Invalid id: {result}"


def test_term_to_id_starts_with_digit():
    result = _term_to_id("123invoice")
    assert result[0].isalpha() or result[0] == "_"


# ---------------------------------------------------------------------------
# Cypher statement generation
# ---------------------------------------------------------------------------


def test_to_cypher_statements_returns_list():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    assert isinstance(statements, list)
    assert len(statements) > 0


def test_to_cypher_statements_has_comment_header():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    # First statement should be a comment
    assert statements[0].startswith("//")
    assert "finance" in statements[0]


def test_to_cypher_statements_node_count():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    # 1 comment + 2 nodes + 1 edge = 4 statements
    merge_stmts = [s for s in statements if s.startswith("MERGE") or s.startswith("MATCH")]
    assert len(merge_stmts) >= 3  # at least 2 nodes + 1 edge


def test_node_statement_contains_term():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    node_stmts = [s for s in statements if "MERGE" in s and "term:" in s]
    terms_found = set()
    for stmt in node_stmts:
        if "invoice" in stmt:
            terms_found.add("invoice")
        if "payment" in stmt:
            terms_found.add("payment")
    assert "invoice" in terms_found
    assert "payment" in terms_found


def test_node_statement_contains_frequency():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    node_stmts = [s for s in statements if "MERGE" in s and "frequency:" in s]
    assert len(node_stmts) >= 1


def test_node_statement_uses_configured_label():
    exporter = Neo4jExporter(node_label="DomainTerm")
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    node_stmts = [s for s in statements if "DomainTerm" in s]
    assert len(node_stmts) >= 1


def test_edge_statement_contains_relationship_type():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    edge_stmts = [s for s in statements if "MATCH" in s and "MERGE" in s]
    assert len(edge_stmts) >= 1
    # Relationship type should be uppercased predicate
    assert any("IS_A" in s for s in edge_stmts)


def test_edge_statement_contains_src_and_dst():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    edge_stmts = [s for s in statements if "MATCH" in s]
    combined = " ".join(edge_stmts)
    assert "invoice" in combined
    assert "payment" in combined


def test_to_cypher_string_is_str():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    result = exporter.to_cypher_string(graph)
    assert isinstance(result, str)
    assert len(result) > 0


def test_to_cypher_string_contains_all_nodes():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()
    cypher = exporter.to_cypher_string(graph)
    assert "invoice" in cypher
    assert "payment" in cypher


def test_larger_graph_generates_all_node_statements():
    exporter = Neo4jExporter()
    graph = make_graph()
    statements = exporter.to_cypher_statements(graph)
    merge_nodes = [s for s in statements if s.startswith("MERGE") and "term:" in s]
    assert len(merge_nodes) == len(graph.nodes)


def test_edge_label_prefix_applied():
    exporter = Neo4jExporter(edge_label_prefix="TAX_")
    graph = make_minimal_graph()
    statements = exporter.to_cypher_statements(graph)
    edge_stmts = [s for s in statements if "MATCH" in s]
    assert any("TAX_IS_A" in s for s in edge_stmts)


# ---------------------------------------------------------------------------
# Malformed edge handling
# ---------------------------------------------------------------------------


def test_malformed_edge_skipped():
    """Edges that are not 3-tuples should be skipped gracefully."""
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="foo", frequency=1)],
        edges=[("only_two",), ("also", "short"), ("valid", "is_a", "edge")],
        source_pack_count=1,
    )
    exporter = Neo4jExporter()
    # Should not raise
    statements = exporter.to_cypher_statements(graph)
    assert isinstance(statements, list)


def test_empty_edges_graph():
    """Graph with no edges generates only node statements."""
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="invoice", frequency=3)],
        edges=[],
        source_pack_count=1,
    )
    exporter = Neo4jExporter()
    statements = exporter.to_cypher_statements(graph)
    match_stmts = [s for s in statements if s.startswith("MATCH")]
    assert len(match_stmts) == 0


# ---------------------------------------------------------------------------
# File output
# ---------------------------------------------------------------------------


def test_write_cypher_file_creates_file():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "output.cypher"
        result_path = exporter.write_cypher_file(graph, output_path)

        assert result_path.exists()
        content = result_path.read_text(encoding="utf-8")
        assert "invoice" in content
        assert "MERGE" in content


def test_write_cypher_file_returns_resolved_path():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "sub" / "output.cypher"
        result_path = exporter.write_cypher_file(graph, output_path)
        assert result_path.is_absolute()
        assert result_path.exists()


def test_write_cypher_file_content_matches_to_cypher_string():
    exporter = Neo4jExporter()
    graph = make_minimal_graph()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "output.cypher"
        exporter.write_cypher_file(graph, output_path)
        file_content = output_path.read_text(encoding="utf-8")
        direct_content = exporter.to_cypher_string(graph)
        assert file_content == direct_content


# ---------------------------------------------------------------------------
# Live Neo4j export — no URI raises RuntimeError
# ---------------------------------------------------------------------------


def test_export_to_neo4j_raises_without_uri():
    exporter = Neo4jExporter()  # no uri
    graph = make_minimal_graph()
    with pytest.raises(RuntimeError, match="uri must be set"):
        exporter.export_to_neo4j(graph)


def test_export_to_neo4j_raises_without_driver():
    """If neo4j package not installed, export_to_neo4j raises RuntimeError."""
    exporter = Neo4jExporter(uri="bolt://localhost:7687", user="neo4j", password="test")
    graph = make_minimal_graph()

    with patch.dict("sys.modules", {"neo4j": None}):
        with pytest.raises(RuntimeError, match="neo4j driver not installed"):
            exporter.export_to_neo4j(graph)


def test_export_to_neo4j_mocked_driver():
    """Mocked driver should execute statements and return summary dict."""
    exporter = Neo4jExporter(uri="bolt://localhost:7687", user="neo4j", password="test")
    graph = make_minimal_graph()

    mock_counters = MagicMock()
    mock_counters.nodes_created = 1
    mock_counters.relationships_created = 0

    mock_summary = MagicMock()
    mock_summary.counters = mock_counters

    mock_result = MagicMock()
    mock_result.consume.return_value = mock_summary

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)
    mock_session.run.return_value = mock_result

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    mock_neo4j_module = MagicMock()
    mock_neo4j_module.GraphDatabase.driver.return_value = mock_driver

    with patch.dict("sys.modules", {"neo4j": mock_neo4j_module}):
        result = exporter.export_to_neo4j(graph)

    assert "nodes_created" in result
    assert "edges_created" in result
    assert "errors" in result
    assert isinstance(result["errors"], list)


# ---------------------------------------------------------------------------
# Special character handling
# ---------------------------------------------------------------------------


def test_node_with_special_chars_in_term():
    """Terms with special characters should not break Cypher generation."""
    graph = TaxonomyGraph(
        domain="test",
        nodes=[TaxonomyNode(term="o'reilly-invoice", frequency=2)],
        edges=[],
        source_pack_count=1,
    )
    exporter = Neo4jExporter()
    statements = exporter.to_cypher_statements(graph)
    # Should not raise and the apostrophe should be escaped
    combined = " ".join(statements)
    assert "o\\'reilly" in combined or "o'reilly" not in combined or "\\'" in combined


def test_node_parent_included_in_statement():
    """Node with a parent term should have parent property in Cypher."""
    graph = TaxonomyGraph(
        domain="test",
        nodes=[
            TaxonomyNode(term="invoice", frequency=5),
            TaxonomyNode(term="payment", frequency=3, parent="invoice"),
        ],
        edges=[],
        source_pack_count=1,
    )
    exporter = Neo4jExporter()
    statements = exporter.to_cypher_statements(graph)
    payment_stmts = [s for s in statements if "payment" in s and "MERGE" in s and "SET" in s]
    assert any("parent" in s for s in payment_stmts)
