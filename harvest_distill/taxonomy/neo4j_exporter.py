"""
Neo4jExporter — export a TaxonomyGraph to Neo4j Cypher statements.

Harvested from: neo4j Python driver patterns + LlamaIndex KG persistence patterns.

Generates CREATE/MERGE Cypher statements for nodes and relationships.
No neo4j driver required at import time — lazy import with graceful fallback.

Constitutional guarantees:
- Driver-optional: importing this module never fails even if neo4j is not installed.
- Pure-string output: `to_cypher_statements()` returns a list of strings — no side effects.
- File output: `write_cypher_file()` writes a `.cypher` file without requiring the driver.
- Live export: `export_to_neo4j()` only attempted when driver available + URI provided.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from harvest_distill.taxonomy.taxonomy_builder import TaxonomyGraph, TaxonomyNode


def _escape_cypher_string(value: str) -> str:
    """Escape a string value for safe embedding in a Cypher statement."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _term_to_id(term: str) -> str:
    """Convert a term to a valid Cypher identifier (variable name)."""
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", term)
    if clean and clean[0].isdigit():
        clean = "t_" + clean
    return clean or "t_unknown"


class Neo4jExporter:
    """
    Export a TaxonomyGraph to Neo4j Cypher statements.

    Usage (pure string / file output — no driver needed):
        exporter = Neo4jExporter()
        statements = exporter.to_cypher_statements(graph)
        exporter.write_cypher_file(graph, Path("output.cypher"))

    Usage (live Neo4j export — requires neo4j driver + running instance):
        exporter = Neo4jExporter(uri="bolt://localhost:7687", user="neo4j", password="secret")
        exporter.export_to_neo4j(graph)
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "neo4j",
        node_label: str = "TaxonomyTerm",
        edge_label_prefix: str = "",
    ) -> None:
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.node_label = node_label
        self.edge_label_prefix = edge_label_prefix

    # ------------------------------------------------------------------
    # Pure-string Cypher generation (no driver required)
    # ------------------------------------------------------------------

    def to_cypher_statements(self, graph: TaxonomyGraph) -> List[str]:
        """
        Generate a list of Cypher MERGE statements for all nodes and edges.

        Returns a list of strings — safe to use without any Neo4j installation.
        """
        statements: List[str] = []

        # Header comment
        statements.append(
            f"// TaxonomyGraph export: domain={_escape_cypher_string(graph.domain)}, "
            f"nodes={len(graph.nodes)}, edges={len(graph.edges)}"
        )

        # Node statements
        for node in graph.nodes:
            statements.append(self._node_statement(node))

        # Edge / relationship statements
        for edge in graph.edges:
            stmt = self._edge_statement(edge)
            if stmt:
                statements.append(stmt)

        return statements

    def to_cypher_string(self, graph: TaxonomyGraph) -> str:
        """Return all Cypher statements joined as a single string."""
        return "\n".join(self.to_cypher_statements(graph))

    def write_cypher_file(self, graph: TaxonomyGraph, output_path: Path) -> Path:
        """
        Write Cypher statements to a .cypher file.

        Args:
            graph: The TaxonomyGraph to export.
            output_path: Destination file path (created/overwritten).

        Returns:
            The resolved path of the written file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cypher = self.to_cypher_string(graph)
        output_path.write_text(cypher, encoding="utf-8")
        return output_path.resolve()

    # ------------------------------------------------------------------
    # Live Neo4j export (lazy driver import)
    # ------------------------------------------------------------------

    def export_to_neo4j(self, graph: TaxonomyGraph) -> Dict[str, Any]:
        """
        Connect to Neo4j and execute all Cypher statements.

        Requires:
        - neo4j Python driver installed: `pip install neo4j`
        - A running Neo4j instance accessible at self.uri

        Returns a summary dict: {"nodes_created": int, "edges_created": int, "errors": list}
        Raises RuntimeError if driver is unavailable or URI not configured.
        """
        if not self.uri:
            raise RuntimeError("Neo4jExporter.uri must be set for live export")

        try:
            from neo4j import GraphDatabase  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "neo4j driver not installed. Install with: pip install neo4j"
            ) from exc

        auth = (self.user or "neo4j", self.password or "")
        driver = GraphDatabase.driver(self.uri, auth=auth)

        results: Dict[str, Any] = {"nodes_created": 0, "edges_created": 0, "errors": []}

        try:
            with driver.session(database=self.database) as session:
                for stmt in self.to_cypher_statements(graph):
                    if stmt.startswith("//"):
                        continue
                    try:
                        summary = session.run(stmt).consume()
                        counters = summary.counters
                        results["nodes_created"] += counters.nodes_created
                        results["edges_created"] += counters.relationships_created
                    except Exception as exc:
                        results["errors"].append(str(exc))
        finally:
            driver.close()

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _node_statement(self, node: TaxonomyNode) -> str:
        """Generate a MERGE statement for a single TaxonomyNode."""
        label = self.node_label
        term_safe = _escape_cypher_string(node.term)
        parent_safe = _escape_cypher_string(node.parent or "")

        props_parts = [
            f"term: '{term_safe}'",
            f"frequency: {node.frequency}",
        ]
        if node.parent:
            props_parts.append(f"parent: '{parent_safe}'")
        if node.children:
            children_str = ", ".join(f"'{_escape_cypher_string(c)}'" for c in node.children)
            props_parts.append(f"children: [{children_str}]")
        if node.metadata:
            for k, v in node.metadata.items():
                if isinstance(v, str):
                    props_parts.append(f"{k}: '{_escape_cypher_string(v)}'")
                elif isinstance(v, (int, float, bool)):
                    props_parts.append(f"{k}: {v}")
                # Skip complex metadata (lists, dicts) in node properties

        props = ", ".join(props_parts)
        var = _term_to_id(node.term)
        return f"MERGE ({var}:{label} {{term: '{term_safe}'}}) SET {var} += {{{props}}};"

    def _edge_statement(self, edge: Any) -> Optional[str]:
        """Generate a MERGE statement for an edge tuple (src, predicate, dst)."""
        if not (isinstance(edge, (list, tuple)) and len(edge) >= 3):
            return None

        src, predicate, dst = str(edge[0]), str(edge[1]), str(edge[2])
        src_safe = _escape_cypher_string(src)
        dst_safe = _escape_cypher_string(dst)

        # Convert predicate to a valid Cypher relationship type
        rel_type = self.edge_label_prefix + re.sub(r"[^a-zA-Z0-9_]", "_", predicate).upper()

        src_var = _term_to_id(src)
        dst_var = _term_to_id(dst) + "_dst"

        return (
            f"MATCH ({src_var}:{self.node_label} {{term: '{src_safe}'}}), "
            f"({dst_var}:{self.node_label} {{term: '{dst_safe}'}}) "
            f"MERGE ({src_var})-[:{rel_type}]->({dst_var});"
        )
