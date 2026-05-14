"""
GraphVisualizer — render a TaxonomyGraph as DOT/Graphviz or Mermaid strings.

Harvested from: Graphviz DOT language spec + Mermaid diagram notation patterns.

Pure string generation — no graphviz package required at import or runtime.
All output is valid DOT / Mermaid syntax that can be piped to external renderers.

Constitutional guarantees:
- Zero external dependencies: pure Python string building only.
- Deterministic output: node/edge order is stable (sorted by term).
- Safe escaping: node labels are quoted and special characters escaped.
- Round-trip friendly: DOT output parseable by pydot / graphviz CLI tools.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from harvest_distill.taxonomy.taxonomy_builder import TaxonomyGraph


def _dot_escape(text: str) -> str:
    """Escape a string for use as a DOT label."""
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _mermaid_escape(text: str) -> str:
    """Escape a string for use as a Mermaid node label."""
    return re.sub(r'["\[\](){}|]', "_", text)


def _term_to_dot_id(term: str) -> str:
    """Convert a term to a valid DOT node identifier."""
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", term)
    if clean and clean[0].isdigit():
        clean = "n_" + clean
    return clean or "n_unknown"


class GraphVisualizer:
    """
    Render a TaxonomyGraph as DOT or Mermaid diagram strings.

    Usage:
        viz = GraphVisualizer()
        dot_str = viz.to_dot(graph)
        mermaid_str = viz.to_mermaid(graph)

    Optional configuration:
        viz = GraphVisualizer(
            graph_name="MyTaxonomy",
            node_shape="ellipse",         # DOT: box, ellipse, circle, diamond, etc.
            highlight_top_n=5,            # Bold top-N highest-frequency nodes
            max_label_length=40,          # Truncate long labels in output
            rankdir="TB",                 # DOT layout: TB, LR, BT, RL
        )
    """

    def __init__(
        self,
        graph_name: Optional[str] = None,
        node_shape: str = "ellipse",
        highlight_top_n: int = 5,
        max_label_length: int = 40,
        rankdir: str = "TB",
    ) -> None:
        self.graph_name = graph_name
        self.node_shape = node_shape
        self.highlight_top_n = highlight_top_n
        self.max_label_length = max_label_length
        self.rankdir = rankdir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_dot(self, graph: TaxonomyGraph) -> str:
        """
        Render TaxonomyGraph as a Graphviz DOT string.

        Returns a valid DOT language string. Pipe to `dot -Tpng` for rendering.
        """
        name = _dot_escape(self.graph_name or graph.domain or "taxonomy")
        lines = [
            f'digraph "{name}" {{',
            f'    rankdir={self.rankdir};',
            '    node [fontname="Helvetica", fontsize=11];',
            '    edge [fontsize=9, color="#555555"];',
            "",
        ]

        top_terms = {graph.nodes[i].term for i in range(min(self.highlight_top_n, len(graph.nodes)))}
        sorted_nodes = sorted(graph.nodes, key=lambda n: n.term)

        # Node definitions
        lines.append("    // Nodes")
        for node in sorted_nodes:
            node_id = _term_to_dot_id(node.term)
            label = self._truncate(node.term)
            label_escaped = _dot_escape(label)
            freq_label = f"{label_escaped}\\n(f={node.frequency})"

            attrs: list[str] = [
                f'label="{freq_label}"',
                f"shape={self.node_shape}",
            ]
            if node.term in top_terms:
                attrs.append('style="bold,filled"')
                attrs.append('fillcolor="#D6EAF8"')
            else:
                attrs.append('style="filled"')
                attrs.append('fillcolor="#FDFEFE"')

            attrs_str = ", ".join(attrs)
            lines.append(f"    {node_id} [{attrs_str}];")

        lines.append("")
        lines.append("    // Edges")

        seen_edges: set = set()
        for edge in graph.edges:
            stmt = self._dot_edge(edge)
            if stmt and stmt not in seen_edges:
                lines.append(f"    {stmt}")
                seen_edges.add(stmt)

        lines.append("}")
        return "\n".join(lines)

    def to_mermaid(self, graph: TaxonomyGraph) -> str:
        """
        Render TaxonomyGraph as a Mermaid flowchart string.

        Returns a valid Mermaid diagram string compatible with:
        - GitHub Markdown ```mermaid``` code fences
        - Mermaid Live Editor (https://mermaid.live)
        - Most modern documentation tools (Notion, Obsidian, etc.)
        """
        lines = [
            "flowchart TD",
            f"    %% TaxonomyGraph — domain: {_mermaid_escape(graph.domain)}",
            f"    %% {len(graph.nodes)} nodes, {len(graph.edges)} edges",
            "",
        ]

        top_terms = {graph.nodes[i].term for i in range(min(self.highlight_top_n, len(graph.nodes)))}
        sorted_nodes = sorted(graph.nodes, key=lambda n: n.term)

        # Node definitions with labels
        lines.append("    %% Nodes")
        for node in sorted_nodes:
            node_id = _term_to_dot_id(node.term)
            label = self._truncate(node.term)
            label_safe = _mermaid_escape(label)
            freq_label = f"{label_safe} [f={node.frequency}]"

            if node.term in top_terms:
                lines.append(f"    {node_id}[{freq_label}]:::highlight")
            else:
                lines.append(f"    {node_id}({freq_label})")

        lines.append("")
        lines.append("    %% Edges")

        seen_edges: set = set()
        for edge in graph.edges:
            stmt = self._mermaid_edge(edge)
            if stmt and stmt not in seen_edges:
                lines.append(f"    {stmt}")
                seen_edges.add(stmt)

        # Style definitions
        lines.append("")
        lines.append("    classDef highlight fill:#D6EAF8,stroke:#2980B9,font-weight:bold")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _truncate(self, text: str) -> str:
        if len(text) <= self.max_label_length:
            return text
        return text[: self.max_label_length - 3] + "..."

    def _dot_edge(self, edge: Any) -> Optional[str]:
        """Convert an edge tuple to a DOT edge statement."""
        if not (isinstance(edge, (list, tuple)) and len(edge) >= 3):
            return None
        src, predicate, dst = str(edge[0]), str(edge[1]), str(edge[2])
        src_id = _term_to_dot_id(src)
        dst_id = _term_to_dot_id(dst)
        pred_escaped = _dot_escape(predicate)
        return f'{src_id} -> {dst_id} [label="{pred_escaped}"];'

    def _mermaid_edge(self, edge: Any) -> Optional[str]:
        """Convert an edge tuple to a Mermaid edge statement."""
        if not (isinstance(edge, (list, tuple)) and len(edge) >= 3):
            return None
        src, predicate, dst = str(edge[0]), str(edge[1]), str(edge[2])
        src_id = _term_to_dot_id(src)
        dst_id = _term_to_dot_id(dst)
        pred_safe = _mermaid_escape(predicate)
        return f"{src_id} -->|{pred_safe}| {dst_id}"
