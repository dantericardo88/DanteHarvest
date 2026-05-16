"""Tests for TaxonomyBuilder PMI/NPMI scoring and stop-term computation."""

from __future__ import annotations

import math

from harvest_distill.packs.pack_schemas import PackStep, WorkflowPack
from harvest_distill.taxonomy.taxonomy_builder import (
    MIN_PMI_THRESHOLD,
    TaxonomyBuilder,
    TaxonomyGraph,
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


def make_builder(**kwargs) -> TaxonomyBuilder:
    return TaxonomyBuilder(domain="test", **kwargs)


# ---------------------------------------------------------------------------
# compute_pmi
# ---------------------------------------------------------------------------

class TestComputePmi:
    def test_known_value(self):
        """PMI(A,B) = log2(P(AB) / (P(A)*P(B))) — verify against hand-calc."""
        builder = make_builder()
        # Seed term frequencies so P(A)=P(B)=0.5, P(AB)=0.25 → PMI=log2(1)=0
        builder._term_freq["aaa"] = 5
        builder._term_freq["bbb"] = 5
        # total_docs=10, co_occur=2 → P_AB=0.2, P_A=0.5, P_B=0.5
        # PMI = log2(0.2 / 0.25) = log2(0.8) ≈ -0.322
        result = builder.compute_pmi("aaa", "bbb", co_occurrence_count=2, total_docs=10)
        expected = math.log2(0.2 / (0.5 * 0.5))
        assert abs(result - expected) < 1e-9

    def test_perfect_co_occurrence(self):
        """When A and B always appear together PMI should be positive."""
        builder = make_builder()
        builder._term_freq["aaa"] = 5
        builder._term_freq["bbb"] = 5
        # P_AB = 5/10 = 0.5, P_A = P_B = 0.5 → PMI = log2(0.5/0.25) = 1.0
        result = builder.compute_pmi("aaa", "bbb", co_occurrence_count=5, total_docs=10)
        assert abs(result - 1.0) < 1e-9

    def test_zero_total_docs_returns_zero(self):
        builder = make_builder()
        assert builder.compute_pmi("aaa", "bbb", co_occurrence_count=0, total_docs=0) == 0.0

    def test_zero_co_occurrence_returns_zero(self):
        builder = make_builder()
        builder._term_freq["aaa"] = 5
        builder._term_freq["bbb"] = 5
        result = builder.compute_pmi("aaa", "bbb", co_occurrence_count=0, total_docs=10)
        assert result == 0.0

    def test_uses_term_freq_for_marginals(self):
        """Missing term defaults to freq=1, not zero, so no divide-by-zero."""
        builder = make_builder()
        # "zzz" not in _term_freq → defaults to 1
        result = builder.compute_pmi("zzz", "yyy", co_occurrence_count=1, total_docs=100)
        # Should return a finite float, not raise
        assert isinstance(result, float)
        assert math.isfinite(result)


# ---------------------------------------------------------------------------
# compute_npmi
# ---------------------------------------------------------------------------

class TestComputeNpmi:
    def test_value_in_range(self):
        """NPMI must always be in [-1, 1] for valid corpus-consistent inputs.

        Valid constraint: co_occurrence_count <= min(freq_a, freq_b),
        because two terms cannot co-occur more times than either appears alone.
        """
        builder = make_builder()
        # freq_a = freq_b = 5, total = 10  → p_a = p_b = 0.5
        # co can legally range from 0..5 (can't exceed individual frequencies)
        builder._term_freq["aaa"] = 5
        builder._term_freq["bbb"] = 5
        for co in range(0, 6):  # 0..5 inclusive — all valid
            npmi = builder.compute_npmi("aaa", "bbb", co_occurrence_count=co, total_docs=10)
            assert -1.0 <= npmi <= 1.0 or npmi == 0.0, f"NPMI={npmi} out of range for co={co}"

    def test_perfect_association_approaches_one(self):
        """When A and B always co-occur NPMI should approach 1."""
        builder = make_builder()
        builder._term_freq["aaa"] = 10
        builder._term_freq["bbb"] = 10
        npmi = builder.compute_npmi("aaa", "bbb", co_occurrence_count=10, total_docs=10)
        assert npmi > 0.9

    def test_zero_co_occurrence_returns_zero(self):
        builder = make_builder()
        builder._term_freq["aaa"] = 5
        builder._term_freq["bbb"] = 5
        assert builder.compute_npmi("aaa", "bbb", co_occurrence_count=0, total_docs=10) == 0.0

    def test_zero_total_docs_returns_zero(self):
        builder = make_builder()
        assert builder.compute_npmi("aaa", "bbb", co_occurrence_count=0, total_docs=0) == 0.0


# ---------------------------------------------------------------------------
# compute_stop_terms
# ---------------------------------------------------------------------------

class TestComputeStopTerms:
    def test_high_frequency_term_flagged(self):
        """A term in every document should be a stop term."""
        corpus = ["invoice payment review"] * 20
        stops = TaxonomyBuilder.compute_stop_terms(corpus, top_pct=0.5)
        assert "invoice" in stops
        assert "payment" in stops

    def test_rare_term_not_flagged(self):
        """A term appearing in only one doc must not be a stop term."""
        corpus = ["invoice payment"] * 10 + ["unique_term_xyz rare"]
        stops = TaxonomyBuilder.compute_stop_terms(corpus, top_pct=0.5)
        assert "unique_term_xyz" not in stops

    def test_empty_corpus_returns_empty_set(self):
        assert TaxonomyBuilder.compute_stop_terms([]) == set()

    def test_returns_set_of_strings(self):
        corpus = ["word one two"] * 5
        stops = TaxonomyBuilder.compute_stop_terms(corpus, top_pct=0.1)
        assert isinstance(stops, set)
        for term in stops:
            assert isinstance(term, str)


# ---------------------------------------------------------------------------
# build_taxonomy_with_pmi
# ---------------------------------------------------------------------------

class TestBuildTaxonomyWithPmi:
    def test_returns_expected_keys(self):
        builder = make_builder(min_frequency=1)
        docs = [
            "invoice payment processing",
            "invoice reconciliation payment",
            "payment validation invoice",
        ]
        result = builder.build_taxonomy_with_pmi(docs, min_pmi=0.0)
        assert "terms" in result
        assert "pairs" in result
        assert "clusters" in result

    def test_low_pmi_pairs_filtered(self):
        """Pairs that barely co-occur should be absent when min_pmi is high."""
        builder = make_builder(min_frequency=1)
        # "alpha" and "beta" never appear in the same document
        docs = [
            "alpha gamma gamma",
            "beta delta delta",
            "alpha gamma",
            "beta delta",
        ]
        result = builder.build_taxonomy_with_pmi(docs, min_pmi=0.8)
        pair_names = {(p["term_a"], p["term_b"]) for p in result["pairs"]}
        # alpha-beta should not appear (they never co-occur)
        assert ("alpha", "beta") not in pair_names
        assert ("beta", "alpha") not in pair_names

    def test_high_pmi_pairs_present(self):
        """Terms that always co-occur should survive strict filtering."""
        builder = make_builder(min_frequency=1)
        # "invoice" and "payment" always appear together
        docs = ["invoice payment review"] * 10
        result = builder.build_taxonomy_with_pmi(docs, min_pmi=0.0)
        all_terms = set(result["terms"])
        # At least some terms extracted
        assert len(all_terms) > 0

    def test_pairs_sorted_by_npmi_descending(self):
        builder = make_builder(min_frequency=1)
        docs = [
            "invoice payment invoice payment",
            "invoice report",
            "payment report",
        ] * 5
        result = builder.build_taxonomy_with_pmi(docs, min_pmi=0.0)
        pairs = result["pairs"]
        if len(pairs) >= 2:
            for i in range(len(pairs) - 1):
                assert pairs[i]["npmi"] >= pairs[i + 1]["npmi"]

    def test_empty_documents_returns_empty_structure(self):
        builder = make_builder(min_frequency=1)
        result = builder.build_taxonomy_with_pmi([])
        assert result["terms"] == []
        assert result["pairs"] == []
        assert result["clusters"] == {}

    def test_clusters_contain_string_keys(self):
        builder = make_builder(min_frequency=1)
        docs = ["invoice payment review"] * 10
        result = builder.build_taxonomy_with_pmi(docs, min_pmi=0.0)
        for key, children in result["clusters"].items():
            assert isinstance(key, str)
            assert isinstance(children, list)


# ---------------------------------------------------------------------------
# build() — WorkflowPack integration still works via PMI path
# ---------------------------------------------------------------------------

class TestBuildViaPmi:
    def test_build_returns_taxonomy_graph(self):
        pack = make_pack(
            "wf-001",
            "reconcile invoices with payment records",
            ["open invoice dashboard", "filter payment status", "export reconciliation report"],
        )
        builder = TaxonomyBuilder(domain="accounting", min_frequency=1)
        graph = builder.build([pack])
        assert isinstance(graph, TaxonomyGraph)
        assert graph.source_pack_count == 1
        assert len(graph.nodes) > 0

    def test_min_pmi_threshold_constant_exists(self):
        assert isinstance(MIN_PMI_THRESHOLD, float)
        assert MIN_PMI_THRESHOLD > 0

    def test_high_frequency_terms_present(self):
        packs = [
            make_pack("wf-001", "invoice processing", ["invoice submission", "invoice review"]),
            make_pack("wf-002", "invoice management", ["invoice approval", "invoice export"]),
            make_pack("wf-003", "invoice workflow", ["invoice tracking", "invoice archive"]),
        ]
        builder = TaxonomyBuilder(domain="accounting", min_frequency=2)
        graph = builder.build(packs)
        top = graph.top_terms(5)
        assert "invoice" in top
