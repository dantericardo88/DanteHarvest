"""Extended tests for knowledge_graph_extraction dimension (6→9).

Covers:
- deduplicate_triples() removes semantic duplicates
- link_entities() maps entities to their triples
- extract_from_markdown() handles bullets with 'is a', tables, and headers
- 15+ relation patterns available in NLPTripleExtractor
"""
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_extractor():
    from harvest_distill.taxonomy.nlp_triple_extractor import NLPTripleExtractor
    return NLPTripleExtractor()


def _make_kg():
    from harvest_distill.taxonomy.knowledge_graph import KnowledgeGraph
    return KnowledgeGraph()  # in-memory


# ---------------------------------------------------------------------------
# 1. deduplicate_triples()
# ---------------------------------------------------------------------------

class TestDeduplicateTriples:
    def test_exact_duplicate_removed(self):
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Python", "is_a", "language", source_text="s1")
        t2 = Triple("Python", "is_a", "language", source_text="s2")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 1

    def test_semantic_duplicate_identity_group(self):
        """is_a and is both map to 'identity' — second should be dropped."""
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Python", "is_a", "language")
        t2 = Triple("Python", "is", "language")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 1

    def test_semantic_duplicate_usage_group(self):
        """uses and leverages both map to 'usage' — second dropped."""
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Django", "uses", "Python")
        t2 = Triple("Django", "leverages", "Python")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 1

    def test_semantic_duplicate_membership_group(self):
        """part_of and belongs_to both map to 'membership'."""
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Spain", "part_of", "Europe")
        t2 = Triple("Spain", "belongs_to", "Europe")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 1

    def test_different_predicates_different_groups_both_kept(self):
        """is_a (identity) and part_of (membership) have different groups."""
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Python", "is_a", "language")
        t2 = Triple("Python", "part_of", "language")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 2

    def test_different_objects_both_kept(self):
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Python", "is_a", "language")
        t2 = Triple("Python", "is_a", "tool")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 2

    def test_case_insensitive_dedup(self):
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Python", "is_a", "Language")
        t2 = Triple("python", "is_a", "language")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 1

    def test_unknown_predicate_uses_predicate_as_own_group(self):
        """Predicates not in any group use themselves — no false dedup."""
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("A", "invented_by", "B")
        t2 = Triple("A", "invented_by", "B")
        result = ext.deduplicate_triples([t1, t2])
        assert len(result) == 1

    def test_empty_input(self):
        ext = _make_extractor()
        assert ext.deduplicate_triples([]) == []

    def test_order_preserved_first_wins(self):
        """The first triple in a semantic group should be kept."""
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("X", "uses", "Y", confidence=0.9, source_text="first")
        t2 = Triple("X", "employs", "Y", confidence=0.5, source_text="second")
        result = ext.deduplicate_triples([t1, t2])
        assert result[0].source_text == "first"


# ---------------------------------------------------------------------------
# 2. link_entities()
# ---------------------------------------------------------------------------

class TestLinkEntities:
    def test_subject_linked(self):
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t = Triple("Python", "is_a", "language")
        mapping = ext.link_entities([t])
        assert "Python" in mapping
        assert t in mapping["Python"]

    def test_object_linked(self):
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t = Triple("Python", "is_a", "language")
        mapping = ext.link_entities([t])
        assert "language" in mapping
        assert t in mapping["language"]

    def test_shared_entity_appears_in_multiple_triples(self):
        ext = _make_extractor()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        t1 = Triple("Django", "uses", "Python")
        t2 = Triple("Flask", "uses", "Python")
        mapping = ext.link_entities([t1, t2])
        # Python appears as object in both
        assert len(mapping["Python"]) == 2

    def test_empty_triples(self):
        ext = _make_extractor()
        assert ext.link_entities([]) == {}

    def test_kg_exposes_link_entities(self):
        """KnowledgeGraph.link_entities() delegates correctly."""
        kg = _make_kg()
        from harvest_distill.taxonomy.nlp_triple_extractor import Triple
        triples = [Triple("A", "is_a", "B"), Triple("C", "uses", "A")]
        mapping = kg.link_entities(triples)
        assert "A" in mapping
        assert len(mapping["A"]) == 2  # both triples involve A
        kg.close()


# ---------------------------------------------------------------------------
# 3. extract_from_markdown() — bullets, tables, headers
# ---------------------------------------------------------------------------

class TestExtractFromMarkdown:
    def test_bullet_is_a(self):
        """'- X is a Y' in a bullet should yield an is_a triple."""
        ext = _make_extractor()
        md = "- Python is a programming language\n"
        triples = ext.extract_from_markdown(md)
        predicates = [t.predicate for t in triples]
        assert "is_a" in predicates

    def test_bullet_key_value(self):
        """'- Key: Value' bullet yields a defined_as triple."""
        ext = _make_extractor()
        md = "- Author: Alice\n"
        triples = ext.extract_from_markdown(md)
        assert any(t.predicate == "defined_as" and "Alice" in t.object_ for t in triples)

    def test_table_two_col(self):
        """A 2-column markdown table row yields a related_to triple."""
        ext = _make_extractor()
        md = "| Python | programming language |\n"
        triples = ext.extract_from_markdown(md)
        assert any(t.predicate == "related_to" for t in triples)

    def test_table_separator_skipped(self):
        """Table separator rows like |---|---| should produce no triples."""
        ext = _make_extractor()
        md = "| --- | --- |\n"
        triples = ext.extract_from_markdown(md)
        # No real triples from separator
        table_triples = [t for t in triples if t.predicate == "related_to"
                         and "---" in t.subject]
        assert len(table_triples) == 0

    def test_header_as_entity_declaration(self):
        """A markdown header should emit an is_section triple."""
        ext = _make_extractor()
        md = "## Programming Languages\n"
        triples = ext.extract_from_markdown(md)
        assert any(t.subject == "Programming Languages" and t.predicate == "is_section"
                   for t in triples)

    def test_definition_list(self):
        """'Term: Definition' in plain text yields a defined_as triple."""
        ext = _make_extractor()
        md = "Python: A high-level programming language\n"
        triples = ext.extract_from_markdown(md)
        assert any(t.predicate == "defined_as" and t.subject == "Python" for t in triples)

    def test_section_membership_under_header(self):
        """Definitions under a header also get a belongs_to_section triple."""
        ext = _make_extractor()
        md = "## Tools\nPython: scripting language\n"
        triples = ext.extract_from_markdown(md)
        subjects = [t.subject for t in triples if t.predicate == "belongs_to_section"]
        assert "Python" in subjects

    def test_empty_markdown(self):
        ext = _make_extractor()
        assert ext.extract_from_markdown("") == []

    def test_full_table_with_header_row(self):
        """Full markdown table: header + separator + data row."""
        ext = _make_extractor()
        md = "| Name | Type |\n| --- | --- |\n| Python | Language |\n"
        triples = ext.extract_from_markdown(md)
        data_triples = [t for t in triples if t.predicate == "related_to"
                        and t.subject == "Name" or t.subject == "Python"]
        assert len(data_triples) >= 1


# ---------------------------------------------------------------------------
# 4. 15+ relation patterns available
# ---------------------------------------------------------------------------

class TestPatternCoverage:
    def test_at_least_15_patterns(self):
        ext = _make_extractor()
        assert len(ext._RAW_PATTERNS) >= 15

    def test_created_by_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Django was created by Adrian Holovaty.")
        assert any(t.predicate == "created_by" for t in triples)

    def test_located_in_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Google is located in California.")
        assert any(t.predicate == "located_in" for t in triples)

    def test_part_of_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Python is part of the Tiobe Index.")
        assert any(t.predicate == "part_of" for t in triples)

    def test_leverages_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("TensorFlow leverages Python.")
        assert any(t.predicate == "leverages" for t in triples)

    def test_employs_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Keras employs TensorFlow.")
        assert any(t.predicate == "leverages" or t.predicate == "employs" for t in triples)

    def test_authored_by_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("The book was authored by Guido.")
        assert any(t.predicate == "authored_by" for t in triples)

    def test_subset_of_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Python is a subset of Programming.")
        assert any(t.predicate in ("subset_of", "is_a") for t in triples)

    def test_synonym_of_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Python also known as Py.")
        assert any(t.predicate == "synonym_of" for t in triples)

    def test_uses_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Django uses Python.")
        assert any(t.predicate == "uses" for t in triples)

    def test_has_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Python has modules.")
        assert any(t.predicate == "has" for t in triples)

    def test_related_to_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Python and Ruby are related.")
        assert any(t.predicate == "related_to" for t in triples)

    def test_succeeds_pattern(self):
        ext = _make_extractor()
        triples = ext.extract_triples("Python3 replaces Python2.")
        assert any(t.predicate == "succeeds" for t in triples)


# ---------------------------------------------------------------------------
# 5. KnowledgeGraph deduplication integration
# ---------------------------------------------------------------------------

class TestKGDeduplication:
    def test_add_from_text_deduplicates(self):
        """add_from_text should not store semantic duplicates."""
        kg = _make_kg()
        # Two sentences that generate same-group triples
        text = "Python uses Django. Python leverages Django."
        count = kg.add_from_text(text)
        # Should be 1 after semantic dedup (uses and leverages are in 'usage' group)
        assert count <= 1
        kg.close()

    def test_add_from_markdown_deduplicates(self):
        kg = _make_kg()
        md = "- Python uses Django\n- Python leverages Django\n"
        count = kg.add_from_markdown(md)
        # Both lines produce same semantic triple — should dedup to ≤ 1
        assert count <= 1
        kg.close()
