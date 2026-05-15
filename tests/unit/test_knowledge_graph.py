"""Tests for harvest_distill.taxonomy.knowledge_graph."""
import pytest
from pathlib import Path


def _make_kg(tmp_path=None):
    from harvest_distill.taxonomy.knowledge_graph import KnowledgeGraph
    if tmp_path:
        return KnowledgeGraph(db_path=tmp_path / "kg.sqlite")
    return KnowledgeGraph()  # in-memory


def test_add_triple_creates_entities():
    kg = _make_kg()
    r = kg.add_triple("Python", "is_a", "language")
    assert kg.entity_count() == 2
    assert kg.relation_count() == 1
    kg.close()


def test_entity_resolution_case_insensitive():
    kg = _make_kg()
    kg.add_triple("Python", "is_a", "language")
    kg.add_triple("python", "used_for", "scripting")  # 'python' should resolve to 'Python'
    assert kg.entity_count() == 3  # Python, language, scripting (not 4)
    kg.close()


def test_neighbors_depth_1():
    kg = _make_kg()
    kg.add_triple("A", "rel", "B")
    kg.add_triple("A", "rel2", "C")
    n = kg.neighbors("A", depth=1)
    objects = [x["object"] for x in n]
    assert "B" in objects
    assert "C" in objects
    kg.close()


def test_neighbors_depth_2():
    kg = _make_kg()
    kg.add_triple("A", "r1", "B")
    kg.add_triple("B", "r2", "C")
    n = kg.neighbors("A", depth=2)
    depths = {x["object"]: x["depth"] for x in n}
    assert depths.get("B") == 1
    assert depths.get("C") == 2
    kg.close()


def test_search_entities():
    kg = _make_kg()
    kg.add_triple("Machine Learning", "is_a", "AI")
    results = kg.search_entities("machine")
    assert any("Machine Learning" in e.canonical_name for e in results)
    kg.close()


def test_stats_structure():
    kg = _make_kg()
    kg.add_triple("X", "knows", "Y")
    kg.add_triple("X", "likes", "Z")
    stats = kg.stats()
    assert "entity_count" in stats
    assert "relation_count" in stats
    assert "top_predicates" in stats
    assert stats["relation_count"] == 2
    kg.close()


def test_to_dict_structure():
    kg = _make_kg()
    kg.add_triple("Foo", "bar", "Baz")
    d = kg.to_dict()
    assert "entities" in d
    assert "relations" in d
    assert len(d["entities"]) == 2
    assert len(d["relations"]) == 1
    kg.close()


def test_to_cypher_output():
    kg = _make_kg()
    kg.add_triple("Python", "IS_A", "Language")
    cypher = kg.to_cypher()
    assert "MERGE" in cypher
    assert "Python" in cypher
    kg.close()


def test_persistent_kg(tmp_path):
    from harvest_distill.taxonomy.knowledge_graph import KnowledgeGraph
    kg1 = KnowledgeGraph(db_path=tmp_path / "persistent.sqlite")
    kg1.add_triple("A", "rel", "B")
    kg1.close()

    kg2 = KnowledgeGraph(db_path=tmp_path / "persistent.sqlite")
    assert kg2.entity_count() == 2
    assert kg2.relation_count() == 1
    kg2.close()


def test_add_alias():
    kg = _make_kg()
    e = kg.resolve_entity("Python")
    kg.add_alias(e.entity_id, "py")
    kg.add_alias(e.entity_id, "Python3")
    # Now 'py' should resolve to Python
    e2 = kg.resolve_entity("py")
    assert e2.entity_id == e.entity_id
    kg.close()


def test_add_triples_from_spo():
    from harvest_distill.taxonomy.knowledge_graph import KnowledgeGraph
    kg = KnowledgeGraph()

    class FakeSPO:
        def __init__(self, s, p, o):
            self.subject = s
            self.predicate = p
            self.object_ = o
            self.confidence = 0.9
            self.source = "test"

    triples = [FakeSPO("A", "r1", "B"), FakeSPO("B", "r2", "C")]
    relations = kg.add_triples_from_spo(triples, source="test")
    assert len(relations) == 2
    assert kg.relation_count() == 2
    kg.close()
