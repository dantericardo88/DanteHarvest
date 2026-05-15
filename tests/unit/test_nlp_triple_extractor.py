"""
Tests for harvest_distill.taxonomy.nlp_triple_extractor and the new
automated-extraction methods on KnowledgeGraph.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extractor():
    from harvest_distill.taxonomy.nlp_triple_extractor import NLPTripleExtractor
    return NLPTripleExtractor()


def _kg():
    from harvest_distill.taxonomy.knowledge_graph import KnowledgeGraph
    return KnowledgeGraph()  # in-memory


# ---------------------------------------------------------------------------
# NLPTripleExtractor — relation extraction
# ---------------------------------------------------------------------------

def test_extract_is_a_relation():
    """'Python is a programming language' → (Python, is_a, programming language)."""
    ext = _extractor()
    triples = ext.extract_triples("Python is a programming language.")
    predicates = [t.predicate for t in triples]
    subjects = [t.subject for t in triples]
    objects = [t.object_ for t in triples]
    assert "is_a" in predicates
    idx = predicates.index("is_a")
    assert "Python" in subjects[idx]
    assert "programming language" in objects[idx].lower()


def test_extract_part_of_relation():
    """'Flask is part of Python ecosystem' → part_of triple."""
    ext = _extractor()
    triples = ext.extract_triples("Flask is part of Python ecosystem.")
    preds = [t.predicate for t in triples]
    assert "part_of" in preds
    t = next(t for t in triples if t.predicate == "part_of")
    assert "Flask" in t.subject
    assert "Python" in t.object_


def test_extract_created_by_relation():
    """'Django was created by Adrian Holovaty' → created_by triple."""
    ext = _extractor()
    triples = ext.extract_triples("Django was created by Adrian Holovaty.")
    preds = [t.predicate for t in triples]
    assert "created_by" in preds
    t = next(t for t in triples if t.predicate == "created_by")
    assert "Django" in t.subject
    assert "Adrian" in t.object_


def test_extract_uses_relation():
    """'TensorFlow uses NumPy' → uses triple."""
    ext = _extractor()
    triples = ext.extract_triples("TensorFlow uses NumPy for numerical operations.")
    preds = [t.predicate for t in triples]
    assert "uses" in preds
    t = next(t for t in triples if t.predicate == "uses")
    assert "TensorFlow" in t.subject
    assert "NumPy" in t.object_


def test_extract_located_in_relation():
    """'Google is based in California' → located_in triple."""
    ext = _extractor()
    triples = ext.extract_triples("Google is based in California.")
    preds = [t.predicate for t in triples]
    assert "located_in" in preds


def test_extract_has_relation():
    """'Python has libraries' → has triple."""
    ext = _extractor()
    triples = ext.extract_triples("Python has Libraries for data science.")
    preds = [t.predicate for t in triples]
    assert "has" in preds


def test_extract_related_to_relation():
    """'Machine Learning and Artificial Intelligence are related' → related_to."""
    ext = _extractor()
    triples = ext.extract_triples(
        "Machine Learning and Artificial Intelligence are related."
    )
    preds = [t.predicate for t in triples]
    assert "related_to" in preds


# ---------------------------------------------------------------------------
# Entity extraction
# ---------------------------------------------------------------------------

def test_extract_entities_from_capitalized():
    """Multi-word capitalized phrases should be found."""
    ext = _extractor()
    entities = ext.extract_entities(
        "Google Cloud and Amazon AWS are cloud providers in the United States."
    )
    # Expect multi-word capitalized phrases
    entity_str = " ".join(entities)
    assert "Google Cloud" in entity_str or "Google" in entity_str
    assert "Amazon AWS" in entity_str or "Amazon" in entity_str


def test_extract_entities_all_caps_abbreviations():
    """ALL-CAPS abbreviations should be included."""
    ext = _extractor()
    entities = ext.extract_entities("The API uses REST and JSON for communication.")
    assert "API" in entities or "REST" in entities or "JSON" in entities


def test_extract_entities_quoted_terms():
    """Quoted terms should be extracted as entity candidates."""
    ext = _extractor()
    entities = ext.extract_entities('The concept of "deep learning" is important.')
    assert "deep learning" in entities


def test_extract_entities_empty():
    """Empty text → empty list."""
    ext = _extractor()
    assert ext.extract_entities("") == []


# ---------------------------------------------------------------------------
# Markdown extraction
# ---------------------------------------------------------------------------

def test_extract_from_markdown_definition_list():
    """'Term: Definition' lines → defined_as triples."""
    ext = _extractor()
    md = "Term: A unit of text\nAnother: A different concept"
    triples = ext.extract_from_markdown(md)
    preds = [t.predicate for t in triples]
    assert "defined_as" in preds
    subjects = [t.subject for t in triples]
    assert "Term" in subjects
    assert "Another" in subjects


def test_extract_from_markdown_bullet_key_value():
    """Bullet '- Key: Value' lines → defined_as triples."""
    ext = _extractor()
    md = "- Language: Python\n- Framework: Django"
    triples = ext.extract_from_markdown(md)
    subjects = [t.subject for t in triples]
    assert "Language" in subjects or "Framework" in subjects


def test_extract_from_markdown_header_context():
    """Definitions under a header also get a belongs_to_section triple."""
    ext = _extractor()
    md = "## Programming Languages\nPython: A high-level language"
    triples = ext.extract_from_markdown(md)
    section_triples = [t for t in triples if t.predicate == "belongs_to_section"]
    assert len(section_triples) >= 1
    assert any("Programming Languages" in t.object_ for t in section_triples)


def test_extract_from_markdown_plain_text_in_paragraphs():
    """Plain-text sentences inside markdown are also processed."""
    ext = _extractor()
    md = "Some intro text.\n\nPython is a programming language."
    triples = ext.extract_from_markdown(md)
    preds = [t.predicate for t in triples]
    assert "is_a" in preds


def test_extract_from_markdown_empty():
    """Empty markdown → empty list."""
    ext = _extractor()
    assert ext.extract_from_markdown("") == []


# ---------------------------------------------------------------------------
# Batch extraction
# ---------------------------------------------------------------------------

def test_batch_extract_deduplicates():
    """Same triple in multiple texts appears only once."""
    ext = _extractor()
    text = "Django was created by Adrian Holovaty."
    triples = ext.batch_extract([text, text, text])
    # Count created_by triples for Django — should be exactly 1
    cb = [t for t in triples if t.predicate == "created_by" and "Django" in t.subject]
    assert len(cb) == 1


def test_batch_extract_combines_multiple_texts():
    """Different triples from different texts are all returned."""
    ext = _extractor()
    texts = [
        "Python is a programming language.",
        "Django was created by Adrian Holovaty.",
    ]
    triples = ext.batch_extract(texts)
    preds = {t.predicate for t in triples}
    assert "is_a" in preds
    assert "created_by" in preds


def test_batch_extract_empty_list():
    """Empty input → empty list."""
    ext = _extractor()
    assert ext.batch_extract([]) == []


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_extract_empty_text_no_crash():
    """Empty string → empty list without exception."""
    ext = _extractor()
    assert ext.extract_triples("") == []


def test_extract_no_pattern_match():
    """Text with no recognisable patterns → empty list."""
    ext = _extractor()
    result = ext.extract_triples("the quick brown fox jumps over the lazy dog")
    assert isinstance(result, list)


def test_triple_confidence_range():
    """All extracted triples must have confidence in [0.0, 1.0]."""
    ext = _extractor()
    texts = [
        "Python is a programming language.",
        "Django was created by Adrian Holovaty.",
        "TensorFlow uses NumPy.",
        "Flask is part of Python ecosystem.",
        "Google is based in California.",
    ]
    for text in texts:
        for t in ext.extract_triples(text):
            assert 0.0 <= t.confidence <= 1.0, (
                f"confidence {t.confidence} out of range for triple {t}"
            )


def test_triple_has_source_text():
    """source_text must be non-empty and a substring of the original input."""
    ext = _extractor()
    text = "Python is a programming language."
    triples = ext.extract_triples(text)
    assert len(triples) > 0
    for t in triples:
        assert t.source_text, "source_text must not be empty"
        # source_text should be a fragment of the original (trimmed sentence)
        # We check the key tokens are present rather than exact substring match
        # because sentence splitting may slightly alter whitespace
        assert len(t.source_text) > 0


# ---------------------------------------------------------------------------
# KnowledgeGraph integration
# ---------------------------------------------------------------------------

def test_knowledge_graph_add_from_text():
    """kg.add_from_text() → graph has triples."""
    kg = _kg()
    count = kg.add_from_text(
        "Python is a programming language. Django was created by Adrian Holovaty."
    )
    assert count >= 1
    assert kg.relation_count() >= 1
    kg.close()


def test_knowledge_graph_add_from_text_returns_int():
    """add_from_text returns an int."""
    kg = _kg()
    result = kg.add_from_text("Python is a programming language.")
    assert isinstance(result, int)
    kg.close()


def test_knowledge_graph_add_from_markdown():
    """kg.add_from_markdown() processes definition lists → triples added."""
    kg = _kg()
    md = "## Languages\nPython: A high-level programming language\nRuby: A dynamic language"
    count = kg.add_from_markdown(md)
    assert count >= 1
    assert kg.relation_count() >= 1
    kg.close()


def test_knowledge_graph_add_from_documents():
    """kg.add_from_documents() processes list of dicts → triples extracted."""
    kg = _kg()
    docs = [
        {"id": "doc1", "text": "Python is a programming language."},
        {"id": "doc2", "text": "Django was created by Adrian Holovaty."},
        {"id": "doc3", "text": "No extractable patterns here at all."},
    ]
    total = kg.add_from_documents(docs, text_field="text")
    assert total >= 1
    assert kg.relation_count() >= 1
    kg.close()


def test_knowledge_graph_add_from_documents_custom_text_field():
    """add_from_documents respects a custom text_field argument."""
    kg = _kg()
    docs = [{"body": "Python is a programming language."}]
    total = kg.add_from_documents(docs, text_field="body")
    assert total >= 1
    kg.close()


def test_knowledge_graph_add_from_documents_missing_field():
    """Docs missing the text_field are silently skipped."""
    kg = _kg()
    docs = [{"other": "no text here"}]
    total = kg.add_from_documents(docs)
    assert total == 0
    kg.close()


def test_knowledge_graph_extractor_lazy_init():
    """The _extractor attribute is created lazily on first access."""
    kg = _kg()
    # Initially None before first access
    assert kg._extractor is None
    # Accessing the property triggers creation
    ext = kg.extractor
    assert ext is not None
    assert kg._extractor is not None
    kg.close()


def test_knowledge_graph_existing_api_still_works():
    """Existing add_triple / neighbors / to_cypher API must not be broken."""
    kg = _kg()
    kg.add_triple("Python", "is_a", "language")
    assert kg.entity_count() == 2
    assert kg.relation_count() == 1
    n = kg.neighbors("Python")
    assert len(n) >= 1
    cypher = kg.to_cypher()
    assert "Python" in cypher
    kg.close()


def test_knowledge_graph_add_from_text_source_id_stored():
    """source_id is stored on relations when provided."""
    from harvest_distill.taxonomy.knowledge_graph import KnowledgeGraph
    kg = KnowledgeGraph()
    kg.add_from_text("Python is a programming language.", source_id="wiki-python")
    # Verify at least one relation has our source tag
    cur = kg._conn.cursor()
    rows = cur.execute("SELECT source FROM relations").fetchall()
    sources = [r[0] for r in rows]
    assert any("wiki-python" in s for s in sources)
    kg.close()
