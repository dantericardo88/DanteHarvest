"""Tests for MetadataEnricher — LlamaIndex-style chunk enrichment."""

from harvest_normalize.chunking.chunker import Chunk, ChunkStrategy
from harvest_normalize.chunking.metadata_enricher import MetadataEnricher


def make_chunk(text: str, index: int = 0) -> Chunk:
    return Chunk(text=text, index=index, start_char=0, end_char=len(text),
                 strategy=ChunkStrategy.FIXED, metadata={})


def test_heading_title_extraction():
    enricher = MetadataEnricher(source_path="doc.md")
    chunks = [make_chunk("## Invoice Processing\n\nThis is the content of the section.")]
    enriched = enricher.enrich(chunks)
    assert enriched[0].metadata["title"] == "Invoice Processing"


def test_sentence_fallback_title():
    enricher = MetadataEnricher()
    chunks = [make_chunk("This is the first sentence. And a second one.")]
    enriched = enricher.enrich(chunks)
    assert enriched[0].metadata["title"] == "This is the first sentence"


def test_word_count_accuracy():
    enricher = MetadataEnricher()
    text = "one two three four five"
    chunks = [make_chunk(text)]
    enriched = enricher.enrich(chunks)
    assert enriched[0].metadata["word_count"] == 5


def test_source_path_passthrough():
    enricher = MetadataEnricher(source_path="storage/docs/report.pdf")
    chunks = [make_chunk("Some content here")]
    enriched = enricher.enrich(chunks)
    assert enriched[0].metadata["source_path"] == "storage/docs/report.pdf"


def test_empty_chunks_returns_empty():
    enricher = MetadataEnricher()
    result = enricher.enrich([])
    assert result == []


def test_chunk_index_recorded():
    enricher = MetadataEnricher()
    chunks = [make_chunk("first", index=0), make_chunk("second", index=1)]
    enriched = enricher.enrich(chunks)
    assert enriched[0].metadata["chunk_index"] == 0
    assert enriched[1].metadata["chunk_index"] == 1


def test_keywords_extracted():
    enricher = MetadataEnricher(top_keywords=3)
    chunks = [make_chunk("invoice invoice invoice payment payment workflow")]
    enriched = enricher.enrich(chunks)
    kws = enriched[0].metadata["keywords"]
    assert "invoice" in kws
    assert len(kws) <= 3


def test_extra_metadata_merged():
    enricher = MetadataEnricher()
    chunks = [make_chunk("Some content")]
    enriched = enricher.enrich(chunks, extra_metadata={"project": "harvest", "run_id": "r1"})
    assert enriched[0].metadata["project"] == "harvest"
    assert enriched[0].metadata["run_id"] == "r1"
