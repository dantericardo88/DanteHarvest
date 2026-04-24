"""Tests for CrawleeAdapter — request-queue-based crawling."""

import pytest
import unittest.mock as mock
from harvest_acquire.crawl.crawlee_adapter import (
    CrawleeAdapter,
    CrawlResult,
    _RequestQueue,
    _extract_links,
    _html_to_markdown,
)
from harvest_core.provenance.chain_writer import ChainWriter


def make_adapter(tmp_path, writer=None) -> CrawleeAdapter:
    return CrawleeAdapter(chain_writer=writer, storage_root=str(tmp_path))


# ------------------------------------------------------------------
# RequestQueue unit tests
# ------------------------------------------------------------------

def test_request_queue_dedup():
    q = _RequestQueue()
    assert q.enqueue("http://a.com") is True
    assert q.enqueue("http://a.com") is False
    assert len(q) == 1


def test_request_queue_fifo():
    q = _RequestQueue()
    q.enqueue("http://a.com", depth=0)
    q.enqueue("http://b.com", depth=1)
    url, depth = q.dequeue()
    assert url == "http://a.com"
    assert depth == 0


def test_request_queue_empty_dequeue_returns_none():
    q = _RequestQueue()
    assert q.dequeue() is None


# ------------------------------------------------------------------
# HTML helpers
# ------------------------------------------------------------------

def test_html_to_markdown_strips_tags():
    html = "<html><body><h1>Hello</h1><p>World</p></body></html>"
    md = _html_to_markdown(html)
    assert "Hello" in md
    assert "World" in md
    assert "<" not in md


def test_html_to_markdown_strips_scripts():
    html = "<html><script>alert('xss')</script><body>Content</body></html>"
    md = _html_to_markdown(html)
    assert "alert" not in md
    assert "Content" in md


def test_extract_links_absolute():
    html = '<a href="https://example.com/page1">link</a>'
    links = _extract_links(html, "https://example.com")
    assert "https://example.com/page1" in links


def test_extract_links_relative():
    html = '<a href="/about">About</a>'
    links = _extract_links(html, "https://example.com")
    assert "https://example.com/about" in links


# ------------------------------------------------------------------
# CrawleeAdapter integration
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_crawl_success(tmp_path):
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    adapter = make_adapter(tmp_path, writer)

    html = "<html><body><h1>Invoice Dashboard</h1><p>Accounting content</p></body></html>"
    with mock.patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=(html, 200)):
        result = await adapter.crawl("http://example.com", run_id="r1")

    assert isinstance(result, CrawlResult)
    assert result.page_count == 1
    assert "Invoice" in result.pages[0].markdown


@pytest.mark.asyncio
async def test_crawl_chain_signals(tmp_path):
    writer = ChainWriter(tmp_path / "chain.jsonl", "r1")
    adapter = make_adapter(tmp_path, writer)

    html = "<html><body>Content</body></html>"
    with mock.patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=(html, 200)):
        await adapter.crawl("http://example.com", run_id="r1")

    signals = [e.signal for e in writer.read_all()]
    assert "crawl.started" in signals
    assert "crawl.page_fetched" in signals
    assert "crawl.completed" in signals


@pytest.mark.asyncio
async def test_crawl_http_error_recorded(tmp_path):
    adapter = make_adapter(tmp_path)
    with mock.patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=("", 404)):
        result = await adapter.crawl("http://example.com", run_id="r1")

    assert result.page_count == 0
    assert len(result.errors) == 1


@pytest.mark.asyncio
async def test_crawl_stores_page_locally(tmp_path):
    adapter = make_adapter(tmp_path)
    html = "<html><body>Invoice content here</body></html>"
    with mock.patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=(html, 200)):
        result = await adapter.crawl("http://example.com", run_id="r1")

    assert result.page_count == 1
    page_file = tmp_path / "crawlee" / result.pages[0].artifact_id / "page.md"
    assert page_file.exists()


@pytest.mark.asyncio
async def test_crawl_max_pages_respected(tmp_path):
    adapter = make_adapter(tmp_path)
    html = '<html><body>Content <a href="/p1">l</a><a href="/p2">l</a></body></html>'
    with mock.patch("harvest_acquire.crawl.crawlee_adapter._fetch_url", return_value=(html, 200)):
        result = await adapter.crawl(
            "http://example.com", run_id="r1",
            follow_links=True, max_pages=1
        )
    assert result.page_count <= 1
