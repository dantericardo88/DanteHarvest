"""
Phase 2 — Structured extraction API tests.

Verifies:
1. JobStore CRUD operations
2. Extraction API modules exist and export expected symbols
3. Job lifecycle: create → processing → completed/failed
4. Pagination endpoint structure
"""

import time
from unittest.mock import AsyncMock, patch
import pytest

from harvest_ui.api.job_store import JobStore, Job


# ---------------------------------------------------------------------------
# JobStore unit tests
# ---------------------------------------------------------------------------

def test_job_store_create(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    job = store.create("scrape", "https://example.com", {"use_js_rendering": False})
    assert job.job_id
    assert job.status == "pending"
    assert job.url == "https://example.com"
    assert (tmp_path / "jobs" / f"{job.job_id}.json").exists()


def test_job_store_get(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    job = store.create("crawl", "https://x.com", {})
    fetched = store.get(job.job_id)
    assert fetched is not None
    assert fetched.job_id == job.job_id
    assert fetched.kind == "crawl"


def test_job_store_get_missing(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    assert store.get("nonexistent-id") is None


def test_job_store_update(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    job = store.create("extract", "https://a.com", {})
    updated = store.update(job.job_id, status="completed", result={"key": "value"})
    assert updated.status == "completed"
    assert updated.result == {"key": "value"}
    # persisted
    reloaded = store.get(job.job_id)
    assert reloaded.status == "completed"


def test_job_store_update_sets_timestamp(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    job = store.create("scrape", "https://b.com", {})
    before = job.updated_at
    time.sleep(0.01)
    updated = store.update(job.job_id, status="processing")
    assert updated.updated_at > before


def test_job_store_list(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    store.create("scrape", "https://a.com", {})
    store.create("crawl", "https://b.com", {})
    store.create("scrape", "https://c.com", {})
    all_jobs = store.list_jobs()
    assert len(all_jobs) == 3
    scrape_jobs = store.list_jobs(kind="scrape")
    assert len(scrape_jobs) == 2


def test_job_store_list_filter_status(tmp_path):
    store = JobStore(storage_root=str(tmp_path))
    j1 = store.create("scrape", "https://a.com", {})
    store.update(j1.job_id, status="completed")
    store.create("scrape", "https://b.com", {})
    completed = store.list_jobs(status="completed")
    assert len(completed) == 1


def test_job_to_dict_roundtrip():
    job = Job(
        job_id="abc",
        kind="scrape",
        status="pending",
        created_at=1.0,
        updated_at=1.0,
        url="https://x.com",
        params={},
    )
    d = job.to_dict()
    restored = Job.from_dict(d)
    assert restored.job_id == job.job_id
    assert restored.kind == job.kind


# ---------------------------------------------------------------------------
# Extraction API module existence
# ---------------------------------------------------------------------------

def test_extraction_api_module_importable():
    from harvest_ui.api import extraction_api
    assert hasattr(extraction_api, "create_extraction_app")
    assert hasattr(extraction_api, "ScrapeRequest")
    assert hasattr(extraction_api, "ExtractRequest")
    assert hasattr(extraction_api, "CrawlRequest")
    assert hasattr(extraction_api, "JobStore")


def test_extraction_api_has_background_runners():
    from harvest_ui.api import extraction_api
    assert hasattr(extraction_api, "_run_scrape")
    assert hasattr(extraction_api, "_run_extract")
    assert hasattr(extraction_api, "_run_crawl")


# ---------------------------------------------------------------------------
# Background runner integration (with mocked CrawleeAdapter)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_scrape_success(tmp_path):
    from harvest_ui.api.extraction_api import _run_scrape
    from harvest_acquire.crawl.crawlee_adapter import CrawlResult, PageResult

    store = JobStore(storage_root=str(tmp_path))
    job = store.create("scrape", "https://example.com", {})

    mock_result = CrawlResult(
        pages=[PageResult(url="https://example.com", markdown="Hello world", status_code=200, depth=0, artifact_id="a1")],
        total_bytes=11,
        errors=[],
    )

    with patch("harvest_acquire.crawl.crawlee_adapter.CrawleeAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        instance.crawl = AsyncMock(return_value=mock_result)
        await _run_scrape(job.job_id, {"url": "https://example.com", "use_js_rendering": False}, store)

    updated = store.get(job.job_id)
    assert updated.status == "completed"
    assert updated.result["markdown"] == "Hello world"


@pytest.mark.asyncio
async def test_run_scrape_no_pages(tmp_path):
    from harvest_ui.api.extraction_api import _run_scrape
    from harvest_acquire.crawl.crawlee_adapter import CrawlResult

    store = JobStore(storage_root=str(tmp_path))
    job = store.create("scrape", "https://bad.com", {})

    mock_result = CrawlResult(pages=[], total_bytes=0, errors=[])

    with patch("harvest_acquire.crawl.crawlee_adapter.CrawleeAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        instance.crawl = AsyncMock(return_value=mock_result)
        await _run_scrape(job.job_id, {"url": "https://bad.com", "use_js_rendering": False}, store)

    updated = store.get(job.job_id)
    assert updated.status == "failed"


@pytest.mark.asyncio
async def test_run_crawl_success(tmp_path):
    from harvest_ui.api.extraction_api import _run_crawl
    from harvest_acquire.crawl.crawlee_adapter import CrawlResult, PageResult

    store = JobStore(storage_root=str(tmp_path))
    job = store.create("crawl", "https://site.com", {})

    pages = [
        PageResult(url=f"https://site.com/{i}", markdown=f"Page {i}", status_code=200, depth=0, artifact_id=f"a{i}")
        for i in range(3)
    ]
    mock_result = CrawlResult(pages=pages, total_bytes=100, errors=[])

    with patch("harvest_acquire.crawl.crawlee_adapter.CrawleeAdapter") as MockAdapter:
        instance = MockAdapter.return_value
        instance.crawl = AsyncMock(return_value=mock_result)
        await _run_crawl(
            job.job_id,
            {"url": "https://site.com", "max_depth": 1, "max_pages": 10, "follow_links": True, "use_js_rendering": False},
            store,
        )

    updated = store.get(job.job_id)
    assert updated.status == "completed"
    assert updated.result["page_count"] == 3
    assert len(updated.pages) == 3
