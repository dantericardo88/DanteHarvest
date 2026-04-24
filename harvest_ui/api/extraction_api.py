"""
Structured Extraction API — Firecrawl-pattern async job endpoints.

POST /api/v1/scrape          URL → markdown (sync, optional JS rendering)
POST /api/v1/extract         URL + schema/prompt → JSON structured data (async)
POST /api/v1/crawl           URL + depth → list of pages (async)
GET  /api/v1/jobs/{job_id}   poll job status + result
GET  /api/v1/jobs/{job_id}/pages  paginated crawl results
GET  /api/v1/jobs            list recent jobs

Constitutional guarantees:
- Local-first: file-backed job store, no Redis/Celery
- Fail-closed: missing job → 404, task errors persisted to job.error
- Zero-ambiguity: all responses are typed JSON
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel as _BM
    _FASTAPI = True
except ImportError:
    _BM = object  # type: ignore[assignment,misc]
    _FASTAPI = False

from harvest_ui.api.job_store import JobStore


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------

class ScrapeRequest(_BM):
    url: str
    use_js_rendering: bool = False
    user_query: Optional[str] = None


class ExtractRequest(_BM):
    url: str
    schema_prompt: Optional[str] = None
    use_js_rendering: bool = False


class CrawlRequest(_BM):
    url: str
    max_depth: int = 1
    max_pages: int = 10
    follow_links: bool = True
    use_js_rendering: bool = False
    user_query: Optional[str] = None


# ---------------------------------------------------------------------------
# Background task runners
# ---------------------------------------------------------------------------

async def _run_scrape(job_id: str, req_data: dict, store: JobStore) -> None:
    store.update(job_id, status="processing")
    try:
        from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
        adapter = CrawleeAdapter(use_js_rendering=req_data.get("use_js_rendering", False))
        result = await adapter.crawl(
            url=req_data["url"],
            run_id=job_id,
            max_pages=1,
            user_query=req_data.get("user_query"),
        )
        if result.pages:
            store.update(job_id, status="completed", result={"markdown": result.pages[0].markdown})
        else:
            store.update(job_id, status="failed", error="No pages fetched")
    except Exception as e:
        store.update(job_id, status="failed", error=str(e))


async def _run_extract(job_id: str, req_data: dict, store: JobStore) -> None:
    store.update(job_id, status="processing")
    try:
        from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
        adapter = CrawleeAdapter(use_js_rendering=req_data.get("use_js_rendering", False))
        result = await adapter.crawl(url=req_data["url"], run_id=job_id, max_pages=1)
        if not result.pages:
            store.update(job_id, status="failed", error="No pages fetched")
            return
        markdown = result.pages[0].markdown
        schema_prompt = req_data.get("schema_prompt")
        extracted = await _llm_extract(markdown, schema_prompt)
        store.update(job_id, status="completed", result=extracted)
    except Exception as e:
        store.update(job_id, status="failed", error=str(e))


async def _llm_extract(markdown: str, schema_prompt: Optional[str]) -> Dict[str, Any]:
    if not schema_prompt:
        return {"markdown": markdown}
    try:
        import anthropic
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": (
                    f"Extract structured data from the following content.\n"
                    f"Schema/instructions: {schema_prompt}\n\n"
                    f"Content:\n{markdown[:8000]}\n\n"
                    "Respond with valid JSON only."
                ),
            }],
        )
        import json
        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception as e:
        return {"markdown": markdown, "extraction_error": str(e)}


async def _run_crawl(job_id: str, req_data: dict, store: JobStore) -> None:
    store.update(job_id, status="processing")
    try:
        from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
        adapter = CrawleeAdapter(use_js_rendering=req_data.get("use_js_rendering", False))
        result = await adapter.crawl(
            url=req_data["url"],
            run_id=job_id,
            max_depth=req_data.get("max_depth", 1),
            max_pages=req_data.get("max_pages", 10),
            follow_links=req_data.get("follow_links", True),
            user_query=req_data.get("user_query"),
        )
        pages = [
            {"url": p.url, "markdown": p.markdown, "status_code": p.status_code, "depth": p.depth}
            for p in result.pages
        ]
        store.update(
            job_id, status="completed",
            pages=pages,
            result={"page_count": len(pages), "error_count": len(result.errors)},
        )
    except Exception as e:
        store.update(job_id, status="failed", error=str(e))


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_extraction_app(storage_root: str = "storage") -> Any:
    if not _FASTAPI:
        raise ImportError("fastapi not installed")

    app = FastAPI(
        title="Harvest Extraction API",
        description="Firecrawl-pattern async scrape/extract/crawl endpoints",
        version="0.1.0",
    )

    store = JobStore(storage_root=storage_root)

    @app.post("/api/v1/scrape")
    async def scrape(req: ScrapeRequest):
        """Sync scrape: URL → markdown. Runs inline (fast path)."""
        try:
            from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
            adapter = CrawleeAdapter(use_js_rendering=req.use_js_rendering)
            result = await adapter.crawl(
                url=req.url, run_id="sync",
                max_pages=1, user_query=req.user_query,
            )
            if not result.pages:
                raise HTTPException(status_code=422, detail={"error": "No content fetched"})
            return {"markdown": result.pages[0].markdown, "url": req.url}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    @app.post("/api/v1/extract", status_code=202)
    async def extract(req: ExtractRequest):
        """Async extract: URL + schema → JSON. Returns job_id to poll."""
        job = store.create("extract", req.url, req.model_dump())
        asyncio.create_task(_run_extract(job.job_id, req.model_dump(), store))
        return {"job_id": job.job_id, "status": "processing"}

    @app.post("/api/v1/crawl", status_code=202)
    async def crawl(req: CrawlRequest):
        """Async crawl: URL + depth → pages. Returns job_id to poll."""
        job = store.create("crawl", req.url, req.model_dump())
        asyncio.create_task(_run_crawl(job.job_id, req.model_dump(), store))
        return {"job_id": job.job_id, "status": "processing"}

    @app.get("/api/v1/jobs")
    def list_jobs(
        kind: Optional[str] = Query(None),
        status: Optional[str] = Query(None),
    ):
        jobs = store.list_jobs(kind=kind, status=status)
        return [
            {"job_id": j.job_id, "kind": j.kind, "status": j.status,
             "url": j.url, "created_at": j.created_at}
            for j in jobs
        ]

    @app.get("/api/v1/jobs/{job_id}")
    def get_job(job_id: str):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail={"error": f"Job {job_id} not found"})
        return {
            "job_id": job.job_id,
            "kind": job.kind,
            "status": job.status,
            "url": job.url,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
            "result": job.result,
            "error": job.error,
        }

    @app.get("/api/v1/jobs/{job_id}/pages")
    def get_job_pages(
        job_id: str,
        offset: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=500),
    ):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail={"error": f"Job {job_id} not found"})
        pages = job.pages[offset: offset + limit]
        return {"job_id": job_id, "total": len(job.pages), "offset": offset, "pages": pages}

    return app
