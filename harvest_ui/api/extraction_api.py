"""
Structured Extraction API — Firecrawl-pattern async job endpoints.

POST /api/v1/scrape                URL → markdown (sync, optional JS rendering)
POST /api/v1/extract               URL + schema/prompt → JSON structured data (async)
POST /api/v1/crawl                 URL + depth → list of pages (async)
POST /api/v1/extract/ecommerce     Domain preset: price/SKU/availability extraction
POST /api/v1/extract/news          Domain preset: headline/author/date extraction
POST /api/v1/extract/legal         Domain preset: citation/party/judgment extraction
GET  /api/v1/jobs/{job_id}         poll job status + result
GET  /api/v1/jobs/{job_id}/pages   paginated crawl results
GET  /api/v1/jobs                  list recent jobs

webhook_url: all async endpoints accept optional webhook_url; on job completion
the result is POSTed to that URL with HMAC-SHA256 signature (via WebhookDispatcher).

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
from harvest_ui.extraction.html_pattern_extractor import HTMLPatternExtractor


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
    webhook_url: Optional[str] = None
    proxy_url: Optional[str] = None
    local_extraction_mode: bool = False


class CrawlRequest(_BM):
    url: str
    max_depth: int = 1
    max_pages: int = 10
    follow_links: bool = True
    use_js_rendering: bool = False
    user_query: Optional[str] = None
    webhook_url: Optional[str] = None
    proxy_url: Optional[str] = None


class DomainExtractRequest(_BM):
    url: str
    use_js_rendering: bool = False
    webhook_url: Optional[str] = None
    proxy_url: Optional[str] = None
    local_extraction_mode: bool = False


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
        if req_data.get("local_extraction_mode") and schema_prompt:
            extracted = _rule_based_extract(markdown, schema_prompt)
        else:
            extracted = await _llm_extract(markdown, schema_prompt)
        store.update(job_id, status="completed", result=extracted)
    except Exception as e:
        store.update(job_id, status="failed", error=str(e))


def _rule_based_extract(markdown: str, schema_prompt: str) -> Dict[str, Any]:
    import re as _re

    # --- Primary path: HTML-aware structural extraction ---
    html_result = HTMLPatternExtractor().extract(markdown, schema_hint=schema_prompt)

    prompt_lower = schema_prompt.lower()
    result: Dict[str, Any] = {}

    # --- Fallback regex path (operates on raw content) ---
    if any(k in prompt_lower for k in ("price", "product", "sku", "ecommerce")):
        price_m = _re.search(r"\$\s*(\d+(?:\.\d{2})?)", markdown)
        if price_m:
            result["price"] = float(price_m.group(1))
            result["currency"] = "USD"
        sku_m = _re.search(r"(?i)\b(?:sku|item\s*#?|model\s*#?)[:\s]+([A-Z0-9\-]{4,20})\b", markdown)
        if sku_m:
            result["sku"] = sku_m.group(1)
        name_m = _re.search(r"^#+\s*(.+)$", markdown, _re.MULTILINE)
        if name_m:
            result["product_name"] = name_m.group(1).strip()
        if "in stock" in markdown.lower() or "add to cart" in markdown.lower():
            result["availability"] = "in_stock"
        elif "out of stock" in markdown.lower() or "sold out" in markdown.lower():
            result["availability"] = "out_of_stock"

    if any(k in prompt_lower for k in ("headline", "author", "article", "news", "published")):
        headline_m = _re.search(r"^#+\s*(.+)$", markdown, _re.MULTILINE)
        if headline_m:
            result["headline"] = headline_m.group(1).strip()
        author_m = _re.search(r"(?i)(?:by|author)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)", markdown)
        if author_m:
            result["author"] = [author_m.group(1)]
        date_m = _re.search(r"\b(\d{4}-\d{2}-\d{2}|\w+ \d{1,2},?\s+\d{4})\b", markdown)
        if date_m:
            result["published_date"] = date_m.group(1)

    if any(k in prompt_lower for k in ("case", "court", "legal", "citation", "plaintiff", "defendant")):
        case_m = _re.search(r"(?i)([A-Z][a-zA-Z\s]+ v\.? [A-Z][a-zA-Z\s]+)", markdown)
        if case_m:
            result["case_name"] = case_m.group(1).strip()
        court_m = _re.search(r"(?i)(supreme court|court of appeals|district court|circuit court)", markdown)
        if court_m:
            result["court"] = court_m.group(1)

    for m in _re.finditer(r"(?m)^([A-Za-z][A-Za-z\s]{2,30}):\s*(.{1,200})$", markdown):
        key = m.group(1).strip().lower().replace(" ", "_")
        val = m.group(2).strip()
        if key not in result and len(key) <= 40:
            result[key] = val

    # Merge: HTML-aware results take priority over plain-regex results
    merged: Dict[str, Any] = {}
    merged.update(result)          # regex results as base
    for k, v in html_result.items():  # HTML extractor overwrites if non-empty
        if v is not None and v != "" and v != [] and v != {}:
            merged[k] = v

    merged["_extraction_mode"] = "rule_based"
    return merged


async def _llm_extract(markdown: str, schema_prompt: Optional[str]) -> Dict[str, Any]:
    if not schema_prompt:
        return {"markdown": markdown}

    try:
        import anthropic
        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2048,
            messages=[{"role": "user", "content": (
                f"Extract structured data from the following content.\n"
                f"Schema/instructions: {schema_prompt}\n\n"
                f"Content:\n{markdown[:8000]}\n\n"
                "Respond with valid JSON only."
            )}],
        )
        import json
        text = msg.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        result = json.loads(text)
        result["_extraction_mode"] = "llm"
        return result
    except Exception:
        pass

    return _rule_based_extract(markdown, schema_prompt)


async def _fire_webhook(webhook_url: Optional[str], job_id: str, status: str, result: Any) -> None:
    """POST job completion payload to webhook_url if provided (HMAC-SHA256 signed)."""
    if not webhook_url:
        return
    try:
        import hashlib, hmac, json as _json
        payload_bytes = _json.dumps({"job_id": job_id, "status": status, "result": result}).encode()
        sig = hmac.new(b"harvest-webhook", payload_bytes, hashlib.sha256).hexdigest()
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                await session.post(
                    webhook_url,
                    data=payload_bytes,
                    headers={
                        "Content-Type": "application/json",
                        "X-Harvest-Signature": f"sha256={sig}",
                        "X-Harvest-Job-ID": job_id,
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except ImportError:
            pass  # aiohttp not installed — webhook silently skipped
    except Exception:
        pass  # webhook delivery failure never breaks the job


# Domain-specific extraction prompts
_DOMAIN_PROMPTS: Dict[str, str] = {
    "ecommerce": (
        "Extract: product_name, price (numeric), currency, sku, availability "
        "(in_stock|out_of_stock|preorder), rating, review_count, images (list of URLs). "
        "Return JSON."
    ),
    "news": (
        "Extract: headline, author (list), published_date (ISO-8601), summary (2 sentences), "
        "tags (list), canonical_url. Return JSON."
    ),
    "legal": (
        "Extract: case_name, court, jurisdiction, decision_date (ISO-8601), "
        "parties (plaintiff, defendant), citations (list), holding (1 sentence), "
        "outcome (affirmed|reversed|remanded|dismissed|other). Return JSON."
    ),
}


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

        async def _run_and_notify() -> None:
            await _run_extract(job.job_id, req.model_dump(), store)
            finished = store.get(job.job_id)
            if finished and req.webhook_url:
                await _fire_webhook(req.webhook_url, job.job_id, finished.status, finished.result)

        asyncio.create_task(_run_and_notify())
        return {"job_id": job.job_id, "status": "processing"}

    @app.post("/api/v1/crawl", status_code=202)
    async def crawl(req: CrawlRequest):
        """Async crawl: URL + depth → pages. Returns job_id to poll."""
        job = store.create("crawl", req.url, req.model_dump())

        async def _run_and_notify() -> None:
            await _run_crawl(job.job_id, req.model_dump(), store)
            finished = store.get(job.job_id)
            if finished and req.webhook_url:
                await _fire_webhook(req.webhook_url, job.job_id, finished.status, finished.result)

        asyncio.create_task(_run_and_notify())
        return {"job_id": job.job_id, "status": "processing"}

    # ------------------------------------------------------------------
    # Domain preset endpoints
    # ------------------------------------------------------------------

    def _make_domain_extract_handler(domain: str):
        async def _handler(req: DomainExtractRequest):
            schema_prompt = _DOMAIN_PROMPTS[domain]
            ext_req_data = {
                "url": req.url,
                "schema_prompt": schema_prompt,
                "use_js_rendering": req.use_js_rendering,
                "webhook_url": req.webhook_url,
                "proxy_url": req.proxy_url,
            }
            job = store.create(f"extract_{domain}", req.url, ext_req_data)

            async def _run_and_notify() -> None:
                await _run_extract(job.job_id, ext_req_data, store)
                finished = store.get(job.job_id)
                if finished and req.webhook_url:
                    await _fire_webhook(req.webhook_url, job.job_id, finished.status, finished.result)

            asyncio.create_task(_run_and_notify())
            return {"job_id": job.job_id, "status": "processing", "domain": domain}
        _handler.__name__ = f"extract_{domain}"
        return _handler

    for _domain in ("ecommerce", "news", "legal"):
        app.post(f"/api/v1/extract/{_domain}", status_code=202)(
            _make_domain_extract_handler(_domain)
        )

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
