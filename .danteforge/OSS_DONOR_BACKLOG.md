# OSS Donor Pattern Backlog — DanteHarvest

Generated: 2026-04-22  
Source universe: `.danteforge/oss-registry.json` (22 local repos, 17 allowed)

This is the ranked P0/P1 implementation backlog derived from the deep second-pass scan.  
Each entry states the specific mechanism to harvest (not the repo in general), the license, and the DH gap it closes.

---

## P0 — Implement This Sprint

### P0-1: Validated State Machine for Pack Review Decisions
**Source**: OpenAdapt Desktop (MIT) — `engine/review.py` adjacency table pattern  
**Implemented**: `harvest_ui/reviewer/review_states.py` ✅  
**Gap closed**: ui_reviewer_workflow (4 → 7)  
**Pattern**: VALID_TRANSITIONS dict + EGRESS_ALLOWED frozenset + InvalidTransitionError + TOCTOU re-read guard  
**Why this beats alternatives**: Hard transition table prevents double-approve races; EGRESS_ALLOWED decouples "reviewed" from "promotable"

### P0-2: LLM-as-Judge Step Evaluator
**Source**: langchain-ai/open_deep_research (MIT) — `tests/evaluators.py` structured output pattern  
**Implemented**: `harvest_index/registry/llm_judge_executor.py` ✅  
**Gap closed**: eval_harness (6 → 9)  
**Pattern**: `StepJudgment(reasoning, passed, score)` Pydantic model + `llm.with_structured_output()` + reasoning-before-verdict anti-hallucination requirement  
**Why**: reasoning field forces CoT before verdict; score 0–1 enables continuous quality tracking alongside binary pass/fail

### P0-3: BM25 + Tag-Weight Content Extraction
**Source**: Crawl4AI (Apache-2.0) — `crawl4ai/content_filter_strategy.py` BM25ContentFilter  
**Implemented**: `harvest_acquire/crawl/content_filter.py` ✅  
**Gap closed**: crawl_acquisition (6 → 8)  
**Pattern**: `_NEGATIVE_PATTERNS` excludes nav/footer/header before scoring; BM25Okapi × tag_weight scores surviving chunks; PruningFilter fallback for queryless crawls  
**Optional deps**: `rank-bm25`, `snowballstemmer` — graceful fallback to regex stripping if absent

### P0-4: PlaywrightStepExecutor + Session Context Manager
**Source**: Playwright (MIT) — async_playwright pattern  
**Implemented**: `harvest_index/registry/playwright_executor.py` ✅  
**Gap closed**: replay_harness_fidelity (6 → 8)  
**Pattern**: action string dispatch (navigate/click/fill/press/wait/expect_text/expect_url/screenshot/eval) + `PlaywrightReplaySession` context manager for persistent browser session  

### P0-5: pre_step_hook + post_step_hook + mean_score in ReplayHarness
**Source**: Stagehand (MIT) — `AgentClient.preStepHook` separation  
**Implemented**: `harvest_index/registry/replay_harness.py` ✅  
**Gap closed**: replay_harness_fidelity (+)  
**Pattern**: Inject `context["current_step"]` before executor call; `pre_step_hook(step, ctx)` for screenshots; `post_step_hook(step, result, ctx)` for assertions; `mean_score` property aggregates judge scores

### P0-6: Playwright JS Rendering Path for CrawleeAdapter
**Source**: Playwright (MIT)  
**Implemented**: `harvest_acquire/crawl/crawlee_adapter.py` ✅  
**Gap closed**: crawl_acquisition (6 → 8)  
**Pattern**: `use_js_rendering=True` → `_fetch_url_playwright()` → networkidle wait → `page.content()` HTML; auto-detects Playwright at construction; falls back to HTTP-only if not installed

---

## P1 — Next Sprint Candidates

### P1-1: Typed Action Schema for Replay Steps
**Source**: trycua/cua (MIT) — `libs/cua-bench/cua_bench/actions.py`  
**Status**: Pattern described, not yet implemented  
**Gap**: replay_harness_fidelity → enable typed action validation before execution  
**Pattern**: Dataclass union (`NavigateAction | ClickAction | FillAction | ...`) + regex dispatch table replacing raw `startswith()` chains; `DoneAction` sentinel for loop termination; enables LLM judge to read typed fields rather than parsing raw strings  
**Effort**: ~2h, zero new deps

### P1-2: Firecrawl Markdown Pipeline (Now MIT)
**Source**: Firecrawl (MIT — upstream license reclassified from AGPL)  
**Status**: Not yet implemented; was previously blocked  
**Gap**: multi_format_ingest — Firecrawl's `apps/api` has production-grade HTML→Markdown with table preservation, list normalization, and link extraction  
**Pattern**: Study `apps/api/src/scraper/scrapeURL/transformers/llmExtract.ts` for LLM-guided extraction fallback; `apps/go-html-to-md-service` for Go-based high-throughput conversion  
**Note**: Verify upstream MIT claim before embedding code; the PRIME.md lesson flags this as "PRD license assumptions are partly stale"

### P1-3: BM25 + Embedding Hybrid Search for PackVectorStore
**Source**: Crawl4AI (Apache-2.0) — hybrid retrieval pattern  
**Status**: Not yet implemented  
**Gap**: vector_search_integration — current PackVectorStore is dense OR tfidf, never hybrid  
**Pattern**: Score = α × BM25_score + (1-α) × cosine_similarity; α tunable at query time; re-rank top-K dense results with BM25 to handle exact-match queries that embeddings miss  
**Effort**: ~3h, no new deps (BM25 already in P0-3)

### P1-4: Browser Session Action Annotation from Browser-Use
**Source**: browser-use (MIT) — `browser_use/agent/` action recording  
**Status**: Not yet implemented  
**Gap**: observation_plane_depth — Browser-use captures DOM state before/after each action; DH only captures screenshots  
**Pattern**: Record `{action, before_dom_hash, after_dom_hash, element_xpath, timestamp}` tuples; enables procedure inference to diff DOM states instead of relying purely on screenshots  
**Effort**: ~4h, requires Playwright

### P1-5: OpenAdapt Recording → PackStep Inference
**Source**: OpenAdapt (MIT) — `openadapt/` recording + segmentation  
**Status**: Not yet implemented  
**Gap**: specialization_pack_quality — OpenAdapt's `create_recording()` + `process_recording()` pipeline infers procedure steps from raw desktop events; DH's `ProcedureInferrer` is stub-level  
**Pattern**: Adapt OpenAdapt's `segment_recording()` → step boundary detection using action type transitions (keyboard→mouse = new step); map to `PackStep` schema  
**Effort**: ~6h, MIT, no AGPL risk

---

## Blocked (AGPL — architecture reference only)

| Repo | Why blocked | What we can learn architecturally |
|------|------------|-----------------------------------|
| Skyvern | AGPL | Task decomposition schema; structured output for browser actions |
| OpenRecall | AGPL | Timestamp-indexed screenshot store; search-over-history query model |
| Paperless-ngx | AGPL | Document intake queue; classification pipeline structure |

---

## Review Required

| Repo | Issue | Action |
|------|-------|--------|
| Screenpipe | License file in `ee/` subdirectory (enterprise edition?) | Read root LICENSE; only use patterns from non-`ee/` crates |
| OpenResearcher | No LICENSE file found | Do not harvest until license confirmed |
| Firecrawl | Upstream now shows MIT, PRD said AGPL | Confirm with current upstream `LICENSE` before any code transplant |

---

## Coverage Map: Gap → Donor

| DH Gap | Primary Donor | Secondary Donor | Status |
|--------|--------------|-----------------|--------|
| ui_reviewer_workflow | openadapt-desktop (MIT) | — | P0 implemented ✅ |
| eval_harness | open-deep-research (MIT) | trycua-cua (MIT) | P0 implemented ✅ |
| crawl_acquisition | crawl4ai (Apache-2.0) | playwright (MIT) | P0 implemented ✅ |
| replay_harness_fidelity | stagehand (MIT) | playwright (MIT) | P0 implemented ✅ |
| vector_search_integration | crawl4ai hybrid pattern | — | P1 |
| observation_plane_depth | browser-use (MIT) | openadapt (MIT) | P1 |
| specialization_pack_quality | openadapt (MIT) | — | P1 |
| multi_format_ingest | firecrawl (MIT, verify) | markitdown (MIT) | P1 |
