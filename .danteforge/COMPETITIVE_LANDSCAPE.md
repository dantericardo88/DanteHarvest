# DANTEHARVEST — Competitive Landscape
**Updated:** 2026-04-21 | **Build:** Phase 8 · 242 tests

## TL;DR

DanteHarvest **leads** on 11 of 20 dimensions — most of which are uncopyable moats (rights model, evidence chain, constitutional doctrine). It has **actionable gaps** on 9 dimensions, the largest being the reviewer UI (gap=6 vs Rewind.ai/Notion/Mem).

No single competitor covers more than 5 of DanteHarvest's 20 dimensions at a competitive score. The space is fragmented: crawlers don't care about rights; screen recorders don't distill packs; RAG pipelines don't replay or promote.

---

## Competitor Profiles

### Firecrawl (AGPL · OSS+Cloud)
**What it does:** LLM-ready web crawling. JS rendering, sitemap parsing, structured extraction.  
**Beats DH on:** `crawl_acquisition` (9 vs 7), `cli_completeness` (8 vs 7)  
**DH beats it on:** Every dimension that matters — rights, chain, promotion, local-first, AGPL quarantine (Firecrawl IS the AGPL to quarantine)  
**Threat level:** LOW. Acquisition-only tool, no provenance, no rights, no distillation.

### Screenpipe (MIT · OSS)
**What it does:** 24/7 local screen + audio capture with Whisper OCR and speaker diarization.  
**Beats DH on:** `observation_plane_depth` (9 vs 7), `transcription_quality` (9 vs 6)  
**DH beats it on:** Rights model, pack promotion, evidence chain, eval harness, taxonomy, DanteAgents integration  
**Threat level:** MEDIUM. Best screen recorder in OSS. Sprint 2 (diarization) + Sprint 3 (keyframes) close the observation gap.

### OpenAdapt (MIT · OSS)
**What it does:** Desktop automation recording with action segmentation and replay.  
**Beats DH on:** `observation_plane_depth` (8 vs 7), `replay_harness_fidelity` (7 vs 8 — DH slightly ahead)  
**DH beats it on:** Rights model, evidence chain, pack schema, CLI, DanteAgents integration  
**Threat level:** LOW-MEDIUM. Closest conceptual sibling. DH is architecturally more rigorous.

### LlamaIndex (MIT · OSS)
**What it does:** Data ingestion pipeline + vector indexing for RAG applications.  
**Beats DH on:** `vector_search_integration` (9 vs 7), `multi_format_ingest` (9 vs 7)  
**DH beats it on:** Rights model, evidence chain, pack promotion, replay, taxonomy, constitutional doctrine  
**Threat level:** MEDIUM. Sprint 4 (embeddings) + Sprint 6 (formats) close the gap.

### LangChain (MIT · OSS)
**What it does:** LLM orchestration framework with document loaders and tool use.  
**Beats DH on:** `vector_search_integration` (8 vs 7), `multi_format_ingest` (8 vs 7)  
**DH beats it on:** All provenance/rights/promotion dimensions  
**Threat level:** LOW. Different scope — LangChain orchestrates, Harvest produces artifacts.

### Haystack (Apache-2.0 · OSS)
**What it does:** NLP pipeline for document QA, RAG, evaluation.  
**Beats DH on:** `redaction_accuracy` (8 vs 7), `eval_harness` (7 vs 8 — DH slightly ahead)  
**DH beats it on:** Rights, chain, observation, taxonomy, DanteAgents  
**Threat level:** LOW. Sprint 8 (NER redaction) closes the redaction gap.

### Microsoft Recall (Proprietary · Closed)
**What it does:** Windows 11 always-on screen memory with semantic search.  
**Beats DH on:** `observation_plane_depth` (10 vs 7), `ui_reviewer_workflow` (8 vs 3)  
**DH beats it on:** Rights model (Recall has no training rights concept), local-first (Recall syncs to cloud), AGPL quarantine, pack promotion, evidence chain  
**Threat level:** MEDIUM for observation plane. Sprint 3 closes depth gap.

### Rewind.ai (Proprietary · Closed)
**What it does:** Mac screen + audio recorder with semantic search, clip sharing, AI summaries.  
**Beats DH on:** `ui_reviewer_workflow` (9 vs 3), `transcription_quality` (9 vs 6), `observation_plane_depth` (9 vs 7)  
**DH beats it on:** Rights model (no concept), pack promotion (none), evidence chain (none), local-first (Rewind syncs)  
**Threat level:** HIGH for UX. Sprint 1 (reviewer UI) is the direct counter.

### Notion AI (Proprietary · Closed)
**What it does:** AI-powered workspace with smart search, knowledge organization, AI writing.  
**Beats DH on:** `ui_reviewer_workflow` (9 vs 3), `taxonomy_builder` (6 vs 8 — DH leads slightly)  
**DH beats it on:** Local-first (Notion is cloud-first), rights model, evidence chain, pack promotion  
**Threat level:** LOW. Different user — Notion is for knowledge workers, DH is for AI training pipeline operators.

### AutoGen / CrewAI (MIT · OSS)
**What they do:** Multi-agent orchestration frameworks.  
**Beats DH on:** `dante_agents_integration` pattern (4 vs 8 — DH leads by having typed pack schema)  
**DH beats them on:** Acquisition, observation, rights, chain, promotion — everything upstream  
**Threat level:** LOW. These are consumers of packs, not producers.

### Cursor / Aider (Closed + Apache · mixed)
**What they do:** AI-assisted coding environments with codebase indexing.  
**Beats DH on:** `cli_completeness` (Aider: 9 vs 7), `ui_reviewer_workflow` (Cursor: 7 vs 3)  
**DH beats them on:** Everything in the knowledge capture → rights → distillation stack  
**Threat level:** NONE for DH's domain. Only comparable on CLI UX.

---

## DanteHarvest Moat Summary

```
Rights Model          ████████████████████ 10/10  ← No competitor within 7 points
Constitutional        ████████████████████ 10/10  ← No competitor tracks this
Local-First           ████████████████████ 10/10  ← Deepest commitment in class
AGPL Quarantine       ████████████████████ 10/10  ← No competitor tracks this
Pack Promotion        ██████████████████░░  9/10  ← No competitor within 7 points
Evidence Chain        ██████████████████░░  9/10  ← No competitor within 5 points
Test Coverage         ██████████████████░░  9/10  ← Best in OSS class
```

These dimensions are the **permanent moat**. They require months of architectural commitment to replicate, not a sprint.

---

## Dimension-by-Dimension: Who to Beat

| Dimension | Primary Target | How to Beat |
|---|---|---|
| UI/Reviewer Workflow | Rewind.ai (9) | FastAPI + React reviewer: pack diff, approve/reject, timeline |
| Transcription Quality | Screenpipe (9) | Add pyannote.audio speaker diarization to WhisperAdapter |
| Observation Depth | Screenpipe (9) | Wire keyframe extractor → OCR → chain pipeline |
| Vector Search | LlamaIndex (9) | sentence-transformers local embedding, Qdrant production |
| Crawl Acquisition | Firecrawl (9) | sitemap.xml seeding, crawl-delay, LLM extraction hook |
| Multi-Format Ingest | LlamaIndex (9) | XLSX/CSV (openpyxl), EPUB (ebooklib), Slack/Notion loaders |
| CLI Completeness | Aider (9) | `harvest ingest batch`, `harvest watch`, progress TUI |
| Redaction Accuracy | Haystack (8) | spaCy NER or presidio for context-aware PII |

---

## Competitive Position Score (Weighted)

| System | Weighted Score | Category |
|---|:---:|---|
| **DanteHarvest (current)** | **7.85** | Leader in provenance/rights/promotion |
| **DanteHarvest (post-sprint)** | **~9.2** | Best-in-class overall |
| LlamaIndex | ~5.1 | Leader in ingestion/indexing |
| Screenpipe | ~4.8 | Leader in observation |
| OpenAdapt | ~4.2 | Leader in desktop replay |
| Haystack | ~4.0 | Leader in NLP pipeline/eval |
| Firecrawl | ~3.5 | Leader in crawl acquisition |
| Rewind.ai | ~3.8 | Leader in closed-source UX |
| Microsoft Recall | ~4.0 | Leader in observation (closed) |

*Weighted by dimension weights defined in matrix.json*
