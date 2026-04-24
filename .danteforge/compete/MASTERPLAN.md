# DANTEHARVEST — Competitive Masterplan
**Generated:** 2026-04-21  
**Build state:** Phase 8 complete · 242 tests · Weighted avg score: **7.85/10**  
**Mission:** Become the definitive rights-governed, evidence-chained, replay-verified knowledge distillation system for the Dante ecosystem and the OSS world.

---

## The One-Sentence Positioning

> DanteHarvest is the **only** knowledge acquisition system that combines append-only evidence provenance, training-rights governance, replay-verified pack promotion, and constitutional doctrine enforcement — shipping everything local-first with zero AGPL contamination.

---

## Competitive Landscape Summary

### Where DanteHarvest Already Wins (11/20 dimensions — best-in-class)

| Dimension | DH Score | Best OSS | Best Closed | Why DH Wins |
|---|:---:|:---:|:---:|---|
| Rights Model Completeness | **10** | 3 (Paperless) | 3 (MS Recall) | Only tool with training rights at artifact level |
| Constitutional Doctrine | **10** | 3 (Screenpipe) | 3 (MS Recall) | robots.txt hard stop, AGPL quarantine, one-door — unique |
| Local-First Guarantee | **10** | 8 (Screenpipe) | 7 (Rewind) | Zero network calls by default across ALL planes |
| AGPL Quarantine | **10** | 4 (Instructor) | 5 (all) | No competitor tracks or enforces this |
| Pack Promotion Pipeline | **9** | 2 (OpenAdapt) | 1 | 6-gate promotion + sealed receipt — unique |
| Evidence Chain Robustness | **9** | 4 (Screenpipe) | 5 (MS Recall) | SHA-256 per entry, sequence, fsync, verify — unique depth |
| Test Coverage | **9** | 8 (LlamaIndex) | 4 (Cursor) | 242 tests, all planes, adversarial |
| Taxonomy Builder | **8** | 7 (LlamaIndex) | 6 (Notion) | Co-occurrence graph — no LLM required |
| Eval Harness | **8** | 7 (Haystack) | 3 (MS Recall) | EvalPack + gate-driven promotion |
| Specialization Pack Quality | **8** | 5 (LlamaIndex) | 4 (Notion) | Typed domain specialization schema |
| DanteAgents Integration | **8** | 4 (AutoGen) | 1 | Only system producing HarvestHandoff |

### Where DanteHarvest Has Gaps (9/20 dimensions — sprint targets)

| Rank | Dimension | DH | Best | Gap | Priority |
|:---:|---|:---:|:---:|:---:|:---:|
| 1 | **UI/Reviewer Workflow** | 3 | 9 (Rewind.ai) | **6** | 🔴 CRITICAL |
| 2 | **Transcription Quality** | 6 | 9 (Screenpipe) | **3** | 🟠 HIGH |
| 3 | **Observation Plane Depth** | 7 | 9 (Screenpipe) | **2** | 🟠 HIGH |
| 4 | **Vector Search Integration** | 7 | 9 (LlamaIndex) | **2** | 🟡 MEDIUM |
| 5 | **Crawl Acquisition** | 7 | 9 (Firecrawl) | **2** | 🟡 MEDIUM |
| 6 | **Multi-Format Ingest** | 7 | 9 (LlamaIndex) | **2** | 🟡 MEDIUM |
| 7 | **CLI Completeness** | 7 | 9 (Aider) | **2** | 🟡 MEDIUM |
| 8 | **Redaction Accuracy** | 7 | 8 (Haystack) | **1** | 🟢 LOW |
| 9 | **Replay Harness Fidelity** | 8 | 7 (OpenAdapt) | **−1** | ✅ DH LEADS |

---

## Sprint Roadmap

### Sprint 1 — CRITICAL: Reviewer UI (Gap 6 vs Rewind.ai)
**Target dimension:** `ui_reviewer_workflow` → 3 → **8**  
**Why first:** This is the only dimension where DanteHarvest scores below 5. Every closed-source competitor (Rewind.ai: 9, Notion: 9, Mem: 8) has a polished reviewer UX. Without one, DanteHarvest is CLI-only and operator-facing only.

```
harvest_ui/reviewer/server.py          — FastAPI server (uvicorn)
harvest_ui/reviewer/routes/packs.py   — GET /packs, GET /packs/{id}, POST /packs/{id}/approve
harvest_ui/reviewer/routes/chain.py   — GET /runs/{id}/chain
harvest_ui/reviewer/static/           — React SPA (pack diff, confidence band, approve/reject)
```

**Done-condition:** `harvest serve` starts a web server; reviewer can view pack steps, see confidence band, click Approve → receipt issued, or Reject → reason stored in chain.

---

### Sprint 2 — HIGH: Speaker Diarization (Gap 3 vs Screenpipe)
**Target dimension:** `transcription_quality` → 6 → **9**  
**Why second:** Screenpipe's killer feature is multi-speaker identification in recorded sessions. Without it, DanteHarvest transcripts can't attribute actions to specific operators.

```
harvest_normalize/transcribe/diarizer.py   — pyannote.audio (MIT) speaker diarization
harvest_normalize/transcribe/whisper_adapter.py  — integrate diarizer output into TranscriptResult
```

**Done-condition:** `WhisperAdapter(diarize=True)` produces `TranscriptWord` with `speaker_id` field; `to_segments()` groups by speaker × window.

---

### Sprint 3 — HIGH: Video Keyframe Integration (Gap 2, weight 9)
**Target dimension:** `observation_plane_depth` → 7 → **9**  
**Why third:** `harvest_normalize/ocr/keyframes.py` already exists but is disconnected from the observation plane. Wiring it closes the video capture loop.

```
harvest_observe/browser_session/video_integrator.py  — extract frames from video artifacts
harvest_observe/screen/screen_recorder.py            — integrate keyframe → OCR → chain pipeline
```

**Done-condition:** `harvest observe browser <trace>` auto-extracts keyframes from video attachments, runs OCR, stores frame artifacts in chain.

---

### Sprint 4 — MEDIUM: Semantic Embedding Engine (Gap 2 vs LlamaIndex)
**Target dimension:** `vector_search_integration` → 7 → **9**  
**Why fourth:** TF-IDF is lexical. LlamaIndex uses sentence-transformers for semantic similarity. Adding local embedding with a zero-config fallback closes the gap.

```
harvest_index/search/embedding_engine.py   — sentence-transformers local embedding
harvest_index/search/pack_vector_store.py  — wire embedding_engine as default when model cached
```

**Done-condition:** `PackVectorStore()` uses sentence-transformers when `all-MiniLM-L6-v2` is cached locally; falls back to TF-IDF on cold start (local-first preserved).

---

### Sprint 5 — MEDIUM: Sitemap + Batch Crawl (Gap 2 vs Firecrawl)
**Target dimension:** `crawl_acquisition` → 7 → **9**  
**Why fifth:** Firecrawl's sitemap seeding lets operators crawl entire documentation sites in one command. CrawleeAdapter currently requires manual URL seeding.

```
harvest_acquire/crawl/sitemap_parser.py    — parse sitemap.xml, seed RequestQueue
harvest_ui/cli.py                          — `harvest crawl --sitemap <url>` flag
```

**Done-condition:** `harvest crawl --sitemap https://docs.example.com` fetches sitemap.xml, enqueues all URLs, crawls up to max_pages with robots.txt crawl-delay respected.

---

### Sprint 6 — MEDIUM: Spreadsheet + EPUB Ingest (Gap 2 vs LlamaIndex)
**Target dimension:** `multi_format_ingest` → 7 → **9**  
**Why sixth:** LlamaIndex has 50+ loaders. Adding XLSX/CSV (openpyxl: MIT) and EPUB (ebooklib: BSD) covers the most common gaps.

```
harvest_acquire/files/file_ingestor.py    — add XLSX/CSV/EPUB dispatch
harvest_normalize/markdown/xlsx_adapter.py — table-to-markdown via openpyxl
```

**Done-condition:** `harvest ingest file report.xlsx` produces markdown table output; `harvest ingest file book.epub` produces chapter-split markdown.

---

### Sprint 7 — MEDIUM: CLI Batch + Watch Mode (Gap 2 vs Aider)
**Target dimension:** `cli_completeness` → 7 → **9**

```
harvest_ui/cli.py   — `harvest ingest batch <dir>` + `harvest watch <dir>`
```

**Done-condition:** `harvest ingest batch ./docs` ingests all supported files in directory; `harvest watch ./inbox` monitors for new files and auto-ingests.

---

### Sprint 8 — LOW: NER-Based Redaction (Gap 1 vs Haystack)
**Target dimension:** `redaction_accuracy` → 7 → **9**

```
harvest_core/rights/ner_redactor.py   — spaCy (MIT) or presidio (MIT) NER pipeline
harvest_core/rights/redaction_scanner.py  — integrate NER as optional enrichment
```

**Done-condition:** `RedactionScanner(use_ner=True)` detects context-dependent PII (person names, org names, locations) that regex misses.

---

## Dimensions DanteHarvest Should OWN Forever

These are architectural moats that no competitor can easily replicate:

1. **Rights Model** — 7 SourceClass × 6 TrainingEligibility × RetentionClass at ingest. Training data governance is a legal necessity in 2026. Nobody else does this.

2. **Constitutional Doctrine** — robots.txt hard stop, AGPL quarantine, fail-closed everywhere, local-first by default. This is a trust anchor that proprietary tools can't credibly claim.

3. **Evidence Chain** — SHA-256 per entry, sequence numbers, cryptographic receipt. This is the audit trail that enterprise and regulated industries need.

4. **Pack Promotion Pipeline** — 6 gates + human signoff + sealed receipt. This is how knowledge graduates from raw capture to trusted, replayable artifact.

5. **DanteAgents Integration** — The only source of HarvestHandoff packs. This is the moat that grows as the Dante ecosystem grows.

---

## What NOT to Build

- **24/7 always-on screen recorder as default mode** — invasive, privacy risk, kills battery. Optional observation is the right call.
- **Distributed crawler fleet** — Firecrawl/Crawlee own this at scale. DanteHarvest is a local-first acquisition tool, not a web-scale scraper.
- **Foundation model training infrastructure** — Harvest produces training-eligible artifacts. Training itself belongs to the sovereign training runtime.
- **Anti-bot / CAPTCHA circumvention** — Constitutional violation. robots.txt is a hard stop, not a hint.
- **AGPL embedding** — AGPL tools (Firecrawl, Skyvern) are integration targets only. Never import.

---

## Target State (Post All 8 Sprints)

| Dimension | Current | Target | vs Best |
|---|:---:|:---:|:---:|
| UI/Reviewer Workflow | 3 | 8 | = Rewind.ai |
| Transcription Quality | 6 | 9 | = Screenpipe |
| Observation Plane Depth | 7 | 9 | = Screenpipe |
| Vector Search | 7 | 9 | = LlamaIndex |
| Crawl Acquisition | 7 | 9 | = Firecrawl |
| Multi-Format Ingest | 7 | 9 | = LlamaIndex |
| CLI Completeness | 7 | 9 | > Aider |
| Redaction Accuracy | 7 | 9 | > Haystack |
| **Weighted Average** | **7.85** | **9.2+** | **Best-in-class** |

**Unique advantages that no sprint can replicate:**  
Rights model, constitutional doctrine, evidence chain, pack promotion, AGPL quarantine — these remain DanteHarvest's permanent moat.

---

## Command to Continue

```bash
# Next sprint:
# Sprint 1 — Reviewer UI
# Create: harvest_ui/reviewer/server.py

# After each sprint, re-score:
danteforge score_all

# Check remaining gaps:
# Review .danteforge/compete/matrix.json sprint_queue
```
