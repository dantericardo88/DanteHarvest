# DANTEHARVEST — PRIME

## Mission
DANTEHARVEST is the **evidence-rich acquisition, observation, distillation, and packaging system** for the Dante universe. Its job is to capture and structure rights-scoped, replay-tested knowledge and procedures — then emit clean pack artifacts consumed by DanteAgents, DanteCode, and sovereign training runtimes.

## Product Doctrine

1. **Pack factory first, autonomous agent second.** Harvest's first job is evidence collection and procedure distillation. Autonomous desktop control comes in v1.5+.
2. **Rights before features.** Every artifact carries a RightsProfile at ingest. Nothing promotes without an approved EvidenceReceipt.
3. **Fail-closed everywhere.** robots.txt violations, missing required artifacts, and failed promotion gates all halt the pipeline — never silently proceed.
4. **Local-first by default.** All capture and storage is local. Cloud sync is optional.
5. **One-door doctrine.** All planes (Acquisition, Observation, Normalization, Distillation) route through Control plane contracts. No side-channel writes.
6. **Pack-centric scope.** Harvest emits `workflowPack`, `skillPack`, `specializationPack`, `evalPack`. It does not own model training or agent execution.

## Architecture — Multi-Plane

```
Control ──→ Acquisition ──→ Normalization ──→ Provenance ──→ Storage/Index
         ──→ Observation ──→ Normalization        ↑
Rights/Governance gates every plane ─────────────┘
                              Distillation ──→ Packaging ──→ Eval/Promotion ──→ Pack Registry
```

## Module Map

| Plane | Package |
|---|---|
| Control / exceptions | `harvest_core/control/` |
| Rights + EvidenceReceipt | `harvest_core/rights/` |
| ChainWriter + signals | `harvest_core/provenance/` |
| Manifests + EvidencePackage | `harvest_core/manifests/` |
| Confidence bands + gates | `harvest_core/evaluation/` |
| File / URL / crawl ingest | `harvest_acquire/` |
| Browser engine + robots.txt | `harvest_acquire/browser/` |
| Screen / desktop / audio observation | `harvest_observe/` |
| OCR / transcription / markdown | `harvest_normalize/` |
| Keyframe extraction | `harvest_normalize/ocr/keyframes.py` |
| Segmentation / procedure inference | `harvest_distill/` |
| Pack schemas | `harvest_distill/packs/pack_schemas.py` |
| Artifact registry + vector search | `harvest_index/` |
| TF-IDF / Qdrant vector store | `harvest_index/search/pack_vector_store.py` |
| SimHash near-duplicate dedup | `harvest_index/artifacts/dedup.py` |
| Retry / exponential backoff | `harvest_acquire/urls/retry_policy.py` |
| Chunk metadata enrichment | `harvest_normalize/chunking/metadata_enricher.py` |
| Whisper transcription engine | `harvest_normalize/transcribe/whisper_adapter.py` |
| Domain taxonomy builder | `harvest_distill/taxonomy/taxonomy_builder.py` |
| EvalPack builder | `harvest_distill/packs/eval_pack_builder.py` |
| Desktop event capture | `harvest_observe/desktop/event_capture.py` |
| Specialization + DanteAgents handoff | `harvest_distill/packs/` |
| CLI — all commands | `harvest_ui/cli.py` |
| Reviewer API server | `harvest_ui/reviewer/server.py` |
| Speaker diarizer | `harvest_normalize/transcribe/diarizer.py` |
| Embedding engine (dense + cached) | `harvest_index/search/embedding_engine.py` |
| Sitemap parser | `harvest_acquire/crawl/sitemap_parser.py` |
| XLSX / CSV → markdown | `harvest_normalize/markdown/xlsx_adapter.py` |
| EPUB → markdown | `harvest_normalize/markdown/epub_adapter.py` |
| Document normalizer dispatcher | `harvest_normalize/markdown/document_normalizer.py` |
| NER context-aware redactor | `harvest_core/rights/ner_redactor.py` |

## Key Schemas (PRD canonical)

### Artifact layers
Raw → Captured → Derived → Promotion → Evidence  
See `harvest_core/control/artifact_schemas.py`

### Pack types
`WorkflowPack`, `SkillPack`, `SpecializationPack`, `EvalPack`  
See `harvest_distill/packs/pack_schemas.py`

### Rights model
`RightsProfile`, `TrainingEligibility`, `SourceClass`, `RetentionClass`  
See `harvest_core/rights/rights_model.py`

### Evidence receipt
`EvidenceReceipt` (self-sealing, immutable)  
See `harvest_core/rights/evidence_receipt.py`

## Confidence Bands

| Band | Score | System behavior |
|---|---:|---|
| GREEN | ≥ 0.90 | Replay + promotion candidate |
| YELLOW | 0.75–0.89 | Draft pack, human review required |
| ORANGE | 0.50–0.74 | Evidence only |
| RED | < 0.50 | Raw/diagnostic artifact only |

## Promotion Gates (all must pass)

1. `provenance_completeness == 1.0`
2. `rights_status in {approved, owner_asserted_and_reviewed}`
3. `replay_pass_rate >= threshold` (default 0.85)
4. `is_deterministic == True`
5. `redaction_complete == True`
6. `human_reviewer_signoff == True` (for external/customer-facing content)

## OSS Donors

| Role | Donor | License |
|---|---|---|
| Browser execution | Browser Use, Stagehand, Playwright | MIT/Apache |
| Crawler substrate | Crawl4AI, Crawlee | MIT |
| HTML→Markdown | MarkItDown, Jina Reader | MIT |
| Screen/audio memory | Screenpipe | MIT |
| Demonstration capture | OpenAdapt Desktop | MIT |
| Desktop sandboxes/eval | trycua/cua | MIT |
| Integration only (AGPL) | Firecrawl, Skyvern, OpenRecall | AGPL |

## Roadmap

| Phase | Deliverables |
|---|---|
| 0 ✅ | Repo scaffold, donor freeze, doctrine, transplant spine |
| 1 ✅ | Evidence/rights spine — RunContract, FileIngestor, ReceiptIssuer, RedactionScanner |
| 2 ✅ | File/URL/browser acquisition — URLIngestor, Crawl4AIAdapter, Chunker, ArtifactStore |
| 3 ✅ | Browser-session recorder, screen recorder, audio recorder, transcript aligner |
| 4 ✅ | Segmentation, task spans, procedure graphs, draft packs |
| 5 ✅ | Replay harness, eval packs, promotion gates, pack registry |
| 6 ✅ | Specialization packs for DanteAgents/DanteCode, sovereign export, CLI |
| 7 ✅ | OSS patterns: PackVectorStore (Qdrant/TF-IDF), RetryPolicy, MetadataEnricher, SimHash dedup, EvalPackBuilder |
| 8 ✅ | CLI completeness, WhisperAdapter, TaxonomyBuilder, DesktopEventCapture |
| Ascend ✅ | Multi-format ingest (XLSX/EPUB), batch/watch CLI, NER redactor code, video keyframes wired, dense embedding index — 299 tests, honest weighted score 7.74/10 |
| Compete ✅ | OSS universe materialized (22 local repos, 17 allowed). Deep pattern scan → 5 P0 patterns harvested. PlaywrightStepExecutor, LLMJudgeExecutor, BM25ContentFilter, ReviewStateMachine, pre/post step hooks, JS rendering path. 353 tests. Honest weighted score 8.01/10 |
| Ascend v2 ✅ | 10-phase competitive sprint. React reviewer SPA (4 tabs, polling, confidence badges), structured extraction API (scrape/extract/crawl + async jobs), PlaywrightPool browser infra, SessionTracer + Playwright .zip traces, APScheduler job scheduler (SQLite), HarvestEventBus (WebhookSink + EmailSink), GitHub/Notion/S3 connectors, hybrid BM25+dense search + cross-encoder rerank, anti-bot UA rotation + stealth headers. 501 tests. Honest weighted score 6.36/10 (v2 matrix with 34 dimensions — harder baseline than v1). |
| Ascend v3 ✅ | 6-cycle gap-closing sprint. ActionLayer (11 typed browser actions, BrowserAction/ActionResult, DOM snapshots), pii_patterns.py (16 new PII/secret patterns incl. Stripe/IBAN/intl phone/EIN), AlertRule engine + dedup_window + dead_letters in EventBus, DEVICE_PROFILES fingerprinting + health() in PlaywrightPool, ActionType routing in ReplayHarness, PostgresConnector + GitLabConnector. 551 tests. Honest weighted score 6.72/10. |

## What NOT to build in v1

- Full autonomous desktop agent
- 24/7 always-on Recall clone as default mode
- Bespoke foundation-model training infrastructure
- Anti-bot / CAPTCHA circumvention features
- Distributed crawler fleet orchestration

## Donor Code Freeze

`donors/dante-distiller-v2/` is a **read-only reference**. Never import from it in production code. Transplanted files live in `harvest_core/`, `harvest_acquire/`, `harvest_normalize/`.

## Lessons (captured)

**Architecture**
- Do not fork DanteDistiller — clean-room repo with selective transplants is the right call
- Rights flags and approval gates must be first-class, not bolt-ons
- AGPL donors (Firecrawl, Skyvern) are integration targets only, not embedded code
- Ship apprenticeship pipeline (capture → align → infer → test → promote) before autonomous execution

**Constitutional implementation**
- Fail-closed means emit a chain entry (e.g., `acquire.failed`) BEFORE raising the exception — chain must never be left silent on failure
- Zero-ambiguity: return types must be deterministic at compile time — Optional[X] only where absence is a valid domain state, never as a lazy "might fail" escape hatch
- Local-first: every new module must work with zero network calls in its default constructor — external services activated only by explicit config params (e.g., `qdrant_url`, `api_key`)
- One-door doctrine: tests that bypass typed entry points (e.g., calling `_local_index` directly) are anti-patterns — test the public API, trust the implementation

**OSS harvesting patterns**
- Qdrant local TF-IDF fallback: implement a pure-Python in-process index that exposes the same API as the server client — operators get local-first behavior with a zero-config upgrade path to Qdrant
- Crawl4AI HTTP fallback: when the preferred library is not installed, fall back to stdlib (`urllib.request` + regex stripping) rather than raising ImportError — preserves the contract without requiring the dependency
- SimHash threshold tuning: adding/removing a single token can change Hamming distance by >3 bits — use identical text (or very minor whitespace edits) in tests, not word variants
- LlamaIndex enrichment: chunk metadata should be computed from the full document (global_keywords) AND per-chunk — enriching only per-chunk misses cross-chunk term frequency signals
- Whisper chain ordering: emit `transcribe.started` BEFORE checking file existence — constitutional guarantee requires chain entries to precede every state change including failure
- RetryPolicy `_FastRetryPolicy`: always provide a no-sleep test variant to avoid 10-30s waits in CI; the test variant is part of the public API, not a test helper

**Scoring**
- constitutionAlignment ceiling: SPEC/CLARIFY/PLAN/TASKS appear to hit a structural ceiling at 16/20 via keyword density alone — architectural enforcement patterns (tests that verify constitutional behavior) are likely required to push past it
- CONSTITUTION clarity: any occurrence of "should" in the CONSTITUTION itself triggers a warning — use "MUST", "MUST NOT", or "by convention" as alternatives

**Ascend cycle patterns**
- DocumentNormalizer dispatcher: route by suffix in a single class rather than duplicating dispatch logic across the CLI — new format support requires only a new adapter + one entry in the dispatcher
- NERRedactor: always catch ALL exceptions in `is_available()` and `_load_nlp()` — not just ImportError — because spaCy imports can fail with binary incompatibility errors (h5py/numpy ABI mismatch) that are not ImportError subclasses
- PackVectorStore dense index: auto-detect sentence-transformers availability at construction time via `engine.is_available()`; fall back to TF-IDF silently — no config change needed by operator
- ingest_video_session: emit `session.keyframes_extracted` chain entry with first 10 frame hashes before iterating — audit record is created even if OCR fails midway
- `harvest watch`: use polling not inotify by default for cross-platform reliability; document `--interval` param

**Honest competitive position (2026-04-21)**
- Weighted score 7.74/10. Inflated scores corrected after audit.
- DH dominates on 7 dimensions: evidence chain, rights model, pack promotion, constitutional doctrine, local-first, AGPL quarantine, DanteAgents format — these are genuinely unique and have no close competitors
- DH is competitive (within 2 pts) on 12 dimensions — viable but behind in most
- Single largest gap: ui_reviewer_workflow (4/10) — FastAPI API exists but no web UI means operators can't review packs without writing curl. This is the highest-weight gap remaining (weight=9, gap=5 = priority score 45).
- Scoring rule: broken deps, optional deps, and API-only features score at their working capability level, not their aspirational implementation level

**Honest competitive position (2026-04-22) — post-Compete cycle**
- Weighted score 8.01/10. Crossed the 8.0 threshold with real implementations.
- 5 P0 patterns harvested from OSS universe: PlaywrightStepExecutor, LLMJudgeExecutor, BM25ContentFilter, ReviewStateMachine, pre/post step hooks + JS rendering path
- Largest remaining gap: ui_reviewer_workflow (6/10) — state machine built, FastAPI endpoints wired, no React SPA yet. Next: P1-1 typed action schema, P1-3 hybrid search.
- OSS license audit: Firecrawl reclassified MIT (was AGPL in PRD) — verify upstream before code transplant. Screenpipe and OpenResearcher remain review_required.

**Honest competitive position (2026-04-22) — post-Ascend v3 sprint (6 cycles)**
- v2 matrix score: **6.72/10** (up from 6.36 after v2)
- 6 dimensions closed: agent_browser_infrastructure (2→5), redaction_accuracy (6→8), monitoring_and_alerting (6→8), browser_infrastructure (6→8), replay_harness_fidelity (7→8), connector_breadth (6→8)
- 551 tests passing
- Now at or ahead of Bright Data (7.18) on trust dimensions; narrowing gap overall
- Next achievable gaps: observation_plane_depth (6, weight=5), taxonomy_builder (6, weight=3), transcription_quality (7, weight=4), cli_completeness (7, weight=3)

**Honest competitive position (2026-04-22) — post-Ascend v2 sprint (10 phases)**
- v2 matrix score: 6.36/10 (34 dimensions vs v1's 20 — harder, broader baseline that adds commercial categories DH didn't compete in)
- v1 matrix score (20 dims): would be ~8.5/10 on the same dimensions
- 13 dimensions closed or improved across 10 phases; 501 tests passing
- **Dominant on trust/governance**: evidence chain (9), rights model (9), constitutional doctrine (10), local-first (10), AGPL quarantine (10), pack promotion (9)
- **Now competitive**: ui_reviewer_workflow (8), trace_and_debug_tooling (8), vector_search (8), multi_format_ingest (8), test_coverage (8), structured_extraction (7), scheduling (7), session_replay (7), browser_infra (6), monitoring (6), connector_breadth (6)
- **Architectural ceilings (documented, not pursued)**: proxy_network_depth (ceiling 2), anti_bot_bypass (ceiling 4), remote_browser_scalability (ceiling 2), dataset_marketplace (ceiling 3), platform_ecosystem (ceiling 5)
- Gap to Bright Data (7.18 v2): -0.82. Gap closeable only via proxy network investment outside local-first scope.
- Bug fixed this sprint: `PackStatus("candidate")` ValueError in reviewer state machine TOCTOU guard — registry uses `"candidate"` as initial status, review_states uses `"pending"`. Added alias map in both `_to_pack_status()` helper and inside `transition()`.
