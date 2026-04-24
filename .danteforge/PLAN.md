# DANTEHARVEST Implementation Plan

## Architecture

DANTEHARVEST is a **multi-plane system** where each plane has a narrow mandate and communicates only through chain entries and run contracts. No plane reads another plane's internal state directly.

```
Control ──→ Acquisition ──→ Normalization ──→ Provenance ──→ Storage/Index
         ──→ Observation ──→ Normalization        ↑
Rights/Governance gates every plane ─────────────┘
         Distillation ──→ Packaging ──→ Eval/Promotion ──→ Pack Registry
```

**Key architectural invariants:**
- All plane I/O is async
- Every artifact write emits a ChainEntry (one-door doctrine)
- Rights plane gates every ingest and every promotion
- No AGPL dependencies in `harvest_*` packages

## Technology

| Concern | Choice | Rationale |
|---|---|---|
| Language | Python 3.11+ | Async-first, type hints, Pydantic v2 |
| Schema validation | Pydantic v2 | Strict mode, JSON serialization, model_config |
| Browser automation | Playwright | Cross-browser, traces, MCP/CLI hooks, MIT |
| HTTP client | httpx | Async, timeout control, robots.txt fetching |
| OCR | pytesseract + Pillow | Swappable via OCRBackend protocol |
| Video keyframes | OpenCV headless | Lightweight, no GUI deps |
| Markdown normalization | MarkItDown | MIT, handles PDF/DOCX/PPTX/image |
| Crawling | Crawl4AI + Crawlee patterns | MIT, async, LLM-friendly output |
| Vector search | Qdrant (Phase 5) | Local or self-hosted, MIT |
| Local storage | Filesystem + SQLite | Zero-infra, local-first |
| Testing | pytest + pytest-asyncio | Async test support, no live network |

## Implementation Strategy

1. **Transplant then extend** — donor code transplanted with updated imports; new Harvest-specific modules layered on top
2. **Interface-first** — OCRBackend Protocol allows engine swap without caller changes
3. **Async everywhere** — ChainWriter, PlaywrightEngine, all ingestors use asyncio
4. **Test coverage gates promotion** — no plane is considered complete without unit + integration tests
5. **No premature abstractions** — each phase ships working code; generalization happens when the third similar implementation appears
6. **Zero-ambiguity contract** — every public API has deterministic behavior: the success path is defined, the failure path raises a typed exception, and silence is never a valid response
7. **Fail-closed by default** — acquisition, normalization, and promotion errors always raise; partial outputs are never emitted silently; chain entries record every failure before propagation
8. **Local-first guarantee** — all capture, storage, and processing default to the local machine; no data leaves the device without explicit operator opt-in; cloud sync and remote vector search are optional add-ons wired through configuration, never hardcoded defaults

## Risk

| Risk | Mitigation |
|---|---|
| Donor drag | Clean-room repo; donors/ is read-only; transplant ledger in PLAN.md |
| AGPL contamination | Enforce no AGPL imports in `harvest_*` via CI lint rule |
| Rights contamination | Default FORBIDDEN for customer/personal data; require human signoff |
| Premature autonomy | Desktop agent blocked until Phase 5+ replay harness proven |
| False confidence | Promotion only after replay evaluation; confidence band gates promotion |
| Monolith relapse | Strict plane boundary enforcement; each plane in its own package |

## Testing Strategy

- **Unit tests**: All models, gates, chain operations — no live network, no filesystem side effects (use tmp_path)
- **Integration tests**: End-to-end ingest → chain → manifest → receipt pipelines with real filesystem
- **Adversarial tests**: Corrupted chain entries, tampered receipts, disallowed URLs, missing required artifacts
- **No mocking of core invariants**: Chain integrity tests use real ChainWriter; rights tests use real RightsProfile — never mock the thing being tested

## Phase 0 — Repo Foundation ✅ COMPLETE
**Gate**: Repo compiles; donor isolated; doctrine documented.

### Deliverables (complete)
- [x] Clean-room repo scaffold: `harvest_core/`, `harvest_acquire/`, `harvest_observe/`, `harvest_normalize/`, `harvest_distill/`, `harvest_index/`, `harvest_ui/`, `tests/`
- [x] `pyproject.toml` with package discovery, dev dependencies
- [x] All `__init__.py` files for subpackages
- [x] Transplanted donor files with updated imports:
  - `harvest_core/provenance/chain_writer.py`
  - `harvest_core/provenance/chain_entry.py`
  - `harvest_core/provenance/signals.py`
  - `harvest_core/manifests/export_manifest.py`
  - `harvest_core/manifests/evidence_package_builder.py`
  - `harvest_acquire/browser/robots_validator.py`
  - `harvest_acquire/browser/playwright_engine.py`
  - `harvest_normalize/ocr/ocr_engine.py`
  - `harvest_normalize/ocr/keyframes.py`
- [x] `harvest_core/control/exceptions.py` — Harvest exception hierarchy
- [x] `harvest_core/control/artifact_schemas.py` — canonical artifact layers (Raw→Captured→Derived→Promotion→Evidence)
- [x] `harvest_core/rights/rights_model.py` — RightsProfile, SourceClass, TrainingEligibility, RetentionClass
- [x] `harvest_core/rights/evidence_receipt.py` — immutable self-sealing EvidenceReceipt
- [x] `harvest_core/evaluation/gates.py` — ConfidenceBand, six promotion gates, evaluate_promotion()
- [x] `harvest_distill/packs/pack_schemas.py` — WorkflowPack, SkillPack, SpecializationPack, EvalPack
- [x] `.danteforge/PRIME.md`, `CONSTITUTION.md`, `SPEC.md`, `CLARIFY.md`
- [x] `docs/harvest/DOCTRINE.md`
- [x] 44 unit tests passing (chain, rights, gates, packs)

---

## Phase 1 — Evidence and Rights Spine ✅ COMPLETE
**Gate**: Every ingest emits a chain entry and a rights receipt. `verify_integrity()` passes on all runs.

### Tasks
- [ ] `harvest_core/control/run_contract.py` — RunContract Pydantic model (run_id, project_id, source_class, initiated_by, config)
- [ ] `harvest_core/control/run_registry.py` — in-memory + SQLite-backed run store
- [ ] `harvest_acquire/files/file_ingestor.py` — ingest local files (PDF, DOCX, image, video) → RawVideoAsset/RawAudioStream + chain entry + RightsProfile
- [ ] `harvest_normalize/markdown/markitdown_adapter.py` — wrap MarkItDown for file-to-markdown normalization
- [ ] `harvest_core/provenance/receipt_issuer.py` — service that builds and seals EvidenceReceipt from chain + manifest
- [ ] `harvest_core/rights/redaction_scanner.py` — detect secrets/PII/credentials in text and flag RightsProfile
- [ ] Integration test: file ingest → chain → manifest → receipt → verify

---

## Phase 2 — Acquisition v1 ✅ COMPLETE
**Gate**: File/URL/browser ingest succeeds deterministically with provenance and rights receipts.

### Tasks
- [ ] `harvest_acquire/urls/url_ingestor.py` — URL → Playwright fetch → HTML → Markdown + chain + receipt
- [ ] `harvest_acquire/crawl/crawl4ai_adapter.py` — LLM-friendly crawl with Crawl4AI
- [ ] `harvest_acquire/crawl/crawlee_adapter.py` — request queue + session management via Crawlee patterns
- [ ] `harvest_normalize/markdown/html_to_markdown.py` — Jina Reader / MarkItDown HTML→MD pipeline
- [ ] `harvest_normalize/chunking/chunker.py` — topic/sentence/semantic chunking strategies
- [ ] `harvest_index/artifacts/artifact_store.py` — local filesystem artifact store with hash-addressed content

---

## Phase 3 — Observation v1 ✅ COMPLETE
**Gate**: Session replay reconstructs observed steps from captured browser/desktop session.

### Tasks
- [ ] `harvest_observe/browser_session/session_recorder.py` — Playwright trace + screenshot + network capture
- [ ] `harvest_observe/screen/screen_recorder.py` — continuous screen capture (Screenpipe patterns, local-first)
- [ ] `harvest_observe/audio/audio_recorder.py` — audio stream capture with language hint
- [ ] `harvest_normalize/transcribe/whisper_adapter.py` — speech-to-text for session audio
- [ ] `harvest_normalize/align/aligner.py` — align transcript segments with UI states and action events

---

## Phase 4 — Distillation v1 ✅ COMPLETE
**Gate**: Human reviewer can view, edit, and approve draft packs from observed sessions.

### Tasks
- [ ] `harvest_distill/segmentation/segmenter.py` — produce AlignedSegment and TaskSpan from aligned sessions
- [ ] `harvest_distill/procedures/procedure_inferrer.py` — infer ProcedureGraph from task spans
- [ ] `harvest_distill/packs/pack_builder.py` — build CandidatePack from ProcedureGraph
- [ ] `harvest_ui/` — minimal reviewer UI (pack diff, approve/reject, merge/split segments)

---

## Phase 5 — Eval and Promotion ✅ COMPLETE
**Gate**: Candidate pack passes replay threshold and receives sealed EvidenceReceipt.

### Tasks
- [ ] `harvest_core/evaluation/replay_harness.py` — execute pack steps in sandboxed browser/desktop and record pass/fail
- [ ] `harvest_index/registry/pack_registry.py` — promoted pack store with version history
- [ ] `harvest_index/search/vector_search.py` — Qdrant-backed semantic search over pack corpus
- [ ] Export API: serialize promoted packs for DanteAgents consumption

---

## Phase 6 — Specialization Handoff ✅ COMPLETE
**Gate**: Downstream agent improves on target tasks using Harvest-produced specialization packs.

### Tasks
- [x] `harvest_distill/packs/specialization_builder.py` — compose SpecializationPack from workflow + skill packs (fail-closed: only PROMOTED packs allowed)
- [x] `harvest_distill/packs/dante_agents_contract.py` — DanteAgents HarvestHandoff export (local-first, zero-ambiguity: only PROMOTED packs exported)
- [x] `harvest_ui/cli.py` — harvest CLI (ingest/pack/stats/version commands)

---

## Phase 7 — OSS Pattern Integration (In Progress)
**Gate**: Each pattern has tests; fail-closed behavior preserved; zero-ambiguity on new APIs; local-first defaults respected.

### Patterns harvested from OSS universe

#### 7a — Qdrant Vector Search (from: qdrant-client)
- [ ] `harvest_index/search/pack_vector_store.py` — Qdrant-backed semantic search over pack corpus
  - Local in-process fallback (no Qdrant server required for local-first use)
  - `upsert(pack_id, text, metadata)` → embeds + indexes
  - `query(text, limit, filter_by_type)` → ranked PackEntry results
  - Fail-closed: missing embedding model raises StorageError (not silent empty)

#### 7b — Retry/Backoff (from: Crawl4AI RateLimiter)
- [ ] `harvest_acquire/urls/retry_policy.py` — exponential backoff + jitter for URL acquisition
  - `max_retries`, `base_delay`, `backoff_factor`, `retry_on` (status codes + exception types)
  - Wraps URLIngestor.ingest() — fail-closed: exhausted retries raise AcquisitionError
  - Emits acquire.retry chain entry on each attempt
  - Zero-ambiguity: retry count is deterministic given seed

#### 7c — Metadata Enrichment (from: LlamaIndex IngestionPipeline)
- [ ] `harvest_normalize/chunking/metadata_enricher.py` — extract title/keywords/summary from chunks
  - `enrich(chunks, source_metadata)` → adds title, word_count, source_path, chunk_index to each chunk
  - Local-first: no LLM call required; heuristic title extraction from first heading or sentence
  - Optional LLM enrichment when `llm_client` is provided

#### 7d — SimHash Near-Duplicate Detection (from: Screenpipe SimHash dedup)
- [ ] `harvest_index/artifacts/dedup.py` — SimHash-based near-duplicate detection
  - `simhash(text)` → 64-bit fingerprint
  - `hamming_distance(a, b)` → bit distance
  - `DedupIndex.is_near_duplicate(text, threshold=3)` → bool
  - ArtifactStore uses DedupIndex to skip near-duplicate text artifacts

#### 7e — EvalPack Builder (PRD gap)
- [x] `harvest_distill/packs/eval_pack_builder.py` — build EvalPack from WorkflowPack + test cases
  - `build(workflow_pack, task_cases, success_metrics)` → EvalPack
  - Emits eval_pack.created chain entry
  - Fail-closed: EvalPack with no task cases raises PackagingError

---

## Phase 8 — CLI Completeness + Observation Depth ✅ COMPLETE
**Gate**: Every PRD feature has a CLI entry point; observation plane has desktop event capture and audio transcription.
**Constitutional alignment**: fail-closed on every CLI error; zero-ambiguity on all return codes; local-first defaults.

### Tasks (all complete)

#### 8a — CLI Completeness
- [x] `harvest ingest url <url>` — URL ingest with robots.txt enforcement (fail-closed: ConstitutionalError on disallowed)
- [x] `harvest crawl <url>` — Crawl4AI adapter (fail-closed: AcquisitionError on failure)
- [x] `harvest run create` — create new run with RunContract (zero-ambiguity: returns run_id as JSON)
- [x] `harvest run status <run-id>` — show run state + chain entry count (fail-closed: HarvestError on unknown run)
- [x] `harvest observe browser <trace>` — ingest browser session trace (local-first: reads local JSON file)

#### 8b — Whisper Transcription Engine (from: OpenAdapt/Screenpipe patterns)
- [x] `harvest_normalize/transcribe/whisper_adapter.py` — local Whisper (MIT) + OpenAI API fallback
  - Local-first: uses local whisper model by default; no network call without api_key
  - Fail-closed: missing whisper package raises NormalizationError with install instructions
  - Zero-ambiguity: TranscriptResult.text always str, words always List[TranscriptWord]
  - `to_segments(window_seconds)` → groups words into time-windowed segments for aligner

#### 8c — Taxonomy Builder (PRD gap)
- [x] `harvest_distill/taxonomy/taxonomy_builder.py` — derive domain taxonomy from WorkflowPacks
  - `build(workflow_packs)` → TaxonomyGraph with nodes + edges
  - Fail-closed: empty workflow list raises PackagingError
  - Local-first: co-occurrence frequency analysis; no LLM required
  - SpecializationPack.taxonomy dict populated from TaxonomyGraph.to_dict()

#### 8d — Desktop Event Capture (from: OpenAdapt PerformanceEvent patterns)
- [x] `harvest_observe/desktop/event_capture.py` — keyboard/mouse/window event recording
  - `ingest_event_file(path)` → CaptureSession from JSONL event log (fail-closed: missing file raises ObservationError)
  - `start_live_capture(duration_seconds)` → real-time pynput capture (fail-closed: pynput not installed raises ObservationError)
  - All events stored locally as JSONL before chain entry (local-first)
  - DesktopEventType enum (zero-ambiguity: event_type always typed, never raw str)
