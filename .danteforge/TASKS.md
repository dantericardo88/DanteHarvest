# DANTEHARVEST Tasks

## Constitutional Doctrine Applied to All Tasks
Every task MUST enforce the following constitutional guarantees:
- **Fail-closed**: errors raise explicitly, never return None silently
- **Zero-ambiguity**: every return value has a deterministic type; no silent degradation
- **Local-first**: no external network calls unless task explicitly requires them
- **One-door doctrine**: all access flows through a single typed entry point
- **AGPL quarantine**: AGPL-licensed dependencies MUST NOT be imported in non-AGPL modules
- **Append-only chain**: every state change MUST emit a ChainEntry before raising

## Phase 0 — Complete ✅
**Done-condition**: 44 unit tests pass, all Phase 0 modules importable, PRIME.md loaded.

### Phase 0 Deliverables (all complete)
- [x] Repo scaffold + pyproject.toml — **done-condition**: `pip install -e .[dev]` succeeds; `harvest_*` packages importable
- [x] All harvest_* packages with __init__.py — **done-condition**: `import harvest_core` succeeds without error
- [x] 10 donor files transplanted — **done-condition**: imports resolve to harvest_* namespace, not donors/
- [x] harvest_core/control/exceptions.py — **done-condition**: all exception types importable; HarvestError is base class
- [x] harvest_core/control/artifact_schemas.py — **done-condition**: all PRD artifact layers modelled; Pydantic v2 validation passes
- [x] harvest_core/rights/rights_model.py — **done-condition**: default_rights_for() returns correct defaults per source_class; UNKNOWN source_class fails loudly
- [x] harvest_core/rights/evidence_receipt.py — **done-condition**: receipt.verify() returns True after creation; tampered receipt returns False
- [x] harvest_core/evaluation/gates.py — **done-condition**: evaluate_promotion() enforces all 6 gates; single failing gate makes eligible=False
- [x] harvest_distill/packs/pack_schemas.py — **done-condition**: all 4 pack types serialize to JSON; Pydantic validation rejects missing required fields
- [x] .danteforge/ artifacts — **done-condition**: danteforge score_all returns non-zero scores for all 5 artifacts

---

## Phase 1 — Evidence and Rights Spine ✅ COMPLETE

### P1-1 RunContract and RunRegistry ✅
- [x] `harvest_core/control/run_contract.py` — **done-condition**: RunContract auto-generates UUID; is frozen (immutable); chain_file_path() includes run_id and project_id; fail-closed: source_class must be a valid SourceClass enum value
- [x] `harvest_core/control/run_registry.py` — **done-condition**: create_run() emits run.created chain entry; update_run_state() rejects invalid transitions with HarvestError; get_run() raises HarvestError on unknown run_id (fail-closed); terminal states block further transitions (zero-ambiguity)

### P1-2 File Ingestor ✅
- [x] `harvest_acquire/files/file_ingestor.py` — **done-condition**: ingest emits acquire.started → acquire.completed; SHA-256 matches file bytes; RightsProfile attached at ingest; acquire.failed emitted on missing file before AcquisitionError raised (fail-closed); local-first: no network calls

### P1-3 MarkItDown Adapter ✅
- [x] `harvest_normalize/markdown/markitdown_adapter.py` — **done-condition**: converts supported file to non-empty markdown; emits normalize.completed chain entry; unsupported extension raises NormalizationError (not empty string — zero-ambiguity); normalize.failed emitted before raise

### P1-4 Receipt Issuer ✅
- [x] `harvest_core/provenance/receipt_issuer.py` — **done-condition**: issued receipt passes receipt.verify(); all 6 gate names appear in policy_decisions; receipt.issued or receipt.denied chain entry always emitted; fail-closed: incomplete provenance triggers receipt.denied not silence

### P1-5 Redaction Scanner ✅
- [x] `harvest_core/rights/redaction_scanner.py` — **done-condition**: detects AWS key (AKIA...) pattern; detects email addresses; detects private key headers; redact() removes all findings; scan_secrets_only=True skips PII patterns; raises HarvestError on non-string input (zero-ambiguity)

### P1-6 Integration Test ✅
- [x] `tests/integration/test_phase1_ingest_to_receipt.py` — **done-condition**: full pipeline (ingest→chain→manifest→receipt) passes with real filesystem; receipt.verify() returns True; missing file test confirms acquire.failed in chain; no live network required (local-first)

---

## Phase 2 — Acquisition v1 ✅ COMPLETE

### P2-1 URL Ingestor ✅
- [x] `harvest_acquire/urls/url_ingestor.py` — **done-condition**: robots.txt violation raises ConstitutionalError and emits acquire.failed; successful fetch produces non-empty markdown artifact stored locally (local-first); acquire.completed chain entry includes sha256 and rights_status; AcquisitionError on network failure (fail-closed)

### P2-2 Crawl4AI Adapter ✅
- [x] `harvest_acquire/crawl/crawl4ai_adapter.py` — **done-condition**: crawl() emits crawl.started + crawl.page_fetched + crawl.completed; HTTP fallback works when Crawl4AI not installed; crawl.failed emitted before AcquisitionError on exception (fail-closed)

### P2-3 Chunker ✅
- [x] `harvest_normalize/chunking/chunker.py` — **done-condition**: fixed strategy: chunks cover all input chars; sentence strategy: no chunk breaks mid-sentence; topic strategy: splits on markdown headings; empty input returns empty list (not error); all chunks have strategy field set (zero-ambiguity)

### P2-4 Artifact Store ✅
- [x] `harvest_index/artifacts/artifact_store.py` — **done-condition**: identical content deduplicated (same sha256 → same artifact_id); get() raises StorageError on missing id (fail-closed); index persists across instances; list() filters correctly by run_id and source_type

---

## Phase 3 — Observation v1 ✅ COMPLETE

### P3-1 Browser Session Recorder ✅
- [x] `harvest_observe/browser_session/session_recorder.py` — **done-condition**: start_session emits session.started; record_action emits session.action_recorded per action; end_session writes session.json manifest and emits session.completed; ingest_trace_file produces same signals from JSON trace (zero-ambiguity: trace format is documented)

### P3-2 Screen Recorder ✅
- [x] `harvest_observe/screen/screen_recorder.py` — **done-condition**: ingest_frame_directory emits screen.frame_captured per frame with sha256; screen.completed includes frame_count; missing directory raises ScreenObservationError immediately (fail-closed); all frames stored locally before any chain entry written (local-first)

### P3-3 Audio Recorder ✅
- [x] `harvest_observe/audio/audio_recorder.py` — **done-condition**: ingest_file emits audio.started → audio.chunk_written → audio.completed; duration estimated from WAV header when available; missing file emits audio.failed before raising AudioObservationError (fail-closed)

### P3-4 Transcript Aligner ✅
- [x] `harvest_normalize/align/transcript_aligner.py` — **done-condition**: action within window_seconds of a segment gets confidence > 0; action outside all segments gets confidence == 0.0 (zero-ambiguity: no None values); alignment_rate computed as aligned/total; align_from_dict handles raw dict input

---

## Phase 4 — Distillation v1 ✅ COMPLETE

### P4-1 Task Segmenter ✅
- [x] `harvest_distill/segmentation/task_segmenter.py` — **done-condition**: navigation action splits current span; idle gap > threshold splits span; empty action list returns 0 spans (not error); all span_ids unique; each span has start_time and end_time set (zero-ambiguity)

### P4-2 Procedure Inferrer ✅
- [x] `harvest_distill/procedures/procedure_inferrer.py` — **done-condition**: repeated n-gram across ≥2 spans produces ProcedureGraph with confidence > 0; graphs sorted by confidence descending; best() returns highest-confidence graph; empty spans returns empty InferenceResult (fail-closed: no exception)

### P4-3 Pack Builder ✅
- [x] `harvest_distill/packs/pack_builder.py` — **done-condition**: build_workflow_pack emits pack.created chain entry; empty graph raises PackagingError (fail-closed); WorkflowPack has promotion_status=CANDIDATE; SkillPack has skill_name set

---

## Phase 5 — Eval and Promotion ✅ COMPLETE

### P5-1 Pack Registry ✅
- [x] `harvest_index/registry/pack_registry.py` — **done-condition**: promote() raises RegistryError when receipt_id is None (fail-closed); promote() raises RegistryError if pack already PROMOTED (zero-ambiguity: double-promotion blocked); index persists across PackRegistry instances; list() filters correctly by status and pack_type

### P5-2 Replay Harness ✅
- [x] `harvest_index/registry/replay_harness.py` — **done-condition**: noop executor produces pass_rate=1.0; always-fail executor produces pass_rate=0.0; executor exception recorded as FAILED step with error field (fail-closed: no silent swallowing); eval.started + eval.step_executed + eval.completed emitted; empty pack has pass_rate=0.0 (zero-ambiguity)

---

## Phase 6 — Specialization Handoff ✅ COMPLETE

### P6-1 SpecializationPack Builder ✅
- [x] `harvest_distill/packs/specialization_builder.py` — **done-condition**: non-PROMOTED pack reference raises PackagingError (fail-closed); build without registry skips validation; specialization.created chain entry emitted; glossary and taxonomy stored verbatim; domain field required (zero-ambiguity)

### P6-2 DanteAgents Contract ✅
- [x] `harvest_distill/packs/dante_agents_contract.py` — **done-condition**: export() raises PackagingError on non-PROMOTED pack (fail-closed); HarvestHandoff.to_json() produces valid JSON with pack field; write_export_bundle writes one file per promoted pack; export_all() returns only PROMOTED packs

### P6-3 harvest CLI ✅
- [x] `harvest_ui/cli.py` — **done-condition**: `harvest version` prints version; `harvest pack list` shows all registered packs; `harvest ingest file <path>` emits artifact_id + sha256 JSON; exit code 1 on error; exit code 0 on success (zero-ambiguity)

---

## Phase 7 — OSS Pattern Integration (In Progress)

### P7-1 Vector Search (from Qdrant)
- [ ] `harvest_index/search/pack_vector_store.py` — **done-condition**: upsert(pack_id, text) succeeds with local in-process fallback; query(text, limit=5) returns ranked results; missing pack_id in query returns empty list (not error); StorageError raised if embedding model unavailable and no fallback configured (fail-closed); local-first: no remote Qdrant server required by default
- [ ] `tests/unit/test_pack_vector_store.py` — **done-condition**: 10 tests covering upsert, query, dedup, filter-by-type, and empty-corpus edge cases

### P7-2 Retry Policy (from Crawl4AI RateLimiter)
- [ ] `harvest_acquire/urls/retry_policy.py` — **done-condition**: exhausted retries raise AcquisitionError (fail-closed); acquire.retry chain entry emitted per attempt; exponential backoff with jitter: delay_n = base * factor^n + random(0, jitter); retry_on list is configurable; zero-ambiguity: max_retries=0 means no retries (immediate fail)
- [ ] `tests/unit/test_retry_policy.py` — **done-condition**: 6 tests: immediate success, success on retry N, exhaustion, custom status codes, zero retries, chain signal verification

### P7-3 Metadata Enricher (from LlamaIndex IngestionPipeline)
- [ ] `harvest_normalize/chunking/metadata_enricher.py` — **done-condition**: enrich() adds title, word_count, source_path, chunk_index to each chunk; heuristic title extracted from first markdown heading or first sentence; no LLM call required by default (local-first); optional llm_client path raises ImportError if dependency missing (zero-ambiguity)
- [ ] `tests/unit/test_metadata_enricher.py` — **done-condition**: 5 tests: heading extraction, sentence fallback, word_count accuracy, source_path passthrough, empty chunk handling

### P7-4 SimHash Dedup (from Screenpipe SimHash)
- [ ] `harvest_index/artifacts/dedup.py` — **done-condition**: identical text → hamming_distance=0; single-char diff → hamming_distance < 5; threshold=3 catches near-duplicates; DedupIndex.is_near_duplicate returns bool (never raises on valid string input — zero-ambiguity); local-first: in-memory index, no external service
- [ ] `tests/unit/test_dedup.py` — **done-condition**: 7 tests: identical text, near-duplicate, distinct text, empty string, threshold boundary, add+query, clear index

### P7-5 EvalPack Builder (PRD gap)
- [ ] `harvest_distill/packs/eval_pack_builder.py` — **done-condition**: build() raises PackagingError on empty task_cases (fail-closed); EvalPack has promotion_status=CANDIDATE; eval_pack.created chain entry emitted; EvalPack serializes to valid JSON; success_metrics stored verbatim (zero-ambiguity: no default metrics injected)
- [ ] `tests/unit/test_eval_pack_builder.py` — **done-condition**: 5 tests: successful build, empty cases raises, chain signal, JSON serialization, success_metrics passthrough
