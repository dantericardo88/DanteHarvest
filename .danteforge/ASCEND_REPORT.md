# DANTEHARVEST Ascend Report

Generated: 2026-04-21

## Summary

| Artifact | Before | After | Delta | Decision |
|---|---:|---:|---:|---|
| CONSTITUTION | 0 | 97 | +97 | advance |
| SPEC | 0 | 96 | +96 | advance |
| CLARIFY | 0 | 96 | +96 | advance |
| PLAN | 0 | 93 | +93 | advance |
| TASKS | 0 | 81 | +81 | warn |

## Build Status: COMPLETE

All 6 phases built and tested. **169 tests passing (0 failures).**

## Phases Complete

### Phase 0 (complete)
- Full `harvest_*` package tree (9 planes, 40+ modules)
- 10 donor files transplanted from DanteDistillerV2 with updated imports
- Constitutional backbone: ChainWriter, ChainEntry, signals, RightsProfile, EvidenceReceipt, gates, pack schemas
- 44 unit tests

### Phase 1 (complete)
- `harvest_core/control/run_contract.py` — immutable RunContract with auto UUID
- `harvest_core/control/run_registry.py` — state machine with fail-closed transitions and chain entries
- `harvest_acquire/files/file_ingestor.py` — SHA-256 + rights + chain entries + fail-closed error path
- `harvest_normalize/markdown/markitdown_adapter.py` — MarkItDown wrapper with chain entries
- `harvest_core/provenance/receipt_issuer.py` — EvidenceReceipt from chain + manifest + all 6 gates
- `harvest_core/rights/redaction_scanner.py` — 14 regex patterns (AWS keys, GitHub tokens, SSN, email, PII)
- `tests/integration/test_phase1_ingest_to_receipt.py` — end-to-end golden path test

### Phase 2 (complete)
- `harvest_acquire/urls/url_ingestor.py` — robots.txt → Playwright → local store → chain entries
- `harvest_acquire/crawl/crawl4ai_adapter.py` — Crawl4AI async crawler with HTTP fallback
- `harvest_normalize/chunking/chunker.py` — fixed/sentence/topic chunking strategies
- `harvest_index/artifacts/artifact_store.py` — hash-addressed content store with deduplication

### Phase 3 (complete)
- `harvest_observe/browser_session/session_recorder.py` — Playwright session + trace ingest
- `harvest_observe/audio/audio_recorder.py` — WAV ingest with wave duration estimation
- `harvest_observe/screen/screen_recorder.py` — frame directory ingest with SHA-256 per frame
- `harvest_normalize/align/transcript_aligner.py` — temporal alignment of transcript ↔ actions

### Phase 4 (complete)
- `harvest_distill/segmentation/task_segmenter.py` — navigation/idle-gap span segmentation
- `harvest_distill/procedures/procedure_inferrer.py` — n-gram frequency → ProcedureGraph
- `harvest_distill/packs/pack_builder.py` — WorkflowPack + SkillPack from ProcedureGraph

### Phase 5 (complete)
- `harvest_index/registry/pack_registry.py` — promotion-gated pack registry (receipt required)
- `harvest_index/registry/replay_harness.py` — deterministic step executor + ReplayReport

### Phase 6 (complete)
- `harvest_distill/packs/specialization_builder.py` — SpecializationPack from promoted packs
- `harvest_distill/packs/dante_agents_contract.py` — HarvestHandoff export for DanteAgents/DanteCode
- `harvest_ui/cli.py` — `harvest` CLI with ingest/pack/stats/version commands

## Test Count: 169 passing, 0 failures

## Ceiling Dimensions

| Dimension | Current | Ceiling | Manual Action Required |
|---|---|---|---|
| `TASKS testability` | 81 | ~85 | Add done-conditions task-by-task as work progresses |
| `communityAdoption` | n/a | 4/10 | Publish project, README, GitHub presence |
| `enterpriseReadiness` | n/a | 5/10 | Requires production deployments and customer validation |

## Lessons Captured
- Clean-room repo + selective transplants beats forking the donor
- Rights model must be first-class at ingest time, not a bolt-on
- RunRegistry state machine must be fail-closed on invalid transitions AND unknown run_id
- Terminal states must have empty transition sets to enforce immutability
- Hash-addressed content stores eliminate duplicate storage automatically
- N-gram frequency is sufficient for procedure inference without LLM dependency
- PackRegistry promotion gate (receipt required) must be enforced at the registry, not the caller
- Only PROMOTED packs flow to DanteAgents — enforced at SpecializationPackBuilder and DanteAgentsExporter
