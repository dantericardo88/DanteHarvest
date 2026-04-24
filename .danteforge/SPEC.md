# DANTEHARVEST Specification

## What
DANTEHARVEST acquires, normalizes, distills, and packages rights-governed procedural knowledge into reusable packs. It is not an autonomous agent — it is a **pack factory and knowledge refinery** that gives the Dante ecosystem a renewable supply of trusted, replay-tested, rights-scoped artifacts.

## Feature List

| # | Feature | Phase |
|---|---|---|
| F1 | File ingest (PDF, DOCX, image, video) with rights profile | 1 |
| F2 | URL ingest with robots.txt enforcement and Playwright rendering | 2 |
| F3 | Crawl acquisition (Crawl4AI / Crawlee adapters) | 2 |
| F4 | OCR normalization (Tesseract) | 1 |
| F5 | Video keyframe extraction | 1 |
| F6 | File-to-Markdown normalization (MarkItDown) | 1 |
| F7 | Append-only evidence chain (ChainWriter) | 0 ✅ |
| F8 | Rights model and RightsProfile (SourceClass, TrainingEligibility) | 0 ✅ |
| F9 | Confidence bands (GREEN/YELLOW/ORANGE/RED) | 0 ✅ |
| F10 | Six-gate promotion evaluator | 0 ✅ |
| F11 | EvidenceReceipt (self-sealing, immutable) | 0 ✅ |
| F12 | Pack schemas (WorkflowPack, SkillPack, SpecializationPack, EvalPack) | 0 ✅ |
| F13 | Browser session recorder (Playwright trace + screenshots) | 3 |
| F14 | Desktop screen/audio observation (local-first) | 3 |
| F15 | Transcript/UI state aligner | 3 |
| F16 | Task segmenter + procedure graph inferrer | 4 |
| F17 | Pack builder from procedure graph | 4 |
| F18 | Replay harness + eval packs | 5 |
| F19 | Pack registry with version history | 5 |
| F20 | SpecializationPack builder for DanteAgents | 6 |

## Product Summary
DANTEHARVEST is a **rights-governed evidence acquisition and apprenticeship backbone**. It ingests files, URLs, browser sessions, and desktop demonstrations; normalizes them via OCR/transcription/markdown; records provenance; infers procedures; and promotes high-confidence workflows into reusable packs consumed by DanteAgents, DanteCode, and sovereign training runtimes.

## Core User Stories

### Acquisition
- As an operator, I can ingest a local file (PDF, DOCX, image, video) and receive a normalized artifact with a RightsProfile and chain entry.
- As an operator, I can ingest a URL; the system checks robots.txt, fetches and renders the page via Playwright, converts to Markdown, and records provenance.
- As an operator, I can start a browser session recorder that captures a Playwright trace, screenshots, and network log as a `rawBrowserTrace`.

### Observation
- As an operator (beta), I can start a desktop session recorder that captures a `rawScreenSession` with screen video, OCR blocks, and input events.
- As an operator, I can import a previously recorded video (Loom, onboarding, SOP) as a `rawVideoAsset` with explicit rights declaration.

### Normalization
- As the system, I convert every raw artifact into a normalized form: text chunks, OCR blocks, aligned segments, and keyframe hashes — all with provenance links.

### Distillation
- As the system, I segment aligned content into `taskSpan` records and infer `procedureGraph` structures from approved demonstrations.
- As a reviewer, I can view, edit, merge, and split task segments before they are promoted.

### Packaging and Promotion
- As the system, I build `candidatePack` records from high-confidence procedure graphs and run replay evaluation.
- As the system, I issue a sealed `EvidenceReceipt` when all six promotion gates pass.
- As an operator, I can export any promoted pack as a JSON artifact consumable by DanteAgents.

## Pack Types

| Type | Purpose |
|---|---|
| `workflowPack` | Full multi-step process |
| `skillPack` | Reusable atomic capability |
| `specializationPack` | Domain bundle for downstream agent |
| `evalPack` | Reproducible benchmark/test case set |

## Source Classes and Default Policies

| Source class | Training eligibility | Retention |
|---|---|---|
| owned_internal | allowed | long |
| customer_confidential | forbidden | policy_bound |
| licensed_reference | unknown | long |
| public_web | reference_only | medium |
| personal_device_memory | forbidden | short |
| synthetic_eval | allowed | long |
| oss_code_or_docs | unknown | long |

## Confidence Bands

| Band | Score | Behavior |
|---|---:|---|
| GREEN | ≥ 0.90 | Replay + promotion candidate |
| YELLOW | 0.75–0.89 | Draft pack, human review required |
| ORANGE | 0.50–0.74 | Evidence only |
| RED | < 0.50 | Raw/diagnostic only |

## Non-Goals (v1)
- Full autonomous desktop agent
- 24/7 always-on screen memory as default
- Bespoke model training infrastructure
- Anti-bot or CAPTCHA circumvention
- Distributed crawler fleet

## Technical Stack
- Language: Python 3.11+
- Pydantic v2 for all schemas
- Playwright for browser acquisition
- Tesseract (via pytesseract) for OCR
- OpenCV for keyframe extraction
- MarkItDown for file-to-markdown normalization
- Asyncio throughout (all I/O is async)
- Local SQLite / filesystem for storage (Phase 1-2)
- Qdrant for vector search (Phase 5)

## User Stories
(Already listed above under Core User Stories — see Acquisition, Observation, Normalization, Packaging and Promotion sections)

## Non-functional Requirements
- All I/O is async (asyncio-first, no blocking calls on the main thread)
- Chain integrity check must complete in < 1s for runs up to 10k entries
- robots.txt check must timeout in ≤ 10s (fail-closed on timeout)
- EvidenceReceipt seal must be deterministic: same inputs → same receipt_hash
- Pack schemas must be JSON-serializable via `model_dump(mode="json")`
- Local-first: zero network calls during normalization of local files
- All tests must run without live network access (mock external calls)

## Acceptance Criteria

### AC1 — Ingest and Chain
- Given a local PDF with source_class=owned_internal
- When file_ingestor.ingest() is called
- Then: artifact is stored, SHA-256 is recorded, chain entry is emitted, RightsProfile has training_eligibility=ALLOWED

### AC2 — robots.txt Hard Stop
- Given a URL whose robots.txt disallows HarvestBot
- When url_ingestor.ingest() is called
- Then: ConstitutionalError is raised, no page is fetched, chain entry records rights.denied

### AC3 — Promotion Gate Enforcement
- Given a CandidatePack with rights_status=pending
- When evaluate_promotion() is called
- Then: eligible=False, failing_gates includes "rights_status"

### AC4 — EvidenceReceipt Integrity
- Given a sealed EvidenceReceipt
- When receipt_hash field is tampered
- Then: receipt.verify() returns False

### AC5 — Chain Integrity
- Given a ChainWriter with 100 appended entries
- When verify_integrity() is called
- Then: returns (True, None) with no sequence gaps or hash mismatches

### AC6 — Confidence Band Classification
- Given confidence_score=0.90 → GREEN
- Given confidence_score=0.89 → YELLOW
- Given confidence_score=0.50 → ORANGE
- Given confidence_score=0.49 → RED

## OSS Integration Targets
- **Direct donors (MIT/Apache)**: Crawl4AI, Crawlee, Stagehand patterns, Screenpipe, OpenAdapt capture
- **Integration only (AGPL)**: Firecrawl API, Skyvern MCP bridge

## Constitutional Doctrine
These are MUST-level guarantees that every module in harvest_* MUST enforce:

1. **Fail-closed**: every module MUST raise a typed exception on error — it MUST NOT return None, empty, or silently degrade
2. **Zero-ambiguity**: every public API MUST return a deterministic type; Optional[X] MUST be used only where absence is a valid domain state
3. **Local-first**: all normalization, storage, and chain writes MUST operate on the local filesystem by default; no remote call MUST occur without `remote_sync=True` in RunContract
4. **One-door doctrine**: access to each subsystem MUST flow through exactly one typed entry point (e.g., FileIngestor, ArtifactStore); callers MUST NOT bypass to internal helpers
5. **AGPL quarantine**: AGPL-licensed code (Firecrawl, Skyvern, OpenRecall) MUST NOT be imported inside harvest_* packages; they MUST be called only as external services
6. **Append-only chain**: every state change MUST emit a ChainEntry before raising any exception — the chain MUST NOT be left in a silent failure state
