# DANTEHARVEST Clarifications

## Ambiguities

| # | Ambiguity | Resolution |
|---|---|---|
| A1 | "Observation plane" scope in v1 | v1 ships browser-session recorder only. Desktop/screen capture is beta. Always-on memory capture is opt-in only, never default. |
| A2 | When is human signoff required? | Required for all source_class except owned_internal and synthetic_eval. Auto-promotion is only allowed for synthetic_eval. |
| A3 | What counts as "deterministic" step graph? | A ProcedureGraph is deterministic if every step has exactly one next step or a bounded set of branches with explicit conditions. Unbounded loops or conditional branches on external state are non-deterministic. |
| A4 | Does Harvest own the DanteAgents pack consumption API? | No. Harvest defines the pack schema (WorkflowPack, SkillPack, etc.). DanteAgents owns the consumption contract. Harvest emits JSON-serializable packs; DanteAgents reads them. |
| A5 | What is "local-first" precisely? | All capture, normalization, storage, and chain writes happen on the local filesystem by default. No artifact is written to remote storage without an explicit RunContract field `remote_sync=True`. |

## Missing Requirements

| # | Gap | Decision |
|---|---|---|
| MR1 | No defined SQLite schema for run registry | Phase 1 task P1-1 will define it. Initial impl uses in-memory dict with optional SQLite persistence. |
| MR2 | No spec for pack versioning strategy | Packs use semver strings. Breaking schema changes require major version bump. Registry keeps all versions. |
| MR3 | No spec for reviewer identity model | Phase 4. For Phase 1-2, reviewer identity is a plain string (email or service ID). |
| MR4 | No spec for retention enforcement | Phase 1 records deletion_at on RightsProfile. Enforcement daemon is a Phase 5+ task. |
| MR5 | No spec for MCP server exposure | Deferred until Phase 5 (pack registry ready). Jina MCP patterns will be the design reference. |

## Consistency Checks

| Check | Status |
|---|---|
| ChainEntry.signal format `plane.action` matches HarvestEventKind values | ✅ Consistent |
| RightsProfile.is_promotion_eligible() checks same conditions as evaluate_promotion() gates | ✅ Consistent — both check review_status, legal_hold, requires_redaction, training_eligibility |
| EvidenceReceipt.all_gates_passed() aligns with PromotionResult.eligible | ✅ Consistent — both require all PolicyDecision.passed == True |
| ConfidenceBand boundaries (0.90/0.75/0.50) match PRD §Confidence and approval logic | ✅ Exact match |
| Pack promotion_status enum matches pack registry lifecycle | ✅ CANDIDATE → PROMOTED | REJECTED | DEPRECATED |

## Clarification Log

**2026-04-21** — Confirmed: AGPL donors (Firecrawl, Skyvern, OpenRecall) are integration targets only. No AGPL code in `harvest_*` packages.

**2026-04-21** — Confirmed: `personal_device_memory` source class defaults to training_eligibility=FORBIDDEN and retention_class=SHORT (rolling window). Opt-in capture requires explicit operator action.

**2026-04-21** — Confirmed: robots.txt check is a constitutional hard stop. No operator config can override it. The only way to fetch a disallowed URL is to change the site's robots.txt.



## Q: Why not fork DanteDistillerV2 as the base?
DanteDistillerV2 contains high-value provenance/export DNA but also duplication smells (dual hybrid search stacks, dual audit bundle paths), no desktop observation, and generated build artifacts committed to the repo. A clean-room Harvest repo with DanteDistillerV2 embedded as a frozen read-only donor avoids transplanting technical debt while preserving the strongest patterns.

## Q: What is the difference between Harvest and a web scraper?
A web scraper extracts data. Harvest **acquires evidence with rights accountability**. Every artifact carries a RightsProfile. Every operation emits a chain entry. Every promoted pack requires a sealed EvidenceReceipt with all six promotion gates passing. The output is a rights-scoped, replay-tested pack — not raw data.

## Q: Why four pack types?
- `workflowPack` captures full multi-step processes (the most common output from demonstrations)
- `skillPack` captures atomic reusable capabilities that workflows can reference
- `specializationPack` bundles a domain for downstream agent specialization (consumes workflow + skill packs)
- `evalPack` provides reproducible benchmarks so promotions can be regression-tested

## Q: Why is training_eligibility UNKNOWN by default for public_web?
Public web content may be under restrictive copyright even when publicly accessible. Defaulting to REFERENCE_ONLY (not UNKNOWN) for public web means it's usable as context and citation but not as training data without an explicit reviewer decision.

## Q: When does an artifact need human reviewer signoff?
Whenever `source_class` is not `owned_internal` or `synthetic_eval`, or when the content was captured from an external session (customer demo, licensed video, public web). `synthetic_eval` artifacts (sandboxed test traces) can be auto-promoted without human signoff.

## Q: What is the Observation plane vs the Acquisition plane?
- **Acquisition**: pulls content from external sources (files, URLs, crawl). The operator triggers it explicitly.
- **Observation**: captures what a human is doing in real-time (screen, desktop, browser session). It runs alongside the human, not instead of the human.

## Q: Why does robots.txt check fail-closed on network errors?
A network error when fetching robots.txt means the site policy is unreadable — whether the site is unreachable or robots.txt is actively blocked. Proceeding with a fetch in either case violates site policies that were not confirmed readable. Fail-closed is the only defensible default: treat unreadable policy as DISALLOW.

## Q: What happens to low-confidence (RED/ORANGE) artifacts?
They are stored as raw evidence but blocked from pack promotion. The system generates reviewer tasks. Humans can correct labels, merge/split segments, or re-run inference. After correction, the confidence score is re-evaluated and the artifact may be re-classified.

## Q: Is Harvest a replacement for DanteAgents?
No. Harvest produces packs. DanteAgents consumes packs. The two systems have a clean interface boundary: Harvest emits, DanteAgents executes. Harvest does not own agent runtime logic.

## Q: What does "fail-closed" mean in Harvest?
Every module MUST raise a typed exception when it cannot complete its contract — it MUST NOT return None, an empty result, or a degraded output. For example: ArtifactStore.get() MUST raise StorageError on a missing id, not return None. This makes failures explicit and prevents silent data loss downstream.

## Q: What does "zero-ambiguity" mean?
Every public API MUST return a deterministic type. Optional[X] MUST only appear where absence is a valid domain state (e.g., replay_environment on an EvalPack). Callers MUST NOT need to check isinstance() or type-guard return values. The violation condition MUST be explicit in the docstring.

## Q: What does "one-door doctrine" mean?
Each subsystem MUST have exactly one typed entry point. Callers MUST NOT bypass typed entry points to call internal helpers directly. This ensures the chain is always written, rights are always checked, and errors are always typed.

## Q: Why is AGPL quarantined?
AGPL requires any user of the code — even over a network — to publish source. Harvest is MIT-licensed. Importing AGPL code into harvest_* packages would force harvest_* to become AGPL. Quarantine means AGPL tools MUST only be called as external processes or services — never imported.
