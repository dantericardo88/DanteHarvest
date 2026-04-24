# DANTEHARVEST Constitution

## Mission
DANTEHARVEST is the evidence-rich acquisition, observation, distillation, and packaging system for the Dante ecosystem. Its single job: capture rights-scoped, replay-tested knowledge and procedures — then emit clean pack artifacts consumed by DanteAgents, DanteCode, and sovereign training runtimes.

## Constitutional Guarantees

### G1 — Rights Before Acquisition
No artifact may be acquired without a RightsProfile assigned at ingest. Every ingest call must declare source_class, and the system must apply default retention and training-eligibility flags appropriate to that class. Violation: raise RightsError, reject ingest.

### G2 — robots.txt Hard Stop
The system MUST check robots.txt before fetching any URL. A disallowed URL raises ConstitutionalError and halts the run. Network errors fetching robots.txt are treated as DISALLOW (fail-closed). No override capability.

### G3 — Append-Only Evidence Chain
Every plane transition that produces or transforms an artifact emits a ChainEntry. The chain is append-only, sequenced, and SHA-256 hashed. Integrity verification must pass before any evidence package is shipped.

### G4 — One-Door Doctrine
All artifact writes flow through the Control plane's run contract. Side-channel writes without a chain entry are forbidden. No plane may write to another plane's storage directly.

### G5 — Fail-Closed Packaging
Missing required artifacts raise PackagingError and halt package creation. Partial evidence packages are never emitted. All artifacts are hashed in the manifest before shipping.

### G6 — Promotion Requires All Gates
A candidate pack is not promoted unless ALL six promotion gates pass:
1. provenance_completeness == 1.0
2. rights_status in {approved, owner_asserted_and_reviewed}
3. replay_pass_rate >= threshold (default 0.85)
4. is_deterministic == True
5. redaction_complete == True
6. human_reviewer_signoff == True (for external/customer-facing content)

### G7 — Local-First Default
All capture, processing, and storage default to the local machine. No data leaves the device without explicit operator opt-in. Cloud sync is an optional add-on, never the default.

### G8 — No Autonomous Desktop Agent in v1
DANTEHARVEST v1 captures and distills. It does not execute autonomously on behalf of users at the desktop level. Replay is a test harness for evaluation — not a production execution surface.

### G9 — License Quarantine for AGPL Donors
AGPL/GPL-licensed OSS (Firecrawl, Skyvern, OpenRecall, Paperless-ngx) may be used as integration targets or design references only. They must not be embedded as in-process dependencies in the Harvest core.

### G10 — EvidenceReceipt Is Immutable
Once sealed, an EvidenceReceipt may not be modified. The receipt_hash is computed at creation and must verify before pack promotion. Any mismatch raises EvaluationError.

## Disallowed Behaviors
- Anti-bot or CAPTCHA circumvention as a Harvest feature
- Automatic training on customer_confidential or personal_device_memory artifacts
- Emitting a pack without a sealed EvidenceReceipt
- Skipping robots.txt for any URL regardless of operator instruction
- Writing artifacts to disk without emitting a chain entry
- Promoting a pack with redaction_complete == False

## Zero Ambiguity Rules
Every constitutional guarantee uses the word MUST or MUST NOT. Guarantees written with qualified language ("may", "optionally", "by convention") are conventions, not constitutional rules — they belong in DOCTRINE.md. Zero-ambiguity means: the behavior is deterministic and the violation condition is explicit.

## Atomic Commit Protocol
Every run that produces output artifacts must complete as an atomic unit:
- All chain entries written before any export is attempted
- Manifest built from finalized chain only
- EvidenceReceipt sealed only after manifest hash is computed
- If any step fails, the entire run is marked FAILED in the run registry — no partial outputs are emitted

## Verify Before Commit
Before promoting any CandidatePack to the registry:
1. `chain_writer.verify_integrity()` must return `(True, None)`
2. `manifest_hash` must match the recomputed hash of all artifact hashes
3. `evidence_receipt.verify()` must return `True`
4. All six promotion gates must have `passed == True`
Promotion is rejected and chain-logged if any verification step fails.

## Definitions
- **Pack**: A rights-scoped, replay-tested, evidence-backed, versioned artifact (workflowPack, skillPack, specializationPack, or evalPack)
- **Plane**: A named subsystem with a declared mandate (Acquisition, Observation, Normalization, Provenance, Distillation, Packaging, Evaluation, Control, Rights)
- **Chain**: The append-only JSONL log of all plane operations for a given run
- **Receipt**: The immutable self-sealing EvidenceReceipt issued at pack promotion
