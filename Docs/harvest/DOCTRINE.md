# DANTEHARVEST Doctrine

Promoted from DanteDistillerV2/docs/masterplan_alignment.md — adapted for Harvest.

## Core Principles

### 1. Machine Sovereignty
Every plane in Harvest has a narrow, declared mandate. It accepts defined inputs, emits defined outputs, and never reads or writes outside its plane boundary. Violations are exceptions, not warnings.

### 2. One-Door Doctrine
All artifact writes flow through the Control plane's run contract. Side-channel writes (direct file creation without a chain entry) are forbidden. Every store operation emits a signal.

### 3. Evidence Chain
Every operation that produces or transforms an artifact emits a ChainEntry. The chain is append-only, sequenced, and SHA-256 hashed. Integrity verification must pass before any evidence package is shipped.

### 4. Fail-Closed Defaults
- robots.txt disallowed → raise ConstitutionalError, never proceed
- Missing required artifact → raise PackagingError, never create partial package
- Rights status not approved → reject promotion, never silently skip gate
- Network error fetching robots.txt → treat as DISALLOW

### 5. Rights Before Features
No artifact may be promoted to a reusable pack without an approved RightsProfile and a sealed EvidenceReceipt. Rights gates run before promotion gates. Training eligibility defaults to UNKNOWN until explicitly set by a human reviewer.

### 6. Pack-Centric Scope
Harvest's output is packs. A pack is rights-scoped, replay-tested, evidence-backed, and version-controlled. Packs are the API surface Harvest exposes to DanteAgents, DanteCode, and sovereign training runtimes.

### 7. Local-First
All capture, storage, and processing defaults to local. Cloud sync, remote capture, and delegated processing are optional add-ons. User data never leaves the machine without explicit opt-in.

### 8. Apprenticeship Over Autonomy
Harvest captures human demonstrations before it executes on behalf of humans. Replay is a test harness — not a production agent. Autonomous promotion (skip human review) is only allowed for `synthetic_eval` source class.

## Constitutional Checklist (per ingest run)

- [ ] Rights profile assigned at ingest
- [ ] robots.txt checked before any URL fetch
- [ ] Chain entry emitted at each plane transition
- [ ] All required artifacts present before packaging
- [ ] Manifest hash verified before evidence receipt issued
- [ ] EvidenceReceipt sealed and stored before pack promotion
- [ ] Human reviewer signoff recorded for external/customer content
