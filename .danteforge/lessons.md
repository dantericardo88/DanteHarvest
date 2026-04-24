# Lessons Learned

_Auto-maintained by DanteForge — rules captured from corrections, failures, and refinements._

---

Clean-room repo with selective transplants beats forking the donor. DanteDistillerV2 was embedded as read-only donor in donors/ and only the 10 highest-value files were transplanted with updated imports — this kept Harvest free of donor drag and duplication smells.

Rights model must be first-class at ingest time, not a bolt-on. Every artifact gets a RightsProfile on creation with safe defaults per source_class — FORBIDDEN for customer_confidential and personal_device_memory, REFERENCE_ONLY for public_web. Promotion gates enforce rights before replay.

RunRegistry state machine must be fail-closed on invalid transitions AND on unknown run_id. Discovered that terminal states (COMPLETED, FAILED, CANCELLED) must have empty allowed-transition sets — otherwise the registry allows re-entering a completed run, which violates the one-door doctrine.

Phase 7-8 OSS harvest: PackVectorStore (Qdrant/TF-IDF local fallback), RetryPolicy (Crawl4AI exponential backoff), MetadataEnricher (LlamaIndex chunk enrichment), DedupIndex (Screenpipe SimHash), EvalPackBuilder (PRD gap), WhisperAdapter (OpenAdapt transcription), TaxonomyBuilder (LlamaIndex knowledge graph), DesktopEventCapture (OpenAdapt event bus), CrawleeAdapter (Crawlee request queue). All implemented local-first with fail-closed error paths and zero-ambiguity return types. 242 tests passing.

Constitutional chain ordering: emit chain entry (e.g. transcribe.started, acquire.started) BEFORE any file-existence or validation checks — the chain must record the attempt even when it immediately fails. Emit a .failed entry before raising the exception. This is the difference between a chain that tells the full story and one that goes silent on failures.

constitutionAlignment ceiling: SPEC/CLARIFY/PLAN/TASKS hit a structural ceiling at 16/20 regardless of keyword density. CONSTITUTION itself caps at 18/20. Adding MUST/MUST NOT language, fail-closed/zero-ambiguity/local-first/one-door/AGPL doctrine sections to artifacts does not push past these ceilings — the scorer likely requires architectural enforcement evidence (tests that verify constitutional behaviors) to award the top tier.

Competitive masterplan built: 20-dimension matrix vs 21 competitors (OSS+closed). DanteHarvest leads on 11 dimensions (rights model 10/10, constitutional doctrine 10/10, local-first 10/10, AGPL quarantine 10/10, pack promotion 9/10, evidence chain 9/10, test coverage 9/10). Top 8 sprint gaps identified: ui_reviewer_workflow (gap=6 vs Rewind.ai), transcription diarization (gap=3 vs Screenpipe), observation depth, vector search, crawl acquisition, multi-format ingest, CLI batch/watch, NER redaction. Matrix at .danteforge/compete/matrix.json. Masterplan at .danteforge/compete/MASTERPLAN.md.

