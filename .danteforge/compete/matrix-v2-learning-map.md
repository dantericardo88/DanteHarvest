# Matrix V2 Learning Map

Date: 2026-04-22

This matrix reframes the competitive model around two questions:

1. Who is the current category leader?
2. Which OSS project is the best teacher for DanteHarvest if we want to close the gap?

Scoring source of truth remains:

- `.danteforge/compete/matrix-v2.json`

## Full Matrix

| Dimension | DH | Overall Leader | Overall Score | OSS Leader | OSS Score | Best OSS To Learn From | What They Do Best |
|---|---:|---|---:|---|---:|---|---|
| Evidence Chain Robustness | 9 | DanteHarvest | 9 | Screenpipe | 4 | Screenpipe | local event persistence patterns |
| Rights Model Completeness | 9 | DanteHarvest | 9 | Paperless-ngx | 3 | Paperless-ngx | practical document metadata and retention tagging |
| Pack Promotion Pipeline | 9 | DanteHarvest | 9 | OpenAdapt | 2 | OpenAdapt | action review flows and automation checkpoints |
| Vector Search Integration | 6 | LlamaIndex | 9 | LlamaIndex | 9 | LlamaIndex | mature embedding, index, retrieval, and reranking stack |
| Replay Harness Fidelity | 7 | Playwright | 9 | Playwright | 9 | Playwright | reliable browser execution, assertions, traces |
| Redaction Accuracy | 6 | Haystack | 8 | Haystack | 8 | Haystack | production-grade PII / anonymization pipeline patterns |
| CLI Completeness | 7 | Aider | 9 | Aider | 9 | Aider | fast operator loop, polish, streaming CLI ergonomics |
| Observation Plane Depth | 6 | Microsoft Recall | 10 | Screenpipe | 9 | Screenpipe | continuous capture, OCR, searchable history |
| Taxonomy Builder | 6 | LlamaIndex | 7 | LlamaIndex | 7 | LlamaIndex | graph enrichment and knowledge extraction |
| Crawl Acquisition | 6 | Firecrawl | 9 | Firecrawl | 9 | Firecrawl | JS-heavy extraction and AI-ready web ingestion |
| Transcription Quality | 7 | Screenpipe | 9 | Screenpipe | 9 | Screenpipe | continuous Whisper-based transcription in real workflows |
| Constitutional Doctrine Enforcement | 10 | DanteHarvest | 10 | Screenpipe | 3 | Screenpipe | local-first guardrail patterns, though DH is already unique here |
| Test Coverage | 7 | LlamaIndex | 8 | LlamaIndex | 8 | LlamaIndex | broad provider/integration test discipline |
| DanteAgents Integration | 8 | DanteHarvest | 8 | AutoGen | 4 | AutoGen | agent memory/runtime interoperability ideas |
| Local-First Guarantee | 10 | DanteHarvest | 10 | Screenpipe | 8 | Screenpipe | productized local-first user experience |
| AGPL Quarantine | 10 | DanteHarvest | 10 | Instructor | 5 | Instructor | narrow dependency posture and simple library boundaries |
| Multi-Format Ingest | 7 | LlamaIndex | 9 | LlamaIndex | 9 | LlamaIndex | connector and loader breadth |
| UI / Reviewer Workflow | 5 | Rewind.ai | 9 | Paperless-ngx | 8 | Paperless-ngx | practical reviewer/document workflow UI patterns |
| Specialization Pack Quality | 7 | DanteHarvest | 7 | LlamaIndex | 5 | LlamaIndex | specialization and configurable retrieval composition |
| Eval Harness | 7 | Haystack | 8 | Haystack | 8 | Haystack | evaluation pipelines, metrics, and judge-style assessment |
| Anti-Bot Bypass | 1 | Bright Data | 10 | Firecrawl | 6 | Firecrawl | approachable OSS patterns for hard-site extraction without full proxy empire |
| Proxy Network Depth | 1 | Bright Data | 10 | Apify | 7 | Apify | practical proxy abstraction and platform routing patterns |
| Browser Infrastructure | 2 | Browserless | 9 | Browserless | 9 | Browserless | remote browser operations, hosted Playwright/Puppeteer |
| Remote Browser Scalability | 2 | Hyperbrowser | 9 | Browserless | 8 | Browserless | scalable remote sessions and browser fleet handling |
| Trace and Debug Tooling | 4 | Playwright | 10 | Playwright | 10 | Playwright | trace viewer, recorder, deterministic browser debugging |
| Session Replay Debuggability | 3 | Browserless | 9 | Browserless | 9 | Browserless | rrweb-style replay plus browser operational debugging |
| Platform Ecosystem | 2 | Apify | 10 | Apify | 10 | Apify | actors, marketplace, storage, scheduler, APIs |
| Job Scheduling and Storage | 3 | Apify | 9 | Apify | 9 | Apify | productionized run scheduling, datasets, key-value stores |
| Structured Extraction Endpoints | 2 | ScraperAPI | 8 | Firecrawl | 7 | Firecrawl | higher-level extraction interfaces rather than raw crawl APIs |
| No-Code Extraction UX | 1 | Octoparse | 9 | Webscraper.io | 7 | Webscraper.io | selector flows and lower-friction scraping UX |
| Dataset Marketplace | 1 | Bright Data | 9 | Apify | 8 | Apify | reusable public/private actor outputs and monetized distribution |
| Connector Breadth | 3 | LlamaIndex | 10 | LlamaIndex | 10 | LlamaIndex | huge ingestion connector surface area |
| Agent Browser Infrastructure | 2 | Hyperbrowser | 9 | Browserless | 8 | Browserless | practical hosted browser substrate for agent execution |
| Monitoring and Alerting | 2 | Browse AI | 9 | Apify | 7 | Apify | schedulers, webhooks, recurring job operations |

## Best OSS Teachers By Theme

### Highest-value OSS teachers

| OSS Project | Why it matters most |
|---|---|
| Playwright | Best single teacher for reliable browser execution, replay, traces, and debug loops |
| Browserless | Best OSS-adjacent teacher for browser infrastructure and replay operations |
| Apify | Best teacher for platformization: jobs, storage, marketplace, and proxies |
| Firecrawl | Best teacher for AI-ready crawl acquisition and higher-level extraction interfaces |
| LlamaIndex | Best teacher for retrieval maturity, connectors, and indexing breadth |
| Screenpipe | Best teacher for continuous capture and local-first observation UX |
| Haystack | Best teacher for eval and redaction-related pipeline rigor |
| Paperless-ngx | Best teacher for reviewer and document workflow UX |
| Aider | Best teacher for CLI/operator experience |
| Webscraper.io | Best lightweight teacher for no-code / low-code extraction usability |

## Where Harvest should study first

### P0 learning donors

| Gap Area | Best OSS teacher | Why this one first |
|---|---|---|
| Browser execution + traces | Playwright | Strongest technical foundation for replay, screenshots, traces, and deterministic automation |
| Browser infrastructure | Browserless | Fastest way to understand hosted session management, replay, and debugging expectations |
| Platform jobs / storage / ecosystem | Apify | Best model for schedulers, datasets, key-value stores, and marketplace thinking |
| Crawl acquisition | Firecrawl | Best modern pattern for AI-ready extraction and JS-heavy crawling |
| Retrieval + connectors | LlamaIndex | Strongest reference for indexing maturity and source breadth |

### P1 learning donors

| Gap Area | Best OSS teacher | Why |
|---|---|---|
| Continuous observation | Screenpipe | Strongest local-first continuous capture reference |
| Eval and anonymization | Haystack | Strong evaluation and processing pipeline patterns |
| Reviewer UI | Paperless-ngx | Strongest OSS reference for review operations UX |
| CLI polish | Aider | Strong operator workflow and tight iteration loop |
| No-code scraping UX | Webscraper.io | Good lower-friction UX reference without enterprise complexity |

## Strategic read

The main lesson from this version of the matrix is:

- DanteHarvest is still the leader in trust/governance dimensions.
- The biggest learning opportunities are concentrated in a handful of OSS projects.
- Closing the next major gaps does not require studying every tool equally.

If we optimize for learning efficiency, the most important OSS teachers are:

1. Playwright
2. Browserless
3. Apify
4. Firecrawl
5. LlamaIndex
6. Screenpipe
7. Haystack
8. Paperless-ngx
