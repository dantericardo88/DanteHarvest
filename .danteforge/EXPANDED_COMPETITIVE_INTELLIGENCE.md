# Expanded Competitive Intelligence

Date: 2026-04-22

This memo supplements `.danteforge/compete/matrix.json`.

## Why the current matrix overstates Harvest

The current matrix strongly rewards DanteHarvest's real moat areas:

- provenance / evidence chain
- rights modeling
- promotion governance
- constitutional doctrine
- local-first posture
- AGPL quarantine

Those are valuable, but they are not the full market. Many leading web-data tools are optimized for different jobs:

- anti-bot reliability
- managed browser infrastructure
- proxy depth and geotargeting
- no-code extraction UX
- scheduling / monitoring
- prebuilt datasets and actor ecosystems
- structured extraction endpoints
- session replay and debugging
- high-concurrency operations

Because those dimensions are underrepresented, the matrix can make market leaders look artificially weak.

## Additional tools and what they do best

### Scrapy

- Best at: programmable crawl control, spider architecture, middleware/pipelines, feed export, sitemap-driven crawling.
- Why it matters: it is still one of the strongest "build your own crawler" frameworks.
- Potential dimension: `crawler_framework_maturity`

### Octoparse

- Best at: no-code visual extraction and template-driven business-user scraping.
- Why it matters: it lowers the skill floor dramatically.
- Potential dimension: `no_code_extraction_ux`

### Browse AI

- Best at: monitored robots, scheduled extraction, alerting, and broad integration automation for non-technical teams.
- Why it matters: it turns scraping into recurring business workflows.
- Potential dimension: `monitoring_and_alerting`

### Webscraper.io

- Best at: sitemap-based browser-extension scraping and accessible selector-driven workflows.
- Why it matters: it is a strong entry point for semi-technical users who want control without coding from scratch.
- Potential dimension: `selector_workflow_usability`

### ScrapingBee

- Best at: simple API-first scraping with JS rendering and proxy rotation abstracted away.
- Why it matters: it compresses a lot of scraping pain into one endpoint.
- Potential dimension: `scraping_api_simplicity`

### Bright Data

- Best at: enterprise proxy network, website unlocking, managed browser scraping, and ready-made datasets.
- Why it matters: this is one of the strongest "I need difficult data at scale" platforms.
- Potential dimensions:
  - `anti_bot_bypass`
  - `proxy_network_depth`
  - `dataset_marketplace`

### ScraperAPI

- Best at: plug-and-play scraping API, JS rendering, sticky sessions, and structured endpoints for common domains.
- Why it matters: it is very strong for developers who want speed to production over owning infrastructure.
- Potential dimensions:
  - `scraping_api_simplicity`
  - `structured_extraction_endpoints`

### Apify

- Best at: actor ecosystem, reusable scraping apps, scheduling, datasets, storage abstractions, and platformization.
- Why it matters: it is less "one scraper" and more "operating system for web automation jobs."
- Potential dimensions:
  - `platform_ecosystem`
  - `job_scheduling_and_storage`
  - `marketplace_depth`

### Browserless

- Best at: hosted browser infrastructure, remote Playwright/Puppeteer execution, session replay, screen recording, and debugging.
- Why it matters: it solves browser ops rather than scraping logic.
- Potential dimensions:
  - `browser_infrastructure`
  - `session_replay_debuggability`
  - `remote_browser_scalability`

### Playwright

- Best at: reliable browser automation primitives, tracing, recording, actionability checks, and test-grade control.
- Why it matters: this is the strongest foundational browser control layer in the field.
- Potential dimensions:
  - `browser_automation_primitives`
  - `trace_and_debug_tooling`

### Hyperbrowser

- Best at: cloud browser infrastructure for agents, reusable profiles, live session viewing, markdown extraction, and recordings.
- Why it matters: it treats the browser as stateful infrastructure for agents rather than just a test runner.
- Potential dimensions:
  - `agent_browser_infrastructure`
  - `session_state_reuse`
  - `live_browser_observability`

## Existing matrix competitors and what they do best

### Firecrawl

- Best at: JS-heavy web acquisition, crawl-to-markdown/document extraction, and scraping ergonomics for AI pipelines.
- Potential dimension: `llm_ready_web_extraction`

### Screenpipe

- Best at: continuous screen/audio capture, OCR, and searchable personal observation history.
- Potential dimension: `continuous_capture`

### OpenAdapt

- Best at: demonstrating record/replay style desktop and browser automation workflows.
- Potential dimension: `human_demo_to_automation`

### LlamaIndex

- Best at: connector breadth, retrieval architecture, indexing patterns, and RAG ecosystem maturity.
- Potential dimensions:
  - `connector_breadth`
  - `retrieval_stack_maturity`

### LangChain

- Best at: orchestration breadth and ecosystem familiarity.
- Potential dimension: `orchestration_ecosystem`

### Haystack

- Best at: eval and retrieval pipeline composition with strong search/NLP components.
- Potential dimension: `retrieval_eval_maturity`

### Crawl4AI

- Best at: approachable open-source AI-oriented crawling/extraction.
- Potential dimension: `ai_crawl_extraction`

### Browser Use

- Best at: agent-facing browser task abstractions.
- Potential dimension: `agent_browser_abstraction`

### Stagehand

- Best at: higher-level browser automation for AI agents on top of browser primitives.
- Potential dimension: `agent_browser_abstraction`

### Skyvern

- Best at: workflow automation against websites with a more managed agentic posture.
- Potential dimension: `web_task_automation_productization`

### OpenRecall

- Best at: local recall-style memory and observation.
- Potential dimension: `personal_memory_ux`

### Paperless-ngx

- Best at: document management UX, tagging, OCR workflows, and reviewer-friendly browser UI.
- Potential dimensions:
  - `document_workflow_ux`
  - `review_operations_ui`

### AutoGen / CrewAI

- Best at: multi-agent orchestration patterns.
- Potential dimension: `multi_agent_runtime`

### Instructor

- Best at: structured output ergonomics.
- Potential dimension: `structured_generation_ergonomics`

### Aider / Cursor

- Best at: developer UX around AI-assisted code execution and iteration loops.
- Potential dimension: `operator_feedback_loop`

### Microsoft Recall / Rewind.ai / Mem.ai / Notion AI

- Best at: polished user-facing memory/search experiences and workflow UX.
- Potential dimensions:
  - `memory_product_ux`
  - `consumer_search_experience`

## Missing dimensions we should add

These are the major missing categories that explain why leading tools can look artificially weak in the current matrix:

1. `anti_bot_bypass`
2. `proxy_network_depth`
3. `browser_infrastructure`
4. `remote_browser_scalability`
5. `trace_and_debug_tooling`
6. `session_replay_debuggability`
7. `platform_ecosystem`
8. `marketplace_depth`
9. `job_scheduling_and_storage`
10. `structured_extraction_endpoints`
11. `no_code_extraction_ux`
12. `monitoring_and_alerting`
13. `dataset_marketplace`
14. `connector_breadth`
15. `crawler_framework_maturity`
16. `agent_browser_infrastructure`
17. `session_state_reuse`
18. `live_browser_observability`
19. `operator_feedback_loop`
20. `memory_product_ux`

## Highest-value dimensions for DanteHarvest to pursue

If DanteHarvest wants to become a genuinely category-leading platform rather than just a trust-heavy architecture, the next dimensions with the most leverage are:

1. `browser_infrastructure`
2. `session_replay_debuggability`
3. `anti_bot_bypass`
4. `structured_extraction_endpoints`
5. `monitoring_and_alerting`
6. `platform_ecosystem`
7. `job_scheduling_and_storage`
8. `no_code_extraction_ux`
9. `connector_breadth`
10. `agent_browser_infrastructure`

## Strategic interpretation

DanteHarvest is strongest where evidence, rights, and governance matter.

DanteHarvest is not yet strong enough in the categories where commercial scraping and browser-infrastructure companies actually win:

- getting blocked less
- scaling browser sessions
- making difficult web extraction operationally easy
- shipping a polished monitoring/reviewer/operator experience
- offering reusable hosted primitives and marketplaces

That means the current matrix is directionally useful, but incomplete. The right conclusion is not "Harvest is almost finished." The right conclusion is "Harvest is advanced in one important slice of the market and underbuilt in several others that deserve first-class dimensions."
