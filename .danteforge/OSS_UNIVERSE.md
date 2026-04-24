# DANTEHARVEST OSS Universe

Generated: 2026-04-22

## Summary

- PRD OSS entries accounted for: `23`
- Local repos materialized in `.danteforge/oss-repos/`: `22`
- `allowed`: `17`
- `blocked`: `3`
- `review_required`: `2`
- `paper_only`: `1`

This universe is grounded in the PRD donor matrix in [Docs/DanteHarvestDeepresearchPRD.md](/c:/Projects/DanteHarvest/Docs/DanteHarvestDeepresearchPRD.md:133), not in a generic market scan. The canonical machine-readable sources are:

- [.danteforge/oss-registry.json](/c:/Projects/DanteHarvest/.danteforge/oss-registry.json)
- [.danteforge/oss-structural-scan.json](/c:/Projects/DanteHarvest/.danteforge/oss-structural-scan.json)

## Coverage

| PRD Project | Local Status | License | Notes |
|---|---|---|---|
| Browser Use | allowed | MIT | Cloned and scanned |
| Skyvern | blocked | AGPL | Cloned for audit only |
| Stagehand | allowed | MIT | Cloned and scanned |
| Playwright | allowed | MIT | Cloned and scanned |
| Firecrawl | allowed | MIT | Cloned and scanned; PRD license assumption appears stale |
| Crawl4AI | allowed | Apache-2.0 | Cloned and scanned |
| Crawlee | allowed | Apache-2.0 | Cloned and scanned |
| Scrapling | allowed | BSD | Cloned and scanned |
| Jina Reader | allowed | Apache-2.0 | Cloned and scanned |
| Jina MCP | allowed | Apache-2.0 | Cloned and scanned |
| MarkItDown | allowed | MIT | Cloned and scanned |
| Paperless-ngx | blocked | AGPL | Cloned for audit only; PRD called out GPL-family restrictions and this repo is blocked in practice |
| OpenAdapt | allowed | MIT | Cloned and scanned |
| OpenAdapt Desktop | allowed | MIT | Cloned and scanned |
| UI-TARS / UI-TARS Desktop | allowed | Apache-2.0 | Canonical UI-TARS repo used for desktop/operator patterns |
| OpenCUA | allowed | MIT | Cloned and scanned |
| Screenpipe | review_required | UNKNOWN | Cloned and scanned; license classification needs manual confirmation |
| OpenRecall | blocked | AGPL | Cloned for audit only |
| Clicky | allowed | MIT | Cloned and scanned |
| Open Deep Research | allowed | MIT | Mapped to `langchain-ai/open_deep_research` |
| OpenResearcher | review_required | NO_LICENSE_FOUND | Cloned and scanned; no license file detected in this pass |
| trycua/cua | allowed | MIT | Cloned and scanned |
| CUA-Suite | paper_only | N/A | Paper/site-first reference; no canonical GitHub repo located in this pass |

## Structural Signals

These are rough structural fingerprints from the cloned repos. They are useful for triage, not for final scoring.

| Project | Agent Files | Tool Files | CLI Files | Command Files |
|---|---:|---:|---:|---:|
| Browser Use | 32 | 12 | 37 | 9 |
| Skyvern | 24 | 33 | 724 | 21 |
| Stagehand | 119 | 26 | 49 | 1 |
| Playwright | 38 | 177 | 277 | 5 |
| Firecrawl | 29 | 0 | 25 | 0 |
| Crawl4AI | 1 | 0 | 24 | 0 |
| Crawlee | 0 | 12 | 77 | 3 |
| Scrapling | 24 | 8 | 7 | 1 |
| Jina Reader | 0 | 0 | 1 | 0 |
| Jina MCP | 0 | 1 | 0 | 0 |
| MarkItDown | 0 | 1 | 2 | 0 |
| Paperless-ngx | 0 | 0 | 2 | 21 |
| OpenAdapt | 0 | 0 | 3 | 0 |
| OpenAdapt Desktop | 0 | 0 | 2 | 1 |
| UI-TARS / UI-TARS Desktop | 0 | 0 | 0 | 0 |
| OpenCUA | 70 | 1 | 0 | 0 |
| Screenpipe | 12 | 5 | 32 | 5 |
| OpenRecall | 0 | 0 | 0 | 0 |
| Clicky | 2 | 2 | 84 | 0 |
| Open Deep Research | 2 | 0 | 0 | 0 |
| OpenResearcher | 2 | 0 | 0 | 0 |
| trycua/cua | 171 | 18 | 167 | 68 |

## Important Corrections To The PRD

- `Firecrawl` is currently classified locally as `MIT`, not AGPL, based on the cloned repo license file. The PRD should be updated if this is the canonical upstream.
- `Paperless-ngx` is still blocked in practice for Harvest embedding, but the current cloned repo classified as `AGPL` in this pass rather than the older GPL-only framing in the PRD.
- `Screenpipe` needs a manual license confirmation because the automated pass did not find a standard license signature quickly enough.
- `OpenResearcher` needs a manual license and maintenance review because no license file was detected in this pass.
- `CUA-Suite` should be treated as a paper/site reference unless a canonical maintained code repo is identified later.

## What This Means

- The Harvest project now has a real local OSS universe instead of a purely narrative one.
- The browser, acquisition, observation, normalization, and research donors named in the PRD are mostly present on disk and available for direct inspection.
- The license gate is now concrete instead of hypothetical: blocked repos are present for audit but can be kept outside `harvest_*` implementation work.

## Next Actions

- Review `Screenpipe`, `OpenResearcher`, and `CUA-Suite` manually to close the remaining classification gaps.
- Re-rank donor value based on actual code quality and relevance, not just the PRD prose.
- Turn the highest-signal donor patterns into a `P0/P1` implementation backlog for Harvest.
