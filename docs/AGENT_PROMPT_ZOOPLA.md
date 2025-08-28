# Agent Prompt for Zoopla

This document captures the autonomous agent prompt for the Zoopla scraper mission. The agent operates in **DO-NOT-ASK** and **PERSISTENCE** mode and must work fully autonomously, only asking the user for credentials or access when unavoidable. The mission is to harden and deploy the Zoopla scraper under `scrapers/zoopla/` such that it reliably extracts UK rental listings (price, beds, address, postcode, URL) and posts validated, deduplicated records to the configured webhook.

Key requirements:

- Use Playwright headless to navigate Zoopla search pages and parse detail pages via JSON-LD with DOM fallbacks.
- Implement quotas, retries and backoff (403/429) with random delays between 800–2200ms and rotate user-agent.
- Validate listings against `schemas/listing_v1.json` and dedupe using `shared/dedupe/listings.py`.
- Post only complete records to `MAKE_WEBHOOK_URL`; buffer to `./out/zoopla.jsonl` if webhook is missing.
- Produce structured logs and a final summary line like `ZP_RUN_COMPLETE ✅ listings=<n> complete=<n> failed=<n> avg_ms=<n>`.
- CI/CD pipeline (`.github/workflows/zoopla.yml`) runs tests, builds Docker, and deploys to the "zoopla" Railway service.
- Maintain docs in `docs/STATUS.md`, `docs/TODO.md`, and `docs/DECISIONS.md` to track progress.

The agent must iterate until tests pass, quality thresholds are met (≥95% complete listings and ≤1% hard failures), and the service is deployed. It must persist progress in the repository between sessions.
