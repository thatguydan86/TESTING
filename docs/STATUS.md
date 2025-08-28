# Zoopla Hardening — Status

State: SCRAPER WIP

Added: search and listing fixtures (4 total), created JSON‑LD, search extraction, and integration tests, and implemented a Playwright scraper skeleton in `scrapers/zoopla/main.py` with JSON‑LD parsing and local fixture fallback.

Next: Implement quotas, retries, random delays, and dedupe/schema validation in the scraper. Integrate proxy handling, backoff logic, and webhook posting. Update tests and Dockerfile as needed, and run CI to validate. Once tests pass, perform live integration runs and tune until quality thresholds are met.
