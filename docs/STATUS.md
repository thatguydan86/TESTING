# Zoopla Hardening — Status

State: SETUP

Added: search and listing fixtures (4 total) and created tests for JSON-LD parsing, search extraction, and basic integration.

Next: Implement the main Zoopla scraper in `scrapers/zoopla/main.py` with Playwright headless scraping, JSON-LD parsing and DOM fallbacks, quotas, retries, and dedupe/schema validation. Update Dockerfile and CI to run tests and deploy. Ensure tests pass and prepare for integration run.
