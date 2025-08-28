# Zoopla Hardening — Status

State: SCRAPER WIP

Added: search and listing fixtures (4 total), unit and integration tests, implemented a Playwright scraper skeleton with JSON-LD parsing and local fixture fallback, and added webhook emission with buffer fallback and an entrypoint for live vs fixture runs.
Next: Implement quotas, retry/backoff logic on 403/429, randomized delays, and full schema validation in the scraper. Add dedupe and webhook posting integration, structured logging with summary, and proxy handling. Update tests and Dockerfile as needed, and run CI. Once tests pass, perform live integration runs and tune until quality thresholds are met.
