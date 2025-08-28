# Zoopla Hardening — Status

State: PROXY/NAV STABILIZATION

Added: robust proxy parsing (parse_proxy) with credential and direct fallback; stable Playwright navigation via goto_stable; fallback to browser context requests to avoid 403s; mobile UA fallback for m.zoopla; random delays and quotas; metrics tracking with final summary line; proxy parser unit tests; tools/log_watch.py for CI/Railway log scanning; CI updated to run log_watch after deploy. Search and listing fixtures and unit/integration tests remain.

Next: complete quota/backoff logic on 403/429 with proxy retry and direct fallback; refine mobile fallback and network idle detection; integrate dedupe and schema validation into the Playwright pipeline; improve structured logging metrics; run CI loop with log_watch until no blocking errors and parse-complete ≥95% and hard-failures ≤1%. Update docs and TODO accordingly.
