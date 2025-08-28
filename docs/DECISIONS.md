# Decisions for Zoopla Hardening

- Use Playwright headless with image/font/css blocking for efficient scraping.
- Primary extraction from JSON-LD scripts; fallback to DOM selectors if JSON-LD absent.
- Rotate user-agents and randomize delays (800–2200 ms) between requests.
- Proxy format: Accept `http://user:pass@host:port` or `http://host:port`. Invalid or missing proxies result in DIRECT mode.
- Fallback ladder on failures: try proxy; retry proxy once; then switch to direct mode with mobile UA (`m.zoopla.co.uk`).
- Use `goto_stable()` to navigate pages: wait for DOMContentLoaded then wait for search result articles; fallback to network idle.
- Replace raw HTTP fallback with Playwright `context.request` to avoid 403s.
- Concurrency=1, limit pages per query to `ZP_PAGES_PER_QUERY` and max requests to `ZP_MAX_REQUESTS_PER_RUN`.
- Validate parsed listings against `schemas/listing_v1.json` and dedupe using `shared/dedupe` on normalized address+postcode and canonical url.
- Buffer incomplete or failed listings to `out/zoopla.jsonl`; send complete listings to `MAKE_WEBHOOK_URL` if provided.
- Track metrics (attempts, proxy mode counts, goto errors, 403 count, listings found and complete, avg_ms per listing) and print a final summary line.
- Use `tools/log_watch.py` to scan CI and Railway logs for errors; CI runs log_watch after deploy and fails the job on blocking errors.

- Ensure final summary line 'ZP_RUN_COMPLETE ✅ listings=<n> complete=<n> failed=<n> avg_ms=<n>' is printed for all code paths; moved summary print before return in fallback.
