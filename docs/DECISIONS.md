# Decisions for Zoopla Hardening

- Use Playwright headless with image/font/css blocking for efficient scraping.
- Extract listing details primarily from JSON-LD scripts; fallback to DOM when absent.
- Implement random User-Agent rotation and random delays between requests (800–2200 ms).
- Implement backoff strategy: soft retry with same proxy on 403/429; hard retry with new proxy after repeated failures.
- Use concurrency of 1, limit pages per query to 3 and max requests per run to 120.
- Store fixtures for tests under scrapers/zoopla/fixtures and use them for unit tests.
- Validate parsed listings against schemas/listing_v1.json; require rent_pcm, beds, address, postcode, url.
- Use shared dedupe logic via shared/dedupe/listings.py to avoid duplicates.
- Buffer incomplete or failed listings to logs/out/zoopla.jsonl but do not send to webhook.
- Deploy as a separate Railway service with its own Dockerfile and CI workflow.
