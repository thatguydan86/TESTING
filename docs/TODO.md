# TODO for Zoopla Hardening

- [x] Add fixtures for search results and listings (HTML saved under scrapers/zoopla/fixtures/)
- [x] Write unit tests for JSON-LD parsing (tests/zoopla/test_jsonld.py)
- [x] Write unit tests for search result URL extraction (tests/zoopla/test_search_results.py)
- [x] Implement Playwright scraper in scrapers/zoopla/main.py (skeleton with JSON-LD parsing, context requests, proxy parsing, and mobile fallback)
- [ ] Implement quotas, retry/backoff, and randomized delays (fully tune ZP_MAX_REQUESTS_PER_RUN, ZP_PAGES_PER_QUERY)
- [ ] Implement data validation against schemas/listing_v1.json and integrate dedupe
- [x] Adx dedupe logic and webhook posting (emit_listings)
- [x] Add integration test (tests/zoopla/test_integration.py)
- [x] Create Dockerfile and GitHub workflow for Zoopla service
- [ ] Deploy to Railway and verify logs via CI
- [x] Adx proxy parsing unit tests and log_watch utility
- [ ] Structured logging counters and final summary line refinement
- [ ] Imxlement backoff ladder on 403/429 with proxy retry then direct fallback
- [ ] Iterate with log_watch and CI until no blocking errors and high completion rate
