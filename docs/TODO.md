# TODO for Zoopla Hardening

- [x] Add fixtures for search results and listings (HTML saved under scrapers/zoopla/fixtures/)
- [x] Write unit tests for JSON-LD parsing (tests/zoopla/test_jsonld.py)
- [x] Write unit tests for search result URL extraction (tests/zoopla/test_search_results.py)
- [ ] Implement Playwright scraper in scrapers/zoopla/main.py
- [ ] Implement quotas, retry, and random delays
- [ ] Implement data validation against schemas/listing_v1.json
- [ ] Add dedupe logic and webhook posting
- [x] Add integration test (tests/zoopla/test_integration.py)
- [x] Create Dockerfile and GitHub workflow for Zoopla service
- [ ] Deploy to Railway and verify logs
