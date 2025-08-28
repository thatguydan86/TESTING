# Zoopla Scraper

This directory contains the Zoopla-specific scraping logic for RentRadar.

## Overview

The scraper runs headlessly via Playwright to navigate Zoopla search pages,
extract listing detail URLs, fetch detail pages, parse them using JSON‑LD,
validate results against `schemas/listing_v1.json`, deduplicate, and post
complete listings to the configured webhook.

During local development and CI, the scraper uses HTML fixtures stored in
`scrapers/zoopla/fixtures/` to run unit and integration tests without network
access.

## Running locally

```bash
python -m scrapers.zoopla.main
```

This will parse the fixture search and listing pages and print a JSON report.

## Environment variables

- `MAKE_WEBHOOK_URL`: URL to POST complete listings.
- `PROXY_URL`: Optional HTTP proxy; if absent the scraper runs without a proxy.
- `ZP_PAGES_PER_QUERY`: Number of pages per search query (default 3).
- `ZP_DELAY_MS_MIN`: Minimum delay between requests in milliseconds (default 800).
- `ZP_DELAY_MS_MAX`: Maximum delay between requests in milliseconds (default 2200).
- `ZP_MAX_REQUESTS_PER_RUN`: Maximum number of listing pages fetched per run (default 120).

## Deployment

The GitHub workflow in `.github/workflows/zoopla.yml` builds and tests the
scraper on pushes that affect Zoopla code. It builds a Docker image and deploys
it to the `zoopla` service on Railway using the Railway CLI.
