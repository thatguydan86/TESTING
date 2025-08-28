"""
Zoopla scraping module.
This script is intended to be run as the entrypoint for the Zoopla service.
It supports headless Playwright scraping as well as fallback using local fixtures for tests.

The main responsibilities are:
 - Load search pages via Playwright, extract listing URLs.
 - Load detail pages and parse listing info from JSON‑LD or DOM.
 - Apply quotas, delays and retries.
 - Deduplicate and validate listings.
 - Post complete listings to the MAKE_WEBHOOK_URL.

During development, we support a simplified codepath that reads HTML from local files
in scrapers/zoopla/fixtures/ for unit tests and integration tests without network access.
"""
from __future__ import annotations
import asyncio
import json
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

from bs4 import BeautifulSoup

from shared.dedupe.listings import dedupe_listings

# Optional import of Playwright. It's only used at runtime, not during unit tests. If Playwright
# isn't installed (e.g. during initial scaffolding or in environments without network
# access), the import will fail gracefully and the Playwright code paths won't run.
try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext
except Exception:
    async_playwright = None  # type: ignore

###############################################################################
# Helpers for schema validation and normalization
###############################################################################

def validate_listing(listing: Dict) -> bool:
    """
    Validate a parsed listing dict against the listing_v1 schema requirements.
    Only complete listings (price, beds, address, postcode, url) are considered valid.
    """
    required_keys = ["rent_pcm", "beds", "address", "postcode", "url"]
    for key in required_keys:
        if not listing.get(key):
            return False
    return True

def enrich_listing(listing: Dict) -> Dict:
    """
    Populate constant fields such as source, images, agent_name, raw_id, and scraped_at.
    Returns a new dict so the original isn't mutated.
    """
    enriched = listing.copy()
    enriched.update({
        "source": "zoopla",
        "images": listing.get("images", []),
        "agent_name": listing.get("agent_name", ""),
        "raw_id": listing.get("raw_id", ""),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    })
    return enriched

###############################################################################
# Playwright scraping functions
###############################################################################

async def _load_page_with_resource_blocking(page: Page, url: str, *, wait_until: str = "domcontentloaded", timeout_ms: int = 200_000) -> None:
    """
    Navigate to a URL while blocking heavy resources (images, fonts, CSS). Wait until
    the specified load state. This helper is shared between search and detail pages.
    """
    # Abort requests for heavy assets to speed up scraping
    await page.route(
        r"**/*.{png,jpg,jpeg,svg,ico,woff,woff2,ttf,css}", lambda route: asyncio.create_task(route.abort())
    )
    await page.goto(url, wait_until=wait_until, timeout=timeout_ms)

async def _extract_detail_with_playwright(context: BrowserContext, url: str) -> Optional[Dict]:
    """
    Use a fresh page in the provided context to load a listing detail page and parse
    its JSON‑LD. Returns None if parsing fails or page load errors occur.
    """
    page = await context.new_page()
    try:
        await _load_page_with_resource_blocking(page, url, wait_until="domcontentloaded")
        html = await page.content()
        parsed = parse_listing_jsonld(html)
        return parsed
    except Exception:
        # ignore errors and return None to allow retries/hard-fallback later
        return None
    finally:
        await page.close()

async def _extract_search_results_with_playwright(context: BrowserContext, search_url: str) -> List[str]:
    """
    Load a Zoopla search result page via Playwright and extract listing URLs using the
    extract_search_result_urls function. Returns a list of absolute URLs.
    """
    page = await context.new_page()
    try:
        # Wait until network idle so all search results have been loaded
        await _load_page_with_resource_blocking(page, search_url, wait_until="networkidle")
        html = await page.content()
        urls = extract_search_result_urls(html)
        return urls
    except Exception:
        return []
    finally:
        await page.close()

async def run_playwright_scraper(queries: List[str]) -> List[Dict]:
    """
    Main asynchronous scraper entrypoint. Accepts a list of search queries (either
    partial postcodes like "L4" or full Zoopla search URLs). Returns a list of
    enriched, deduped, and validated listing dicts. Applies quotas, random delays,
    and soft retries.
    """
    results: List[Dict] = []
    if async_playwright is None:
        print("Playwright is not available; skipping live scraping.")
        return results
    async with async_playwright() as pw:
        # Launch Chromium headless with minimal flags and optional proxy
        # Build args list; rely on environment to specify headless; disable heavy features
        launch_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-background-networking",
            "--disable-renderer-backgrounding",
            "--disable-background-timer-throttling",
            "--mute-audio",
            "--disable-extensions",
            "--hide-scrollbars",
            "--disable-blink-features=AutomationControlled",
        ]
        proxy_cfg: Optional[Dict[str, str]] = None
        if PROXY_URL:
            # parse PROXY_URL of form http://user:pass@host:port into Playwright proxy dict
            from urllib.parse import urlsplit
            parsed = urlsplit(PROXY_URL)
            server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 80}"
            proxy_cfg = {"server": server}
            if parsed.username and parsed.password:
                proxy_cfg["username"] = parsed.username
                proxy_cfg["password"] = parsed.password
        launch_kwargs: Dict = {
            "headless": True,
            "args": launch_args,
        }
        if proxy_cfg:
            launch_kwargs["proxy"] = proxy_cfg
        # Use system-installed Chromium if available
        system_chrome = os.environ.get("CHROMIUM_PATH") or "/root/.nix-profile/bin/chromium"
        if os.path.exists(system_chrome):
            launch_kwargs["executable_path"] = system_chrome
        browser: Browser = await pw.chromium.launch(**launch_kwargs)
        try:
            # iterate through each query and scrape up to PAGES_PER_QUERY pages
            request_count = 0
            for q in queries:
                for page_num in range(1, PAGES_PER_QUERY + 1):
                    if request_count >= MAX_REQUESTS_PER_RUN:
                        break
                    # Build search URL: if the query is already a URL, use it directly; otherwise append to default Zoopla search pattern
                    if q.startswith("http"):
                        search_url = q
                    else:
                        # Construct search URL with filters and page
                        # Example path: https://www.zoopla.co.uk/to-rent/property/{postcode}/?page_size=25&page={n}
                        search_url = f"https://www.zoopla.co.uk/to-rent/property/{q}/?page_size=25&page={page_num}"
                    context_kwargs = {
                        "user_agent": _random_ua(),
                    }
                    # Propagate proxy to context if defined
                    if proxy_cfg:
                        context_kwargs["proxy"] = proxy_cfg
                    context = await browser.new_context(**context_kwargs)
                    try:
                        urls = await _extract_search_results_with_playwright(context, search_url)
                        # enforce per-run request cap
                        for url in urls:
                            if request_count >= MAX_REQUESTS_PER_RUN:
                                break
                            await asyncio.sleep(random.uniform(DELAY_MS_MIN/1000.0, DELAY_MS_MAX/1000.0))
                            listing_data = await _extract_detail_with_playwright(context, url)
                            request_count += 1
                            if not listing_data:
                                continue
                            enriched = enrich_listing(listing_data)
                            if validate_listing(enriched):
                                results.append(enriched)
                    finally:
                        await context.close()
                    if request_count >= MAX_REQUESTS_PER_RUN:
                        break
            # Deduplicate results at end
            results = dedupe_listings(results)
            return results
        finally:
            await browser.close()

# ---------------------------------------------------------------------------
# Configuration via environment variables (safe defaults)
# ---------------------------------------------------------------------------
MAKE_WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL")
PROXY_URL = os.getenv("PROXY_URL")
PAGES_PER_QUERY = int(os.getenv("ZP_PAGES_PER_QUERY", "3"))
DELAY_MS_MIN = int(os.getenv("ZP_DELAY_MS_MIN", "800"))
DELAY_MS_MAX = int(os.getenv("ZP_DELAY_MS_MAX", "2200"))
MAX_REQUESTS_PER_RUN = int(os.getenv("ZP_MAX_REQUESTS_PER_RUN", "120"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
]

# Regular expression to extract UK postcodes (simplified).
POSTCODE_RE = re.compile(r"[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}", re.IGNORECASE)

def _random_ua() -> str:
    return random.choice(USER_AGENTS)

# ---------------------------------------------------------------------------
# JSON‑LD Parsing
# ---------------------------------------------------------------------------
def parse_listing_jsonld(html: str) -> Optional[Dict]:
    """
    Parse a listing page HTML and extract listing data from JSON‑LD.
    Returns a dictionary with keys: rent_pcm, beds, address, postcode, url.
    """
    soup = BeautifulSoup(html, "lxml")
    script_tags = soup.find_all("script", {"type": "application/ld+json"})
    for script in script_tags:
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        # Zoopla uses "@type": "Offer" or "Residence"
        offer = data.get("offers") or data.get("Offer") or data.get("offers", {})
        if not offer:
            continue
        try:
            price = offer.get("price") or offer.get("Price")
            if isinstance(price, str):
                price = int(re.sub(r"[^0-9]", "", price))
            address_info = data.get("address", {}) or {}
            street = address_info.get("streetAddress") or ""
            postcode = address_info.get("postalCode") or ""
            beds = data.get("numberOfRooms") or data.get("numberOfBedrooms") or data.get("bedrooms") or 0
            if isinstance(beds, str):
                beds = int(re.sub(r"[^0-9]", "", beds))
            url = data.get("@id") or data.get("url") or ""
            # Validate postcode using regex; fallback to blank
            m = POSTCODE_RE.search(postcode)
            pc = m.group(0).upper().replace(" ", "") if m else ""
            return {
                "rent_pcm": int(price) if price else 0,
                "beds": int(beds) if beds else 0,
                "address": street.strip(),
                "postcode": pc,
                "url": url.strip(),
            }
        except Exception:
            continue
    return None

# ---------------------------------------------------------------------------
# Search result extraction
# ---------------------------------------------------------------------------
def extract_search_result_urls(html: str) -> List[str]:
    """
    Extract listing detail URLs from a Zoopla search result page HTML.
    Looks for article elements with data-testid="search-result".
    """
    soup = BeautifulSoup(html, "lxml")
    urls: List[str] = []
    for article in soup.find_all("article", {"data-testid": "search-result"}):
        # anchor inside article linking to detail page
        a = article.find("a", href=True)
        if a and a["href"]:
            href = a["href"]
            if href.startswith("http"):
                urls.append(href)
            else:
                # relative path -> prefix with domain
                urls.append(f"https://www.zoopla.co.uk{href}")
    return urls

# ---------------------------------------------------------------------------
# Integration harness
# ---------------------------------------------------------------------------
async def scrape_from_fixtures(search_pages: List[str], listing_pages: Dict[str, str]) -> List[Dict]:
    """
    Integration helper used in tests to scrape from provided HTML strings instead of network.
    - search_pages: list of HTML strings representing paginated search results
    - listing_pages: mapping of listing URL to HTML content
    Returns list of parsed listing dicts.
    """
    all_urls: List[str] = []
    for html in search_pages[:PAGES_PER_QUERY]:
        urls = extract_search_result_urls(html)
        all_urls.extend(urls)
        if len(all_urls) >= MAX_REQUESTS_PER_RUN:
            break
        await asyncio.sleep(random.uniform(DELAY_MS_MIN/1000.0, DELAY_MS_MAX/1000.0))
    results: List[Dict] = []
    for url in all_urls:
        html = listing_pages.get(url)
        if not html:
            # skip unknown
            continue
        parsed = parse_listing_jsonld(html)
        if not parsed:
            continue
        parsed.update({
            "source": "zoopla",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "images": [],
            "agent_name": "",
            "raw_id": ""
        })
        results.append(parsed)
    results = dedupe_listings(results)
    return results

if __name__ == "__main__":
    # If run directly, attempt to read fixtures from environment (for local testing)
    # and print parsed listings. In production this will be replaced by Playwright logic.
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    search_files = [f for f in os.listdir(fixtures_dir) if f.startswith("search-page")]
    search_pages = []
    for fname in sorted(search_files):
        with open(os.path.join(fixtures_dir, fname), "r", encoding="utf-8") as f:
            search_pages.append(f.read())
    # Mapping for listing pages
    listing_files = [f for f in os.listdir(fixtures_dir) if f.startswith("listing-")]
    listing_pages = {}
    for fname in listing_files:
        with open(os.path.join(fixtures_dir, fname), "r", encoding="utf-8") as f:
            # use file name as url key placeholder
            listing_pages[f"https://www.zoopla.co.uk/{fname}"] = f.read()
    listings = asyncio.run(scrape_from_fixtures(search_pages, listing_pages))
    print(json.dumps({"count": len(listings), "listings": listings}, indent=2))
