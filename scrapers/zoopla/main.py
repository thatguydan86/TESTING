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
import requests
from urllib.parse import urlsplit

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

def emit_listings(listings: List[Dict]) -> None:
    """
    Emit validated listings either to the webhook configured via MAKE_WEBHOOK_URL
    or write them to a local buffer file if the webhook URL is not provided.

    This function is synchronous and should be called after all asynchronous scraping
    completes. It logs success and failure counts.
    """
    if not listings:
        return
    # Determine output directory relative to project root
    out_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "out")
    os.makedirs(out_dir, exist_ok=True)
    buffer_path = os.path.join(out_dir, "zoopla.jsonl")
    sent = 0
    buffered = 0
    for listing in listings:
        if MAKE_WEBHOOK_URL:
            try:
                resp = requests.post(MAKE_WEBHOOK_URL, json=listing, timeout=10)
                if resp.status_code >= 200 and resp.status_code < 300:
                    sent += 1
                else:
                    # Treat non‑2xx responses as failures and buffer the listing
                    with open(buffer_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(listing) + "\n")
                    buffered += 1
            except Exception:
                # On any request exception, buffer the listing
                with open(buffer_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(listing) + "\n")
                buffered += 1
        else:
            # No webhook configured; always buffer
            with open(buffer_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(listing) + "\n")
            buffered += 1
    print(f"Emitted {sent} listings to webhook, buffered {buffered} listings.")

###############################################################################
# Proxy handling
###############################################################################

def parse_proxy(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
    """
    Parse a proxy URL into a Playwright proxy configuration.

    Accepts the following formats:
    - ``http://user:pass@host:port`` → returns ``{"server": "http://host:port", "username": user, "password": pass}``
    - ``http://host:port`` → returns ``{"server": "http://host:port"}``

    Invalid or missing proxies return ``None``. A proxy is considered invalid if
    it lacks a host/port or otherwise cannot be parsed. This helper never raises.
    """
    if not proxy_url:
        return None
    try:
        # Ensure a scheme prefix so urlsplit parses correctly. Only support http proxies.
        candidate = proxy_url.strip()
        if "://" not in candidate:
            candidate = f"http://{candidate}"
        parsed = urlsplit(candidate)
        # Must have hostname and port; port must be an integer
        if not parsed.hostname or parsed.port is None:
            return None
        # Build server string without credentials
        server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        proxy_cfg: Dict[str, str] = {"server": server}
        # Attach credentials if present
        if parsed.username and parsed.password:
            proxy_cfg["username"] = parsed.username
            proxy_cfg["password"] = parsed.password
        return proxy_cfg
    except Exception:
        # For any parsing error, treat proxy as invalid
        return None

async def launch_browser_with_fallback(pw, proxy_url: Optional[str], use_mobile: bool = False) -> Tuple[Browser, BrowserContext, str]:
    """
    Launch a Chromium browser and context with optional proxy. If launching with the
    proxy fails due to invalid configuration (e.g. ERR_INVALID_ARGUMENT), retry
    without the proxy. Returns a tuple of (browser, context, proxy_mode), where
    proxy_mode is "proxy" or "direct" depending on which succeeded.

    The context is configured with sensible headers: Accept-Language, Referer,
    Sec-Fetch hints, locale and timezone, and blocks heavy resources.
    """
    proxy_cfg = parse_proxy(proxy_url)
    modes = [(proxy_cfg, "proxy"), (None, "direct")] if proxy_cfg else [(None, "direct")]
    last_exc = None
    for cfg, mode in modes:
        try:
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
            launch_kwargs: Dict = {"headless": True, "args": launch_args}
            if cfg:
                launch_kwargs["proxy"] = cfg
            # Use system Chromium if available
            system_chrome = os.environ.get("CHROMIUM_PATH") or "/root/.nix-profile/bin/chromium"
            if os.path.exists(system_chrome):
                launch_kwargs["executable_path"] = system_chrome
            browser = await pw.chromium.launch(**launch_kwargs)
            # Build extra headers and options for context
            ua = _random_ua()
            if use_mobile:
                # Mobile UA string (simplified) for fallback
                ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
            headers = {
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": "https://www.google.com/",
                "DNT": "1",
            }
            # Attach proxy authorization header if credentials exist
            if cfg and cfg.get("username") and cfg.get("password"):
                import base64
                token = base64.b64encode(f"{cfg['username']}:{cfg['password']}".encode()).decode()
                headers["Proxy-Authorization"] = f"Basic {token}"
            context_kwargs = {
                "user_agent": ua,
                "locale": "en-GB",
                "timezone_id": os.getenv("TZ", "Europe/London"),
                "extra_http_headers": headers,
            }
            if cfg:
                context_kwargs["proxy"] = cfg
            context = await browser.new_context(**context_kwargs)
            # Block heavy resources on each page created via context.route
            async def _block_heavy(route):
                return await route.abort()
            await context.route(r"**/*.{png,jpg,jpeg,svg,ico,woff,woff2,ttf,css}", _block_heavy)
            return browser, context, mode
        except Exception as e:
            last_exc = e
            try:
                await browser.close()
            except Exception:
                pass
            continue
    raise last_exc or RuntimeError("Failed to launch browser")

async def goto_stable(page: Page, url: str) -> None:
    """
    Navigate to a URL with robust waiting. First waits for domcontentloaded, then
    waits for either search result articles or network idle. Total timeout ~60s.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
    try:
        # Wait for search result articles to load quickly
        await page.wait_for_selector("article[data-testid='search-result']", timeout=15_000)
    except Exception:
        # Fallback: wait for network idle
        await page.wait_for_load_state("networkidle", timeout=15_000)

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
    """
    Load a listing detail page using Playwright. First attempt to navigate with a page
    and parse JSON‑LD. On failure or parse miss, fallback to using the browser
    context's request API to fetch HTML with the same headers.
    """
    page = await context.new_page()
    try:
        try:
            await goto_stable(page, url)
            html = await page.content()
            parsed = parse_listing_jsonld(html)
            if parsed:
                return parsed
        except Exception:
            # swallow and fallback below
            pass
        # Fallback: use context.request to fetch HTML
        try:
            resp = await context.request.get(url, headers=context._options.get("extraHTTPHeaders"))
            html = await resp.text()
            parsed = parse_listing_jsonld(html)
            return parsed
        except Exception:
            return None
    finally:
        await page.close()

async def _extract_search_results_with_playwright(context: BrowserContext, search_url: str) -> List[str]:
    """
    Load a Zoopla search result page via Playwright and extract listing URLs using the
    extract_search_result_urls function. Returns a list of absolute URLs.
    """
    """
    Load a Zoopla search result page via Playwright and extract listing URLs.
    Uses goto_stable for navigation and falls back to context.request on failure.
    """
    page = await context.new_page()
    try:
        try:
            await goto_stable(page, search_url)
            html = await page.content()
            urls = extract_search_result_urls(html)
            if urls:
                return urls
        except Exception:
            pass
        # Fallback: fetch HTML via context.request
        try:
            resp = await context.request.get(search_url, headers=context._options.get("extraHTTPHeaders"))
            html = await resp.text()
            return extract_search_result_urls(html)
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
    # Prepare a container for results and metrics
    results: List[Dict] = []
    # If Playwright isn't installed, skip and return empty results
    if async_playwright is None:
        print("Playwright is not available; skipping live scraping.")
        return results
    # Metrics tracking
    total_attempts = 0
    proxy_mode_counts = {"proxy": 0, "direct": 0}
    goto_errors = 0
    http_403_count = 0
    start_time = time.time()
    async with async_playwright() as pw:
        # Limit total requests per run
        request_count = 0
        # Up to three passes: proxy, proxy retry, direct/mobile
        for attempt in range(1, 3 + 1):
            if request_count >= MAX_REQUESTS_PER_RUN:
                break
            use_mobile = (attempt == 3)
            try:
                browser, context, proxy_mode = await launch_browser_with_fallback(pw, PROXY_URL, use_mobile=use_mobile)
            except Exception:
                # Failed to launch; skip to next attempt
                continue
            # Record which proxy mode used
            proxy_mode_counts[proxy_mode] = proxy_mode_counts.get(proxy_mode, 0) + 1
            try:
                for q in queries:
                    for page_num in range(1, PAGES_PER_QUERY + 1):
                        if request_count >= MAX_REQUESTS_PER_RUN:
                            break
                        # Build search URL. For mobile fallback, use m.zoopla.co.uk
                        if q.startswith("http"):
                            search_url = q
                        else:
                            base = "https://www.zoopla.co.uk"
                            if use_mobile:
                                base = "https://m.zoopla.co.uk"
                            search_url = f"{base}/to-rent/property/{q}/?page_size=25&page={page_num}"
                        # Extract listing URLs from search page
                        try:
                            urls = await _extract_search_results_with_playwright(context, search_url)
                        except Exception:
                            goto_errors += 1
                            urls = []
                        for url in urls:
                            if request_count >= MAX_REQUESTS_PER_RUN:
                                break
                            # Random jitter between requests
                            await asyncio.sleep(random.uniform(DELAY_MS_MIN/1000.0, DELAY_MS_MAX/1000.0))
                            try:
                                listing_data = await _extract_detail_with_playwright(context, url)
                            except Exception as exc:
                                # Count potential 403/429 if message includes the code
                                if "403" in str(exc):
                                    http_403_count += 1
                                goto_errors += 1
                                listing_data = None
                            request_count += 1
                            total_attempts += 1
                            if not listing_data:
                                continue
                            enriched = enrich_listing(listing_data)
                            if validate_listing(enriched):
                                results.append(enriched)
                        if request_count >= MAX_REQUESTS_PER_RUN:
                            break
                    if request_count >= MAX_REQUESTS_PER_RUN:
                        break
            finally:
                # Always close resources
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass
    # Deduplicate results
    results = dedupe_listings(results)
    # Compute summary metrics
    end_time = time.time()
    total = len(results)
    complete = sum(1 for l in results if validate_listing(l))
    failed = total_attempts - complete
    avg_ms = 0
    if complete > 0:
        avg_ms = int(((end_time - start_time) * 1000) / complete)
    # Print summary metrics
    print(f"Total attempts: {total_attempts}, proxy_mode={proxy_mode_counts}, goto_errors={goto_errors}, http_403_count={http_403_count}")
    print(f"ZP_RUN_COMPLETE ✅ listings={total} complete={complete} failed={failed} avg_ms={avg_ms}")
    return results

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
    """
    Entrypoint for the Zoopla scraper.

    When run locally (e.g. via `python scrapers/zoopla/main.py`), this script will
    either consume fixtures (if they exist) or perform a live scrape using Playwright
    based on the ZP_QUERIES environment variable.
    After scraping, it emits listings via the webhook or to the buffer and prints a
    summary line required by the acceptance criteria.
    """
    # Determine if fixtures exist; if so, run fixture mode for tests
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    run_fixture_mode = os.path.isdir(fixtures_dir) and bool(os.listdir(fixtures_dir))
    if run_fixture_mode:
        search_files = [f for f in os.listdir(fixtures_dir) if f.startswith("search-page")]
        search_pages: List[str] = []
        for fname in sorted(search_files):
            with open(os.path.join(fixtures_dir, fname), "r", encoding="utf-8") as f:
                search_pages.append(f.read())
        # Mapping for listing pages
        listing_files = [f for f in os.listdir(fixtures_dir) if f.startswith("listing-")]
        listing_pages: Dict[str, str] = {}
        for fname in listing_files:
            with open(os.path.join(fixtures_dir, fname), "r", encoding="utf-8") as f:
                listing_pages[f"https://www.zoopla.co.uk/{fname}"] = f.read()
        listings = asyncio.run(scrape_from_fixtures(search_pages, listing_pages))
        enriched = [enrich_listing(l) for l in listings]
        emit_listings(enriched)
        print(json.dumps({"count": len(enriched), "listings": enriched}, indent=2))
    else:
        # Live scraping mode
        # Determine queries: comma‑separated list of postcodes or full URLs
        query_env = os.getenv("ZP_QUERIES", "L4,L5,L6")
        queries = [q.strip() for q in query_env.split(",") if q.strip()]
        listings = asyncio.run(run_playwright_scraper(queries))
        emit_listings(listings)
        total = len(listings)
        complete = sum(1 for l in listings if validate_listing(l))
        failed = total - complete
        avg_ms = 0  # average ms per listing not available in this script
        print(f"ZP_RUN_COMPLETE ✅ listings={total} complete={complete} failed={failed} avg_ms={avg_ms}")
