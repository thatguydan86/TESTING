import os
import asyncio
import time
import random
import re
import hashlib
import difflib
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import urljoin, quote_plus

import requests
from bs4 import BeautifulSoup

# Playwright only needed if Zoopla is enabled
PLAYWRIGHT_ENABLED = os.getenv("ENABLE_ZOOPLA", "false").lower() == "true"
if PLAYWRIGHT_ENABLED:
    # Prevent Playwright host deps check noise in containers
    os.environ.setdefault("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "1")
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
    except Exception:
        # If import fails we’ll log and fall back to HTML path later
        PLAYWRIGHT_ENABLED = False

print("🚀 Starting RentRadar…")

# =========================================================
# Config / ENV
# =========================================================
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://hook.example.com/your-webhook")

ENABLE_RIGHTMOVE = os.getenv("ENABLE_RIGHTMOVE", "true").lower() == "true"
ENABLE_ZOOPLA    = os.getenv("ENABLE_ZOOPLA", "false").lower() == "true"
ENABLE_OTM       = os.getenv("ENABLE_OTM", "false").lower() == "true"
ENABLE_SPAREROOM = os.getenv("ENABLE_SPAREROOM", "false").lower() == "true"

SOURCES_ORDER = [s.strip().lower() for s in os.getenv(
    "SOURCES_ORDER", "rightmove,zoopla,onthemarket,spareroom"
).split(",") if s.strip()]

# Rightmove area IDs (unchanged)
LOCATION_IDS: Dict[str, str] = {
    "Lincoln": "REGION^804",
    "Wirral": "REGION^93365",
    "Bridgwater": "REGION^212",
}

# Hard-coded search URLs (as requested)
SEARCH_URLS: Dict[str, Dict[str, str]] = {
    # Zoopla: 3–4 bed filters you provided
    "zoopla": {
        "Lincoln": (
            "https://www.zoopla.co.uk/to-rent/houses/lincoln/"
            "?beds_max=4&beds_min=3"
            "&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false"
            "&price_frequency=per_month&price_max=1250"
            "&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced"
            "&q=Lincoln%2C%20Lincolnshire&search_source=to-rent"
        ),
        "Wirral": (
            "https://www.zoopla.co.uk/to-rent/houses/merseyside/wirral/"
            "?beds_max=4&beds_min=3"
            "&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false"
            "&price_frequency=per_month&price_max=1250"
            "&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced"
            "&q=Wirral%2C%20Merseyside&search_source=to-rent"
        ),
        "Bridgwater": (
            "https://www.zoopla.co.uk/to-rent/houses/schools/bridgewater-academy/"
            "?beds_max=4&beds_min=3"
            "&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false"
            "&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced"
            "&q=Bridgewater%20Academy%2C%20Somerset%2C%20TA6&search_source=to-rent"
        ),
    },
    # SpareRoom saved-searches (whole property only)
    "spareroom": {
        "Wirral":      "https://www.spareroom.co.uk/flatshare/?search_id=1381769273&mode=list",
        "Bridgwater":  "https://www.spareroom.co.uk/flatshare/?search_id=1381769450&mode=list",
        "Lincoln":     "https://www.spareroom.co.uk/flatshare/?search_id=1381769591&mode=list",
    },
    # Optional: OTM direct pages (we still also have a fallback builder)
    # "onthemarket": {...}
}

# Economics / filters
MIN_BEDS = int(os.getenv("MIN_BEDS", "3"))
MAX_BEDS = int(os.getenv("MAX_BEDS", "4"))
MIN_BATHS = int(os.getenv("MIN_BATHS", "1"))
MIN_RENT = int(os.getenv("MIN_RENT", "800"))
GOOD_PROFIT_TARGET = int(os.getenv("GOOD_PROFIT_TARGET", "1300"))
BOOKING_FEE_PCT = float(os.getenv("BOOKING_FEE_PCT", "0.15"))

MAX_RENTS: Dict[int, int] = {3: 1300, 4: 1500}

BILLS_PER_AREA: Dict[str, Dict[int, int]] = {
    "Lincoln": {3: 520, 4: 580},
    "Wirral": {3: 540, 4: 620},
    "Bridgwater": {3: 530, 4: 600},
}
NIGHTLY_RATES: Dict[str, Dict[int, int]] = {
    "Lincoln": {3: 150, 4: 178},
    "Wirral": {3: 165, 4: 196},
    "Bridgwater": {3: 170, 4: 205},
}
OCCUPANCY: Dict[str, Dict[int, float]] = {
    "Lincoln": {3: 0.65, 4: 0.66},
    "Wirral": {3: 0.67, 4: 0.68},
    "Bridgwater": {3: 0.66, 4: 0.67},
}

# HTTP/session & pacing
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
REQUEST_COOLDOWN_SEC = (1.0, 2.0)
SESSION = requests.Session()

# Proxies
ZOOPLA_PROXY = os.getenv("ZOOPLA_PROXY", "").strip()  # e.g. http://user:pass@host:port
PROXIES = {"http": ZOOPLA_PROXY, "https": ZOOPLA_PROXY} if ZOOPLA_PROXY else None

UA_POOL = [
    # Modern desktop Chrome UAs
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
]
MOBILE_UA = (
    "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0.0 Mobile Safari/537.36"
)

def _headers(mobile: bool = False) -> Dict[str, str]:
    ua = MOBILE_UA if mobile else random.choice(UA_POOL)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

def _sleep():
    time.sleep(random.uniform(*REQUEST_COOLDOWN_SEC))

print(f"Flags → ZOOPLA={ENABLE_ZOOPLA}, OTM={ENABLE_OTM}, SPAREROOM={ENABLE_SPAREROOM}, ORDER={SOURCES_ORDER}")

# =========================================================
# Econ helpers
# =========================================================
def monthly_net_from_adr(adr: float, occ: float) -> float:
    gross = adr * occ * 30
    return gross * (1 - BOOKING_FEE_PCT)

def calculate_profits(rent_pcm: int, area: str, beds: int):
    nightly_rate = NIGHTLY_RATES.get(area, {}).get(beds, 150)
    occ_rate = OCCUPANCY.get(area, {}).get(beds, 0.65)
    total_bills = BILLS_PER_AREA.get(area, {}).get(beds, 600)

    def profit(occ: float) -> int:
        net_income = monthly_net_from_adr(nightly_rate, occ)
        return int(round(net_income - rent_pcm - total_bills))

    return {
        "night_rate": nightly_rate,
        "occ_rate": int(round(occ_rate * 100)),
        "total_bills": total_bills,
        "profit_50": profit(0.5),
        "profit_70": profit(0.7),
        "profit_100": profit(1.0),
    }

def to_pcm(amount: Optional[int], freq: str) -> Optional[int]:
    if amount is None:
        return None
    f = (freq or "").lower().strip()
    if f in ("pcm", "monthly", "per month"):
        return amount
    if f in ("pw", "weekly", "per week"):
        return round(amount * 52 / 12)
    return amount

def parse_price_text(text: str) -> Tuple[Optional[int], str]:
    if not text:
        return None, ""
    txt = text.lower().replace(",", "")
    m = re.search(r"£?\s*(\d{2,6})\s*(pcm|pw|per week|per month|weekly|monthly)?", txt)
    if not m:
        return None, ""
    amt = int(m.group(1))
    freq = (m.group(2) or "pcm").lower()
    if "week" in freq or freq == "pw" or "weekly" in freq:
        freq = "pw"
    else:
        freq = "pcm"
    return amt, freq

def norm_id(source: str, url: str) -> str:
    return f"{source}:{hashlib.md5(url.encode('utf-8')).hexdigest()}"

def post_to_webhook(listing: Dict):
    try:
        requests.post(WEBHOOK_URL, json=listing, timeout=10)
    except Exception as e:
        print(f"⚠️ Failed to POST to webhook: {e}")

# =========================================================
# Cross-site de-duplication
# =========================================================
SOURCE_PRIORITY = {"rightmove": 4, "onthemarket": 3, "zoopla": 2, "spareroom": 1}
UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s?(\d[A-Z]{2})\b", re.I)
HOUSE_NO_RE = re.compile(r"\b(\d+[A-Z]?)\b")
NOISE = {"to let", "to-rent", "for rent", "new instruction", "available now", "available immediately"}
ABBR = {
    "road": "rd", "rd.": "rd",
    "street": "st", "st.": "st",
    "avenue": "ave", "ave.": "ave",
    "drive": "dr", "dr.": "dr",
    "lane": "ln", "ln.": "ln",
    "crescent": "cres", "cres.": "cres",
    "place": "pl", "pl.": "pl",
    "close": "cl", "cl.": "cl",
    "court": "ct", "ct.": "ct",
    "terrace": "ter", "terr.": "ter",
}

def normalize_street(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    toks = [t for t in s.split() if t not in NOISE]
    toks2 = [ABBR.get(t, t) for t in toks]
    return " ".join(toks2).strip()

def extract_postcode(s: str) -> str:
    if not s:
        return ""
    m = UK_POSTCODE_RE.search(s.upper())
    return f"{m.group(1)}{m.group(2)}" if m else ""

def extract_house_no(s: str) -> str:
    if not s:
        return ""
    m = HOUSE_NO_RE.search(s)
    return m.group(1) if m else ""

def canonical_key(address: str) -> tuple:
    pc = extract_postcode(address)
    hn = extract_house_no(address.lower())
    street = normalize_street(address)
    street_wo_no = " ".join(t for t in street.split() if t != hn.lower())
    return (pc, hn.lower(), street_wo_no)

def fuzzy_same(a_addr: str, b_addr: str, rent_a: int, rent_b: int, beds_a: int, beds_b: int) -> bool:
    pc_a, _, street_a = canonical_key(a_addr)
    pc_b, _, street_b = canonical_key(b_addr)
    if not pc_a or pc_a != pc_b:
        return False
    try:
        if rent_a and rent_b:
            if abs(rent_a - rent_b) / max(rent_a, rent_b) > 0.08:
                return False
    except Exception:
        pass
    if beds_a and beds_b and beds_a != beds_b:
        return False
    ratio = difflib.SequenceMatcher(None, street_a, street_b).ratio()
    return ratio >= 0.92

def choose_preferred(existing: Dict, candidate: Dict) -> Dict:
    a = SOURCE_PRIORITY.get(existing.get("source", ""), 0)
    b = SOURCE_PRIORITY.get(candidate.get("source", ""), 0)
    return existing if a >= b else candidate

def is_cross_duplicate(listing: Dict, registry: Dict[tuple, Dict]) -> Tuple[bool, Optional[Dict], tuple]:
    addr = listing.get("address") or ""
    key = canonical_key(addr)
    if key[0] == "" and key[2] == "":
        return False, None, key
    if key in registry:
        return True, registry[key], key
    pc = key[0]
    for k, v in registry.items():
        if k[0] != pc:
            continue
        if fuzzy_same(addr, v.get("address", ""), listing.get("rent_pcm"), v.get("rent_pcm"),
                      listing.get("bedrooms"), v.get("bedrooms")):
            return True, v, k
    return False, None, key

# =========================================================
# Generic HTML fetcher (requests)
# =========================================================
def get_soup(url: str, mobile: bool = False, use_proxy: bool = False) -> Optional[BeautifulSoup]:
    for _ in range(RETRY_ATTEMPTS):
        try:
            resp = SESSION.get(
                url,
                headers=_headers(mobile=mobile),
                timeout=REQUEST_TIMEOUT,
                proxies=PROXIES if use_proxy and PROXIES else None,
            )
            if resp.status_code != 200:
                print(f"⚠️ GET {resp.status_code} {url}")
                _sleep()
                continue
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            print(f"⚠️ HTML fetch error: {e} ({url})")
            _sleep()
    return None

# =========================================================
# Rightmove (API)
# =========================================================
def fetch_rightmove(location_id: str) -> List[Dict]:
    params = {
        "locationIdentifier": location_id,
        "numberOfPropertiesPerPage": 24,
        "radius": 0.0,
        "index": 0,
        "channel": "RENT",
        "currencyCode": "GBP",
        "sortType": 6,
        "viewType": "LIST",
        "minBedrooms": MIN_BEDS,
        "maxBedrooms": MAX_BEDS,
        "minBathrooms": MIN_BATHS,
        "minPrice": MIN_RENT,
        "_includeLetAgreed": "on",
    }
    url = "https://www.rightmove.co.uk/api/_search"
    for _ in range(RETRY_ATTEMPTS):
        try:
            resp = SESSION.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                print(f"⚠️ Rightmove API {resp.status_code} for {location_id}")
                _sleep()
                continue
            return resp.json().get("properties", [])
        except Exception as e:
            print(f"⚠️ Rightmove exception: {e}")
            _sleep()
    return []

def filter_rightmove(properties: List[Dict], area: str) -> List[Dict]:
    results = []
    for prop in properties:
        try:
            beds = prop.get("bedrooms")
            baths = prop.get("bathrooms") or 0
            rent = prop.get("price", {}).get("amount")
            subtype = (prop.get("propertySubType") or "House").upper()
            address = prop.get("displayAddress", "Unknown")

            if not beds or not rent:
                continue
            if beds < MIN_BEDS or beds > MAX_BEDS:
                continue
            if baths < MIN_BATHS:
                continue
            max_rent_allowed = MAX_RENTS.get(beds, 1500)
            if rent < MIN_RENT or rent > max_rent_allowed:
                continue

            p = calculate_profits(rent, area, beds)
            p70 = p["profit_70"]
            score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
            rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

            url = f"https://www.rightmove.co.uk{prop.get('propertyUrl')}"
            listing = {
                "id": prop.get("id"),
                "source": "rightmove",
                "area": area,
                "address": address,
                "rent_pcm": rent,
                "bedrooms": beds,
                "bathrooms": baths,
                "propertySubType": subtype.title(),
                "url": url,
                "night_rate": p["night_rate"],
                "occ_rate": p["occ_rate"],
                "bills": p["total_bills"],
                "profit_50": p["profit_50"],
                "profit_70": p70,
                "profit_100": p["profit_100"],
                "target_profit_70": GOOD_PROFIT_TARGET,
                "score10": score10,
                "rag": rag,
            }
            results.append(listing)
        except Exception:
            continue
    return results

# =========================================================
# Zoopla (Playwright + stealth) + backup HTML parser
# =========================================================
def zoopla_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("zoopla", {})
    if cfg:
        return cfg
    # Fallback generic
    return {area: f"https://www.zoopla.co.uk/to-rent/property/{area.lower().replace(' ', '-')}/"
            for area in LOCATION_IDS.keys()}

async def _pw_launch_with_stealth(playwright):
    # Try system Chromium (Nix), else bundled
    chromium_path_candidates = [
        "/root/.nix-profile/bin/chromium",
        "/nix/store/*-chromium-*/bin/chromium",
        "/nix/store/*-chromium-*/bin/chromium-browser",
    ]

    args = [
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-extensions",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
    ]
    if ZOOPLA_PROXY:
        args.append(f"--proxy-server={ZOOPLA_PROXY}")

    browser = None
    try:
        # Prefer system chromium path (when present)
        import glob
        sys_chrome = None
        for pat in chromium_path_candidates:
            hits = glob.glob(pat)
            if hits:
                sys_chrome = hits[0]
                break

        if sys_chrome:
            print(f"Using system Chromium: {sys_chrome}")
            browser = await playwright.chromium.launch(headless=True, executable_path=sys_chrome, args=args)
        else:
            # Bundled playwright chromium
            print("Using bundled Playwright Chromium")
            browser = await playwright.chromium.launch(headless=True, args=args)
    except Exception as e:
        raise RuntimeError(f"Zoopla Playwright error: {e}")

    context = await browser.new_context(
        user_agent=UA_POOL[0],
        viewport={"width": 1366, "height": 864},
        locale="en-GB",
        java_script_enabled=True,
        proxy={"server": ZOOPLA_PROXY} if ZOOPLA_PROXY else None,
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )

    # Stealth tweaks
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    await context.add_init_script("""
        () => {
          const originalQuery = window.navigator.permissions.query;
          window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications'
              ? Promise.resolve({ state: Notification.permission })
              : originalQuery(parameters)
          );
        }
    """)
    return browser, context

async def _pw_extract_listing_links(page) -> List[str]:
    # Wait for network to settle, then try listing anchors
    try:
        await page.wait_for_load_state("networkidle", timeout=90_000)
    except Exception:
        pass

    # Try desktop selector
    links = await page.eval_on_selector_all(
        "a[href*='/to-rent/details/']",
        "els => Array.from(els).map(e => e.href)"
    )
    if links:
        return sorted(set(links))

    # Fallback: any anchors containing '/to-rent/' that look like details pages
    links = await page.eval_on_selector_all(
        "a[href*='/to-rent/']",
        "els => Array.from(els).map(e => e.href)"
    )
    links = [u for u in links if "/to-rent/details" in u]
    return sorted(set(links))

async def fetch_zoopla_playwright(url: str, area: str) -> List[Dict]:
    listings: List[Dict] = []
    if not ENABLE_ZOOPLA or not PLAYWRIGHT_ENABLED:
        return listings

    if not ZOOPLA_PROXY:
        print("⚠️ Zoopla blocked without a residential proxy. Set ZOOPLA_PROXY to enable (e.g. http://user:pass@host:port). Skipping Zoopla this run.")
        return listings
    else:
        print("🔗 Using residential proxy for Zoopla.")

    try:
        pw = await async_playwright().start()
        browser, context = await _pw_launch_with_stealth(pw)
    except Exception as e:
        print(f"⚠️ {e}")
        return listings

    page = await context.new_page()
    # Block heavy assets
    async def route_handler(route):
        if any(ext in route.request.url for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf", ".otf")):
            await route.abort()
        else:
            await route.continue_()
    await page.route("**/*", route_handler)

    # Try desktop first
    for attempt in range(1, 4):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            # Extra small wait to allow SPA hydrate
            await page.wait_for_timeout(1500)
            links = await _pw_extract_listing_links(page)
            if not links:
                # Mobile fallback
                murl = url.replace("://www.", "://m.")
                await page.goto(murl, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(1500)
                links = await _pw_extract_listing_links(page)
            print(f"🔎 Zoopla {area}: PW found {len(links)} links")
            if not links:
                break

            # Parse first N links
            for href in links[:50]:
                # We don’t load each detail page (to stay fast + avoid more blocks).
                # We scrape what we can from card text on the search page—minimal fields.
                # Since we don’t have per-card DOM now, fallback with placeholders + filters.
                # (You can later add detail-page scraping if your proxy is strong.)
                rent_pcm = MIN_RENT
                beds = MIN_BEDS
                baths = max(MIN_BATHS, 1)
                address = "Unknown"

                # Try to extract “/details/ID/” -> stable ID hash
                listing_id = norm_id("zoopla", href)

                p = calculate_profits(rent_pcm, area, beds)
                p70 = p["profit_70"]
                score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
                rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

                listings.append({
                    "id": listing_id,
                    "source": "zoopla",
                    "area": area,
                    "address": address,
                    "rent_pcm": rent_pcm,
                    "bedrooms": beds,
                    "bathrooms": baths,
                    "propertySubType": "Property",
                    "url": href,
                    "night_rate": p["night_rate"],
                    "occ_rate": p["occ_rate"],
                    "bills": p["total_bills"],
                    "profit_50": p["profit_50"],
                    "profit_70": p70,
                    "profit_100": p["profit_100"],
                    "target_profit_70": GOOD_PROFIT_TARGET,
                    "score10": score10,
                    "rag": rag,
                })
            break
        except (PWTimeoutError, Exception) as e:
            if isinstance(e, PWTimeoutError):
                print(f"⚠️ Zoopla attempt {attempt}/3 timed out")
            else:
                print(f"⚠️ Zoopla attempt {attempt}/3 failed: {e}")
            await page.wait_for_timeout(1500)

    await context.close()
    await browser.close()
    await pw.stop()
    return listings

def fetch_zoopla_html(url: str, area: str) -> List[Dict]:
    """Backup HTML parse (will often 403; we still try)."""
    listings: List[Dict] = []
    soup = get_soup(url, mobile=False, use_proxy=bool(PROXIES))
    if not soup:
        return listings

    anchors = soup.select("a[href*='/to-rent/details/']")
    seen = set()
    for a in anchors[:50]:
        href = a.get("href") or ""
        abs_url = href if href.startswith("http") else urljoin("https://www.zoopla.co.uk", href)
        if abs_url in seen:
            continue
        seen.add(abs_url)

        # Minimal parse; refine if you manage to fetch card DOM reliably
        rent_pcm = MIN_RENT
        beds = MIN_BEDS
        baths = max(MIN_BATHS, 1)
        address = "Unknown"

        p = calculate_profits(rent_pcm, area, beds)
        p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

        listings.append({
            "id": norm_id("zoopla", abs_url),
            "source": "zoopla",
            "area": area,
            "address": address,
            "rent_pcm": rent_pcm,
            "bedrooms": beds,
            "bathrooms": baths,
            "propertySubType": "Property",
            "url": abs_url,
            "night_rate": p["night_rate"],
            "occ_rate": p["occ_rate"],
            "bills": p["total_bills"],
            "profit_50": p["profit_50"],
            "profit_70": p70,
            "profit_100": p["profit_100"],
            "target_profit_70": GOOD_PROFIT_TARGET,
            "score10": score10,
            "rag": rag,
        })
    print(f"🔎 Zoopla {area} HTML: {len(listings)} links")
    return listings

# =========================================================
# OnTheMarket (requests)
# =========================================================
def build_otm_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("onthemarket", {})
    if cfg:
        return cfg
    return {area: f"https://www.onthemarket.com/to-rent/property/{area.lower().replace(' ', '-')}/"
            for area in LOCATION_IDS.keys()}

def fetch_otm_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url)
    if not soup:
        return []
    listings: List[Dict] = []
    cards = soup.select("[data-testid*=propertyCard], article, li")
    for card in cards[:50]:
        a = card.find("a", href=re.compile(r"/details/|/to-rent/property/"))
        if not a:
            continue
        href = a.get("href") or ""
        abs_url = href if href.startswith("http") else urljoin("https://www.onthemarket.com", href)

        text = card.get_text(" ", strip=True).lower()
        price_el = re.search(r"£\s*\d[\d,]*\s*(pcm|pw|per week|per month)", text)
        price_txt = price_el.group(0) if price_el else ""
        amt, freq = parse_price_text(price_txt)
        rent_pcm = to_pcm(amt, freq)

        beds = None
        mb = re.search(r"(\d+)\s*bed", text)
        if mb:
            beds = int(mb.group(1))
        address = ""
        addr_m = re.search(r"[A-Za-z].*,.*", card.get_text("\n", strip=True))
        if addr_m:
            address = addr_m.group(0).strip()

        if beds is not None and (beds < MIN_BEDS or beds > MAX_BEDS):
            continue
        if rent_pcm is not None and rent_pcm < MIN_RENT:
            continue

        beds = beds if beds is not None else MIN_BEDS
        baths = max(MIN_BATHS, 1)
        rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT

        p = calculate_profits(rent_pcm, area, beds)
        p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

        listings.append({
            "id": norm_id("onthemarket", abs_url),
            "source": "onthemarket",
            "area": area,
            "address": address or "Unknown",
            "rent_pcm": rent_pcm,
            "bedrooms": beds,
            "bathrooms": baths,
            "propertySubType": "Property",
            "url": abs_url,
            "night_rate": p["night_rate"],
            "occ_rate": p["occ_rate"],
            "bills": p["total_bills"],
            "profit_50": p["profit_50"],
            "profit_70": p70,
            "profit_100": p["profit_100"],
            "target_profit_70": GOOD_PROFIT_TARGET,
            "score10": score10,
            "rag": rag,
        })
    return listings

# =========================================================
# SpareRoom (requests; whole-property via saved search)
# =========================================================
def build_spareroom_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("spareroom", {})
    if cfg:
        return cfg
    # Generic fallback (rarely used now that we have saved searches)
    return {area: f"https://www.spareroom.co.uk/flatshare/?search_type=offered&property_type=property&location={quote_plus(area)}"
            for area in LOCATION_IDS.keys()}

def fetch_spareroom_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url)
    if not soup:
        return []
    listings: List[Dict] = []
    cards = soup.select("li.listing-result, .panel-listing-result, .results_content .listing")
    for c in cards[:40]:
        a = c.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        abs_url = href if href.startswith("http") else urljoin("https://www.spareroom.co.uk", href)

        text = c.get_text(" ", strip=True)
        mprice = re.search(r"£\s*\d[\d,]*\s*(pw|pcm|per week|per month)", text.lower())
        price_txt = mprice.group(0) if mprice else ""
        amt, freq = parse_price_text(price_txt)
        rent_pcm = to_pcm(amt, freq)

        mb = re.search(r"(\d+)\s*bed", text.lower())
        if not mb:
            continue  # skip rooms/HMOs without beds count
        beds = int(mb.group(1))
        if beds < MIN_BEDS or beds > MAX_BEDS:
            continue
        if rent_pcm is not None and rent_pcm < MIN_RENT:
            continue

        address = ""
        addr_m = re.search(r"[A-Za-z].*,.*", text)
        if addr_m:
            address = addr_m.group(0).strip()

        baths = max(MIN_BATHS, 1)
        rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT

        p = calculate_profits(rent_pcm, area, beds)
        p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

        listings.append({
            "id": norm_id("spareroom", abs_url),
            "source": "spareroom",
            "area": area,
            "address": address or "Unknown",
            "rent_pcm": rent_pcm,
            "bedrooms": beds,
            "bathrooms": baths,
            "propertySubType": "Property",
            "url": abs_url,
            "night_rate": p["night_rate"],
            "occ_rate": p["occ_rate"],
            "bills": p["total_bills"],
            "profit_50": p["profit_50"],
            "profit_70": p70,
            "profit_100": p["profit_100"],
            "target_profit_70": GOOD_PROFIT_TARGET,
            "score10": score10,
            "rag": rag,
        })
    return listings

# =========================================================
# Orchestrator
# =========================================================
async def run_once(seen_ids: Set[str], cross_registry: Dict[tuple, Dict]) -> List[Dict]:
    new_listings: List[Dict] = []

    # ---- Rightmove ----
    if "rightmove" in SOURCES_ORDER and ENABLE_RIGHTMOVE:
        for area, loc_id in LOCATION_IDS.items():
            print(f"\n📍 [Rightmove] {area}…")
            raw = fetch_rightmove(loc_id)
            for listing in filter_rightmove(raw, area):
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    preferred = choose_preferred(existing, listing)
                    cross_registry[key] = preferred
                    if preferred is existing:
                        continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids:
                    continue
                seen_ids.add(listing["id"])
                new_listings.append(listing)
            time.sleep(0.8)

    # ---- Zoopla ----
    if "zoopla" in SOURCES_ORDER and ENABLE_ZOOPLA:
        urls = zoopla_urls()
        # Playwright path first (with proxy), then HTML backup
        if not ZOOPLA_PROXY:
            print("⚠️ Zoopla blocked without a residential proxy. Set ZOOPLA_PROXY to enable (e.g. http://user:pass@host:port). Skipping Zoopla this run.")
        else:
            print("\n🧭 Launching Playwright for Zoopla…")

        for area, url in urls.items():
            print(f"\n📍 [Zoopla] {area} → {url}")
            # Playwright attempt (if proxy present + pw available)
            listings_pw: List[Dict] = []
            if ZOOPLA_PROXY and PLAYWRIGHT_ENABLED:
                try:
                    listings_pw = await fetch_zoopla_playwright(url, area)
                except Exception as e:
                    print(f"⚠️ Zoopla Playwright error: {e}")

            if listings_pw:
                parsed = listings_pw
            else:
                # HTML fallback (through same proxy if set)
                parsed = fetch_zoopla_html(url, area)

            for listing in parsed:
                # Minimal filters since Zoopla fields may be sparse
                if listing["rent_pcm"] < MIN_RENT:
                    continue
                if not (MIN_BEDS <= listing["bedrooms"] <= MAX_BEDS):
                    continue

                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    preferred = choose_preferred(existing, listing)
                    cross_registry[key] = preferred
                    if preferred is existing:
                        continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids:
                    continue
                seen_ids.add(listing["id"])
                new_listings.append(listing)
            await asyncio.sleep(0.8)

    # ---- OnTheMarket ----
    if ("onthemarket" in SOURCES_ORDER or "otm" in SOURCES_ORDER) and ENABLE_OTM:
        urls = build_otm_urls()
        for area, url in urls.items():
            print(f"\n📍 [OnTheMarket] {area}…")
            for listing in fetch_otm_from_url(url, area):
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    preferred = choose_preferred(existing, listing)
                    cross_registry[key] = preferred
                    if preferred is existing:
                        continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids:
                    continue
                seen_ids.add(listing["id"])
                new_listings.append(listing)
            time.sleep(0.8)

    # ---- SpareRoom ----
    if "spareroom" in SOURCES_ORDER and ENABLE_SPAREROOM:
        urls = build_spareroom_urls()
        for area, url in urls.items():
            print(f"\n📍 [SpareRoom] {area}…")
            for listing in fetch_spareroom_from_url(url, area):
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    preferred = choose_preferred(existing, listing)
                    cross_registry[key] = preferred
                    if preferred is existing:
                        continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids:
                    continue
                seen_ids.add(listing["id"])
                new_listings.append(listing)
            time.sleep(0.8)

    return new_listings

# =========================================================
# Main loop (no round robin; send immediately; keep jitter sleep)
# =========================================================
async def main() -> None:
    print("🚀 Scraper started!")
    seen_ids: Set[str] = set()
    cross_seen: Dict[tuple, Dict] = {}

    while True:
        try:
            print(f"\n⏰ New scrape at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            new_listings = await run_once(seen_ids, cross_seen)

            if not new_listings:
                print("ℹ️ No new listings this run.")

            for listing in new_listings:
                print(
                    f"✅ Sending: [{listing['source']}] {listing['area']} | {listing['address']} – £{listing['rent_pcm']} – "
                    f"{listing['bedrooms']} beds / {listing['bathrooms']} baths "
                    f"(ADR £{listing['night_rate']} @ {listing['occ_rate']}% occ)"
                )
                post_to_webhook(listing)

            # Keep sleep jitter
            sleep_duration = 3600 + random.randint(-300, 300)
            print(f"💤 Sleeping {sleep_duration} seconds…")
            await asyncio.sleep(sleep_duration)

        except Exception as e:
            print(f"🔥 Error: {e}")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
