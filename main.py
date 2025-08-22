import os
os.environ.setdefault("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "1")

import asyncio
import time
import random
import re
import glob
import hashlib
import difflib
import requests
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import urljoin, quote_plus
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except Exception:
    async_playwright = None

print("🚀 Starting RentRadar…")

# ========= Config =========
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://hook.eu2.make.com/6k1jjsv0khxbqbfplzv5ibt4d1wh3dwj")

ENABLE_RIGHTMOVE = os.getenv("ENABLE_RIGHTMOVE", "true").lower() == "true"
ENABLE_ZOOPLA    = os.getenv("ENABLE_ZOOPLA", "true").lower() == "true"
ENABLE_OTM       = os.getenv("ENABLE_OTM", "true").lower() == "true"
ENABLE_SPAREROOM = os.getenv("ENABLE_SPAREROOM", "true").lower() == "true"

# Force Zoopla to Playwright only (HTML gets 403 on your host)
ZOOPLA_PW_ONLY = True

SOURCES_ORDER = [s.strip().lower() for s in os.getenv(
    "SOURCES_ORDER",
    "rightmove,zoopla,onthemarket,spareroom"
).split(",") if s.strip()]

LOCATION_IDS: Dict[str, str] = {
    "Lincoln": "REGION^804",
    "Wirral": "REGION^93365",
    "Bridgwater": "REGION^212",
}

# ======= Explicit search URLs =======
SEARCH_URLS: Dict[str, Dict[str, List[str] | str]] = {
    "zoopla": {
        "Lincoln": [
            "https://www.zoopla.co.uk/to-rent/houses/lincoln/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&price_frequency=per_month&price_max=1250&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Lincoln%2C%20Lincolnshire&search_source=to-rent"
        ],
        "Wirral": [
            "https://www.zoopla.co.uk/to-rent/houses/merseyside/wirral/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&price_frequency=per_month&price_max=1250&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Wirral%2C%20Merseyside&search_source=to-rent"
        ],
        "Bridgwater": [
            "https://www.zoopla.co.uk/to-rent/houses/schools/bridgewater-academy/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Bridgewater%20Academy%2C%20Somerset%2C%20TA6&search_source=to-rent"
        ],
    },
    "spareroom": {
        "Wirral":     "https://www.spareroom.co.uk/flatshare/?search_id=1381769273&mode=list",
        "Bridgwater": "https://www.spareroom.co.uk/flatshare/?search_id=1381769450&mode=list",
        "Lincoln":    "https://www.spareroom.co.uk/flatshare/?search_id=1381769591&mode=list",
    },
}

# ========= Economics & filters =========
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

# ========= HTTP/session pacing =========
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
REQUEST_COOLDOWN_SEC = (1.0, 2.0)
SESSION = requests.Session()

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
]

def _headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }

def _sleep():
    time.sleep(random.uniform(*REQUEST_COOLDOWN_SEC))

print(f"Flags → ZOOPLA={ENABLE_ZOOPLA}, OTM={ENABLE_OTM}, SPAREROOM={ENABLE_SPAREROOM}, ORDER={SOURCES_ORDER}")

# ========= Economics helpers =========
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

# ========= Cross-site de-duplication =========
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

# ========= Generic HTML fetcher (requests) =========
def get_soup(url: str) -> Optional[BeautifulSoup]:
    for _ in range(RETRY_ATTEMPTS):
        try:
            resp = SESSION.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                print(f"⚠️ GET {resp.status_code} {url}")
                _sleep()
                continue
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            print(f"⚠️ HTML fetch error: {e} ({url})")
            _sleep()
    return None

# ========= Rightmove (API) =========
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

# ========= Zoopla (Playwright‑only) =========
def build_zoopla_urls() -> Dict[str, List[str]]:
    cfg = SEARCH_URLS.get("zoopla", {})
    out: Dict[str, List[str]] = {}
    for area, urls in cfg.items():
        out[area] = [urls] if isinstance(urls, str) else urls
    if out:
        return out
    return {area: [f"https://www.zoopla.co.uk/to-rent/property/{area.lower().replace(' ', '-')}/"]
            for area in LOCATION_IDS.keys()}

def _to_mobile_zoopla(url: str) -> str:
    # Use mobile host (lighter, fewer anti-bot scripts)
    return url.replace("https://www.zoopla.co.uk", "https://m.zoopla.co.uk")

async def zoopla_launch_browser(pw):
    system_chromium = (
        next(iter(glob.glob("/root/.nix-profile/bin/chromium")), None) or
        next(iter(glob.glob("/nix/store/*-chromium-*/bin/chromium")), None) or
        next(iter(glob.glob("/nix/store/*-chromium-*/bin/chromium-browser")), None)
    )
    args = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--lang=en-GB",
        "--js-flags=--max-old-space-size=128",
        "--disable-features=AutomationControlled",
        "--disable-blink-features=AutomationControlled",
        "--single-process",
        "--renderer-process-limit=1",
        "--window-size=1280,2200",
    ]
    if system_chromium:
        print(f"Using system Chromium: {system_chromium}")
        return await pw.chromium.launch(headless=True, executable_path=system_chromium, args=args)
    print("Using bundled Chromium")
    return await pw.chromium.launch(headless=True, args=args)

async def zoopla_new_context(browser):
    ctx = await browser.new_context(
        locale="en-GB",
        timezone_id="Europe/London",
        user_agent=random.choice(UA_POOL),
        viewport={"width": 1280, "height": 2200},
    )
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en']});
        Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4]});
        window.chrome = window.chrome || { runtime: {} };
    """)
    return ctx

async def zoopla_accept_cookies(page):
    try:
        for sel in [
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept all')",
            "button:has-text('Accept All')",
            "button:has-text('Accept & close')",
            "button:has-text('I accept')",
        ]:
            if await page.locator(sel).count():
                await page.locator(sel).first.click(timeout=1500)
                await page.wait_for_timeout(400)
                break
    except Exception:
        pass

async def zoopla_extract_links(page) -> List[str]:
    links: List[str] = []
    try:
        sel_candidates = [
            "a[data-testid='listing-card-link']",
            "a[data-testid='regular-listing-card-link']",
            "article[data-testid='regular-listing'] a[href]",
            "li[data-testid*='regular-listing'] a[href]",
            "a:has-text('View details')",
            "a[href*='/to-rent/details/']",
        ]
        for sel in sel_candidates:
            try:
                if await page.locator(sel).count():
                    hrefs = await page.eval_on_selector_all(sel, "els => els.map(e => e.href)")
                    if hrefs:
                        links.extend(hrefs)
            except Exception:
                pass

        # sweep all anchors as fallback
        try:
            hrefs_all = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for u in hrefs_all or []:
                if "/to-rent/details/" in u:
                    links.append(u)
        except Exception:
            pass
    except Exception:
        pass

    uniq, seen = [], set()
    for u in links:
        if not u:
            continue
        if not u.startswith("http"):
            u = "https://www.zoopla.co.uk" + u
        if u.endswith("/") and "/to-rent/details/" in u:
            u = u.rstrip("/")
        if "/to-rent/details/" not in u:
            continue
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq

async def fetch_zoopla_playwright(context, url: str, area: str) -> List[Dict]:
    listings: List[Dict] = []
    page = await context.new_page()
    page.set_default_timeout(45000)
    page.set_default_navigation_timeout(45000)
    await page.set_extra_http_headers({"Accept-Language": "en-GB,en;q=0.9"})

    target = _to_mobile_zoopla(url)
    try:
        # Try DOMContentLoaded first — more stable on low-mem hosts
        await page.goto(target, wait_until="domcontentloaded")
        await zoopla_accept_cookies(page)

        # Gentle scroll & optional "load more"
        for _ in range(10):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(700)
            try:
                btn = page.locator("button:has-text('Show more'), button:has-text('Load more')")
                if await btn.count():
                    await btn.first.click(timeout=1200)
                    await page.wait_for_timeout(900)
            except Exception:
                pass

        links = await zoopla_extract_links(page)
        print(f"🔎 Zoopla {area}: PW found {len(links)} links")

        if not links:
            # One retry with a fresh page to avoid “page crashed” residue
            await page.close()
            page = await context.new_page()
            page.set_default_timeout(45000)
            await page.goto(target, wait_until="domcontentloaded")
            await page.wait_for_timeout(1200)
            links = await zoopla_extract_links(page)
            print(f"🔁 Zoopla {area}: PW retry found {len(links)} links")

        for abs_url in links[:120]:
            beds = MIN_BEDS
            rent_pcm = MIN_RENT
            baths = max(MIN_BATHS, 1)
            p = calculate_profits(rent_pcm, area, beds)
            p70 = p["profit_70"]
            score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
            rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")
            listings.append({
                "id": norm_id("zoopla", abs_url),
                "source": "zoopla",
                "area": area,
                "address": "Unknown",
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
    except Exception as e:
        print(f"⚠️ Zoopla PW failed for {target}: {e}")
    finally:
        try:
            await page.close()
        except Exception:
            pass

    print(f"🧮 Zoopla {area}: parsed {len(listings)} listings")
    return listings

# ========= OnTheMarket (requests) =========
def build_otm_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("onthemarket", {})
    if cfg:
        return cfg  # type: ignore
    return {area: f"https://www.onthemarket.com/to-rent/property/{area.lower().replace(' ', '-')}/"
            for area in LOCATION_IDS.keys()}

def fetch_otm_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url)
    if not soup:
        return []
    listings: List[Dict] = []
    cards = soup.select("[data-testid*=propertyCard], article, li")
    for card in cards[:60]:
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

# ========= SpareRoom (requests; whole property via saved search) =========
def build_spareroom_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("spareroom", {})
    if cfg:
        return cfg  # type: ignore
    return {area: f"https://www.spareroom.co.uk/flatshare/?search_type=offered&property_type=property&location={quote_plus(area)}"
            for area in LOCATION_IDS.keys()}

def fetch_spareroom_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url)
    if not soup:
        return []
    listings: List[Dict] = []
    cards = soup.select("li.listing-result, .panel-listing-result, .results_content .listing")
    for c in cards[:60]:
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
            continue
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

# ========= Orchestrator (async) =========
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
            time.sleep(1.0)

    # ---- Zoopla (PW only) ----
    if "zoopla" in SOURCES_ORDER and ENABLE_ZOOPLA and async_playwright is not None:
        urls = build_zoopla_urls()
        print("\n🧭 Launching Playwright for Zoopla…")
        try:
            async with async_playwright() as pw:
                browser = await zoopla_launch_browser(pw)
                context = await zoopla_new_context(browser)

                for area, url_list in urls.items():
                    for url in url_list:
                        print(f"\n📍 [Zoopla] {area} → {url}")
                        try:
                            listings = await fetch_zoopla_playwright(context, url, area)
                            for listing in listings:
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
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            print(f"⚠️ Zoopla PW area run failed for {url}: {e}")

                await context.close()
                await browser.close()
        except Exception as e:
            print(f"⚠️ Playwright not available or failed to launch: {e}")

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
            time.sleep(1.0)

    # ---- SpareRoom ----
    if "spareroom" in SOURCES_ORDER and ENABLE_SPAREROOM:
        urls = build_spareroom_urls()
        for area, url in urls.items():
            print(f"\n📍 [SpareRoom] {area} → {url}")
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
            time.sleep(1.0)

    return new_listings

# ========= Main loop =========
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

            # jittered sleep (no round robin)
            sleep_duration = 3600 + random.randint(-300, 300)
            print(f"💤 Sleeping {sleep_duration} seconds…")
            await asyncio.sleep(sleep_duration)

        except Exception as e:
            print(f"🔥 Error: {e}")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
