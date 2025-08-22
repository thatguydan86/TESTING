# main.py

import os
import glob
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
from playwright.async_api import async_playwright

# --- Environment hardening / defaults ----------------------------------------

# Skip host dep checks in containers; we only need Chromium with --with-deps
os.environ.setdefault("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "1")

# Some hosts need this so Chrome can start headless as root
os.environ.setdefault("CHROME_NO_SANDBOX", "1")

# --- Boot banner --------------------------------------------------------------

print("🚀 Starting RentRadar…")

# ========= Config =============================================================

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://hook.eu2.make.com/6k1jjsv0khxbqbfplzv5ibt4d1wh3dwj")

# Enable/disable sources (deterministic)
ENABLE_RIGHTMOVE  = os.getenv("ENABLE_RIGHTMOVE", "true").lower() == "true"
ENABLE_ZOOPLA     = os.getenv("ENABLE_ZOOPLA", "true").lower() == "true"
ENABLE_OTM        = os.getenv("ENABLE_OTM", "true").lower() == "true"
ENABLE_SPAREROOM  = os.getenv("ENABLE_SPAREROOM", "true").lower() == "true"

# Deterministic execution order
SOURCES_ORDER = [s.strip().lower() for s in os.getenv(
    "SOURCES_ORDER",
    "rightmove,zoopla,onthemarket,spareroom"
).split(",") if s.strip()]

# Optional Zoopla residential proxy (highly recommended)
# Example: http://user:pass@p.webshare.io:80
ZOOPLA_PROXY = os.getenv("ZOOPLA_PROXY", "").strip()

# Areas (Rightmove API needs location IDs)
LOCATION_IDS: Dict[str, str] = {
    "Lincoln": "REGION^804",
    "Wirral": "REGION^93365",
    "Bridgwater": "REGION^212",
}

# Hard-coded Zoopla search URLs (3–4 bed, ≤£1250 where relevant)
Z_SEARCH: Dict[str, List[str]] = {
    "Lincoln": [
        # 3–4 beds, houses, ≤£1250
        "https://www.zoopla.co.uk/to-rent/houses/lincoln/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&price_frequency=per_month&price_max=1250&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Lincoln%2C%20Lincolnshire&search_source=to-rent",
    ],
    "Wirral": [
        # 3–4 beds, houses, ≤£1250
        "https://www.zoopla.co.uk/to-rent/houses/merseyside/wirral/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&price_frequency=per_month&price_max=1250&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Wirral%2C%20Merseyside&search_source=to-rent",
    ],
    "Bridgwater": [
        # 3–4 beds around Bridgewater Academy
        "https://www.zoopla.co.uk/to-rent/houses/schools/bridgewater-academy/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Bridgewater%20Academy%2C%20Somerset%2C%20TA6&search_source=to-rent",
    ],
}

# SpareRoom saved search links you provided
SPAREROOM_URLS: Dict[str, str] = {
    "Wirral":      "https://www.spareroom.co.uk/flatshare/?search_id=1381769273&mode=list",
    "Bridgwater":  "https://www.spareroom.co.uk/flatshare/?search_id=1381769450&mode=list",
    "Lincoln":     "https://www.spareroom.co.uk/flatshare/?search_id=1381769591&mode=list",
}

# Economics & filters
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

# Requests session & pacing
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
SESSION = requests.Session()

UA_POOL = [
    # Modern Chrome (desktop)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    # Alt variants
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
]

def _headers(host: str = "") -> Dict[str, str]:
    ua = random.choice(UA_POOL)
    h = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Upgrade-Insecure-Requests": "1",
    }
    if host:
        h["Referer"] = f"https://{host}/"
    return h

def _sleep():
    # No round-robin delays; just tiny think time to be polite
    time.sleep(random.uniform(0.6, 1.4))

# ======== Debug flags print ===================================================
print(f"Flags → ZOOPLA={ENABLE_ZOOPLA}, OTM={ENABLE_OTM}, SPAREROOM={ENABLE_SPAREROOM}, ORDER={SOURCES_ORDER}")

# ========= Economics helpers ==================================================
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

# ========= Cross-site de-duplication =========================================
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

# ========= Generic HTML fetcher (requests) ===================================
def get_soup(url: str, host_hint: str = "", use_proxy: bool = False) -> Optional[BeautifulSoup]:
    proxies = None
    if use_proxy and ZOOPLA_PROXY:
        proxies = {"http": ZOOPLA_PROXY, "https": ZOOPLA_PROXY}
    for _ in range(RETRY_ATTEMPTS):
        try:
            resp = SESSION.get(
                url,
                headers=_headers(host_hint),
                timeout=REQUEST_TIMEOUT,
                proxies=proxies,
                allow_redirects=True,
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

# ========= Rightmove (API) ====================================================
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
            resp = SESSION.get(url, params=params, headers=_headers("www.rightmove.co.uk"), timeout=REQUEST_TIMEOUT)
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

# ========= Zoopla (Playwright + HTML fallback) ================================

async def _launch_browser(pw):
    # Try system Chromium path first (Nixpacks often installs here)
    system_chromium = (
        next(iter(glob.glob("/root/.nix-profile/bin/chromium")), None)
        or next(iter(glob.glob("/nix/store/*-chromium-*/bin/chromium")), None)
        or next(iter(glob.glob("/nix/store/*-chromium-*/bin/chromium-browser")), None)
    )
    browser = None
    if system_chromium:
        print(f"Using system Chromium: {system_chromium}")
        proxy_opt = None
        if ZOOPLA_PROXY:
            proxy_opt = {"server": ZOOPLA_PROXY}
        browser = await pw.chromium.launch(
            headless=True,
            executable_path=system_chromium,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            proxy=proxy_opt
        )
        return browser

    # Fall back to Playwright’s bundled chromium (requires playwright install in image)
    proxy_opt = None
    if ZOOPLA_PROXY:
        proxy_opt = {"server": ZOOPLA_PROXY}
    browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"], proxy=proxy_opt)
    return browser

async def fetch_zoopla_playwright(url: str, area: str) -> List[Dict]:
    listings: List[Dict] = []
    try:
        pw = await async_playwright().start()
        browser = await _launch_browser(pw)
        context = await browser.new_context(
            locale="en-GB",
            user_agent=random.choice(UA_POOL),
            viewport={"width": 1366, "height": 900},
        )

        page = await context.new_page()

        # Block heavy assets
        async def route_handler(route):
            if any(ext in route.request.url for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf", ".otf")):
                await route.abort()
            else:
                await route.continue_()
        await page.route("**/*", route_handler)

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)

        # Click cookie banner if present
        try:
            await page.click("button:has-text('Accept all')", timeout=3000)
        except Exception:
            pass

        # Try client-rendered list links
        links = set()
        # Newer Zoopla marks listing anchors with /to-rent/details/
        for sel in ["a[href*='/to-rent/details/']", "a[href*='/to-rent/property/']", "a[href*='/to-rent/']"]:
            anchors = await page.eval_on_selector_all(sel, "els => els.map(e => e.href)")
            for a in anchors or []:
                if "/to-rent/" in a:
                    links.add(a)

        print(f"🔎 Zoopla {area}: PW found {len(links)} links")

        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("a[href*='/to-rent/details/'], a[href*='/to-rent/property/']")
        # Parse ~50 items max
        seen = set()
        for a in cards[:50]:
            href = a.get("href") or ""
            abs_url = href if href.startswith("http") else urljoin("https://www.zoopla.co.uk", href)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            text = a.get_text(" ", strip=True)

            # Peek price/address near anchor
            parent = a.find_parent()
            price_txt, address = "", ""
            if parent:
                parent_text = parent.get_text(" ", strip=True).lower()
                m_price = re.search(r"£\s*\d[\d,]*\s*(pcm|pw|per week|per month)", parent_text)
                if m_price:
                    price_txt = m_price.group(0)
                addr_el = parent.find(string=re.compile(r"[A-Za-z].*,"))
                if addr_el:
                    address = str(addr_el).strip()

            amt, freq = parse_price_text(price_txt)
            rent_pcm = to_pcm(amt, freq)
            mb = re.search(r"(\d+)\s*bed", text.lower())
            beds = int(mb.group(1)) if mb else MIN_BEDS
            if beds < MIN_BEDS or beds > MAX_BEDS:
                continue
            if rent_pcm is not None and rent_pcm < MIN_RENT:
                continue
            rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT
            baths = max(MIN_BATHS, 1)

            p = calculate_profits(rent_pcm, area, beds)
            p70 = p["profit_70"]
            score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
            rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

            listings.append({
                "id": norm_id("zoopla", abs_url),
                "source": "zoopla",
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

        await context.close()
        await browser.close()
        await pw.stop()
    except Exception as e:
        print(f"⚠️ Zoopla Playwright error: {e}")
    return listings

def fetch_zoopla_html(url: str, area: str) -> List[Dict]:
    soup = get_soup(url, host_hint="www.zoopla.co.uk", use_proxy=bool(ZOOPLA_PROXY))
    if not soup:
        return []
    listings: List[Dict] = []

    anchors = soup.select("a[href*='/to-rent/details/'], a[href*='/to-rent/property/']")
    seen = set()
    for a in anchors[:50]:
        href = a.get("href") or ""
        abs_url = href if href.startswith("http") else urljoin("https://www.zoopla.co.uk", href)
        if abs_url in seen:
            continue
        seen.add(abs_url)

        # Try to harvest basic info from surrounding card
        parent = a.find_parent()
        price_txt, address, text = "", "", a.get_text(" ", strip=True)

        if parent:
            parent_text = parent.get_text(" ", strip=True).lower()
            m_price = re.search(r"£\s*\d[\d,]*\s*(pcm|pw|per week|per month)", parent_text)
            if m_price:
                price_txt = m_price.group(0)
            addr_el = parent.find(string=re.compile(r"[A-Za-z].*,"))
            if addr_el:
                address = str(addr_el).strip()

        amt, freq = parse_price_text(price_txt)
        rent_pcm = to_pcm(amt, freq)
        mb = re.search(r"(\d+)\s*bed", text.lower())
        beds = int(mb.group(1)) if mb else MIN_BEDS
        if beds < MIN_BEDS or beds > MAX_BEDS:
            continue
        if rent_pcm is not None and rent_pcm < MIN_RENT:
            continue
        rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT
        baths = max(MIN_BATHS, 1)

        p = calculate_profits(rent_pcm, area, beds)
        p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70 / GOOD_PROFIT_TARGET) * 10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET * 0.7 else "🔴")

        listings.append({
            "id": norm_id("zoopla", abs_url),
            "source": "zoopla",
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

# ========= OnTheMarket (requests) ============================================
def build_otm_urls() -> Dict[str, str]:
    return {area: f"https://www.onthemarket.com/to-rent/property/{area.lower().replace(' ', '-')}/"
            for area in LOCATION_IDS.keys()}

def fetch_otm_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url, host_hint="www.onthemarket.com")
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

# ========= SpareRoom (requests; whole property via saved search) ==============
def fetch_spareroom_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url, host_hint="www.spareroom.co.uk")
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

# ========= Orchestrator (async) ==============================================

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
            _sleep()

    # ---- Zoopla (Playwright → HTML fallback) ----
    if "zoopla" in SOURCES_ORDER and ENABLE_ZOOPLA:
        if ZOOPLA_PROXY:
            print("\n🔗 Using residential proxy for Zoopla.")
        else:
            print("\n⚠️ Zoopla is much more reliable with a residential proxy. Proceeding without one…")

        for area, urls in Z_SEARCH.items():
            for url in urls:
                print(f"\n📍 [Zoopla] {area} → {url}")
                listings = await fetch_zoopla_playwright(url, area)
                if not listings:
                    # Playwright failed or returned nothing → try HTML with proxy + strong headers
                    listings = fetch_zoopla_html(url, area)

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
                await asyncio.sleep(0.6)

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
            _sleep()

    # ---- SpareRoom ----
    if "spareroom" in SOURCES_ORDER and ENABLE_SPAREROOM:
        for area, url in SPAREROOM_URLS.items():
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
            _sleep()

    return new_listings

# ========= Main loop ==========================================================

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

            # Sleep ~1 hour (no round-robin jitter needed beyond this)
            sleep_duration = 3600
            print(f"💤 Sleeping {sleep_duration} seconds…")
            await asyncio.sleep(sleep_duration)

        except Exception as e:
            print(f"🔥 Error: {e}")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
