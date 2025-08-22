import os
os.environ.setdefault("PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS", "1")

import asyncio
import time
import random
import re
import hashlib
import difflib
import glob
import requests
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import urljoin, quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

print("🚀 Starting RentRadar…")

# ========= Config =========
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://hook.eu2.make.com/6k1jjsv0khxbqbfplzv5ibt4d1wh3dwj")

# Enable/disable sources (deterministic, no round robin)
ENABLE_RIGHTMOVE = os.getenv("ENABLE_RIGHTMOVE", "true").lower() == "true"
ENABLE_ZOOPLA    = os.getenv("ENABLE_ZOOPLA", "true").lower() == "true"
ENABLE_OTM       = os.getenv("ENABLE_OTM", "true").lower() == "true"
ENABLE_SPAREROOM = os.getenv("ENABLE_SPAREROOM", "true").lower() == "true"

# Deterministic execution order
SOURCES_ORDER = [s.strip().lower() for s in os.getenv(
    "SOURCES_ORDER",
    "rightmove,zoopla,onthemarket,spareroom"
).split(",") if s.strip()]

# Per‑listing optional jitter before POST (kept by request)
POST_JITTER_MAX_SEC = float(os.getenv("POST_JITTER_MAX_SEC", "0"))  # e.g. 0.8 for up to ~800ms

# Rightmove location IDs
LOCATION_IDS: Dict[str, str] = {
    "Lincoln": "REGION^804",
    "Wirral": "REGION^93365",
    "Bridgwater": "REGION^212",
}

# Hardcoded search URLs (Option B)
SEARCH_URLS: Dict[str, Dict[str, str]] = {
    "zoopla": {
        # 3–4 beds, houses, filtered
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
    "spareroom": {
        # Saved whole‑property searches (user provided)
        "Wirral":      "https://www.spareroom.co.uk/flatshare/?search_id=1381769273&mode=list",
        "Bridgwater":  "https://www.spareroom.co.uk/flatshare/?search_id=1381769450&mode=list",
        "Lincoln":     "https://www.spareroom.co.uk/flatshare/?search_id=1381769591&mode=list",
    },
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
REQUEST_COOLDOWN_SEC = (1.0, 2.0)
SESSION = requests.Session()

UA_POOL = [
    # desktop-like
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
        if POST_JITTER_MAX_SEC > 0:
            time.sleep(random.uniform(0, POST_JITTER_MAX_SEC))
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

# ========= Zoopla (Playwright; resilient) =========
def _zoopla_mobile_ua() -> str:
    # Many variants; randomize among a few modern Android Chrome strings
    uas = [
        "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Mobile Safari/537.36",
    ]
    return random.choice(uas)

async def _block_heavy(route):
    url = route.request.url
    if any(ext in url for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf", ".otf", ".mp4", ".avi", ".mov")):
        try:
            await route.abort()
            return
        except:
            pass
    try:
        await route.continue_()
    except:
        pass

async def zoopla_accept_cookies(page):
    # Try to accept consent banners
    selectors = [
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button[aria-label*='Accept']",
        "button:has-text('Got it')",
    ]
    for sel in selectors:
        try:
            if await page.locator(sel).count():
                await page.locator(sel).first.click(timeout=1000)
                await page.wait_for_timeout(200)
                break
        except:
            pass

def _to_mobile_zoopla(url: str) -> str:
    if "//m.zoopla.co.uk" in url:
        return url
    return url.replace("://www.zoopla.co.uk", "://m.zoopla.co.uk")

async def zoopla_launch_browser(pw):
    proxy = os.getenv("ZOOPLA_PROXY")  # optional: http://user:pass@host:port
    system_chromium = (
        "/root/.nix-profile/bin/chromium"
        if os.path.exists("/root/.nix-profile/bin/chromium") else
        next(iter(glob.glob("/nix/store/*-chromium-*/bin/chromium")), None) or
        next(iter(glob.glob("/nix/store/*-chromium-*/bin/chromium-browser")), None)
    )
    args = [
        "--no-sandbox", "--disable-setuid-sandbox", "--no-zygote",
        "--disable-dev-shm-usage", "--disable-gpu", "--disable-software-rasterizer",
        "--disable-features=AutomationControlled", "--disable-blink-features=AutomationControlled",
        "--lang=en-GB", "--window-size=390,1600",
    ]
    launch_kwargs = {"headless": True, "args": args}
    if system_chromium:
        print(f"Using system Chromium: {system_chromium}")
        launch_kwargs["executable_path"] = system_chromium
    if proxy:
        print("Using proxy for Zoopla")
        launch_kwargs["proxy"] = {"server": proxy}
    return await pw.chromium.launch(**launch_kwargs)

async def zoopla_new_context(browser):
    ua = _zoopla_mobile_ua()
    ctx = await browser.new_context(
        locale="en-GB",
        timezone_id="Europe/London",
        user_agent=ua,
        viewport={"width": 390, "height": 1600},
    )
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
        Object.defineProperty(navigator, 'platform', {get: () => 'Linux armv8l'});
        window.chrome = window.chrome || { runtime: {} };
    """)
    return ctx

async def _zoopla_warmup(context):
    p = await context.new_page()
    p.set_default_timeout(20000)
    try:
        await p.route("**/*", _block_heavy)
        await p.set_extra_http_headers({
            "Accept-Language": "en-GB,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
        })
        await p.goto("https://m.zoopla.co.uk/", wait_until="domcontentloaded")
        await zoopla_accept_cookies(p)
        await p.wait_for_timeout(300)
    finally:
        try: await p.close()
        except: pass

async def zoopla_extract_links(page) -> List[str]:
    links: List[str] = []
    try:
        if await page.locator("a[data-testid='listing-card-link']").count():
            hrefs = await page.eval_on_selector_all("a[data-testid='listing-card-link']", "els => els.map(e => e.href)")
            links.extend(hrefs or [])
        if not links and await page.locator("a[href*='/to-rent/details/']").count():
            hrefs = await page.eval_on_selector_all("a[href*='/to-rent/details/']", "els => els.map(e => e.href)")
            links.extend(hrefs or [])
    except:
        pass

    uniq, seen = [], set()
    for u in links:
        if not u:
            continue
        if not u.startswith("http"):
            u = "https://www.zoopla.co.uk" + u
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
    target = _to_mobile_zoopla(url)

    async def run_once() -> Tuple[List[str], Optional[str]]:
        page = await context.new_page()
        page.set_default_timeout(25000)
        page.set_default_navigation_timeout(25000)
        await page.route("**/*", _block_heavy)
        await page.set_extra_http_headers({
            "Accept-Language": "en-GB,en;q=0.9",
            "Sec-Ch-Ua": '"Chromium";v="126", "Not=A?Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": '"Android"',
            "Upgrade-Insecure-Requests": "1",
        })
        try:
            await page.goto(target, wait_until="domcontentloaded")
            await zoopla_accept_cookies(page)

            for _ in range(6):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(random.randint(350, 650))
                try:
                    btn = page.locator("button:has-text('Show more'), button:has-text('Load more')")
                    if await btn.count():
                        await btn.first.click(timeout=1000)
                        await page.wait_for_timeout(random.randint(400, 700))
                except:
                    pass

            links = await zoopla_extract_links(page)
            return links, None
        except Exception as e:
            return [], str(e)
        finally:
            try: await page.close()
            except: pass

    try:
        await _zoopla_warmup(context)
    except:
        pass

    attempts = 3
    links: List[str] = []
    for i in range(1, attempts + 1):
        lnk, err = await run_once()
        if lnk:
            links = lnk
            break
        print(f"⚠️ Zoopla attempt {i}/{attempts} failed for {target}: {err}")
        if "has been closed" in (err or "").lower():
            raise RuntimeError("ctx-closed")
        await asyncio.sleep(random.uniform(0.6, 1.2))

    print(f"🔎 Zoopla {area}: PW found {len(links)} links")

    # We return minimal stubs; details page parsing can be added later
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
    print(f"🧮 Zoopla {area}: parsed {len(listings)} listings")
    return listings

# ========= OnTheMarket (requests) =========
def build_otm_urls() -> Dict[str, str]:
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

# ========= SpareRoom (requests; whole property via saved search) =========
def build_spareroom_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("spareroom", {})
    if cfg:
        return cfg
    # generic fallback if needed
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
            continue  # usually HMO rooms; skip
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
            time.sleep(0.6)

    # ---- Zoopla (Playwright) ----
    if "zoopla" in SOURCES_ORDER and ENABLE_ZOOPLA:
        print("\n🧭 Launching Playwright for Zoopla…")
        async with async_playwright() as pw:
            for cycle in range(1, 4):
                browser = None
                context = None
                try:
                    browser = await zoopla_launch_browser(pw)
                    context = await zoopla_new_context(browser)
                    urls = SEARCH_URLS.get("zoopla", {})
                    for area, url in urls.items():
                        print(f"\n📍 [Zoopla] {area} → {url}")
                        try:
                            listings = await fetch_zoopla_playwright(context, url, area)
                        except RuntimeError as e_ctx:
                            if "ctx-closed" in str(e_ctx):
                                raise  # relaunch browser/context
                            listings = []
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
                        await asyncio.sleep(0.3)
                    try: await context.close()
                    except: pass
                    try: await browser.close()
                    except: pass
                    break
                except Exception as e:
                    print(f"⚠️ Zoopla cycle {cycle}/3 failed: {e}")
                    try: 
                        if context: await context.close()
                    except: pass
                    try:
                        if browser: await browser.close()
                    except: pass
                    await asyncio.sleep(1.0)

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
            time.sleep(0.6)

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
            time.sleep(0.6)

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

            sleep_duration = 3600 + random.randint(-300, 300)
            print(f"💤 Sleeping {sleep_duration} seconds…")
            await asyncio.sleep(sleep_duration)

        except Exception as e:
            print(f"🔥 Error: {e}")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
