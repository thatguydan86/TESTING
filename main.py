# main.py
import os, re, time, random, asyncio, hashlib, difflib, glob
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import urljoin, quote_plus, urlparse
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# -----------------------------
# Startup + env
# -----------------------------
print("🚀 Starting RentRadar…")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://hook.eu2.make.com/6k1jjsv0khxbqbfplzv5ibt4d1wh3dwj")

ENABLE_RIGHTMOVE = os.getenv("ENABLE_RIGHTMOVE", "true").lower() == "true"
ENABLE_ZOOPLA    = os.getenv("ENABLE_ZOOPLA", "true").lower() == "true"
ENABLE_OTM       = os.getenv("ENABLE_OTM", "true").lower() == "true"
ENABLE_SPAREROOM = os.getenv("ENABLE_SPAREROOM", "true").lower() == "true"

SOURCES_ORDER = [s.strip().lower() for s in os.getenv(
    "SOURCES_ORDER", "rightmove,zoopla,onthemarket,spareroom"
).split(",") if s.strip()]

# proxy envs
ZOOPLA_PROXY = os.getenv("ZOOPLA_PROXY", "").strip()
ZOOPLA_PROXY_LIST = [p.strip() for p in os.getenv("ZOOPLA_PROXY_LIST", "").split(",") if p.strip()]
USE_HTML_FALLBACK = os.getenv("ZOOPLA_HTML_FALLBACK", "true").lower() == "true"

# Browser timeouts / retries
PW_NAV_TIMEOUT_MS = int(os.getenv("PW_NAV_TIMEOUT_MS", "120000"))  # 120s
ZOOPLA_PW_RETRIES = int(os.getenv("ZOOPLA_PW_RETRIES", "3"))

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

# Rightmove area IDs
LOCATION_IDS: Dict[str, str] = {
    "Lincoln": "REGION^804",
    "Wirral": "REGION^93365",
    "Bridgwater": "REGION^212",
}

# Your curated search links
SEARCH_URLS: Dict[str, Dict[str, str]] = {
    "zoopla": {
        # 3–4 bed Lincoln / Wirral / Bridgwater (schools page)
        "Lincoln":     "https://www.zoopla.co.uk/to-rent/houses/lincoln/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&price_frequency=per_month&price_max=1250&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Lincoln%2C%20Lincolnshire&search_source=to-rent",
        "Wirral":      "https://www.zoopla.co.uk/to-rent/houses/merseyside/wirral/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&price_frequency=per_month&price_max=1250&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Wirral%2C%20Merseyside&search_source=to-rent",
        "Bridgwater":  "https://www.zoopla.co.uk/to-rent/houses/schools/bridgewater-academy/?beds_max=4&beds_min=3&is_retirement_home=false&is_shared_accommodation=false&is_student_accommodation=false&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&q=Bridgewater%20Academy%2C%20Somerset%2C%20TA6&search_source=to-rent",
    },
    "spareroom": {
        "Wirral":      "https://www.spareroom.co.uk/flatshare/?search_id=1381769273&mode=list",
        "Bridgwater":  "https://www.spareroom.co.uk/flatshare/?search_id=1381769450&mode=list",
        "Lincoln":     "https://www.spareroom.co.uk/flatshare/?search_id=1381769591&mode=list",
    }
}

# Requests session (we’ll add proxy per-request if needed)
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
REQUEST_COOLDOWN_SEC = (1.0, 2.0)
SESSION = requests.Session()

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
]

def _headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

def _sleep():
    time.sleep(random.uniform(*REQUEST_COOLDOWN_SEC))

print(f"Flags → ZOOPLA={ENABLE_ZOOPLA}, OTM={ENABLE_OTM}, SPAREROOM={ENABLE_SPAREROOM}, ORDER={SOURCES_ORDER}")

# -----------------------------
# Helpers / economics
# -----------------------------
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
    if not m: return None, ""
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

# -----------------------------
# Cross-site de-duplication
# -----------------------------
SOURCE_PRIORITY = {"rightmove": 4, "onthemarket": 3, "zoopla": 2, "spareroom": 1}
UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?)\s?(\d[A-Z]{2})\b", re.I)
HOUSE_NO_RE = re.compile(r"\b(\d+[A-Z]?)\b")
NOISE = {"to let", "to-rent", "for rent", "new instruction", "available now", "available immediately"}
ABBR = {"road":"rd","rd.":"rd","street":"st","st.":"st","avenue":"ave","ave.":"ave","drive":"dr","dr.":"dr",
        "lane":"ln","ln.":"ln","crescent":"cres","cres.":"cres","place":"pl","pl.":"pl","close":"cl","cl.":"cl",
        "court":"ct","ct.":"ct","terrace":"ter","terr.":"ter"}

def normalize_street(s: str) -> str:
    if not s: return ""
    s = s.lower()
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    toks = [t for t in s.split() if t not in NOISE]
    return " ".join(ABBR.get(t,t) for t in toks).strip()

def extract_postcode(s: str) -> str:
    if not s: return ""
    m = UK_POSTCODE_RE.search(s.upper())
    return f"{m.group(1)}{m.group(2)}" if m else ""

def extract_house_no(s: str) -> str:
    if not s: return ""
    m = HOUSE_NO_RE.search(s)
    return m.group(1) if m else ""

def canonical_key(address: str) -> tuple:
    pc = extract_postcode(address)
    hn = extract_house_no((address or "").lower())
    street = normalize_street(address or "")
    street_wo_no = " ".join(t for t in street.split() if t != hn.lower())
    return (pc, hn.lower(), street_wo_no)

def fuzzy_same(a_addr: str, b_addr: str, rent_a: int, rent_b: int, beds_a: int, beds_b: int) -> bool:
    pc_a, _, street_a = canonical_key(a_addr)
    pc_b, _, street_b = canonical_key(b_addr)
    if not pc_a or pc_a != pc_b: return False
    try:
        if rent_a and rent_b and abs(rent_a - rent_b) / max(rent_a, rent_b) > 0.08:
            return False
    except Exception: pass
    if beds_a and beds_b and beds_a != beds_b: return False
    ratio = difflib.SequenceMatcher(None, street_a, street_b).ratio()
    return ratio >= 0.92

def choose_preferred(existing: Dict, candidate: Dict) -> Dict:
    a = SOURCE_PRIORITY.get(existing.get("source",""),0)
    b = SOURCE_PRIORITY.get(candidate.get("source",""),0)
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
        if k[0] != pc: continue
        if fuzzy_same(addr, v.get("address",""), listing.get("rent_pcm"), v.get("rent_pcm"),
                      listing.get("bedrooms"), v.get("bedrooms")):
            return True, v, k
    return False, None, key

# -----------------------------
# HTTP helpers (requests)
# -----------------------------
def requests_proxies_from_url(proxy_url: str) -> Dict[str, str]:
    if not proxy_url: return {}
    return {"http": proxy_url, "https": proxy_url}

def get_soup(url: str, proxy_url: str = "") -> Optional[BeautifulSoup]:
    for _ in range(RETRY_ATTEMPTS):
        try:
            proxies = requests_proxies_from_url(proxy_url)
            resp = SESSION.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT, proxies=proxies if proxy_url else None)
            if resp.status_code != 200:
                print(f"⚠️ GET {resp.status_code} {url}")
                _sleep(); continue
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            print(f"⚠️ HTML fetch error: {e} ({url})"); _sleep()
    return None

# -----------------------------
# Rightmove (API)
# -----------------------------
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
                print(f"⚠️ Rightmove API {resp.status_code} for {location_id}"); _sleep(); continue
            return resp.json().get("properties", [])
        except Exception as e:
            print(f"⚠️ Rightmove exception: {e}"); _sleep()
    return []

def filter_rightmove(properties: List[Dict], area: str) -> List[Dict]:
    results = []
    for prop in properties:
        try:
            beds = prop.get("bedrooms"); baths = prop.get("bathrooms") or 0
            rent = prop.get("price",{}).get("amount"); subtype = (prop.get("propertySubType") or "House").upper()
            address = prop.get("displayAddress", "Unknown")
            if not beds or not rent: continue
            if beds < MIN_BEDS or beds > MAX_BEDS: continue
            if baths < MIN_BATHS: continue
            max_rent_allowed = MAX_RENTS.get(beds, 1500)
            if rent < MIN_RENT or rent > max_rent_allowed: continue
            p = calculate_profits(rent, area, beds); p70 = p["profit_70"]
            score10 = round(max(0, min(10, (p70/GOOD_PROFIT_TARGET)*10)), 1)
            rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET*0.7 else "🔴")
            url = f"https://www.rightmove.co.uk{prop.get('propertyUrl')}"
            results.append({
                "id": prop.get("id"), "source": "rightmove", "area": area, "address": address,
                "rent_pcm": rent, "bedrooms": beds, "bathrooms": baths,
                "propertySubType": subtype.title(), "url": url,
                "night_rate": p["night_rate"], "occ_rate": p["occ_rate"], "bills": p["total_bills"],
                "profit_50": p["profit_50"], "profit_70": p70, "profit_100": p["profit_100"],
                "target_profit_70": GOOD_PROFIT_TARGET, "score10": score10, "rag": rag,
            })
        except Exception:
            continue
    return results

# -----------------------------
# OnTheMarket (requests)
# -----------------------------
def build_otm_urls() -> Dict[str, str]:
    return {area: f"https://www.onthemarket.com/to-rent/property/{area.lower().replace(' ','-')}/"
            for area in LOCATION_IDS.keys()}

def fetch_otm_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url); 
    if not soup: return []
    results: List[Dict] = []
    cards = soup.select("[data-testid*=propertyCard], article, li")
    for card in cards[:50]:
        a = card.find("a", href=re.compile(r"/details/|/to-rent/property/")); 
        if not a: continue
        href = a.get("href") or ""; abs_url = href if href.startswith("http") else urljoin("https://www.onthemarket.com", href)
        text = card.get_text(" ", strip=True).lower()
        price_el = re.search(r"£\s*\d[\d,]*\s*(pcm|pw|per week|per month)", text)
        amt, freq = parse_price_text(price_el.group(0) if price_el else "")
        rent_pcm = to_pcm(amt, freq)
        mb = re.search(r"(\d+)\s*bed", text); beds = int(mb.group(1)) if mb else None
        if beds is not None and (beds < MIN_BEDS or beds > MAX_BEDS): continue
        if rent_pcm is not None and rent_pcm < MIN_RENT: continue
        address = ""
        addr_m = re.search(r"[A-Za-z].*,.*", card.get_text("\n", strip=True))
        if addr_m: address = addr_m.group(0).strip()
        beds = beds if beds is not None else MIN_BEDS
        baths = max(MIN_BATHS, 1)
        rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT
        p = calculate_profits(rent_pcm, area, beds); p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70/GOOD_PROFIT_TARGET)*10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET*0.7 else "🔴")
        results.append({
            "id": norm_id("onthemarket", abs_url), "source":"onthemarket", "area":area,
            "address": address or "Unknown", "rent_pcm": rent_pcm, "bedrooms": beds, "bathrooms": baths,
            "propertySubType":"Property", "url": abs_url,
            "night_rate": p["night_rate"], "occ_rate": p["occ_rate"], "bills": p["total_bills"],
            "profit_50": p["profit_50"], "profit_70": p70, "profit_100": p["profit_100"],
            "target_profit_70": GOOD_PROFIT_TARGET, "score10": score10, "rag": rag,
        })
    return results

# -----------------------------
# SpareRoom (requests)
# -----------------------------
def build_spareroom_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("spareroom", {})
    if cfg: return cfg
    return {area: f"https://www.spareroom.co.uk/flatshare/?search_type=offered&property_type=property&location={quote_plus(area)}"
            for area in LOCATION_IDS.keys()}

def fetch_spareroom_from_url(url: str, area: str) -> List[Dict]:
    soup = get_soup(url); 
    if not soup: return []
    results: List[Dict] = []
    cards = soup.select("li.listing-result, .panel-listing-result, .results_content .listing")
    for c in cards[:40]:
        a = c.find("a", href=True); 
        if not a: continue
        href = a["href"]; abs_url = href if href.startswith("http") else urljoin("https://www.spareroom.co.uk", href)
        text = c.get_text(" ", strip=True)
        mprice = re.search(r"£\s*\d[\d,]*\s*(pw|pcm|per week|per month)", text.lower())
        amt, freq = parse_price_text(mprice.group(0) if mprice else "")
        rent_pcm = to_pcm(amt, freq)
        mb = re.search(r"(\d+)\s*bed", text.lower())
        if not mb: continue
        beds = int(mb.group(1))
        if beds < MIN_BEDS or beds > MAX_BEDS: continue
        if rent_pcm is not None and rent_pcm < MIN_RENT: continue
        addr_m = re.search(r"[A-Za-z].*,.*", text); address = addr_m.group(0).strip() if addr_m else ""
        baths = max(MIN_BATHS, 1)
        rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT
        p = calculate_profits(rent_pcm, area, beds); p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70/GOOD_PROFIT_TARGET)*10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET*0.7 else "🔴")
        results.append({
            "id": norm_id("spareroom", abs_url), "source":"spareroom", "area":area,
            "address": address or "Unknown", "rent_pcm": rent_pcm, "bedrooms": beds, "bathrooms": baths,
            "propertySubType":"Property", "url": abs_url,
            "night_rate": p["night_rate"], "occ_rate": p["occ_rate"], "bills": p["total_bills"],
            "profit_50": p["profit_50"], "profit_70": p70, "profit_100": p["profit_100"],
            "target_profit_70": GOOD_PROFIT_TARGET, "score10": score10, "rag": rag,
        })
    return results

# -----------------------------
# Zoopla - proxy utils & stealth
# -----------------------------
def parse_proxy(proxy_url: str) -> Optional[Dict[str, str]]:
    """Return Playwright proxy dict or None."""
    if not proxy_url: return None
    parsed = urlparse(proxy_url)
    hostport = f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
    d = {"server": f"{parsed.scheme}://{hostport}"}
    if parsed.username:
        d["username"] = parsed.username
    if parsed.password:
        d["password"] = parsed.password
    return d

STEALTH_JS = """
// basic stealth patches
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
window.chrome = { runtime: {} };
Object.defineProperty(navigator, 'languages', {get: () => ['en-GB','en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
const origQuery = window.navigator.permissions && window.navigator.permissions.query;
if (origQuery) {
  window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
      Promise.resolve({ state: Notification.permission }) :
      origQuery(parameters)
  );
}
"""

def system_chromium_path() -> Optional[str]:
    # Prefer known Nix paths first
    candidates = [
        "/root/.nix-profile/bin/chromium",
        "/nix/store/*-chromium-*/bin/chromium",
        "/nix/store/*-chromium-*/bin/chromium-browser",
    ]
    for c in candidates:
        if "*" in c:
            paths = glob.glob(c)
            if paths: return paths[0]
        else:
            if os.path.exists(c): return c
    return None

async def zoopla_collect_with_playwright(area: str, url: str) -> List[Dict]:
    """Try Playwright with stealth + proxy rotation. Returns parsed listings."""
    listings: List[Dict] = []
    proxies = ZOOPLA_PROXY_LIST[:] if ZOOPLA_PROXY_LIST else ([ZOOPLA_PROXY] if ZOOPLA_PROXY else [])
    if not proxies:
        print("⚠️ Zoopla blocked without a residential proxy. Set ZOOPLA_PROXY or ZOOPLA_PROXY_LIST.")
        return []

    chromium_path = system_chromium_path()
    if not chromium_path:
        print("⚠️ System Chromium not found; cannot use Playwright. Falling back to HTML.")
        return []

    for attempt in range(1, ZOOPLA_PW_RETRIES + 1):
        proxy = parse_proxy(random.choice(proxies))
        print(f"🔗 Using residential proxy for Zoopla (attempt {attempt}/{ZOOPLA_PW_RETRIES}).")
        pw = await async_playwright().start()
        browser = None
        try:
            browser = await pw.chromium.launch(headless=True, executable_path=chromium_path, args=["--no-sandbox"])
            context = await browser.new_context(locale="en-GB",
                                               user_agent=random.choice(UA_POOL),
                                               proxy=proxy if proxy else None)
            await context.add_init_script(STEALTH_JS)
            page = await context.new_page()
            # Block heavy assets
            async def route_handler(route):
                if any(ext in route.request.url for ext in (".png",".jpg",".jpeg",".gif",".webp",".svg",".woff",".woff2",".ttf",".otf",".mp4",".avi",".webm",".json")):
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", route_handler)

            print(f"\n📍 [Zoopla] {area} → {url}\nUsing system Chromium: {chromium_path}")
            await page.goto(url, wait_until="domcontentloaded", timeout=PW_NAV_TIMEOUT_MS)
            # try to wait for a key container
            try:
                await page.wait_for_selector("a[href*='/to-rent/details/']", timeout=8000)
            except Exception:
                pass

            # 1) Try extracting from DOM
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            anchors = soup.select("a[href*='/to-rent/details/']")
            seen = set()
            links = []
            for a in anchors:
                href = a.get("href") or ""
                if "/to-rent/details" not in href: continue
                abs_url = href if href.startswith("http") else urljoin("https://www.zoopla.co.uk", href)
                if abs_url in seen: continue
                seen.add(abs_url); links.append(abs_url)

            if not links:
                # 2) Try minimal JS to expand results if needed
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1500)
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    anchors = soup.select("a[href*='/to-rent/details/']")
                    for a in anchors:
                        href = a.get("href") or ""
                        if "/to-rent/details" not in href: continue
                        abs_url = href if href.startswith("http") else urljoin("https://www.zoopla.co.uk", href)
                        if abs_url in seen: continue
                        seen.add(abs_url); links.append(abs_url)
                except Exception:
                    pass

            print(f"🔎 Zoopla {area}: PW found {len(links)} links")
            # Rough parsing for price/beds by scanning nearby text
            for abs_url in links[:50]:
                # Minimal fields; full details can be scraped on detail pages later if needed
                # Here we guess beds from URL slug when present, else default MIN_BEDS
                beds = MIN_BEDS
                mbed = re.search(r"/(\d+)-bed", abs_url)
                if mbed:
                    try: beds = int(mbed.group(1))
                    except: pass
                if beds < MIN_BEDS or beds > MAX_BEDS: continue
                rent_pcm = None  # unknown at list view; allow through, compute profit with fallback
                baths = max(MIN_BATHS, 1)
                address = "Unknown"

                # Conservative: if we can't see rent here, skip applying rent filters strictly
                rent_pcm = rent_pcm if rent_pcm is not None else MIN_RENT
                p = calculate_profits(rent_pcm, area, beds); p70 = p["profit_70"]
                score10 = round(max(0, min(10, (p70/GOOD_PROFIT_TARGET)*10)), 1)
                rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET*0.7 else "🔴")

                listings.append({
                    "id": norm_id("zoopla", abs_url), "source":"zoopla", "area":area,
                    "address": address, "rent_pcm": rent_pcm, "bedrooms": beds, "bathrooms": baths,
                    "propertySubType":"Property", "url": abs_url,
                    "night_rate": p["night_rate"], "occ_rate": p["occ_rate"], "bills": p["total_bills"],
                    "profit_50": p["profit_50"], "profit_70": p70, "profit_100": p["profit_100"],
                    "target_profit_70": GOOD_PROFIT_TARGET, "score10": score10, "rag": rag,
                })

            await context.close()
            await browser.close()
            await pw.stop()
            if listings:
                return listings
            # if zero links, try next attempt/proxy
        except Exception as e:
            print(f"⚠️ Zoopla Playwright error: {e}")
        finally:
            try:
                if browser: await browser.close()
            except: pass
            try:
                await pw.stop()
            except: pass
    return listings

def build_zoopla_urls() -> Dict[str, str]:
    cfg = SEARCH_URLS.get("zoopla", {})
    if cfg: return cfg
    return {area: f"https://www.zoopla.co.uk/to-rent/property/{area.lower().replace(' ','-')}/"
            for area in LOCATION_IDS.keys()}

def zoopla_extract_from_html(url: str, area: str, proxy_url: str = "") -> List[Dict]:
    soup = get_soup(url, proxy_url=proxy_url)
    if not soup: return []
    anchors = soup.select("a[href*='/to-rent/details/']")
    seen, links = set(), []
    for a in anchors[:80]:
        href = a.get("href") or ""
        if "/to-rent/details" not in href: continue
        abs_url = href if href.startswith("http") else urljoin("https://www.zoopla.co.uk", href)
        if abs_url in seen: continue
        seen.add(abs_url); links.append(abs_url)
    print(f"🔎 Zoopla {area} HTML: {len(links)} links")
    listings: List[Dict] = []
    for abs_url in links[:50]:
        beds = MIN_BEDS
        mbed = re.search(r"/(\d+)-bed", abs_url)
        if mbed:
            try: beds = int(mbed.group(1))
            except: pass
        if beds < MIN_BEDS or beds > MAX_BEDS: continue
        rent_pcm = MIN_RENT
        baths = max(MIN_BATHS, 1)
        address = "Unknown"
        p = calculate_profits(rent_pcm, area, beds); p70 = p["profit_70"]
        score10 = round(max(0, min(10, (p70/GOOD_PROFIT_TARGET)*10)), 1)
        rag = "🟢" if p70 >= GOOD_PROFIT_TARGET else ("🟡" if p70 >= GOOD_PROFIT_TARGET*0.7 else "🔴")
        listings.append({
            "id": norm_id("zoopla", abs_url), "source":"zoopla", "area":area,
            "address": address, "rent_pcm": rent_pcm, "bedrooms": beds, "bathrooms": baths,
            "propertySubType":"Property", "url": abs_url,
            "night_rate": p["night_rate"], "occ_rate": p["occ_rate"], "bills": p["total_bills"],
            "profit_50": p["profit_50"], "profit_70": p70, "profit_100": p["profit_100"],
            "target_profit_70": GOOD_PROFIT_TARGET, "score10": score10, "rag": rag,
        })
    return listings

# -----------------------------
# Orchestrator
# -----------------------------
async def run_once(seen_ids: Set[str], cross_registry: Dict[tuple, Dict]) -> List[Dict]:
    new_listings: List[Dict] = []

    # Rightmove
    if "rightmove" in SOURCES_ORDER and ENABLE_RIGHTMOVE:
        for area, loc_id in LOCATION_IDS.items():
            print(f"\n📍 [Rightmove] {area}…")
            raw = fetch_rightmove(loc_id)
            for listing in filter_rightmove(raw, area):
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    pref = choose_preferred(existing, listing); cross_registry[key] = pref
                    if pref is existing: continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids: continue
                seen_ids.add(listing["id"]); new_listings.append(listing)
            time.sleep(1.0)

    # Zoopla
    if "zoopla" in SOURCES_ORDER and ENABLE_ZOOPLA:
        urls = build_zoopla_urls()
        for area, url in urls.items():
            print("\n🧭 Launching Playwright for Zoopla…")
            pw_listings: List[Dict] = []
            try:
                pw_listings = await zoopla_collect_with_playwright(area, url)
            except Exception as e:
                print(f"⚠️ Zoopla PW failed: {e}")

            if not pw_listings and USE_HTML_FALLBACK:
                print(f"\n📍 [Zoopla] {area} (HTML) → {url}")
                proxy_url = random.choice(ZOOPLA_PROXY_LIST) if ZOOPLA_PROXY_LIST else ZOOPLA_PROXY
                pw_listings = zoopla_extract_from_html(url, area, proxy_url=proxy_url)

            for listing in pw_listings:
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    pref = choose_preferred(existing, listing); cross_registry[key] = pref
                    if pref is existing: continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids: continue
                seen_ids.add(listing["id"]); new_listings.append(listing)
            await asyncio.sleep(1.0)

    # OTM
    if ("onthemarket" in SOURCES_ORDER or "otm" in SOURCES_ORDER) and ENABLE_OTM:
        urls = build_otm_urls()
        for area, url in urls.items():
            print(f"\n📍 [OnTheMarket] {area}…")
            for listing in fetch_otm_from_url(url, area):
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    pref = choose_preferred(existing, listing); cross_registry[key] = pref
                    if pref is existing: continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids: continue
                seen_ids.add(listing["id"]); new_listings.append(listing)
            time.sleep(1.0)

    # SpareRoom
    if "spareroom" in SOURCES_ORDER and ENABLE_SPAREROOM:
        urls = build_spareroom_urls()
        for area, url in urls.items():
            print(f"\n📍 [SpareRoom] {area}…")
            for listing in fetch_spareroom_from_url(url, area):
                is_dup, existing, key = is_cross_duplicate(listing, cross_registry)
                if is_dup:
                    pref = choose_preferred(existing, listing); cross_registry[key] = pref
                    if pref is existing: continue
                else:
                    cross_registry[key] = listing
                if listing["id"] in seen_ids: continue
                seen_ids.add(listing["id"]); new_listings.append(listing)
            time.sleep(1.0)

    return new_listings

# -----------------------------
# Main loop
# -----------------------------
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

            sleep_seconds = 3600 + random.randint(-300, 300)
            print(f"💤 Sleeping {sleep_seconds} seconds…")
            await asyncio.sleep(sleep_seconds)

        except Exception as e:
            print(f"🔥 Error: {e}")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
