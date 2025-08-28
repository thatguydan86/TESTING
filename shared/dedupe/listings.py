"""
Simple dedupe utilities for listings.
Listings are dictionaries with at least address, postcode and url.
We dedupe by normalized address + postcode or canonical URL.
"""

from typing import List, Dict, Tuple


def _normalize_address(addr: str) -> str:
    return addr.lower().strip() if addr else ""


def dedupe_listings(listings: List[Dict]) -> List[Dict]:
    seen: set[Tuple[str, str]] = set()
    deduped: List[Dict] = []
    for item in listings:
        key = None
        addr = item.get("address")
        postcode = item.get("postcode")
        url = item.get("url")
        if addr and postcode:
            key = (_normalize_address(addr), postcode.lower())
        elif url:
            key = (url.strip().lower(), "")
        else:
            # cannot dedupe without key, accept as unique
            deduped.append(item)
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped
