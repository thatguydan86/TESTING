import os
from scrapers.zoopla.main import parse_listing_jsonld

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scrapers", "zoopla", "fixtures")

def load_fixture(name: str) -> str:
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def test_parse_listing_jsonld_listing1():
    html = load_fixture("listing-1.html")
    data = parse_listing_jsonld(html)
    assert data is not None
    assert data["rent_pcm"] == 1200
    assert data["beds"] == 3
    assert data["address"] == "123 Fake Street"
    assert data["postcode"] == "L12AB"  # L1 2AB becomes L12AB in simple regex remove space
    assert data["url"] == "https://www.zoopla.co.uk/to-rent/details/1"

def test_parse_listing_jsonld_listing2():
    html = load_fixture("listing-2.html")
    data = parse_listing_jsonld(html)
    assert data["rent_pcm"] == 800
    assert data["beds"] == 2
    assert data["address"] == "45 Example Ave"
    assert data["postcode"] == "L13CD"
    assert data["url"] == "https://www.zoopla.co.uk/to-rent/details/2"
