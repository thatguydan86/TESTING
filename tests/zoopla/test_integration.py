import os
import asyncio
from scrapers.zoopla.main import scrape_from_fixtures

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scrapers", "zoopla", "fixtures")

def load_fixture(name: str) -> str:
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def test_scrape_integration():
    search_pages = [
        load_fixture("search-page1.html"),
        load_fixture("search-page2.html"),
    ]
    listing_pages = {
        "https://www.zoopla.co.uk/to-rent/details/1": load_fixture("listing-1.html"),
        "https://www.zoopla.co.uk/to-rent/details/2": load_fixture("listing-2.html"),
    }
    results = asyncio.run(scrape_from_fixtures(search_pages, listing_pages))
    assert len(results) >= 2
    for row in results:
        assert row["source"] == "zoopla"
        assert row["rent_pcm"] > 0
        assert row["beds"] > 0
        assert row["address"]
        assert row["postcode"]
        assert row["url"].startswith("https://www.zoopla.co.uk")
        assert row["scraped_at"]
