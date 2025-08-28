import os
from scrapers.zoopla.main import extract_search_result_urls

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scrapers", "zoopla", "fixtures")

def load_fixture(name: str) -> str:
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def test_extract_urls_page1():
    html = load_fixture("search-page1.html")
    urls = extract_search_result_urls(html)
    assert "https://www.zoopla.co.uk/to-rent/details/1" in urls
    assert "https://www.zoopla.co.uk/to-rent/details/2" in urls
    assert len(urls) == 2

def test_extract_urls_page2():
    html = load_fixture("search-page2.html")
    urls = extract_search_result_urls(html)
    assert "https://www.zoopla.co.uk/to-rent/details/3" in urls
    assert len(urls) == 1
