"""Tests for the Zoopla proxy parser."""
from scrapers.zoopla.main import parse_proxy


def test_parse_proxy_with_credentials():
    proxy = parse_proxy("http://user:pass@host:1234")
    assert proxy == {"server": "http://host:1234", "username": "user", "password": "pass"}


def test_parse_proxy_without_credentials():
    proxy = parse_proxy("http://host:4321")
    assert proxy == {"server": "http://host:4321"}


def test_parse_proxy_invalid():
    assert parse_proxy("not-a-url") is None
