"""Tests for address_search.search_addresses."""
from __future__ import annotations

import json
from unittest.mock import patch

from address_search import search_addresses


def test_search_addresses_too_short():
    assert search_addresses("ab") == []


def test_search_addresses_census_results():
    census_payload = {
        "result": {
            "addressMatches": [
                {"matchedAddress": "123 MAIN ST, LOS ANGELES, CA, 90001"},
            ]
        }
    }

    def fake_fetch(url, *, params):
        if "census.gov" in url:
            return census_payload
        return []

    with patch("address_search._fetch_json", side_effect=fake_fetch):
        out = search_addresses("123 main los angeles")

    assert len(out) == 1
    assert "123 Main St" in out[0]["label"]


def test_search_addresses_dedupes_sources():
    census_payload = {
        "result": {
            "addressMatches": [
                {"matchedAddress": "123 MAIN ST, LOS ANGELES, CA, 90001"},
            ]
        }
    }
    nominatim_payload = [
        {
            "display_name": "123 Main St, Los Angeles, CA 90001, United States",
            "address": {},
        }
    ]

    def fake_fetch(url, *, params):
        if "census.gov" in url:
            return census_payload
        return nominatim_payload

    with patch("address_search._fetch_json", side_effect=fake_fetch):
        out = search_addresses("123 main los angeles ca")

    assert len(out) >= 1
    assert all("label" in row for row in out)
