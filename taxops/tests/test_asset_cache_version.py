"""CACHE / GitHub #143 — asset version token helper."""

from __future__ import annotations

from config import taxops_asset_cache_version


def test_taxops_asset_cache_version_is_non_empty_string() -> None:
    v = taxops_asset_cache_version()
    assert isinstance(v, str)
    assert len(v) >= 1
