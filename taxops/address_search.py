"""US address search for intake autofill (Census geocoder + Nominatim fallback)."""
from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

_CENSUS_URL = (
    "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
)
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_USER_AGENT = "TaxOps/1.0 (intake address search; internal office use)"
_TIMEOUT = 4.0


def _fetch_json(url: str, *, params: dict[str, str]) -> Any:
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(
        f"{url}?{qs}",
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _search_census(query: str, *, limit: int = 6) -> list[str]:
    try:
        data = _fetch_json(
            _CENSUS_URL,
            params={
                "address": query,
                "benchmark": "Public_AR_Current",
                "format": "json",
            },
        )
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        log.debug("Census geocoder failed: %s", exc)
        return []

    matches = (data.get("result") or {}).get("addressMatches") or []
    out: list[str] = []
    for m in matches[:limit]:
        addr = (m.get("matchedAddress") or "").strip()
        if addr:
            out.append(addr.title() if addr.isupper() else addr)
    return out


def _format_nominatim(item: dict) -> str:
    disp = (item.get("display_name") or "").strip()
    if disp:
        # Drop trailing "United States" for brevity on intake.
        if disp.endswith(", United States"):
            disp = disp[: -len(", United States")]
        return disp
    addr = item.get("address") or {}
    parts = [
        " ".join(
            p
            for p in (
                (addr.get("house_number") or "").strip(),
                (addr.get("road") or "").strip(),
            )
            if p
        ),
        (addr.get("city") or addr.get("town") or addr.get("village") or "").strip(),
        (addr.get("state") or "").strip(),
        (addr.get("postcode") or "").strip(),
    ]
    line = ", ".join(p for p in parts if p)
    return line


def _search_nominatim(query: str, *, limit: int = 6) -> list[str]:
    try:
        data = _fetch_json(
            _NOMINATIM_URL,
            params={
                "q": query,
                "format": "json",
                "addressdetails": "1",
                "countrycodes": "us",
                "limit": str(limit),
            },
        )
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        log.debug("Nominatim geocoder failed: %s", exc)
        return []

    if not isinstance(data, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        label = _format_nominatim(item)
        key = label.lower()
        if label and key not in seen:
            seen.add(key)
            out.append(label)
    return out


def _label_key(label: str) -> str:
    """Normalize for dedup (punctuation/case insensitive)."""
    return re.sub(r"[^a-z0-9]", "", label.lower())


def search_addresses(query: str, *, limit: int = 6) -> list[dict[str, str]]:
    """
    Return address suggestions for intake autofill.

    Each item: {"label": "123 Main St, City, ST, 90210"}
    """
    q = " ".join((query or "").split())
    if len(q) < 3:
        return []

    labels: list[str] = []
    seen: set[str] = set()
    for src in (_search_census(q, limit=limit), _search_nominatim(q, limit=limit)):
        for label in src:
            key = _label_key(label)
            if key in seen:
                continue
            seen.add(key)
            labels.append(label)
            if len(labels) >= limit:
                break
        if len(labels) >= limit:
            break

    return [{"label": lbl} for lbl in labels]
