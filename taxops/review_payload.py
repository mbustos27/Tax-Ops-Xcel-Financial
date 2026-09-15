"""Normalize review-queue raw_json (Drake/CSM or UI upload) into import rows."""
from __future__ import annotations

import json
from typing import Any

from name_matcher import parse_name
from normalizer import normalize_string


def _as_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _first(row: dict, *keys: str) -> str | None:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return normalize_string(str(row[key]))
        lower = {str(k).lower(): k for k in row}
        if key.lower() in lower:
            val = row[lower[key.lower()]]
            if val not in (None, ""):
                return normalize_string(str(val))
    return None


def extract_review_identity(raw: Any, fallback_year: int | None = None) -> dict[str, Any]:
    """Display fields for the review queue UI."""
    row = _as_dict(raw)
    last = _first(row, "clients.last_name", "Taxpayer Last Name", "LAST", "Last")
    first = _first(row, "clients.first_name", "Taxpayer First Name", "FIRST", "First")
    client_name = _first(row, "Client Name", "TAX PAYER NAME (S)")
    if not last and client_name:
        parsed_last, parsed_first = parse_name(client_name)
        last = parsed_last
        first = first or parsed_first
    log_number = _first(row, "returns.log_number", "LOG 2025", "LOG 2026", "Log #", "LOG")
    year_raw = _first(row, "returns.tax_year", "YR", "Tax Year", "YEAR")
    year: int | None = fallback_year
    if year_raw and str(year_raw).isdigit():
        year = int(year_raw)
    return {
        "csv_last": last,
        "csv_first": first,
        "csv_log": log_number,
        "csv_year": year,
    }


def to_import_row(raw: Any, csv_year: int | None) -> dict[str, Any]:
    """Shape a review-queue payload for ``_import_row`` / ``_import_row_forced``."""
    row = _as_dict(raw)
    if any(str(k).startswith("clients.") or str(k).startswith("returns.") for k in row):
        out = dict(row)
        if csv_year and not out.get("returns.tax_year"):
            out["returns.tax_year"] = str(csv_year)
        return out

    ident = extract_review_identity(row, csv_year)
    last = ident.get("csv_last") or ""
    first = ident.get("csv_first") or ""
    tax_year = ident.get("csv_year") or csv_year
    status = _first(row, "CLIENT STATUS", "Status", "returns.client_status") or "PROCESSING"
    processor = _first(row, "PROCESSOR", "Preparer", "returns.processor")
    log_number = ident.get("csv_log") or ""

    payload: dict[str, Any] = {
        "clients.last_name": last,
        "clients.first_name": first,
        "returns.log_number": log_number,
        "returns.tax_year": str(tax_year) if tax_year else "",
        "returns.client_status": status,
        "returns.processor": processor or "",
    }
    return payload
