from __future__ import annotations

from datetime import datetime
import re
from typing import Dict, List, Tuple


TRUE_VALUES = {"X", "Y", "YES", "TRUE", "1"}
FALSE_VALUES = {"", "N", "NO", "FALSE", "0"}
DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y")


def collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def canonical_header(value: str | None) -> str:
    if value is None:
        return ""
    return collapse_ws(value).upper()


def build_header_lookup(fieldnames: List[str] | None) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    if not fieldnames:
        return lookup
    for header in fieldnames:
        if header is None:
            continue
        clean = str(header).strip()
        if not clean:
            continue
        key = canonical_header(clean)
        if key and key not in lookup:
            lookup[key] = clean
    return lookup


def get_value(row: Dict[str, str], lookup: Dict[str, str], header: str) -> str | None:
    actual = lookup.get(canonical_header(header))
    if actual is None:
        return None
    return row.get(actual)


def normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    clean = collapse_ws(str(value))
    return clean if clean else None


def normalize_status(value: str | None) -> str | None:
    text = normalize_string(value)
    if text is None:
        return None
    return text.upper()


def normalize_bool_flag(value: str | None) -> bool | None:
    text = normalize_string(value)
    if text is None:
        return None
    upper = text.upper()
    if upper in TRUE_VALUES:
        return True
    if upper in FALSE_VALUES:
        return False
    return None


def normalize_currency(value: str | None) -> float | None:
    text = normalize_string(value)
    if text is None:
        return None
    clean = text.replace("$", "").replace(",", "").replace(" ", "")
    if not clean:
        return None
    return float(clean)


def normalize_date(value: str | None) -> Tuple[str | None, str | None]:
    text = normalize_string(value)
    if text is None:
        return None, None

    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.date().isoformat(), None
        except ValueError:
            pass

    return None, f"Invalid date: {text}"

