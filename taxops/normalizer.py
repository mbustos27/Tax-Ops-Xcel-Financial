from __future__ import annotations

from datetime import datetime
import re
from typing import Dict, List, Tuple


TRUE_VALUES = {"X", "Y", "YES", "TRUE", "1"}
FALSE_VALUES = {"", "N", "NO", "FALSE", "0"}
DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y")


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


def get_value_any(row: Dict[str, str], lookup: Dict[str, str], headers: Tuple[str, ...]) -> str | None:
    """Return the first CSV cell found for whichever of ``headers`` exists in ``lookup``."""
    for header in headers:
        v = get_value(row, lookup, header)
        if v is not None:
            return v
    return None


def normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    clean = collapse_ws(str(value))
    return clean if clean else None


# Canonical status map: any alias collapses to the canonical DB form.
# Keys are UPPERCASED stripped inputs; values are what we store / compare against.
_STATUS_CANONICAL: Dict[str, str] = {
    # ── Highest-priority terminal status — overrides everything ───────────────
    "CANCEL":       "CANCELLED",
    "CANCELED":     "CANCELLED",
    "CANCELLED":    "CANCELLED",
    "VOID":         "CANCELLED",
    "VOIDED":       "CANCELLED",
    # ── Normal workflow statuses ──────────────────────────────────────────────
    "LOGOUT": "LOG OUT",
    "LOGGED OUT": "LOG OUT",
    "LOG-OUT": "LOG OUT",
    "COMPLETE": "COMPLETE",
    "COMPLETED": "COMPLETE",
    "PICK UP": "PICKUP",
    "PICKUP": "PICKUP",
    "PICKED UP": "PICKUP",
    "EFILE READY": "EFILE READY",
    "E-FILE READY": "EFILE READY",
    "EFILEREADY": "EFILE READY",
    "IN PROGRESS": "IN PROGRESS",
    "IN-PROGRESS": "IN PROGRESS",
    "INPROGRESS": "IN PROGRESS",
    "INTAKE": "INTAKE",
    "INTAKED": "INTAKE",
    "PENDING": "PENDING",
    "HOLD":          "HOLD",
    "ON HOLD":       "HOLD",
    "ONHOLD":        "HOLD",
    "WAITING":       "HOLD",
    "WAITING DOCS":  "HOLD",
    "MISSING DOCS":  "HOLD",
    "REVIEW": "REVIEW",
    "NEEDS REVIEW": "REVIEW",
    "ERROR": "ERROR",
    "AMENDED": "AMENDED",
    "EXTENSION": "EXTENSION",
    "EXT": "EXTENSION",
}

def canonical_status(raw: str | None) -> str | None:
    """Collapse status aliases to a single DB-canonical form (e.g. LOGOUT → LOG OUT)."""
    text = normalize_string(raw)
    if text is None:
        return None
    upper = text.upper()
    # Strip internal whitespace for lookup (LOG OUT → LOGOUT key)
    compact = re.sub(r"\s+", "", upper)
    return _STATUS_CANONICAL.get(upper) or _STATUS_CANONICAL.get(compact) or upper


# Statuses that are terminal and must not be overwritten by any import source.
_LOCKED_STATUSES: frozenset[str] = frozenset({"CANCELLED"})


def is_locked_status(status: str | None) -> bool:
    """Return True if this status must never be overwritten by an import.

    A cancelled file stays cancelled regardless of what any CSV says.
    The only way to un-cancel is a manual DB edit.
    """
    if not status:
        return False
    return canonical_status(status) in _LOCKED_STATUSES


def normalize_status(value: str | None) -> str | None:
    return canonical_status(value)


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
    # Treat lone dash / em-dash as "no value"
    if not clean or clean in ("-", "–", "—"):
        return None
    try:
        return float(clean)
    except ValueError:
        return None


_CURRENT_CENTURY = 2000
_TAX_YEAR_MIN = 1990
_TAX_YEAR_MAX = 2099


def normalize_tax_year(value: str | None) -> int | None:
    """
    Coerce a raw tax-year string to a valid 4-digit integer year, handling:
      - 2-digit shorthand : "25"    → 2025
      - Repeated digits   : "22025" → 2025  (leading repeated century digit)
      - Already correct   : "2025"  → 2025
      - Garbage           : "abc"   → None
    Returns None when the value cannot be resolved to a plausible tax year.
    """
    raw = (value or "").strip()
    if not raw or not raw.isdigit():
        return None
    n = int(raw)
    # Already a plausible 4-digit year
    if _TAX_YEAR_MIN <= n <= _TAX_YEAR_MAX:
        return n
    # 2-digit year: 0–99 → 2000–2099
    if 0 <= n <= 99:
        return _CURRENT_CENTURY + n
    # Typo like "22025" → strip leading repeated century prefix
    s = str(n)
    # Try trimming one leading digit at a time until we get a valid year
    for start in range(1, len(s)):
        candidate = int(s[start:])
        if _TAX_YEAR_MIN <= candidate <= _TAX_YEAR_MAX:
            return candidate
    return None


def normalize_date(value: str | None) -> Tuple[str | None, str | None]:
    text = normalize_string(value)
    if text is None:
        return None, None

    # Strip time component: "04/12/2026 12:25:08" → "04/12/2026"
    date_part = text.split(" ")[0].strip()
    if not date_part:
        return None, None

    for fmt in DATE_FORMATS:
        try:
            parsed = datetime.strptime(date_part, fmt)
            return parsed.date().isoformat(), None
        except ValueError:
            pass

    return None, f"Invalid date: {date_part}"

