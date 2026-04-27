"""
Office preparer / processor: normalize all LY- and MB-family inputs to full legal names
stored in `returns.processor`.
"""
from __future__ import annotations

from typing import Any

# Only these two values are written for the current staff; extend mapping in _build_alias_to_full.
LUCILA_YANEZ = "Lucila Yanez"
MOISES_BUSTOS = "Moises Bustos"


def _norm_key(s: str) -> str:
    return " ".join(s.split()).casefold()


def _build_alias_to_full() -> dict[str, str]:
    """Map every common variant to exactly one display string each."""
    m: dict[str, str] = {}
    for full, codes in (
        (
            LUCILA_YANEZ,
            (
                "LY",
                "L.Y.",
                "L Y",
                "Lucila",
                "Yanez",
                "Lucila Yanez",
            ),
        ),
        (
            MOISES_BUSTOS,
            (
                "MB",
                "M.B.",
                "M B",
                "Moises",
                "Bustos",
                "Moises Bustos",
            ),
        ),
    ):
        m[_norm_key(full)] = full
        for c in codes:
            m[_norm_key(c)] = full
    return m


_ALIAS_TO_FULL: dict[str, str] = _build_alias_to_full()


def normalize_preparer(value: Any) -> str | None:
    """
    Collapse LY (and name variants) → Lucila Yanez, MB (and variants) → Moises Bustos.
    Other text is returned trimmed unchanged.
    """
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    collapsed = " ".join(v.split())
    k = _norm_key(collapsed)
    if k in _ALIAS_TO_FULL:
        return _ALIAS_TO_FULL[k]
    if len(collapsed) <= 4 and collapsed.isalpha() and collapsed.upper() == collapsed:
        up = collapsed.upper()
        if up == "LY":
            return LUCILA_YANEZ
        if up == "MB":
            return MOISES_BUSTOS
    return collapsed


def preparer_list_label(value: str | None) -> str:
    """Display label — normalizes legacy codes/names; unknown text passes through."""
    if not value or not str(value).strip():
        return "—"
    n = normalize_preparer(value)
    return n if n else str(value).strip()


# All `returns.processor` values that mean Lucila / Moises (for dashboard SQL `IN` vs legacy data)
_PREPARER_SQL_LUCILA: list[str] = list(
    {
        LUCILA_YANEZ,
        "LY",
        "ly",
        "L.Y.",
        "L Y",
        "LUCILA",
        "YANEZ",
        "Lucila",
        "Yanez",
        "lucila",
        "yanez",
        "lucila yanez",
    }
)
_PREPARER_SQL_MOISES: list[str] = list(
    {
        MOISES_BUSTOS,
        "MB",
        "mb",
        "M.B.",
        "M B",
        "MOISES",
        "BUSTOS",
        "Moises",
        "Bustos",
        "moises",
        "bustos",
        "moises bustos",
    }
)


def preparer_filter_match_values(choice: str | None) -> list[str]:
    """
    Values for `WHERE r.processor IN (...)` so filtering by full name matches legacy LY/MB in DB.
    """
    if not choice or not str(choice).strip():
        return []
    c = str(choice).strip()
    n = normalize_preparer(c)
    if n == LUCILA_YANEZ:
        return _PREPARER_SQL_LUCILA
    if n == MOISES_BUSTOS:
        return _PREPARER_SQL_MOISES
    return [c]


def preparer_dropdown_options(distinct_processors: list[str | None]) -> list[str]:
    """
    Build sorted dropdown options: always offer known staff, plus any other distinct preparers
    in the current season (normalized to full name where applicable).
    """
    out: set[str] = {LUCILA_YANEZ, MOISES_BUSTOS}
    for p in distinct_processors:
        if not p or not str(p).strip():
            continue
        n = normalize_preparer(p)
        if n:
            out.add(n)
    return sorted(out, key=str.lower)
