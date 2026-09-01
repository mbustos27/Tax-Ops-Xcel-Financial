"""Return-level tax deadline resolution (dashboard flags + filters)."""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any, Callable, Optional

from engagement_status_rules import drake_indicates_extension_accepted

UPCOMING_DEADLINE_DAYS = 30
UPCOMING_DEADLINE_FLAG = "UPCOMING DEADLINE"

_CLOSED_STATUSES = frozenset({"LOG OUT", "CANCELLED"})

# Legacy keys kept for existing tax_deadlines rows / extension track on 1040 returns.
_APPLIES_EXTENSION = "federal/extension"
_APPLIES_INDIVIDUAL = "federal/1040"

EntityKind = str  # 1040 | 1120 | 1120s | 1065 | 990_1041

_ENTITY_FORM_FIELDS: tuple[tuple[str, EntityKind], ...] = (
    ("form_1120", "1120"),
    ("form_1120s", "1120s"),
    ("form_1065_llc", "1065"),
    ("form_990_1041", "990_1041"),
)

# Drake CSM statuses that mean an extension was filed (return still open).
_DRAKE_EXTENSION_STATUS_RAW = frozenset(
    {
        "EF EXT ACCEPTED",
        "ON EXTENSION",
        "EF EXTENSION",
        "EXTENSION",
        "EF EXT",
    }
)

# Calendar-year federal defaults for tax_year T (due dates fall in year T+1).
_ENTITY_DEADLINE_DEFAULTS: dict[EntityKind, dict[str, tuple[str, Callable[[int], str], str]]] = {
    "1040": {
        "regular": (_APPLIES_INDIVIDUAL, lambda ty: f"{ty + 1}-04-15", "Federal individual return due"),
        "extension": (_APPLIES_EXTENSION, lambda ty: f"{ty + 1}-10-15", "Federal extended return due"),
    },
    "1120": {
        "regular": ("federal/1120", lambda ty: f"{ty + 1}-04-15", "C corporation return due"),
        "extension": ("federal/1120/extension", lambda ty: f"{ty + 1}-10-15", "C corporation extended return due"),
    },
    "1120s": {
        "regular": ("federal/1120s", lambda ty: f"{ty + 1}-03-15", "S corporation return due"),
        "extension": ("federal/1120s/extension", lambda ty: f"{ty + 1}-09-15", "S corporation extended return due"),
    },
    "1065": {
        "regular": ("federal/1065", lambda ty: f"{ty + 1}-03-15", "Partnership / LLC return due"),
        "extension": ("federal/1065/extension", lambda ty: f"{ty + 1}-09-15", "Partnership / LLC extended return due"),
    },
    "990_1041": {
        "regular": ("federal/990_1041", lambda ty: f"{ty + 1}-05-15", "990 / 1041 return due"),
        "extension": (
            "federal/990_1041/extension",
            lambda ty: f"{ty + 1}-11-15",
            "990 / 1041 extended return due",
        ),
    },
}


def default_extension_due_date(tax_year: int, entity: EntityKind = "1040") -> str:
    return _ENTITY_DEADLINE_DEFAULTS[entity]["extension"][1](int(tax_year))


def load_tax_deadline_lookup(conn: sqlite3.Connection) -> dict[tuple[int, str], dict[str, str]]:
    """Map (tax_year, applies_to) -> {due_date, label}."""
    rows = conn.execute(
        """
        SELECT tax_year, applies_to, due_date, label
        FROM tax_deadlines
        WHERE is_active = 1
        """
    ).fetchall()
    out: dict[tuple[int, str], dict[str, str]] = {}
    for row in rows:
        key = (int(row["tax_year"]), str(row["applies_to"]))
        out[key] = {
            "due_date": str(row["due_date"])[:10],
            "label": str(row["label"] or ""),
        }
    return out


def _parse_iso(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    try:
        return date.fromisoformat(str(d)[:10])
    except ValueError:
        return None


def _drake_on_extension_track(drake_status_raw: Optional[str]) -> bool:
    raw = (drake_status_raw or "").strip()
    if drake_indicates_extension_accepted(raw):
        return True
    return raw.upper() in _DRAKE_EXTENSION_STATUS_RAW


def _on_extension_track(row: dict[str, Any]) -> bool:
    if bool(int(row.get("is_extension") or 0)):
        return True
    if bool(int(row.get("extension_requested") or 0)):
        return True
    return _drake_on_extension_track(row.get("drake_status_raw"))


def detect_entity_kind(row: dict[str, Any]) -> EntityKind:
    """Pick deadline family from normalized return_forms flags (first match wins)."""
    for field, kind in _ENTITY_FORM_FIELDS:
        if int(row.get(field) or 0):
            return kind
    return "1040"


def _resolve_entity_deadline(
    tax_year: int,
    entity: EntityKind,
    track: str,
    deadline_lookup: dict[tuple[int, str], dict[str, str]] | None,
) -> tuple[str, str]:
    applies_to, default_fn, default_label = _ENTITY_DEADLINE_DEFAULTS[entity][track]
    lookup = deadline_lookup or {}
    meta = lookup.get((tax_year, applies_to))
    if meta:
        return meta["due_date"], meta["label"]
    return default_fn(tax_year), default_label


def _effective_deadline_track(
    row: dict[str, Any],
    tax_year: int,
    entity: EntityKind,
    deadline_lookup: dict[tuple[int, str], dict[str, str]] | None,
    *,
    today: Optional[date] = None,
) -> str:
    """Regular vs extended due — open returns past regular due use extended."""
    if _on_extension_track(row):
        return "extension"
    regular_due, _ = _resolve_entity_deadline(tax_year, entity, "regular", deadline_lookup)
    reg_days = days_until_deadline(regular_due, today=today)
    if reg_days is not None and reg_days < 0:
        return "extension"
    return "regular"


def effective_deadline_for_return(
    row: dict[str, Any],
    deadline_lookup: dict[tuple[int, str], dict[str, str]] | None = None,
    *,
    today: Optional[date] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Return (due_date_iso, label) for an open return, or (None, None)."""
    status = (row.get("client_status") or "").strip().upper()
    if status in _CLOSED_STATUSES:
        return None, None

    ext_due = (row.get("extension_due_date") or "").strip()
    if ext_due:
        return ext_due[:10], "Extended return due"

    ty = row.get("tax_year")
    if ty is None:
        return None, None

    tax_year = int(ty)
    entity = detect_entity_kind(row)
    track = _effective_deadline_track(
        row, tax_year, entity, deadline_lookup, today=today
    )
    due, label = _resolve_entity_deadline(tax_year, entity, track, deadline_lookup)
    return due, label


def days_until_deadline(due_iso: Optional[str], *, today: Optional[date] = None) -> Optional[int]:
    due = _parse_iso(due_iso)
    if not due:
        return None
    ref = today or date.today()
    return (due - ref).days


def upcoming_deadline_flag(
    row: dict[str, Any],
    deadline_lookup: dict[tuple[int, str], dict[str, str]] | None = None,
    *,
    within_days: int = UPCOMING_DEADLINE_DAYS,
    today: Optional[date] = None,
) -> bool:
    ref = today or date.today()
    due, _label = effective_deadline_for_return(row, deadline_lookup, today=ref)
    days = days_until_deadline(due, today=ref)
    if days is None:
        return False
    return 0 <= days <= int(within_days)


def enrich_deadline_fields(
    row: dict[str, Any],
    deadline_lookup: dict[tuple[int, str], dict[str, str]] | None = None,
) -> None:
    due, label = effective_deadline_for_return(row, deadline_lookup)
    days = days_until_deadline(due)
    row["deadline_date"] = due or ""
    row["deadline_label"] = label or ""
    row["days_until_deadline"] = days if days is not None else ""
    row["upcoming_deadline_flag"] = upcoming_deadline_flag(row, deadline_lookup)


def _sql_entity_match(entity: EntityKind, forms_alias: str = "rf") -> str:
    if entity == "1040":
        return " AND ".join(
            f"COALESCE({forms_alias}.{field}, 0) = 0" for field, _kind in _ENTITY_FORM_FIELDS
        )

    idx = [kind for _, kind in _ENTITY_FORM_FIELDS].index(entity)
    higher_kinds = [kind for _, kind in _ENTITY_FORM_FIELDS][:idx]
    parts = [f"COALESCE({forms_alias}.{_field_for_kind(entity)}, 0) = 1"]
    for kind in higher_kinds:
        parts.append(f"COALESCE({forms_alias}.{_field_for_kind(kind)}, 0) = 0")
    return " AND ".join(parts)


def _field_for_kind(kind: EntityKind) -> str:
    for field, entity_kind in _ENTITY_FORM_FIELDS:
        if entity_kind == kind:
            return field
    raise KeyError(kind)


def _sql_on_extension_track(return_alias: str) -> str:
    a = return_alias
    drake_vals = ", ".join(f"'{v}'" for v in sorted(_DRAKE_EXTENSION_STATUS_RAW))
    return f"""(
        COALESCE({a}.is_extension, 0) = 1
        OR COALESCE({a}.extension_requested, 0) = 1
        OR TRIM(COALESCE({a}.drake_status_raw, '')) = 'EF Ext Accepted'
        OR UPPER(TRIM(COALESCE({a}.drake_status_raw, ''))) IN ({drake_vals})
    )"""


def _sql_deadline_in_window(
    *,
    applies_to: str,
    default_date_sql: str,
    tax_year_expr: str,
    win: int,
) -> str:
    return f"""(
        EXISTS (
            SELECT 1 FROM tax_deadlines td
            WHERE td.tax_year = {tax_year_expr}
              AND td.is_active = 1
              AND td.applies_to = '{applies_to}'
              AND td.due_date >= date('now')
              AND td.due_date <= date('now', '+{win} day')
        )
        OR (
            {default_date_sql} >= date('now')
            AND {default_date_sql} <= date('now', '+{win} day')
        )
    )"""


def _sql_default_date_expr(entity: EntityKind, track: str, return_alias: str) -> str:
    _applies_to, default_fn, _label = _ENTITY_DEADLINE_DEFAULTS[entity][track]
    month_day = default_fn(2025)[5:]
    return f"date(printf('%d-{month_day}', {return_alias}.tax_year + 1))"


def _sql_entity_upcoming_window(
    entity: EntityKind,
    *,
    return_alias: str,
    forms_alias: str,
    win: int,
) -> str:
    """Entity row matches when its effective deadline falls in the next N days."""
    a = return_alias
    entity_match = _sql_entity_match(entity, forms_alias)
    reg_date = _sql_default_date_expr(entity, "regular", a)
    ext_applies, _ext_fn, _ = _ENTITY_DEADLINE_DEFAULTS[entity]["extension"]
    ext_date = _sql_default_date_expr(entity, "extension", a)
    ext_window = _sql_deadline_in_window(
        applies_to=ext_applies,
        default_date_sql=ext_date,
        tax_year_expr=f"{a}.tax_year",
        win=win,
    )
    reg_applies, _reg_fn, _ = _ENTITY_DEADLINE_DEFAULTS[entity]["regular"]
    reg_window = _sql_deadline_in_window(
        applies_to=reg_applies,
        default_date_sql=reg_date,
        tax_year_expr=f"{a}.tax_year",
        win=win,
    )
    on_ext = _sql_on_extension_track(a)
    return f"""(
        {entity_match}
        AND (
            ({reg_window} AND {reg_date} >= date('now'))
            OR ({on_ext} AND {ext_window})
            OR ({reg_date} < date('now') AND {ext_window})
        )
    )"""


def sql_upcoming_deadline_clause(
    return_alias: str = "r",
    forms_alias: str = "rf",
) -> str:
    """Dashboard/campaign filter: open returns with entity-aware deadline in next 30 days."""
    a = return_alias
    win = UPCOMING_DEADLINE_DAYS
    ext_due = f"""(
        {a}.extension_due_date IS NOT NULL
        AND TRIM({a}.extension_due_date) != ''
        AND {a}.extension_due_date >= date('now')
        AND {a}.extension_due_date <= date('now', '+{win} day')
    )"""
    no_explicit_ext = (
        f"({a}.extension_due_date IS NULL OR TRIM({a}.extension_due_date) = '')"
    )
    entity_windows = " OR ".join(
        _sql_entity_upcoming_window(
            entity,
            return_alias=a,
            forms_alias=forms_alias,
            win=win,
        )
        for entity in ("1120", "1120s", "1065", "990_1041", "1040")
    )

    return f"""(
        UPPER(COALESCE({a}.client_status, '')) NOT IN ('LOG OUT', 'CANCELLED')
        AND (
            {ext_due}
            OR (
                {no_explicit_ext}
                AND ({entity_windows})
            )
        )
    )"""
