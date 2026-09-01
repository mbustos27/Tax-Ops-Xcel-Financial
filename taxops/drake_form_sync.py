"""Normalize return_forms from Drake CSV exports and purple prefill."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

RETURN_FORM_FIELDS: tuple[str, ...] = (
    "form_1040",
    "sched_a_d",
    "sched_c",
    "sched_e",
    "form_1120",
    "form_1120s",
    "form_1065_llc",
    "corp_officer",
    "business_owner",
    "form_990_1041",
)

SCHEDULE_FORM_FIELDS: frozenset[str] = frozenset({"sched_a_d", "sched_c", "sched_e"})
MANUAL_ONLY_FORM_FIELDS: frozenset[str] = frozenset({"corp_officer", "business_owner"})

# Drake purple form-count headers → return_forms columns (count ≥ 1).
PREFILL_FORM_CHECKBOX_MAP: dict[str, str] = {
    "Schedule A": "sched_a_d",
    "Schedule C": "sched_c",
    "Schedule E": "sched_e",
    "Form 1120": "form_1120",
    "Form 1120S": "form_1120s",
    "Form 1065": "form_1065_llc",
    "Form 990": "form_990_1041",
    "Form 1041": "form_990_1041",
}


def _blank_forms() -> dict[str, int]:
    return {field: 0 for field in RETURN_FORM_FIELDS}


def family_flags_from_prefill_return_type(return_type: str | None) -> dict[str, int]:
    """Map purple Return Type to entity/family flags."""
    out = _blank_forms()
    rt = (return_type or "").strip().upper().replace("-", "")
    if rt in ("1040", "1040SR"):
        out["form_1040"] = 1
    elif rt == "1120":
        out["form_1120"] = 1
    elif rt == "1120S":
        out["form_1120s"] = 1
    elif rt == "1065":
        out["form_1065_llc"] = 1
    elif rt in ("990", "1041"):
        out["form_990_1041"] = 1
    return out


def schedule_flags_from_prefill_counts(form_counts: dict[str, Any] | None) -> dict[str, int]:
    """Schedule flags from Drake purple counts (explicit 0/1)."""
    out = {field: 0 for field in SCHEDULE_FORM_FIELDS}
    if not form_counts:
        return out
    for header, field in PREFILL_FORM_CHECKBOX_MAP.items():
        if field not in SCHEDULE_FORM_FIELDS:
            continue
        val = form_counts.get(header)
        if isinstance(val, (int, float)) and val >= 1:
            out[field] = 1
    return out


def _has_family_flag(forms: dict[str, int]) -> bool:
    return any(
        int(forms.get(field) or 0)
        for field in RETURN_FORM_FIELDS
        if field not in MANUAL_ONLY_FORM_FIELDS and field not in SCHEDULE_FORM_FIELDS
    )


def merge_drake_return_forms(
    *,
    csm_type_forms: dict[str, Any] | None,
    prefill_counts: dict[str, Any] | None = None,
    prefill_return_type: str | None = None,
    existing: dict[str, Any] | None = None,
) -> dict[str, int]:
    """
    Build normalized return_forms:
    - Family flags from Drake CSV Type (CSM/Tax Ops export)
    - Schedule flags from linked purple prefill when present
    - Preserve manual-log-only corp_officer / business_owner
    """
    merged = _blank_forms()

    if csm_type_forms:
        for field in RETURN_FORM_FIELDS:
            if field in MANUAL_ONLY_FORM_FIELDS:
                continue
            merged[field] = int(csm_type_forms.get(field) or 0)

    schedule = schedule_flags_from_prefill_counts(prefill_counts)
    merged.update(schedule)

    if prefill_counts is not None and not _has_family_flag(merged):
        for field, val in family_flags_from_prefill_return_type(prefill_return_type).items():
            if field in MANUAL_ONLY_FORM_FIELDS or field in SCHEDULE_FORM_FIELDS:
                continue
            if val:
                merged[field] = val

    if existing:
        for field in MANUAL_ONLY_FORM_FIELDS:
            if existing.get(field) is not None:
                merged[field] = int(existing.get(field) or 0)

    return merged


def fetch_prefill_forms_for_client(
    conn: sqlite3.Connection,
    client_id: int,
    return_tax_year: int,
) -> tuple[dict[str, Any], str | None] | None:
    """Best purple prefill row for a client (prefers same tax year, then newest)."""
    has_fp = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='drake_form_prefill'"
    ).fetchone()
    if not has_fp:
        return None

    row = conn.execute(
        """
        SELECT fp.form_counts, fp.return_type
        FROM drake_prefill_links l
        JOIN drake_form_prefill fp ON fp.link_id = l.id
        WHERE l.client_id = ?
        ORDER BY
          CASE WHEN l.tax_year = ? THEN 0 ELSE 1 END,
          CASE l.prefill_status
            WHEN 'PRIOR_YEAR_FORMS_AVAILABLE' THEN 0
            ELSE 1
          END,
          l.tax_year DESC,
          l.id DESC
        LIMIT 1
        """,
        (client_id, return_tax_year),
    ).fetchone()
    if not row:
        return None

    try:
        counts = json.loads(row["form_counts"] or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        counts = {}
    if not isinstance(counts, dict):
        counts = {}
    return counts, row["return_type"]


def upsert_return_forms(
    conn: sqlite3.Connection,
    return_id: int,
    forms: dict[str, Any],
    *,
    overwrite: bool = False,
) -> None:
    """Insert or update return_forms (overwrite=True for Drake normalization)."""
    row = conn.execute(
        "SELECT id FROM return_forms WHERE return_id=? LIMIT 1",
        (return_id,),
    ).fetchone()
    values = tuple(int(forms.get(field) or 0) for field in RETURN_FORM_FIELDS)
    if row is None:
        conn.execute(
            f"""
            INSERT INTO return_forms (return_id, {', '.join(RETURN_FORM_FIELDS)})
            VALUES (?, {', '.join('?' for _ in RETURN_FORM_FIELDS)})
            """,
            (return_id, *values),
        )
    elif overwrite:
        conn.execute(
            f"""
            UPDATE return_forms SET
              {', '.join(f'{field} = ?' for field in RETURN_FORM_FIELDS)}
            WHERE return_id = ?
            """,
            (*values, return_id),
        )
    else:
        conn.execute(
            f"""
            UPDATE return_forms SET
              {', '.join(f'{field} = COALESCE(?, {field})' for field in RETURN_FORM_FIELDS)}
            WHERE return_id = ?
            """,
            (*values, return_id),
        )


def fetch_existing_forms(conn: sqlite3.Connection, return_id: int) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT form_1040, sched_a_d, sched_c, sched_e,
               form_1120, form_1120s, form_1065_llc,
               corp_officer, business_owner, form_990_1041
        FROM return_forms WHERE return_id = ?
        """,
        (return_id,),
    ).fetchone()
    return dict(row) if row else {}


def forms_differ(existing: dict[str, Any], target: dict[str, int]) -> bool:
    for field in RETURN_FORM_FIELDS:
        old = int(existing.get(field) or 0) if existing else 0
        new = int(target.get(field) or 0)
        if old != new:
            return True
    return False


def sync_drake_forms_from_csv(
    conn: sqlite3.Connection,
    csv_path: str,
    tax_year: int,
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Match Drake rows to returns and normalize return_forms."""
    from drake_importer import _match_return, iter_drake_csv_rows

    updated = 0
    inserted = 0
    unchanged = 0
    skipped_no_match = 0
    schedule_updates = 0
    family_updates = 0
    samples: list[dict[str, Any]] = []

    for _row_num, normalized, _warnings, err in iter_drake_csv_rows(csv_path, tax_year):
        if err or not normalized:
            continue

        match = _match_return(conn, normalized)
        if not match.get("return_id"):
            skipped_no_match += 1
            continue

        return_id = int(match["return_id"])
        client_id = int(match["client_id"])
        existing = fetch_existing_forms(conn, return_id)

        prefill = fetch_prefill_forms_for_client(conn, client_id, tax_year)
        prefill_counts = prefill[0] if prefill else None
        prefill_return_type = prefill[1] if prefill else None

        target = merge_drake_return_forms(
            csm_type_forms=normalized.get("return_forms"),
            prefill_counts=prefill_counts,
            prefill_return_type=prefill_return_type,
            existing=existing,
        )

        if not forms_differ(existing, target):
            unchanged += 1
            continue

        if dry_run:
            if not existing:
                inserted += 1
            else:
                updated += 1
            for field in SCHEDULE_FORM_FIELDS:
                if int(existing.get(field) or 0) != int(target.get(field) or 0):
                    schedule_updates += 1
                    break
            for field in RETURN_FORM_FIELDS:
                if field in SCHEDULE_FORM_FIELDS or field in MANUAL_ONLY_FORM_FIELDS:
                    continue
                if int(existing.get(field) or 0) != int(target.get(field) or 0):
                    family_updates += 1
                    break
            if len(samples) < 15:
                samples.append(
                    {
                        "return_id": return_id,
                        "before": {f: int(existing.get(f) or 0) for f in RETURN_FORM_FIELDS},
                        "after": target,
                    }
                )
            continue

        had_row = bool(existing)
        upsert_return_forms(conn, return_id, target, overwrite=True)
        if had_row:
            updated += 1
        else:
            inserted += 1

    return {
        "dry_run": dry_run,
        "tax_year": tax_year,
        "csv_path": csv_path,
        "updated": updated,
        "inserted": inserted,
        "unchanged": unchanged,
        "skipped_no_match": skipped_no_match,
        "schedule_rows_changed": schedule_updates if dry_run else None,
        "family_rows_changed": family_updates if dry_run else None,
        "samples": samples,
    }
