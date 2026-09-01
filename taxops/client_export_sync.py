"""Sync TaxOps from Drake client-export CSV (Engagement Status + Client Type)."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any

from drake_form_sync import (
    fetch_existing_forms,
    merge_drake_return_forms,
    upsert_return_forms,
)
from drake_importer import _match_return, _split_client_name
from engagement_status_rules import (
    CLIENT_TYPE_ENTITY_FORM,
    engagement_to_drake_status_raw,
)
from efile_logout_sync import maybe_sync_efile_logout, revert_premature_logouts, sync_efile_accepted_logouts
from normalizer import normalize_string
from preparer import normalize_preparer
from utils import now

_CLIENT_EXPORT_HEADERS = frozenset(
    {
        "CLIENT ID",
        "DISPLAY NAME",
        "CLIENT TYPE",
        "ENGAGEMENT STATUS",
        "TAX ID (LAST 4)",
    }
)


def _header_key(row: dict[str, str]) -> str:
    return next(iter(row.keys())).strip().lower() if row else ""


def is_client_export_csv(csv_path: str | Path) -> bool:
    path = Path(csv_path)
    if not path.is_file():
        return False
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            return False
        cols = {c.strip().upper() for c in reader.fieldnames if c}
        return _CLIENT_EXPORT_HEADERS.issubset(cols)


def _col(row: dict[str, str], name: str) -> str:
    for k, v in row.items():
        if k and k.strip().upper() == name.upper():
            return (v or "").strip()
    return ""


def normalize_client_export_row(
    row: dict[str, str],
    *,
    tax_year: int,
) -> dict[str, Any] | None:
    display = normalize_string(_col(row, "Display Name"))
    if not display:
        return None

    client_type = _col(row, "Client Type")
    if client_type == "Individual":
        last_name, first_name = _split_client_name(display)
    else:
        last_name, first_name = display, None

    if not last_name:
        return None

    ssn_raw = _col(row, "Tax ID (Last 4)")
    ssn_digits = "".join(c for c in ssn_raw if c.isdigit())
    ssn_last4 = ssn_digits[-4:] if len(ssn_digits) >= 4 else None

    engagement = _col(row, "Engagement Status")
    drake_raw = engagement_to_drake_status_raw(engagement)

    type_forms: dict[str, int] = {}
    entity_col = CLIENT_TYPE_ENTITY_FORM.get(client_type)
    if entity_col:
        type_forms[entity_col] = 1

    owner = normalize_preparer(_col(row, "Owner"))

    return {
        "clients": {
            "last_name": last_name,
            "first_name": first_name,
            "ssn_last4": ssn_last4,
            "taxpayer_email": normalize_string(_col(row, "Primary Email")) or None,
            "taxpayer_cell": normalize_string(_col(row, "Primary Phone")) or None,
        },
        "returns": {
            "tax_year": int(tax_year),
            "processor": owner,
            "drake_status_raw": drake_raw,
        },
        "return_forms": type_forms,
        "engagement_status": engagement or None,
        "client_type": client_type or None,
    }


def sync_client_export(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    *,
    tax_year: int = 2025,
    entity_only: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Apply client-export engagement status (+ entity forms) to matched TY returns."""
    path = Path(csv_path)
    if not is_client_export_csv(path):
        raise ValueError(f"Not a Drake client-export CSV: {path}")

    updated_status = 0
    updated_forms = 0
    logout_synced = 0
    skipped_no_match = 0
    skipped_not_entity = 0
    samples: list[dict[str, Any]] = []

    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            norm = normalize_client_export_row(row, tax_year=tax_year)
            if not norm:
                continue

            is_entity = bool(norm.get("return_forms"))
            if entity_only and not is_entity:
                skipped_not_entity += 1
                continue

            match = _match_return(conn, norm)
            if not match.get("return_id") or match.get("ambiguous"):
                skipped_no_match += 1
                continue

            rid = int(match["return_id"])
            drake_raw = norm["returns"].get("drake_status_raw")
            if not drake_raw and not norm.get("return_forms"):
                continue

            before = conn.execute(
                "SELECT client_status, drake_status_raw FROM returns WHERE id=?",
                (rid,),
            ).fetchone()

            if dry_run:
                if len(samples) < 25:
                    samples.append(
                        {
                            "return_id": rid,
                            "name": norm["clients"].get("last_name"),
                            "engagement": norm.get("engagement_status"),
                            "drake_raw": drake_raw,
                            "from_status": before["client_status"] if before else None,
                            "from_drake": before["drake_status_raw"] if before else None,
                        }
                    )
                if drake_raw or norm.get("return_forms"):
                    updated_status += 1
                continue

            if drake_raw:
                conn.execute(
                    "UPDATE returns SET drake_status_raw = ?, updated_at = ? WHERE id = ?",
                    (drake_raw, now(), rid),
                )
                updated_status += 1

            type_forms = norm.get("return_forms") or {}
            if type_forms:
                existing = fetch_existing_forms(conn, rid)
                merged = merge_drake_return_forms(
                    csm_type_forms=type_forms,
                    prefill_counts=None,
                    prefill_return_type=None,
                    existing=existing,
                )
                upsert_return_forms(conn, rid, merged, overwrite=True)
                updated_forms += 1

            if maybe_sync_efile_logout(conn, rid, source="CLIENT_EXPORT"):
                logout_synced += 1

    if not dry_run:
        sync_efile_accepted_logouts(conn, tax_year=tax_year, dry_run=False)
        revert_premature_logouts(conn, tax_year=tax_year, dry_run=False)

    return {
        "dry_run": dry_run,
        "tax_year": tax_year,
        "entity_only": entity_only,
        "updated_status": updated_status,
        "updated_forms": updated_forms,
        "logout_synced": logout_synced,
        "skipped_no_match": skipped_no_match,
        "skipped_not_entity": skipped_not_entity,
        "samples": samples,
    }
