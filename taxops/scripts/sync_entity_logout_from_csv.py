"""Entity LOG OUT must match Drake CSMDATA EF Accepted — CSV is source of truth."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from config import DRAKE_STATUS_MAP
from db import get_connection
from drake_importer import _match_return, iter_drake_csv_rows
from engagement_status_rules import drake_indicates_efile_complete
from utils import now

DB = "T:/taxops/taxops.db"
CSV = str(_TAXOPS / "data" / "processed" / "CSMDATA.csv")


def _csm_type(forms: dict) -> str | None:
    if forms.get("form_1120s"):
        return "1120S"
    if forms.get("form_1120"):
        return "1120"
    if forms.get("form_1065_llc"):
        return "1065"
    if forms.get("form_990_1041"):
        return "990/1041"
    return None


def _target_from_csv(csv_raw: str | None, db_raw: str | None) -> str:
    raw = (csv_raw or db_raw or "").strip()
    if not raw:
        return "PROCESSING"
    mapped = DRAKE_STATUS_MAP.get(raw.upper(), "PROCESSING")
    return "PROCESSING" if mapped == "LOG OUT" and not drake_indicates_efile_complete(raw) else mapped


def find_invalid_entity_logouts(conn, tax_year: int) -> list[dict]:
    invalid: list[dict] = []
    for _n, norm, _w, err in iter_drake_csv_rows(CSV, tax_year):
        if err or not norm:
            continue
        forms = norm.get("return_forms") or {}
        et = _csm_type(forms)
        if not et:
            continue
        csv_raw = (norm["returns"].get("drake_status_raw") or "").strip()
        match = _match_return(conn, norm)
        if not match.get("return_id"):
            continue
        rid = int(match["return_id"])
        row = conn.execute(
            """
            SELECT r.id, r.log_number, r.client_status, r.drake_status_raw, r.ack_date,
                   c.last_name, c.first_name
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE r.id = ?
            """,
            (rid,),
        ).fetchone()
        if not row:
            continue
        d = dict(row)
        if (d["client_status"] or "").strip().upper() != "LOG OUT":
            continue
        if drake_indicates_efile_complete(csv_raw):
            continue
        if (d.get("ack_date") or "").strip() and not csv_raw:
            continue
        d["entity"] = et
        d["csv_status"] = csv_raw or None
        d["target_status"] = _target_from_csv(csv_raw, d.get("drake_status_raw"))
        invalid.append(d)

    # TaxOps entity LOG OUT not in CSV — only valid if DB proves e-file
    orphan_rows = conn.execute(
        """
        SELECT r.id, r.log_number, r.client_status, r.drake_status_raw, r.ack_date,
               c.last_name, c.first_name,
               rf.form_1120, rf.form_1120s, rf.form_1065_llc, rf.form_990_1041
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        LEFT JOIN return_forms rf ON rf.return_id = r.id
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) = 'LOG OUT'
          AND COALESCE(c.is_test, 0) = 0
          AND (
            COALESCE(rf.form_1120, 0) = 1 OR COALESCE(rf.form_1120s, 0) = 1
            OR COALESCE(rf.form_1065_llc, 0) = 1 OR COALESCE(rf.form_990_1041, 0) = 1
          )
        ORDER BY c.last_name COLLATE NOCASE
        """,
        (tax_year,),
    ).fetchall()
    seen = {x["id"] for x in invalid}
    for row in orphan_rows:
        d = dict(row)
        if d["id"] in seen:
            continue
        if _csm_type(
            {
                "form_1120": d.get("form_1120"),
                "form_1120s": d.get("form_1120s"),
                "form_1065_llc": d.get("form_1065_llc"),
                "form_990_1041": d.get("form_990_1041"),
            }
        ) is None:
            continue
        db_ok = drake_indicates_efile_complete(d.get("drake_status_raw")) or bool(
            (d.get("ack_date") or "").strip()
        )
        if db_ok:
            continue
        et = _csm_type(
            {
                "form_1120": d.get("form_1120"),
                "form_1120s": d.get("form_1120s"),
                "form_1065_llc": d.get("form_1065_llc"),
                "form_990_1041": d.get("form_990_1041"),
            }
        )
        d["entity"] = et
        d["csv_status"] = None
        d["target_status"] = _target_from_csv(None, d.get("drake_status_raw"))
        invalid.append(d)

    invalid.sort(key=lambda x: ((x.get("last_name") or "").upper(), (x.get("first_name") or "").upper()))
    return invalid


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tax-year", type=int, default=2025)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    conn = get_connection(DB)
    invalid = find_invalid_entity_logouts(conn, int(args.tax_year))
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] Entity LOG OUT without CSV EF Accepted: {len(invalid)}")
    for x in invalid:
        log_disp = x["log_number"] if x["log_number"] else f"id={x['id']}"
        print(
            f"  {log_disp:>6} | {x['last_name']}, {x['first_name'] or ''} | {x['entity']} | "
            f"LOG OUT -> {x['target_status']} | db={x['drake_status_raw']!r} | csv={x['csv_status']!r}"
        )

    if not args.apply or not invalid:
        conn.close()
        if not args.apply and invalid:
            print("\nRe-run with --apply to commit.")
        return

    conn.execute("BEGIN")
    ts = now()
    for x in invalid:
        rid = int(x["id"])
        target = x["target_status"]
        conn.execute(
            "UPDATE returns SET client_status = ?, updated_at = ? WHERE id = ?",
            (target, ts, rid),
        )
        if x.get("csv_status"):
            conn.execute(
                "UPDATE returns SET drake_status_raw = ?, updated_at = ? WHERE id = ?",
                (x["csv_status"], ts, rid),
            )
        conn.execute(
            """
            INSERT INTO status_events
              (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
            VALUES (?, 'STATUS_CHANGED', ?, ?, ?, ?, ?)
            """,
            (
                rid,
                "LOG OUT",
                target,
                ts,
                "ENTITY_LOGOUT_SYNC",
                "Reverted LOG OUT — Drake CSV does not show full e-file acceptance",
            ),
        )
    conn.commit()
    print(f"\nUpdated {len(invalid)} entity returns.")
    conn.close()


if __name__ == "__main__":
    main()
