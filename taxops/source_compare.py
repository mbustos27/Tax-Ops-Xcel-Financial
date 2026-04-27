"""
Compare the live database to the two on-disk source types: manual office log CSV and Drake export.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import INCOMING_DIR, PROCESSED_DIR
from drake_importer import _match_return as drake_match_return, iter_drake_csv_rows
from importer import _match_return as manual_match_return, iter_manual_csv_rows

# (key, label) — values come from flattened dicts with these keys
COMPARE_FIELDS: List[Tuple[str, str]] = [
    ("log_number", "Log #"),
    ("client_status", "Status"),
    ("processor", "Processor"),
    ("last_name", "Last name"),
    ("first_name", "First name"),
    ("intake_date", "Intake"),
    ("logout_date", "Logout"),
    ("updated_date", "Updated"),
    ("date_emailed", "Date emailed"),
    ("pickup_date", "Pickup"),
    ("efile_date", "E-file date"),
    ("ack_date", "Ack date"),
    ("drake_status_raw", "Drake status (raw)"),
    ("verified", "Verified"),
    ("is_extension", "Extension"),
    ("total_fee", "Total fee"),
    ("fee_paid", "Fee paid"),
    ("refund_amount", "Refund"),
]


def _drake_basename_year(name: str) -> int | None:
    m = re.match(r"^drake_(\d{4})\.csv$", name, re.IGNORECASE)
    return int(m.group(1)) if m else None


def is_drake_filename(name: str) -> bool:
    return _drake_basename_year(name) is not None


def is_manual_log_candidate(name: str) -> bool:
    """
    A file suitable as the default office (manual) log: not Drake Tax export,
    not CSM data (those are handled by the Drake importer path).
    """
    if is_drake_filename(name):
        return False
    n = name.lower()
    if n.startswith("csm") or "csmdata" in n:
        return False
    return True


def list_csv_basenames() -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for base in (Path(PROCESSED_DIR), Path(INCOMING_DIR)):
        if not base.is_dir():
            continue
        for f in base.glob("*.csv"):
            if f.name not in seen:
                seen.add(f.name)
                out.append(f.name)
    return sorted(out, key=str.lower)


def safe_resolve_csv(basename: str) -> Path | None:
    """Only allow simple basenames under incoming/ or processed/."""
    if not basename or basename != Path(basename).name:
        return None
    if ".." in basename:
        return None
    for base in (Path(PROCESSED_DIR), Path(INCOMING_DIR)):
        if not base.is_dir():
            continue
        p = (base / basename).resolve()
        b = base.resolve()
        try:
            p.relative_to(b)
        except ValueError:
            continue
        if p.is_file():
            return p
    return None


def _newest_match_across_incoming(
    pred,
) -> Path | None:
    """Newest .csv in incoming/processed for which pred(name) is True."""
    best: Path | None = None
    best_mtime = -1.0
    for base in (Path(PROCESSED_DIR), Path(INCOMING_DIR)):
        if not base.is_dir():
            continue
        for f in base.glob("*.csv"):
            if not pred(f.name):
                continue
            try:
                mt = f.stat().st_mtime
            except OSError:
                continue
            if mt > best_mtime:
                best_mtime = mt
                best = f
    return best


def discover_default_paths(tax_year: int) -> tuple[Path | None, Path | None]:
    """
    Manual: most recently modified .csv that is not drake_YYYY.csv.
    Drake: drake_{year}.csv, else newest drake_*.csv, else newest *csmdata* (CI case).
    """
    drake: Path | None = None
    for base in (Path(PROCESSED_DIR), Path(INCOMING_DIR)):
        if not base.is_dir():
            continue
        cand = base / f"drake_{tax_year}.csv"
        if cand.is_file():
            drake = cand
            break
    if drake is None:
        drake = _newest_match_across_incoming(
            lambda n: n.lower().startswith("drake_") and n.lower().endswith(".csv")
        )
    if drake is None:
        drake = _newest_match_across_incoming(lambda n: "csm" in n.lower() and n.lower().endswith(".csv"))

    manual = _newest_match_across_incoming(is_manual_log_candidate)
    return manual, drake


def _norm_scalar(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        if isinstance(v, float):
            return f"{v:.2f}" if v == v else ""  # NaN
        return str(int(v))
    s = str(v).strip()
    return s


def _flat_from_manual(norm: Dict[str, Any]) -> Dict[str, Any]:
    c = norm.get("clients") or {}
    r = norm.get("returns") or {}
    p = norm.get("payments") or {}
    return {
        "log_number": r.get("log_number"),
        "client_status": r.get("client_status"),
        "processor": r.get("processor"),
        "last_name": c.get("last_name"),
        "first_name": c.get("first_name"),
        "intake_date": r.get("intake_date"),
        "logout_date": r.get("logout_date"),
        "updated_date": r.get("updated_date"),
        "date_emailed": r.get("date_emailed"),
        "pickup_date": r.get("pickup_date"),
        "efile_date": r.get("efile_date"),
        "ack_date": r.get("ack_date"),
        "drake_status_raw": r.get("drake_status_raw"),
        "verified": r.get("verified"),
        "is_extension": r.get("is_extension"),
        "total_fee": p.get("total_fee"),
        "fee_paid": p.get("fee_paid"),
        "refund_amount": p.get("refund_amount"),
    }


def _flat_from_drake(norm: Dict[str, Any]) -> Dict[str, Any]:
    c = norm.get("clients") or {}
    r = norm.get("returns") or {}
    p = norm.get("payments") or {}
    ext = r.get("is_extension")
    return {
        "log_number": r.get("log_number"),
        "client_status": r.get("client_status"),
        "processor": r.get("processor"),
        "last_name": c.get("last_name"),
        "first_name": c.get("first_name"),
        "intake_date": r.get("intake_date"),
        "logout_date": r.get("logout_date"),
        "updated_date": r.get("updated_date"),
        "date_emailed": r.get("date_emailed"),
        "pickup_date": r.get("pickup_date"),
        "efile_date": r.get("efile_date"),
        "ack_date": r.get("ack_date"),
        "drake_status_raw": r.get("drake_status_raw"),
        "verified": r.get("verified"),
        "is_extension": 1 if ext else (0 if ext is not None else None),
        "total_fee": p.get("total_fee"),
        "fee_paid": p.get("fee_paid"),
        "refund_amount": p.get("refund_amount"),
    }


def _flat_from_db(row: sqlite3.Row | Dict[str, Any]) -> Dict[str, Any]:
    d = dict(row)
    return {
        "log_number": d.get("log_number"),
        "client_status": d.get("client_status"),
        "processor": d.get("processor"),
        "last_name": d.get("last_name"),
        "first_name": d.get("first_name"),
        "intake_date": d.get("intake_date"),
        "logout_date": d.get("logout_date"),
        "updated_date": d.get("updated_date"),
        "date_emailed": d.get("date_emailed"),
        "pickup_date": d.get("pickup_date"),
        "efile_date": d.get("efile_date"),
        "ack_date": d.get("ack_date"),
        "drake_status_raw": d.get("drake_status_raw"),
        "verified": d.get("verified"),
        "is_extension": d.get("is_extension"),
        "total_fee": d.get("total_fee"),
        "fee_paid": d.get("fee_paid"),
        "refund_amount": d.get("refund_amount"),
    }


def run_compare(
    conn: sqlite3.Connection,
    tax_year: int,
    manual_path: Path | None,
    drake_path: Path | None,
    only_mismatch: bool = True,
) -> Dict[str, Any]:
    """
    Build a report: each DB return for the year, with side-by-side flat values from both files.
    """
    error: str | None = None
    file_note: str | None = None
    if manual_path is not None and not manual_path.is_file():
        error = f"Manual file not found: {manual_path.name}"
    if drake_path is not None and not drake_path.is_file():
        extra = f"Drake file not found: {drake_path.name}"
        error = f"{error} · {extra}" if error else extra
    if not manual_path and not drake_path and not error:
        file_note = (
            "No source CSVs are available. Place files in data/incoming or data/processed, "
            "or choose filenames below."
        )

    manual_by_rid: Dict[int, Dict[str, Any]] = {}
    manual_orphans: List[Dict[str, Any]] = []

    if not error and manual_path is not None and manual_path.is_file():
        try:
            for row_num, norm, _warn, err in iter_manual_csv_rows(str(manual_path)):
                if err or not norm:
                    manual_orphans.append({"row": row_num, "error": err or "parse", "source": "manual"})
                    continue
                if (norm["returns"].get("tax_year")) != tax_year:
                    continue
                m = manual_match_return(conn, norm)
                if m.get("ambiguous"):
                    manual_orphans.append(
                        {
                            "row": row_num,
                            "error": "ambiguous_match",
                            "source": "manual",
                            "hint": f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")}',
                        }
                    )
                elif m.get("return_id") is None:
                    manual_orphans.append(
                        {
                            "row": row_num,
                            "error": "no_db_match",
                            "source": "manual",
                            "hint": f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")} log={norm["returns"].get("log_number")}',
                        }
                    )
                else:
                    rid = int(m["return_id"])
                    manual_by_rid[rid] = {
                        "row": row_num,
                        "flat": _flat_from_manual(norm),
                    }
        except (OSError, ValueError) as exc:
            file_note = (
                f"Manual file {manual_path.name!r} is not a valid office log (missing required columns, etc.): {exc}"
            )

    drake_by_rid: Dict[int, Dict[str, Any]] = {}
    drake_orphans: List[Dict[str, Any]] = []

    if not error and drake_path is not None and drake_path.is_file():
        try:
            for row_num, norm, _warn, err in iter_drake_csv_rows(str(drake_path), tax_year):
                if err or not norm:
                    drake_orphans.append({"row": row_num, "error": err or "parse", "source": "drake"})
                    continue
                m = drake_match_return(conn, norm)
                if m.get("ambiguous"):
                    drake_orphans.append(
                        {
                            "row": row_num,
                            "error": "ambiguous_match",
                            "source": "drake",
                            "hint": f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")}',
                        }
                    )
                elif m.get("return_id") is None:
                    drake_orphans.append(
                        {
                            "row": row_num,
                            "error": "no_db_match",
                            "source": "drake",
                            "hint": f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")}',
                        }
                    )
                else:
                    rid = int(m["return_id"])
                    drake_by_rid[rid] = {
                        "row": row_num,
                        "flat": _flat_from_drake(norm),
                    }
        except (OSError, ValueError) as e:
            extra = f"Drake/CSM file {drake_path.name!r} could not be read: {e}"
            file_note = f"{file_note} · {extra}" if file_note else extra

    db_rows = conn.execute(
        """
        SELECT
          r.id, r.log_number, r.tax_year, r.client_status, r.processor,
          r.verified, r.intake_date, r.date_emailed, r.pickup_date,
          r.logout_date, r.updated_date,
          r.efile_date, r.ack_date, r.drake_status_raw, r.is_extension,
          c.last_name, c.first_name, c.display_name,
          p.total_fee, p.fee_paid, p.refund_amount
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE r.tax_year = ?
        ORDER BY c.last_name COLLATE NOCASE, c.first_name COLLATE NOCASE, r.id
        """,
        (tax_year,),
    ).fetchall()

    out_rows: List[Dict[str, Any]] = []

    for row in db_rows:
        rowd = dict(row)
        dflat = _flat_from_db(rowd)
        rid = int(rowd["id"])
        m_data = manual_by_rid.get(rid)
        d_data = drake_by_rid.get(rid)
        mflat = m_data["flat"] if m_data else None
        d_dflat = d_data["flat"] if d_data else None

        fields_out: List[Dict[str, Any]] = []
        n_diffs = 0
        for key, label in COMPARE_FIELDS:
            db_s = _norm_scalar(dflat.get(key))
            m_norm = _norm_scalar(mflat.get(key)) if mflat is not None else None
            d_norm = _norm_scalar(d_dflat.get(key)) if d_dflat is not None else None
            m_diff = m_norm is not None and m_norm != db_s
            d_diff = d_norm is not None and d_norm != db_s
            if m_diff or d_diff:
                n_diffs += 1

            fields_out.append(
                {
                    "key": key,
                    "label": label,
                    "db": db_s or "—",
                    "manual": "—" if mflat is None else (m_norm or "—"),
                    "drake": "—" if d_dflat is None else (d_norm or "—"),
                    "m_diff": m_diff,
                    "d_diff": d_diff,
                    "m_missing": mflat is None,
                    "d_missing": d_dflat is None,
                }
            )

        if only_mismatch and n_diffs == 0:
            continue

        last = rowd["last_name"] or ""
        first = rowd["first_name"] or ""
        name = (rowd.get("display_name") or f"{last}, {first}".strip(", ")).strip() or f"Return #{rid}"
        out_rows.append(
            {
                "return_id": rid,
                "name": name,
                "log": rowd["log_number"] or "—",
                "n_diffs": n_diffs,
                "fields": fields_out,
            }
        )

    return {
        "year": tax_year,
        "manual_file": manual_path.name if manual_path and manual_path.is_file() else None,
        "drake_file": drake_path.name if drake_path and drake_path.is_file() else None,
        "manual_path_resolved": str(manual_path) if manual_path and manual_path.is_file() else None,
        "drake_path_resolved": str(drake_path) if drake_path and drake_path.is_file() else None,
        "file_note": file_note,
        "error": error,
        "rows": out_rows,
        "manual_orphans": manual_orphans,
        "drake_orphans": drake_orphans,
        "db_count": len(db_rows),
        "summary": {
            "db_returns": len(db_rows),
            "manual_matched": len(manual_by_rid),
            "drake_matched": len(drake_by_rid),
            "manual_orphan_rows": len(manual_orphans),
            "drake_orphan_rows": len(drake_orphans),
        },
    }
