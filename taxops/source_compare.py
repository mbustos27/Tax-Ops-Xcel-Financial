"""
Compare the live database to the two on-disk source types: manual office log CSV and Drake export.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import INCOMING_DIR, PROCESSED_DIR
from normalizer import canonical_status, is_locked_status
from drake_importer import _match_return as drake_match_return, iter_drake_csv_rows
from importer import _match_return as manual_match_return
from importer import fetch_returns_clients_for_tax_year, iterate_manual_prep, prepare_manual_csv

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
    ("balance_due",   "Balance Due (IRS)"),
]

# Fields where Drake's value is unreliable / non-standard — never flag Drake as
# a diff source and never offer a Drake apply button.  Status in particular uses
# Drake's own internal wording (e.g. "E-Filed: YES") which has no direct mapping.
# Manual log is the single source of truth for these fields.
_MANUAL_ONLY_FIELDS: frozenset[str] = frozenset({"client_status", "drake_status_raw"})

# drake_status_raw is display-only — shown so staff can read it but never
# treated as differing from DB (it's Drake's format, not ours).
_DISPLAY_ONLY_FIELDS: frozenset[str] = frozenset({"drake_status_raw"})

# Priority source for each field when both manual and Drake differ from the DB.
# Possible values:
#   "manual"  – manual log is authoritative (e.g. fee_paid, processor)
#   "drake"   – Drake/CSM is authoritative (e.g. refund amounts)
#   "latest"  – whichever source has the more recent date wins (event dates)
#   "spouse"  – prefer whichever value contains a spouse indicator (&); fall
#               back to "drake" when neither or both have it (names)
#   "none"    – no automatic recommendation (info-only / read-only fields)
_FIELD_PRIORITY: Dict[str, str] = {
    "log_number":     "manual",
    "client_status":  "manual",
    "processor":      "manual",
    "last_name":      "spouse",
    "first_name":     "spouse",
    "intake_date":    "manual",   # set once at intake; manual log is definitive
    "logout_date":    "latest",   # whichever recorded the logout more recently
    "updated_date":   "latest",   # most recent update wins
    "date_emailed":   "latest",
    "pickup_date":    "latest",
    "efile_date":     "latest",   # Drake usually has this but latest still wins
    "ack_date":       "latest",
    "drake_status_raw": "none",
    "verified":       "drake",
    "is_extension":   "drake",
    "total_fee":      "manual",
    "fee_paid":       "manual",
    "refund_amount":  "drake",
    "balance_due":    "drake",
}

# Date formats to try when parsing a value for "latest" comparison
_DATE_PARSE_FMTS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d")


def _parse_date_str(val: str):
    """Return a comparable date object from a string, or None if unparseable."""
    from datetime import date as _date
    for fmt in _DATE_PARSE_FMTS:
        try:
            from datetime import datetime
            return datetime.strptime(val.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


def _recommended_source(key: str, m_val: str, d_val: str) -> str:
    """Return 'manual', 'drake', or '' (no recommendation) for a differing field.

    *m_val* and *d_val* are the already-normalised values from each source.
    An empty-string value means the source has nothing to say about this field.
    """
    rule = _FIELD_PRIORITY.get(key, "manual")
    if rule == "none":
        return ""
    if rule == "latest":
        # If only one source has a value, prefer it.
        if not m_val and d_val:
            return "drake"
        if m_val and not d_val:
            return "manual"
        # Both have a value — pick the more recent date.
        m_date = _parse_date_str(m_val)
        d_date = _parse_date_str(d_val)
        if m_date and d_date:
            return "manual" if m_date >= d_date else "drake"
        # Unparseable — fall back to manual as tiebreaker for event dates
        return "manual"
    if rule == "spouse":
        # Score each name by completeness:
        #   +10  for containing a spouse indicator (&)
        #   +1   per name token (words)
        # More complete name wins; Drake breaks ties (more reliable spelling).
        def _name_score(v: str) -> int:
            s = 10 if "&" in v else 0
            s += len(v.split())
            return s
        ms, ds = _name_score(m_val), _name_score(d_val)
        if ms > ds:
            return "manual"
        if ds > ms:
            return "drake"
        return "drake"   # equal completeness — Drake spelling is authoritative
    return rule


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


_STATUS_FIELDS: frozenset[str] = frozenset({"client_status", "drake_status_raw"})


def _norm_scalar(v: Any, field: str = "") -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        if isinstance(v, float):
            return f"{v:.2f}" if v == v else ""  # NaN
        return str(int(v))
    s = str(v).strip()
    if field in _STATUS_FIELDS:
        return canonical_status(s) or s
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
        "balance_due": p.get("balance_due"),
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
        "balance_due": p.get("balance_due"),
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
        "balance_due": d.get("balance_due"),
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
    manual_orphans_other_ty: List[Dict[str, Any]] = []
    manual_orphan_parse = 0
    manual_orphan_ambiguous = 0
    manual_orphan_no_db = 0
    manual_other_ty_ambiguous = 0
    manual_other_ty_no_db = 0
    manual_rows_matched_other_years = 0

    manual_review_pending_rows = 0

    if not error and manual_path is not None and manual_path.is_file():
        try:
            prep = prepare_manual_csv(str(manual_path), tax_year)
            manual_returns_cache: Dict[int, List[Any]] = {}
            if prep.fallback_note:
                file_note = prep.fallback_note if not file_note else f"{prep.fallback_note}\n\n{file_note}"
            for row_num, norm, _warn, err in iterate_manual_prep(prep):
                if err or not norm:
                    manual_orphans.append({"row": row_num, "error": err or "parse", "source": "manual"})
                    manual_orphan_parse += 1
                    continue

                row_ty = norm["returns"].get("tax_year")
                prefetch = (
                    manual_returns_cache.setdefault(row_ty, fetch_returns_clients_for_tax_year(conn, row_ty))
                    if row_ty is not None
                    else None
                )
                m = manual_match_return(conn, norm, prefetch)

                if m.get("ambiguous"):
                    hint = f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")}'
                    base = {"row": row_num, "error": "ambiguous_match", "source": "manual", "hint": hint}
                    if row_ty == tax_year:
                        manual_orphans.append(base)
                        manual_orphan_ambiguous += 1
                    else:
                        manual_orphans_other_ty.append({**base, "row_ty": row_ty})
                        manual_other_ty_ambiguous += 1
                    continue

                if m.get("needs_review"):
                    reason = m.get("review_reason") or "REVIEW"
                    hint = f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")}'
                    hint = hint + (f"; log #{norm['returns'].get('log_number')}" if norm["returns"].get("log_number") else "")
                    base = {
                        "row": row_num,
                        "error": f"manual_{reason}",
                        "source": "manual",
                        "hint": hint,
                    }
                    if row_ty == tax_year:
                        manual_orphans.append(base)
                        manual_review_pending_rows += 1
                    else:
                        manual_orphans_other_ty.append({**base, "row_ty": row_ty})
                    continue

                if m.get("return_id") is None:
                    hint = (
                        f'{norm["clients"].get("last_name")}, {norm["clients"].get("first_name")} '
                        f"log={norm['returns'].get('log_number')}"
                    )
                    base = {"row": row_num, "error": "no_db_match", "source": "manual", "hint": hint}
                    if row_ty == tax_year:
                        manual_orphans.append(base)
                        manual_orphan_no_db += 1
                    else:
                        manual_orphans_other_ty.append({**base, "row_ty": row_ty})
                        manual_other_ty_no_db += 1
                    continue

                rid = int(m["return_id"])
                if row_ty == tax_year:
                    manual_by_rid[rid] = {
                        "row": row_num,
                        "flat": _flat_from_manual(norm),
                    }
                else:
                    manual_rows_matched_other_years += 1
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
          p.total_fee, p.fee_paid, p.refund_amount, p.balance_due
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

        # Special alert: Drake has a logout_date but DB status is not LOG OUT.
        # This is the one date-driven signal we surface from Drake even though
        # we ignore its status field entirely.
        drake_logout_date = _norm_scalar(d_dflat.get("logout_date") if d_dflat else None)
        db_status = _norm_scalar(dflat.get("client_status"), "client_status")
        logout_date_alert = (
            bool(drake_logout_date)
            and db_status not in ("LOG OUT", "")
        )

        fields_out: List[Dict[str, Any]] = []
        n_diffs = 0
        n_conflicts = 0
        # recommended_fields: {field_key: value} to apply via "Apply recommended"
        recommended_fields: Dict[str, str] = {}
        for key, label in COMPARE_FIELDS:
            db_s = _norm_scalar(dflat.get(key), key)
            m_norm = _norm_scalar(mflat.get(key), key) if mflat is not None else None
            d_norm_raw = _norm_scalar(d_dflat.get(key), key) if d_dflat is not None else None

            # For manual-only fields Drake is never a source of truth.
            d_norm = None if key in _MANUAL_ONLY_FIELDS else d_norm_raw

            m_diff = m_norm is not None and m_norm != "" and m_norm != db_s
            # Drake diffs only for non-manual-only, non-display-only fields
            d_diff = (
                key not in _MANUAL_ONLY_FIELDS
                and d_norm is not None
                and d_norm != ""
                and d_norm != db_s
            )

            # CANCELLED DB status is terminal — lock this field regardless of sources
            if key == "client_status" and is_locked_status(db_s):
                sync_state = "cancelled_lock"
                rec_source = ""
                rec_val = ""
                fields_out.append({
                    "key": key, "label": label,
                    "db": db_s or "—",
                    "manual": "—" if mflat is None else (m_norm or "—"),
                    "drake": "—" if d_dflat is None else (d_norm_raw or "—"),
                    "m_diff": False, "d_diff": False,
                    "m_missing": mflat is None, "d_missing": d_dflat is None,
                    "sync_state": "cancelled_lock",
                    "m_raw": "", "d_raw": "",
                    "rec_source": "", "rec_val": "",
                })
                continue

            # Classify sync state
            if key in _DISPLAY_ONLY_FIELDS:
                sync_state = "info_only"
            elif not m_diff and not d_diff:
                sync_state = "in_sync"
            elif m_diff and d_diff:
                sync_state = "conflict" if m_norm != d_norm else "both_agree_vs_db"
                if sync_state == "conflict":
                    n_conflicts += 1
            elif m_diff:
                sync_state = "manual_only"
            else:
                sync_state = "drake_only"

            if sync_state not in ("in_sync", "info_only"):
                n_diffs += 1

            # Determine the recommended source and value for this field.
            # Conflicts always need human review — we never auto-recommend there.
            rec_source = ""
            rec_val = ""
            if sync_state in ("manual_only", "drake_only", "both_agree_vs_db"):
                mv = m_norm or ""
                dv = d_norm or ""
                if sync_state == "manual_only":
                    rec_source = "manual"
                    rec_val = mv
                elif sync_state == "drake_only":
                    rec_source = "drake"
                    rec_val = dv
                else:  # both_agree_vs_db — either value works, use manual as tiebreak
                    rec_source = _recommended_source(key, mv, dv)
                    rec_val = mv if rec_source == "manual" else dv
            elif sync_state == "conflict":
                # Use priority rules to suggest a winner, but mark as "review"
                mv = m_norm or ""
                dv = d_norm or ""
                rs = _recommended_source(key, mv, dv)
                if rs:
                    rec_source = rs + "_conflict"   # flag: suggestion, not auto
                    rec_val = mv if rs == "manual" else dv

            # Collect fields that can be auto-applied (no conflicts)
            if rec_source in ("manual", "drake") and rec_val:
                recommended_fields[key] = {"source": rec_source, "value": rec_val}

            fields_out.append(
                {
                    "key": key,
                    "label": label,
                    "db": db_s or "—",
                    "manual": "—" if mflat is None else (m_norm or "—"),
                    # For display-only fields show Drake's raw value without diff style
                    "drake": "—" if d_dflat is None else (d_norm_raw or "—"),
                    "m_diff": m_diff,
                    "d_diff": d_diff,
                    "m_missing": mflat is None,
                    "d_missing": d_dflat is None,
                    "sync_state": sync_state,
                    "m_raw": m_norm or "",
                    "d_raw": d_norm or "",
                    "rec_source": rec_source,
                    "rec_val": rec_val,
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
                "n_conflicts": n_conflicts,
                "n_auto": len(recommended_fields),
                "logout_date_alert": logout_date_alert,
                "recommended_fields": recommended_fields,
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
        "manual_orphans_other_ty": manual_orphans_other_ty,
        "drake_orphans": drake_orphans,
        "db_count": len(db_rows),
        "summary": {
            "db_returns": len(db_rows),
            "manual_matched": len(manual_by_rid),
            "manual_rows_matched_other_years": manual_rows_matched_other_years,
            "drake_matched": len(drake_by_rid),
            "manual_orphan_rows": len(manual_orphans),
            "manual_orphan_parse": manual_orphan_parse,
            "manual_orphan_ambiguous": manual_orphan_ambiguous,
            "manual_orphan_no_db": manual_orphan_no_db,
            "manual_review_pending_rows": manual_review_pending_rows,
            "manual_orphans_other_ty_rows": len(manual_orphans_other_ty),
            "manual_other_ty_ambiguous": manual_other_ty_ambiguous,
            "manual_other_ty_no_db": manual_other_ty_no_db,
            "drake_orphan_rows": len(drake_orphans),
        },
    }
