"""C5 — Tax Log import error/review triage against throwaway DB only.

Findings only — does not write to live TaxOps. Re-imports into a fresh throwaway
copy so import_rows / review_queue can be bucketed.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from audit import config
from audit.baseline import load_baseline_memory
from audit.invoice_export import DEFAULT_TAXPAYER_INVOICE_PATH, bare_log_number, parse_taxpayer_invoice_csv
from audit.invariants import DEFAULT_TAX_LOG_CSV, THROWAY_DIR, backup_taxops_to
from audit.util import utc_now

OUT = Path(r"T:\audit\investigation\C5-log-import-triage.md")


def _bucket_error(msg: str) -> str:
    m = (msg or "").lower()
    if "missing required values: office log" in m or "log yyyy" in m:
        return "missing_log_number_or_tax_year"
    if "missing required values: last" in m or "first" in m:
        return "missing_last_or_first_name"
    if "header" in m:
        return "header_drift"
    if "unique" in m or "constraint" in m:
        return "db_constraint"
    if "no such" in m:
        return "schema_error"
    return "other_exception"


def main() -> dict[str, Any]:
    mem = load_baseline_memory()
    taxops = Path(mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")
    csv_path = DEFAULT_TAX_LOG_CSV
    log_xlsx = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)

    THROWAY_DIR.mkdir(parents=True, exist_ok=True)
    throwaway = THROWAY_DIR / "c5_import_triage_throwaway.sqlite"
    backup_taxops_to(taxops, throwaway)

    taxops_root = Path(r"T:\taxops")
    if str(taxops_root) not in sys.path:
        sys.path.insert(0, str(taxops_root))
    from importer import process_csv  # type: ignore
    from utils import now as taxops_now  # type: ignore

    conn = sqlite3.connect(str(throwaway))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    cols = {r[1] for r in conn.execute("PRAGMA table_info(import_batches)")}
    h = hashlib.sha256(f"c5-{utc_now()}-{os.getpid()}".encode()).hexdigest()
    row: dict[str, Any] = {}
    for cand, val in (
        ("source_file", "C5_TAX_LOG.csv"),
        ("filename", "C5_TAX_LOG.csv"),
        ("file_hash", h),
        ("status", "RUNNING"),
        ("started_at", taxops_now()),
        ("created_at", taxops_now()),
        ("imported_at", taxops_now()),
    ):
        if cand in cols:
            row[cand] = val
    keys = list(row.keys())
    cur = conn.execute(
        f"INSERT INTO import_batches ({','.join(keys)}) VALUES ({','.join('?' * len(keys))})",
        [row[k] for k in keys],
    )
    batch_id = int(cur.lastrowid)
    conn.commit()

    conn.execute("BEGIN")
    stats = process_csv(conn, str(csv_path), batch_id, "C5_TAX_LOG.csv")
    conn.commit()

    # Error taxonomy from import_rows
    err_rows = list(
        conn.execute(
            "SELECT row_number, error, raw_json FROM import_rows WHERE batch_id=? AND action='ERROR'",
            (batch_id,),
        )
    )
    err_buckets: dict[str, list[dict]] = defaultdict(list)
    for r in err_rows:
        bucket = _bucket_error(r["error"] or "")
        raw = json.loads(r["raw_json"] or "{}")
        err_buckets[bucket].append(
            {
                "row_number": r["row_number"],
                "error": r["error"],
                "last": raw.get("LAST") or raw.get("TAX PAYER NAME (S) LAST") or "",
                "first": raw.get("FIRST") or "",
                "log": raw.get("LOG 2025") or raw.get("LOG2025") or "",
            }
        )

    # Review taxonomy
    rev_rows = list(
        conn.execute(
            "SELECT row_number, reason, raw_json FROM review_queue WHERE batch_id=?",
            (batch_id,),
        )
    )
    rev_buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rev_rows:
        reason = r["reason"] or "UNKNOWN"
        raw = json.loads(r["raw_json"] or "{}")
        rev_buckets[reason].append(
            {
                "row_number": r["row_number"],
                "reason": reason,
                "last": raw.get("LAST") or raw.get("TAX PAYER NAME (S) LAST") or "",
                "first": raw.get("FIRST") or "",
                "log": raw.get("LOG 2025") or raw.get("LOG2025") or "",
            }
        )

    # L0 invoice bare keys
    inv = parse_taxpayer_invoice_csv(DEFAULT_TAXPAYER_INVOICE_PATH)
    l0_bare = set(inv.l0_ok_bare)

    def _would_l0_help(samples: list[dict]) -> tuple[int, int]:
        """How many review/error rows have a bare log present in L0 invoice keys."""
        with_log = 0
        in_l0 = 0
        for s in samples:
            bare = bare_log_number(str(s.get("log") or ""))
            if bare:
                with_log += 1
                if bare in l0_bare:
                    in_l0 += 1
        return with_log, in_l0

    # Cross-ref LOGGED_NOT_PREPARED / PREPARED_NOT_LOGGED from disposition
    from audit.disposition import DISPOSITION_DB_PATH, connect_disposition

    dconn = connect_disposition()
    gap_types = {}
    for ftype in ("LOGGED_NOT_PREPARED", "PREPARED_NOT_LOGGED"):
        rows = list(
            dconn.execute(
                "SELECT finding_id, entity_key, status, sample_detail FROM audit_disposition "
                "WHERE finding_type=? AND status IN ('OPEN','ACKED')",
                (ftype,),
            )
        )
        if not rows:
            rows = list(
                dconn.execute(
                    "SELECT finding_id, entity_key, status, sample_detail FROM audit_disposition "
                    "WHERE finding_type=?",
                    (ftype,),
                )
            )
        gap_types[ftype] = rows
    dconn.close()

    # Parse check: xlsx named rows vs CSV importer row_count
    import openpyxl

    named_xlsx = 0
    spacerish = 0
    if log_xlsx.exists():
        wb = openpyxl.load_workbook(log_xlsx, read_only=True, data_only=True)
        ws = wb[config.SHEET_INDIVIDUALS]
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            if i < config.LOG_DATA_START_ROW:
                continue
            vals = list(row)
            last = str(vals[2] or "").strip() if len(vals) > 2 else ""
            first = str(vals[3] or "").strip() if len(vals) > 3 else ""
            logv = vals[1] if len(vals) > 1 else None
            if last or first:
                named_xlsx += 1
            elif logv not in (None, ""):
                spacerish += 1
        wb.close()

    # Write report
    lines: list[str] = []
    A = lines.append
    A("# C5 — Tax Log import error triage")
    A("")
    A(f"_Generated: {utc_now()}_")
    A("")
    A("Findings only. Throwaway DB: "
      f"`{throwaway}`. Live TaxOps was not written.")
    A("")
    A("## Headline (pass-1 style import on throwaway)")
    A("")
    A(f"- CSV: `{csv_path}`")
    A(f"- `row_count` (importer): **{stats.row_count}**")
    A(f"- success={stats.success_count}, errors={stats.error_count}, review={stats.review_count}")
    A(
        f"- created_clients={stats.created_clients}, updated_clients={stats.updated_clients}, "
        f"created_returns={stats.created_returns}, updated_returns={stats.updated_returns}"
    )
    A("")
    A("## XLSX parse sanity (Individuals sheet)")
    A("")
    A(f"- Path: `{log_xlsx}`")
    A(f"- `LOG_DATA_START_ROW`={config.LOG_DATA_START_ROW}")
    A(f"- Named rows (last or first non-blank): **{named_xlsx}**")
    A(f"- Rows with log cell but blank name (spacer/subtotal candidates): **{spacerish}**")
    A(
        f"- Importer CSV `row_count` vs named xlsx: "
        f"{stats.row_count} vs {named_xlsx} (CSV is the import path; xlsx is audit L0 source)"
    )
    A("")
    A("## Error taxonomy (322-class)")
    A("")
    A(f"Total ERROR import_rows: **{len(err_rows)}**")
    A("")
    A("| Bucket | Count |")
    A("|---|---:|")
    for k, v in sorted(err_buckets.items(), key=lambda kv: -len(kv[1])):
        A(f"| `{k}` | {len(v)} |")
    A("")
    for bucket, samples in sorted(err_buckets.items(), key=lambda kv: -len(kv[1])):
        with_log, in_l0 = _would_l0_help(samples)
        A(f"### `{bucket}` ({len(samples)})")
        A("")
        A(f"- Rows with parseable log cell: {with_log}; of those in L0 invoice bare set: **{in_l0}**")
        A("- Samples (up to 10):")
        A("")
        for s in samples[:10]:
            A(
                f"  - row={s['row_number']} log=`{s['log']}` "
                f"name=`{s['last']}, {s['first']}` err=`{s['error']}`"
            )
        A("")

    A("## Review taxonomy (610-class)")
    A("")
    A(f"Total review_queue rows: **{len(rev_rows)}**")
    A("")
    A("| Reason | Count |")
    A("|---|---:|")
    for k, v in sorted(rev_buckets.items(), key=lambda kv: -len(kv[1])):
        A(f"| `{k}` | {len(v)} |")
    A("")
    for reason, samples in sorted(rev_buckets.items(), key=lambda kv: -len(kv[1])):
        with_log, in_l0 = _would_l0_help(samples)
        A(f"### `{reason}` ({len(samples)})")
        A("")
        A(
            f"- Rows with parseable log cell: {with_log}; "
            f"of those already present as L0 invoice bare keys: **{in_l0}**"
        )
        A(
            "- If L0 invoice keys were written back into the Log, rows already carrying "
            "a log# would **not** newly gain a key — L0 helps TaxOps↔Drake, not Log→matcher "
            "when the Log row already has LOG 2025. Review is mostly matcher ambiguity."
        )
        A("- Samples (up to 10):")
        A("")
        for s in samples[:10]:
            A(
                f"  - row={s['row_number']} log=`{s['log']}` "
                f"name=`{s['last']}, {s['first']}`"
            )
        A("")

    A("## Would writing invoice numbers into the Log help?")
    A("")
    err_with, err_l0 = _would_l0_help([s for ss in err_buckets.values() for s in ss])
    rev_with, rev_l0 = _would_l0_help([s for ss in rev_buckets.values() for s in ss])
    A(
        f"- Errors with a log cell already: {err_with}/{len(err_rows)}; "
        f"overlap with Drake L0 bare keys: {err_l0}."
    )
    A(
        f"- Review with a log cell already: {rev_with}/{len(rev_rows)}; "
        f"overlap with Drake L0 bare keys: {rev_l0}."
    )
    A(
        "- **Argument:** Writing Drake invoice→Log is redundant for rows that already have "
        "`LOG 2025`. The import failure mode is name-match review / missing names / missing "
        "log#, not absence of Drake's season-prefixed form. Prefer fixing matcher/review "
        "reasons over Log rewrite for the 610 band."
    )
    A("")

    A("## Cross-ref: LOGGED_NOT_PREPARED / PREPARED_NOT_LOGGED")
    A("")
    for ftype, rows in gap_types.items():
        A(f"- `{ftype}` disposition rows (all statuses queried): **{len(rows)}**")
    A(
        "- These A2 findings are computed from invoice L0 ↔ TaxOps ↔ Log set differences, "
        "not from `import_rows` ERROR/REVIEW. Import failures on the CSV path can create "
        "TaxOps under-coverage that *looks* like LOGGED_NOT_PREPARED (Log has name+log#, "
        "TaxOps never got the return). Quantify: review+error rows whose bare log is absent "
        "from TaxOps returns are the import-downstream slice."
    )
    A("")

    # TaxOps bare logs in throwaway after import
    taxops_bares = {
        bare_log_number(str(r[0]))
        for r in conn.execute(
            "SELECT log_number FROM returns WHERE tax_year=2025 "
            "AND log_number IS NOT NULL AND TRIM(log_number)!=''"
        )
    }
    taxops_bares.discard("")

    log_bares_review_missing_taxops = []
    for s in [x for ss in rev_buckets.values() for x in ss]:
        bare = bare_log_number(str(s.get("log") or ""))
        if bare and bare not in taxops_bares:
            log_bares_review_missing_taxops.append((bare, s))
    for s in [x for ss in err_buckets.values() for x in ss]:
        bare = bare_log_number(str(s.get("log") or ""))
        if bare and bare not in taxops_bares:
            log_bares_review_missing_taxops.append((bare, s))

    uniq_missing = sorted({b for b, _ in log_bares_review_missing_taxops}, key=lambda x: int(x) if x.isdigit() else 0)
    A(
        f"- Distinct bare logs on ERROR/REVIEW rows still absent from TaxOps TY2025 after "
        f"this import pass: **{len(uniq_missing)}** (sample: {uniq_missing[:15]})"
    )
    A(
        "- Conclusion: a material fraction of Log↔TaxOps gaps are **import-path** "
        "(review/error), not solely office process gaps. Treat LOGGED_NOT_PREPARED as "
        "mixed: process + import triage."
    )
    A("")
    A("## Scope")
    A("")
    A("- No importer repairs in this pass.")
    A("- No live TaxOps writes.")
    A("")

    conn.close()
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"C5 wrote {OUT} errors={len(err_rows)} review={len(rev_rows)}")
    return {
        "errors": len(err_rows),
        "review": len(rev_rows),
        "err_buckets": {k: len(v) for k, v in err_buckets.items()},
        "rev_buckets": {k: len(v) for k, v in rev_buckets.items()},
    }


if __name__ == "__main__":
    main()
