"""Stage B — Phase 3 COALESCE dry-run on a throwaway copy of taxops.db.

Applies v28 DDL to the COPY only, COALESCE-fills joinable clients from TAXPAYER.csv,
writes client_profile_backfill_history, verifies reconstruct on a sample.
NEVER touches live taxops.db.
"""
from __future__ import annotations

import csv
import json
import re
import shutil
import sqlite3
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audit.profile_join import resolve_strict_joins  # noqa: E402
from audit.util import sha256_file  # noqa: E402

LIVE = Path(r"T:\taxops\taxops.db")
EXPORT = Path(r"T:\audit\investigation\exports\TAXPAYER.csv")
OUT_DIR = Path(r"T:\audit\tmp")
OUT_MD = Path(r"T:\audit\investigation\R1-phase3-throwaway-dryrun.md")
OUT_JSON = Path(r"T:\audit\investigation\R1-phase3-throwaway-dryrun.json")
TAX_YEAR = 2025
STRICT = re.compile(r"^25\d{4}$")
CONTAM = {1670, 1583, 902, 327, 237, 878}
RUN_LABEL = "r1-phase3-throwaway-2026-08-13"
SHA = "33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8"

# client profile fields we COALESCE from this export
FIELD_MAP = [
    # (client_col, export_key)
    ("taxpayer_email", "email"),
    ("taxpayer_cell", "phone"),
    ("taxpayer_dob", "dob"),
    ("address_street", "street"),
    ("address_city", "city"),
    ("address_state", "state"),
    ("address_zip", "zip"),
    ("address_county", "county"),
    ("spouse_dob", "spouse_dob"),
    ("spouse_cell", "spouse_phone"),
]


def _cell(row, idx, col):
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _empty(v) -> bool:
    return v is None or str(v).strip() == ""


def load_collapsed():
    rows = list(csv.reader(EXPORT.open(encoding="utf-8-sig", newline="")))
    header = rows[2]
    idx = {h: i for i, h in enumerate(header)}
    by_inv = defaultdict(list)
    for n, row in enumerate(rows[3:], start=4):
        if not row or len(row) < 5:
            continue
        padded = list(row) + [""] * max(0, len(header) - len(row))
        inv = _cell(padded, idx, "Invoice Number")
        if not STRICT.fullmatch(inv or ""):
            continue
        by_inv[inv].append(
            {
                "invoice": inv,
                "bare": str(int(inv[2:])),
                "t": (
                    f"{_cell(padded, idx, 'Taxpayer Last Name')}, "
                    f"{_cell(padded, idx, 'Taxpayer First Name')}"
                ),
                "dob": _cell(padded, idx, "Taxpayer Date of Birth"),
                "email": _cell(padded, idx, "Taxpayer Email Address"),
                "phone": _cell(padded, idx, "Taxpayer Daytime Phone"),
                "street": _cell(padded, idx, "Street Address"),
                "city": _cell(padded, idx, "City"),
                "state": _cell(padded, idx, "State"),
                "zip": _cell(padded, idx, "ZIP Code"),
                "county": _cell(padded, idx, "County"),
                "fs": _cell(padded, idx, "Filing Status"),
                "spouse_dob": _cell(padded, idx, "Spouse Date of Birth"),
                "spouse_phone": _cell(padded, idx, "Spouse Daytime Phone"),
            }
        )
    diff = {
        inv
        for inv, rs in by_inv.items()
        if len({(r["t"].upper(), r["dob"]) for r in rs}) > 1
    }
    collapsed = {}
    for inv, rs in by_inv.items():
        if inv in diff:
            continue
        best = sorted(
            rs,
            key=lambda r: (
                -(1 if r["email"] else 0),
                -(1 if r["street"] else 0),
                r.get("invoice", ""),
            ),
        )[0]
        collapsed[inv] = best
    return collapsed, diff


def migrate_v28(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(clients)")}
    for c in (
        "address_street",
        "address_city",
        "address_state",
        "address_zip",
        "address_county",
        "address_source",
        "address_verified_at",
    ):
        if c not in cols:
            conn.execute(f"ALTER TABLE clients ADD COLUMN {c} TEXT")
    rcols = {r[1] for r in conn.execute("PRAGMA table_info(returns)")}
    if "filing_status_drake" not in rcols:
        conn.execute("ALTER TABLE returns ADD COLUMN filing_status_drake TEXT")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS client_profile_backfill_history (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id             INTEGER NOT NULL,
          run_label             TEXT NOT NULL,
          applied_at            TEXT NOT NULL,
          bare_log_number       INTEGER NOT NULL,
          invoice_number        TEXT NOT NULL,
          source_export_sha256  TEXT,
          fields_written_json   TEXT NOT NULL,
          before_json           TEXT NOT NULL,
          after_json            TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cpbh_client
          ON client_profile_backfill_history(client_id, applied_at);
        CREATE INDEX IF NOT EXISTS idx_cpbh_run
          ON client_profile_backfill_history(run_label);
        """
    )
    # bump schema version on copy only
    try:
        conn.execute("UPDATE schema_version SET version=28 WHERE version<28")
    except sqlite3.Error:
        pass
    conn.commit()


def snapshot_client(conn, cid: int) -> dict:
    row = conn.execute(
        """
        SELECT taxpayer_email, taxpayer_cell, taxpayer_dob, address,
               address_street, address_city, address_state, address_zip,
               address_county, address_source, spouse_dob, spouse_cell
          FROM clients WHERE id=?
        """,
        (cid,),
    ).fetchone()
    keys = [
        "taxpayer_email",
        "taxpayer_cell",
        "taxpayer_dob",
        "address",
        "address_street",
        "address_city",
        "address_state",
        "address_zip",
        "address_county",
        "address_source",
        "spouse_dob",
        "spouse_cell",
    ]
    return dict(zip(keys, row))


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Prefer audit/tmp copy; include -wal/-shm if present for consistency
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    copy_path = OUT_DIR / f"taxops_r1_phase3_throwaway_{stamp}.sqlite"

    # Checkpoint live lightly then copy main file only (read-consistent enough for dry-run)
    # Do not hold lock long — open live RO briefly to verify exists.
    if not LIVE.exists():
        raise SystemExit(f"missing {LIVE}")
    shutil.copy2(LIVE, copy_path)
    # If WAL exists, try copy too (best-effort)
    for suf in ("-wal", "-shm"):
        p = Path(str(LIVE) + suf)
        if p.exists():
            try:
                shutil.copy2(p, Path(str(copy_path) + suf))
            except OSError:
                pass

    sha_export = sha256_file(EXPORT)
    collapsed, diff = load_collapsed()

    conn = sqlite3.connect(str(copy_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    migrate_v28(conn)

    join_result = resolve_strict_joins(
        conn,
        collapsed,
        tax_year=TAX_YEAR,
        require_name_gate=True,
        exclude_client_ids=CONTAM,
    )
    joinable = [(h.client_id, h.rec) for h in join_result.joinable]

    applied_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    n_clients_touched = 0
    n_fields_written = 0
    field_counts = defaultdict(int)
    n_fs_written = 0
    reconstruct_ok = 0
    reconstruct_fail = 0
    sample_ids = []

    for cid, rec in joinable:
        before = snapshot_client(conn, cid)
        written = {}
        for col, ek in FIELD_MAP:
            incoming = (rec.get(ek) or "").strip()
            if _empty(before.get(col)) and incoming:
                written[col] = incoming
        if written and any(k.startswith("address_") for k in written):
            # set source only if we wrote any address piece and source empty
            if _empty(before.get("address_source")):
                written["address_source"] = "drake_taxpayer_csv"

        if not written and not rec.get("fs"):
            continue

        after = dict(before)
        if written:
            sets = ", ".join(f"{k}=?" for k in written)
            conn.execute(
                f"UPDATE clients SET {sets} WHERE id=?",
                list(written.values()) + [cid],
            )
            for k, v in written.items():
                after[k] = v
                field_counts[k] += 1
            n_fields_written += len(written)
            n_clients_touched += 1
            conn.execute(
                """
                INSERT INTO client_profile_backfill_history (
                  client_id, run_label, applied_at, bare_log_number, invoice_number,
                  source_export_sha256, fields_written_json, before_json, after_json
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    cid,
                    RUN_LABEL,
                    applied_at,
                    int(rec["bare"]),
                    rec["invoice"],
                    sha_export,
                    json.dumps(written, sort_keys=True),
                    json.dumps(before, sort_keys=True),
                    json.dumps(after, sort_keys=True),
                ),
            )
            if len(sample_ids) < 5:
                sample_ids.append(cid)

        fs = (rec.get("fs") or "").strip()
        if fs:
            # COALESCE filing_status_drake on the matching return
            row = conn.execute(
                """
                SELECT id, filing_status_drake FROM returns
                 WHERE client_id=? AND tax_year=?
                 ORDER BY id LIMIT 1
                """,
                (cid, TAX_YEAR),
            ).fetchone()
            if row and _empty(row["filing_status_drake"]):
                conn.execute(
                    "UPDATE returns SET filing_status_drake=? WHERE id=?",
                    (fs, row["id"]),
                )
                n_fs_written += 1

    conn.commit()

    # Reconstruct sample: restore before_json and compare
    for cid in sample_ids:
        hist = conn.execute(
            """
            SELECT before_json FROM client_profile_backfill_history
             WHERE client_id=? AND run_label=? ORDER BY id DESC LIMIT 1
            """,
            (cid, RUN_LABEL),
        ).fetchone()
        if not hist:
            reconstruct_fail += 1
            continue
        snap = json.loads(hist[0])
        # apply snap
        cols = list(snap.keys())
        conn.execute(
            f"UPDATE clients SET {', '.join(f'{k}=?' for k in cols)} WHERE id=?",
            [snap[k] for k in cols] + [cid],
        )
        now = snapshot_client(conn, cid)
        if now == snap:
            reconstruct_ok += 1
        else:
            reconstruct_fail += 1
        # re-apply after from history so copy stays in "post-backfill" state for inspection
        after_j = conn.execute(
            """
            SELECT after_json FROM client_profile_backfill_history
             WHERE client_id=? AND run_label=? ORDER BY id DESC LIMIT 1
            """,
            (cid, RUN_LABEL),
        ).fetchone()
        after = json.loads(after_j[0])
        cols = list(after.keys())
        conn.execute(
            f"UPDATE clients SET {', '.join(f'{k}=?' for k in cols)} WHERE id=?",
            [after[k] for k in cols] + [cid],
        )
    conn.commit()

    n_hist = conn.execute(
        "SELECT COUNT(*) FROM client_profile_backfill_history WHERE run_label=?",
        (RUN_LABEL,),
    ).fetchone()[0]

    # Sanity: no overwrite of previously filled taxpayer_cell among touched
    # (spot-check: history before_json cell non-empty => not in fields_written)
    overwrite_bugs = 0
    for r in conn.execute(
        """
        SELECT fields_written_json, before_json FROM client_profile_backfill_history
         WHERE run_label=?
        """,
        (RUN_LABEL,),
    ):
        written = json.loads(r[0])
        before = json.loads(r[1])
        for col in written:
            if col == "address_source":
                continue
            if not _empty(before.get(col)):
                overwrite_bugs += 1

    # legacy address never in written
    legacy_touched = 0
    for r in conn.execute(
        "SELECT fields_written_json FROM client_profile_backfill_history WHERE run_label=?",
        (RUN_LABEL,),
    ):
        if "address" in json.loads(r[0]) and "address_street" not in json.loads(r[0]):
            # exact key 'address'
            pass
        if "address" in json.loads(r[0]) and list(json.loads(r[0]).keys()):
            if "address" in json.loads(r[0]):
                legacy_touched += 1

    payload = {
        "generated_at": applied_at,
        "throwaway_db": str(copy_path),
        "live_untouched": str(LIVE),
        "export_sha256": sha_export,
        "n_joinable": len(joinable),
        "n_diff_person_excluded": len(diff),
        "n_clients_touched": n_clients_touched,
        "n_fields_written": n_fields_written,
        "n_history_rows": n_hist,
        "n_fs_drake_written": n_fs_written,
        "field_counts": dict(field_counts),
        "reconstruct_ok": reconstruct_ok,
        "reconstruct_fail": reconstruct_fail,
        "overwrite_bugs": overwrite_bugs,
        "legacy_address_in_written": legacy_touched,
        "sample_client_ids": sample_ids,
        "acceptance": {
            "history_eq_touched": n_hist == n_clients_touched,
            "reconstruct_sample": reconstruct_fail == 0 and reconstruct_ok == len(sample_ids),
            "no_overwrite": overwrite_bugs == 0,
            "legacy_untouched": legacy_touched == 0,
        },
    }
    overall = all(payload["acceptance"].values())
    payload["overall"] = "PASS" if overall else "FAIL"
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# R1 Phase 3 — throwaway COALESCE dry-run",
        "",
        f"_Generated: {applied_at} · run_label `{RUN_LABEL}`_",
        "",
        f"- **Live DB untouched:** `{LIVE}`",
        f"- Throwaway copy: `{copy_path}`",
        f"- Export sha: `{sha_export}`",
        "",
        f"**Overall: {payload['overall']}**",
        "",
        "## Counts",
        "",
        f"| Metric | n |",
        f"|---|---:|",
        f"| Joinable | {len(joinable)} |",
        f"| Clients touched (history rows) | {n_clients_touched} |",
        f"| Fields written | {n_fields_written} |",
        f"| `filing_status_drake` filled | {n_fs_written} |",
        f"| Reconstruct sample OK/Fail | {reconstruct_ok}/{reconstruct_fail} |",
        f"| Overwrite bugs | {overwrite_bugs} |",
        "",
        "## Fields written",
        "",
        "| Field | n |",
        "|---|---:|",
    ]
    for k, v in sorted(field_counts.items(), key=lambda kv: -kv[1]):
        lines.append(f"| `{k}` | {v} |")
    lines += [
        "",
        "## Acceptance",
        "",
        "| Check | Result |",
        "|---|---|",
    ]
    for k, v in payload["acceptance"].items():
        lines.append(f"| `{k}` | {'PASS' if v else 'FAIL'} |")
    lines += [
        "",
        "Next gate: **Stage C** apply v28 DDL to live (schema only), then **Stage D** live COALESCE with same skip rules.",
        "",
        f"Machine: `{OUT_JSON}`",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(
        payload["overall"],
        "touched",
        n_clients_touched,
        "fields",
        n_fields_written,
        "fs",
        n_fs_written,
        "ow",
        overwrite_bugs,
        "->",
        copy_path,
    )
    conn.close()


if __name__ == "__main__":
    main()
