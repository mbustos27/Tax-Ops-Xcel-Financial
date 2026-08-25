"""Stage A — Phase 4 FS cross-check on TAXPAYER.csv (joinable set). Read-only."""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audit.invoice_export import bare_log_number  # noqa: E402
from audit.util import sha256_file  # noqa: E402

EXPORT = Path(r"T:\audit\investigation\exports\TAXPAYER.csv")
TAXOPS = Path(r"T:\taxops\taxops.db")
TAX_YEAR = 2025
STRICT = re.compile(r"^25\d{4}$")
CONTAM = {1670, 1583, 902, 327, 237, 878}
FS_LABEL = {"1": "Single", "2": "MFJ", "3": "MFS", "4": "HOH", "5": "QW"}
OUT_MD = Path(r"T:\audit\investigation\R1-phase4-fs-taxpayer.md")
OUT_JSON = Path(r"T:\audit\investigation\R1-phase4-fs-taxpayer.json")


def _cell(row, idx, col):
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


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
                "fs": _cell(padded, idx, "Filing Status"),
                "spouse": _cell(padded, idx, "Spouse Name"),
                "email": _cell(padded, idx, "Taxpayer Email Address"),
                "street": _cell(padded, idx, "Street Address"),
                "zip": _cell(padded, idx, "ZIP Code"),
            }
        )
    diff = set()
    for inv, rs in by_inv.items():
        keys = {(r["t"].upper(), r["dob"]) for r in rs}
        if len(keys) > 1:
            diff.add(inv)
    collapsed = {}
    for inv, rs in by_inv.items():
        if inv in diff:
            continue
        best = sorted(
            rs,
            key=lambda r: (-(1 if r["email"] else 0), -(1 if r["street"] else 0), inv),
        )[0]
        collapsed[inv] = best
    return collapsed, diff


def main():
    sha = sha256_file(EXPORT)
    collapsed, diff = load_collapsed()
    conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    log_to_clients = defaultdict(set)
    for r in conn.execute(
        """
        SELECT r.client_id, r.log_number
          FROM returns r JOIN clients c ON c.id=r.client_id
         WHERE r.tax_year=? AND r.log_number IS NOT NULL AND trim(r.log_number)!=''
           AND COALESCE(c.is_test,0)=0
        """,
        (TAX_YEAR,),
    ):
        bare = bare_log_number(str(r["log_number"]), TAX_YEAR)
        if bare:
            log_to_clients[bare].add(int(r["client_id"]))

    # spouse presence: spouses table OR clients.spouse_* names
    has_spouse = set()
    for r in conn.execute(
        "SELECT DISTINCT client_id FROM spouses WHERE "
        "COALESCE(last_name,'')!='' OR COALESCE(first_name,'')!=''"
    ):
        has_spouse.add(int(r["client_id"]))
    for r in conn.execute(
        """
        SELECT id FROM clients
         WHERE COALESCE(is_test,0)=0
           AND (COALESCE(spouse_last_name,'')!='' OR COALESCE(spouse_first_name,'')!='')
        """
    ):
        has_spouse.add(int(r["id"]))

    client_to_inv = defaultdict(list)
    for inv, rec in collapsed.items():
        cids = log_to_clients.get(rec["bare"]) or set()
        if len(cids) == 1:
            client_to_inv[next(iter(cids))].append(inv)

    joinable = []
    for cid, invs in client_to_inv.items():
        if len(invs) != 1 or cid in CONTAM:
            continue
        joinable.append((cid, collapsed[invs[0]]))

    single_with_spouse = []
    mfj_no_spouse = []
    fs_counts = defaultdict(int)
    for cid, rec in joinable:
        fs = rec["fs"]
        if fs:
            fs_counts[fs] += 1
        sp = cid in has_spouse
        if fs == "1" and sp:
            single_with_spouse.append(
                {
                    "client_id": cid,
                    "invoice": rec["invoice"],
                    "bare": rec["bare"],
                    "name": rec["t"],
                    "drake_spouse": rec["spouse"],
                }
            )
        if fs == "2" and not sp:
            mfj_no_spouse.append(
                {
                    "client_id": cid,
                    "invoice": rec["invoice"],
                    "bare": rec["bare"],
                    "name": rec["t"],
                    "drake_spouse": rec["spouse"],
                }
            )

    n_all = conn.execute(
        "SELECT COUNT(*) FROM clients WHERE COALESCE(is_test,0)=0"
    ).fetchone()[0]
    conn.close()

    n_j = len(joinable)
    rate_s = len(single_with_spouse) / n_j if n_j else 0
    rate_m = len(mfj_no_spouse) / n_j if n_j else 0

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sha256": sha,
        "n_joinable": n_j,
        "n_clients_live": n_all,
        "n_diff_person_excluded": len(diff),
        "fs_counts_joinable": dict(fs_counts),
        "single_with_spouse": len(single_with_spouse),
        "mfj_no_spouse": len(mfj_no_spouse),
        "extrapolate_single_spouse": round(rate_s * n_all),
        "extrapolate_mfj_none": round(rate_m * n_all),
        "samples_single_spouse": single_with_spouse[:15],
        "samples_mfj_none": mfj_no_spouse[:15],
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# R1 Phase 4 — FS cross-check on `TAXPAYER.csv`",
        "",
        f"_Generated: {payload['generated_at']} · sha `{sha}` · joinable={n_j}_",
        "",
        "## Assertions (joinable only)",
        "",
        f"| Assertion | n | Extrapolate ×{n_all} clients |",
        f"|---|---:|---:|",
        f"| Drake Single (1) + TaxOps has spouse | **{len(single_with_spouse)}** | ~{payload['extrapolate_single_spouse']} |",
        f"| Drake MFJ (2) + TaxOps no spouse | **{len(mfj_no_spouse)}** | ~{payload['extrapolate_mfj_none']} |",
        "",
        "FS on joinable: " + ", ".join(f"{k}={v}" for k, v in sorted(fs_counts.items())),
        "",
        "### Sample Single+spouse",
        "",
        "| Client | Bare | Invoice | Name | Drake spouse |",
        "|---|---|---|---|---|",
    ]
    for s in single_with_spouse[:12]:
        lines.append(
            f"| `{s['client_id']}` | `{s['bare']}` | `{s['invoice']}` | "
            f"{s['name'][:40]} | {s['drake_spouse'][:30]} |"
        )
    lines += [
        "",
        "### Sample MFJ+no-spouse",
        "",
        "| Client | Bare | Invoice | Name | Drake spouse |",
        "|---|---|---|---|---|",
    ]
    for s in mfj_no_spouse[:12]:
        lines.append(
            f"| `{s['client_id']}` | `{s['bare']}` | `{s['invoice']}` | "
            f"{s['name'][:40]} | {s['drake_spouse'][:30]} |"
        )
    lines += ["", f"Machine: `{OUT_JSON}`", ""]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(
        "joinable",
        n_j,
        "single+spouse",
        len(single_with_spouse),
        "mfj+none",
        len(mfj_no_spouse),
        "->",
        OUT_MD,
    )


if __name__ == "__main__":
    main()
