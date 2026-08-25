"""R1 Phase 1 — join quality on TAXPAYER.csv (strict + dep-collapse). Read-only."""
from __future__ import annotations

import csv
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
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
OUT_MD = Path(r"T:\audit\investigation\R1-phase1-join-taxpayer.md")
OUT_JSON = Path(r"T:\audit\investigation\R1-phase1-join-taxpayer.json")
CONTAM = {1670, 1583, 902, 327, 237, 878}


def _cell(row, idx, col):
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _empty(s) -> bool:
    return not (s or "").strip()


def load_collapsed() -> tuple[dict[str, dict], dict]:
    rows = list(csv.reader(EXPORT.open(encoding="utf-8-sig", newline="")))
    header = rows[2]
    idx = {h: i for i, h in enumerate(header)}
    by_inv: dict[str, list[dict]] = defaultdict(list)
    bad = Counter()
    empty = 0
    data_n = 0
    for n, row in enumerate(rows[3:], start=4):
        if not row or not any((c or "").strip() for c in row):
            continue
        if len(row) < 5:
            continue  # junk trailer
        data_n += 1
        padded = list(row) + [""] * max(0, len(header) - len(row))
        inv = _cell(padded, idx, "Invoice Number")
        if not inv:
            empty += 1
            continue
        if not STRICT.fullmatch(inv):
            bad[inv] += 1
            continue
        by_inv[inv].append(
            {
                "source_row": n,
                "invoice": inv,
                "bare": str(int(inv[2:])),
                "t_last": _cell(padded, idx, "Taxpayer Last Name"),
                "t_first": _cell(padded, idx, "Taxpayer First Name"),
                "email": _cell(padded, idx, "Taxpayer Email Address"),
                "phone": _cell(padded, idx, "Taxpayer Daytime Phone"),
                "dob": _cell(padded, idx, "Taxpayer Date of Birth"),
                "street": _cell(padded, idx, "Street Address"),
                "city": _cell(padded, idx, "City"),
                "state": _cell(padded, idx, "State"),
                "zip": _cell(padded, idx, "ZIP Code"),
                "county": _cell(padded, idx, "County"),
                "fs": _cell(padded, idx, "Filing Status"),
                "spouse": _cell(padded, idx, "Spouse Name"),
                "spouse_dob": _cell(padded, idx, "Spouse Date of Birth"),
                "spouse_phone": _cell(padded, idx, "Spouse Daytime Phone"),
                "dep": (
                    f"{_cell(padded, idx, 'Dependent Last Name')}, "
                    f"{_cell(padded, idx, 'Dependent First Name')}"
                ),
            }
        )

    # true multi-person invoices (exclude from join)
    diff_person_inv = set()
    for inv, rs in by_inv.items():
        keys = {(r["t_last"].upper(), r["t_first"].upper(), r["dob"]) for r in rs}
        if len(keys) > 1:
            diff_person_inv.add(inv)

    collapsed = {}
    for inv, rs in by_inv.items():
        if inv in diff_person_inv:
            continue
        best = sorted(
            rs,
            key=lambda r: (
                -(1 if r["email"] else 0),
                -(1 if r["street"] else 0),
                -(1 if r["zip"] else 0),
                r["source_row"],
            ),
        )[0]
        collapsed[inv] = best

    meta = {
        "data_rows": data_n,
        "strict_row_hits": sum(len(v) for v in by_inv.values()),
        "distinct_strict_inv": len(by_inv),
        "diff_person_inv": sorted(diff_person_inv),
        "n_diff_person_inv": len(diff_person_inv),
        "collapsed": len(collapsed),
        "invoice_empty": empty,
        "invoice_bad": dict(bad),
        "n_bad": sum(bad.values()),
    }
    return collapsed, meta


def main() -> None:
    sha = sha256_file(EXPORT)
    collapsed, meta = load_collapsed()

    # bare collisions among collapsed (two different 25XXXX -> same bare) — should be 0
    bare_to_inv: dict[str, list[str]] = defaultdict(list)
    for inv, rec in collapsed.items():
        bare_to_inv[rec["bare"]].append(inv)
    bare_collisions = {b: invs for b, invs in bare_to_inv.items() if len(invs) > 1}

    conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    n_clients = conn.execute(
        "SELECT COUNT(*) FROM clients WHERE COALESCE(is_test,0)=0"
    ).fetchone()[0]

    log_to_clients: dict[str, set[int]] = defaultdict(set)
    client_logs: dict[int, set[str]] = defaultdict(set)
    client_email = {}
    client_addr = {}
    for r in conn.execute(
        """
        SELECT r.client_id, r.log_number,
               c.taxpayer_email, c.address, c.taxpayer_dob, c.taxpayer_cell,
               c.spouse_email, c.spouse_cell, c.spouse_dob
          FROM returns r JOIN clients c ON c.id = r.client_id
         WHERE r.tax_year=? AND r.log_number IS NOT NULL AND trim(r.log_number)!=''
           AND COALESCE(c.is_test,0)=0
        """,
        (TAX_YEAR,),
    ):
        bare = bare_log_number(str(r["log_number"]), TAX_YEAR)
        if not bare:
            continue
        cid = int(r["client_id"])
        log_to_clients[bare].add(cid)
        client_logs[cid].add(bare)
        client_email[cid] = (r["taxpayer_email"] or "").strip()
        client_addr[cid] = (r["address"] or "").strip()
        # stash row for COALESCE gaps
        if cid not in client_email:
            pass

    # richer snapshot for COALESCE
    client_fields = {}
    for r in conn.execute(
        """
        SELECT id, taxpayer_email, taxpayer_cell, taxpayer_dob, address,
               spouse_email, spouse_cell, spouse_dob,
               spouse_first_name, spouse_last_name
          FROM clients WHERE COALESCE(is_test,0)=0
        """
    ):
        client_fields[int(r["id"])] = dict(r)

    conn.close()

    joinable = []
    skip_no_taxops = []
    skip_multi_client = []
    skip_multi_invoice_client = []
    skip_contam = []
    client_to_inv: dict[int, list[str]] = defaultdict(list)

    for inv, rec in collapsed.items():
        bare = rec["bare"]
        cids = log_to_clients.get(bare) or set()
        if len(cids) == 0:
            skip_no_taxops.append(rec)
            continue
        if len(cids) != 1:
            skip_multi_client.append({"bare": bare, "invoice": inv, "clients": sorted(cids)})
            continue
        cid = next(iter(cids))
        client_to_inv[cid].append(inv)

    # clients with >1 invoice after collapse
    multi_inv_clients = {c: invs for c, invs in client_to_inv.items() if len(invs) > 1}

    for cid, invs in client_to_inv.items():
        if len(invs) > 1:
            skip_multi_invoice_client.append({"client_id": cid, "invoices": invs})
            continue
        inv = invs[0]
        rec = collapsed[inv]
        if cid in CONTAM:
            skip_contam.append({"client_id": cid, "invoice": inv, "bare": rec["bare"]})
            continue
        joinable.append({"client_id": cid, "invoice": inv, **rec})

    # Acceptance: zero double-writes — each joinable client gets exactly one invoice
    double = [j for j in joinable if client_to_inv[j["client_id"]] != [j["invoice"]]]
    # Also: no two joinable rows share a client_id
    seen = Counter(j["client_id"] for j in joinable)
    dup_clients = [c for c, n in seen.items() if n > 1]

    # COALESCE gaps on joinable (exclude contam already)
    gaps = Counter()
    for j in joinable:
        cf = client_fields.get(j["client_id"], {})
        pairs = [
            ("taxpayer_email", j["email"], cf.get("taxpayer_email")),
            ("taxpayer_cell", j["phone"], cf.get("taxpayer_cell")),
            ("taxpayer_dob", j["dob"], cf.get("taxpayer_dob")),
            ("address_street", j["street"], None),  # new col — always gap if drake has
            ("address_city", j["city"], None),
            ("address_state", j["state"], None),
            ("address_zip", j["zip"], None),
            ("address_county", j["county"], None),
            ("legacy_address_empty", j["street"], cf.get("address")),
            ("spouse_dob", j["spouse_dob"], cf.get("spouse_dob")),
            ("spouse_cell", j["spouse_phone"], cf.get("spouse_cell")),
        ]
        for name, drake, cur in pairs:
            if name.startswith("address_") and not _empty(drake):
                gaps[name] += 1
            elif name == "legacy_address_empty":
                if not _empty(drake) and _empty(cur):
                    gaps[name] += 1
            elif not _empty(drake) and _empty(cur):
                gaps[name] += 1

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "export": str(EXPORT),
        "sha256": sha,
        "meta": meta,
        "n_bare_collisions": len(bare_collisions),
        "bare_collisions": {k: v for k, v in list(bare_collisions.items())[:20]},
        "n_clients_live": n_clients,
        "n_joinable": len(joinable),
        "joinable_pct": round(100.0 * len(joinable) / n_clients, 1) if n_clients else 0,
        "skip_no_taxops": len(skip_no_taxops),
        "skip_multi_client": len(skip_multi_client),
        "skip_multi_invoice_client": len(skip_multi_invoice_client),
        "skip_contam": len(skip_contam),
        "n_diff_person_excluded": meta["n_diff_person_inv"],
        "double_write_violations": len(dup_clients),
        "dup_client_ids": dup_clients,
        "coalesce_gaps": dict(gaps),
        "acceptance_double_write_zero": len(dup_clients) == 0,
        "contam_excluded": sorted(CONTAM),
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# R1 Phase 1 — join on `TAXPAYER.csv`",
        "",
        f"_Generated: {payload['generated_at']} · sha `{sha}` · strict `^25\\d{{4}}$` · dep-collapse_",
        "",
        "## Acceptance",
        "",
        f"| Check | Result |",
        f"|---|---|",
        f"| Zero double-write clients | "
        f"{'**PASS**' if payload['acceptance_double_write_zero'] else '**FAIL**'} "
        f"(dup={len(dup_clients)}) |",
        f"| Bare collisions after collapse | {len(bare_collisions)} |",
        f"| Diff-person invoices excluded | {meta['n_diff_person_inv']} |",
        "",
        "## Population",
        "",
        f"| Metric | n |",
        f"|---|---:|",
        f"| Live non-test clients | {n_clients} |",
        f"| Distinct strict invoices (pre-exclude) | {meta['distinct_strict_inv']} |",
        f"| Collapsed (excl. diff-person) | {meta['collapsed']} |",
        f"| **Joinable (1:1, not contam)** | **{len(joinable)}** "
        f"({payload['joinable_pct']}%) |",
        f"| No TaxOps bare | {len(skip_no_taxops)} |",
        f"| Multi-client bare | {len(skip_multi_client)} |",
        f"| Multi-invoice client | {len(skip_multi_invoice_client)} |",
        f"| Contam skip | {len(skip_contam)} |",
        "",
        "## COALESCE gaps (joinable only — Drake present, TaxOps empty)",
        "",
        "| Field | n |",
        "|---|---:|",
    ]
    for k, v in sorted(gaps.items(), key=lambda kv: -kv[1]):
        lines.append(f"| `{k}` | {v} |")
    lines += [
        "",
        "Address_* gap counts = Drake has value (new cols empty by definition until Phase 3).",
        "`legacy_address_empty` = Drake street present and `clients.address` empty.",
        "",
        f"Machine: `{OUT_JSON}`",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(
        "joinable",
        len(joinable),
        "double",
        len(dup_clients),
        "gaps",
        dict(gaps),
        "->",
        OUT_MD,
    )


if __name__ == "__main__":
    main()
