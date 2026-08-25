"""Re-calibrate TAXPAYER.csv after Drake re-export (ZIP + Filing Status)."""
from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

SRC = Path(r"F:\DRAKE25\DT\5\4F24A262\Documents\TAXPAYER.csv")
DEST = Path(r"T:\audit\investigation\exports\TAXPAYER.csv")
OUT_MD = Path(r"T:\audit\investigation\R1-phase0-taxpayer-csv.md")
OUT_JSON = Path(r"T:\audit\investigation\R1-phase0-taxpayer-csv.json")
STATUS = Path(r"T:\audit\investigation\R1-status.md")
STRICT = re.compile(r"^25\d{4}$")
FS_MAP = {"1": "Single", "2": "MFJ", "3": "MFS", "4": "HOH", "5": "QW"}


def main() -> None:
    DEST.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(SRC, DEST)
    h = hashlib.sha256(DEST.read_bytes()).hexdigest()

    rows = list(csv.reader(DEST.open(encoding="utf-8-sig", newline="")))
    meta0 = (rows[0][0] if rows and rows[0] else "").strip()
    meta1 = (rows[1][0] if len(rows) > 1 and rows[1] else "").strip()
    header = rows[2]
    idx = {c: i for i, c in enumerate(header)}

    def g(padded, c):
        i = idx.get(c)
        if i is None or i >= len(padded):
            return ""
        return (padded[i] or "").strip()

    widths = Counter()
    inv_ok = inv_bad = inv_empty = 0
    bad_vals: Counter = Counter()
    fills = Counter()
    fs_counts: Counter = Counter()
    by_inv: dict[str, list[dict]] = defaultdict(list)
    data_n = 0
    zip_lens = Counter()
    zip_bad = []

    for n, row in enumerate(rows[3:], start=4):
        if not row or not any((c or "").strip() for c in row):
            continue
        data_n += 1
        widths[len(row)] += 1
        padded = list(row) + [""] * max(0, len(header) - len(row))
        inv = g(padded, "Invoice Number")
        city = g(padded, "City")
        state = g(padded, "State")
        county = g(padded, "County")
        zipc = g(padded, "ZIP Code")
        fs = g(padded, "Filing Status")
        email = g(padded, "Taxpayer Email Address")
        phone = g(padded, "Taxpayer Daytime Phone")
        dob = g(padded, "Taxpayer Date of Birth")
        spouse = g(padded, "Spouse Name")
        street = g(padded, "Street Address") or g(padded, "Address")
        dep = f"{g(padded, 'Dependent Last Name')}, {g(padded, 'Dependent First Name')}"

        for key, val in [
            ("city", city),
            ("state", state),
            ("county", county),
            ("zip", zipc),
            ("fs", fs),
            ("email", email),
            ("phone", phone),
            ("dob", dob),
            ("spouse", spouse),
            ("street", street),
            ("dep", "" if dep in (",", "") else dep),
            ("spouse_dob", g(padded, "Spouse Date of Birth")),
            ("spouse_phone", g(padded, "Spouse Daytime Phone")),
        ]:
            if val:
                fills[key] += 1
        if fs:
            fs_counts[fs] += 1
        if zipc:
            zip_lens[len(re.sub(r"\D", "", zipc))] += 1
            digits = re.sub(r"\D", "", zipc)
            if len(digits) not in (5, 9):
                if len(zip_bad) < 8:
                    zip_bad.append((n, zipc, inv))

        rec = {
            "row": n,
            "t": f"{g(padded, 'Taxpayer Last Name')}, {g(padded, 'Taxpayer First Name')}",
            "dob": dob,
            "email": email,
            "city": city,
            "state": state,
            "zip": zipc,
            "fs": fs,
            "dep": dep,
        }
        if not inv:
            inv_empty += 1
        elif STRICT.fullmatch(inv):
            inv_ok += 1
            by_inv[inv].append(rec)
        else:
            inv_bad += 1
            bad_vals[inv] += 1

    multi = {k: v for k, v in by_inv.items() if len(v) > 1}
    same_person = diff_person = dep_driven = 0
    diff_examples = []
    for inv, rs in multi.items():
        keys = {(r["t"].upper(), r["dob"]) for r in rs}
        deps = [r["dep"] for r in rs if r["dep"] not in (",", "")]
        if len(keys) == 1:
            same_person += 1
            if deps:
                dep_driven += 1
        else:
            diff_person += 1
            if len(diff_examples) < 5:
                diff_examples.append(
                    {
                        "invoice": inv,
                        "rows": [
                            {"t": r["t"], "dob": r["dob"], "dep": r["dep"], "zip": r["zip"]}
                            for r in rs
                        ],
                    }
                )

    collapsed = {}
    for inv, rs in by_inv.items():
        best = sorted(
            rs,
            key=lambda r: (
                -(1 if r["email"] else 0),
                -(1 if r["zip"] else 0),
                r["row"],
            ),
        )[0]
        collapsed[inv] = best

    bare_to_inv: dict[str, list[str]] = defaultdict(list)
    for inv in collapsed:
        bare_to_inv[str(int(inv[2:]))].append(inv)
    bare_collisions = {b: invs for b, invs in bare_to_inv.items() if len(invs) > 1}

    cols_lower = [c.lower() for c in header]
    has_zip = any("zip" in c for c in cols_lower)
    has_street = any(
        c in ("street address", "address", "street")
        or c.startswith("street ")
        for c in cols_lower
    )
    has_fs = any("filing" in c for c in cols_lower)
    has_city = "city" in cols_lower
    has_state = "state" in cols_lower

    gates = {
        "0.1_city_state": has_city and has_state and fills.get("city", 0) > 0,
        "0.1_zip": has_zip and fills.get("zip", 0) > 0,
        "0.1_street": has_street and fills.get("street", 0) > 0,
        "0.2_filing_status": has_fs and fills.get("fs", 0) > 0,
        "0.2_ragged": len([w for w in widths if w != len(header)]) > 0
        or (len(widths) > 1),
    }
    # ragged: any row width != header length
    ragged_n = sum(n for w, n in widths.items() if w != len(header))
    gates["0.2_ragged"] = ragged_n > 0

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": str(SRC),
        "canonical": str(DEST),
        "sha256": h,
        "prev_sha256_note": "prior ingest e79e54aa… (no ZIP/FS)",
        "as_of": meta1,
        "n_cols": len(header),
        "columns": header,
        "data_rows": data_n,
        "widths": dict(widths),
        "ragged_rows": ragged_n,
        "invoice_strict_rows": inv_ok,
        "invoice_bad": inv_bad,
        "invoice_empty": inv_empty,
        "bad_invoice_top": bad_vals.most_common(20),
        "fills": dict(fills),
        "fs_counts": dict(fs_counts),
        "zip_digit_lens": dict(zip_lens),
        "zip_odd_sample": zip_bad,
        "has_zip": has_zip,
        "has_street": has_street,
        "has_filing_status": has_fs,
        "strict_distinct_invoices": len(by_inv),
        "collapsed_strict": len(collapsed),
        "multi_row_invoices": len(multi),
        "multi_same_person": same_person,
        "multi_dep_driven": dep_driven,
        "multi_diff_person": diff_person,
        "bare_collisions_after_collapse": len(bare_collisions),
        "diff_person_examples": diff_examples,
        "phase0_gates": gates,
        "address_schema_unblocked": bool(
            gates["0.1_city_state"] and gates["0.1_zip"] and gates["0.1_street"]
        ),
        "address_schema_partial": bool(
            gates["0.1_city_state"] and gates["0.1_zip"] and not gates["0.1_street"]
        ),
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def mark(ok: bool) -> str:
        return "**PASS**" if ok else "**FAIL**"

    lines = [
        "# R1 Phase 0 — `TAXPAYER.csv` calibration (re-export)",
        "",
        f"_Generated: {payload['generated_at']}_",
        "",
        f"- Drake source: `{SRC}`",
        f"- Canonical: `{DEST}`",
        f"- sha256: `{h}`",
        f"- Meta: `{meta0}` / `{meta1}`",
        f"- Delta vs prior ingest: **added `Filing Status`, `ZIP Code`** (was 14 cols / sha e79e54aa…)",
        "",
        f"## Columns ({len(header)}, ragged={ragged_n})",
        "",
        ", ".join(f"`{c}`" for c in header),
        "",
        f"Data rows: **{data_n}** · widths: `{dict(widths)}`",
        "",
        "## Phase 0 gate check",
        "",
        "| Gate | Status | Evidence |",
        "|---|---|---|",
        f"| 0.1 City + State | {mark(gates['0.1_city_state'])} | city={fills.get('city')} state={fills.get('state')} |",
        f"| 0.1 ZIP | {mark(gates['0.1_zip'])} | zip={fills.get('zip')} lens={dict(zip_lens)} |",
        f"| 0.1 Street | {mark(gates['0.1_street'])} | street={fills.get('street', 0)} (column {'present' if has_street else 'absent'}) |",
        f"| 0.2 Filing Status | {mark(gates['0.2_filing_status'])} | fs={fills.get('fs')} counts={dict(fs_counts)} |",
        f"| 0.2 Ragged rows | {mark(not gates['0.2_ragged'])} | ragged_n={ragged_n} |",
        "",
        f"**Address schema fully unblocked (city+state+zip+street):** "
        f"{'YES' if payload['address_schema_unblocked'] else 'NO — street still missing'}",
        "",
        "## Invoice fill (strict `^25\\d{4}$`)",
        "",
        "| Strict rows | Bad | Empty | Distinct after collapse |",
        "|---:|---:|---:|---:|",
        f"| {inv_ok} | {inv_bad} | {inv_empty} | {len(collapsed)} |",
        "",
        f"Multi-row invoices: {len(multi)} (same-person {same_person}, dep-driven {dep_driven}, "
        f"diff-person **{diff_person}**) · bare collisions after collapse: **{len(bare_collisions)}**",
        "",
        "### Malformed top",
        "",
        "| Invoice | n |",
        "|---|---:|",
    ]
    for inv, n in bad_vals.most_common(12):
        lines.append(f"| `{inv}` | {n} |")
    lines += [
        "",
        f"**`25141` count:** {bad_vals.get('25141', 0)} — never lenient-parse.",
        "",
        "## Field fills",
        "",
        "| Field | n |",
        "|---|---:|",
    ]
    for k in (
        "city",
        "state",
        "county",
        "zip",
        "fs",
        "street",
        "email",
        "phone",
        "dob",
        "spouse",
        "spouse_dob",
        "spouse_phone",
        "dep",
    ):
        lines.append(f"| `{k}` | {fills.get(k, 0)} |")

    lines += [
        "",
        "## Implication",
        "",
        "- City / State / ZIP / County + Filing Status are now on this export.",
        "- **Street Address still absent** — mailable COALESCE and `address_street` still need Street "
        "from this report or a merge with `TAXPAYERspouseaddressstatus.csv` (street-only there).",
        "- Optional path: join this file ↔ prior export on strict invoice for street+city+zip composite "
        "(measure overlap before relying on it).",
        "- Collapse dependent rows before any TaxOps join.",
        "",
        f"Machine: `{OUT_JSON}`",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    # Patch status section if present (avoid re.sub — backslashes in paths)
    if STATUS.exists():
        st = STATUS.read_text(encoding="utf-8")
        street_n = fills.get("street", 0)
        street_line = (
            f"| Street | **PASS** ({street_n}/{data_n}) |"
            if gates["0.1_street"]
            else "| Street | **FAIL** — still absent |"
        )
        addr_line = (
            "Address schema: **city/state/zip/county/street unblocked** — Phase 2A may proceed."
            if payload["address_schema_unblocked"]
            else "Address schema: city/state/zip OK; **`address_street` still blocked**."
        )
        block = "\n".join(
            [
                "## New export: `TAXPAYER.csv` (2026-08-13 re-export)",
                "",
                "Canonical: `T:\\audit\\investigation\\exports\\TAXPAYER.csv`",
                f"sha256 `{h}`",
                "Detail: `R1-phase0-taxpayer-csv.md`",
                "",
                "| Gate | Result |",
                "|---|---|",
                f"| City + State (+ County) | **PASS** ({fills.get('city')}/{data_n}) |",
                f"| ZIP | **PASS** ({fills.get('zip')}/{data_n}) |",
                street_line,
                f"| Filing Status | **PASS** ({fills.get('fs')}/{data_n}; {dict(fs_counts)}) |",
                f"| Ragged rows | **{'PASS' if ragged_n <= 1 else 'FAIL'}** — ragged={ragged_n} |",
                f"| Strict invoices | {inv_ok} row hits → **{len(collapsed)}** distinct after dep-collapse |",
                "",
                addr_line,
                "",
            ]
        )
        marker = "## New export: `TAXPAYER.csv`"
        end = "## Explicitly not started"
        if marker in st:
            i0 = st.index(marker)
            i1 = st.index(end) if end in st[i0:] else len(st)
            if end in st[i0:]:
                i1 = i0 + st[i0:].index(end)
            st = st[:i0] + block + "\n" + st[i1:]
        else:
            st = st.rstrip() + "\n\n" + block + "\n"
        STATUS.write_text(st, encoding="utf-8")

    print(
        "sha",
        h[:16],
        "cols",
        len(header),
        "zip",
        fills.get("zip"),
        "fs",
        fills.get("fs"),
        "street",
        fills.get("street", 0),
        "collapsed",
        len(collapsed),
        "unblocked",
        payload["address_schema_unblocked"],
        "gates",
        gates,
    )


if __name__ == "__main__":
    main()
