"""R1 Phase 0/1/4 calibration — strict invoice gate + C5/A1 cross-check.

Amendments (2026-08-13):
  - Copy Drake export into T:\\audit\\investigation\\exports\\ and hash at ingest
  - Join only invoices matching ^25\\d{4}$ exactly (no lenient bare-parse)
  - Office worklist for every non-canonical invoice (27-class analysis)
  - Diff no-TaxOps clean keys vs C5 ERROR/REVIEW absent + L0_DRAKE_ONLY + A1 L5
  - Scope Phase 4 rates to joinable / all-clients

Never writes TaxOps.
"""
from __future__ import annotations

import csv
import json
import re
import shutil
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

DRAKE_SRC = Path(r"F:\DRAKE25\DT\5\4F24A262\Documents\TAXPAYERspouseaddressstatus.csv")
EXPORT_DIR = Path(r"T:\audit\investigation\exports")
CANONICAL = EXPORT_DIR / "TAXPAYERspouseaddressstatus.csv"
TAXOPS = Path(r"T:\taxops\taxops.db")
C5_THROWAY = Path(r"T:\audit\tmp\c5_import_triage_throwaway.sqlite")
DISP = Path(r"T:\audit\audit_disposition.sqlite")
TAX_YEAR = 2025
OUT_DIR = Path(r"T:\audit\investigation")
OUT_JSON = OUT_DIR / "R1-calibration.json"
OUT_P0 = OUT_DIR / "R1-phase0-calibration.md"
OUT_P1 = OUT_DIR / "R1-phase1-join.md"
OUT_P4 = OUT_DIR / "R1-phase4-fs-crosscheck.md"
OUT_MAL = OUT_DIR / "R1-malformed-invoice-worklist.md"

STRICT_INV = re.compile(r"^25\d{4}$")
FS_LABEL = {"1": "Single", "2": "MFJ", "3": "MFS", "4": "HOH", "5": "QW"}
OFFICE_BARES = {
    "141", "203", "206", "207", "247", "335", "339", "353", "426", "589",
    "634", "787", "862", "888", "910", "1038", "1075", "1095", "1103",
}


def ingest_export() -> tuple[Path, str]:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    if not DRAKE_SRC.exists():
        if CANONICAL.exists():
            return CANONICAL, sha256_file(CANONICAL)
        raise FileNotFoundError(f"Missing Drake export: {DRAKE_SRC}")
    shutil.copy2(DRAKE_SRC, CANONICAL)
    return CANONICAL, sha256_file(CANONICAL)


def _cell(row: list[str], idx: dict[str, int], col: str) -> str:
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _has_state_token(street: str) -> bool:
    u = (street or "").upper()
    return bool(
        re.search(r"\b[A-Z]{2}\s+\d{5}(-\d{4})?\b", u)
        or re.search(r",\s*[A-Z]{2}\b", u)
    )


def classify_noncanonical(inv: str) -> dict:
    """Classify invoice that fails ^25\\d{4}$. Lenient reading is diagnostic only."""
    digits = re.sub(r"\D", "", inv or "")
    lenient = bare_log_number(inv, TAX_YEAR) if inv else ""
    if not digits:
        klass, verdict = "blank", "no invoice"
    elif digits in ("25", "250", "251") or (digits.startswith("25") and len(digits) <= 3):
        klass, verdict = "stub", "never parse"
    elif re.fullmatch(r"25\d{3}", digits):
        klass, verdict = "25_plus_3", "plausible missing zero-pad — confirm"
    elif re.fullmatch(r"25\d{5}", digits):
        klass, verdict = "7_digit", "extra digit; position ambiguous — confirm"
    elif digits.startswith("25") and len(digits) == 5:
        # 25141 etc — 25 + 3 digits would be 5 total... wait 25141 is 5 digits = 25+3
        # already caught by 25_plus_3. 25141 is 25 + 141 = actually 5 digits after? 
        # 25141 = 5 chars total = 25 + 3 digits → 25_plus_3 with lenient 141
        klass, verdict = "25_plus_3", "plausible missing zero-pad — confirm"
    elif not digits.startswith("25"):
        klass, verdict = "not_25_prefix", "prior season or typo — reject"
    else:
        klass, verdict = "other_noncanonical", "office confirm"

    hazard = None
    if lenient == "141" or digits == "25141":
        hazard = "COLLIDES_CLEAN_BARE_141_PEREZ"
        verdict = "NEVER parse — manufactures I7 collision on PEREZ/141"

    return {
        "invoice": inv,
        "digits": digits,
        "class": klass,
        "lenient_bare": lenient or None,
        "verdict": verdict,
        "hazard": hazard,
    }


def load_export(path: Path) -> tuple[list[dict], dict]:
    raw_rows = list(csv.reader(path.open(encoding="utf-8-sig", newline="")))
    header = raw_rows[2]
    idx = {h: i for i, h in enumerate(header)}
    data = []
    width_counts: Counter[int] = Counter()
    for n, row in enumerate(raw_rows[3:], start=4):
        if not row or not any((c or "").strip() for c in row):
            continue
        width_counts[len(row)] += 1
        padded = list(row) + [""] * max(0, len(header) - len(row))
        inv = _cell(padded, idx, "Invoice Number")
        strict_ok = bool(STRICT_INV.fullmatch(inv)) if inv else False
        # ONLY strict invoices get a join bare
        bare = bare_log_number(inv, TAX_YEAR) if strict_ok else None
        lenient_bare = bare_log_number(inv, TAX_YEAR) if inv else ""
        noncanon = None
        if inv and not strict_ok:
            noncanon = classify_noncanonical(inv)
        rec = {
            "source_row": n,
            "width": len(row),
            "t_last": _cell(padded, idx, "Taxpayer Last Name"),
            "t_first": _cell(padded, idx, "Taxpayer First Name"),
            "invoice": inv,
            "strict_ok": strict_ok,
            "bare_log": bare,
            "lenient_bare": lenient_bare or None,
            "noncanonical": noncanon,
            "dob": _cell(padded, idx, "Taxpayer Date of Birth"),
            "street": _cell(padded, idx, "Street Address"),
            "county": _cell(padded, idx, "County"),
            "email": _cell(padded, idx, "Email Address"),
            "spouse_bday": _cell(padded, idx, "Spouse Date of Birth")
            or _cell(padded, idx, "Spouse Birthday"),
            "spouse_first": _cell(padded, idx, "Spouse First Name"),
            "spouse_last": _cell(padded, idx, "Spouse Last Name"),
            "spouse_cell": _cell(padded, idx, "Spouse Cell Phone"),
            "spouse_email": _cell(padded, idx, "Spouse Email Address"),
            "filing_status": _cell(padded, idx, "Filing Status"),
        }
        data.append(rec)
    meta = {
        "header": header,
        "header_width": len(header),
        "width_counts": dict(sorted(width_counts.items())),
        "n_data": len(data),
        "canonical_path": str(path),
        "drake_source": str(DRAKE_SRC),
    }
    return data, meta


def phase0(data: list[dict], meta: dict, sha: str) -> dict:
    streets = [r["street"] for r in data if r["street"]]
    counties = Counter(r["county"] for r in data if r["county"])
    with_inv = [r for r in data if r["invoice"]]
    strict = [r for r in data if r["strict_ok"]]
    noncanon = [r for r in data if r["noncanonical"]]
    ragged = [r for r in data if r["width"] < meta["header_width"]]
    by_class = Counter(r["noncanonical"]["class"] for r in noncanon)
    hazards = [r for r in noncanon if r["noncanonical"].get("hazard")]
    # unique invoice values that are non-canonical
    uniq_nc = {}
    for r in noncanon:
        inv = r["invoice"]
        if inv not in uniq_nc:
            uniq_nc[inv] = r["noncanonical"]

    return {
        "n_data": len(data),
        "sha256": sha,
        "width_counts": meta["width_counts"],
        "n_ragged": len(ragged),
        "n_ragged_missing_fs": sum(1 for r in ragged if not r["filing_status"]),
        "street_max_len": max((len(s) for s in streets), default=0),
        "street_with_state_token": sum(1 for s in streets if _has_state_token(s)),
        "n_distinct_counties": len(counties),
        "top_counties": counties.most_common(12),
        "n_with_invoice": len(with_inv),
        "invoice_fill_pct": round(100.0 * len(with_inv) / len(data), 1) if data else 0,
        "n_blank_invoice": len(data) - len(with_inv),
        "n_strict_ok": len(strict),
        "n_noncanonical_rows": len(noncanon),
        "n_noncanonical_distinct": len(uniq_nc),
        "noncanonical_by_class": dict(by_class),
        "noncanonical_worklist": sorted(uniq_nc.values(), key=lambda x: x["invoice"]),
        "hazards": [
            {
                "invoice": r["invoice"],
                "name": f"{r['t_last']}, {r['t_first']}",
                "lenient_bare": r["lenient_bare"],
                "hazard": r["noncanonical"]["hazard"],
            }
            for r in hazards
        ],
        "has_city_col": any("city" in (h or "").lower() for h in meta["header"]),
        "has_state_col": any(h and h.lower() in ("state", "st") for h in meta["header"]),
        "has_zip_col": any("zip" in (h or "").lower() for h in meta["header"]),
        "header": meta["header"],
    }


def _c5_absent_bares(taxops_bares: set[str]) -> set[str]:
    """ERROR/REVIEW import_rows (and review_queue) whose bare log is still absent from TaxOps."""
    if not C5_THROWAY.exists():
        return set()
    conn = sqlite3.connect(f"file:{C5_THROWAY}?mode=ro", uri=True)
    absent: set[str] = set()
    for row in conn.execute("SELECT raw_json, action, error FROM import_rows"):
        try:
            raw = json.loads(row[0] or "{}")
        except json.JSONDecodeError:
            raw = {}
        logv = raw.get("LOG 2025") or raw.get("log") or raw.get("log_number")
        if not logv:
            for k, v in raw.items():
                if v and "log" in str(k).lower() and "2024" not in str(k):
                    logv = v
                    break
        bare = bare_log_number(str(logv or ""), TAX_YEAR)
        action = (row[1] or "").upper()
        if action in ("ERROR", "REVIEW") and bare and bare not in taxops_bares:
            absent.add(bare)
    # review_queue csv_log when populated
    rq_cols = {r[1] for r in conn.execute("PRAGMA table_info(review_queue)")}
    if "csv_log" in rq_cols:
        for (logv,) in conn.execute(
            "SELECT csv_log FROM review_queue WHERE csv_log IS NOT NULL AND trim(csv_log)!=''"
        ):
            bare = bare_log_number(str(logv), TAX_YEAR)
            if bare and bare not in taxops_bares:
                absent.add(bare)
    conn.close()
    return absent


def _l0_drake_only_bares() -> set[str]:
    if not DISP.exists():
        return set()
    conn = sqlite3.connect(f"file:{DISP}?mode=ro", uri=True)
    out = set()
    for (ek,) in conn.execute(
        "SELECT entity_key FROM audit_disposition "
        "WHERE finding_type='L0_DRAKE_ONLY' AND status='OPEN'"
    ):
        # entity_key = bare|tax_year
        bare = str(ek or "").split("|")[0]
        if bare:
            out.add(bare)
    conn.close()
    return out


def phase1(data: list[dict]) -> dict:
    conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    log_to_clients: dict[str, set[int]] = defaultdict(set)
    client_meta: dict[int, dict] = {}
    taxops_bares: set[str] = set()
    n_clients = conn.execute(
        "SELECT count(*) FROM clients WHERE COALESCE(is_test,0)=0"
    ).fetchone()[0]

    for r in conn.execute(
        """
        SELECT r.client_id, r.log_number, c.last_name, c.first_name,
               c.taxpayer_email, c.taxpayer_dob, c.address,
               c.spouse_last_name, c.spouse_first_name, c.spouse_dob,
               c.spouse_cell, c.spouse_email,
               (SELECT COUNT(*) FROM spouses s WHERE s.client_id=c.id) AS n_spouse_rows
          FROM returns r
          JOIN clients c ON c.id=r.client_id
         WHERE r.tax_year=? AND r.log_number IS NOT NULL AND trim(r.log_number)!=''
           AND COALESCE(c.is_test,0)=0
        """,
        (TAX_YEAR,),
    ):
        bare = bare_log_number(str(r["log_number"]), TAX_YEAR)
        if not bare:
            continue
        taxops_bares.add(bare)
        log_to_clients[bare].add(int(r["client_id"]))
        client_meta[int(r["client_id"])] = dict(r)
    conn.close()

    # Strict-only by_bare
    by_bare: dict[str, list[dict]] = defaultdict(list)
    for r in data:
        if r["bare_log"]:
            by_bare[r["bare_log"]].append(r)
    collision_bares = {b for b, rows in by_bare.items() if len(rows) > 1}
    unique_bares = {b for b, rows in by_bare.items() if len(rows) == 1}

    joinable = []
    skipped = Counter()
    no_taxops_rows = []
    client_to_invoices: dict[int, list[str]] = defaultdict(list)
    double_write_risk = []

    for r in data:
        if not r["invoice"]:
            skipped["blank_invoice"] += 1
            continue
        if not r["strict_ok"]:
            skipped["noncanonical_invoice"] += 1
            continue
        bare = r["bare_log"]
        if bare in collision_bares:
            skipped["collision_bare"] += 1
            continue
        clients = log_to_clients.get(bare) or set()
        if not clients:
            skipped["no_taxops_return"] += 1
            no_taxops_rows.append(r)
            continue
        if len(clients) > 1:
            skipped["multi_client_on_bare"] += 1
            continue
        cid = next(iter(clients))
        client_to_invoices[cid].append(r["invoice"])
        joinable.append(
            {
                "bare_log": bare,
                "invoice": r["invoice"],
                "client_id": cid,
                "export_name": f"{r['t_last']}, {r['t_first']}".strip(", "),
                "taxops_name": f"{client_meta[cid]['last_name']}, {client_meta[cid]['first_name']}",
                "filing_status": r["filing_status"],
                "has_spouse_export": bool(r["spouse_first"] or r["spouse_last"]),
                "n_spouse_rows": client_meta[cid]["n_spouse_rows"],
            }
        )

    for cid, invs in client_to_invoices.items():
        if len(set(invs)) > 1:
            double_write_risk.append({"client_id": cid, "invoices": invs})

    # COALESCE
    coalesce = Counter()
    by_bare_one = {b: rows[0] for b, rows in by_bare.items() if len(rows) == 1}
    for j in joinable:
        c = client_meta[j["client_id"]]
        exp = by_bare_one[j["bare_log"]]
        for field, src, dst in [
            ("taxpayer_email", exp["email"], c["taxpayer_email"]),
            ("taxpayer_dob", exp["dob"], c["taxpayer_dob"]),
            ("address", exp["street"], c["address"]),
            ("spouse_last_name", exp["spouse_last"], c["spouse_last_name"]),
            ("spouse_first_name", exp["spouse_first"], c["spouse_first_name"]),
            ("spouse_dob", exp["spouse_bday"], c["spouse_dob"]),
            ("spouse_cell", exp["spouse_cell"], c["spouse_cell"]),
            ("spouse_email", exp["spouse_email"], c["spouse_email"]),
        ]:
            if src and not (dst or "").strip():
                coalesce[field] += 1

    # Cross-check 98 vs C5 / L0 / approximate L5
    no_taxops_bares = {r["bare_log"] for r in no_taxops_rows if r["bare_log"]}
    c5_absent = _c5_absent_bares(taxops_bares)
    l0_only = _l0_drake_only_bares()
    # A1 L5 ≈ Drake L0-eligible with no TaxOps (and often no log) — use disposition
    # L0_DRAKE_ONLY is the closest stored set; L5 count from A1 was 35

    overlap_c5 = no_taxops_bares & c5_absent
    overlap_l0 = no_taxops_bares & l0_only
    only_profile = no_taxops_bares - c5_absent - l0_only

    return {
        "n_clients_live": n_clients,
        "n_export_strict": sum(1 for r in data if r["strict_ok"]),
        "n_distinct_strict_bares": len(by_bare),
        "n_collision_bares": len(collision_bares),
        "collision_bares": sorted(collision_bares, key=lambda x: int(x) if x.isdigit() else 0),
        "office_packet_overlap": sorted(
            collision_bares & OFFICE_BARES, key=lambda x: int(x) if x.isdigit() else 0
        ),
        "n_unique_bares": len(unique_bares),
        "skipped": dict(skipped),
        "n_joinable": len(joinable),
        "joinable_pct_of_clients": round(100.0 * len(joinable) / n_clients, 1)
        if n_clients
        else 0,
        "n_double_write_risk": len(double_write_risk),
        "double_write_risk": double_write_risk[:20],
        "coalesce_opportunities": dict(coalesce),
        "email_headline": {
            "coalesce_gap": coalesce.get("taxpayer_email", 0),
            "note": "55 filled today -> ~55+gap after COALESCE on joinable set",
        },
        "no_taxops_crosscheck": {
            "n_no_taxops": len(no_taxops_bares),
            "n_c5_error_review_absent": len(c5_absent),
            "n_l0_drake_only_open": len(l0_only),
            "overlap_c5": len(overlap_c5),
            "overlap_l0_drake_only": len(overlap_l0),
            "only_in_profile_export": len(only_profile),
            "overlap_c5_sample": sorted(overlap_c5, key=lambda x: int(x) if x.isdigit() else 0)[
                :25
            ],
            "only_profile_sample": sorted(
                only_profile, key=lambda x: int(x) if x.isdigit() else 0
            )[:25],
            "no_taxops_sample": [
                {
                    "bare": r["bare_log"],
                    "invoice": r["invoice"],
                    "name": f"{r['t_last']}, {r['t_first']}",
                }
                for r in no_taxops_rows[:20]
            ],
        },
        "write_rule_2": (
            "Skip any row whose invoice does not match ^25\\d{4}$ exactly — "
            "no normalization, no repair. Non-canonical -> office worklist."
        ),
    }


def phase4(data: list[dict], n_joinable: int, n_clients: int) -> dict:
    conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    log_to_clients: dict[str, set[int]] = defaultdict(set)
    for r in conn.execute(
        "SELECT client_id, log_number FROM returns "
        "WHERE tax_year=? AND log_number IS NOT NULL AND trim(log_number)!=''",
        (TAX_YEAR,),
    ):
        bare = bare_log_number(str(r[1]), TAX_YEAR)
        if bare:
            log_to_clients[bare].add(int(r[0]))

    has_spouse = {
        int(r[0])
        for r in conn.execute(
            """
            SELECT id FROM clients WHERE COALESCE(is_test,0)=0 AND (
              (spouse_last_name IS NOT NULL AND trim(spouse_last_name)!='')
              OR (spouse_first_name IS NOT NULL AND trim(spouse_first_name)!='')
              OR id IN (SELECT client_id FROM spouses)
            )
            """
        )
    }
    fold_ids = {
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT client_id FROM spouses WHERE source LIKE 'wave4_clients_fold%'"
        )
    }
    conn.close()

    by_bare: dict[str, list[dict]] = defaultdict(list)
    for r in data:
        if r["bare_log"]:
            by_bare[r["bare_log"]].append(r)

    single_with_spouse = []
    mfj_without = []
    fs_counts = Counter()
    linked = 0
    for bare, rows in by_bare.items():
        if len(rows) != 1:
            continue
        exp = rows[0]
        fs = (exp["filing_status"] or "").strip()
        if not fs:
            continue
        fs_counts[fs] += 1
        clients = log_to_clients.get(bare) or set()
        if len(clients) != 1:
            continue
        cid = next(iter(clients))
        linked += 1
        sp = cid in has_spouse
        if fs == "1" and sp:
            single_with_spouse.append(
                {
                    "bare_log": bare,
                    "client_id": cid,
                    "export_name": f"{exp['t_last']}, {exp['t_first']}",
                    "wave4_fold": cid in fold_ids,
                }
            )
        if fs == "2" and not sp:
            mfj_without.append(
                {
                    "bare_log": bare,
                    "client_id": cid,
                    "export_name": f"{exp['t_last']}, {exp['t_first']}",
                    "drake_spouse": f"{exp['spouse_last']}, {exp['spouse_first']}".strip(
                        ", "
                    ),
                }
            )

    coverage = round(100.0 * n_joinable / n_clients, 1) if n_clients else 0
    # Extrapolate if rate holds on unmeasured clients
    scale = (n_clients / n_joinable) if n_joinable else 1.0
    return {
        "n_unique_bare_with_fs_linked": linked,
        "fs_counts": dict(fs_counts),
        "n_single_with_spouse": len(single_with_spouse),
        "n_single_with_spouse_wave4_fold": sum(
            1 for x in single_with_spouse if x["wave4_fold"]
        ),
        "n_mfj_without_spouse": len(mfj_without),
        "scope": {
            "measured_on_joinable": n_joinable,
            "n_clients_live": n_clients,
            "joinable_pct_of_clients": coverage,
            "note": (
                f"Counts are on the joinable {n_joinable} "
                f"({coverage}% of {n_clients} clients). "
                f"If the rate holds firm-wide, expect roughly "
                f"~{int(round(len(single_with_spouse)*scale))} Single+spouse and "
                f"~{int(round(len(mfj_without)*scale))} MFJ+no-spouse."
            ),
            "extrapolated_single_with_spouse": int(
                round(len(single_with_spouse) * scale)
            ),
            "extrapolated_mfj_without_spouse": int(round(len(mfj_without) * scale)),
        },
        "single_with_spouse": single_with_spouse[:40],
        "mfj_without_spouse": mfj_without[:40],
    }


def write_reports(p0: dict, p1: dict, p4: dict, meta: dict, sha: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Malformed worklist
    lines = [
        "# R1 — Non-canonical invoice worklist (office)",
        "",
        f"_Generated: {now} · rule: only `^25\\d{{4}}$` joins; no lenient bare-parse_",
        "",
        f"**{p0['n_noncanonical_distinct']}** distinct non-canonical invoices "
        f"({p0['n_noncanonical_rows']} rows).",
        "",
        "## Hazard",
        "",
    ]
    if p0["hazards"]:
        for h in p0["hazards"]:
            lines.append(
                f"- **`{h['invoice']}`** ({h['name']}) — lenient→`{h['lenient_bare']}` — "
                f"**{h['hazard']}**. Never parse."
            )
    else:
        lines.append("- (none flagged)")
    lines += [
        "",
        "## By class",
        "",
        "| Class | n rows | Proposed reading |",
        "|---|---:|---|",
    ]
    class_help = {
        "25_plus_3": "lenient bare = last 3 digits; likely missing zero-pad — confirm then rewrite as 25XXXX",
        "7_digit": "extra digit; position ambiguous — confirm",
        "not_25_prefix": "prior season or typo — reject for TY2025 join",
        "stub": "never parse (`25`/`250`/`251`)",
        "other_noncanonical": "office confirm",
        "blank": "no invoice",
    }
    for klass, n in sorted(p0["noncanonical_by_class"].items(), key=lambda x: -x[1]):
        lines.append(f"| `{klass}` | {n} | {class_help.get(klass, '')} |")
    lines += [
        "",
        "## Full list",
        "",
        "| Invoice | Class | Lenient bare | Verdict |",
        "|---|---|---|---|",
    ]
    for w in p0["noncanonical_worklist"]:
        mark = " **HAZARD**" if w.get("hazard") else ""
        lines.append(
            f"| `{w['invoice']}` | `{w['class']}` | `{w['lenient_bare'] or '—'}` | "
            f"{w['verdict']}{mark} |"
        )
    OUT_MAL.write_text("\n".join(lines) + "\n", encoding="utf-8")

    OUT_P0.write_text(
        "\n".join(
            [
                "# R1 Phase 0 — export defects (calibration)",
                "",
                f"_Generated: {now}_",
                f"_Canonical ingest: `{meta['canonical_path']}` · sha256 `{sha}`_",
                f"_Drake source (fragile): `{meta['drake_source']}`_",
                "",
                "| Check | Result |",
                "|---|---|",
                f"| Data rows | {p0['n_data']} |",
                f"| Ragged | **{p0['n_ragged']}** `{p0['width_counts']}` |",
                f"| City/State/ZIP cols | "
                f"{p0['has_city_col']}/{p0['has_state_col']}/{p0['has_zip_col']} |",
                f"| Street max / state-token | {p0['street_max_len']} / "
                f"**{p0['street_with_state_token']}** |",
                f"| Invoice fill | {p0['n_with_invoice']} / {p0['n_data']} "
                f"({p0['invoice_fill_pct']}%) |",
                f"| Strict `^25\\d{{4}}$` | **{p0['n_strict_ok']}** |",
                f"| Non-canonical (office worklist) | "
                f"**{p0['n_noncanonical_distinct']}** distinct / "
                f"{p0['n_noncanonical_rows']} rows |",
                f"| Hazards (lenient→141) | **{len(p0['hazards'])}** |",
                "",
                "See `R1-malformed-invoice-worklist.md`. "
                "**Do not bare-parse non-canonical invoices.**",
                "",
            ]
        ),
        encoding="utf-8",
    )

    xc = p1["no_taxops_crosscheck"]
    email = p1["email_headline"]
    OUT_P1.write_text(
        "\n".join(
            [
                "# R1 Phase 1 — join quality (strict invoice)",
                "",
                f"_Generated: {now} · write rule 2: `{p1['write_rule_2']}`_",
                "",
                "## Headline",
                "",
                f"**`taxpayer_email` COALESCE gap = {email['coalesce_gap']}** on joinable set "
                f"— office fill ~55 → ~{55 + email['coalesce_gap']} (~11×).",
                "",
                "| Metric | n |",
                "|---|---:|",
                f"| Strict export rows | {p1['n_export_strict']} |",
                f"| Distinct strict bares | {p1['n_distinct_strict_bares']} |",
                f"| Collision bares | **{p1['n_collision_bares']}** |",
                f"| **Joinable clients** | **{p1['n_joinable']}** "
                f"({p1['joinable_pct_of_clients']}% of {p1['n_clients_live']}) |",
                f"| Double-write risk | **{p1['n_double_write_risk']}** |",
                "",
                "### Skip reasons",
                "",
            ]
            + [f"- `{k}`: {v}" for k, v in sorted(p1["skipped"].items())]
            + [
                "",
                "### COALESCE opportunities",
                "",
                "| Field | n |",
                "|---|---:|",
            ]
            + [
                f"| `{k}` | {v} |"
                for k, v in sorted(
                    p1["coalesce_opportunities"].items(), key=lambda x: -x[1]
                )
            ]
            + [
                "",
                "## No-TaxOps cross-check (strict unique bares)",
                "",
                f"| Set | n |",
                f"|---|---:|",
                f"| Profile export, clean key, no TaxOps return | **{xc['n_no_taxops']}** |",
                f"| C5 ERROR/REVIEW bares still absent from TaxOps | {xc['n_c5_error_review_absent']} |",
                f"| Overlap with C5 absent | **{xc['overlap_c5']}** |",
                f"| A2 `L0_DRAKE_ONLY` OPEN | {xc['n_l0_drake_only_open']} |",
                f"| Overlap with L0_DRAKE_ONLY | **{xc['overlap_l0_drake_only']}** |",
                f"| Only in profile export (not C5∪L0) | **{xc['only_in_profile_export']}** |",
                "",
                (
                    "If C5 overlap is large, these are import-path damage — fixing the "
                    "importer unlocks profiles as a side effect. "
                    f"{xc['n_no_taxops']} > L0_DRAKE_ONLY ({xc['n_l0_drake_only_open']}) "
                    "and A1 L5 (~35), so the profile export is picking up bares those "
                    "ladders do not."
                ),
                "",
                "C5 overlap sample: "
                + ", ".join(f"`{b}`" for b in xc["overlap_c5_sample"]),
                "",
                "Only-profile sample: "
                + ", ".join(f"`{b}`" for b in xc["only_profile_sample"]),
                "",
                f"Acceptance **{'PASS' if p1['n_double_write_risk']==0 else 'FAIL'}** "
                "(zero double-writes).",
                "",
                f"Machine: `{OUT_JSON}`",
                "",
            ]
        ),
        encoding="utf-8",
    )

    sc = p4["scope"]
    OUT_P4.write_text(
        "\n".join(
            [
                "# R1 Phase 4 — Filing Status × spouse (scoped)",
                "",
                f"_Generated: {now}_",
                "",
                f"**Scope:** measured on joinable **{sc['measured_on_joinable']}** "
                f"({sc['joinable_pct_of_clients']}% of {sc['n_clients_live']} clients). "
                f"{sc['note']}",
                "",
                "| Assertion | Measured | Extrapolated (~) |",
                "|---|---:|---:|",
                f"| Drake Single + TaxOps has spouse | **{p4['n_single_with_spouse']}** | "
                f"~{sc['extrapolated_single_with_spouse']} |",
                f"| … wave4_clients_fold | {p4['n_single_with_spouse_wave4_fold']} | — |",
                f"| Drake MFJ + no TaxOps spouse | **{p4['n_mfj_without_spouse']}** | "
                f"~{sc['extrapolated_mfj_without_spouse']} |",
                "",
                "Do not read 15 / 36 as firm-wide totals.",
                "",
                f"Machine: `{OUT_JSON}`",
                "",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    path, sha = ingest_export()
    print("ingested", path, "sha256", sha)
    data, meta = load_export(path)
    meta["sha256"] = sha
    p0 = phase0(data, meta, sha)
    p1 = phase1(data)
    p4 = phase4(data, p1["n_joinable"], p1["n_clients_live"])
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "meta": meta,
        "phase0": p0,
        "phase1": p1,
        "phase4": p4,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_reports(p0, p1, p4, meta, sha)
    print(
        "strict",
        p0["n_strict_ok"],
        "noncanon",
        p0["n_noncanonical_distinct"],
        "hazards",
        len(p0["hazards"]),
    )
    print(
        "joinable",
        p1["n_joinable"],
        "no_taxops",
        p1["no_taxops_crosscheck"]["n_no_taxops"],
        "c5_overlap",
        p1["no_taxops_crosscheck"]["overlap_c5"],
        "email_gap",
        p1["email_headline"]["coalesce_gap"],
    )
    print(
        "P4",
        p4["n_single_with_spouse"],
        "/",
        p4["scope"]["extrapolated_single_with_spouse"],
        "mfj",
        p4["n_mfj_without_spouse"],
        "/",
        p4["scope"]["extrapolated_mfj_without_spouse"],
    )


if __name__ == "__main__":
    main()
