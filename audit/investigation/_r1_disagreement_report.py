"""R1 Phase 3 prep — PROFILE disagreement report (read-only, no writes).

For each strict ^25\\d{4}$ export row that joins to a TaxOps client (including
collision bares for measurement), compare fields where BOTH sides are non-empty.
Classify:

  PROFILE_TRUNCATION   — Drake is superstring / TaxOps clipped (~40) / prefix match
  PROFILE_CONTAMINATION — TaxOps value names a different real person found elsewhere
  PROFILE_STALE         — differs, neither of the above

Never matches on name to establish the link — invoice→bare→returns only.
**However**, profile COALESCE *writes* must also pass ``audit.profile_join``
name affinity (OK/VARIANT). See R1-status.md "Name gate".
"""
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

EXPORT = Path(r"T:\audit\investigation\exports\TAXPAYERspouseaddressstatus.csv")
TAXOPS = Path(r"T:\taxops\taxops.db")
TAX_YEAR = 2025
STRICT = re.compile(r"^25\d{4}$")
OUT_JSON = Path(r"T:\audit\investigation\R1-disagreement-report.json")
OUT_MD = Path(r"T:\audit\investigation\R1-disagreement-report.md")

# Known Wave 4/5 contamination smoking guns (confirm still present)
KNOWN_CONTAM = {
    610: "MARTINEZ/TAREEN",
    819: "QUINTANA/NELSON",
    495: "HUERTA/EVELYN",  # may already be fixed to BRIDGET
}


def _cell(row, idx, col):
    i = idx.get(col)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9@.+ ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _norm_email(s: str) -> str:
    """Lowercase; if comma/semicolon list, compare on first address only."""
    s = (s or "").strip().lower()
    if not s:
        return ""
    first = re.split(r"[,;]", s, maxsplit=1)[0].strip()
    return first


def _norm_dob(s: str) -> str:
    """Normalize to YYYY-MM-DD when parseable; else empty (fall through to string compare)."""
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # bare digits YYYYMMDD
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8:
        try:
            return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def _values_agree(field: str, taxops: str, drake: str) -> bool:
    if field in ("taxpayer_dob", "spouse_dob"):
        a, b = _norm_dob(taxops), _norm_dob(drake)
        if a and b:
            return a == b
    if field in ("taxpayer_email", "spouse_email"):
        a, b = _norm_email(taxops), _norm_email(drake)
        if a and b and a == b:
            return True
        # Drake multi-email containing TaxOps address
        if a and a in _norm(drake).lower().replace(" ", ""):
            return True
    return _norm(taxops) == _norm(drake)


def _tokens(s: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", _norm(s)) if len(t) > 1}


def _empty(s) -> bool:
    return not (s or "").strip()


def is_truncation(taxops: str, drake: str) -> bool:
    a, b = _norm(taxops), _norm(drake)
    if not a or not b or a == b:
        return False
    # Drake is superstring of TaxOps
    if a in b and len(b) > len(a):
        return True
    # TaxOps clipped near Drake 40-char ceiling
    if len(a) >= 38 and b.startswith(a[:30]):
        return True
    # token bag of TaxOps is subset of Drake (partial name)
    ta, tb = _tokens(a), _tokens(b)
    if ta and ta < tb:
        return True
    return False


def load_export() -> list[dict]:
    rows = list(csv.reader(EXPORT.open(encoding="utf-8-sig", newline="")))
    header = rows[2]
    idx = {h: i for i, h in enumerate(header)}
    out = []
    for n, row in enumerate(rows[3:], start=4):
        if not row or not any((c or "").strip() for c in row):
            continue
        padded = list(row) + [""] * max(0, len(header) - len(row))
        inv = _cell(padded, idx, "Invoice Number")
        if not STRICT.fullmatch(inv or ""):
            continue
        bare = bare_log_number(inv, TAX_YEAR)
        if not bare:
            continue
        out.append(
            {
                "source_row": n,
                "invoice": inv,
                "bare_log": bare,
                "t_last": _cell(padded, idx, "Taxpayer Last Name"),
                "t_first": _cell(padded, idx, "Taxpayer First Name"),
                "email": _cell(padded, idx, "Email Address"),
                "dob": _cell(padded, idx, "Taxpayer Date of Birth"),
                "street": _cell(padded, idx, "Street Address"),
                "spouse_last": _cell(padded, idx, "Spouse Last Name"),
                "spouse_first": _cell(padded, idx, "Spouse First Name"),
                "spouse_dob": _cell(padded, idx, "Spouse Date of Birth")
                or _cell(padded, idx, "Spouse Birthday"),
                "spouse_cell": _cell(padded, idx, "Spouse Cell Phone"),
                "spouse_email": _cell(padded, idx, "Spouse Email Address"),
            }
        )
    return out


def main() -> None:
    export = load_export()
    conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    log_to_clients: dict[str, set[int]] = defaultdict(set)
    clients: dict[int, dict] = {}
    # Index of person-ish strings elsewhere for contamination
    name_index: dict[str, list[tuple[str, int]]] = defaultdict(list)  # norm -> [(where, id)]

    for r in conn.execute(
        """
        SELECT r.client_id, r.log_number,
               c.last_name, c.first_name, c.taxpayer_email, c.taxpayer_dob, c.address,
               c.spouse_last_name, c.spouse_first_name, c.spouse_dob,
               c.spouse_cell, c.spouse_email
          FROM returns r JOIN clients c ON c.id=r.client_id
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
        clients[cid] = dict(r)

    for cid, c in clients.items():
        for label, val in [
            ("client", f"{c['last_name']}, {c['first_name']}"),
            ("spouse_cols", f"{c['spouse_last_name']}, {c['spouse_first_name']}"),
        ]:
            n = _norm(val)
            if n and n != ",":
                name_index[n].append((label, cid))

    for r in conn.execute(
        "SELECT client_id, last_name, first_name FROM spouses WHERE COALESCE(last_name,'')!='' OR COALESCE(first_name,'')!=''"
    ):
        n = _norm(f"{r['last_name']}, {r['first_name']}")
        if n and n != ",":
            name_index[n].append(("spouses", int(r["client_id"])))

    # Collision bares in export
    by_bare: dict[str, list[dict]] = defaultdict(list)
    for e in export:
        by_bare[e["bare_log"]].append(e)
    collision = {b for b, rs in by_bare.items() if len(rs) > 1}

    findings = []
    compared = Counter()
    agree = Counter()

    fields = [
        ("taxpayer_email", "email", "taxpayer_email"),
        ("taxpayer_dob", "dob", "taxpayer_dob"),
        ("address", "street", "address"),
        ("spouse_last_name", "spouse_last", "spouse_last_name"),
        ("spouse_first_name", "spouse_first", "spouse_first_name"),
        ("spouse_dob", "spouse_dob", "spouse_dob"),
        ("spouse_cell", "spouse_cell", "spouse_cell"),
        ("spouse_email", "spouse_email", "spouse_email"),
    ]

    for e in export:
        bare = e["bare_log"]
        clients_hit = log_to_clients.get(bare) or set()
        if len(clients_hit) != 1:
            continue
        cid = next(iter(clients_hit))
        c = clients[cid]
        on_collision = bare in collision

        for field, exp_key, cli_key in fields:
            dv = (e.get(exp_key) or "").strip()
            tv = (c.get(cli_key) or "").strip()
            if _empty(dv) or _empty(tv):
                continue
            compared[field] += 1
            if _values_agree(field, tv, dv):
                agree[field] += 1
                continue

            # classify
            kind = "PROFILE_STALE"
            detail = ""
            if is_truncation(tv, dv):
                kind = "PROFILE_TRUNCATION"
                detail = "drake_superstring_or_clip"
            else:
                # contamination: TaxOps spouse/name appears as someone else's identity
                if field.startswith("spouse_"):
                    taxops_spouse = _norm(
                        f"{c.get('spouse_last_name')}, {c.get('spouse_first_name')}"
                    )
                    elsewhere = [
                        (where, oid)
                        for where, oid in name_index.get(taxops_spouse, [])
                        if oid != cid
                    ]
                    # also: taxops spouse tokens match a different client's primary name
                    if elsewhere:
                        kind = "PROFILE_CONTAMINATION"
                        detail = f"taxops_spouse_elsewhere:{elsewhere[:3]}"
                    else:
                        # TaxOps spouse name equals another client's primary
                        for other_cid, oc in clients.items():
                            if other_cid == cid:
                                continue
                            other_primary = _norm(f"{oc['last_name']}, {oc['first_name']}")
                            if taxops_spouse and taxops_spouse == other_primary:
                                kind = "PROFILE_CONTAMINATION"
                                detail = f"taxops_spouse_is_client:{other_cid}"
                                break
                        # Drake spouse vs TaxOps spouse token-disjoint + both real names
                        if kind != "PROFILE_CONTAMINATION":
                            dt = _tokens(f"{e['spouse_last']} {e['spouse_first']}")
                            tt = _tokens(
                                f"{c.get('spouse_last_name')} {c.get('spouse_first_name')}"
                            )
                            if dt and tt and dt.isdisjoint(tt) and len(tt) >= 1:
                                # check taxops spouse appears as primary elsewhere
                                for other_cid, oc in clients.items():
                                    if other_cid == cid:
                                        continue
                                    op = _tokens(f"{oc['last_name']} {oc['first_name']}")
                                    if tt & op and len(tt & op) >= max(1, len(tt) - 1):
                                        kind = "PROFILE_CONTAMINATION"
                                        detail = f"taxops_spouse_matches_client:{other_cid}"
                                        break

            findings.append(
                {
                    "kind": kind,
                    "field": field,
                    "client_id": cid,
                    "bare_log": bare,
                    "invoice": e["invoice"],
                    "on_collision_bare": on_collision,
                    "export_name": f"{e['t_last']}, {e['t_first']}",
                    "taxops_name": f"{c['last_name']}, {c['first_name']}",
                    "drake": dv,
                    "taxops": tv,
                    "detail": detail,
                    "known_contam_tag": KNOWN_CONTAM.get(cid),
                }
            )

    conn.close()

    by_kind = Counter(f["kind"] for f in findings)
    by_field = Counter(f["field"] for f in findings)
    contam = [f for f in findings if f["kind"] == "PROFILE_CONTAMINATION"]
    trunc = [f for f in findings if f["kind"] == "PROFILE_TRUNCATION"]
    stale = [f for f in findings if f["kind"] == "PROFILE_STALE"]

    # unique clients in contam
    contam_clients = sorted({f["client_id"] for f in contam})

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_export_strict": len(export),
        "n_collision_bares": len(collision),
        "compared_both_present": dict(compared),
        "agree": dict(agree),
        "n_findings": len(findings),
        "by_kind": dict(by_kind),
        "by_field": dict(by_field),
        "n_contam_clients": len(contam_clients),
        "contam_client_ids": contam_clients,
        "findings": findings,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# R1 — PROFILE disagreement report (pre-write)",
        "",
        f"_Generated: {payload['generated_at']} · strict `^25\\d{{4}}$` · invoice→bare→client only_",
        "",
        "Where TaxOps and Drake both hold a value and they differ — **no writes**.",
        "",
        "| Kind | n |",
        "|---|---:|",
        f"| `PROFILE_TRUNCATION` | {by_kind.get('PROFILE_TRUNCATION', 0)} |",
        f"| `PROFILE_CONTAMINATION` | {by_kind.get('PROFILE_CONTAMINATION', 0)} |",
        f"| `PROFILE_STALE` | {by_kind.get('PROFILE_STALE', 0)} |",
        f"| **Total disagreements** | **{len(findings)}** |",
        f"| Distinct contamination clients | **{len(contam_clients)}** |",
        "",
        "## Compared (both sides non-empty)",
        "",
        "| Field | Compared | Agree |",
        "|---|---:|---:|",
    ]
    for field in sorted(compared):
        lines.append(
            f"| `{field}` | {compared[field]} | {agree.get(field, 0)} |"
        )

    lines += [
        "",
        f"## PROFILE_CONTAMINATION ({len(contam)} findings / {len(contam_clients)} clients)",
        "",
        "TaxOps holds a different real person. Escalate — do not auto-fix.",
        "",
        "| Client | Bare | Field | TaxOps | Drake | Detail |",
        "|---|---|---|---|---|---|",
    ]
    for f in contam[:40]:
        lines.append(
            f"| `{f['client_id']}` | `{f['bare_log']}` | `{f['field']}` | "
            f"{f['taxops'][:40]} | {f['drake'][:40]} | {f['detail'][:50]} |"
        )
    if len(contam) > 40:
        lines.append(f"| … | +{len(contam)-40} more | | | | |")

    lines += [
        "",
        f"## PROFILE_TRUNCATION ({len(trunc)})",
        "",
        "| Client | Field | TaxOps | Drake |",
        "|---|---|---|---|",
    ]
    for f in trunc[:25]:
        lines.append(
            f"| `{f['client_id']}` | `{f['field']}` | {f['taxops'][:40]} | {f['drake'][:50]} |"
        )

    lines += [
        "",
        f"## PROFILE_STALE sample ({min(25, len(stale))} / {len(stale)})",
        "",
        "| Client | Field | TaxOps | Drake |",
        "|---|---|---|---|",
    ]
    for f in stale[:25]:
        lines.append(
            f"| `{f['client_id']}` | `{f['field']}` | {f['taxops'][:40]} | {f['drake'][:40]} |"
        )

    lines += [
        "",
        "Prior 3/22 sample rate was too thin. This is the full strict-invoice disagreement census.",
        "",
        f"Machine: `{OUT_JSON}`",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(
        "findings",
        len(findings),
        "by_kind",
        dict(by_kind),
        "contam_clients",
        len(contam_clients),
    )


if __name__ == "__main__":
    main()
