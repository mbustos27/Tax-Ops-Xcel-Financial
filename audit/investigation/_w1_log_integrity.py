"""Wave 1 — Log-number integrity triage (read-only).

Classifies Drake bare-log collisions (a/b/c), malformed invoices, and Tax Log
internal repeats (legitimate multi-row vs genuine reuse). No TaxOps writes.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OUT_MD = Path(r"T:\audit\investigation\W1-log-integrity-triage.md")
OUT_JSON = Path(r"T:\audit\investigation\W1-log-integrity.json")


def _norm_person(last: str, first: str) -> str:
    s = f"{(last or '').upper()}|{(first or '').upper()}"
    s = re.sub(r"[^A-Z0-9|]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _display(last: str, first: str) -> str:
    last = (last or "").strip()
    first = (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


def _surname_token(last: str) -> str:
    last = re.sub(r"[^A-Z0-9 ]", " ", (last or "").upper())
    last = re.sub(r"\s+", " ", last).strip()
    return last.split()[0] if last else ""


def _excel_log_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v).rstrip("0").rstrip(".")
    if isinstance(v, int):
        return str(v)
    return str(v).strip()


def classify_collision(raw_invoices: list[str], claimants: list[dict]) -> dict[str, Any]:
    """Return primary class a|b|c with hybrid notes."""
    surnames = [_surname_token(c.get("last") or "") for c in claimants]
    surnames = [s for s in surnames if s]
    surname_counts = Counter(surnames)
    shared = [s for s, n in surname_counts.items() if n >= 2]
    distinct_raw = sorted(set(raw_invoices))
    format_variant = len(distinct_raw) > 1

    flags: list[str] = []
    if format_variant:
        flags.append("FORMAT_VARIANT")
    if shared and len(claimants) >= 2:
        # majority or all share a surname
        if len(shared) == 1 and surname_counts[shared[0]] == len(claimants):
            flags.append("FAMILY_ALL_SAME_SURNAME")
        elif shared:
            flags.append("FAMILY_PARTIAL_SAME_SURNAME")

    # Primary class
    if "FAMILY_ALL_SAME_SURNAME" in flags and not format_variant:
        primary = "c_family"
    elif "FAMILY_ALL_SAME_SURNAME" in flags and format_variant:
        primary = "c_family"
    elif "FAMILY_PARTIAL_SAME_SURNAME" in flags:
        primary = "c_family_plus_other" if len(claimants) > len(shared) + 0 else "c_family"
        # if there's a third different surname, note reuse
        if len(surname_counts) > 1:
            primary = "c_family_plus_other"
    elif format_variant:
        primary = "a_format_variant"
    else:
        primary = "b_genuine_reuse"

    # Bare 141 style: format variant among multi-surname → treat as b with a note
    if format_variant and len(surname_counts) > 1 and "FAMILY" not in "".join(flags):
        primary = "b_genuine_reuse"

    return {
        "primary": primary,
        "flags": flags,
        "shared_surnames": shared,
        "format_variant": format_variant,
        "distinct_raw_invoices": distinct_raw,
        "n_claimants": len(claimants),
        "n_surnames": len(surname_counts),
    }


def classify_log_repeat(rows: list[dict]) -> dict[str, Any]:
    people = {_norm_person(r["last"], r["first"]) for r in rows}
    people.discard("|")
    people.discard("")
    surnames = [_surname_token(r["last"]) for r in rows]
    surnames = [s for s in surnames if s]
    sc = Counter(surnames)
    shared = [s for s, n in sc.items() if n >= 2]

    if len(people) <= 1:
        kind = "legitimate_same_person"
    elif shared and len(sc) == 1:
        kind = "family_same_surname"
    elif shared and len(sc) > 1:
        kind = "family_plus_other"
    else:
        kind = "genuine_reuse"

    out: dict[str, Any] = {
        "kind": kind,
        "n_rows": len(rows),
        "n_distinct_people": len(people),
        "n_surnames": len(sc),
        "shared_surnames": shared,
        "names": sorted(_display(r["last"], r["first"]) for r in rows),
    }
    # Keep source_row when present (banding / layout diagnostics). Previously dropped.
    if rows and all("source_row" in r for r in rows):
        out["source_rows"] = [int(r["source_row"]) for r in rows]
    return out


def main() -> None:
    import openpyxl
    import sqlite3

    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.invoice_export import (
        DEFAULT_TAXPAYER_INVOICE_PATH,
        bare_log_number,
        parse_taxpayer_invoice_csv,
    )

    mem = load_baseline_memory()
    tax_year = 2025
    inv = parse_taxpayer_invoice_csv(DEFAULT_TAXPAYER_INVOICE_PATH)
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)
    taxops = Path(mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")

    # ── Tax Log index ──────────────────────────────────────────────────
    log_by_bare: dict[str, list[dict]] = defaultdict(list)
    wb = openpyxl.load_workbook(log_path, read_only=True, data_only=True)
    ws = wb[config.SHEET_INDIVIDUALS]
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i < config.LOG_DATA_START_ROW:
            continue
        vals = list(row)
        logn = _excel_log_cell(vals[1]) if len(vals) > 1 else ""
        last = str(vals[config.LOG_LAST_COL] or "").strip() if len(vals) > config.LOG_LAST_COL else ""
        first = (
            str(vals[config.LOG_FIRST_COL] or "").strip()
            if len(vals) > config.LOG_FIRST_COL
            else ""
        )
        if not logn:
            continue
        if not (last or first):
            continue  # match ladder / I3 named-row universe
        bare = bare_log_number(logn, tax_year)
        if not bare:
            continue
        log_by_bare[bare].append(
            {
                "source_row": i,
                "log_raw": logn,
                "last": last,
                "first": first,
                "name": _display(last, first),
                "person_key": _norm_person(last, first),
            }
        )
    wb.close()

    nonempty = sum(len(v) for v in log_by_bare.values())
    distinct = len(log_by_bare)
    repeats = {b: rows for b, rows in log_by_bare.items() if len(rows) > 1}

    log_repeat_classes: dict[str, list[dict]] = defaultdict(list)
    for bare, rows in sorted(repeats.items(), key=lambda x: -len(x[1])):
        cls = classify_log_repeat(rows)
        cls["bare_log"] = bare
        log_repeat_classes[cls["kind"]].append(cls)

    # ── TaxOps index ───────────────────────────────────────────────────
    taxops_by_bare: dict[str, list[dict]] = defaultdict(list)
    conn = sqlite3.connect(f"file:{taxops}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        for row in conn.execute(
            """
                SELECT r.id AS return_id, r.log_number, r.tax_year, r.client_id,
                       c.last_name, c.first_name
                FROM returns r
                JOIN clients c ON c.id = r.client_id
                WHERE r.tax_year = ? AND r.log_number IS NOT NULL AND TRIM(r.log_number) != ''
                """,
            (tax_year,),
        ):
            bare = bare_log_number(str(row["log_number"]), tax_year)
            if not bare:
                continue
            taxops_by_bare[bare].append(
                {
                    "return_id": row["return_id"],
                    "client_id": row["client_id"],
                    "log_number": row["log_number"],
                    "name": _display(row["last_name"], row["first_name"]),
                }
            )
    finally:
        conn.close()

    # ── Drake collisions ───────────────────────────────────────────────
    collisions_out: list[dict] = []
    for c in inv.collisions:
        bare = str(c.get("bare_log") or "")
        claimants_raw = c.get("claimants") or []
        claimants = []
        for cl in claimants_raw:
            name = cl.get("name") or ""
            # recover last/first from person_key or name
            pk = cl.get("person_key") or ""
            if "|" in pk:
                last, first = pk.split("|", 1)
            elif "," in name:
                last, first = [x.strip() for x in name.split(",", 1)]
            else:
                last, first = name, ""
            claimants.append(
                {
                    "name": name,
                    "last": last,
                    "first": first,
                    "raw_invoices": cl.get("raw_invoices") or [],
                    "row_count": cl.get("row_count"),
                }
            )
        raws = list(c.get("raw_invoices") or [])
        cls = classify_collision(raws, claimants)
        log_rows = log_by_bare.get(bare) or []
        log_cls = classify_log_repeat(log_rows) if len(log_rows) > 1 else None
        if log_rows and not log_cls:
            log_cls = {
                "kind": "single_row" if len(log_rows) == 1 else "absent",
                "n_rows": len(log_rows),
                "names": [r["name"] for r in log_rows],
            }
        elif not log_rows:
            log_cls = {"kind": "absent_from_log", "n_rows": 0, "names": []}

        tops = taxops_by_bare.get(bare) or []
        collisions_out.append(
            {
                "bare_log": bare,
                "raw_invoices": raws,
                "claimants": claimants,
                "classification": cls,
                "tax_log": {
                    "n_rows": len(log_rows),
                    "names": [r["name"] for r in log_rows],
                    "classification": log_cls,
                },
                "taxops": {
                    "n_returns": len(tops),
                    "names": [t["name"] for t in tops],
                    "return_ids": [t["return_id"] for t in tops],
                    "client_ids": [t["client_id"] for t in tops],
                },
                "office_fix": _office_fix(cls, bare, claimants, tops, log_rows),
            }
        )

    # ── Malformed ──────────────────────────────────────────────────────
    malformed_out: list[dict] = []
    for m in inv.malformed_invoices:
        inv_no = str(m.get("invoice") or "")
        bare = str(m.get("bare_log") or "")
        reason = m.get("reason") or ""
        note = m.get("note") or ""
        if reason == "empty_bare_after_normalize":
            bucket = "truncated_season_only"
            fix = "Replace truncated Invoice (25 / 250) with the real season-padded log in Drake."
        elif note == "raw_does_not_start_with_season_prefix" or (
            inv_no and not inv_no.startswith("25")
        ):
            bucket = "prior_or_non_season"
            fix = (
                "Confirm whether this is a prior-season invoice left on the TY2025 return; "
                "clear or replace with the TY2025 log, or move the return to the correct season."
            )
        elif reason == "out_of_range":
            bucket = "out_of_range_bare"
            fix = (
                f"Bare {bare} exceeds observed ceiling {inv.bare_log_max}. "
                "Verify digits (typo / extra digit) against Tax Log assignment."
            )
        else:
            bucket = "other"
            fix = "Manual review."
        log_hits = log_by_bare.get(bare) if bare else []
        malformed_out.append(
            {
                "invoice": inv_no,
                "bare_log": bare,
                "name": m.get("name"),
                "reason": reason,
                "note": note,
                "bucket": bucket,
                "office_fix": fix,
                "tax_log_names": [r["name"] for r in (log_hits or [])],
                "taxops_names": [t["name"] for t in (taxops_by_bare.get(bare) or [])] if bare else [],
            }
        )

    # ── Summaries ──────────────────────────────────────────────────────
    coll_by_primary = Counter(c["classification"]["primary"] for c in collisions_out)
    mal_by_bucket = Counter(m["bucket"] for m in malformed_out)
    log_kind_counts = {k: len(v) for k, v in log_repeat_classes.items()}
    log_kind_row_sums = {
        k: sum(x["n_rows"] for x in v) for k, v in log_repeat_classes.items()
    }

    payload = {
        "generated_at": __import__("datetime")
        .datetime.now(__import__("datetime").timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "invoice_layout": inv.layout,
        "l0_eligible": inv.valid_l0_invoice_count,
        "bare_log_max": inv.bare_log_max,
        "drake_collisions": collisions_out,
        "drake_collision_counts": dict(coll_by_primary),
        "malformed": malformed_out,
        "malformed_counts": dict(mal_by_bucket),
        "tax_log": {
            "path": str(log_path),
            "named_nonempty_log_rows": nonempty,
            "distinct_bare_logs": distinct,
            "repeated_bare_logs": len(repeats),
            "repeat_kind_counts": log_kind_counts,
            "repeat_kind_row_sums": log_kind_row_sums,
            "repeats_by_kind": {k: v for k, v in log_repeat_classes.items()},
        },
        "taxops": {
            "path": str(taxops),
            "filled_distinct_note": "TaxOps returns.log_number is unique per tax_year (ux constraint).",
            "n_logs_with_return": len(taxops_by_bare),
        },
        "i7_blocker": {
            "bare_log": "141",
            "note": (
                "I7 pass-2 created return id=2842 log=141 PEREZ & GARCIA VILLAREAL, ANDRES — "
                "resolves only after bare-141 collision is cleared in Drake/Log."
            ),
        },
    }
    # Scrub last4 from markdown path — keep in JSON for office only under audit/
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # ── Markdown (no SSN) ──────────────────────────────────────────────
    lines: list[str] = []
    A = lines.append
    A("# W1 — Log-number integrity triage")
    A("")
    A(f"_Generated: {payload['generated_at']} · layout=`{inv.layout}` · L0-eligible={inv.valid_l0_invoice_count} · bare_max={inv.bare_log_max}_")
    A("")
    A("Findings-only. **No TaxOps writes.** Fixes are office edits in Drake Invoice Number and/or Tax Log.")
    A("")
    A("## Headline")
    A("")
    A(f"| Bucket | Count |")
    A(f"|---|---:|")
    for k, v in sorted(coll_by_primary.items()):
        A(f"| Drake collision `{k}` | {v} |")
    A(f"| Drake malformed (all) | {len(malformed_out)} |")
    A(f"| Tax Log repeated bare logs | {len(repeats)} |")
    for k, v in sorted(log_kind_counts.items()):
        A(f"| Tax Log repeat `{k}` | {v} keys ({log_kind_row_sums[k]} rows) |")
    A("")
    A("### Class legend (Drake collisions)")
    A("")
    A("- **a_format_variant** — same bare from different raw Invoice spellings (e.g. `250141` vs `25141`). Fix pad in Drake.")
    A("- **b_genuine_reuse** — different taxpayers, same bare, one raw form. Assign a new log to the wrong claimant in Log + Drake.")
    A("- **c_family** — claimants share a surname (household / spouse dual Drake records). Decide office rule before re-keying.")
    A("- **c_family_plus_other** — family cluster plus an unrelated third claimant.")
    A("")
    A("## Drake collisions (19) — office queue")
    A("")
    A("| Bare | Class | Drake claimants | Raw invoices | Tax Log (n / names) | TaxOps (n / names) | Fix |")
    A("|---|---|---|---|---|---|---|")
    for c in sorted(collisions_out, key=lambda x: (x["classification"]["primary"], int(x["bare_log"] or 0))):
        cl = c["classification"]
        drake_names = "; ".join(x["name"] for x in c["claimants"])
        raws = ", ".join(f"`{x}`" for x in (c["raw_invoices"] or cl["distinct_raw_invoices"]))
        log_n = c["tax_log"]["n_rows"]
        log_names = "; ".join(c["tax_log"]["names"][:4]) or "—"
        if log_n > 4:
            log_names += f" (+{log_n - 4})"
        top_n = c["taxops"]["n_returns"]
        top_names = "; ".join(c["taxops"]["names"][:3]) or "—"
        A(
            f"| `{c['bare_log']}` | `{cl['primary']}` | {drake_names} | {raws} | "
            f"{log_n} / {log_names} | {top_n} / {top_names} | {c['office_fix']} |"
        )
    A("")
    A("### Priority: bare `141` (I7 blocker)")
    A("")
    c141 = next((x for x in collisions_out if x["bare_log"] == "141"), None)
    if c141:
        A(f"- Class: `{c141['classification']['primary']}` flags={c141['classification']['flags']}")
        A(f"- Drake: {', '.join(x['name'] + ' @' + str(x['raw_invoices']) for x in c141['claimants'])}")
        A(f"- Tax Log: {c141['tax_log']}")
        A(f"- TaxOps: return_ids={c141['taxops']['return_ids']} names={c141['taxops']['names']}")
        A(f"- Fix: {c141['office_fix']}")
        A("- After Drake/Log fix: re-export TAXPAYER.csv → re-run A0–A4; expect I7 PASS if 141 is unique.")
    A("")
    A("## Malformed invoices")
    A("")
    A("| Invoice | Bare | Bucket | Name | Tax Log | Fix |")
    A("|---|---|---|---|---|---|")
    for m in malformed_out:
        A(
            f"| `{m['invoice']}` | `{m['bare_log']}` | `{m['bucket']}` | {m['name']} | "
            f"{'; '.join(m['tax_log_names']) or '—'} | {m['office_fix']} |"
        )
    A("")
    A("## Tax Log internal repeats")
    A("")
    A(
        f"XCEL 2025: **{nonempty}** non-empty log cells → **{distinct}** distinct bare logs → "
        f"**{len(repeats)}** bare logs with >1 named row "
        f"(~{nonempty - distinct} extra rows on repeated keys)."
    )
    A("")
    A("| Kind | Distinct bare logs | Total rows on those keys | Meaning |")
    A("|---|---:|---:|---|")
    meanings = {
        "legitimate_same_person": "Same normalized person on every row — continuation / multi-line entry. Keep.",
        "family_same_surname": "Multiple first names, one surname — household under one log. Office rule.",
        "family_plus_other": "Shared surname plus unrelated name — treat as reuse + family.",
        "genuine_reuse": "Distinct surnames under one log — must split before any Log write-back.",
    }
    for k in ("legitimate_same_person", "family_same_surname", "family_plus_other", "genuine_reuse"):
        if k in log_kind_counts:
            A(f"| `{k}` | {log_kind_counts[k]} | {log_kind_row_sums[k]} | {meanings[k]} |")
    A("")
    A("### Genuine reuse in Tax Log (office must split)")
    A("")
    reuse = log_repeat_classes.get("genuine_reuse") or []
    A(f"**{len(reuse)}** bare logs. Showing all:")
    A("")
    A("| Bare | n_rows | Names |")
    A("|---|---:|---|")
    for x in sorted(reuse, key=lambda r: -r["n_rows"]):
        A(f"| `{x['bare_log']}` | {x['n_rows']} | {'; '.join(x['names'])} |")
    A("")
    A("### Family same-surname repeats (sample)")
    A("")
    fam = log_repeat_classes.get("family_same_surname") or []
    A(f"**{len(fam)}** keys. First 25:")
    A("")
    A("| Bare | n_rows | Names |")
    A("|---|---:|---|")
    for x in sorted(fam, key=lambda r: -r["n_rows"])[:25]:
        A(f"| `{x['bare_log']}` | {x['n_rows']} | {'; '.join(x['names'])} |")
    A("")
    A("### Legitimate same-person repeats (count only)")
    A("")
    leg = log_repeat_classes.get("legitimate_same_person") or []
    A(f"**{len(leg)}** bare logs are multi-row with one normalized person — not collisions for L0 purposes.")
    A("")
    A("## Acceptance vs R0 Wave 1")
    A("")
    A("| Criterion | Status |")
    A("|---|---|")
    A("| Zero Drake bare-log collisions on re-export | **BLOCKED** — office must edit Drake/Log, then re-export |")
    A("| Log-internal repeats classified | **DONE** — see above |")
    A("| A4 I7 PASS | **BLOCKED** on bare `141` until collision cleared |")
    A("")
    A("## Next actions (human)")
    A("")
    A("1. Fix bare `141` first (I7 + three claimants).")
    A("2. Work `b_genuine_reuse` Drake queue (assign new logs).")
    A("3. Decide household rule, then clear `c_family*`.")
    A("4. Clear malformed buckets (truncated / prior-season / out-of-range).")
    A("5. Split Tax Log `genuine_reuse` keys before any Log→TaxOps write-back design.")
    A("6. Re-export `TAXPAYER.csv` → A0→A4.")
    A("")
    A(f"Machine-readable: `{OUT_JSON}`")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_MD}")
    print(f"Wrote {OUT_JSON}")
    print("collision_counts", dict(coll_by_primary))
    print("malformed_counts", dict(mal_by_bucket))
    print("log_repeat_kinds", log_kind_counts)
    print("log_repeats", len(repeats), "of", distinct)


def _office_fix(
    cls: dict,
    bare: str,
    claimants: list[dict],
    tops: list[dict],
    log_rows: list[dict],
) -> str:
    primary = cls["primary"]
    if primary == "a_format_variant":
        return f"Normalize all Drake Invoice spellings to `25`+zero-pad for bare {bare}."
    if primary == "c_family":
        return (
            f"Household under log {bare}: pick one Drake primary (or issue spouse a new log). "
            "Do not merge TaxOps clients until Wave 2A trail exists."
        )
    if primary == "c_family_plus_other":
        return (
            f"Family share + outsider on {bare}: peel the unrelated claimant to a new log; "
            "then apply household rule to the family remainder."
        )

    def _tokens(name: str) -> set[str]:
        return {t for t in re.findall(r"[A-Z0-9]+", (name or "").upper()) if len(t) > 1}

    drake_tok = set()
    for c in claimants:
        drake_tok |= _tokens(c.get("name") or "")
    log_tok = set()
    for r in log_rows:
        log_tok |= _tokens(r.get("name") or "")
    office_tok = drake_tok | log_tok

    keep_hint = ""
    if len(tops) == 1:
        top_name = tops[0]["name"]
        top_tok = _tokens(top_name)
        if office_tok and top_tok.isdisjoint(office_tok):
            keep_hint = (
                f" TaxOps return {tops[0]['return_id']} is `{top_name}` — **name does not overlap** "
                f"Drake/Log claimants; do not treat TaxOps as keeper — reconcile all three systems."
            )
        else:
            keep_hint = (
                f" TaxOps already holds `{top_name}` (return {tops[0]['return_id']}) — "
                "prefer that claim if it matches Log; re-key the others."
            )
    elif len(log_rows) == 1:
        keep_hint = f" Tax Log has single row `{log_rows[0]['name']}` — prefer that owner."
    fmt = ""
    if cls.get("format_variant"):
        fmt = f" Also normalize raw Invoice spellings {cls.get('distinct_raw_invoices')}."
    return (
        f"Genuine reuse of log {bare}: issue a new log to every Drake claimant except the keeper."
        f"{keep_hint}{fmt}"
    )


if __name__ == "__main__":
    main()
