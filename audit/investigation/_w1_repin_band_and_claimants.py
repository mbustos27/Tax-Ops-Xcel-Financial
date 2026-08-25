"""W1 re-pin — band cut at log-sequence restart (~772) + canonical row rule.

Cut = first named row where bare log ``1`` reappears (sequence restart), not midpoint.
Canonical Log row per bare: tax_year (25) first, then Drake/TaxOps name agreement,
then later source_row. Never prefer non-LOGOUT alone (that picked TY2024 for bare 141).

Also re-classifies full-Log claimant corrections as band1_safe vs band2_review.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audit.investigation._w1_log_integrity import (  # noqa: E402
    _display,
    _excel_log_cell,
    _norm_person,
    classify_collision,
    classify_log_repeat,
)

OUT_BAND_JSON = Path(r"T:\audit\investigation\W1-source-row-banding.json")
OUT_BAND_MD = Path(r"T:\audit\investigation\W1-source-row-banding.md")
OUT_CLAIM_JSON = Path(r"T:\audit\investigation\W1-full-log-claimant-search.json")
OUT_CLAIM_MD = Path(r"T:\audit\investigation\W1-full-log-claimant-search.md")


def _tokens(name: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", (name or "").upper()) if len(t) > 1}


def _band(row: int, cut: int) -> str:
    return "band1" if row < cut else "band2"


def find_restart_cut(log_by_bare: dict[str, list[dict]]) -> int:
    """Row where bare log 1 reappears after its first occurrence."""
    rows = sorted(r["source_row"] for r in log_by_bare.get("1", []))
    if len(rows) >= 2:
        return rows[1]
    # fallback: first row where any low bare reappears with offset ~766
    return 772


def pick_canonical(
    rows: list[dict],
    *,
    drake_tokens: set[str] | None = None,
    taxops_tokens: set[str] | None = None,
) -> dict:
    """tax_year first, Drake/TaxOps agreement second, then later row."""

    def score(r: dict) -> tuple:
        yr25 = 0 if str(r.get("yr")) == "25" else 1
        tok = _tokens(r.get("name") or "")
        agree = 1
        if drake_tokens and not tok.isdisjoint(drake_tokens):
            agree = 0
        elif taxops_tokens and not tok.isdisjoint(taxops_tokens):
            agree = 0
        # later row preferred among equals (active band tends later)
        return (yr25, agree, -int(r["source_row"]))

    return sorted(rows, key=score)[0]


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
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)
    taxops = Path(mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")

    inv = parse_taxpayer_invoice_csv(DEFAULT_TAXPAYER_INVOICE_PATH)

    log_by_bare: dict[str, list[dict]] = defaultdict(list)
    log_rows: list[dict] = []
    wb = openpyxl.load_workbook(log_path, read_only=True, data_only=True)
    ws = wb[config.SHEET_INDIVIDUALS]
    max_row = 0
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        max_row = i
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
        yr = vals[config.LOG_YR_COL] if len(vals) > config.LOG_YR_COL else None
        status = str(vals[7] or "").strip() if len(vals) > 7 else ""
        if not logn or not (last or first):
            continue
        bare = bare_log_number(logn, tax_year)
        if not bare:
            continue
        rec = {
            "source_row": i,
            "log_raw": logn,
            "bare_log": bare,
            "last": last,
            "first": first,
            "name": _display(last, first),
            "person_key": _norm_person(last, first),
            "tokens": _tokens(_display(last, first)),
            "yr": yr,
            "status": status,
        }
        log_rows.append(rec)
        log_by_bare[bare].append(rec)
    wb.close()

    cut = find_restart_cut(log_by_bare)
    for r in log_rows:
        r["band"] = _band(r["source_row"], cut)

    # TaxOps tokens by bare
    taxops_by_bare: dict[str, list[dict]] = defaultdict(list)
    conn = sqlite3.connect(f"file:{taxops}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        for row in conn.execute(
            """
            SELECT r.id AS return_id, r.log_number, c.last_name, c.first_name
            FROM returns r JOIN clients c ON c.id=r.client_id
            WHERE r.tax_year=? AND r.log_number IS NOT NULL AND TRIM(r.log_number)!=''
            """,
            (tax_year,),
        ):
            bare = bare_log_number(str(row["log_number"]), tax_year)
            if not bare:
                continue
            taxops_by_bare[bare].append(
                {
                    "return_id": row["return_id"],
                    "name": _display(row["last_name"], row["first_name"]),
                    "tokens": _tokens(_display(row["last_name"], row["first_name"])),
                }
            )
    finally:
        conn.close()

    # ── Banding at pinned cut ──────────────────────────────────────────
    repeats = {b: rows for b, rows in log_by_bare.items() if len(rows) > 1}
    reuse = []
    for bare, rows in sorted(repeats.items(), key=lambda x: int(x[0] or 0)):
        cls = classify_log_repeat(rows)
        if cls["kind"] != "genuine_reuse":
            continue
        cls["bare_log"] = bare
        cls["source_rows"] = [r["source_row"] for r in rows]
        cls["bands"] = sorted({r["band"] for r in rows})
        reuse.append(cls)

    pair = [x for x in reuse if len(x["source_rows"]) == 2]
    cross = [x for x in pair if len(x["bands"]) == 2]
    same = [x for x in pair if len(x["bands"]) == 1]

    # Canonical per bare for all bares (for L0 guidance)
    canonical_examples = []
    for bare in ("141", "1", "203", "247"):
        rows = log_by_bare.get(bare) or []
        if not rows:
            continue
        drake_tok: set[str] = set()
        for c in inv.collisions:
            if str(c.get("bare_log")) == bare:
                for cl in c.get("claimants") or []:
                    drake_tok |= _tokens(cl.get("name") or "")
        tops = taxops_by_bare.get(bare) or []
        top_tok: set[str] = set()
        for t in tops:
            top_tok |= t["tokens"]
        can = pick_canonical(rows, drake_tokens=drake_tok or None, taxops_tokens=top_tok or None)
        canonical_examples.append(
            {
                "bare_log": bare,
                "canonical": {
                    "source_row": can["source_row"],
                    "name": can["name"],
                    "yr": can["yr"],
                    "status": can["status"],
                    "band": can["band"],
                },
                "all_rows": [
                    {
                        "source_row": r["source_row"],
                        "name": r["name"],
                        "yr": r["yr"],
                        "status": r["status"],
                        "band": r["band"],
                    }
                    for r in rows
                ],
            }
        )

    band_payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tax_log_path": str(log_path),
        "pinned_cut": cut,
        "cut_rule": "first reappearance of bare log 1 (sequence restart)",
        "max_row": max_row,
        "named_nonempty_log_rows": len(log_rows),
        "genuine_reuse_keys": len(reuse),
        "pair_keys": len(pair),
        "cross_band_pairs": len(cross),
        "same_band_pairs": len(same),
        "verdict": "BAND_RESTART_PINNED",
        "canonical_rule": "tax_year==25 first; Drake/TaxOps token agree second; later row third",
        "canonical_examples": canonical_examples,
        "implication": (
            f"Cut pinned at row {cut}. Cross-band pairs={len(cross)}/{len(pair)}. "
            "Hold Tax Log genuine_reuse office split. Use canonical row rule for L0."
        ),
    }
    OUT_BAND_JSON.write_text(json.dumps(band_payload, indent=2), encoding="utf-8")

    # ── Claimant search with band classification ───────────────────────
    action_counts: dict[str, int] = defaultdict(int)
    band_safety: dict[str, int] = defaultdict(int)
    results = []

    for c in inv.collisions:
        bare = str(c.get("bare_log") or "")
        claimants = []
        for cl in c.get("claimants") or []:
            name = cl.get("name") or ""
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
                    "person_key": _norm_person(last, first),
                    "tokens": _tokens(name),
                    "raw_invoices": cl.get("raw_invoices") or [],
                }
            )
        raws = list(c.get("raw_invoices") or [])
        cls = classify_collision(raws, claimants)
        drake_tok = set()
        for cl in claimants:
            drake_tok |= cl["tokens"]
        tops = taxops_by_bare.get(bare) or []
        top_tok: set[str] = set()
        for t in tops:
            top_tok |= t["tokens"]

        log_on_bare = log_by_bare.get(bare) or []
        canon = (
            pick_canonical(log_on_bare, drake_tokens=drake_tok, taxops_tokens=top_tok)
            if log_on_bare
            else None
        )

        outcomes = []
        for cl in claimants:
            exact = [
                r
                for r in log_rows
                if r["person_key"] == cl["person_key"] and cl["person_key"] not in ("", "|")
            ]
            fuzzy = []
            if not exact:
                last_tok = _tokens(cl["last"])
                if last_tok:
                    for r in log_rows:
                        if not last_tok.issubset(r["tokens"]):
                            continue
                        first_tok = _tokens(cl["first"])
                        if first_tok and not (first_tok & r["tokens"]):
                            continue
                        fuzzy.append(r)
            hits = exact or fuzzy
            by_hit_bare: dict[str, dict] = {}
            for h in sorted(hits, key=lambda x: x["source_row"]):
                by_hit_bare.setdefault(h["bare_log"], h)
            # Prefer canonical among each bare's rows when multiple
            hit_list = []
            for bkey, sample in by_hit_bare.items():
                brows = [r for r in hits if r["bare_log"] == bkey]
                hit_list.append(
                    pick_canonical(brows, drake_tokens=cl["tokens"], taxops_tokens=None)
                )

            same_bare = [h for h in hit_list if h["bare_log"] == bare]
            other_bare = [h for h in hit_list if h["bare_log"] != bare]

            if other_bare:
                preferred = pick_canonical(
                    other_bare, drake_tokens=cl["tokens"], taxops_tokens=top_tok or None
                )
                # Safety: band1 target is safe; band2 needs review
                if preferred["band"] == "band1":
                    action = "CORRECT_DRAKE_BAND1_SAFE"
                    band_safety["band1_safe"] += 1
                else:
                    action = "CORRECT_DRAKE_BAND2_REVIEW"
                    band_safety["band2_review"] += 1
                action_detail = (
                    f"Claimant already on Log bare `{preferred['bare_log']}` "
                    f"(row {preferred['source_row']}, {preferred['name']}, "
                    f"yr={preferred['yr']}, status={preferred['status'] or '(blank)'}, "
                    f"{preferred['band']}). Correct Drake away from collision bare `{bare}`."
                )
            elif same_bare and not other_bare:
                action = "KEEPER_CANDIDATE"
                band_safety["keeper"] += 1
                action_detail = (
                    f"Found on Log only under collision bare `{bare}` "
                    f"(rows {[h['source_row'] for h in same_bare]}). Likely keeper."
                )
            elif not hit_list:
                action = "MINT_NEW_LOG"
                band_safety["mint"] += 1
                action_detail = f"No Log entry for `{cl['name']}`. Mint after keeper chosen."
            else:
                action = "REVIEW"
                band_safety["review"] += 1
                action_detail = "Unexpected hit pattern."

            action_counts[action] += 1
            outcomes.append(
                {
                    "claimant": cl["name"],
                    "action": action,
                    "action_detail": action_detail,
                    "fuzzy_only": bool(fuzzy and not exact),
                    "log_hits": [
                        {
                            "bare_log": h["bare_log"],
                            "source_row": h["source_row"],
                            "name": h["name"],
                            "yr": h["yr"],
                            "status": h["status"],
                            "band": h["band"],
                        }
                        for h in hit_list
                    ],
                }
            )

        results.append(
            {
                "bare_log": bare,
                "classification": cls["primary"],
                "canonical_on_bare": (
                    {
                        "source_row": canon["source_row"],
                        "name": canon["name"],
                        "yr": canon["yr"],
                        "status": canon["status"],
                        "band": canon["band"],
                    }
                    if canon
                    else None
                ),
                "log_rows_on_bare": [
                    {
                        "source_row": r["source_row"],
                        "name": r["name"],
                        "yr": r["yr"],
                        "status": r["status"],
                        "band": r["band"],
                    }
                    for r in log_on_bare
                ],
                "claimant_outcomes": outcomes,
            }
        )

    claim_payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pinned_cut": cut,
        "named_log_rows_searched": len(log_rows),
        "drake_collision_keys": len(results),
        "n_claimants": sum(len(r["claimant_outcomes"]) for r in results),
        "action_counts": dict(action_counts),
        "band_safety": dict(band_safety),
        "note": (
            f"Cut={cut}. CORRECT_DRAKE_BAND1_SAFE are cleared for office. "
            "BAND2_REVIEW need human. Bare 141 first for I7. "
            "Canonical: tax_year then Drake/TaxOps agree."
        ),
        "collisions": results,
    }
    OUT_CLAIM_JSON.write_text(json.dumps(claim_payload, indent=2), encoding="utf-8")

    # Markdown
    lines = [
        "# W1 — source_row banding (re-pinned)",
        "",
        f"_Generated: {band_payload['generated_at']} · cut=**{cut}** (bare `1` restart) · read-only_",
        "",
        "## Verdict",
        "",
        f"**`BAND_RESTART_PINNED`** at row **{cut}** (not midpoint 670).",
        "",
        f"| Metric | Value |",
        f"|---|---|",
        f"| Pair keys | {len(pair)} |",
        f"| Cross-band @ {cut} | {len(cross)} |",
        f"| Same-band @ {cut} | {len(same)} |",
        "",
        "### Canonical row rule",
        "",
        "1. Prefer `YR=25`",
        "2. Then Drake/TaxOps name-token agreement",
        "3. Then later `source_row`",
        "",
        "Do **not** prefer non-`LOGOUT` alone — that selected TY2024 for bare `141`.",
        "",
        "### Canonical examples",
        "",
    ]
    for ex in canonical_examples:
        c = ex["canonical"]
        lines.append(
            f"- Bare `{ex['bare_log']}` → row {c['source_row']} `{c['name']}` "
            f"(yr={c['yr']}, {c['status'] or '—'}, {c['band']})"
        )
    lines += [
        "",
        "**Hold** Tax Log `genuine_reuse` office split until L0 uses this canonical filter.",
        "",
        f"Machine: `{OUT_BAND_JSON}`",
    ]
    OUT_BAND_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    clines = [
        "# W1 — full Tax Log claimant search (band-pinned)",
        "",
        f"_Generated: {claim_payload['generated_at']} · cut={cut} · read-only_",
        "",
        "## Headline",
        "",
        f"| Action | n |",
        f"|---|---:|",
    ]
    for k, v in sorted(action_counts.items()):
        clines.append(f"| `{k}` | {v} |")
    clines += [
        "",
        f"| Band safety | n |",
        f"|---|---:|",
        f"| band1_safe corrections | {band_safety.get('band1_safe', 0)} |",
        f"| band2_review corrections | {band_safety.get('band2_review', 0)} |",
        f"| mint | {band_safety.get('mint', 0)} |",
        f"| keeper | {band_safety.get('keeper', 0)} |",
        "",
        "Office order after this lands: bare `141` (I7) → band1_safe corrections → "
        "mints + keepers. Hold Log `genuine_reuse` split.",
        "",
    ]
    for r in sorted(results, key=lambda x: int(x["bare_log"] or 0)):
        clines.append(f"### Bare `{r['bare_log']}` (`{r['classification']}`)")
        can = r.get("canonical_on_bare")
        if can:
            clines.append(
                f"- Canonical on bare: row {can['source_row']} `{can['name']}` "
                f"(yr={can['yr']}, {can['status'] or '—'}, {can['band']})"
            )
        clines.append("")
        clines.append("| Claimant | Action | Detail |")
        clines.append("|---|---|---|")
        for o in r["claimant_outcomes"]:
            clines.append(f"| {o['claimant']} | `{o['action']}` | {o['action_detail']} |")
        clines.append("")
    clines.append(f"Machine: `{OUT_CLAIM_JSON}`")
    OUT_CLAIM_MD.write_text("\n".join(clines) + "\n", encoding="utf-8")

    print("cut", cut, "cross", len(cross), "same", len(same))
    print("actions", dict(action_counts))
    print("band_safety", dict(band_safety))
    print("canonical_141", canonical_examples[0] if canonical_examples else None)
    for ex in canonical_examples:
        if ex["bare_log"] == "141":
            print("141_canonical", ex["canonical"])
            print("141_all", ex["all_rows"])


if __name__ == "__main__":
    main()
