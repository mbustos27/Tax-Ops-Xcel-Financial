"""Full Tax Log claimant search for Drake collision claimants (read-only).

Invert W1's re-key-first rule: search the whole Log for each Drake collision
claimant by name; if they already hold a different log, prefer correcting Drake
to that log. Mint new numbers only when the claimant has no Log entry.

Cross-check previously covered only the 206 repeated keys (~1/3 of Log).
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OUT_JSON = Path(r"T:\audit\investigation\W1-full-log-claimant-search.json")
OUT_MD = Path(r"T:\audit\investigation\W1-full-log-claimant-search.md")


def _tokens(name: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", (name or "").upper()) if len(t) > 1}


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


def _excel_log_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, float):
        return str(v).rstrip("0").rstrip(".")
    if isinstance(v, int):
        return str(v)
    return str(v).strip()


def main() -> None:
    import openpyxl

    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.invoice_export import (
        DEFAULT_TAXPAYER_INVOICE_PATH,
        bare_log_number,
        parse_taxpayer_invoice_csv,
    )
    from audit.investigation._w1_log_integrity import classify_collision

    mem = load_baseline_memory()
    tax_year = 2025
    inv = parse_taxpayer_invoice_csv(DEFAULT_TAXPAYER_INVOICE_PATH)
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)

    # Full log index: by bare, and flat named rows for name search
    log_rows: list[dict] = []
    by_bare: dict[str, list[dict]] = defaultdict(list)
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
            "band": "low" if i < 670 else "high",
        }
        log_rows.append(rec)
        by_bare[bare].append(rec)
    wb.close()

    results: list[dict] = []
    action_counts: dict[str, int] = defaultdict(int)

    for c in inv.collisions:
        bare = str(c.get("bare_log") or "")
        claimants_raw = c.get("claimants") or []
        claimants = []
        for cl in claimants_raw:
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

        claimant_outcomes = []
        for cl in claimants:
            # Exact person_key hits anywhere in Log
            exact = [r for r in log_rows if r["person_key"] == cl["person_key"] and cl["person_key"] not in ("", "|")]
            # Token overlap (surname+given) — require last token present
            last_tok = _tokens(cl["last"])
            fuzzy = []
            if not exact and last_tok:
                for r in log_rows:
                    if not last_tok.issubset(r["tokens"]):
                        continue
                    # also require at least one first-name token if present
                    first_tok = _tokens(cl["first"])
                    if first_tok and not (first_tok & r["tokens"]):
                        continue
                    fuzzy.append(r)
            hits = exact or fuzzy
            # Dedupe by bare_log keeping lowest source_row
            by_hit_bare: dict[str, dict] = {}
            for h in sorted(hits, key=lambda x: x["source_row"]):
                by_hit_bare.setdefault(h["bare_log"], h)
            hit_list = list(by_hit_bare.values())

            same_bare = [h for h in hit_list if h["bare_log"] == bare]
            other_bare = [h for h in hit_list if h["bare_log"] != bare]

            if other_bare:
                # Prefer YR=25 / non-PRIOR HOLD if available
                preferred = sorted(
                    other_bare,
                    key=lambda h: (
                        0 if str(h["yr"]) == "25" else 1,
                        0 if h["status"] != "PRIOR HOLD" else 1,
                        h["source_row"],
                    ),
                )[0]
                action = "CORRECT_DRAKE_TO_EXISTING_LOG"
                action_detail = (
                    f"Claimant already on Log bare `{preferred['bare_log']}` "
                    f"(row {preferred['source_row']}, {preferred['name']}, "
                    f"yr={preferred['yr']}, status={preferred['status'] or '(blank)'}). "
                    f"Correct Drake invoice away from collision bare `{bare}`."
                )
            elif same_bare and not other_bare:
                action = "KEEPER_CANDIDATE"
                action_detail = (
                    f"Found on Log only under collision bare `{bare}` "
                    f"(rows {[h['source_row'] for h in same_bare]}). "
                    "Likely keeper if office confirms."
                )
            elif not hit_list:
                action = "MINT_NEW_LOG"
                action_detail = (
                    f"No Log entry under any bare for `{cl['name']}`. "
                    "Mint a new log only after keeper chosen for this collision."
                )
            else:
                action = "REVIEW"
                action_detail = "Unexpected hit pattern."

            action_counts[action] += 1
            claimant_outcomes.append(
                {
                    "claimant": cl["name"],
                    "action": action,
                    "action_detail": action_detail,
                    "exact_hits": len(exact),
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
                "raw_invoices": raws,
                "drake_claimants": [c["name"] for c in claimants],
                "log_rows_on_bare": [
                    {
                        "source_row": r["source_row"],
                        "name": r["name"],
                        "yr": r["yr"],
                        "status": r["status"],
                        "band": r["band"],
                    }
                    for r in by_bare.get(bare, [])
                ],
                "claimant_outcomes": claimant_outcomes,
            }
        )

    # Summary: of claimants, how many already hold a different log?
    n_claimants = sum(len(r["claimant_outcomes"]) for r in results)
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tax_log_path": str(log_path),
        "named_log_rows_searched": len(log_rows),
        "drake_collision_keys": len(results),
        "n_claimants": n_claimants,
        "action_counts": dict(action_counts),
        "note": (
            "Invert W1: search full Log first. CORRECT_DRAKE_TO_EXISTING_LOG beats mint. "
            "Bare 141 remains first office fix for I7 regardless."
        ),
        "collisions": results,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines: list[str] = []
    A = lines.append
    A("# W1 — full Tax Log claimant search")
    A("")
    A(f"_Generated: {payload['generated_at']} · read-only · searched {len(log_rows)} named Log rows_")
    A("")
    A("## Rule (inverted)")
    A("")
    A("1. Search the **full** Tax Log for each Drake collision claimant.")
    A("2. If they already hold a **different** log → correct Drake invoice to that log.")
    A("3. Mint a new log **only** when the claimant has no Log entry.")
    A("4. Bare `141` stays the first office fix for I7.")
    A("")
    A("## Headline")
    A("")
    A(f"| Action | Claimants |")
    A(f"|---|---:|")
    for k, v in sorted(action_counts.items()):
        A(f"| `{k}` | {v} |")
    A(f"| **Total claimants** | **{n_claimants}** |")
    A("")
    A("## Per collision")
    A("")
    for r in sorted(results, key=lambda x: int(x["bare_log"] or 0)):
        A(f"### Bare `{r['bare_log']}` (`{r['classification']}`)")
        A("")
        A(f"- Drake: {'; '.join(r['drake_claimants'])}")
        A(
            "- Log on this bare: "
            + (
                "; ".join(
                    f"{x['name']} (row {x['source_row']}, yr={x['yr']}, {x['status'] or '—'}, {x['band']})"
                    for x in r["log_rows_on_bare"]
                )
                or "—"
            )
        )
        A("")
        A("| Claimant | Action | Detail |")
        A("|---|---|---|")
        for o in r["claimant_outcomes"]:
            A(f"| {o['claimant']} | `{o['action']}` | {o['action_detail']} |")
        A("")
    A(f"Machine-readable: `{OUT_JSON}`")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_MD}")
    print(f"Wrote {OUT_JSON}")
    print("action_counts", dict(action_counts))
    print("n_claimants", n_claimants)


if __name__ == "__main__":
    main()
