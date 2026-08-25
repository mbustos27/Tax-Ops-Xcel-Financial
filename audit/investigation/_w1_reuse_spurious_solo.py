"""Tax Log genuine_reuse: drop spurious co-occupants who already have a solo bare.

For each genuine_reuse bare, if a name also appears on any non-repeat bare,
that appearance on the repeat bare is spurious. Drop it; if one holder remains,
the bare resolves.

Read-only. No TaxOps / Log writes.
"""
from __future__ import annotations

import json
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
    classify_log_repeat,
)

OUT_JSON = Path(r"T:\audit\investigation\W1-reuse-spurious-solo.json")
OUT_MD = Path(r"T:\audit\investigation\W1-reuse-spurious-solo.md")


def main() -> None:
    import openpyxl

    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.invoice_export import bare_log_number

    mem = load_baseline_memory()
    tax_year = 2025
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)

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
        if not logn or not (last or first):
            continue
        bare = bare_log_number(logn, tax_year)
        if not bare:
            continue
        pk = _norm_person(last, first)
        log_by_bare[bare].append(
            {
                "source_row": i,
                "log_raw": logn,
                "last": last,
                "first": first,
                "name": _display(last, first),
                "person_key": pk,
            }
        )
    wb.close()

    # Distinct-person count per bare (repeat = >1 distinct person)
    people_by_bare: dict[str, set[str]] = {
        b: {r["person_key"] for r in rows if r["person_key"] and r["person_key"] != "|"}
        for b, rows in log_by_bare.items()
    }
    repeat_bares = {b for b, people in people_by_bare.items() if len(people) > 1}
    solo_bares = {b for b, people in people_by_bare.items() if len(people) == 1}

    # person_key -> solo bares where they are the sole holder
    solo_homes: dict[str, list[dict]] = defaultdict(list)
    for bare in solo_bares:
        rows = log_by_bare[bare]
        pk = next(iter(people_by_bare[bare]))
        solo_homes[pk].append(
            {
                "bare_log": bare,
                "source_row": rows[0]["source_row"],
                "name": rows[0]["name"],
            }
        )

    # genuine_reuse universe (same classifier as W1)
    reuse_cases: list[dict] = []
    for bare in sorted(repeat_bares, key=lambda x: int(x) if x.isdigit() else 0):
        rows = log_by_bare[bare]
        cls = classify_log_repeat(rows)
        if cls["kind"] != "genuine_reuse":
            continue
        # unique people on this bare (keep one representative row each)
        by_person: dict[str, dict] = {}
        for r in rows:
            pk = r["person_key"]
            if not pk or pk == "|":
                continue
            if pk not in by_person or int(r["source_row"]) < int(by_person[pk]["source_row"]):
                by_person[pk] = r

        occupants = []
        spurious = []
        keepers = []
        for pk, r in sorted(by_person.items(), key=lambda x: x[1]["name"]):
            homes = solo_homes.get(pk) or []
            # exclude current bare if somehow listed (it isn't — it's a repeat)
            homes = [h for h in homes if h["bare_log"] != bare]
            entry = {
                "person_key": pk,
                "name": r["name"],
                "source_row": r["source_row"],
                "solo_homes": homes,
                "spurious": bool(homes),
            }
            occupants.append(entry)
            if homes:
                spurious.append(entry)
            else:
                keepers.append(entry)

        if len(keepers) == 1 and spurious:
            outcome = "RESOLVE_SINGLE"
        elif len(keepers) == 0 and spurious:
            outcome = "ALL_SPURIOUS"  # every name has a solo home elsewhere
        elif len(keepers) >= 2:
            outcome = "STILL_COLLISION"  # 2+ names with no solo home
        elif len(keepers) == 1 and not spurious:
            outcome = "STILL_COLLISION"  # only one distinct? shouldn't happen
        else:
            outcome = "STILL_COLLISION"

        holder = keepers[0] if outcome == "RESOLVE_SINGLE" else None
        reuse_cases.append(
            {
                "bare_log": bare,
                "n_rows": len(rows),
                "n_people": len(by_person),
                "occupants": occupants,
                "n_spurious": len(spurious),
                "n_keepers": len(keepers),
                "outcome": outcome,
                "resolved_holder": (
                    {
                        "name": holder["name"],
                        "person_key": holder["person_key"],
                        "source_row": holder["source_row"],
                    }
                    if holder
                    else None
                ),
                "drop": [
                    {
                        "name": s["name"],
                        "source_row": s["source_row"],
                        "keep_on_bares": [h["bare_log"] for h in s["solo_homes"]],
                    }
                    for s in spurious
                ],
            }
        )

    outcomes = Counter(c["outcome"] for c in reuse_cases)
    pairish = sum(1 for c in reuse_cases if c["n_people"] == 2)

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "log_path": str(log_path),
        "n_named_bares": len(log_by_bare),
        "n_solo_bares": len(solo_bares),
        "n_repeat_bares": len(repeat_bares),
        "n_genuine_reuse": len(reuse_cases),
        "n_two_person_reuse": pairish,
        "outcomes": dict(outcomes),
        "cases": reuse_cases,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# W1 — genuine_reuse spurious co-occupants (solo-bare test)",
        "",
        f"_Generated: {payload['generated_at']} · read-only_",
        "",
        "Rule: if a name on a `genuine_reuse` bare also appears as the **sole** name on "
        "any other bare, its repeat-bare row is spurious — drop it. When one keeper remains, "
        "the bare resolves.",
        "",
        "| Metric | n |",
        "|---|---:|",
        f"| genuine_reuse bares | {payload['n_genuine_reuse']} |",
        f"| of which 2-person pairs | {pairish} |",
        f"| `RESOLVE_SINGLE` | {outcomes.get('RESOLVE_SINGLE', 0)} |",
        f"| `ALL_SPURIOUS` | {outcomes.get('ALL_SPURIOUS', 0)} |",
        f"| `STILL_COLLISION` | {outcomes.get('STILL_COLLISION', 0)} |",
        "",
    ]

    resolve = [c for c in reuse_cases if c["outcome"] == "RESOLVE_SINGLE"]
    lines += [
        f"## Resolves to single holder ({len(resolve)})",
        "",
        "| Bare | Keep | Drop (solo home) |",
        "|---|---|---|",
    ]
    for c in sorted(resolve, key=lambda x: int(x["bare_log"]) if x["bare_log"].isdigit() else 0):
        drops = "; ".join(
            f"{d['name']} -> {','.join('`'+b+'`' for b in d['keep_on_bares'])}"
            for d in c["drop"]
        )
        h = c["resolved_holder"] or {}
        lines.append(f"| `{c['bare_log']}` | {h.get('name')} | {drops} |")

    all_spur = [c for c in reuse_cases if c["outcome"] == "ALL_SPURIOUS"]
    lines += [
        "",
        f"## All occupants have solo homes ({len(all_spur)})",
        "",
        "Every name on the bare also owns a non-repeat bare — bare has no residual keeper "
        "under this rule (office: pick canonical or clear).",
        "",
    ]
    if all_spur:
        lines += ["| Bare | Occupants → solo homes |", "|---|---|"]
        for c in all_spur[:40]:
            bits = "; ".join(
                f"{o['name']} -> {','.join('`'+h['bare_log']+'`' for h in o['solo_homes'])}"
                for o in c["occupants"]
            )
            lines.append(f"| `{c['bare_log']}` | {bits} |")
        if len(all_spur) > 40:
            lines.append(f"| … | +{len(all_spur)-40} more |")

    still = [c for c in reuse_cases if c["outcome"] == "STILL_COLLISION"]
    # split: partial spurious vs none
    partial = [c for c in still if c["n_spurious"] > 0]
    none = [c for c in still if c["n_spurious"] == 0]
    lines += [
        "",
        f"## Still collision ({len(still)})",
        "",
        f"- Partial drop (spurious removed but ≥2 keepers left): **{len(partial)}**",
        f"- No solo home for any occupant: **{len(none)}**",
        "",
    ]
    if partial:
        lines += ["### Partial", "", "| Bare | Keepers left | Dropped |", "|---|---|---|"]
        for c in partial[:30]:
            keep = ", ".join(o["name"] for o in c["occupants"] if not o["spurious"])
            drop = ", ".join(d["name"] for d in c["drop"])
            lines.append(f"| `{c['bare_log']}` | {keep} | {drop} |")
    lines += [
        "",
        f"### No solo home (sample of {min(25, len(none))}/{len(none)})",
        "",
        "| Bare | Names |",
        "|---|---|",
    ]
    for c in none[:25]:
        names = "; ".join(o["name"] for o in c["occupants"])
        lines.append(f"| `{c['bare_log']}` | {names} |")

    lines += ["", f"Machine: `{OUT_JSON}`", ""]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(
        "reuse",
        payload["n_genuine_reuse"],
        "pairs2",
        pairish,
        "outcomes",
        dict(outcomes),
    )


if __name__ == "__main__":
    main()
