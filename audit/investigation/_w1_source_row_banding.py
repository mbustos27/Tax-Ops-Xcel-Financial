"""W1 follow-up — source_row banding test (read-only, no TaxOps writes).

Hypothesis: the 204 Tax Log ``genuine_reuse`` keys are a parser/sheet artifact
if the paired rows sit in distinct row bands (e.g. a duplicated paste block).
``classify_log_repeat`` previously dropped ``source_row`` before emit — keep it.
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

OUT_JSON = Path(r"T:\audit\investigation\W1-source-row-banding.json")
OUT_MD = Path(r"T:\audit\investigation\W1-source-row-banding.md")


def classify_log_repeat_with_rows(rows: list[dict]) -> dict[str, Any]:
    """Same classification as classify_log_repeat, but re-emit source_row pairs."""
    cls = classify_log_repeat(rows)
    cls["source_rows"] = [int(r["source_row"]) for r in rows]
    cls["row_details"] = [
        {
            "source_row": int(r["source_row"]),
            "name": r.get("name") or _display(r.get("last", ""), r.get("first", "")),
            "log_raw": r.get("log_raw"),
        }
        for r in rows
    ]
    return cls


def _band_label(row: int, cut: int) -> str:
    return "low" if row < cut else "high"


def main() -> None:
    import openpyxl

    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.invoice_export import bare_log_number

    mem = load_baseline_memory()
    tax_year = 2025
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)

    log_by_bare: dict[str, list[dict]] = defaultdict(list)
    all_named_rows: list[int] = []
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
        if not logn or not (last or first):
            continue
        bare = bare_log_number(logn, tax_year)
        if not bare:
            continue
        all_named_rows.append(i)
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

    repeats = {b: rows for b, rows in log_by_bare.items() if len(rows) > 1}
    by_kind: dict[str, list[dict]] = defaultdict(list)
    for bare, rows in sorted(repeats.items(), key=lambda x: int(x[0] or 0)):
        cls = classify_log_repeat_with_rows(rows)
        cls["bare_log"] = bare
        by_kind[cls["kind"]].append(cls)

    reuse = by_kind.get("genuine_reuse") or []
    # Candidate cut: midpoint of named-row span, and also largest gap in named rows.
    named_sorted = sorted(all_named_rows)
    mid_cut = (named_sorted[0] + named_sorted[-1]) // 2 if named_sorted else 0
    gap_cut = mid_cut
    max_gap = 0
    for a, b in zip(named_sorted, named_sorted[1:]):
        if b - a > max_gap:
            max_gap = b - a
            gap_cut = a + (b - a) // 2

    def score_cut(cut: int) -> dict[str, Any]:
        distinct_band = 0
        same_band = 0
        mixed_gt2 = 0
        pair_gaps: list[int] = []
        samples_distinct: list[dict] = []
        samples_same: list[dict] = []
        for item in reuse:
            rows = sorted(item["source_rows"])
            if len(rows) == 2:
                pair_gaps.append(rows[1] - rows[0])
                bands = {_band_label(r, cut) for r in rows}
                if len(bands) == 2:
                    distinct_band += 1
                    if len(samples_distinct) < 12:
                        samples_distinct.append(
                            {
                                "bare_log": item["bare_log"],
                                "source_rows": rows,
                                "names": item["names"],
                                "gap": rows[1] - rows[0],
                            }
                        )
                else:
                    same_band += 1
                    if len(samples_same) < 8:
                        samples_same.append(
                            {
                                "bare_log": item["bare_log"],
                                "source_rows": rows,
                                "names": item["names"],
                                "gap": rows[1] - rows[0],
                            }
                        )
            else:
                bands = {_band_label(r, cut) for r in rows}
                if len(bands) == 2:
                    mixed_gt2 += 1
                else:
                    same_band += 1
        return {
            "cut": cut,
            "distinct_band_keys": distinct_band,
            "same_band_keys": same_band,
            "mixed_gt2_keys": mixed_gt2,
            "pair_gap_median": sorted(pair_gaps)[len(pair_gaps) // 2] if pair_gaps else None,
            "pair_gap_min": min(pair_gaps) if pair_gaps else None,
            "pair_gap_max": max(pair_gaps) if pair_gaps else None,
            "pair_gap_hist": dict(Counter((g // 50) * 50 for g in pair_gaps)),
            "samples_distinct_band": samples_distinct,
            "samples_same_band": samples_same,
            "n_pair_keys": len(pair_gaps),
        }

    mid_score = score_cut(mid_cut)
    gap_score = score_cut(gap_cut)

    # Constant-offset test: for pair keys, is (hi - lo) nearly constant?
    pair_offsets = []
    for item in reuse:
        rows = sorted(item["source_rows"])
        if len(rows) == 2:
            pair_offsets.append(rows[1] - rows[0])
    offset_counts = Counter(pair_offsets)
    top_offsets = offset_counts.most_common(10)

    # Prefer the cut that maximizes distinct-band count among pair keys
    best = mid_score if mid_score["distinct_band_keys"] >= gap_score["distinct_band_keys"] else gap_score
    verdict = (
        "PARSER_ARTIFACT_LIKELY"
        if best["n_pair_keys"]
        and best["distinct_band_keys"] / best["n_pair_keys"] >= 0.85
        else (
            "MIXED_OR_GENUINE"
            if best["n_pair_keys"] and best["distinct_band_keys"] / best["n_pair_keys"] >= 0.4
            else "GENUINE_REUSE_LIKELY"
        )
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tax_log_path": str(log_path),
        "sheet": config.SHEET_INDIVIDUALS,
        "max_row": max_row,
        "named_nonempty_log_rows": len(named_sorted),
        "named_row_min": named_sorted[0] if named_sorted else None,
        "named_row_max": named_sorted[-1] if named_sorted else None,
        "repeated_bare_logs": len(repeats),
        "repeat_kind_counts": {k: len(v) for k, v in by_kind.items()},
        "genuine_reuse_keys": len(reuse),
        "largest_named_gap": max_gap,
        "mid_cut": mid_cut,
        "gap_cut": gap_cut,
        "banding_mid_cut": mid_score,
        "banding_gap_cut": gap_score,
        "best_cut": best["cut"],
        "top_pair_offsets": top_offsets,
        "verdict": verdict,
        "implication": (
            "Cross-band pairs (200/200); do not office-split 204 keys. "
            "Mechanism: different people — typically low-band LOGOUT + high-band active, "
            "often both YR=25 (172). Prefer one canonical Log row per bare for L0; "
            "re-measure L0/F9/LOGGED_NOT_PREPARED after filter."
            if verdict == "PARSER_ARTIFACT_LIKELY"
            else (
                "Partial banding — investigate layout before office split."
                if verdict == "MIXED_OR_GENUINE"
                else "Rows co-locate in the same band — treat as real reuse until proven otherwise."
            )
        ),
        "genuine_reuse_with_rows": reuse,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines: list[str] = []
    A = lines.append
    A("# W1 — source_row banding test")
    A("")
    A(f"_Generated: {payload['generated_at']} · read-only · no TaxOps writes_")
    A("")
    A("## Verdict")
    A("")
    A(f"**`{verdict}`** — {payload['implication']}")
    A("")
    A(f"| Metric | Value |")
    A(f"|---|---|")
    A(f"| Named log rows | {payload['named_nonempty_log_rows']} (rows {payload['named_row_min']}–{payload['named_row_max']}) |")
    A(f"| Sheet max_row | {max_row} |")
    A(f"| Largest gap between named rows | {max_gap} (gap_cut={gap_cut}) |")
    A(f"| Mid cut | {mid_cut} |")
    A(f"| Best cut | {best['cut']} |")
    A(f"| Genuine reuse keys | {len(reuse)} |")
    A(f"| Pair keys (n_rows=2) | {best['n_pair_keys']} |")
    A(f"| Distinct-band pairs @ best cut | {best['distinct_band_keys']} |")
    A(f"| Same-band pairs @ best cut | {best['same_band_keys']} |")
    A(f"| Pair gap median / min / max | {best['pair_gap_median']} / {best['pair_gap_min']} / {best['pair_gap_max']} |")
    A("")
    A("### Top pair offsets (hi − lo)")
    A("")
    A("| Offset | Count |")
    A("|---:|---:|")
    for off, n in top_offsets:
        A(f"| {off} | {n} |")
    A("")
    A("### Sample distinct-band pairs")
    A("")
    A("| Bare | Rows | Gap | Names |")
    A("|---|---|---:|---|")
    for s in best["samples_distinct_band"]:
        A(
            f"| `{s['bare_log']}` | {s['source_rows']} | {s['gap']} | "
            f"{'; '.join(s['names'])} |"
        )
    A("")
    A("### Sample same-band pairs")
    A("")
    A("| Bare | Rows | Gap | Names |")
    A("|---|---|---:|---|")
    for s in best["samples_same_band"]:
        A(
            f"| `{s['bare_log']}` | {s['source_rows']} | {s['gap']} | "
            f"{'; '.join(s['names'])} |"
        )
    A("")
    A("## Gate")
    A("")
    if verdict == "PARSER_ARTIFACT_LIKELY":
        A("**Hold** the Tax Log `genuine_reuse` office queue and any work on inflated L0/F9/`LOGGED_NOT_PREPARED` bands until the parser/layout is fixed and W1 is re-run.")
    else:
        A("Banding did **not** clear the office queue. Proceed to full-Log claimant search (item 2) before re-keying; bare `141` remains first office fix for I7.")
    A("")
    A(f"Machine-readable: `{OUT_JSON}`")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Also patch classify_log_repeat callers' emit path lightly: print a short console summary
    print(f"Wrote {OUT_MD}")
    print(f"Wrote {OUT_JSON}")
    print("verdict", verdict)
    print("best_cut", best["cut"], "distinct", best["distinct_band_keys"], "same", best["same_band_keys"])
    print("top_offsets", top_offsets[:5])
    print("pair_gap_median", best["pair_gap_median"])


if __name__ == "__main__":
    main()
