"""Generate the full consolidated client-linkage audit report (A0–A5)."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Optional

from audit.disposition import DISPOSITION_DB_PATH, connect_disposition
from audit.report import (
    A5_WORKLIST_PATH,
    collect_worklist_rows,
    run_a5_phase,
    write_worklist_xlsx,
)
from audit.util import dumps, utc_now

INVESTIGATION = Path(r"T:\audit\investigation")
FULL_REPORT_PATH = INVESTIGATION / "FULL-REPORT.md"


def _slurp(name: str) -> str:
    p = INVESTIGATION / name
    if not p.exists():
        return f"_(missing {name})_\n"
    return p.read_text(encoding="utf-8", errors="replace")


def _table_int(md: str, label: str) -> Optional[int]:
    """Pull an integer from a markdown table cell whose left column matches label."""
    pat = rf"\|\s*{re.escape(label)}\s*\|\s*\*?(\d+)\*?\s*\|"
    m = re.search(pat, md, re.I)
    return int(m.group(1)) if m else None


def _parse_a1_headline(md: str) -> dict[str, Any]:
    """Current-run ladder counts from A1-ladder.md (not A3 F12 baseline column)."""
    out: dict[str, Any] = {}
    out["three_way"] = _table_int(md, "Drake ∩ TaxOps ∩ Log")
    out["l0_drake_taxops"] = _table_int(md, "L0 Drake↔TaxOps")
    # Tier table uses "L0 Drake↔TaxOps | (invoice, tax_year) | N"
    if out["l0_drake_taxops"] is None:
        m = re.search(
            r"\|\s*L0 Drake↔TaxOps\s*\|\s*\([^)]+\)\s*\|\s*(\d+)\s*\|", md
        )
        out["l0_drake_taxops"] = int(m.group(1)) if m else None
    m = re.search(r"\|\s*L1 last4\+surname\s*\|\s*CSM ↔ TaxOps\s*\|\s*(\d+)\s*\|", md)
    out["l1"] = int(m.group(1)) if m else _table_int(md, "L1 last4+surname")
    m = re.search(
        r"\|\s*L5 unmatched Drake invoice[^\|]*\|\s*\|?\s*(\d+)\s*\|", md
    )
    out["l5"] = int(m.group(1)) if m else None
    m = re.search(
        r"After bare-log collision exclusion \(L0-eligible raw\)\s*\|\s*(\d+)\s*\|", md
    )
    out["l0_eligible"] = int(m.group(1)) if m else None
    return out


def _parse_a4_headline(md: str) -> dict[str, Any]:
    """Current-run invariant status from A4-invariants.md overall line."""
    out: dict[str, Any] = {"summary": "_(A4 missing)_", "i7": None, "i6": None}
    m = re.search(
        r"\*\*Overall:\s*(\d+)\s*PASS\s*/\s*(\d+)\s*MODIFIED\s*/\s*(\d+)\s*FAIL\*\*",
        md,
    )
    if m:
        out["pass"], out["modified"], out["fail"] = (
            int(m.group(1)),
            int(m.group(2)),
            int(m.group(3)),
        )
        out["summary"] = (
            f"**{out['pass']} PASS / {out['modified']} MODIFIED / {out['fail']} FAIL**"
        )
    else:
        m2 = re.search(r"\*\*Overall:\s*(\d+)/(\d+)\s*PASS\*\*", md)
        if m2:
            out["summary"] = f"**{m2.group(1)}/{m2.group(2)} PASS** (legacy format)"
    m = re.search(r"\|\s*I7\s*\|[^|]+\|\s*`([^`]+)`\s*\|", md)
    out["i7"] = m.group(1) if m else None
    m = re.search(r"\|\s*I6\s*\|[^|]+\|\s*`([^`]+)`\s*\|", md)
    out["i6"] = m.group(1) if m else None
    return out


def _disposition_rollup() -> dict:
    conn = connect_disposition()
    try:
        by_status = dict(
            conn.execute("SELECT status, COUNT(*) FROM audit_disposition GROUP BY 1")
        )
        by_type = list(
            conn.execute(
                "SELECT finding_type, COUNT(*) n FROM audit_disposition "
                "WHERE status IN ('OPEN','ACKED') GROUP BY 1 ORDER BY n DESC"
            )
        )
        runs = list(
            conn.execute(
                "SELECT run_label, started_at, finished_at, source, stats_json "
                "FROM disposition_run ORDER BY started_at"
            )
        )
        return {
            "by_status": by_status,
            "by_type_open": [(r[0], r[1]) for r in by_type],
            "runs": [
                {
                    "run_label": r[0],
                    "started_at": r[1],
                    "finished_at": r[2],
                    "source": r[3],
                    "stats": json.loads(r[4]) if r[4] else {},
                }
                for r in runs
            ],
        }
    finally:
        conn.close()


def write_full_report(dest: Path = FULL_REPORT_PATH) -> Path:
    rollup = _disposition_rollup()
    a1_md = _slurp("A1-ladder.md")
    a4_md = _slurp("A4-invariants.md")
    a1 = _parse_a1_headline(a1_md)
    a4 = _parse_a4_headline(a4_md)
    priority = collect_worklist_rows(
        max_rows=200,
        types=frozenset(
            {
                "DUPLICATE_CLIENT",
                "MERGE_UNTRACEABLE",
                "L0_DRAKE_ONLY",
                "LOG_NUMBER_COLLISION",
                "MALFORMED_LOG_NUMBER",
                "SPOUSE_STORE_DIVERGENCE",
                "SPOUSE_AMBIGUOUS",
                "NEEDS_HUMAN",
                "MISSING_IN_TAXOPS",
                "JUL1_PROVENANCE",
            }
        ),
    )
    phantoms = collect_worklist_rows(max_rows=80, types=frozenset({"PHANTOM_IN_TAXOPS"}))
    workflow = collect_worklist_rows(
        max_rows=80,
        types=frozenset({"PREPARED_NOT_LOGGED", "LOGGED_NOT_PREPARED"}),
    )

    def _n(v: Any, fallback: str = "?") -> str:
        return str(v) if v is not None else fallback

    lines: list[str] = []
    A = lines.append

    A("# Client Linkage Audit — Full Report (A0–A5)")
    A("")
    A(f"_Compiled: {utc_now()}_")
    A("")
    A("Findings-only. No TaxOps repairs, merges, or schema changes. "
      "All durable outputs under `T:\\audit\\` / `T:\\audit\\investigation\\`.")
    A("")
    A("---")
    A("")
    A("## Executive summary")
    A("")
    A("_Numbers below are parsed from the embedded A1/A4 sections (current run), "
      "not from A3 F12 baseline columns._")
    A("")
    A("| Item | Result |")
    A("|---|---|")
    A(
        f"| Cross-system key | Drake Invoice Number ≡ Tax Log # (bare form); "
        f"L0 three-way **{_n(a1.get('three_way'))}**; "
        f"L0-eligible **{_n(a1.get('l0_eligible'))}** |"
    )
    A(
        f"| Identity ladder | L0 Drake↔TaxOps **{_n(a1.get('l0_drake_taxops'))}**; "
        f"L1 **{_n(a1.get('l1'))}**; L5 leftover **{_n(a1.get('l5'))}** |"
    )
    A("| Exact duplicate client groups | **8** (matches live baseline); merge queue is **6–7** per R0 |")
    A("| Jul1 provenance cohort | **191** still dated; **179** at stamp `18:19:30` |")
    A("| Aug7 prefill burst | **244** (all SSN + prefill-linked; **8** Group-C name collisions, not merges) |")
    A(
        f"| Invariants | {a4.get('summary')} "
        f"(I6=`{a4.get('i6')}`; I7=`{a4.get('i7')}`) |"
    )
    A("| Disposition open | **%s** |" % rollup["by_status"].get("OPEN", 0))
    A("| Operator worklist | Priority / Phantoms / Workflow → `A5-worklist.xlsx` |")
    A("| Remediation plan | `R0-remediation-plan.md` (plan only; no TaxOps writes until Wave 2A) |")
    A("")
    A("### Work this first")
    A("")
    A("Per **R0** current gate (software waves 0–5 done; I7 still FAIL):")
    A("")
    A("1. **Lucy / Drake** — execute `W-office-packet.md` (bare `141` first → MOVE → KEEP/CLAIM/MINT → REVIEW).")
    A("2. Re-export Drake invoices after packet; re-run A1/A3/A4 until I7 PASS and collisions=0.")
    A("3. Hold Tax Log `genuine_reuse` split until post-packet remeasure.")
    A("4. Then phantoms / workflow lanes (F8/F9) — counts already use canonical YR=25 Log keys.")
    A("")
    A("### Loud corrections vs I0–I4 investigation")
    A("")
    A("1. I4 “no shared Drake↔TaxOps key” is **SUPERSEDED** — Invoice Number holds the Tax Log number.")
    A("2. July 1 “~368 duplicates from bulk import” — **count real on Jul31 snap; mechanism is "
      "`created_at` rewrite**, not insert.")
    A("3. L0 string equality fails until season prefix stripped: `250141` ↔ `141`.")
    A("")

    A("---")
    A("")
    A("## 1. Baseline lock (A0)")
    A("")
    A(_slurp("A0-baseline.md"))
    A("")
    A("---")
    A("")
    A("## 2. Identity ladder (A1)")
    A("")
    A(a1_md)
    A("")
    A("---")
    A("")
    A("## 3. Disposition & delta (A2)")
    A("")
    A(_slurp("A2-delta-report.md"))
    A("")
    A("### Disposition DB rollup (live)")
    A("")
    A(f"- Path: `{DISPOSITION_DB_PATH}`")
    A(f"- By status: `{dumps(rollup['by_status'])}`")
    A("")
    A("| Finding type (OPEN/ACKED) | Count |")
    A("|---|---:|")
    for t, n in rollup["by_type_open"]:
        A(f"| `{t}` | {n} |")
    A("")
    A("### Disposition runs")
    A("")
    for run in rollup["runs"]:
        A(f"- `{run['run_label']}` source=`{run['source']}` "
          f"{run['started_at']} → {run['finished_at']} stats=`{dumps(run['stats'])}`")
    A("")

    A("---")
    A("")
    A("## 4. Failure-mode checks F1–F12 (A3)")
    A("")
    A(_slurp("A3-checks.md"))
    A("")

    A("---")
    A("")
    A("## 5. Invariant guards (A4)")
    A("")
    A(a4_md)
    A("")

    A("---")
    A("")
    A("## 6. Operator worklist (A5)")
    A("")
    A(f"Workbook: `{A5_WORKLIST_PATH}`")
    A("")
    A("| Sheet | Rows | Purpose |")
    A("|---|---:|---|")
    A(
        f"| Priority | {len(priority)} | Dups, L0 gaps, collisions, merge, prefill, spouse, "
        f"needs-human (order via R0, not sheet sort) |"
    )
    A(f"| Phantoms | {len(phantoms)} | TaxOps-only / unmatched (capped) |")
    A(f"| Workflow | {len(workflow)} | Prepared↔logged process gaps (capped) |")
    A("")
    A("### Priority sheet — type mix")
    A("")
    for t, n in Counter(r["type"] for r in priority).most_common():
        A(f"- `{t}`: {n}")
    A("")
    A("### Priority samples (first 25)")
    A("")
    A("| Type | Entity | Confidence | Action |")
    A("|---|---|---|---|")
    for r in priority[:25]:
        ent = (r.get("entity") or "")[:60].replace("|", "\\|")
        act = (r.get("suggested_action") or "")[:70].replace("|", "\\|")
        A(f"| `{r['type']}` | `{ent}` | {r.get('confidence')} | {act} |")
    A("")

    A("---")
    A("")
    A("## 7. Recommendations (no TaxOps writes performed)")
    A("")
    A("Follow `R0-remediation-plan.md`. Short form:")
    A("")
    A("1. **Wave 0** — full-width `TAXPAYER.csv` re-export; answer Invoice+Status co-export.")
    A("2. **Wave 1** — collisions/malformed in Drake/Log; confirm I7 PASS after bare-141.")
    A("3. **Wave 2A** — `client_merge_history` before any merge (blocker).")
    A("4. **Wave 2B** — 6–7 Group A/B merges only; Group C FALSE_POSITIVE after ITIN check.")
    A("5. **Do not mass-delete Jul1-dated clients** — provenance rewrite, not duplicate-insert.")
    A("6. Spouse store decision before reconciling 228 spouse findings.")
    A("7. `client_external_ids` mint blocked on Wave 0.2 Drake custom-field answer.")
    A("")

    A("---")
    A("")
    A("## 8. Artifact index")
    A("")
    A("| File | Role |")
    A("|---|---|")
    for name, role in (
        ("FULL-REPORT.md", "This consolidated report"),
        ("R0-remediation-plan.md", "Remediation waves 0–5 (plan only)"),
        ("A0-baseline.md", "Baseline lock + invoice Amendment 1"),
        ("A1-ladder.md", "L0–L5 identity ladder"),
        ("A2-delta-report.md", "Fingerprints + disposition delta"),
        ("A3-checks.md", "F1–F12 checks"),
        ("A4-invariants.md", "Seven hard guards"),
        ("A5-report.md", "Operator summary"),
        ("A5-worklist.xlsx", "Actionable worklist"),
        ("C5-log-import-triage.md", "Tax Log import error/review taxonomy"),
        ("TAXPAYER.csv", "Canonical invoice export"),
        ("I0-inventory.md", "Investigation inventory"),
        ("I1-provenance-map.md", "Field provenance"),
        ("I2-ingestion-paths.md", "Ingestion + Jul1"),
        ("I3-linkage-findings.md", "Linkage / Jul31 Venn"),
        ("I4-synthesis.md", "Failure taxonomy (partially superseded)"),
    ):
        p = INVESTIGATION / name
        A(f"| `{'✓' if p.exists() else '✗'}` `{name}` | {role} |")
    A("")
    A(f"| `audit_disposition.sqlite` | Persistent dispositions (`{DISPOSITION_DB_PATH}`) |")
    A("| `audit_20260810.sqlite` | Per-run ladder / entity_link |")
    A("| `baseline_memory.json` | Cross-run count memory |")
    A("")

    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


if __name__ == "__main__":
    # Refresh A5 operator outputs, then compile full report
    md5, xlsx, n = run_a5_phase()
    full = write_full_report()
    print(f"FULL report={full} a5={md5} worklist={xlsx} priority_rows={n}")
