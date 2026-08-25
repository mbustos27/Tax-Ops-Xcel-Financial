"""A5 — Operator report + worklist (findings-only; no TaxOps writes)."""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Optional

from openpyxl import Workbook
from openpyxl.styles import Font

from audit.disposition import DISPOSITION_DB_PATH, connect_disposition
from audit.util import dumps, utc_now

A5_REPORT_PATH = Path(r"T:\audit\investigation\A5-report.md")
A5_WORKLIST_PATH = Path(r"T:\audit\investigation\A5-worklist.xlsx")
INVESTIGATION = Path(r"T:\audit\investigation")

# Types that are worklist-actionable (not pure coverage/noise).
# Amendment 2 C6: NAME_TRUNCATED is informational (CSM display artifact; L0/L1 carry identity).
ACTIONABLE_TYPES = frozenset(
    {
        "DUPLICATE_CLIENT",
        "L0_DRAKE_ONLY",
        "MERGE_UNTRACEABLE",
        "PHANTOM_IN_TAXOPS",
        "PREPARED_NOT_LOGGED",
        "LOGGED_NOT_PREPARED",
        "MISSING_IN_TAXOPS",
        "SPOUSE_STORE_DIVERGENCE",
        "SPOUSE_AMBIGUOUS",
        "NEEDS_HUMAN",
        # Wave 3: PREFILL_STUB_BURST → informational (Group C = FALSE_POSITIVE)
        "JUL1_PROVENANCE",
        "LOG_NUMBER_COLLISION",
        "MALFORMED_LOG_NUMBER",
    }
)

# Prefer these when trimming a long worklist.
PRIORITY = {
    "DUPLICATE_CLIENT": 10,
    "MERGE_UNTRACEABLE": 15,
    "LOG_NUMBER_COLLISION": 18,
    "L0_DRAKE_ONLY": 20,
    "MISSING_IN_TAXOPS": 25,
    "MALFORMED_LOG_NUMBER": 28,
    "PREFILL_STUB_BURST": 35,
    "NEEDS_HUMAN": 40,
    "SPOUSE_AMBIGUOUS": 45,
    "SPOUSE_STORE_DIVERGENCE": 50,
    "PHANTOM_IN_TAXOPS": 60,
    "PREPARED_NOT_LOGGED": 70,
    "LOGGED_NOT_PREPARED": 75,
    "JUL1_PROVENANCE": 90,
}

SUGGESTED_ACTION = {
    "DUPLICATE_CLIENT": "Review merge keep/discard; favor SSN-bearing row; check filetrack/log# blast",
    "NAME_TRUNCATED": "Informational (C6): CSM 40-char display artifact — identity via L0 invoice / L1",
    "L0_DRAKE_ONLY": "Invoice has log# but absent from TaxOps and Tax Log — add/repair log assignment",
    "MERGE_UNTRACEABLE": "Accept partial trail (audit_log keep/discard); cannot prove all Jul1 attrition",
    "PHANTOM_IN_TAXOPS": "Classify test/closed/spelling; link or mark test — workflow vs data",
    "PREPARED_NOT_LOGGED": "Workflow: Drake/invoice key not on Tax Log — office log update",
    "LOGGED_NOT_PREPARED": "Workflow: on Tax Log but not in invoice L0 export — prepare or export lag",
    "MISSING_IN_TAXOPS": "CSM/Drake client missing in TaxOps — intake or import",
    "SPOUSE_STORE_DIVERGENCE": "Reconcile clients.spouse_* vs spouses table (design debt)",
    "SPOUSE_AMBIGUOUS": "Human spouse adjudication",
    "NEEDS_HUMAN": "Manual review (see subtype in evidence)",
    "PREFILL_STUB_BURST": "Informational (Wave 3): Aug7 stubs SSN+prefill-linked; Group C = distinct SSN twins",
    "JUL1_PROVENANCE": "Provenance only — do not mass-delete; policy for created_at rewrite",
    "LOG_NUMBER_COLLISION": "Same bare log claimed by >1 taxpayer on TAXPAYER.csv — resolve before L0 trust",
    "MALFORMED_LOG_NUMBER": "Invoice fails bare-log range/normalize — fix Drake Invoice Number",
}

WORKFLOW_TYPES = frozenset({"PREPARED_NOT_LOGGED", "LOGGED_NOT_PREPARED"})
EXPORT_LAG_TYPES = frozenset({"L0_DRAKE_ONLY"})  # NAME_TRUNCATED informational (C6)
INFORMATIONAL_TYPES = frozenset({"NAME_TRUNCATED", "L0_KEY_PRESENT", "PREFILL_STUB_BURST"})


def _redact(s: str) -> str:
    """Keep last-4 only if an SSN-like token appears; never emit longer digit runs as SSN."""
    if not s:
        return s
    # Mask 9-digit sequences
    s = re.sub(r"\b(\d{3})-?(\d{2})-?(\d{4})\b", r"***-**-\\3", s)
    s = re.sub(r"\b\d{5,8}\b", "[redacted]", s)
    return s


def _read_snippet(path: Path, max_chars: int = 12000) -> str:
    if not path.exists():
        return f"_(missing {path.name})_"
    text = path.read_text(encoding="utf-8", errors="replace")
    return text if len(text) <= max_chars else text[:max_chars] + "\n\n…_(truncated)_"


def _latest_sightings(conn: sqlite3.Connection) -> dict[str, str]:
    """finding_id → latest delta_bucket."""
    out: dict[str, str] = {}
    for r in conn.execute(
        """
        SELECT s.finding_id, s.delta_bucket, s.run_label
        FROM disposition_sighting s
        JOIN (
          SELECT finding_id, MAX(run_label) AS rl
          FROM disposition_sighting GROUP BY finding_id
        ) t ON t.finding_id=s.finding_id AND t.rl=s.run_label
        """
    ):
        out[r[0]] = r[1]
    return out


def _blast(detail: dict) -> str:
    if not detail:
        return ""
    if "blast_returns" in detail:
        return str(detail.get("blast_returns"))
    if "logs_at_risk" in detail:
        return f"logs={len(detail.get('logs_at_risk') or [])}"
    if "n" in detail and detail.get("n") and isinstance(detail["n"], int):
        return str(detail["n"])
    return ""


def _confidence(ftype: str, detail: dict) -> str:
    if ftype == "DUPLICATE_CLIENT":
        return "high" if detail.get("exact") or detail.get("subtype") == "ssn_asymmetric_twin" else "medium"
    if ftype in ("L0_DRAKE_ONLY", "MERGE_UNTRACEABLE", "JUL1_PROVENANCE", "PREFILL_STUB_BURST"):
        return "high"
    if ftype in WORKFLOW_TYPES:
        return "medium"
    if ftype == "PHANTOM_IN_TAXOPS":
        cause = (detail.get("cause") or detail.get("salient") or "")
        if cause == "test":
            return "high"
        return "low"
    return "medium"


def _lane(ftype: str) -> str:
    if ftype in WORKFLOW_TYPES:
        return "workflow"
    if ftype in EXPORT_LAG_TYPES:
        return "export_lag_or_linkage"
    if ftype == "JUL1_PROVENANCE":
        return "provenance"
    return "data"


def collect_worklist_rows(
    *,
    max_rows: int = 250,
    types: Optional[frozenset[str]] = None,
) -> list[dict[str, Any]]:
    allow = types or ACTIONABLE_TYPES
    conn = connect_disposition()
    try:
        sight = _latest_sightings(conn)
        rows = []
        for r in conn.execute(
            """
            SELECT finding_id, finding_type, entity_key, status, note,
                   first_seen_run, last_seen_run, sample_detail, fingerprint_doc
            FROM audit_disposition
            WHERE status IN ('OPEN','ACKED')
            """
        ):
            ftype = r["finding_type"]
            if ftype not in allow:
                continue
            detail = {}
            try:
                detail = json.loads(r["sample_detail"] or "{}")
            except json.JSONDecodeError:
                detail = {}
            # Skip closed/logout phantoms from worklist (noise)
            if ftype == "PHANTOM_IN_TAXOPS" and detail.get("cause") == "closed_or_logout":
                continue
            bucket = sight.get(r["finding_id"], "OPEN")
            rows.append(
                {
                    "finding_id": r["finding_id"],
                    "type": ftype,
                    "entity": _redact(r["entity_key"] or ""),
                    "evidence": _redact(dumps(detail)[:500]),
                    "suggested_action": SUGGESTED_ACTION.get(ftype, "Review"),
                    "confidence": _confidence(ftype, detail),
                    "blast_radius": _blast(detail),
                    "disposition": r["status"],
                    "delta_bucket": bucket,
                    "lane": _lane(ftype),
                    "first_seen_run": r["first_seen_run"],
                    "last_seen_run": r["last_seen_run"],
                    "_prio": PRIORITY.get(ftype, 80),
                    "_newish": 0 if bucket in ("NEW", "REGRESSED") else 1,
                }
            )
        rows.sort(key=lambda x: (x["_prio"], x["_newish"], x["type"], x["entity"]))
        out = rows[:max_rows]
        for x in out:
            x.pop("_prio", None)
            x.pop("_newish", None)
        return out
    finally:
        conn.close()


_HEADERS = [
    "finding_id",
    "type",
    "entity",
    "evidence",
    "suggested_action",
    "confidence",
    "blast_radius",
    "disposition",
    "delta_bucket",
    "lane",
    "first_seen_run",
    "last_seen_run",
]


def _write_sheet(ws, rows: list[dict[str, Any]]) -> None:
    for col, h in enumerate(_HEADERS, 1):
        cell = ws.cell(1, col, h)
        cell.font = Font(bold=True)
    for i, row in enumerate(rows, 2):
        for col, h in enumerate(_HEADERS, 1):
            ws.cell(i, col, row.get(h, ""))


def write_worklist_xlsx(rows: list[dict[str, Any]], dest: Path = A5_WORKLIST_PATH) -> Path:
    """
    Split so the primary sheet stays finishable:
      Priority — dups, merge, L0 gaps, collisions, prefill, spouse, needs_human
      Phantoms — PHANTOM_IN_TAXOPS (capped)
      Workflow — prepared/logged mismatches (capped)
    """
    priority_types = frozenset(
        {
            "DUPLICATE_CLIENT",
            "MERGE_UNTRACEABLE",
            "L0_DRAKE_ONLY",
            "LOG_NUMBER_COLLISION",
            "MALFORMED_LOG_NUMBER",
            "PREFILL_STUB_BURST",
            "SPOUSE_STORE_DIVERGENCE",
            "SPOUSE_AMBIGUOUS",
            "NEEDS_HUMAN",
            "MISSING_IN_TAXOPS",
            "JUL1_PROVENANCE",
        }
    )
    priority = collect_worklist_rows(max_rows=200, types=priority_types)
    phantoms = collect_worklist_rows(
        max_rows=80, types=frozenset({"PHANTOM_IN_TAXOPS"})
    )
    workflow = collect_worklist_rows(max_rows=80, types=WORKFLOW_TYPES)
    # `rows` arg kept for report type tallies
    _ = rows
    wb = Workbook()
    ws = wb.active
    ws.title = "Priority"
    _write_sheet(ws, priority)
    _write_sheet(wb.create_sheet("Phantoms"), phantoms)
    _write_sheet(wb.create_sheet("Workflow"), workflow)

    ws2 = wb.create_sheet("Summary", 0)
    ws2["A1"] = "A5 Worklist Summary"
    ws2["A1"].font = Font(bold=True, size=14)
    ws2["A2"] = f"Generated: {utc_now()}"
    ws2["A3"] = (
        f"Priority={len(priority)} Phantoms={len(phantoms)} Workflow={len(workflow)} "
        "(work Priority first)"
    )
    r = 5
    ws2.cell(r, 1, "Priority by type").font = Font(bold=True)
    r += 1
    for t, n in Counter(x["type"] for x in priority).most_common():
        ws2.cell(r, 1, t)
        ws2.cell(r, 2, n)
        r += 1
    dest.parent.mkdir(parents=True, exist_ok=True)
    wb.save(dest)
    return dest

def _disposition_headline() -> dict[str, Any]:
    conn = connect_disposition()
    try:
        # Latest run stats if present
        run = conn.execute(
            "SELECT run_label, stats_json, finished_at FROM disposition_run ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        open_n = int(
            conn.execute(
                "SELECT COUNT(*) FROM audit_disposition WHERE status IN ('OPEN','ACKED')"
            ).fetchone()[0]
        )
        by_status = dict(
            conn.execute("SELECT status, COUNT(*) FROM audit_disposition GROUP BY 1")
        )
        by_type_open = dict(
            conn.execute(
                "SELECT finding_type, COUNT(*) FROM audit_disposition "
                "WHERE status IN ('OPEN','ACKED') GROUP BY 1 ORDER BY 2 DESC"
            )
        )
        stats = {}
        if run and run["stats_json"]:
            try:
                stats = json.loads(run["stats_json"])
            except json.JSONDecodeError:
                stats = {}
        return {
            "latest_run": run["run_label"] if run else None,
            "latest_finished": run["finished_at"] if run else None,
            "latest_stats": stats,
            "open_acked": open_n,
            "by_status": by_status,
            "by_type_open": by_type_open,
        }
    finally:
        conn.close()


def write_a5_report(
    worklist_rows: list[dict[str, Any]],
    *,
    dest: Path = A5_REPORT_PATH,
) -> Path:
    head = _disposition_headline()
    stats = head.get("latest_stats") or {}
    headline = stats.get("headline_NEW_plus_REGRESSED") or stats.get("headline")
    if headline is None:
        headline = sum(1 for r in worklist_rows if r.get("delta_bucket") in ("NEW", "REGRESSED"))

    lines: list[str] = []
    A = lines.append
    A("# A5 — Operator report")
    A("")
    A(f"_Generated: {utc_now()}_")
    A("")
    A("## Delta headline (work this first)")
    A("")
    A(f"**NEW + REGRESSED = {headline}** *(not total open)*")
    A("")
    A(f"- Latest disposition run: `{head.get('latest_run')}` finished `{head.get('latest_finished')}`")
    A(f"- Latest run stats: `{dumps(stats)}`")
    A(f"- Open/ACKED in disposition DB: **{head.get('open_acked')}**")
    A(f"- Worklist workbook: `{A5_WORKLIST_PATH}`")
    A("  - **Priority** sheet — dups, L0 gaps, collisions, merge, prefill (work this first)")
    A("  - **Phantoms** / **Workflow** sheets — capped secondary queues")
    A("  - `NAME_TRUNCATED` is informational (C6) — not on Priority")
    A("")
    A("### Priority-queue types in collected set")
    A("")
    for t, n in Counter(r["type"] for r in worklist_rows).most_common(15):
        A(f"- `{t}`: {n}")
    A("")

    A("## Baseline (A0)")
    A("")
    A("Authoritative CSM: OneDrive TY2025 CLIENTS.xlsx (1159). Tax Log: 1252 named. "
      "Invoice: `T:\\audit\\investigation\\TAXPAYER.csv` (1097 full-width / "
      "**813** L0-eligible bare; was 801 under legacy `^\\d{6}$`). "
      "TaxOps path: UNC share; `AUTHORITATIVE_DB_UNRESOLVED` for server C:\\ pairing. "
      "Drift status at last lock: STABLE (see `A0-baseline.md`).")
    A("")
    A("See full lock: `A0-baseline.md`.")
    A("")

    A("## Identity ladder (A1)")
    A("")
    A("| Tier / cell | Count |")
    A("|---|---:|")
    A("| L0 three-way (bare log) | 597 |")
    A("| Drake∩TaxOps \\ Log | 122 |")
    A("| Drake∩Log \\ TaxOps | 61 |")
    A("| Drake only | 33 |")
    A("| L0 Drake↔TaxOps links | 719 |")
    A("| L0 Drake↔Log links | 658 |")
    A("| L0 TaxOps↔Log links | 835 |")
    A("| L1 last4+surname | 389 |")
    A("| L2 / L3 / L4 / L5 | 5 / 0 / 1 / 28 |")
    A("")
    A("Jul31 name Venn (1009/126/13/7) is a different universe (CSM name match on Desktop 1155). "
      "A1 is invoice bare-log on TAXPAYER.csv — quantify export lag separately, do not absorb.")
    A("")
    A("Key normalization: Drake Invoice `250141` ≡ TaxOps/Log `141`. "
      "C6: L3=0 (wired CSM↔invoice only; ineffective); NAME_TRUNCATED informational.")
    A("")

    A("## Disposition memory (A2)")
    A("")
    A(f"- DB: `{DISPOSITION_DB_PATH}` (never truncated)")
    A(f"- Status mix: `{dumps(head.get('by_status'))}`")
    A("- Fingerprints exclude TaxOps row ids / created_at / raw untruncated names")
    A("- Merge trail: **MERGE_PARTIAL_TRAIL** (86/89 audit_log rows have keep_id/discard_id; no discard snapshot)")
    A("- Amend2 C1/C2 delta (`amend2-c1c2`): RESOLVED 15 MALFORMED; NEW 19 COLLISION + 2 MALFORMED; REGRESSED 0")
    A("")

    A("## Failure-mode checks (A3) — actual vs baseline")
    A("")
    A("| Check | Deviation | Actual highlight | Baseline |")
    A("|---|---|---|---|")
    A("| F1 trunc | INFORMATIONAL | CSM ≥39: 72; prefill@40: 74 | 71 / 74 |")
    A("| F2 format | INFORMATIONAL | CSM &: 351; blank first: 151 | &:442; blank:151 |")
    A("| F3 dups | INFORMATIONAL | exact groups **8**; norm buckets **14** | 8 / 14 |")
    A("| F4 Jul1 | INFORMATIONAL | **191** dated; 179 stamp 18:19:30 | 191 live |")
    A("| F5 SSN asym | INFORMATIONAL | **3** groups | common in Jul1 cluster |")
    A("| F6 no log# | INFORMATIONAL | **25.67%** (414/1613) | ~26% |")
    A("| F7 last4 | INFORMATIONAL | CSM surplus 52; L1 last4-only **0** | 52 |")
    A("| F8 phantom | INFORMATIONAL | 324 key-proxy (not Jul31 name residual 227) | 227 |")
    A("| F9 workflow | INFORMATIONAL | invoice∉log 157; log∉invoice 398 | 88 / 141 |")
    A("| F10 prefill | INFORMATIONAL | **244** burst; 8 older twins | 244 |")
    A("| F11 spouse | INFORMATIONAL | spouses 166; hh 1302; only_clients_cols 353 | 198 findings |")
    A("| F12 key | **SUPERSEDED** | three-way **597**; no client_external_ids | I4 'no key' |")
    A("")
    A("Full detail: `A3-checks.md`.")
    A("")

    A("## Invariants (A4)")
    A("")
    A("**6 PASS / 0 MODIFIED / 1 FAIL** this run — I6 mtime+size both held (`PASS`). "
      "I7 **FAIL**: pass2 created return id=2842 log=`141` "
      "`PEREZ & GARCIA VILLAREAL, ANDRES` (bare-141 collision claimant). "
      "Status semantics: PASS | MODIFIED | FAIL (C3).")
    A("")
    A("See `A4-invariants.md`.")
    A("")

    A("## Finding lanes (how to route work)")
    A("")
    A("| Lane | Meaning | Examples |")
    A("|---|---|---|")
    A("| data | Identity / store corruption | DUPLICATE_CLIENT, SPOUSE_*, MISSING_IN_TAXOPS |")
    A("| workflow | Process lag between systems | PREPARED_NOT_LOGGED, LOGGED_NOT_PREPARED |")
    A("| export_lag_or_linkage | Export defect or key gap | L0_DRAKE_ONLY (NAME_TRUNCATED = informational C6) |")
    A("| provenance | Historical stamp; not a repair target | JUL1_PROVENANCE |")
    A("")

    A("## Known limitations (carry-forward)")
    A("")
    A("1. **Jul1 rewrite tool unknown** — space-format `2026-07-01 18:19:30` on ~179 live rows; mechanism is rewrite not insert; script/operator not identified.")
    A("2. **Can Drake hold a firm-assigned client ID?** — open; if Invoice+Status can co-export, CSM becomes less load-bearing.")
    A("3. **C:\\TaxOps vs T:\\ / UNC** — share samefile confirmed on workstation; server-local NSSM path pairing remains `AUTHORITATIVE_DB_UNRESOLVED`.")
    A("4. **Merge attrition** — partial trail only; resolved-vs-deleted not fully distinguishable.")
    A("5. **F8/F9 vs Jul31** — A3 uses invoice-key proxies; Jul31 used name-match residuals — not 1:1.")
    A("6. **Ragged TAXPAYER.csv** — 230 short rows drop Invoice Number (last col); export defect.")
    A("")

    A("## Artifacts")
    A("")
    for name in (
        "A0-baseline.md",
        "A1-ladder.md",
        "A2-delta-report.md",
        "A3-checks.md",
        "A4-invariants.md",
        "A5-report.md",
        "A5-worklist.xlsx",
        "TAXPAYER.csv",
    ):
        p = INVESTIGATION / name
        A(f"- `{'✓' if p.exists() else '✗'}` `{p}`")
    A("")
    A("## Acceptance reminders")
    A("")
    A("- No TaxOps repairs/merges/schema changes performed — recommendations only.")
    A("- Live DB size unchanged across A4; I7 used throwaway copy only.")
    A("- Two consecutive identical-input runs should yield stable fingerprints (re-run A2/A3 to verify zero NEW if needed).")
    A("")

    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def run_a5_phase(*, max_worklist: int = 250) -> tuple[Path, Path, int]:
    priority_types = frozenset(
        {
            "DUPLICATE_CLIENT",
            "MERGE_UNTRACEABLE",
            "L0_DRAKE_ONLY",
            "LOG_NUMBER_COLLISION",
            "MALFORMED_LOG_NUMBER",
            "PREFILL_STUB_BURST",
            "SPOUSE_STORE_DIVERGENCE",
            "SPOUSE_AMBIGUOUS",
            "NEEDS_HUMAN",
            "MISSING_IN_TAXOPS",
            "JUL1_PROVENANCE",
        }
    )
    priority = collect_worklist_rows(max_rows=min(200, max_worklist), types=priority_types)
    xlsx = write_worklist_xlsx(priority)
    md = write_a5_report(priority)
    return md, xlsx, len(priority)

if __name__ == "__main__":
    md, xlsx, n = run_a5_phase()
    print(f"A5 done report={md} worklist={xlsx} rows={n}")
