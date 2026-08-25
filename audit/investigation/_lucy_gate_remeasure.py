"""Remeasure A1/A2/A3/A4 + FULL-REPORT after R0 software waves + Lucy packet."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from audit.ladder import run_a1_phase
    from audit.disposition import run_a2_phase
    from audit.checks import run_a3_phase
    from audit.invariants import STATUS_FAIL, STATUS_MODIFIED, STATUS_PASS, run_a4_phase
    from audit.full_report import write_full_report
    from collections import Counter

    print("=== A1 ===")
    st, a1 = run_a1_phase(
        operator="lucy-gate",
        audit_db=Path(r"T:\audit\audit_20260810.sqlite"),
    )
    print(
        f"A1 run={st.run_id} L0_drake_taxops={st.l0_drake_taxops} "
        f"L0_3way={st.l0_three_way} L1={st.l1} L5={st.l5_unmatched_drake} -> {a1}"
    )

    print("=== A2 ===")
    delta, a2 = run_a2_phase(run_label="lucy-gate-remeasure")

    def by_type(items):
        if not items:
            return {}
        if isinstance(items[0], dict):
            return dict(Counter(x.get("finding_type") for x in items))
        return dict(Counter(getattr(x, "finding_type", None) for x in items))

    print(
        f"A2 NEW={len(delta.new)} RECURRING={len(delta.recurring)} "
        f"RESOLVED={len(delta.resolved)} REGRESSED={len(delta.regressed)} -> {a2}"
    )
    print("  NEW:", by_type(delta.new))
    print("  RESOLVED:", by_type(delta.resolved))
    if len(delta.new) > 150:
        print("WARN: NEW spike — fingerprint churn?")

    print("=== A3 ===")
    results, a3 = run_a3_phase()
    alarming = [r.code for r in results if r.deviation == "ALARMING"]
    print(f"A3 alarming={alarming or 'none'} -> {a3}")

    print("=== A4 ===")
    guards, a4 = run_a4_phase(audit_db=Path(r"T:\audit\audit_20260810.sqlite"))
    n_pass = sum(1 for g in guards if g.status == STATUS_PASS)
    n_mod = sum(1 for g in guards if g.status == STATUS_MODIFIED)
    n_fail = sum(1 for g in guards if g.status == STATUS_FAIL)
    print(f"A4 {n_pass} PASS / {n_mod} MODIFIED / {n_fail} FAIL -> {a4}")
    for g in guards:
        print(f"  {g.code} {g.status}")

    print("=== FULL ===")
    full = write_full_report()
    print("FULL ->", full)


if __name__ == "__main__":
    main()
