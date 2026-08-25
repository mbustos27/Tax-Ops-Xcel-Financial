"""Amendment 2 re-run: A0 → A1 → A2 → A3 → A4 → A5 + FULL + C5 triage.

TaxOps read-only. Writes only under T:\\audit\\.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    from audit.baseline import build_baseline_lock, write_a0_report
    from audit import config
    from audit.ladder import run_a1_phase
    from audit.disposition import run_a2_phase
    from audit.checks import run_a3_phase
    from audit.invariants import STATUS_FAIL, STATUS_MODIFIED, STATUS_PASS, run_a4_phase
    from audit.report import run_a5_phase
    from audit.full_report import write_full_report
    import runpy

    print("=== A0 baseline lock ===")
    lock = build_baseline_lock(tax_log_path=Path(config.DEFAULT_TAX_LOG_PATH))
    a0 = write_a0_report(lock)
    inv = lock.invoice_export or {}
    print(
        f"A0 done {a0} L0 legacy={inv.get('legacy_l0_invoice_count')} "
        f"after={inv.get('valid_l0_invoice_count')} bare_max={inv.get('bare_log_max')} "
        f"malformed={len(inv.get('malformed_invoices') or [])} "
        f"collisions={len(inv.get('collisions') or [])}"
    )

    print("=== A1 ladder ===")
    st, a1 = run_a1_phase(operator="amend2", audit_db=Path(r"T:\audit\audit_20260810.sqlite"))
    print(
        f"A1 done run={st.run_id} L0_drake_taxops={st.l0_drake_taxops} "
        f"L0_3way={st.l0_three_way} L1={st.l1} L2={st.l2} L3={st.l3} L5={st.l5_unmatched_drake}"
    )

    print("=== A2 disposition delta ===")
    delta, a2 = run_a2_phase(run_label="amend2-c1c2")
    from collections import Counter

    def by_type(items, key="finding_type"):
        if not items:
            return {}
        if isinstance(items[0], dict):
            return dict(Counter(x.get(key) or x.get("finding_type") for x in items))
        return dict(Counter(getattr(x, "finding_type", None) for x in items))

    print(
        f"A2 done NEW={len(delta.new)} RECURRING={len(delta.recurring)} "
        f"RESOLVED={len(delta.resolved)} REGRESSED={len(delta.regressed)}"
    )
    print("  NEW by type:", by_type(delta.new))
    print("  RESOLVED by type:", by_type(delta.resolved))
    print("  REGRESSED by type:", by_type(delta.regressed))
    print("  seed:", (delta.backfill_stats or {}).get("c1_legacy_malformed_seed"))

    # Fingerprint churn guard
    if len(delta.new) > 100:
        print("STOP: NEW spiked >100 — fingerprint recipe may have churned")
        sys.exit(2)

    print("=== A3 checks ===")
    results, a3 = run_a3_phase()
    alarming = [r.code for r in results if r.deviation == "ALARMING"]
    print(f"A3 done alarming={alarming or 'none'}")

    print("=== A4 invariants ===")
    guards, a4 = run_a4_phase(audit_db=Path(r"T:\audit\audit_20260810.sqlite"))
    n_pass = sum(1 for g in guards if g.status == STATUS_PASS)
    n_mod = sum(1 for g in guards if g.status == STATUS_MODIFIED)
    n_fail = sum(1 for g in guards if g.status == STATUS_FAIL)
    print(f"A4 done {n_pass} PASS / {n_mod} MODIFIED / {n_fail} FAIL")
    for g in guards:
        print(f"  {g.code} {g.status}")
        if g.code == "I7":
            print("    pass2_diag:", g.detail.get("pass2_return_diagnosis"))
            print("    returns_idempotent:", g.detail.get("returns_idempotent"))

    print("=== C5 log import triage ===")
    runpy.run_path(str(Path(r"T:\audit\investigation\_c5_log_import_triage.py")), run_name="__main__")

    print("=== A5 + FULL ===")
    md5, xlsx, n = run_a5_phase()
    full = write_full_report()
    print(f"A5 priority_rows={n} full={full}")
    print("DONE amend2 rerun")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
