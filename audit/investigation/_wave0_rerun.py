"""Wave 0 re-run: A0 → A1 → A2 → A3 (link_5 INVOICE NUMBER LINK export).

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
    from collections import Counter

    from audit import config
    from audit.baseline import build_baseline_lock, write_a0_report
    from audit.checks import run_a3_phase
    from audit.disposition import run_a2_phase
    from audit.ladder import run_a1_phase

    print("=== A0 baseline lock ===")
    lock = build_baseline_lock(tax_log_path=Path(config.DEFAULT_TAX_LOG_PATH))
    a0 = write_a0_report(lock)
    inv = lock.invoice_export or {}
    print(
        f"A0 done {a0} layout={inv.get('layout')} full={inv.get('full_width_count')} "
        f"ragged={inv.get('ragged_count')} empty_inv={inv.get('empty_invoice_full_width')} "
        f"L0 legacy={inv.get('legacy_l0_invoice_count')} after={inv.get('valid_l0_invoice_count')} "
        f"bare_max={inv.get('bare_log_max')} "
        f"malformed={len(inv.get('malformed_invoices') or [])} "
        f"collisions={len(inv.get('collisions') or [])}"
    )

    print("=== A1 ladder ===")
    st, a1 = run_a1_phase(operator="wave0", audit_db=Path(r"T:\audit\audit_20260810.sqlite"))
    print(
        f"A1 done run={st.run_id} L0_drake_taxops={st.l0_drake_taxops} "
        f"L0_3way={st.l0_three_way} L1={st.l1} L2={st.l2} L3={st.l3} L5={st.l5_unmatched_drake}"
    )

    print("=== A2 disposition delta ===")
    delta, a2 = run_a2_phase(run_label="wave0-link5")

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

    if len(delta.new) > 100:
        print("STOP: NEW spiked >100 — fingerprint recipe may have churned")
        sys.exit(2)

    print("=== A3 checks ===")
    results, a3 = run_a3_phase()
    alarming = [r.code for r in results if r.deviation == "ALARMING"]
    print(f"A3 done alarming={alarming or 'none'}")
    for r in results:
        print(f"  {r.code} {r.deviation} count={getattr(r, 'count', None)}")
    print("DONE wave0 A0–A3")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
