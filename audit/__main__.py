"""CLI entry: python -m audit ..."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow `python -m audit` from repo root (T:\)
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from audit import config
from audit.classify import run_classify
from audit.db import audit_db_path, connect_audit
from audit.ingest import run_ingest
from audit.match import run_match
from audit.preflight import PreflightError, run_preflight
from audit.residual import run_residual
from audit.spouse import run_spouse
from audit.util import stamp_day, utc_now
from audit.workbook import write_workbook


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="TaxOps three-way client audit (findings-only; no TaxOps writes)."
    )
    p.add_argument("--operator", required=True, help="Operator name recorded on audit_run")
    p.add_argument(
        "--backup-verified",
        action="store_true",
        help="Required: you confirmed a fresh manual backup exists on the server",
    )
    p.add_argument(
        "--csm-unfiltered",
        action="store_true",
        help="Required: Drake CSM export was taken from an unfiltered grid",
    )
    p.add_argument("--drake", type=Path, default=config.DEFAULT_DRAKE_PATH)
    p.add_argument("--tax-log", type=Path, default=config.DEFAULT_TAX_LOG_PATH)
    p.add_argument("--taxops-db", type=Path, default=config.DEFAULT_TAXOPS_DB)
    p.add_argument(
        "--audit-db",
        type=Path,
        default=None,
        help="Defaults to audit/audit_YYYYMMDD.sqlite",
    )
    p.add_argument(
        "--acknowledge-baseline-drift",
        action="store_true",
        help=(
            "Deprecated (A0): count drift vs previous run emits BASELINE_DRIFT and no "
            "longer blocks preflight. Flag retained for CLI compatibility only."
        ),
    )
    p.add_argument(
        "--snapshot",
        type=Path,
        default=None,
        help="Reuse an existing TaxOps snapshot sqlite instead of copying again",
    )
    p.add_argument(
        "--m1-only",
        action="store_true",
        help="Run normalizer fixture suite + Drake↔Log rate harness only",
    )
    return p


def _print_json(obj) -> None:
    # Counts/structures only — callers must not put names here.
    print(json.dumps(obj, indent=2, sort_keys=True, default=str))


def run_m1_harness(drake: Path, tax_log: Path) -> dict:
    """M1 acceptance: fixtures + end-to-end Drake↔Log rate."""
    import unittest

    from audit.match import drake_log_match_rate
    from audit.tests.test_normalizer import NormalizerFixtureTests

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(NormalizerFixtureTests)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    if not result.wasSuccessful():
        return {"fixtures_ok": False, "errors": len(result.failures) + len(result.errors)}

    import openpyxl
    from audit import config as cfg

    wb = openpyxl.load_workbook(drake, read_only=True, data_only=True)
    ws = wb.active
    drake_names = []
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        # individuals only for rate vs XCEL
        rtype = str(vals[2] or "").strip().upper() if len(vals) > 2 else ""
        if rtype in cfg.ENTITY_DRAKE_TYPES:
            continue
        drake_names.append(name)
    wb.close()

    wb = openpyxl.load_workbook(tax_log, read_only=True, data_only=True)
    ws = wb[cfg.SHEET_INDIVIDUALS]
    log_rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i < cfg.LOG_DATA_START_ROW:
            continue
        vals = list(row)
        last = str(vals[cfg.LOG_LAST_COL] or "").strip() if len(vals) > cfg.LOG_LAST_COL else ""
        first = (
            str(vals[cfg.LOG_FIRST_COL] or "").strip()
            if len(vals) > cfg.LOG_FIRST_COL
            else ""
        )
        if last or first:
            log_rows.append((last, first))
    wb.close()

    stats = drake_log_match_rate(drake_names, log_rows)
    stats["fixtures_ok"] = True
    stats["baseline_to_beat"] = 0.881
    stats["beats_baseline"] = stats["rate"] > 0.881
    return stats


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    if args.m1_only:
        stats = run_m1_harness(args.drake, args.tax_log)
        _print_json(stats)
        if not stats.get("fixtures_ok"):
            return 2
        if not stats.get("beats_baseline"):
            print(
                "M1 FAIL: match rate "
                f"{stats.get('rate')} does not beat 0.881 "
                f"(residual={stats.get('residual')})",
                file=sys.stderr,
            )
            return 3
        return 0

    audit_db = args.audit_db or audit_db_path(stamp_day(), config.AUDIT_ROOT)

    try:
        pref, run_id = run_preflight(
            operator=args.operator,
            drake_path=args.drake,
            tax_log_path=args.tax_log,
            taxops_db=args.taxops_db,
            backup_verified=args.backup_verified,
            csm_unfiltered=args.csm_unfiltered,
            audit_db=audit_db,
            skip_snapshot=bool(args.snapshot),
            snapshot_path=args.snapshot,
        )
    except PreflightError as exc:
        print(f"PREFLIGHT ERROR: {exc}", file=sys.stderr)
        return 2

    _print_json(
        {
            "milestone": "M0",
            "run_id": run_id,
            "audit_db": str(audit_db),
            "preflight_ok": pref.ok,
            "errors": pref.errors,
            "checks": {
                k: v
                for k, v in pref.checks.items()
                if k != "errors"
            },
            "taxops_snapshot": pref.taxops_snapshot,
        }
    )

    if not pref.ok:
        if not args.acknowledge_baseline_drift:
            print(
                "M0 HALT: preflight failed. Re-export sources to match baselines, "
                "or re-run with --acknowledge-baseline-drift after reviewing the diff.",
                file=sys.stderr,
            )
            return 2
        if not args.backup_verified or not args.csm_unfiltered:
            print("M0 HALT: backup/CSM flags still required.", file=sys.stderr)
            return 2
        print(
            "M0 WARN: continuing with --acknowledge-baseline-drift "
            "(diff recorded on audit_run).",
            file=sys.stderr,
        )

    if not pref.taxops_snapshot:
        print("M0 HALT: no TaxOps snapshot path.", file=sys.stderr)
        return 2

    # M2
    ingest_stats = run_ingest(
        audit_db, run_id, args.drake, args.tax_log, Path(pref.taxops_snapshot)
    )
    _print_json({"milestone": "M2", "ingest": ingest_stats})

    # M1 harness (also required mid-pipeline)
    m1 = run_m1_harness(args.drake, args.tax_log)
    _print_json({"milestone": "M1", **m1})
    if not m1.get("beats_baseline"):
        print("M1 HALT: rate did not beat 88.1%.", file=sys.stderr)
        return 3

    # M3
    match_stats = run_match(audit_db, run_id)
    _print_json({"milestone": "M3", "match": match_stats})

    # M4
    spouse_stats = run_spouse(audit_db, run_id)
    _print_json({"milestone": "M4", "spouse": spouse_stats})

    # M5
    classify_stats = run_classify(audit_db, run_id)
    _print_json({"milestone": "M5", "classify": classify_stats})

    # M6
    xlsx = write_workbook(audit_db, run_id, config.OUTPUT_DIR)
    _print_json({"milestone": "M6", "workbook": str(xlsx)})

    # M7
    residual_stats = run_residual(audit_db, run_id)
    _print_json({"milestone": "M7", "residual": residual_stats})

    # M8 is static file — confirm present
    mig = config.AUDIT_ROOT / "proposed_migration.sql"
    _print_json(
        {
            "milestone": "M8",
            "proposed_migration": str(mig),
            "exists": mig.is_file(),
            "executed": False,
        }
    )

    conn = connect_audit(audit_db)
    try:
        conn.execute(
            "UPDATE audit_run SET finished_at=? WHERE id=?",
            (utc_now(), run_id),
        )
        conn.commit()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
