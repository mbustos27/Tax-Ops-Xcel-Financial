"""M0 / A0 — preflight, baseline lock, and snapshot (read-only TaxOps)."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from audit import config
from audit.baseline import (
    build_baseline_lock,
    lock_to_dict,
    write_a0_report,
)
from audit.db import connect_audit, connect_taxops_readonly
from audit.util import dumps, sha256_file, stamp_day, utc_now


class PreflightError(Exception):
    """Raised when preflight fails; message must contain no client PII."""


@dataclass
class PreflightResult:
    ok: bool
    checks: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    drake_sha256: str = ""
    tax_log_sha256: str = ""
    taxops_snapshot: str = ""
    taxops_sha256: str = ""
    observed: dict[str, Any] = field(default_factory=dict)
    baseline: dict[str, Any] = field(default_factory=dict)


def _taxops_counts(conn: sqlite3.Connection) -> dict[str, int]:
    def one(sql: str) -> int:
        return int(conn.execute(sql).fetchone()[0])

    schema = conn.execute(
        "SELECT value FROM app_settings WHERE key='schema_version'"
    ).fetchone()
    return {
        "clients": one("SELECT COUNT(*) FROM clients"),
        "returns": one("SELECT COUNT(*) FROM returns"),
        "ty2025": one("SELECT COUNT(*) FROM returns WHERE tax_year=2025"),
        "spouses": one("SELECT COUNT(*) FROM spouses"),
        "schema_version": int(schema[0]) if schema else -1,
    }


def snapshot_taxops(src: Path, dest_dir: Path) -> Path:
    """
    Consistent copy via sqlite backup API into audit/snapshots/.
    Opens source read-only; writes only to dest (audit tree).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"taxops_snapshot_{stamp_day()}.sqlite"
    if dest.exists():
        dest.unlink()

    src_conn = connect_taxops_readonly(src)
    try:
        dst = sqlite3.connect(str(dest))
        try:
            src_conn.backup(dst)
            dst.commit()
        finally:
            dst.close()
    finally:
        src_conn.close()
    return dest


def run_preflight(
    *,
    operator: str,
    drake_path: Path,
    tax_log_path: Path,
    taxops_db: Path,
    backup_verified: bool,
    csm_unfiltered: bool,
    audit_db: Path,
    skip_snapshot: bool = False,
    snapshot_path: Optional[Path] = None,
    acknowledge_baseline_drift: bool = False,  # retained for CLI compat; drift no longer blocks
) -> tuple[PreflightResult, int]:
    """
    A0 baseline lock + snapshot + audit_run insert.
    Hard refuse only: missing backup/csm flags, missing source files, snapshot failure.
    Count drift vs previous run → BASELINE_DRIFT finding (recorded), not a crash.
    """
    del acknowledge_baseline_drift  # no longer a gate (A0)
    result = PreflightResult(ok=True)
    checks: dict[str, Any] = {}

    if not backup_verified:
        result.ok = False
        result.errors.append(
            "Refusing: pass --backup-verified after confirming a fresh manual backup exists."
        )
    checks["backup_verified"] = backup_verified

    if not csm_unfiltered:
        result.ok = False
        result.errors.append(
            "Refusing: pass --csm-unfiltered after confirming Drake CSM grid was unfiltered."
        )
    checks["csm_unfiltered"] = csm_unfiltered

    # A0: explicit baseline lock (may re-resolve Drake path to OneDrive preferred)
    baseline_dict: dict[str, Any] = {}
    try:
        lock = build_baseline_lock(
            tax_log_path=tax_log_path,
            cli_drake=drake_path if drake_path else None,
            taxops_snapshot=None,
        )
        # If operator passed a path, build_baseline_lock already honored it.
        # Prefer lock's authoritative Drake for the rest of the run.
        drake_path = Path(lock.authoritative_drake_path)
        taxops_db = Path(lock.authoritative_taxops_path)
        baseline_dict = lock_to_dict(lock)
        result.baseline = baseline_dict
        checks["baseline"] = {
            "csm_key": lock.authoritative_drake_key,
            "taxops_resolution": lock.taxops_path_resolution,
            "drift_status": lock.drift_vs_previous.get("status"),
            "findings": lock.findings,
        }
        write_a0_report(lock)
    except (FileNotFoundError, ValueError, OSError) as exc:
        result.ok = False
        result.errors.append(f"Baseline lock failed: {type(exc).__name__}: {exc}")
        checks["baseline_error"] = str(exc)

    if not drake_path.is_file():
        result.ok = False
        result.errors.append(f"Drake file NOT FOUND: {drake_path}")
    if not tax_log_path.is_file():
        result.ok = False
        result.errors.append(f"Tax Log file NOT FOUND: {tax_log_path}")
    if not taxops_db.exists():
        result.ok = False
        result.errors.append(f"TaxOps DB NOT FOUND: {taxops_db}")

    # Populate observed from baseline sources when available
    if baseline_dict:
        for src in baseline_dict.get("sources") or []:
            if src.get("role") == "csm:AUTHORITATIVE":
                result.drake_sha256 = src.get("sha256") or ""
                result.observed["drake_rows"] = src.get("row_count")
                result.observed["drake_types"] = (src.get("extra") or {}).get("types")
                result.observed["drake_mtime_utc"] = src.get("mtime_utc")
            if src.get("role") == "tax_log":
                result.tax_log_sha256 = src.get("sha256") or ""
                result.observed["log_named"] = src.get("row_count")
                result.observed["tax_log_mtime_utc"] = src.get("mtime_utc")
            if src.get("role") == "taxops:AUTHORITATIVE":
                result.observed["taxops_mtime_utc"] = src.get("mtime_utc")
                result.observed["taxops_page_count"] = (src.get("extra") or {}).get("page_count")

        # Soft compare to historical Jul31 lock (informational only)
        hist = {
            "drake_rows": config.HISTORICAL_DRAKE_ROWS,
            "log_named": config.HISTORICAL_LOG_NAMED_ROWS,
        }
        soft = {}
        if result.observed.get("drake_rows") != hist["drake_rows"]:
            soft["drake_rows"] = {
                "historical": hist["drake_rows"],
                "observed": result.observed.get("drake_rows"),
            }
        if result.observed.get("log_named") != hist["log_named"]:
            soft["log_named"] = {
                "historical": hist["log_named"],
                "observed": result.observed.get("log_named"),
            }
        checks["vs_historical_jul31_lock"] = soft or {"ok": True}
        checks["drift_vs_previous_run"] = baseline_dict.get("drift_vs_previous")
        checks["csm_prefill_lag_note"] = baseline_dict.get("csm_prefill_lag_note")

    snap: Optional[Path] = snapshot_path
    taxops_mtime_before = None
    if taxops_db.exists():
        try:
            taxops_mtime_before = taxops_db.stat().st_mtime
            if skip_snapshot and snapshot_path and snapshot_path.is_file():
                snap = snapshot_path
            elif not skip_snapshot:
                snap = snapshot_taxops(taxops_db, config.SNAPSHOT_DIR)
            if snap and snap.is_file():
                result.taxops_snapshot = str(snap)
                result.taxops_sha256 = sha256_file(snap)
                tconn = connect_taxops_readonly(snap)
                try:
                    tc = _taxops_counts(tconn)
                    page_count = int(tconn.execute("PRAGMA page_count").fetchone()[0])
                finally:
                    tconn.close()
                result.observed["taxops"] = tc
                result.observed["taxops_page_count"] = page_count
                # Informational vs historical — not a crash
                hist_tax = {
                    "clients": config.HISTORICAL_TAXOPS_CLIENTS,
                    "returns": config.HISTORICAL_TAXOPS_RETURNS,
                    "ty2025": config.HISTORICAL_TAXOPS_TY2025,
                    "spouses": config.HISTORICAL_TAXOPS_SPOUSES,
                }
                tax_diff = {
                    k: {"historical": e, "observed": tc.get(k)}
                    for k, e in hist_tax.items()
                    if tc.get(k) != e
                }
                checks["taxops_vs_historical"] = {"diff": tax_diff, "observed": tc}
            # A4 precursor: record mtime unchanged across snapshot
            taxops_mtime_after = taxops_db.stat().st_mtime
            checks["taxops_mtime_unchanged_during_snapshot"] = (
                taxops_mtime_before == taxops_mtime_after
            )
            if taxops_mtime_before != taxops_mtime_after:
                result.errors.append(
                    "ALERT: TaxOps DB mtime changed during snapshot (external writer?)"
                )
                # Do not flip ok=False solely for this — A4 will hard-alert; record it
                checks["taxops_mtime_alert"] = True
        except Exception as exc:  # noqa: BLE001
            result.ok = False
            result.errors.append(f"TaxOps snapshot/count failed: {type(exc).__name__}: {exc}")

    checks["errors"] = list(result.errors)
    result.checks = checks

    # Extract A0 fields for audit_run columns
    auth_src = next(
        (s for s in (baseline_dict.get("sources") or []) if s.get("role") == "csm:AUTHORITATIVE"),
        {},
    )
    log_src = next(
        (s for s in (baseline_dict.get("sources") or []) if s.get("role") == "tax_log"),
        {},
    )
    tax_src = next(
        (s for s in (baseline_dict.get("sources") or []) if s.get("role") == "taxops:AUTHORITATIVE"),
        {},
    )

    conn = connect_audit(audit_db)
    try:
        cur = conn.execute(
            """
            INSERT INTO audit_run (
              started_at, operator, tool_version, backup_verified, csm_unfiltered,
              drake_path, drake_sha256, tax_log_path, tax_log_sha256,
              taxops_snapshot, taxops_sha256, preflight_json, preflight_ok, notes,
              authoritative_taxops_path, taxops_path_resolution, taxops_page_count,
              drake_row_count, tax_log_named_count,
              drake_mtime_utc, tax_log_mtime_utc, taxops_mtime_utc,
              baseline_json, csm_baseline_key
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now(),
                operator,
                config.TOOL_VERSION,
                1 if backup_verified else 0,
                1 if csm_unfiltered else 0,
                str(drake_path),
                result.drake_sha256 or "",
                str(tax_log_path),
                result.tax_log_sha256 or "",
                result.taxops_snapshot or "",
                result.taxops_sha256 or "",
                dumps(checks),
                1 if result.ok else 0,
                dumps(result.observed),
                baseline_dict.get("authoritative_taxops_path") or str(taxops_db),
                baseline_dict.get("taxops_path_resolution"),
                result.observed.get("taxops_page_count"),
                result.observed.get("drake_rows"),
                result.observed.get("log_named"),
                auth_src.get("mtime_utc"),
                log_src.get("mtime_utc"),
                tax_src.get("mtime_utc"),
                dumps(baseline_dict) if baseline_dict else None,
                baseline_dict.get("authoritative_drake_key"),
            ),
        )
        run_id = int(cur.lastrowid)

        # Persist baseline findings onto audit_finding for the run (typed)
        for f in baseline_dict.get("findings") or []:
            conn.execute(
                """
                INSERT INTO audit_finding (
                  run_id, finding_type, subtype, severity, subject_kind, subject_id,
                  tax_year, detail_json, source_refs
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    f.get("type") or "BASELINE",
                    "a0",
                    40 if f.get("type") == "BASELINE_DRIFT" else 60,
                    "baseline",
                    None,
                    config.PRIMARY_TAX_YEAR,
                    dumps(f.get("detail")),
                    dumps({"phase": "A0"}),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return result, run_id
