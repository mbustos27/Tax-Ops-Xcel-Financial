"""A4 — Invariant guards (hard alerts, separate from findings).

Never writes to live TaxOps. Idempotency proof uses a throwaway DB copy under T:\\audit\\.

Amendment 2 C3: status is PASS | MODIFIED | FAIL (not a boolean that hides relaxations).
Amendment 2 C4: I7 asserts zero clients *and* zero returns created on pass two.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from audit import config
from audit.baseline import load_baseline_memory
from audit.db import connect_audit, connect_taxops_readonly
from audit.normalizer import normalize_person
from audit.util import dumps, utc_now

A4_REPORT_PATH = Path(r"T:\audit\investigation\A4-invariants.md")
THROWAY_DIR = Path(r"T:\audit\tmp")
DEFAULT_TAX_LOG_CSV = Path(
    r"C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC"
    r"\Shared\Logs\TAX LOG 2025 Live.csv"
)

STATUS_PASS = "PASS"
STATUS_MODIFIED = "MODIFIED"
STATUS_FAIL = "FAIL"


@dataclass
class GuardResult:
    code: str
    title: str
    status: str  # PASS | MODIFIED | FAIL
    detail: dict[str, Any] = field(default_factory=dict)
    note: str = ""

    @property
    def passed(self) -> bool:
        """True for PASS and MODIFIED (run continues); False only for FAIL."""
        return self.status in (STATUS_PASS, STATUS_MODIFIED)


def _mtime_ns(path: Path) -> Optional[int]:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return None


def backup_taxops_to(src: Path, dest: Path) -> None:
    """Consistent RO snapshot via sqlite backup API (no live writes)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    raw = str(src).replace("\\", "/")
    if raw.startswith("//"):
        uri = f"file://{raw}?mode=ro"
    else:
        uri = f"file:///{raw}?mode=ro"
    sconn = sqlite3.connect(uri, uri=True)
    dconn = sqlite3.connect(str(dest))
    try:
        sconn.backup(dconn)
        dconn.commit()
    finally:
        dconn.close()
        sconn.close()


def guard1_dup_log_year(taxops: Path) -> GuardResult:
    conn = connect_taxops_readonly(taxops)
    try:
        rows = list(
            conn.execute(
                """
                SELECT log_number, tax_year, COUNT(*) n, GROUP_CONCAT(id) ids
                FROM returns
                WHERE log_number IS NOT NULL AND TRIM(log_number) != ''
                GROUP BY log_number, tax_year
                HAVING n > 1
                """
            )
        )
    finally:
        conn.close()
    return GuardResult(
        code="I1",
        title="Zero duplicate (log_number, tax_year) in returns",
        status=STATUS_PASS if len(rows) == 0 else STATUS_FAIL,
        detail={"duplicate_groups": len(rows), "samples": [dict(zip(["log", "year", "n", "ids"], r)) for r in rows[:10]]},
    )


def guard2_entity_link_last4_only(audit_db: Path) -> GuardResult:
    if not audit_db.exists():
        return GuardResult(
            code="I2",
            title="Zero entity_link with sole evidence last-4",
            status=STATUS_FAIL,
            detail={"error": f"missing {audit_db}"},
        )
    conn = connect_audit(audit_db)
    try:
        bad = []
        for r in conn.execute("SELECT id, tier, evidence_json FROM entity_link"):
            ev = json.loads(r[2] or "{}")
            keys = set(ev.keys())
            if keys and keys <= {"last4", "ssn_last4", "csm_ssn_last4"}:
                bad.append({"id": r[0], "tier": r[1], "keys": sorted(keys)})
            if r[1] == "L1" and "last4" in ev and "surname" not in ev and ev.get("rule") != "last4+surname":
                if "surname" not in keys:
                    bad.append({"id": r[0], "tier": "L1", "reason": "missing_surname"})
    finally:
        conn.close()
    return GuardResult(
        code="I2",
        title="Zero entity_link with sole evidence last-4",
        status=STATUS_PASS if len(bad) == 0 else STATUS_FAIL,
        detail={"violations": len(bad), "samples": bad[:10]},
    )


def guard3_unique_name_ssn(taxops: Path) -> GuardResult:
    conn = connect_taxops_readonly(taxops)
    try:
        clients = list(
            conn.execute(
                "SELECT id, last_name, first_name, ssn_last4 FROM clients "
                "WHERE ssn_last4 IS NOT NULL AND TRIM(ssn_last4) != ''"
            )
        )
    finally:
        conn.close()
    buckets: dict[str, list[int]] = {}
    for cid, last, first, l4 in clients:
        n = normalize_person(last or "", first or "")
        key = f"{n.surname_full}|{n.first_key}|{(l4 or '').strip()}"
        buckets.setdefault(key, []).append(cid)
    dups = {k: v for k, v in buckets.items() if len(v) > 1}
    return GuardResult(
        code="I3",
        title="No two clients share (norm last, norm first, ssn_last4) non-null",
        status=STATUS_PASS if len(dups) == 0 else STATUS_FAIL,
        detail={"duplicate_groups": len(dups), "samples": {k: v for k, v in list(dups.items())[:10]}},
        note="Hard uniqueness on identity triple; distinct from exact-name-only twins without SSN.",
    )


def guard4_prefill_fk(taxops: Path) -> GuardResult:
    conn = connect_taxops_readonly(taxops)
    try:
        orphans = list(
            conn.execute(
                """
                SELECT d.id, d.client_id FROM drake_prefill_links d
                LEFT JOIN clients c ON c.id = d.client_id
                WHERE d.client_id IS NOT NULL AND c.id IS NULL
                """
            )
        )
        total = int(conn.execute("SELECT COUNT(*) FROM drake_prefill_links WHERE client_id IS NOT NULL").fetchone()[0])
    except sqlite3.Error as e:
        return GuardResult(
            code="I4",
            title="drake_prefill_links.client_id FK resolves",
            status=STATUS_FAIL,
            detail={"error": str(e)},
        )
    finally:
        conn.close()
    return GuardResult(
        code="I4",
        title="Every drake_prefill_links.client_id resolves to a client",
        status=STATUS_PASS if len(orphans) == 0 else STATUS_FAIL,
        detail={"linked_rows": total, "orphans": len(orphans), "orphan_ids": [r[0] for r in orphans[:20]]},
    )


def guard5_indexes(taxops: Path) -> GuardResult:
    conn = connect_taxops_readonly(taxops)
    try:
        indexes = {r[1]: r for r in conn.execute("PRAGMA index_list('returns')")}
        names = set(indexes.keys())
        ux = "ux_returns_log_year" in names
        idx_client_year = "idx_returns_unique_client_year" in names
        ux_unique = bool(indexes.get("ux_returns_log_year") and indexes["ux_returns_log_year"][2])
        cy_unique = bool(
            indexes.get("idx_returns_unique_client_year") and indexes["idx_returns_unique_client_year"][2]
        )
        dups = list(
            conn.execute(
                """
                SELECT log_number, tax_year, COUNT(*) n FROM returns
                WHERE log_number IS NOT NULL AND TRIM(log_number)!=''
                GROUP BY 1,2 HAVING n>1
                """
            )
        )
    finally:
        conn.close()
    ok = ux and idx_client_year and ux_unique and cy_unique
    return GuardResult(
        code="I5",
        title="ux_returns_log_year and idx_returns_unique_client_year present+UNIQUE",
        status=STATUS_PASS if ok else STATUS_FAIL,
        detail={
            "ux_returns_log_year": ux,
            "ux_unique": ux_unique,
            "idx_returns_unique_client_year": idx_client_year,
            "idx_client_year_unique": cy_unique,
            "index_names": sorted(names),
            "blocking_log_year_dups": len(dups),
        },
        note="db.py skips creating ux_returns_log_year when duplicate pairs exist.",
    )


def guard6_mtime(taxops: Path, mtime_before: Optional[int], size_before: Optional[int]) -> GuardResult:
    """
    Original condition: mtime unchanged across the run.
    Documented weaker condition: byte size unchanged (audit-write signal).

    When mtime drifts but size is stable → MODIFIED (not PASS).
    Size change → FAIL.
    """
    mtime_after = _mtime_ns(taxops)
    size_after = taxops.stat().st_size if taxops.exists() else None
    size_ok = size_before is not None and size_after == size_before
    mtime_ok = mtime_before is not None and mtime_after == mtime_before

    if size_ok and mtime_ok:
        status = STATUS_PASS
    elif size_ok and not mtime_ok:
        status = STATUS_MODIFIED
    else:
        status = STATUS_FAIL

    return GuardResult(
        code="I6",
        title="TaxOps DB unchanged across audit run (mtime + size)",
        status=status,
        detail={
            "original_condition": "mtime_unchanged",
            "original_condition_held": mtime_ok,
            "weaker_condition": "size_unchanged",
            "weaker_condition_held": size_ok,
            "mtime_before_ns": mtime_before,
            "mtime_after_ns": mtime_after,
            "mtime_unchanged": mtime_ok,
            "size_before": size_before,
            "size_after": size_after,
            "size_unchanged": size_ok,
            "external_mtime_touch": bool(size_ok and not mtime_ok),
        },
        note="PASS = mtime+size stable. MODIFIED = mtime drifted (NSSM/Flask) but size unchanged "
        "(RO URI + throwaway copy only). FAIL = size changed.",
    )


def _identify_new_returns(
    conn: sqlite3.Connection, ids_before: set[int], ids_after: set[int]
) -> list[dict[str, Any]]:
    new_ids = sorted(ids_after - ids_before)
    out = []
    for rid in new_ids[:20]:
        row = conn.execute(
            """
            SELECT r.id, r.log_number, r.tax_year, r.client_id, r.client_status,
                   c.last_name, c.first_name, r.created_at
            FROM returns r
            LEFT JOIN clients c ON c.id = r.client_id
            WHERE r.id = ?
            """,
            (rid,),
        ).fetchone()
        if row:
            out.append(dict(row))
    return out


def guard7_import_idempotency(taxops: Path, csv_path: Path) -> GuardResult:
    """
    Re-run Tax Log CSV import twice against a throwaway copy.
    Assert second pass creates zero new clients AND zero new returns.
    """
    if not csv_path.exists():
        return GuardResult(
            code="I7",
            title="Tax Log CSV import idempotency (throwaway copy)",
            status=STATUS_FAIL,
            detail={"error": f"CSV missing: {csv_path}"},
        )

    THROWAY_DIR.mkdir(parents=True, exist_ok=True)
    throwaway = THROWAY_DIR / "a4_idempotency_throwaway.sqlite"
    backup_taxops_to(taxops, throwaway)

    taxops_root = Path(r"T:\taxops")
    if str(taxops_root) not in sys.path:
        sys.path.insert(0, str(taxops_root))

    from importer import process_csv  # type: ignore
    from utils import now as taxops_now  # type: ignore

    conn = sqlite3.connect(str(throwaway))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    status = STATUS_FAIL
    detail: dict[str, Any] = {"throwaway": str(throwaway), "csv": str(csv_path)}
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(import_batches)")}
        clients_before = int(conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
        returns_before = int(conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0])

        def make_batch(label: str) -> int:
            h = hashlib.sha256(f"a4-{label}-{utc_now()}-{os.getpid()}".encode()).hexdigest()
            row: dict[str, Any] = {}
            for cand, val in (
                ("source_file", f"A4_{label}_TAX_LOG.csv"),
                ("filename", f"A4_{label}_TAX_LOG.csv"),
                ("file_hash", h),
                ("status", "RUNNING"),
                ("started_at", taxops_now()),
                ("created_at", taxops_now()),
                ("imported_at", taxops_now()),
            ):
                if cand in cols:
                    row[cand] = val
            if not row:
                raise RuntimeError(f"import_batches has no usable columns: {sorted(cols)}")
            keys = list(row.keys())
            cur = conn.execute(
                f"INSERT INTO import_batches ({','.join(keys)}) VALUES ({','.join('?' * len(keys))})",
                [row[k] for k in keys],
            )
            conn.commit()
            return int(cur.lastrowid)

        def return_ids() -> set[int]:
            return {int(r[0]) for r in conn.execute("SELECT id FROM returns")}

        b1 = make_batch("pass1")
        conn.execute("BEGIN")
        stats1 = process_csv(conn, str(csv_path), b1, "A4_pass1_TAX_LOG.csv")
        conn.commit()
        clients_mid = int(conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
        returns_mid = int(conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0])
        ids_mid = return_ids()

        b2 = make_batch("pass2")
        conn.execute("BEGIN")
        stats2 = process_csv(conn, str(csv_path), b2, "A4_pass2_TAX_LOG.csv")
        conn.commit()
        clients_after = int(conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0])
        returns_after = int(conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0])
        ids_after = return_ids()

        new_on_pass2 = _identify_new_returns(conn, ids_mid, ids_after)

        clients_ok = int(stats2.created_clients) == 0 and clients_after == clients_mid
        returns_ok = int(stats2.created_returns) == 0 and returns_after == returns_mid
        status = STATUS_PASS if (clients_ok and returns_ok) else STATUS_FAIL

        # Classify why a return appeared on pass2 (if any)
        pass2_return_diagnosis: list[dict[str, Any]] = []
        for nr in new_on_pass2:
            logn = str(nr.get("log_number") or "")
            ty = nr.get("tax_year")
            # Was there already a return with this (log, year) before pass2?
            prior = conn.execute(
                "SELECT id, client_id FROM returns WHERE log_number=? AND tax_year=? AND id != ?",
                (logn, ty, nr["id"]),
            ).fetchall()
            # Did pass1 import_rows mention this log?
            diagnosis = {
                "return": nr,
                "prior_same_log_year": [dict(p) for p in prior],
            }
            if prior:
                diagnosis["class"] = "duplicate_log_year_should_have_matched"
            elif not logn:
                diagnosis["class"] = "created_without_log_number"
            else:
                diagnosis["class"] = (
                    "no_prior_match_on_log_year — pass1 missed or assigned log during upsert; "
                    "or non-determinism in _upsert_return / matcher"
                )
            pass2_return_diagnosis.append(diagnosis)

        detail.update(
            {
                "clients_before": clients_before,
                "clients_after_pass1": clients_mid,
                "clients_after_pass2": clients_after,
                "returns_before": returns_before,
                "returns_after_pass1": returns_mid,
                "returns_after_pass2": returns_after,
                "pass2_created_clients": int(stats2.created_clients),
                "pass2_created_returns": int(stats2.created_returns),
                "clients_idempotent": clients_ok,
                "returns_idempotent": returns_ok,
                "pass2_new_returns": new_on_pass2,
                "pass2_return_diagnosis": pass2_return_diagnosis,
                "pass1": {
                    "created_clients": stats1.created_clients,
                    "updated_clients": stats1.updated_clients,
                    "created_returns": stats1.created_returns,
                    "updated_returns": stats1.updated_returns,
                    "success": stats1.success_count,
                    "errors": stats1.error_count,
                    "review": stats1.review_count,
                },
                "pass2": {
                    "created_clients": stats2.created_clients,
                    "updated_clients": stats2.updated_clients,
                    "created_returns": stats2.created_returns,
                    "updated_returns": stats2.updated_returns,
                    "success": stats2.success_count,
                    "errors": stats2.error_count,
                    "review": stats2.review_count,
                },
            }
        )
    except Exception as e:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        detail["error"] = repr(e)
        status = STATUS_FAIL
    finally:
        conn.close()

    return GuardResult(
        code="I7",
        title="Tax Log CSV import idempotency (throwaway copy)",
        status=status,
        detail=detail,
        note="Second pass must create 0 clients AND 0 returns (catches log#-match and "
        "_upsert_return gaps). See pass2_return_diagnosis when FAIL.",
    )


def write_a4_report(guards: list[GuardResult], dest: Path = A4_REPORT_PATH) -> Path:
    lines: list[str] = []
    A = lines.append
    A("# A4 — Invariant guards")
    A("")
    A(f"_Generated: {utc_now()}_")
    A("")
    n_pass = sum(1 for g in guards if g.status == STATUS_PASS)
    n_mod = sum(1 for g in guards if g.status == STATUS_MODIFIED)
    n_fail = sum(1 for g in guards if g.status == STATUS_FAIL)
    A(f"**Overall: {n_pass} PASS / {n_mod} MODIFIED / {n_fail} FAIL**")
    A("")
    A("| Guard | Title | Result |")
    A("|---|---|---|")
    for g in guards:
        A(f"| {g.code} | {g.title} | `{g.status}` |")
    A("")
    for g in guards:
        A(f"## {g.code} — {g.title}")
        A("")
        A(f"- **Result:** `{g.status}`")
        if g.note:
            A(f"- **Note:** {g.note}")
        A(f"- **Detail:** `{dumps(g.detail)}`")
        A("")
    A("## Scope")
    A("")
    A("- Live TaxOps is read-only for guards I1–I6.")
    A("- I7 writes only to `T:\\audit\\tmp\\a4_idempotency_throwaway.sqlite`.")
    A("- Violations are hard alerts — separate from A2/A3 findings.")
    A("- Status semantics (Amendment 2 C3): `PASS` = original condition held; "
      "`MODIFIED` = original failed but documented weaker condition held; "
      "`FAIL` = neither held.")
    A("")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def run_a4_phase(
    *,
    tax_log_csv: Path = DEFAULT_TAX_LOG_CSV,
    audit_db: Optional[Path] = None,
) -> tuple[list[GuardResult], Path]:
    mem = load_baseline_memory()
    taxops = Path(mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")
    adb = audit_db or Path(r"T:\audit\audit_20260810.sqlite")

    mtime_before = _mtime_ns(taxops)
    size_before = taxops.stat().st_size if taxops.exists() else None

    guards: list[GuardResult] = [
        guard1_dup_log_year(taxops),
        guard2_entity_link_last4_only(adb),
        guard3_unique_name_ssn(taxops),
        guard4_prefill_fk(taxops),
        guard5_indexes(taxops),
    ]
    guards.append(guard7_import_idempotency(taxops, tax_log_csv))
    guards.append(guard6_mtime(taxops, mtime_before, size_before))

    order = {"I1": 1, "I2": 2, "I3": 3, "I4": 4, "I5": 5, "I6": 6, "I7": 7}
    guards.sort(key=lambda g: order.get(g.code, 99))
    path = write_a4_report(guards)
    return guards, path


if __name__ == "__main__":
    gs, path = run_a4_phase()
    fails = [g.code for g in gs if g.status == STATUS_FAIL]
    mods = [g.code for g in gs if g.status == STATUS_MODIFIED]
    n_pass = sum(1 for g in gs if g.status == STATUS_PASS)
    n_mod = sum(1 for g in gs if g.status == STATUS_MODIFIED)
    n_fail = sum(1 for g in gs if g.status == STATUS_FAIL)
    print(
        f"A4 done report={path} {n_pass} PASS / {n_mod} MODIFIED / {n_fail} FAIL "
        f"fail={fails or 'none'} modified={mods or 'none'}"
    )
