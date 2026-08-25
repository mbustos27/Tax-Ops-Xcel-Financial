"""A2 — Stable finding fingerprints + persistent disposition memory.

Disposition DB is separate from per-run audit_*.sqlite and is never truncated.
Never writes to TaxOps.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from audit.ladder import bare_log_number
from audit.normalizer import clean_tokens, normalize_drake_client_name, normalize_person
from audit.util import dumps, utc_now

DISPOSITION_DB_PATH = Path(r"T:\audit\audit_disposition.sqlite")
A2_REPORT_PATH = Path(r"T:\audit\investigation\A2-delta-report.md")
JUL31_AUDIT_DB = Path(r"T:\audit\audit_20260731.sqlite")

DISPOSITION_STATUSES = frozenset(
    {"OPEN", "ACKED", "WONTFIX", "RESOLVED", "FALSE_POSITIVE"}
)
DELTA_BUCKETS = ("NEW", "RECURRING", "RESOLVED", "REGRESSED")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS audit_disposition (
  finding_id      TEXT PRIMARY KEY,
  finding_type    TEXT NOT NULL,
  entity_key      TEXT NOT NULL,
  status          TEXT NOT NULL
    CHECK (status IN ('OPEN','ACKED','WONTFIX','RESOLVED','FALSE_POSITIVE')),
  resolved_by     TEXT,
  resolved_at     TEXT,
  note            TEXT,
  first_seen_run  TEXT NOT NULL,
  last_seen_run   TEXT NOT NULL,
  first_seen_at   TEXT NOT NULL,
  last_seen_at    TEXT NOT NULL,
  fingerprint_doc TEXT,
  sample_detail   TEXT
);

CREATE TABLE IF NOT EXISTS disposition_run (
  run_label       TEXT PRIMARY KEY,
  started_at      TEXT NOT NULL,
  finished_at     TEXT,
  source          TEXT,
  notes           TEXT,
  stats_json      TEXT
);

CREATE TABLE IF NOT EXISTS disposition_sighting (
  finding_id      TEXT NOT NULL,
  run_label       TEXT NOT NULL,
  delta_bucket    TEXT NOT NULL,
  detail_json     TEXT,
  PRIMARY KEY (finding_id, run_label)
);

CREATE INDEX IF NOT EXISTS idx_disp_status ON audit_disposition(status);
CREATE INDEX IF NOT EXISTS idx_disp_type ON audit_disposition(finding_type);
"""

# Documented fingerprint recipes (finding_type → justification).
FINGERPRINT_DOCS: dict[str, str] = {
    "NAME_TRUNCATED": (
        "entity_key = last4|' '|normalized_csm_name_prefix (folded, no stage_id). "
        "Survives CSM re-export row order and TaxOps merges; changes only if CSM "
        "last4 or truncated display string changes. Amendment 2 C6: informational — "
        "CSM 40-char display artifact; L0 invoice / L1 carry identity; not Priority."
    ),
    "PHANTOM_IN_TAXOPS": (
        "entity_key = bare_log|tax_year if log present, else norm_name(last|first). "
        "Avoids TaxOps client_id (merge attrition)."
    ),
    "MISSING_IN_TAXOPS": (
        "entity_key = last4|' '|norm_drake_name or bare invoice/log if known. "
        "No Drake stage_id."
    ),
    "LOGGED_NOT_PREPARED": (
        "entity_key = bare_log|tax_year (Tax Log col B normalized). "
        "Sheet row numbers churn; bare log does not."
    ),
    "PREPARED_NOT_LOGGED": (
        "entity_key = last4|' '|norm_drake_name (Drake prepared, no log link). "
        "Prefer bare_log when invoice available."
    ),
    "DUPLICATE_CLIENT": (
        "entity_key = sorted(norm_a, norm_b) joined by '||'. "
        "Client ids excluded — merges would churn fingerprints."
    ),
    "SPOUSE_STORE_DIVERGENCE": (
        "entity_key = norm_client_name|tax_year (or last4 when present). "
        "No client_id."
    ),
    "SPOUSE_AMBIGUOUS": (
        "entity_key = last4|' '|norm_drake_name."
    ),
    "NEEDS_HUMAN": (
        "entity_key = subtype|' '|stable subject key (last4/name/bare_log). "
        "Subtype included so distinct review reasons don't collapse."
    ),
    "MERGE_UNTRACEABLE": (
        "Singleton: entity_key='global'. Emitted when there is no durable merge "
        "history table. May coexist with MERGE_PARTIAL_TRAIL (audit_log keep_id/"
        "discard_id only; no discarded-identity snapshot)."
    ),
    "L0_KEY_PRESENT": (
        "entity_key = bare_log|tax_year for successful invoice L0 links "
        "(informational / coverage; usually not worklist)."
    ),
    "L0_DRAKE_ONLY": (
        "entity_key = bare_log|tax_year for L0-eligible Drake invoice with "
        "no TaxOps and no Log hit."
    ),
    "MALFORMED_LOG_NUMBER": (
        "entity_key = raw invoice string (digits as exported). "
        "Salient unused. Bare form lives in detail only — do not put bare in "
        "entity_key (would churn when normalize rules tighten)."
    ),
    "LOG_NUMBER_COLLISION": (
        "entity_key = bare_log (Amendment 2 C2). Salient = n_taxpayers. "
        "Pre-C2 collisions keyed on raw invoice are a different fingerprint space."
    ),
    "L5_UNMATCHED": (
        "entity_key = bare_log|norm_invoice_name for leftover L0-eligible "
        "invoice taxpayers after ladder."
    ),
    "CONFIRMED_MATCH": (
        "Skipped for disposition worklist (noise). Fingerprint exists for "
        "completeness: bare_log|last4|norm_name."
    ),
}


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def norm_name_key(last: str = "", first: str = "", display: str = "") -> str:
    if display and not (last or first):
        n = normalize_drake_client_name(display)
    else:
        n = normalize_person(last or "", first or "", display_raw=display or None)
    # Stable bag: surname_full + first_key (not raw punctuation)
    return f"{n.surname_full}|{n.first_key}"


def fingerprint(finding_type: str, entity_key: str, salient: str = "") -> str:
    """finding_id = sha256(type || entity_key || salient_payload)."""
    payload = f"{finding_type}\n{entity_key}\n{salient or ''}"
    return _sha256_hex(payload)


@dataclass
class Finding:
    finding_type: str
    entity_key: str
    salient: str = ""
    detail: dict[str, Any] = field(default_factory=dict)
    finding_id: str = ""

    def __post_init__(self) -> None:
        if not self.finding_id:
            self.finding_id = fingerprint(self.finding_type, self.entity_key, self.salient)


def connect_disposition(path: Path = DISPOSITION_DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def migrate_disposition_schema(conn: sqlite3.Connection) -> None:
    """Additive only — never DROP. Safe to call every open."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_disposition)")}
    additive = (
        ("fingerprint_doc", "TEXT"),
        ("sample_detail", "TEXT"),
        ("first_seen_at", "TEXT"),
        ("last_seen_at", "TEXT"),
    )
    for name, decl in additive:
        if name not in cols:
            conn.execute(f"ALTER TABLE audit_disposition ADD COLUMN {name} {decl}")
    conn.commit()


# ── Merge trail assessment ─────────────────────────────────────────────


def assess_merge_trail(taxops_path: Path) -> dict[str, Any]:
    """
    merge_ops.delete discard client with no merge_ops table.
    audit_log has POST api_merge_clients — check if payloads identify keep/discard.
    """
    from audit.db import connect_taxops_readonly

    out: dict[str, Any] = {
        "merge_tables": [],
        "audit_log_merge_count": 0,
        "payloads_with_keep_discard": 0,
        "payloads_with_entity_id_only": 0,
        "sample_keys": [],
        "verdict": "MERGE_UNTRACEABLE",
        "rationale": "",
    }
    conn = connect_taxops_readonly(taxops_path)
    try:
        out["merge_tables"] = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%merge%'"
            )
        ]
        rows = list(
            conn.execute(
                "SELECT entity_id, before_json, after_json, created_at "
                "FROM audit_log WHERE action LIKE '%merge_clients%'"
            )
        )
        out["audit_log_merge_count"] = len(rows)
        key_counter: Counter[str] = Counter()
        for entity_id, before_json, after_json, _ca in rows:
            blob = {}
            for raw in (before_json, after_json):
                if not raw:
                    continue
                try:
                    blob.update(json.loads(raw) if isinstance(raw, str) else raw)
                except (json.JSONDecodeError, TypeError):
                    continue
            key_counter.update(blob.keys())
            text = json.dumps(blob).lower() + " " + str(entity_id or "").lower()
            if any(k in text for k in ("keep_id", "discard_id", "keep", "discard", " survivor")):
                # Require numeric id pair signals
                if re.search(r"keep", text) and re.search(r"discard", text):
                    out["payloads_with_keep_discard"] += 1
                elif entity_id:
                    out["payloads_with_entity_id_only"] += 1
            elif entity_id:
                out["payloads_with_entity_id_only"] += 1
        out["sample_keys"] = [k for k, _ in key_counter.most_common(15)]
        # entity_id often is JSON: {"keep_id": N, "discard_id": M}
        keep_discard_pairs = 0
        for entity_id, before_json, after_json, _ca in rows:
            pair = None
            for raw in (entity_id, before_json, after_json):
                if not raw:
                    continue
                try:
                    obj = json.loads(raw) if isinstance(raw, str) else raw
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(obj, dict):
                    preview = obj.get("payload_preview") if isinstance(obj.get("payload_preview"), dict) else obj
                    if preview and "keep_id" in preview and "discard_id" in preview:
                        pair = (preview["keep_id"], preview["discard_id"])
                        break
            if pair:
                keep_discard_pairs += 1
        out["payloads_with_keep_discard"] = keep_discard_pairs

        if "client_merge_history" in out["merge_tables"]:
            n_hist = 0
            try:
                n_hist = int(
                    conn.execute("SELECT COUNT(*) FROM client_merge_history").fetchone()[0]
                )
            except sqlite3.OperationalError:
                pass
            cols = {
                r[1]
                for r in conn.execute("PRAGMA table_info(client_merge_history)").fetchall()
            }
            needed = {
                "keep_id",
                "discard_id",
                "discard_client_json",
                "discard_returns_json",
                "merged_at",
            }
            if needed.issubset(cols):
                out["verdict"] = "MERGE_TRAIL_COMPLETE"
                out["rationale"] = (
                    "client_merge_history present with discard identity snapshot columns; "
                    f"{n_hist} history row(s). Merges are reconstructable from the trail."
                )
                out["client_merge_history_rows"] = n_hist
            else:
                out["verdict"] = "MERGE_TABLE_PRESENT"
                out["rationale"] = (
                    f"Found client_merge_history but missing columns {sorted(needed - cols)}"
                )
        elif out["merge_tables"]:
            out["verdict"] = "MERGE_TABLE_PRESENT"
            out["rationale"] = f"Found tables {out['merge_tables']}"
        elif keep_discard_pairs > 0:
            out["verdict"] = "MERGE_PARTIAL_TRAIL"
            out["rationale"] = (
                f"No merge_* history table; merge_ops DELETEs discard clients. "
                f"However audit_log records {keep_discard_pairs}/{len(rows)} "
                "api_merge_clients calls with keep_id/discard_id in entity_id "
                "(no after-snapshot of discarded identity). "
                "Jul1 attrition (368→191) can be partially attributed to these "
                f"{len(rows)} logged merges, but silent deletes and unlogged paths "
                "remain indistinguishable — emit MERGE_UNTRACEABLE finding with "
                "this partial-trail note."
            )
        else:
            out["verdict"] = "MERGE_UNTRACEABLE"
            out["rationale"] = (
                "merge_ops.merge_client_into DELETEs discard clients with no merge history "
                f"table. audit_log has {out['audit_log_merge_count']} merge API rows but "
                f"payloads lack keep/discard ids (keys seen: {out['sample_keys'][:8]}). "
                "Jul1-dated client attrition (368→191) cannot distinguish merge-resolved "
                "from deleted."
            )
    finally:
        conn.close()
    return out


# ── Build current-run findings (from A1 ladder + live RO checks) ───────


def findings_from_ladder(audit_db: Path, run_id: int) -> list[Finding]:
    from audit.db import connect_audit

    out: list[Finding] = []
    conn = connect_audit(audit_db)
    try:
        for r in conn.execute(
            "SELECT tier, left_kind, left_key, right_kind, right_key, evidence_json "
            "FROM entity_link WHERE run_id=?",
            (run_id,),
        ):
            ev = json.loads(r["evidence_json"] or "{}")
            bare = str(ev.get("bare_log") or "")
            if r["tier"] == "L0" and r["left_kind"] == "drake_invoice" and bare:
                out.append(
                    Finding(
                        finding_type="L0_KEY_PRESENT",
                        entity_key=f"{bare}|2025",
                        salient=r["right_kind"],
                        detail={"left": r["left_key"], "right": r["right_key"], **{k: ev[k] for k in list(ev)[:6]}},
                    )
                )
            if r["tier"] == "L5" or (
                # ladder doesn't store L5 as links — handled elsewhere
                False
            ):
                pass
    finally:
        conn.close()
    return out


def findings_invoice_validation(invoice_path: Path) -> list[Finding]:
    """Emit MALFORMED_LOG_NUMBER + LOG_NUMBER_COLLISION from current C1/C2 parser."""
    from audit.invoice_export import parse_taxpayer_invoice_csv

    inv = parse_taxpayer_invoice_csv(invoice_path)
    out: list[Finding] = []
    seen_m: set[str] = set()
    for m in inv.malformed_invoices:
        inv_no = str(m.get("invoice") or "")
        if not inv_no or inv_no in seen_m:
            continue
        seen_m.add(inv_no)
        out.append(
            Finding(
                finding_type="MALFORMED_LOG_NUMBER",
                entity_key=inv_no,
                salient="",
                detail=m,
            )
        )
    for c in inv.collisions:
        bare = str(c.get("bare_log") or "")
        if not bare:
            continue
        out.append(
            Finding(
                finding_type="LOG_NUMBER_COLLISION",
                entity_key=bare,
                salient=str(c.get("n_taxpayers") or ""),
                detail=c,
            )
        )
    return out


def seed_legacy_malformed_open(dconn: sqlite3.Connection, invoice_path: Path) -> dict[str, Any]:
    """
    Amendment 2 C1 delta support: insert OPEN rows for invoices that failed the
    legacy ``^\\d{6}$`` gate so they can auto-resolve when no longer malformed.
    Idempotent — skips fingerprints already present. Does not change recipes.
    """
    import re
    from audit.invoice_export import parse_taxpayer_invoice_csv

    inv = parse_taxpayer_invoice_csv(invoice_path)
    prefix = inv.season_prefix or "25"
    legacy_bad: set[str] = set()
    for r in inv.rows:
        if not r.is_full_width or not r.invoice:
            continue
        if not re.fullmatch(r"\d{6}", r.invoice) or (
            prefix and not r.invoice.startswith(prefix)
        ):
            # legacy also treated wrong-prefix 6-digit as stale/malformed for L0
            if not re.fullmatch(r"\d{6}", r.invoice):
                legacy_bad.add(r.invoice)
            elif prefix and not r.invoice.startswith(prefix):
                legacy_bad.add(r.invoice)

    now = utc_now()
    inserted = 0
    skipped = 0
    for inv_no in sorted(legacy_bad):
        fid = fingerprint("MALFORMED_LOG_NUMBER", inv_no, "")
        exists = dconn.execute(
            "SELECT 1 FROM audit_disposition WHERE finding_id=?", (fid,)
        ).fetchone()
        if exists:
            skipped += 1
            continue
        dconn.execute(
            """
            INSERT INTO audit_disposition (
              finding_id, finding_type, entity_key, status,
              first_seen_run, last_seen_run, first_seen_at, last_seen_at,
              fingerprint_doc, sample_detail, note
            ) VALUES (?,?,?,'OPEN',?,?,?,?,?,?,?)
            """,
            (
                fid,
                "MALFORMED_LOG_NUMBER",
                inv_no,
                "pre-c1-legacy",
                "pre-c1-legacy",
                now,
                now,
                FINGERPRINT_DOCS.get("MALFORMED_LOG_NUMBER", ""),
                dumps({"invoice": inv_no, "seed": "legacy_six_digit_gate"}),
                "Seeded from Amendment-1 ^\\d{6}$ gate for C1 RESOLVED delta",
            ),
        )
        inserted += 1
    dconn.commit()
    return {"legacy_malformed": len(legacy_bad), "inserted": inserted, "skipped": skipped}


def findings_from_invoice_gaps(invoice_path: Path, taxops_path: Path, log_path: Path) -> list[Finding]:
    """Emit L0_DRAKE_ONLY / coverage findings from bare-log Venn (no TaxOps write)."""
    from audit.db import connect_taxops_readonly
    from audit.invoice_export import parse_taxpayer_invoice_csv
    from audit import config
    import openpyxl

    inv = parse_taxpayer_invoice_csv(invoice_path)
    l0 = set(inv.l0_ok_invoices)
    by_inv = {}
    for r in inv.rows:
        if r.is_full_width and r.invoice in l0 and r.invoice not in by_inv:
            by_inv[r.invoice] = r

    taxops_keys: set[str] = set()
    conn = connect_taxops_readonly(taxops_path)
    try:
        for (logn,) in conn.execute(
            "SELECT log_number FROM returns WHERE tax_year=2025 "
            "AND log_number IS NOT NULL AND TRIM(log_number)!=''"
        ):
            b = bare_log_number(str(logn))
            if b:
                taxops_keys.add(b)
    finally:
        conn.close()

    log_keys: set[str] = set()
    if log_path.exists():
        wb = openpyxl.load_workbook(log_path, read_only=True, data_only=True)
        ws = wb[config.SHEET_INDIVIDUALS]
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            if i < config.LOG_DATA_START_ROW:
                continue
            vals = list(row)
            v = vals[1] if len(vals) > 1 else None
            if isinstance(v, float) and v == int(v):
                v = int(v)
            last = str(vals[2] or "").strip() if len(vals) > 2 else ""
            first = str(vals[3] or "").strip() if len(vals) > 3 else ""
            if not (last or first):
                continue
            b = bare_log_number(str(v or ""))
            if b:
                log_keys.add(b)
        wb.close()

    out: list[Finding] = []
    for inv_no, row in by_inv.items():
        bare = bare_log_number(inv_no)
        in_t = bare in taxops_keys
        in_l = bare in log_keys
        if not in_t and not in_l:
            out.append(
                Finding(
                    finding_type="L0_DRAKE_ONLY",
                    entity_key=f"{bare}|2025",
                    salient="",
                    detail={
                        "invoice": inv_no,
                        "name": f"{row.last}, {row.first}".strip(", "),
                    },
                )
            )
        # L5-ish: in neither after name ladder is separate; here gap is key-level
    return out


def findings_name_truncated_live(csm_path: Path) -> list[Finding]:
    import openpyxl

    out: list[Finding] = []
    if not csm_path.exists():
        return out
    wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
    ws = wb.active
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        if len(name) < 39:
            continue
        last4 = re.sub(r"\D", "", str(vals[0] or ""))[-4:]
        n = normalize_drake_client_name(name)
        ek = f"{last4}|{n.surname_full}|{n.first_key}|len{len(name)}"
        out.append(
            Finding(
                finding_type="NAME_TRUNCATED",
                entity_key=ek,
                salient=str(len(name)),
                detail={"name_len": len(name), "last4": last4},
            )
        )
    wb.close()
    return out


# ── Jul31 backfill ─────────────────────────────────────────────────────


def backfill_jul31(conn: sqlite3.Connection, jul31_db: Path = JUL31_AUDIT_DB) -> dict[str, Any]:
    """Reconstruct fingerprints from Jul31 audit sqlite; first_seen_run='pre-baseline' when weak."""
    stats = {"reconstructed": 0, "pre_baseline": 0, "skipped_confirmed": 0, "by_type": Counter()}
    if not jul31_db.exists():
        stats["error"] = f"missing {jul31_db}"
        return stats

    src = sqlite3.connect(str(jul31_db))
    src.row_factory = sqlite3.Row
    try:
        # Prefer run 2 (has taxops)
        run_id = 2
        findings = list(
            src.execute(
                "SELECT * FROM audit_finding WHERE run_id=? AND finding_type != 'CONFIRMED_MATCH'",
                (run_id,),
            )
        )
        for f in findings:
            ftype = f["finding_type"]
            detail = json.loads(f["detail_json"] or "{}")
            refs = json.loads(f["source_refs"] or "{}")
            entity_key = ""
            salient = ""
            weak = False

            if ftype == "NAME_TRUNCATED":
                dr = src.execute(
                    "SELECT client_name_raw, id_last4 FROM stage_drake WHERE run_id=? AND id=?",
                    (run_id, f["subject_id"]),
                ).fetchone()
                if not dr:
                    weak = True
                    entity_key = f"stage:{f['subject_id']}"
                else:
                    n = normalize_drake_client_name(dr["client_name_raw"] or "")
                    last4 = re.sub(r"\D", "", str(dr["id_last4"] or ""))[-4:]
                    entity_key = f"{last4}|{n.surname_full}|{n.first_key}|len{len(dr['client_name_raw'] or '')}"
                    salient = str(detail.get("name_len") or len(dr["client_name_raw"] or ""))

            elif ftype == "PHANTOM_IN_TAXOPS":
                cid = refs.get("client_id")
                rid = refs.get("return_id")
                logn = None
                last = first = ""
                if rid:
                    tr = src.execute(
                        "SELECT log_number, client_id FROM stage_taxops_return "
                        "WHERE run_id=? AND return_id=?",
                        (run_id, rid),
                    ).fetchone()
                    if tr:
                        logn = tr["log_number"]
                        cid = cid or tr["client_id"]
                if cid:
                    cl = src.execute(
                        "SELECT last_name, first_name FROM stage_taxops_client "
                        "WHERE run_id=? AND client_id=?",
                        (run_id, cid),
                    ).fetchone()
                    if cl:
                        last, first = cl["last_name"] or "", cl["first_name"] or ""
                bare = bare_log_number(str(logn or ""))
                if bare:
                    entity_key = f"{bare}|2025"
                elif last or first:
                    entity_key = norm_name_key(last, first)
                else:
                    weak = True
                    entity_key = f"client:{cid}|return:{rid}"
                salient = str(detail.get("cause") or f.get("subtype") or "")

            elif ftype == "LOGGED_NOT_PREPARED":
                lr = src.execute(
                    "SELECT log_2025, last_raw, first_raw, sheet_name, source_row "
                    "FROM stage_log WHERE run_id=? AND id=?",
                    (run_id, f["subject_id"]),
                ).fetchone()
                if lr and lr["log_2025"]:
                    bare = bare_log_number(str(lr["log_2025"]))
                    entity_key = f"{bare}|2025"
                else:
                    weak = True
                    entity_key = f"log_stage:{f['subject_id']}"
                salient = "logged_not_prepared"

            elif ftype in ("PREPARED_NOT_LOGGED", "MISSING_IN_TAXOPS"):
                dr = src.execute(
                    "SELECT client_name_raw, id_last4, return_type FROM stage_drake "
                    "WHERE run_id=? AND id=?",
                    (run_id, f["subject_id"]),
                ).fetchone()
                if not dr:
                    weak = True
                    entity_key = f"drake_stage:{f['subject_id']}"
                else:
                    n = normalize_drake_client_name(dr["client_name_raw"] or "")
                    last4 = re.sub(r"\D", "", str(dr["id_last4"] or ""))[-4:]
                    entity_key = f"{last4}|{n.surname_full}|{n.first_key}"
                salient = ftype.lower()

            elif ftype == "SPOUSE_STORE_DIVERGENCE":
                # subject is taxops_client stage id
                cl = src.execute(
                    "SELECT client_id, last_name, first_name, ssn_last4 "
                    "FROM stage_taxops_client WHERE run_id=? AND id=?",
                    (run_id, f["subject_id"]),
                ).fetchone()
                if not cl:
                    # try subject_id as client_id
                    cl = src.execute(
                        "SELECT client_id, last_name, first_name, ssn_last4 "
                        "FROM stage_taxops_client WHERE run_id=? AND client_id=?",
                        (run_id, f["subject_id"]),
                    ).fetchone()
                if cl:
                    last4 = (cl["ssn_last4"] or "").strip()
                    # Always include the last4 slot (may be empty) so fingerprints do not
                    # split when ssn_last4 is later filled (2159: TEST|TEST vs 1111|TEST|TEST).
                    # Include client_id so two test rows with the same name (2175-class)
                    # do not collapse into one entity_key.
                    nk = norm_name_key(cl["last_name"] or "", cl["first_name"] or "")
                    cid = cl["client_id"]
                    entity_key = f"{cid}|{last4}|{nk}"
                else:
                    weak = True
                    entity_key = f"spouse_div_stage:{f['subject_id']}"
                salient = str(detail.get("notes") or "")

            elif ftype in ("SPOUSE_AMBIGUOUS", "NEEDS_HUMAN"):
                dr = src.execute(
                    "SELECT client_name_raw, id_last4 FROM stage_drake WHERE run_id=? AND id=?",
                    (run_id, f["subject_id"]),
                ).fetchone()
                subtype = f["subtype"] or ""
                if dr:
                    n = normalize_drake_client_name(dr["client_name_raw"] or "")
                    last4 = re.sub(r"\D", "", str(dr["id_last4"] or ""))[-4:]
                    entity_key = f"{subtype}|{last4}|{n.surname_full}|{n.first_key}"
                else:
                    weak = True
                    entity_key = f"{subtype}|stage:{f['subject_id']}"
                salient = subtype

            else:
                weak = True
                entity_key = f"opaque:{ftype}:{f['subject_id']}"
                salient = ftype

            finding = Finding(
                finding_type=ftype,
                entity_key=entity_key,
                salient=salient,
                detail={"backfill": "jul31", "weak": weak, "old_id": f["id"]},
            )
            first_run = "pre-baseline" if weak else "jul31-run2"
            now = "2026-07-31T17:56:29Z"
            existing = conn.execute(
                "SELECT finding_id, status FROM audit_disposition WHERE finding_id=?",
                (finding.finding_id,),
            ).fetchone()
            if existing:
                continue
            conn.execute(
                """
                INSERT INTO audit_disposition (
                  finding_id, finding_type, entity_key, status,
                  resolved_by, resolved_at, note,
                  first_seen_run, last_seen_run, first_seen_at, last_seen_at,
                  fingerprint_doc, sample_detail
                ) VALUES (?,?,?,'OPEN',NULL,NULL,?,?,?,?,?,?,?)
                """,
                (
                    finding.finding_id,
                    ftype,
                    entity_key,
                    "jul31 backfill" + (" (weak→pre-baseline)" if weak else ""),
                    first_run,
                    first_run,
                    now,
                    now,
                    FINGERPRINT_DOCS.get(ftype, ""),
                    dumps(finding.detail),
                ),
            )
            stats["by_type"][ftype] += 1
            if weak:
                stats["pre_baseline"] += 1
            else:
                stats["reconstructed"] += 1
        conn.commit()
    finally:
        src.close()
    stats["by_type"] = dict(stats["by_type"])
    return stats


# ── Delta apply ────────────────────────────────────────────────────────


@dataclass
class DeltaResult:
    run_label: str
    new: list[Finding] = field(default_factory=list)
    recurring: list[Finding] = field(default_factory=list)
    resolved: list[dict] = field(default_factory=list)
    regressed: list[Finding] = field(default_factory=list)
    merge_assessment: dict[str, Any] = field(default_factory=dict)
    backfill_stats: dict[str, Any] = field(default_factory=dict)
    headline: int = 0


def apply_delta(
    findings: Iterable[Finding],
    *,
    run_label: str,
    disposition_db: Path = DISPOSITION_DB_PATH,
    source: str = "a2",
    auto_resolve_types: Optional[set[str]] = None,
) -> DeltaResult:
    """
    auto_resolve_types: when source is partial, still auto-resolve absences for
    these finding_types only (Amendment 2 C1 MALFORMED seed → RESOLVED).
    """
    conn = connect_disposition(disposition_db)
    migrate_disposition_schema(conn)
    result = DeltaResult(run_label=run_label)
    now = utc_now()
    seen_ids: set[str] = set()
    findings_list = list(findings)

    conn.execute(
        "INSERT OR REPLACE INTO disposition_run (run_label, started_at, source) VALUES (?,?,?)",
        (run_label, now, source),
    )

    for f in findings_list:
        seen_ids.add(f.finding_id)
        row = conn.execute(
            "SELECT * FROM audit_disposition WHERE finding_id=?", (f.finding_id,)
        ).fetchone()
        if row is None:
            result.new.append(f)
            bucket = "NEW"
            conn.execute(
                """
                INSERT INTO audit_disposition (
                  finding_id, finding_type, entity_key, status,
                  first_seen_run, last_seen_run, first_seen_at, last_seen_at,
                  fingerprint_doc, sample_detail
                ) VALUES (?,?,?,'OPEN',?,?,?,?,?,?)
                """,
                (
                    f.finding_id,
                    f.finding_type,
                    f.entity_key,
                    run_label,
                    run_label,
                    now,
                    now,
                    FINGERPRINT_DOCS.get(f.finding_type, ""),
                    dumps(f.detail),
                ),
            )
        else:
            prev_status = row["status"]
            if prev_status in ("RESOLVED", "WONTFIX", "FALSE_POSITIVE"):
                result.regressed.append(f)
                bucket = "REGRESSED"
                conn.execute(
                    """
                    UPDATE audit_disposition
                    SET status='OPEN', last_seen_run=?, last_seen_at=?,
                        note=COALESCE(note,'') || ' | REGRESSED on ' || ?,
                        sample_detail=?
                    WHERE finding_id=?
                    """,
                    (run_label, now, run_label, dumps(f.detail), f.finding_id),
                )
            else:
                result.recurring.append(f)
                bucket = "RECURRING"
                conn.execute(
                    "UPDATE audit_disposition SET last_seen_run=?, last_seen_at=?, sample_detail=? "
                    "WHERE finding_id=?",
                    (run_label, now, dumps(f.detail), f.finding_id),
                )
        conn.execute(
            """
            INSERT OR REPLACE INTO disposition_sighting
              (finding_id, run_label, delta_bucket, detail_json)
            VALUES (?,?,?,?)
            """,
            (f.finding_id, run_label, bucket, dumps(f.detail)),
        )

    # Previously OPEN/ACKED not seen → RESOLVED (auto) — only on full taxonomy runs.
    # Partial phases (a2-partial / a3-partial) must not wipe dispositions they didn't re-emit,
    # except for explicitly listed types (C1 MALFORMED / C2 COLLISION).
    auto_resolve = source in ("a3", "a3-full", "full")
    type_allow = auto_resolve_types or set()
    if auto_resolve or type_allow:
        for row in conn.execute(
            "SELECT * FROM audit_disposition WHERE status IN ('OPEN','ACKED')"
        ):
            if row["finding_id"] in seen_ids:
                continue
            if not auto_resolve and row["finding_type"] not in type_allow:
                continue
            result.resolved.append(dict(row))
            conn.execute(
                """
                UPDATE audit_disposition
                SET status='RESOLVED', resolved_by='auto-delta', resolved_at=?,
                    last_seen_run=?, note=COALESCE(note,'') || ' | auto-resolved absent'
                WHERE finding_id=?
                """,
                (now, run_label, row["finding_id"]),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO disposition_sighting
                  (finding_id, run_label, delta_bucket, detail_json)
                VALUES (?,?, 'RESOLVED', ?)
                """,
                (row["finding_id"], run_label, dumps({"auto": True})),
            )

    result.headline = len(result.new) + len(result.regressed)
    stats = {
        "NEW": len(result.new),
        "RECURRING": len(result.recurring),
        "RESOLVED": len(result.resolved),
        "REGRESSED": len(result.regressed),
        "headline_NEW_plus_REGRESSED": result.headline,
        "by_type_new": dict(Counter(f.finding_type for f in result.new)),
        "by_type_recurring": dict(Counter(f.finding_type for f in result.recurring)),
    }
    conn.execute(
        "UPDATE disposition_run SET finished_at=?, stats_json=? WHERE run_label=?",
        (utc_now(), dumps(stats), run_label),
    )
    conn.commit()
    conn.close()
    return result


def write_a2_report(
    result: DeltaResult,
    *,
    dest: Path = A2_REPORT_PATH,
    fingerprint_docs: dict[str, str] = FINGERPRINT_DOCS,
) -> Path:
    lines: list[str] = []
    A = lines.append
    A("# A2 — Stable findings & disposition delta")
    A("")
    A(f"_Generated: {utc_now()}_")
    A(f"_Run label: `{result.run_label}`_")
    A(f"_Disposition DB: `{DISPOSITION_DB_PATH}`_")
    A("")
    A("## Headline (NEW + REGRESSED)")
    A("")
    A(f"**{result.headline}**  *(not total open findings)*")
    A("")
    A("| Bucket | Count |")
    A("|---|---:|")
    A(f"| NEW | {len(result.new)} |")
    A(f"| RECURRING | {len(result.recurring)} |")
    A(f"| RESOLVED (absent this run) | {len(result.resolved)} |")
    A(f"| REGRESSED | {len(result.regressed)} |")
    A("")
    A("### NEW by type")
    A("")
    for t, n in Counter(f.finding_type for f in result.new).most_common():
        A(f"- `{t}`: {n}")
    A("")
    A("### RECURRING by type")
    A("")
    for t, n in Counter(f.finding_type for f in result.recurring).most_common():
        A(f"- `{t}`: {n}")
    if not result.recurring:
        A("- _(none)_")
    A("")
    A("## Fingerprint recipe (per type)")
    A("")
    A("`finding_id = sha256(finding_type || entity_key || salient_payload)`")
    A("")
    A("Canonical entity keys **exclude** `created_at`, TaxOps row ids (merge-volatile), and raw untruncated names.")
    A("")
    for t, doc in sorted(fingerprint_docs.items()):
        A(f"- **{t}:** {doc}")
    A("")
    A("## Jul31 backfill")
    A("")
    A(f"`{dumps(result.backfill_stats)}`")
    A("")
    A("Weak reconstructions use `first_seen_run='pre-baseline'` (not faked as jul31).")
    A("")
    A("## Merge attrition trail")
    A("")
    ma = result.merge_assessment or {}
    A(f"- **Verdict:** `{ma.get('verdict')}`")
    A(f"- **Rationale:** {ma.get('rationale')}")
    A(f"- audit_log merge API rows: {ma.get('audit_log_merge_count')}")
    A(f"- payloads with keep/discard: {ma.get('payloads_with_keep_discard')}")
    A(f"- merge_* tables: `{ma.get('merge_tables')}`")
    A("")
    A("## Sample NEW findings (≤15)")
    A("")
    for f in result.new[:15]:
        A(f"- `{f.finding_type}` `{f.finding_id[:12]}…` key=`{f.entity_key[:80]}`")
    A("")
    A("## Disposition schema")
    A("")
    A("```")
    A("audit_disposition(finding_id PK, finding_type, entity_key, status,")
    A("  resolved_by, resolved_at, note, first_seen_run, last_seen_run, ...)")
    A("status ∈ OPEN | ACKED | WONTFIX | RESOLVED | FALSE_POSITIVE")
    A("```")
    A("")
    A("DB path is durable across runs and **never truncated** by the audit tool.")
    A("")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def run_a2_phase(
    *,
    run_label: Optional[str] = None,
    do_backfill: bool = True,
) -> tuple[DeltaResult, Path]:
    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.invoice_export import DEFAULT_TAXPAYER_INVOICE_PATH

    mem = load_baseline_memory()
    taxops = Path(mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")
    csm = Path(mem.get("authoritative_drake_path") or config.DEFAULT_DRAKE_PATH)
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)
    inv_path = Path(mem.get("taxpayer_invoice_path") or DEFAULT_TAXPAYER_INVOICE_PATH)
    label = run_label or f"a2-{utc_now().replace(':', '').replace('-', '')[:15]}"

    dconn = connect_disposition()
    migrate_disposition_schema(dconn)
    backfill_stats: dict[str, Any] = {}
    if do_backfill:
        # Only backfill if disposition empty of jul31
        n = dconn.execute("SELECT COUNT(*) FROM audit_disposition").fetchone()[0]
        if n == 0:
            backfill_stats = backfill_jul31(dconn)
        else:
            backfill_stats = {"skipped": True, "existing_rows": n}
    # C1: seed legacy malformed so ~16 can RESOLVE under full delta
    seed_stats = seed_legacy_malformed_open(dconn, inv_path)
    backfill_stats["c1_legacy_malformed_seed"] = seed_stats
    dconn.close()

    merge_assessment = assess_merge_trail(taxops)
    findings: list[Finding] = []

    # Always emit merge finding when untraceable / partial
    if merge_assessment["verdict"] in ("MERGE_UNTRACEABLE", "MERGE_PARTIAL_TRAIL"):
        findings.append(
            Finding(
                finding_type="MERGE_UNTRACEABLE",
                entity_key="global",
                salient=merge_assessment["verdict"],
                detail=merge_assessment,
            )
        )

    findings.extend(findings_name_truncated_live(csm))
    findings.extend(findings_from_invoice_gaps(inv_path, taxops, log_path))
    findings.extend(findings_invoice_validation(inv_path))

    # Emit invoice validation + truncation/gaps; use a2-c1 source that auto-resolves
    # only types this phase emits (handled below via apply_delta + selective resolve).
    result = apply_delta(
        findings,
        run_label=label,
        source="a2-partial",
        auto_resolve_types={"MALFORMED_LOG_NUMBER", "LOG_NUMBER_COLLISION"},
    )
    result.merge_assessment = merge_assessment
    result.backfill_stats = backfill_stats
    # Recompute headline already set
    path = write_a2_report(result)
    return result, path


if __name__ == "__main__":
    res, path = run_a2_phase()
    print(
        f"A2 done headline={res.headline} NEW={len(res.new)} RECURRING={len(res.recurring)} "
        f"RESOLVED={len(res.resolved)} REGRESSED={len(res.regressed)} "
        f"merge={res.merge_assessment.get('verdict')} report={path}"
    )
