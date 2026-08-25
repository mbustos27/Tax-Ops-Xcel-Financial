"""M7 — residual measurement (no LLM recommendations)."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from audit.db import connect_audit
from audit.normalizer import normalize_drake_client_name, normalize_log_row
from audit.util import dumps


def _characterize(conn, run_id: int, left_kind: str, left_id: int) -> str:
    if left_kind == "drake":
        row = conn.execute(
            "SELECT client_name_raw, is_entity FROM stage_drake WHERE id=?", (left_id,)
        ).fetchone()
        if not row:
            return "missing_stage_row"
        n = normalize_drake_client_name(row["client_name_raw"] or "")
        if row["is_entity"]:
            return "entity_unmatched"
        if n.truncated:
            return "truncated_name"
        if not n.first_key or not n.surname_full:
            return "incomplete_parse"
        if n.is_joint:
            return "joint_name_unmatched"
        return "no_key_overlap"
    if left_kind == "log":
        row = conn.execute(
            "SELECT last_raw, first_raw, is_entity_sheet FROM stage_log WHERE id=?",
            (left_id,),
        ).fetchone()
        if not row:
            return "missing_stage_row"
        if row["is_entity_sheet"]:
            return "entity_sheet_unmatched"
        n = normalize_log_row(row["last_raw"] or "", row["first_raw"] or "")
        if not n.first_key:
            return "missing_first_token"
        return "no_key_overlap"
    return "unknown"


def run_residual(audit_db: Path, run_id: int) -> dict:
    conn = connect_audit(audit_db)
    try:
        # Refresh failure_cause with characterization
        rows = conn.execute(
            "SELECT id, pair, left_kind, left_id FROM audit_residual WHERE run_id=?",
            (run_id,),
        ).fetchall()
        causes: Counter[str] = Counter()
        for r in rows:
            cause = _characterize(conn, run_id, r["left_kind"], int(r["left_id"]))
            causes[cause] += 1
            conn.execute(
                "UPDATE audit_residual SET failure_cause=?, detail_json=? WHERE id=?",
                (cause, dumps({"pair": r["pair"]}), int(r["id"])),
            )
        conn.commit()

        # Final Drake↔Log indiv rate from matches
        m = conn.execute(
            """
            SELECT COUNT(*) FROM audit_match
            WHERE run_id=? AND pair='DRAKE_LOG:indiv'
            """,
            (run_id,),
        ).fetchone()[0]
        d = conn.execute(
            """
            SELECT COUNT(*) FROM stage_drake
            WHERE run_id=? AND dropped=0 AND is_entity=0
            """,
            (run_id,),
        ).fetchone()[0]
        rate = (m / d) if d else 0.0
        return {
            "drake_log_indiv_matched": m,
            "drake_log_indiv_total": d,
            "drake_log_indiv_rate": round(rate, 4),
            "residual_total": len(rows),
            "residual_by_cause": dict(causes),
        }
    finally:
        conn.close()
