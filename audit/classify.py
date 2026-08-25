"""M5 — classify findings and characterize TY2025 excess returns."""

from __future__ import annotations

import sqlite3
from collections import Counter
from typing import Optional

from audit import config
from audit.db import connect_audit
from audit.util import dumps

# Fixed finding enum
FINDING_TYPES = (
    "MISSING_IN_TAXOPS",
    "PHANTOM_IN_TAXOPS",
    "DUPLICATE_CLIENT",
    "LOGGED_NOT_PREPARED",
    "PREPARED_NOT_LOGGED",
    "FIELD_MISMATCH",
    "NAME_TRUNCATED",
    "SPOUSE_MISSING",
    "SPOUSE_UNLINKED",
    "SPOUSE_AMBIGUOUS",
    "SPOUSE_STORE_DIVERGENCE",
    "NEEDS_HUMAN",
    "CONFIRMED_MATCH",
)

SEVERITY = {
    "NEEDS_HUMAN": 10,
    "DUPLICATE_CLIENT": 15,
    "SPOUSE_AMBIGUOUS": 20,
    "MISSING_IN_TAXOPS": 25,
    "PHANTOM_IN_TAXOPS": 30,
    "PREPARED_NOT_LOGGED": 35,
    "LOGGED_NOT_PREPARED": 40,
    "SPOUSE_MISSING": 45,
    "SPOUSE_UNLINKED": 50,
    "SPOUSE_STORE_DIVERGENCE": 55,
    "NAME_TRUNCATED": 60,
    "FIELD_MISMATCH": 70,
    "CONFIRMED_MATCH": 90,
}


def _ins_finding(conn, run_id, ftype, **kw):
    conn.execute(
        """
        INSERT INTO audit_finding (
          run_id, finding_type, subtype, severity, subject_kind, subject_id,
          tax_year, detail_json, source_refs
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            ftype,
            kw.get("subtype"),
            SEVERITY.get(ftype, 50),
            kw.get("subject_kind"),
            kw.get("subject_id"),
            kw.get("tax_year", config.PRIMARY_TAX_YEAR),
            dumps(kw.get("detail") or {}),
            dumps(kw.get("source_refs") or {}),
        ),
    )


def run_classify(audit_db, run_id: int) -> dict:
    conn = connect_audit(audit_db)
    try:
        # Matched Drake indiv → TaxOps
        matched_drake_taxops = {
            int(r["left_id"]): int(r["right_id"])
            for r in conn.execute(
                """
                SELECT left_id, right_id FROM audit_match
                WHERE run_id=? AND pair='DRAKE_TAXOPS' AND left_kind='drake'
                """,
                (run_id,),
            )
        }
        matched_log_taxops = {
            int(r["left_id"]): int(r["right_id"])
            for r in conn.execute(
                """
                SELECT left_id, right_id FROM audit_match
                WHERE run_id=? AND pair='LOG_TAXOPS' AND left_kind='log'
                """,
                (run_id,),
            )
        }
        matched_drake_log = {
            int(r["left_id"]): int(r["right_id"])
            for r in conn.execute(
                """
                SELECT left_id, right_id FROM audit_match
                WHERE run_id=? AND pair LIKE 'DRAKE_LOG:%' AND left_kind='drake'
                """,
                (run_id,),
            )
        }

        # Drake individuals
        for r in conn.execute(
            "SELECT id, source_row, client_name_raw, return_type, is_entity FROM stage_drake WHERE run_id=? AND dropped=0",
            (run_id,),
        ):
            did = int(r["id"])
            refs = {"drake_row": int(r["source_row"]), "type": r["return_type"]}
            if did in matched_drake_taxops:
                _ins_finding(
                    conn, run_id, "CONFIRMED_MATCH",
                    subject_kind="drake", subject_id=did, source_refs=refs,
                    detail={"taxops_stage_id": matched_drake_taxops[did]},
                )
            else:
                _ins_finding(
                    conn, run_id, "MISSING_IN_TAXOPS",
                    subject_kind="drake", subject_id=did, source_refs=refs,
                )
            if did not in matched_drake_log and not r["is_entity"]:
                _ins_finding(
                    conn, run_id, "PREPARED_NOT_LOGGED",
                    subject_kind="drake", subject_id=did, source_refs=refs,
                )

        # Log individuals YR 2025
        for r in conn.execute(
            """
            SELECT id, source_row, sheet_name, yr_norm, is_entity_sheet
            FROM stage_log WHERE run_id=? AND dropped=0 AND is_entity_sheet=0
            """,
            (run_id,),
        ):
            lid = int(r["id"])
            refs = {"log_sheet": r["sheet_name"], "log_row": int(r["source_row"])}
            if lid in matched_log_taxops:
                # avoid double CONFIRMED if also via drake — still ok
                _ins_finding(
                    conn, run_id, "CONFIRMED_MATCH",
                    subject_kind="log", subject_id=lid, source_refs=refs,
                    subtype="log_taxops",
                )
            else:
                if r["yr_norm"] == config.PRIMARY_TAX_YEAR or r["yr_norm"] is None:
                    _ins_finding(
                        conn, run_id, "LOGGED_NOT_PREPARED",
                        subject_kind="log", subject_id=lid, source_refs=refs,
                        detail={"yr_norm": r["yr_norm"]},
                    )

        # Truncation + spouse findings
        for r in conn.execute(
            "SELECT id, client_name_raw FROM stage_drake WHERE run_id=? AND dropped=0",
            (run_id,),
        ):
            name = r["client_name_raw"] or ""
            if len(name.rstrip()) >= 39:
                _ins_finding(
                    conn, run_id, "NAME_TRUNCATED",
                    subject_kind="drake", subject_id=int(r["id"]),
                    source_refs={"drake_id": int(r["id"])},
                    detail={"name_len": len(name.rstrip())},
                )

        for r in conn.execute(
            "SELECT * FROM audit_spouse WHERE run_id=?", (run_id,)
        ):
            if r["needs_review"]:
                _ins_finding(
                    conn, run_id, "NEEDS_HUMAN",
                    subject_kind="drake", subject_id=r["drake_stage_id"],
                    subtype="spouse_unrecovered",
                    detail={"provenance": r["provenance"], "class": r["spouse_class"]},
                )
            elif r["provenance"] == "INFERRED" and not r["spouse_last"]:
                _ins_finding(
                    conn, run_id, "SPOUSE_MISSING",
                    subject_kind="drake", subject_id=r["drake_stage_id"],
                )
            if r["notes"] and "conflict" in (r["notes"] or ""):
                _ins_finding(
                    conn, run_id, "SPOUSE_AMBIGUOUS",
                    subject_kind="drake", subject_id=r["drake_stage_id"],
                    detail={"notes": r["notes"]},
                )

        for r in conn.execute(
            "SELECT * FROM audit_spouse_store_div WHERE run_id=?", (run_id,)
        ):
            _ins_finding(
                conn, run_id, "SPOUSE_STORE_DIVERGENCE",
                subject_kind="taxops_client", subject_id=int(r["client_id"]),
                detail={"better_store": r["better_store"], "notes": r["notes"]},
            )

        # Duplicate clients in TaxOps: same lower name + last4
        dupes = conn.execute(
            """
            SELECT lower(last_name) ln, lower(coalesce(first_name,'')) fn,
                   coalesce(ssn_last4,'') ssn, COUNT(*) c,
                   GROUP_CONCAT(client_id) ids
            FROM stage_taxops_client WHERE run_id=?
            GROUP BY 1,2,3 HAVING COUNT(*) > 1
            """,
            (run_id,),
        ).fetchall()
        for d in dupes:
            _ins_finding(
                conn, run_id, "DUPLICATE_CLIENT",
                subject_kind="taxops_client",
                detail={"count": d["c"], "client_ids": d["ids"]},
            )

        # TY2025 gap: TaxOps returns not matched from Drake
        matched_taxops_stage_ids = set(matched_drake_taxops.values()) | set(
            matched_log_taxops.values()
        )
        # Map stage client id → client_id
        stage_client = {
            int(r["id"]): int(r["client_id"])
            for r in conn.execute(
                "SELECT id, client_id FROM stage_taxops_client WHERE run_id=?", (run_id,)
            )
        }
        matched_client_ids = {stage_client[s] for s in matched_taxops_stage_ids if s in stage_client}

        # Other-year clients
        other_year_clients = {
            int(r[0])
            for r in conn.execute(
                """
                SELECT DISTINCT client_id FROM stage_taxops_return
                WHERE run_id=? AND tax_year != 2025 AND tax_year IS NOT NULL
                """,
                (run_id,),
            )
        }

        cause_counts: Counter[str] = Counter()
        ty_rows = conn.execute(
            """
            SELECT * FROM stage_taxops_return WHERE run_id=? AND tax_year=2025
            """,
            (run_id,),
        ).fetchall()

        for r in ty_rows:
            cid = int(r["client_id"])
            has_log = 1 if (r["log_number"] not in (None, "", "0", 0)) else 0
            has_other = 1 if cid in other_year_clients else 0
            status = (r["client_status"] or "").strip().upper()

            if cid in matched_client_ids:
                cause = "matched_to_drake_or_log"
            elif status == "CANCELLED":
                cause = "cancelled"
            elif not has_log:
                cause = "no_log_number"
            elif has_other:
                cause = "multi_year_client_unmatched_name"
            elif status in ("PROCESSING", "HOLD", ""):
                cause = "unmatched_active_status"
            else:
                cause = "unmatched_other"

            # Plausible Other Office Svcs: no processor / odd status
            oos = 1 if (not r["processor"] and not has_log) else 0
            if oos and cause.startswith("unmatched"):
                cause = "other_office_plausible"

            cause_counts[cause] += 1
            conn.execute(
                """
                INSERT INTO audit_gap_ty2025 (
                  run_id, return_id, client_id, cause, client_status, has_log_number,
                  created_at, import_batch_id, has_other_year, other_office_plausible,
                  detail_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id, int(r["return_id"]), cid, cause, r["client_status"], has_log,
                    r["created_at"], r["import_batch_id"], has_other, oos,
                    dumps({"processor": r["processor"]}),
                ),
            )
            if cause != "matched_to_drake_or_log":
                _ins_finding(
                    conn, run_id, "PHANTOM_IN_TAXOPS",
                    subject_kind="taxops_return", subject_id=int(r["return_id"]),
                    subtype=cause,
                    detail={"cause": cause},
                    source_refs={"return_id": int(r["return_id"]), "client_id": cid},
                )

        conn.commit()
        total_gap = sum(cause_counts.values())
        return {
            "finding_counts": dict(
                Counter(
                    r[0]
                    for r in conn.execute(
                        "SELECT finding_type FROM audit_finding WHERE run_id=?", (run_id,)
                    )
                )
            ),
            "ty2025_gap_by_cause": dict(cause_counts),
            "ty2025_total": total_gap,
            "ty2025_expected": config.EXPECTED_TAXOPS_TY2025,
        }
    finally:
        conn.close()
