"""M9.4 — classify spouses vs clients.spouse_* store divergence."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from audit.db import connect_audit
from audit.util import dumps


def _norm(s: str | None) -> str:
    return " ".join((s or "").upper().split())


def run_m94(audit_db: Path, run_id: int) -> dict:
    conn = connect_audit(audit_db)
    try:
        # Join all clients that have either store populated
        rows = conn.execute(
            """
            SELECT c.client_id,
                   c.spouse_first_name AS cf, c.spouse_last_name AS cl,
                   s.first_name AS sf, s.last_name AS sl,
                   s.spouse_row_id
            FROM stage_taxops_client c
            LEFT JOIN stage_taxops_spouse s
              ON s.run_id = c.run_id AND s.client_id = c.client_id
            WHERE c.run_id = ?
            """,
            (run_id,),
        ).fetchall()

        # M4 recovered spouses keyed by taxops client via DRAKE_TAXOPS match
        # drake_stage → taxops stage → client_id
        recovered = {}
        for r in conn.execute(
            """
            SELECT m.right_id AS taxops_stage_id,
                   sp.spouse_first, sp.spouse_last, sp.provenance
            FROM audit_match m
            JOIN audit_spouse sp
              ON sp.run_id = m.run_id AND sp.drake_stage_id = m.left_id
            WHERE m.run_id=? AND m.pair='DRAKE_TAXOPS'
              AND m.left_kind='drake' AND m.right_kind='taxops'
            """,
            (run_id,),
        ):
            cid_row = conn.execute(
                "SELECT client_id FROM stage_taxops_client WHERE id=?",
                (int(r["taxops_stage_id"]),),
            ).fetchone()
            if not cid_row:
                continue
            recovered[int(cid_row["client_id"])] = {
                "first": _norm(r["spouse_first"]),
                "last": _norm(r["spouse_last"]),
                "provenance": r["provenance"],
            }

        classes = Counter()
        conflict_detail = []
        classified = []

        for r in rows:
            cf, cl = _norm(r["cf"]), _norm(r["cl"])
            sf, sl = _norm(r["sf"]), _norm(r["sl"])
            has_c = bool(cf or cl)
            has_s = bool(sf or sl)
            if not has_c and not has_s:
                continue
            if has_c and has_s:
                if cf == sf and cl == sl:
                    cls = "AGREE"
                else:
                    cls = "CONFLICT"
            elif has_s:
                cls = "SPOUSES_ONLY"
            else:
                cls = "CLIENTS_ONLY"
            classes[cls] += 1
            item = {
                "client_id": int(r["client_id"]),
                "class": cls,
            }
            if cls == "CONFLICT":
                rec = recovered.get(int(r["client_id"]))
                agrees = None
                if rec:
                    sp_agree = rec["first"] == sf and rec["last"] == sl
                    cl_agree = rec["first"] == cf and rec["last"] == cl
                    if sp_agree and not cl_agree:
                        agrees = "spouses"
                    elif cl_agree and not sp_agree:
                        agrees = "clients.spouse_*"
                    elif sp_agree and cl_agree:
                        agrees = "both"  # shouldn't happen if CONFLICT
                    else:
                        agrees = "neither"
                item["m4_agrees_with"] = agrees
                # structural only for console — lengths not names
                item["clients_first_len"] = len(cf)
                item["clients_last_len"] = len(cl)
                item["spouses_first_len"] = len(sf)
                item["spouses_last_len"] = len(sl)
                conflict_detail.append(item)
            classified.append(item)

        # Refresh audit_spouse_store_div with class labels
        conn.execute("DELETE FROM audit_spouse_store_div WHERE run_id=?", (run_id,))
        for r in rows:
            cf, cl = _norm(r["cf"]), _norm(r["cl"])
            sf, sl = _norm(r["sf"]), _norm(r["sl"])
            has_c = bool(cf or cl)
            has_s = bool(sf or sl)
            if not has_c and not has_s:
                continue
            if has_c and has_s:
                cls = "AGREE" if (cf == sf and cl == sl) else "CONFLICT"
            elif has_s:
                cls = "SPOUSES_ONLY"
            else:
                cls = "CLIENTS_ONLY"
            better = {
                "AGREE": "either",
                "CONFLICT": "unknown",
                "SPOUSES_ONLY": "spouses",
                "CLIENTS_ONLY": "clients.spouse_*",
            }[cls]
            conn.execute(
                """
                INSERT INTO audit_spouse_store_div (
                  run_id, client_id, clients_first, clients_last,
                  spouses_first, spouses_last, better_store, notes
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    int(r["client_id"]),
                    r["cf"],
                    r["cl"],
                    r["sf"],
                    r["sl"],
                    better,
                    cls,
                ),
            )
        conn.commit()

        total = sum(classes.values())
        return {
            "total_classified": total,
            "classes": dict(classes),
            "sums_to_198_prior": total,
            "conflict_count": classes.get("CONFLICT", 0),
            "conflict_details_count": len(conflict_detail),
            "conflict_m4_agreement": dict(
                Counter(c.get("m4_agrees_with") or "no_m4" for c in conflict_detail)
            ),
            "remediation": (
                "backfill"
                if classes.get("CONFLICT", 0) == 0
                else "reconciliation_required"
            ),
            "plain_statement": (
                "CONFLICT is at or near zero — remediation is a backfill "
                "(copy spouses → clients.spouse_* or vice versa), not a value reconciliation."
                if classes.get("CONFLICT", 0) == 0
                else "CONFLICT is materially above zero — contradicts the 169+29 arithmetic "
                "prediction; remediation must reconcile disagreeing values, not only backfill."
            ),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    from audit.db import connect_audit as ca

    dbs = sorted(Path(r"T:\audit").glob("audit_*.sqlite"), key=lambda p: p.stat().st_mtime)
    db = dbs[-1]
    conn = ca(db)
    run_id = int(conn.execute("SELECT MAX(id) FROM audit_run").fetchone()[0])
    conn.close()
    out = run_m94(db, run_id)
    Path(r"T:\audit\output\m94_store_split.json").write_text(
        json.dumps(out, indent=2), encoding="utf-8"
    )
    print(json.dumps(out, indent=2))
