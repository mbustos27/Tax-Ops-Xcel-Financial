"""
Diagnostic: why 147 LOG↔TaxOps self-match failures dropped.

Instruments existing matcher (does not rewrite production match path permanently).
Re-runs LOG_TAXOPS with each hypothesized cause corrected individually.
Findings only — no TaxOps writes.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from audit.db import connect_audit, connect_taxops_readonly
from audit.match import (
    Side,
    _load_sides,
    _log_keys,
    _taxops_keys,
    match_sides,
)
from audit.normalizer import (
    full_normalized_string,
    keys_with_transposition,
    normalize_log_row,
    normalize_person,
)


@dataclass
class FailureCase:
    return_id: int
    batch1_row_number: int
    taxops_surname: str
    taxops_first_key: str
    batch1_surname: str
    batch1_first_key: str
    client_id: Optional[int] = None
    taxops_stage_id: Optional[int] = None
    log_stage_ids: list[int] = field(default_factory=list)
    causes: list[str] = field(default_factory=list)


def _load_failures() -> list[dict]:
    data = json.loads(Path(r"T:\audit\output\task2_log_churn.json").read_text(encoding="utf-8"))
    fails = data["part_b"]["self_match_failures"]
    return [
        f
        for f in fails
        if f["batch1_norm_surname"] == f["taxops_norm_surname"]
        and f["batch1_norm_first_key"] == f["taxops_norm_first_key"]
    ]


def _index_log_by_keys(log_sides: list[Side]) -> dict[tuple[str, str], list[Side]]:
    idx: dict[tuple[str, str], list[Side]] = defaultdict(list)
    for s in log_sides:
        for k in _log_keys(s):
            idx[k].append(s)
    return idx


def diagnose(audit_db: Path, run_id: int, snapshot: Path) -> dict:
    fails_raw = _load_failures()
    aconn = connect_audit(audit_db)
    tconn = connect_taxops_readonly(snapshot)

    try:
        sides = _load_sides(aconn, run_id)
        # Pools as matcher actually builds them
        log_all = sides["log"]
        log_indiv = [x for x in log_all if not x.is_entity]
        log_entity_flagged = [x for x in log_all if x.is_entity]
        taxops_clients = sides["taxops"]

        # Stage log details for filter analysis
        stage_logs = {
            int(r["id"]): dict(r)
            for r in aconn.execute(
                "SELECT * FROM stage_log WHERE run_id=? AND dropped=0", (run_id,)
            )
        }

        # Existing LOG_TAXOPS matches: left=log stage id, right=taxops stage (client) id
        existing_matches = list(
            aconn.execute(
                """
                SELECT left_id, right_id, tier, confidence
                FROM audit_match
                WHERE run_id=? AND pair='LOG_TAXOPS'
                """,
                (run_id,),
            )
        )
        matched_log_ids = {int(r["left_id"]) for r in existing_matches}
        matched_taxops_stage = {int(r["right_id"]) for r in existing_matches}
        # Which taxops client consumed which log
        log_consumed_by: dict[int, int] = {
            int(r["left_id"]): int(r["right_id"]) for r in existing_matches
        }

        stage_client = {
            int(r["id"]): int(r["client_id"])
            for r in aconn.execute(
                "SELECT id, client_id FROM stage_taxops_client WHERE run_id=?",
                (run_id,),
            )
        }
        client_to_stage = {cid: sid for sid, cid in stage_client.items()}

        # Returns for grain analysis
        returns_by_client: dict[int, list[dict]] = defaultdict(list)
        for r in aconn.execute(
            "SELECT * FROM stage_taxops_return WHERE run_id=?", (run_id,)
        ):
            returns_by_client[int(r["client_id"])].append(dict(r))

        # Map failure return_id → client_id
        fail_cases: list[FailureCase] = []
        for f in fails_raw:
            rid = int(f["return_id"])
            row = tconn.execute(
                "SELECT client_id FROM returns WHERE id=?", (rid,)
            ).fetchone()
            cid = int(row["client_id"]) if row else None
            fail_cases.append(
                FailureCase(
                    return_id=rid,
                    batch1_row_number=int(f["batch1_row_number"]),
                    taxops_surname=f["taxops_norm_surname"],
                    taxops_first_key=f["taxops_norm_first_key"],
                    batch1_surname=f["batch1_norm_surname"],
                    batch1_first_key=f["batch1_norm_first_key"],
                    client_id=cid,
                    taxops_stage_id=client_to_stage.get(cid) if cid else None,
                )
            )

        # Candidate log rows for each failure via name keys
        log_idx = _index_log_by_keys(log_all)
        log_idx_indiv = _index_log_by_keys(log_indiv)

        for fc in fail_cases:
            # Use stage client names
            if fc.taxops_stage_id:
                sc = aconn.execute(
                    "SELECT last_name, first_name FROM stage_taxops_client WHERE id=?",
                    (fc.taxops_stage_id,),
                ).fetchone()
                last = sc["last_name"] or ""
                first = sc["first_name"] or ""
            else:
                last, first = fc.taxops_surname, fc.taxops_first_key
            keys = set(keys_with_transposition(last, first))
            # also try surname/first_key as last/first
            keys.add((fc.taxops_surname, fc.taxops_first_key))

            cands_all = []
            seen = set()
            for k in keys:
                for s in log_idx.get(k, []):
                    if s.id not in seen:
                        seen.add(s.id)
                        cands_all.append(s)
            fc.log_stage_ids = [s.id for s in cands_all]

            # --- H4 Presence: in LOG_TAXOPS pool (indiv, not entity-flagged)? ---
            in_indiv_pool = any(s.id in {x.id for x in log_indiv} for s in cands_all)
            in_any_log = bool(cands_all)
            if not in_any_log:
                fc.causes.append("H4_absent_from_all_log_sides")
            elif not in_indiv_pool:
                # present but only as entity-flagged (blank FIRST etc.)
                fc.causes.append("H4_excluded_from_LOG_TAXOPS_pool")
                # why excluded?
                for s in cands_all:
                    st = stage_logs.get(s.id, {})
                    if s.is_entity:
                        if not (st.get("first_raw") or "").strip():
                            fc.causes.append("H4_blank_FIRST_routed_to_entity")
                        elif st.get("is_entity_sheet"):
                            fc.causes.append("H4_entity_sheet_row")
                        else:
                            fc.causes.append("H4_entity_flag_other")

            # --- H2 Pool YR: is candidate YR!=2025 filtered? (currently NOT filtered) ---
            # Record whether candidates are YR=2025
            for s in cands_all:
                st = stage_logs.get(s.id, {})
                yr = st.get("yr_norm")
                if yr is not None and yr != 2025:
                    fc.causes.append("H2_candidate_yr_not_2025")
                if not (st.get("processor") or "").strip():
                    fc.causes.append("H4_blank_PROCESSOR")  # not currently filtered, note only

            # --- H1 Grain: client matched a log row? ---
            if fc.taxops_stage_id and fc.taxops_stage_id in matched_taxops_stage:
                fc.causes.append("H1_client_matched_log")
                # Does this return's client have match while we care about return?
                rets = returns_by_client.get(fc.client_id or -1, [])
                ty2025 = [r for r in rets if r.get("tax_year") == 2025]
                if len(rets) > 1:
                    fc.causes.append("H1_multi_return_client")
                if len(ty2025) > 1:
                    fc.causes.append("H1_multi_ty2025_return_client")
            else:
                fc.causes.append("H1_client_did_NOT_match_log")

            # --- H3 Consumption: best candidate already assigned? ---
            for s in cands_all:
                if s.id in matched_log_ids:
                    other = log_consumed_by.get(s.id)
                    if other != fc.taxops_stage_id:
                        fc.causes.append("H3_candidate_consumed_by_other_client")
                    else:
                        fc.causes.append("H3_candidate_consumed_by_self")  # shouldn't if unmatched

            # If in pool, has keys, not consumed, client unmatched — pure miss
            if (
                in_indiv_pool
                and fc.taxops_stage_id not in matched_taxops_stage
                and not any(s.id in matched_log_ids for s in cands_all if s.id in {x.id for x in log_indiv})
            ):
                fc.causes.append("H_miss_despite_available_candidate")

            # Ambiguous multi-candidate on key
            indiv_cands = [s for s in cands_all if s.id in {x.id for x in log_indiv}]
            if len(indiv_cands) > 1:
                fc.causes.append("H3_ambiguous_multi_log_candidate")

        # --- Aggregate cause counts (multi-label) ---
        cause_counts = Counter()
        for fc in fail_cases:
            # dedupe causes per row for counting rows-with-cause
            for c in set(fc.causes):
                cause_counts[c] += 1

        # Pool description
        yr2025_indiv = sum(
            1
            for s in log_indiv
            if stage_logs.get(s.id, {}).get("yr_norm") == 2025
        )
        pool_info = {
            "LOG_TAXOPS_left_definition": "stage_log sides where is_entity==False",
            "is_entity_true_when": "is_entity_sheet OR (XCEL blank FIRST)",
            "pool_count_indiv": len(log_indiv),
            "pool_count_all_log_sides": len(log_all),
            "entity_flagged_count": len(log_entity_flagged),
            "yr2025_within_indiv_pool": yr2025_indiv,
            "YR_filter_applied": False,
            "PROCESSOR_filter_applied": False,
            "blank_FIRST_excluded_via_entity_flag": True,
            "right_side": "stage_taxops_CLIENT (client grain, not return)",
            "matching_cardinality": "one-to-one (each log row and each taxops client used at most once)",
            "existing_LOG_TAXOPS_matches": len(existing_matches),
            "baseline_rate": round(len(existing_matches) / len(log_indiv), 4)
            if log_indiv
            else 0,
            "baseline_left": len(log_indiv),
            "baseline_matched": len(existing_matches),
        }

        # H1 specifically among 147
        h1_client_matched = sum(1 for fc in fail_cases if "H1_client_matched_log" in fc.causes)
        h1_client_not = sum(1 for fc in fail_cases if "H1_client_did_NOT_match_log" in fc.causes)
        h1_multi = sum(1 for fc in fail_cases if "H1_multi_return_client" in fc.causes)

        grain_credit = {
            "match_stored_as": "log_stage_id → taxops_CLIENT_stage_id (pair=LOG_TAXOPS)",
            "return_level_match_exists": False,
            "among_147_client_DID_match": h1_client_matched,
            "among_147_client_did_NOT_match": h1_client_not,
            "among_147_multi_return_client": h1_multi,
            "note": (
                "Matching is client-grained. M5 unmatched-return logic credits a return "
                "as matched iff its client_id appears in matched clients from "
                "DRAKE_TAXOPS|LOG_TAXOPS. So grain alone cannot explain a return miss "
                "when the client also did not match — check H1_client_did_NOT_match."
            ),
        }

        presence = {
            "in_indiv_pool": sum(
                1
                for fc in fail_cases
                if "H4_excluded_from_LOG_TAXOPS_pool" not in fc.causes
                and "H4_absent_from_all_log_sides" not in fc.causes
            ),
            "excluded_from_pool": sum(
                1 for fc in fail_cases if "H4_excluded_from_LOG_TAXOPS_pool" in fc.causes
            ),
            "absent_entirely": sum(
                1 for fc in fail_cases if "H4_absent_from_all_log_sides" in fc.causes
            ),
            "blank_FIRST_routed": sum(
                1 for fc in fail_cases if "H4_blank_FIRST_routed_to_entity" in fc.causes
            ),
        }

        consumption = {
            "mode": "one-to-one greedy (log left iterates; first hit claims taxops client)",
            "direction": "LEFT=log rows claim RIGHT=taxops clients",
            "candidate_consumed_by_other": sum(
                1
                for fc in fail_cases
                if "H3_candidate_consumed_by_other_client" in fc.causes
            ),
            "ambiguous_multi_candidate": sum(
                1
                for fc in fail_cases
                if "H3_ambiguous_multi_log_candidate" in fc.causes
            ),
            "available_but_still_missed": sum(
                1
                for fc in fail_cases
                if "H_miss_despite_available_candidate" in fc.causes
            ),
        }

        # ── Individual corrective re-runs ─────────────────────────────────
        baseline_matches, _, _ = match_sides(
            log_indiv, taxops_clients, _taxops_keys, require_same_entity_flag=False
        )
        baseline = {
            "left": len(log_indiv),
            "matched": len(baseline_matches),
            "rate": round(len(baseline_matches) / len(log_indiv), 4) if log_indiv else 0,
        }

        experiments = {}

        # Fix H2: restrict log left to YR=2025 only (tests whether WRONG direction —
        # user hypothesized pool too small; we currently have NO yr filter, so
        # applying YR=2025 shrinks pool. Also test opposite: match taxops returns
        # TY2025 to YR=2025 log — grain+year alignment.)
        log_yr25 = [
            s
            for s in log_indiv
            if stage_logs.get(s.id, {}).get("yr_norm") == 2025
        ]
        m_h2, _, _ = match_sides(
            log_yr25, taxops_clients, _taxops_keys, require_same_entity_flag=False
        )
        experiments["H2_restrict_log_YR2025"] = {
            "left": len(log_yr25),
            "matched": len(m_h2),
            "rate": round(len(m_h2) / len(log_yr25), 4) if log_yr25 else 0,
            "delta_matched_vs_baseline": len(m_h2) - baseline["matched"],
            "delta_rate_vs_baseline": round(
                (len(m_h2) / len(log_yr25) if log_yr25 else 0) - baseline["rate"], 4
            ),
            "note": "Current code does NOT filter YR; this APPLIES a YR=2025 filter (shrinks pool).",
        }

        # Fix H4: include blank-FIRST (entity-flagged XCEL) back into person pool
        log_plus_blank = list(log_indiv) + [
            s
            for s in log_entity_flagged
            if not stage_logs.get(s.id, {}).get("is_entity_sheet")
            and (stage_logs.get(s.id, {}).get("sheet_name") or "")
            .upper()
            .startswith("XCEL")
        ]
        m_h4, _, _ = match_sides(
            log_plus_blank, taxops_clients, _taxops_keys, require_same_entity_flag=False
        )
        experiments["H4_include_blank_FIRST"] = {
            "left": len(log_plus_blank),
            "matched": len(m_h4),
            "rate": round(len(m_h4) / len(log_plus_blank), 4) if log_plus_blank else 0,
            "delta_matched_vs_baseline": len(m_h4) - baseline["matched"],
            "delta_rate_vs_baseline": round(
                (len(m_h4) / len(log_plus_blank) if log_plus_blank else 0)
                - baseline["rate"],
                4,
            ),
        }

        # Fix H3a: flip direction (taxops claims log) — same 1:1, different greed order
        m_h3_flip, _, _ = match_sides(
            taxops_clients, log_indiv, _log_keys, require_same_entity_flag=False
        )
        experiments["H3_flip_direction_taxops_claims_log"] = {
            "left_taxops_clients": len(taxops_clients),
            "matched": len(m_h3_flip),
            "rate_over_taxops": round(len(m_h3_flip) / len(taxops_clients), 4)
            if taxops_clients
            else 0,
            "rate_over_log_pool": round(len(m_h3_flip) / len(log_indiv), 4)
            if log_indiv
            else 0,
            "delta_matched_vs_baseline": len(m_h3_flip) - baseline["matched"],
            "note": (
                "Flips greedy order: each taxops client claims a log row. "
                "Same 1:1 cardinality; tests whether log-iteration order starved clients."
            ),
        }

        # Fix H3b: many-to-one — one log row may satisfy many taxops clients
        # (do not consume the log / right when taxops is left)
        def match_no_consume_right(lefts, rights, right_key_fn):
            from audit.match import DEFAULT_TIERS

            matches = []
            for left in lefts:
                pool = list(rights)
                hit = None
                for _name, fn in DEFAULT_TIERS:
                    hit = fn(left, pool, right_key_fn)
                    if hit:
                        break
                if hit:
                    right, tier, conf = hit
                    matches.append((left, right, tier, conf))
            return matches

        m_h3_many = match_no_consume_right(taxops_clients, log_indiv, _log_keys)
        experiments["H3_many_to_one_no_log_consume"] = {
            "left_taxops_clients": len(taxops_clients),
            "matched": len(m_h3_many),
            "rate_over_taxops": round(len(m_h3_many) / len(taxops_clients), 4)
            if taxops_clients
            else 0,
            "rate_over_log_pool_note": "denominator unchanged; matches can exceed unique logs",
            "delta_matched_vs_baseline": len(m_h3_many) - baseline["matched"],
            "unique_logs_used": len({m[1].id for m in m_h3_many}),
            "note": (
                "TaxOps-as-left; log rows are NOT consumed. Tests whether 1:1 "
                "starvation (duplicate names / shared log key) explains the 147."
            ),
        }
        m_h3 = m_h3_flip  # for rescued_of_147 block below

        # Fix H1: match at RETURN grain for TY2025 — build taxops return sides
        taxops_returns: list[Side] = []
        for r in aconn.execute(
            """
            SELECT ret.id as rid, ret.client_id, ret.tax_year,
                   c.last_name, c.first_name, c.ssn_last4, c.id as stage_client_id
            FROM stage_taxops_return ret
            JOIN stage_taxops_client c
              ON c.client_id = ret.client_id AND c.run_id = ret.run_id
            WHERE ret.run_id=? AND ret.tax_year=2025
            """,
            (run_id,),
        ):
            taxops_returns.append(
                Side(
                    kind="taxops_return",
                    id=int(r["rid"]),
                    last=r["last_name"] or "",
                    first=r["first_name"] or "",
                    last4=r["ssn_last4"],
                )
            )

        # Log YR=2025 ↔ TaxOps TY2025 returns (aligned grain+year)
        log_yr25_for_h1 = log_yr25
        m_h1, _, _ = match_sides(
            log_yr25_for_h1,
            taxops_returns,
            lambda s: normalize_person(s.last or "", s.first or "").match_keys,
            require_same_entity_flag=False,
        )
        experiments["H1_return_grain_TY2025_vs_log_YR2025"] = {
            "left_log_yr25": len(log_yr25_for_h1),
            "right_taxops_ty2025_returns": len(taxops_returns),
            "matched": len(m_h1),
            "rate_over_log": round(len(m_h1) / len(log_yr25_for_h1), 4)
            if log_yr25_for_h1
            else 0,
            "rate_over_returns": round(len(m_h1) / len(taxops_returns), 4)
            if taxops_returns
            else 0,
            "unmatched_ty2025_returns": len(taxops_returns) - len(m_h1),
            "note": (
                "Return-grained match: each TY2025 return claims a YR=2025 log row. "
                "Compares to client-grained baseline."
            ),
        }

        # How many of the 147 get rescued by each experiment?
        fail_stage_ids = {fc.taxops_stage_id for fc in fail_cases if fc.taxops_stage_id}
        fail_return_ids = {fc.return_id for fc in fail_cases}

        def rescued_clients(matches, left_is_log=True):
            """Count failure clients that appear in matches."""
            if left_is_log:
                # match: log→taxops stage id
                rights = {m[1].id for m in matches}
                return sum(1 for sid in fail_stage_ids if sid in rights)
            else:
                lefts = {m[0].id for m in matches}
                return sum(1 for sid in fail_stage_ids if sid in lefts)

        experiments["H3_flip_direction_taxops_claims_log"]["rescued_of_147"] = (
            rescued_clients(m_h3, left_is_log=False)
        )
        experiments["H4_include_blank_FIRST"]["rescued_of_147"] = rescued_clients(
            m_h4, left_is_log=True
        )
        experiments["H2_restrict_log_YR2025"]["rescued_of_147"] = rescued_clients(
            m_h2, left_is_log=True
        )
        # H1 return grain — rescue by return id
        matched_returns = {m[1].id for m in m_h1}
        experiments["H1_return_grain_TY2025_vs_log_YR2025"]["rescued_of_147"] = sum(
            1 for rid in fail_return_ids if rid in matched_returns
        )

        # Also: client-grain but taxops-as-left with only the 147's clients
        # (already covered by H3 flip)

        return {
            "n_failures_analyzed": len(fail_cases),
            "pool": pool_info,
            "baseline_rerun": baseline,
            "H1_grain": grain_credit,
            "H2_pool": {
                "hypothesis": "pool capped at YR=25 (~1048)",
                "actual_indiv_pool": len(log_indiv),
                "yr2025_in_pool": yr2025_indiv,
                "YR_filter_in_current_code": False,
                "verdict": (
                    "H2 as stated is FALSE for current code — no YR filter; "
                    f"indiv pool is {len(log_indiv)} (larger than 1048). "
                    "The 1413−1048=365 ceiling is a hypothetical if YR were filtered, "
                    "not the live defect."
                ),
            },
            "H3_consumption": consumption,
            "H4_presence": presence,
            "cause_counts_multilabel": dict(cause_counts),
            "experiments": experiments,
        }
    finally:
        aconn.close()
        tconn.close()


if __name__ == "__main__":
    dbs = sorted(Path(r"T:\audit").glob("audit_*.sqlite"), key=lambda p: p.stat().st_mtime)
    db = dbs[-1]
    conn = connect_audit(db)
    run_id = int(conn.execute("SELECT MAX(id) FROM audit_run").fetchone()[0])
    conn.close()
    snap = Path(r"T:\audit\snapshots\taxops_snapshot_20260731.sqlite")
    out = diagnose(db, run_id, snap)
    Path(r"T:\audit\output\matcher_defect_diag.json").write_text(
        json.dumps(out, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(out, indent=2, default=str))
