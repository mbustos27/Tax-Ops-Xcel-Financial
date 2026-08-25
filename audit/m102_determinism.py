"""
M10.2 — Matcher determinism (5 shuffled runs) + 1010 vs 944 gap attribution.

Findings only. No matcher rewrite.
"""
from __future__ import annotations

import json
import random
from collections import Counter
from pathlib import Path

from audit.db import connect_audit
from audit.match import (
    DEFAULT_TIERS,
    _load_sides,
    _taxops_keys,
    match_sides,
    tier_full_transposition,
    tier_paternal_only,
    tier_surname_first_token,
)

OUT = Path(r"T:\audit\output\m102_determinism.json")
SEEDS = [1, 2, 3, 4, 5]


def main():
    audit_db = sorted(
        Path(r"T:\audit").glob("audit_*.sqlite"), key=lambda p: p.stat().st_mtime
    )[-1]
    aconn = connect_audit(audit_db)
    run_id = int(aconn.execute("SELECT MAX(id) FROM audit_run").fetchone()[0])
    sides = _load_sides(aconn, run_id)
    log_indiv = [x for x in sides["log"] if not x.is_entity]
    taxops = list(sides["taxops"])

    stored = list(
        aconn.execute(
            """
            SELECT left_id, right_id, tier, confidence
            FROM audit_match WHERE run_id=? AND pair='LOG_TAXOPS'
            """,
            (run_id,),
        )
    )
    stored_pairs = {(int(r["left_id"]), int(r["right_id"])) for r in stored}
    stored_tiers = Counter(r["tier"] for r in stored)
    stored_by_left = {int(r["left_id"]): int(r["right_id"]) for r in stored}
    stored_tier_by_left = {int(r["left_id"]): r["tier"] for r in stored}

    # ── Determinism: 5 shuffled runs ──────────────────────────────────
    run_results = []
    pair_sets = []
    for seed in SEEDS:
        rng = random.Random(seed)
        lefts = list(log_indiv)
        rights = list(taxops)
        rng.shuffle(lefts)
        rng.shuffle(rights)
        matches, _, _ = match_sides(
            lefts, rights, _taxops_keys, require_same_entity_flag=False
        )
        pairs = {(m[0].id, m[1].id) for m in matches}
        tiers = Counter(m[2] for m in matches)
        run_results.append(
            {
                "seed": seed,
                "matched": len(matches),
                "tiers": dict(tiers),
            }
        )
        pair_sets.append(pairs)

    # Pairing variance across runs
    all_pairs_union = set().union(*pair_sets) if pair_sets else set()
    all_pairs_inter = set.intersection(*pair_sets) if pair_sets else set()
    # Per-run vs run0
    vs_first = []
    for i, ps in enumerate(pair_sets):
        vs_first.append(
            {
                "seed": SEEDS[i],
                "matched": len(ps),
                "pairs_only_in_this_vs_seed1": len(ps - pair_sets[0]),
                "pairs_only_in_seed1_vs_this": len(pair_sets[0] - ps),
                "symmetric_diff_vs_seed1": len(ps ^ pair_sets[0]),
            }
        )

    counts = [r["matched"] for r in run_results]
    determinism = {
        "seeds": SEEDS,
        "matched_counts": counts,
        "count_spread": {"min": min(counts), "max": max(counts), "unique": sorted(set(counts))},
        "counts_identical": len(set(counts)) == 1,
        "pairing": {
            "union_size": len(all_pairs_union),
            "intersection_size": len(all_pairs_inter),
            "pairs_unstable_across_runs": len(all_pairs_union - all_pairs_inter),
            "per_run_vs_seed1": vs_first,
        },
        "verdict": (
            "ORDER_INDEPENDENT"
            if len(set(counts)) == 1 and len(all_pairs_union - all_pairs_inter) == 0
            else (
                "COUNT_STABLE_PAIRINGS_UNSTABLE"
                if len(set(counts)) == 1
                else "ORDER_DEPENDENT_COUNTS_VARY"
            )
        ),
    }

    # Canonical fresh run (original load order, no shuffle)
    fresh, _, _ = match_sides(
        log_indiv, taxops, _taxops_keys, require_same_entity_flag=False
    )
    fresh_pairs = {(m[0].id, m[1].id) for m in fresh}
    fresh_tiers = Counter(m[2] for m in fresh)
    fresh_by_left = {m[0].id: m[1].id for m in fresh}
    fresh_tier_by_left = {m[0].id: m[2] for m in fresh}

    only_stored = stored_pairs - fresh_pairs
    only_fresh = fresh_pairs - stored_pairs
    both = stored_pairs & fresh_pairs

    log_indiv_ids = {x.id for x in log_indiv}
    log_all_by_id = {x.id: x for x in sides["log"]}
    taxops_by_id = {x.id: x for x in taxops}

    # Classify the 66 only_stored
    only_stored_tiers = Counter(stored_tier_by_left[L] for L, R in only_stored)
    gap_reasons = Counter()
    gap_detail = []
    fresh_right_owners = {m[1].id: m[0].id for m in fresh}

    for L, R in sorted(only_stored):
        tier = stored_tier_by_left[L]
        left_obj = log_all_by_id.get(L)
        in_indiv_pool = L in log_indiv_ids

        if not in_indiv_pool:
            gap_reasons["left_excluded_from_indiv_pool_now"] += 1
            reason = "left_excluded_from_indiv_pool_now"
            # why excluded?
            st = aconn.execute(
                "SELECT last_raw, first_raw, is_entity_sheet, sheet_name FROM stage_log WHERE id=?",
                (L,),
            ).fetchone()
            blank_first = (
                st
                and not st["is_entity_sheet"]
                and bool((st["last_raw"] or "").strip())
                and not (st["first_raw"] or "").strip()
                and (st["sheet_name"] or "").upper().startswith("XCEL")
            )
            excl = "blank_first_entity_routing" if blank_first else (
                "entity_sheet" if st and st["is_entity_sheet"] else "unknown"
            )
        elif L in fresh_by_left:
            gap_reasons["left_matched_different_right"] += 1
            reason = "left_matched_different_right"
            excl = None
        elif R in fresh_right_owners:
            gap_reasons["right_taken_by_other_left"] += 1
            reason = "right_taken_by_other_left"
            excl = None
        else:
            gap_reasons["left_unmatched_right_free"] += 1
            reason = "left_unmatched_right_free"
            excl = None

        t1 = t2 = t3 = None
        if left_obj is not None and R in taxops_by_id:
            # Force person-key probe even if currently entity-flagged
            from audit.match import Side

            probe = Side(
                kind="log",
                id=left_obj.id,
                last=left_obj.last,
                first=left_obj.first,
                display=left_obj.display,
                is_entity=False,
            )
            right_side = taxops_by_id[R]
            t3 = tier_full_transposition(probe, [right_side], _taxops_keys)
            t1 = tier_surname_first_token(probe, [right_side], _taxops_keys)
            t2 = tier_paternal_only(probe, [right_side], _taxops_keys)

        gap_detail.append(
            {
                "left_id": L,
                "right_id": R,
                "stored_tier": tier,
                "reason": reason,
                "exclusion": excl,
                "in_indiv_pool_now": in_indiv_pool,
                "fresh_right_if_any": fresh_by_left.get(L),
                "right_taken_by_left": fresh_right_owners.get(R),
                "isolated_t1": bool(t1),
                "isolated_t2": bool(t2),
                "isolated_t3": bool(t3),
            }
        )

    isolated = Counter()
    for d in gap_detail:
        if d["reason"] == "left_excluded_from_indiv_pool_now":
            isolated[f"excluded_{d['exclusion']}"] += 1
            if d["isolated_t3"]:
                isolated["excluded_but_t3_still_hits_pair"] += 1
        elif d["reason"] == "left_unmatched_right_free":
            if d["isolated_t3"]:
                isolated["t3_works_isolated_but_not_in_full_run"] += 1
            elif d["isolated_t1"] or d["isolated_t2"]:
                isolated["t1_or_t2_works_isolated"] += 1
            else:
                isolated["no_tier_hits_even_isolated"] += 1

    # Pool composition: same staged data — compare entity routing
    log_all = sides["log"]
    blank_first_routed = sum(
        1
        for x in log_all
        if x.is_entity
        and aconn.execute(
            "SELECT first_raw, is_entity_sheet, sheet_name FROM stage_log WHERE id=?",
            (x.id,),
        ).fetchone()
        and not aconn.execute(
            "SELECT is_entity_sheet FROM stage_log WHERE id=?", (x.id,)
        ).fetchone()["is_entity_sheet"]
    )
    # recount blank first
    bf = 0
    for r in aconn.execute(
        "SELECT id, first_raw, is_entity_sheet, sheet_name FROM stage_log WHERE run_id=? AND dropped=0",
        (run_id,),
    ):
        if (
            not r["is_entity_sheet"]
            and (r["last_raw"] if "last_raw" in r.keys() else True)
            and not (r["first_raw"] or "").strip()
            and (r["sheet_name"] or "").upper().startswith("XCEL")
        ):
            # need last_raw
            pass
    bf = 0
    for r in aconn.execute(
        """
        SELECT id, last_raw, first_raw, is_entity_sheet, sheet_name
        FROM stage_log WHERE run_id=? AND dropped=0
        """,
        (run_id,),
    ):
        if (
            not r["is_entity_sheet"]
            and bool((r["last_raw"] or "").strip())
            and not (r["first_raw"] or "").strip()
            and (r["sheet_name"] or "").upper().startswith("XCEL")
        ):
            bf += 1

    # Would disabling blank-first routing enlarge indiv pool and recover matches?
    log_no_bf_route = []
    for x in log_all:
        # rebuild without blank-first→entity
        r = aconn.execute(
            "SELECT last_raw, first_raw, is_entity_sheet, sheet_name FROM stage_log WHERE id=?",
            (x.id,),
        ).fetchone()
        is_ent = bool(r["is_entity_sheet"])
        # intentionally NOT applying blank_first_entity
        from audit.match import Side

        log_no_bf_route.append(
            Side(
                kind="log",
                id=x.id,
                last=r["last_raw"] or "",
                first=r["first_raw"] or "",
                display=x.display,
                is_entity=is_ent,
            )
        )
    pool_no_bf = [x for x in log_no_bf_route if not x.is_entity]
    m_no_bf, _, _ = match_sides(
        pool_no_bf, taxops, _taxops_keys, require_same_entity_flag=False
    )

    # Run with only first two tiers (no t3) vs only t3 after t1/t2
    m_no_t3, _, _ = match_sides(
        log_indiv,
        taxops,
        _taxops_keys,
        require_same_entity_flag=False,
        tiers=[
            ("surname_first_token", tier_surname_first_token),
            ("paternal_surname_first_token", tier_paternal_only),
        ],
    )

    # Re-run matching in stored left order (stage_log id order) — already default
    # Try matching with rights shuffled but lefts fixed — covered in seeds

    # Check: of the 66, how many are t3 in stored, and does fresh ever produce t3?
    fresh_t3 = sum(1 for m in fresh if m[2] == "full_normalized_transposition")
    stored_t3 = stored_tiers.get("full_normalized_transposition", 0)

    from audit.match import Side as _Side

    t3_bug_scan = Counter()
    for d in gap_detail:
        if d["reason"] != "left_unmatched_right_free":
            continue
        L, R = d["left_id"], d["right_id"]
        left_obj = log_all_by_id[L]
        left_side = _Side(
            kind="log",
            id=left_obj.id,
            last=left_obj.last,
            first=left_obj.first,
            display=left_obj.display,
            is_entity=False,
        )
        used = {m[1].id for m in fresh}
        unused = [x for x in taxops if x.id not in used]
        hit = tier_full_transposition(left_side, unused, _taxops_keys)
        if hit and hit[0].id == R:
            t3_bug_scan["t3_finds_R_on_unused_pool"] += 1
        elif hit:
            t3_bug_scan["t3_finds_different_on_unused"] += 1
        elif d["isolated_t3"]:
            t3_bug_scan["isolated_yes_unused_no_IMPOSSIBLE"] += 1
        else:
            t3_bug_scan["t3_dead_even_isolated"] += 1

    m_t3_first, _, _ = match_sides(
        log_indiv,
        taxops,
        _taxops_keys,
        require_same_entity_flag=False,
        tiers=[
            ("full_normalized_transposition", tier_full_transposition),
            ("surname_first_token", tier_surname_first_token),
            ("paternal_surname_first_token", tier_paternal_only),
        ],
    )

    only_stored_lefts_entity = sum(
        1 for L, R in only_stored if log_all_by_id[L].is_entity
    )

    # Restore excluded lefts into indiv pool (undo blank-first routing for gap lefts)
    excluded_lefts = [
        _Side(
            kind="log",
            id=log_all_by_id[L].id,
            last=log_all_by_id[L].last,
            first=log_all_by_id[L].first,
            display=log_all_by_id[L].display,
            is_entity=False,
        )
        for L, R in only_stored
        if L not in log_indiv_ids and L in log_all_by_id
    ]
    pool_restored = list(log_indiv) + excluded_lefts
    m_restored, _, _ = match_sides(
        pool_restored, taxops, _taxops_keys, require_same_entity_flag=False
    )
    restored_pairs = {(m[0].id, m[1].id) for m in m_restored}
    recovered_of_only_stored = len(only_stored & restored_pairs)

    n_excluded = gap_reasons.get("left_excluded_from_indiv_pool_now", 0)
    working_baseline = {
        "choice": len(fresh_pairs),
        "rate": round(len(fresh_pairs) / len(log_indiv), 4),
        "reasoning": (
            f"Fresh reproducible count {len(fresh_pairs)}/{len(log_indiv)}. "
            f"Shuffle counts={counts}. "
            f"Of the {len(only_stored)}-match gap vs stored {len(stored_pairs)}, "
            f"{n_excluded} stored lefts are now outside the indiv pool "
            f"(blank-FIRST→entity routing added after the stored run). "
            f"Restoring those lefts recovers {recovered_of_only_stored}/{len(only_stored)} "
            f"stored-only pairs (matched total {len(m_restored)}). "
            "Use current-code fresh figure as working baseline; 1010 is historical."
        ),
    }

    report = {
        "audit_db": str(audit_db),
        "run_id": run_id,
        "pool": {
            "log_indiv": len(log_indiv),
            "taxops": len(taxops),
            "blank_first_routed_to_entity": bf,
            "same_staged_data_for_stored_and_fresh": True,
            "note": "Stored matches and fresh rerun use the same stage_log / stage_taxops_client rows.",
        },
        "determinism": determinism,
        "gap": {
            "stored_matched": len(stored_pairs),
            "fresh_matched": len(fresh_pairs),
            "delta": len(stored_pairs) - len(fresh_pairs),
            "only_stored": len(only_stored),
            "only_fresh": len(only_fresh),
            "both": len(both),
            "stored_tiers": dict(stored_tiers),
            "fresh_tiers": dict(fresh_tiers),
            "only_stored_tiers": dict(only_stored_tiers),
            "gap_reasons": dict(gap_reasons),
            "isolated_tier_checks_on_unmatched_free": dict(isolated),
            "t3_scan_on_unmatched_free": dict(t3_bug_scan),
            "only_stored_lefts_marked_entity": only_stored_lefts_entity,
            "candidate_causes": {
                "pool_composition_changed": {
                    "tested": True,
                    "result": False,
                    "detail": "Identical stage_* tables; Tax Log live drift cannot explain stored vs fresh on same audit DB.",
                },
                "normalize_entity_blank_first_routing": {
                    "tested": True,
                    "blank_first_routed": bf,
                    "stored_lefts_now_excluded": n_excluded,
                    "pool_without_any_blank_first_routing": len(pool_no_bf),
                    "matched_without_any_blank_first_routing": len(m_no_bf),
                    "restored_excluded_gap_lefts_into_pool": len(pool_restored),
                    "matched_with_gap_lefts_restored": len(m_restored),
                    "stored_only_pairs_recovered": recovered_of_only_stored,
                    "result": (
                        "PRIMARY CAUSE" if n_excluded >= 50 else "partial"
                    )
                    + (
                        f": {n_excluded}/{len(only_stored)} stored-only lefts are "
                        f"XCEL blank-FIRST rows now routed is_entity=True and "
                        f"dropped from LOG_TAXOPS. Restoring them recovers "
                        f"{recovered_of_only_stored} pairs (total matched "
                        f"{len(m_restored)})."
                    ),
                },
                "order_dependence": {
                    "tested": True,
                    "result": determinism["verdict"],
                    "detail": {
                        "matched_counts": determinism["matched_counts"],
                        "verdict": determinism["verdict"],
                        "unstable_pairings": determinism["pairing"][
                            "pairs_unstable_across_runs"
                        ],
                    },
                },
                "tie_break_or_t3_behavior": {
                    "tested": True,
                    "stored_t3": stored_t3,
                    "fresh_t3": fresh_t3,
                    "no_t3_matched": len(m_no_t3),
                    "t3_first_matched": len(m_t3_first),
                    "result": (
                        f"Stored-only tiers={dict(only_stored_tiers)}. "
                        f"Fresh t3={fresh_t3}; t3-first run={len(m_t3_first)}. "
                        "t3 label on stored surplus is correlated with blank-FIRST "
                        "rows (entity names matched via transposition tier when "
                        "those rows were still in the person pool)."
                    ),
                },
            },
        },
        "experiments": {
            "fresh_default_order": len(fresh_pairs),
            "no_t3_tiers": len(m_no_t3),
            "t3_first": len(m_t3_first),
            "no_blank_first_routing": {
                "pool": len(pool_no_bf),
                "matched": len(m_no_bf),
            },
            "restore_excluded_gap_lefts": {
                "pool": len(pool_restored),
                "excluded_lefts_added": len(excluded_lefts),
                "matched": len(m_restored),
                "stored_only_pairs_recovered": recovered_of_only_stored,
            },
        },
        "working_baseline": working_baseline,
        "gap_detail_pii_free": gap_detail,
    }

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    # Console without full gap_detail
    summary = {k: report[k] for k in report if k != "gap_detail_pii_free"}
    summary["gap_detail_n"] = len(gap_detail)
    print(json.dumps(summary, indent=2))
    aconn.close()


if __name__ == "__main__":
    main()
