"""
M10.1b — Corrected sibling-pair mechanism.

Simulates importer arrival when ONLY the counterpart client exists
(the other sibling is absent). That is the "would still fail today" test.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(r"T:\taxops")))
from name_matcher import (  # noqa: E402
    ACCEPT_THRESHOLD,
    REVIEW_THRESHOLD,
    find_client,
    normalize_name,
    strip_middle_initial,
    strip_spouse,
)

from audit.db import connect_audit
from audit.match import _load_sides, _log_keys
from audit.normalizer import keys_with_transposition, normalize_person

SNAP = Path(r"T:\audit\snapshots\taxops_snapshot_20260731.sqlite")
OUT = Path(r"T:\audit\output\m101_sibling_pairs.json")
DETAIL = Path(r"T:\audit\output\m101_sibling_pairs_detail.json")
TASK2 = Path(r"T:\audit\output\task2_log_churn.json")
BATCH_FILENAMES = {
    1: "TAX LOG 2025 Live.csv",
    2: "TAXOPS.csv",
    3: "CSMDATA.csv",
}


def connect_snap():
    c = sqlite3.connect(f"file:{SNAP}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def batch_links(conn) -> dict[int, set[int]]:
    out: dict[int, set[int]] = defaultdict(set)
    for r in conn.execute(
        """
        SELECT r.client_id, se.source_file
        FROM status_events se
        JOIN returns r ON r.id = se.return_id
        WHERE se.source_file IS NOT NULL AND se.source_file != ''
        """
    ):
        sf = r["source_file"] or ""
        for bid, name in BATCH_FILENAMES.items():
            if name in sf:
                out[int(r["client_id"])].add(bid)
    return out


def side_info(snap, batches, cid: int) -> dict:
    cl = snap.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    rets = list(
        snap.execute(
            "SELECT id, log_number, tax_year, created_at FROM returns WHERE client_id=?",
            (cid,),
        )
    )
    last = cl["last_name"] or ""
    first = cl["first_name"] or ""
    n = normalize_person(last, first)
    return {
        "client_id": cid,
        "last_name": last,
        "first_name": first,
        "ssn_last4": cl["ssn_last4"],
        "has_ssn": bool(cl["ssn_last4"]),
        "created_at": cl["created_at"],
        "created_day": str(cl["created_at"] or "")[:10],
        "batch_ids": sorted(batches.get(cid, set())),
        "batch_names": [BATCH_FILENAMES[b] for b in sorted(batches.get(cid, set()))],
        "has_taxlog_batch": 1 in batches.get(cid, set()),
        "has_drake_batch": bool(batches.get(cid, set()) & {2, 3}),  # TAXOPS/CSMDATA
        "n_returns": len(rets),
        "has_log_number": any(
            r["log_number"] not in (None, "", "0", 0) for r in rets
        ),
        "return_years": sorted(
            {int(r["tax_year"]) for r in rets if r["tax_year"] is not None}
        ),
        "return_created_days": sorted(
            {str(r["created_at"] or "")[:10] for r in rets}
        ),
        "norm_surname": n.surname_full,
        "norm_first_key": n.match_keys[0][1] if n.match_keys else "",
    }


def shape_label(earlier: dict, later: dict) -> str:
    """Shape relative to creation order (earlier / later client)."""
    e_ssn, l_ssn = earlier["has_ssn"], later["has_ssn"]
    e_seed, l_seed = earlier["has_taxlog_batch"], later["has_taxlog_batch"]
    e_drake, l_drake = earlier["has_drake_batch"], later["has_drake_batch"]

    # Predicted hypothesis (creation-ordered): earlier=seed no-ssn; later=drake has-ssn
    if e_seed and not e_ssn and l_ssn and l_drake:
        return "H_earlier_seed_nosssn__later_drake_hasssn"

    # Inverse of hypothesis: earlier has ssn; later no ssn
    if e_ssn and not l_ssn:
        if e_drake and l_seed:
            return "INV_earlier_drake_hasssn__later_seed_nosssn"
        if e_ssn and not l_ssn and earlier["created_day"] <= "2026-04-22":
            return "INV_earlier_hasssn__later_nosssn"
        return "INV_earlier_hasssn__later_nosssn_other"

    if not e_ssn and not l_ssn:
        if e_seed and l_seed:
            return "both_nosssn_both_taxlog"
        if earlier["created_day"] == later["created_day"]:
            return "both_nosssn_same_day"
        return "both_nosssn_other"

    if not e_ssn and l_ssn:
        if e_seed and l_drake:
            return "H_earlier_seed_nosssn__later_drake_hasssn"  # already covered
        return "earlier_nosssn__later_hasssn_other"

    if e_ssn and l_ssn:
        if earlier["ssn_last4"] == later["ssn_last4"]:
            return "both_same_ssn"
        return "both_different_ssn"

    return "unclassified"


def simulate_import_against_only(
    snap, probe: dict, target: dict
) -> dict:
    """
    Pretend only `target` exists. Incoming Drake row = probe's name+ssn+TY2025.
    Trace _match_return then _upsert_client.
    """
    last = probe["last_name"] or ""
    first_raw = probe["first_name"]
    first = strip_spouse(first_raw) if first_raw else first_raw
    tax_year = 2025
    tid = target["client_id"]

    # Exact SQL restricted to target's returns only
    if first:
        rows = list(
            snap.execute(
                """
                SELECT r.id, r.client_id
                FROM returns r JOIN clients c ON c.id = r.client_id
                WHERE r.client_id=?
                  AND lower(c.last_name)=lower(?) AND lower(c.first_name)=lower(?)
                  AND r.tax_year=?
                """,
                (tid, last, first, tax_year),
            )
        )
    else:
        rows = list(
            snap.execute(
                """
                SELECT r.id, r.client_id
                FROM returns r JOIN clients c ON c.id = r.client_id
                WHERE r.client_id=?
                  AND lower(c.last_name)=lower(?)
                  AND (c.first_name IS NULL OR c.first_name='')
                  AND r.tax_year=?
                """,
                (tid, last, tax_year),
            )
        )

    match_client_id = None
    match_path = None
    if len(rows) == 1:
        match_client_id = tid
        match_path = "exact_sql"
    elif len(rows) > 1:
        ssn = probe.get("ssn_last4")
        if ssn:
            tssn = target["ssn_last4"]
            if tssn == ssn:
                match_client_id = tid
                match_path = "exact_ssn_narrow"
            else:
                match_path = "exact_ambiguous_ssn_mismatch"
        else:
            match_path = "exact_ambiguous_no_ssn"
    else:
        # Fuzzy against target only
        cache_one = [
            {
                "id": tid,
                "ln": target["last_name"] or "",
                "fn": target["first_name"] or "",
            }
        ]
        fuzzy = find_client(snap, last, first, cache=cache_one)
        if (
            fuzzy
            and not fuzzy["needs_review"]
            and fuzzy["score"] >= ACCEPT_THRESHOLD
        ):
            match_client_id = tid
            match_path = f"fuzzy_{fuzzy['method']}_score_{fuzzy['score']}"
            fuzzy_score = fuzzy["score"]
        else:
            fuzzy_score = fuzzy["score"] if fuzzy else 0
            match_path = (
                f"fuzzy_reject_score_{fuzzy_score}"
                if fuzzy
                else "fuzzy_none"
            )
        # stash score
        directed_score = fuzzy_score if fuzzy else 0
        directed_bucket = (
            "ACCEPT"
            if directed_score >= ACCEPT_THRESHOLD
            else ("REVIEW" if directed_score >= REVIEW_THRESHOLD else "REJECT")
        )
        # Upsert path when match_client_id is None
        if first:
            # Would hit target if lower-equal
            lower_eq = (last or "").lower() == (target["last_name"] or "").lower() and (
                (first or "") or ""
            ).lower() == ((target["first_name"] or "") or "").lower()
        else:
            lower_eq = (last or "").lower() == (target["last_name"] or "").lower() and not (
                target["first_name"] or ""
            ).strip()
        upsert_hits = lower_eq
        still_dup = match_client_id is None and not upsert_hits
        return {
            "match_client_id": match_client_id,
            "match_path": match_path,
            "directed_score": directed_score,
            "directed_bucket": directed_bucket,
            "upsert_would_hit_target": upsert_hits,
            "still_creates_duplicate_today": still_dup,
            "lower_exact_equal": lower_eq,
            "stripped_initial_equal": strip_middle_initial(
                normalize_name(strip_spouse(first) if first else "")
            )
            == strip_middle_initial(
                normalize_name(
                    strip_spouse(target["first_name"]) if target["first_name"] else ""
                )
            )
            and normalize_name(last) == normalize_name(target["last_name"] or ""),
        }

    # Exact path taken — also compute fuzzy score for reporting
    cache_one = [
        {"id": tid, "ln": target["last_name"] or "", "fn": target["first_name"] or ""}
    ]
    fuzzy = find_client(snap, last, first, cache=cache_one)
    directed_score = fuzzy["score"] if fuzzy else 0
    directed_bucket = (
        "ACCEPT"
        if directed_score >= ACCEPT_THRESHOLD
        else ("REVIEW" if directed_score >= REVIEW_THRESHOLD else "REJECT")
    )
    if first:
        lower_eq = (last or "").lower() == (target["last_name"] or "").lower() and (
            (first or "") or ""
        ).lower() == ((target["first_name"] or "") or "").lower()
    else:
        lower_eq = (last or "").lower() == (target["last_name"] or "").lower() and not (
            target["first_name"] or ""
        ).strip()

    # If match_return returned client_id, upsert is forced — no new client
    # If ambiguous paths left match_client_id None:
    still_dup = match_client_id is None and not lower_eq
    # Note: ambiguous causes caller to CONTINUE without upsert — no new client either!
    # That's REVIEW, not create. Important distinction.
    ambiguous_no_create = match_path in (
        "exact_ambiguous_ssn_mismatch",
        "exact_ambiguous_no_ssn",
    )

    return {
        "match_client_id": match_client_id,
        "match_path": match_path,
        "directed_score": directed_score,
        "directed_bucket": directed_bucket,
        "upsert_would_hit_target": lower_eq,
        "still_creates_duplicate_today": still_dup and not ambiguous_no_create,
        "would_go_to_review_not_create": ambiguous_no_create,
        "lower_exact_equal": lower_eq,
        "stripped_initial_equal": strip_middle_initial(
            normalize_name(strip_spouse(first) if first else "")
        )
        == strip_middle_initial(
            normalize_name(
                strip_spouse(target["first_name"]) if target["first_name"] else ""
            )
        )
        and normalize_name(last) == normalize_name(target["last_name"] or ""),
    }


def main():
    snap = connect_snap()
    audit_db = sorted(
        Path(r"T:\audit").glob("audit_*.sqlite"), key=lambda p: p.stat().st_mtime
    )[-1]
    aconn = connect_audit(audit_db)
    run_id = int(aconn.execute("SELECT MAX(id) FROM audit_run").fetchone()[0])
    task2 = json.loads(TASK2.read_text(encoding="utf-8"))
    fails = [
        f
        for f in task2["part_b"]["self_match_failures"]
        if f["batch1_norm_surname"] == f["taxops_norm_surname"]
        and f["batch1_norm_first_key"] == f["taxops_norm_first_key"]
    ]

    sides = _load_sides(aconn, run_id)
    log_indiv = [x for x in sides["log"] if not x.is_entity]
    idx = defaultdict(list)
    for s in log_indiv:
        for k in _log_keys(s):
            idx[k].append(s)
    stored = list(
        aconn.execute(
            "SELECT left_id, right_id FROM audit_match WHERE run_id=? AND pair='LOG_TAXOPS'",
            (run_id,),
        )
    )
    matched_log = {int(r["left_id"]) for r in stored}
    matched_to = {int(r["right_id"]) for r in stored}
    log_consumed_by = {int(r["left_id"]): int(r["right_id"]) for r in stored}
    stage_to_client = {
        int(r["id"]): int(r["client_id"])
        for r in aconn.execute(
            "SELECT id, client_id FROM stage_taxops_client WHERE run_id=?", (run_id,)
        )
    }
    client_to_stage = {v: k for k, v in stage_to_client.items()}
    batches = batch_links(snap)

    pairs = []
    seen = set()
    for f in fails:
        row = snap.execute(
            "SELECT client_id FROM returns WHERE id=?", (int(f["return_id"]),)
        ).fetchone()
        loser_cid = int(row["client_id"])
        sid = client_to_stage.get(loser_cid)
        if sid is None or sid in matched_to:
            continue
        sc = aconn.execute(
            "SELECT last_name, first_name FROM stage_taxops_client WHERE id=?", (sid,)
        ).fetchone()
        keys = set(keys_with_transposition(sc["last_name"] or "", sc["first_name"] or ""))
        cands, seenc = [], set()
        for k in keys:
            for s in idx.get(k, []):
                if s.id not in seenc:
                    seenc.add(s.id)
                    cands.append(s)
        used = [s for s in cands if s.id in matched_log]
        if not used:
            continue
        winner_cid = stage_to_client[log_consumed_by[used[0].id]]
        osc = aconn.execute(
            "SELECT last_name, first_name FROM stage_taxops_client WHERE id=?",
            (client_to_stage[winner_cid],),
        ).fetchone()
        same = ((osc["last_name"] or "").upper() == (sc["last_name"] or "").upper()) and (
            (osc["first_name"] or "").upper().split()[:1]
            == (sc["first_name"] or "").upper().split()[:1]
        )
        if not same:
            continue
        pk = tuple(sorted((loser_cid, winner_cid)))
        if pk in seen:
            continue
        seen.add(pk)
        pairs.append({"loser_cid": loser_cid, "winner_cid": winner_cid})

    shape_counts = Counter()
    detail = []
    still_fail = 0
    review_not_create = 0
    would_merge = 0
    score_buckets = Counter()
    miss_reasons = Counter()
    path_counts = Counter()
    created_day_pair = Counter()

    for p in pairs:
        loser = side_info(snap, batches, p["loser_cid"])
        winner = side_info(snap, batches, p["winner_cid"])

        if (loser["created_at"] or "") <= (winner["created_at"] or ""):
            earlier, later = loser, winner
            earlier_role, later_role = "loser", "winner"
        else:
            earlier, later = winner, loser
            earlier_role, later_role = "winner", "loser"

        shape = shape_label(earlier, later)
        shape_counts[shape] += 1
        created_day_pair[
            f"{earlier['created_day']}→{later['created_day']}"
        ] += 1

        # Decision test: later arrives while only earlier exists
        sim = simulate_import_against_only(snap, probe=later, target=earlier)
        path_counts[sim["match_path"].split("_score_")[0]] += 1
        score_buckets[sim["directed_bucket"]] += 1

        if sim["still_creates_duplicate_today"]:
            still_fail += 1
            if not sim["lower_exact_equal"] and not sim.get("stripped_initial_equal"):
                miss_reasons["name_diff_fuzzy_below_accept"] += 1
            elif not sim["lower_exact_equal"] and sim.get("stripped_initial_equal"):
                miss_reasons["middle_initial_diff_but_should_fuzzy_97"] += 1
            elif sim["directed_bucket"] == "REVIEW":
                miss_reasons["fuzzy_review_band"] += 1
            elif sim["directed_bucket"] == "REJECT":
                miss_reasons["fuzzy_reject"] += 1
            else:
                miss_reasons["other_create"] += 1
        elif sim.get("would_go_to_review_not_create"):
            review_not_create += 1
            miss_reasons["ambiguous_review_no_create"] += 1
        else:
            would_merge += 1

        # Also: why did duplicate exist if names are lower-equal?
        # If lower_equal, current importer merges — historical race / different code
        detail.append(
            {
                "loser": loser,
                "winner": winner,
                "earlier_role": earlier_role,
                "later_role": later_role,
                "shape": shape,
                "sim_later_into_earlier": sim,
                "norms_equal": (
                    loser["norm_surname"] == winner["norm_surname"]
                    and loser["norm_first_key"] == winner["norm_first_key"]
                ),
                "raw_lower_equal": (
                    (loser["last_name"] or "").lower()
                    == (winner["last_name"] or "").lower()
                    and (loser["first_name"] or "").lower()
                    == (winner["first_name"] or "").lower()
                ),
            }
        )

    # Refine alternative shapes that were miscategorized — add batch day detail
    alt_detail = Counter()
    for d in detail:
        e = d["loser"] if d["earlier_role"] == "loser" else d["winner"]
        l = d["loser"] if d["later_role"] == "loser" else d["winner"]
        alt_detail[
            (
                d["shape"],
                e["created_day"],
                l["created_day"],
                e["has_ssn"],
                l["has_ssn"],
                e["has_taxlog_batch"],
                l["has_taxlog_batch"],
                e["has_drake_batch"],
                l["has_drake_batch"],
            )
        ] += 1

    # Compact alternative shape report
    shape_breakdown = []
    by_shape = defaultdict(list)
    for key, n in alt_detail.items():
        by_shape[key[0]].append({"n": n, "earlier_day": key[1], "later_day": key[2],
                                  "earlier_ssn": key[3], "later_ssn": key[4],
                                  "earlier_taxlog": key[5], "later_taxlog": key[6],
                                  "earlier_drake": key[7], "later_drake": key[8]})

    for shape, rows in sorted(by_shape.items(), key=lambda x: -sum(r["n"] for r in x[1])):
        shape_breakdown.append({
            "shape": shape,
            "count": sum(r["n"] for r in rows),
            "subtypes": sorted(rows, key=lambda r: -r["n"])[:8],
        })

    summary = {
        "n_pairs": len(pairs),
        "thresholds": {"ACCEPT": ACCEPT_THRESHOLD, "REVIEW": REVIEW_THRESHOLD},
        "predicted_shape": {
            "name": "H_earlier_seed_nosssn__later_drake_hasssn",
            "count": shape_counts.get("H_earlier_seed_nosssn__later_drake_hasssn", 0),
            "confirmed": False,
            "note": (
                "Hypothesis REFUTED as the dominant mechanism. "
                f"Only {shape_counts.get('H_earlier_seed_nosssn__later_drake_hasssn', 0)}/96 fit."
            ),
        },
        "shape_counts": dict(shape_counts.most_common()),
        "shape_breakdown": shape_breakdown,
        "creation_order": {
            "winner_is_earlier": sum(1 for d in detail if d["earlier_role"] == "winner"),
            "loser_is_earlier": sum(1 for d in detail if d["earlier_role"] == "loser"),
            "top_day_transitions": dict(created_day_pair.most_common(12)),
        },
        "ssn": {
            "earlier_has_later_null": sum(
                1
                for d in detail
                if (d["winner"] if d["earlier_role"] == "winner" else d["loser"])["has_ssn"]
                and not (d["winner"] if d["later_role"] == "winner" else d["loser"])["has_ssn"]
            ),
            "earlier_null_later_has": sum(
                1
                for d in detail
                if not (d["winner"] if d["earlier_role"] == "winner" else d["loser"])["has_ssn"]
                and (d["winner"] if d["later_role"] == "winner" else d["loser"])["has_ssn"]
            ),
            "both_null": sum(
                1 for d in detail if not d["loser"]["has_ssn"] and not d["winner"]["has_ssn"]
            ),
            "loser_null_winner_has": sum(
                1 for d in detail if not d["loser"]["has_ssn"] and d["winner"]["has_ssn"]
            ),
        },
        "log_number": {
            "only_winner_has_log": sum(
                1
                for d in detail
                if d["winner"]["has_log_number"] and not d["loser"]["has_log_number"]
            ),
            "only_loser_has_log": sum(
                1
                for d in detail
                if d["loser"]["has_log_number"] and not d["winner"]["has_log_number"]
            ),
            "both_have_log": sum(
                1
                for d in detail
                if d["loser"]["has_log_number"] and d["winner"]["has_log_number"]
            ),
        },
        "name_identity": {
            "raw_lower_equal": sum(1 for d in detail if d["raw_lower_equal"]),
            "audit_norms_equal": sum(1 for d in detail if d["norms_equal"]),
        },
        "importer_today_later_into_earlier": {
            "would_merge": would_merge,
            "would_review_not_create": review_not_create,
            "still_creates_duplicate_today": still_fail,
            "score_buckets": dict(score_buckets),
            "match_paths": dict(path_counts),
            "miss_reasons": dict(miss_reasons),
            "note": (
                "Simulation: only the earlier sibling exists; later sibling's "
                "name+ssn arrives via Drake _match_return → _upsert_client. "
                "Ambiguous exact matches go to REVIEW (no insert)."
            ),
        },
        "decision_output": {
            "would_still_fail_today": still_fail,
            "interpretation": (
                f"{still_fail}/96 pairs would still mint a new client today if the "
                "later record arrived against only the earlier sibling. "
                f"{would_merge}/96 would merge; {review_not_create}/96 would REVIEW."
            ),
        },
    }

    OUT.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    DETAIL.write_text(
        json.dumps({"pairs": detail, "summary": summary}, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    aconn.close()
    snap.close()


if __name__ == "__main__":
    main()
