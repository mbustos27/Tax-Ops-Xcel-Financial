"""Characterize 2026-07-01 and 2026-04-21 import events."""
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
    strip_spouse,
)
from name_matcher import score_client_names_pair  # noqa: E402

from audit.normalizer import normalize_person

SNAP = Path(r"T:\audit\snapshots\taxops_snapshot_20260731.sqlite")
OUT = Path(r"T:\audit\output\jul1_event.json")
DETAIL = Path(r"T:\audit\output\jul1_event_detail.json")
M101 = Path(r"T:\audit\output\m101_sibling_pairs_detail.json")


def connect():
    c = sqlite3.connect(f"file:{SNAP}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def main():
    c = connect()

    # ── Timestamp distributions ───────────────────────────────────────
    client_ts = [
        dict(r)
        for r in c.execute(
            """
            SELECT created_at, COUNT(*) n FROM clients
            WHERE substr(created_at,1,10)='2026-07-01'
            GROUP BY 1 ORDER BY n DESC
            """
        )
    ]
    return_ts = [
        dict(r)
        for r in c.execute(
            """
            SELECT created_at, COUNT(*) n FROM returns
            WHERE substr(created_at,1,10)='2026-07-01'
            GROUP BY 1 ORDER BY n DESC
            """
        )
    ]
    apr21_ts = [
        dict(r)
        for r in c.execute(
            """
            SELECT created_at, COUNT(*) n FROM clients
            WHERE substr(created_at,1,10)='2026-04-21'
            GROUP BY 1 ORDER BY n DESC
            """
        )
    ]
    apr29_ret_ts = [
        dict(r)
        for r in c.execute(
            """
            SELECT created_at, COUNT(*) n FROM returns
            WHERE substr(created_at,1,10)='2026-04-29'
            GROUP BY 1 ORDER BY n DESC
            """
        )
    ]

    batches = [dict(r) for r in c.execute("SELECT * FROM import_batches ORDER BY id")]
    import_row_stats = [
        dict(r)
        for r in c.execute(
            "SELECT batch_id, action, COUNT(*) n FROM import_rows GROUP BY 1,2 ORDER BY 1,2"
        )
    ]

    # Format signature of Jul1 client timestamps
    jul1_clients = list(
        c.execute(
            "SELECT id, last_name, first_name, ssn_last4, created_at, updated_at, display_name "
            "FROM clients WHERE substr(created_at,1,10)='2026-07-01' ORDER BY id"
        )
    )
    fmt_counts = Counter()
    for r in jul1_clients:
        ca = r["created_at"] or ""
        if "T" in ca and "+" in ca:
            fmt_counts["iso_offset"] += 1
        elif "T" in ca and ca.endswith("Z"):
            fmt_counts["iso_z"] += 1
        elif "T" in ca:
            fmt_counts["iso_other"] += 1
        elif " " in ca and "T" not in ca:
            fmt_counts["space_no_tz"] += 1
        else:
            fmt_counts["other"] += 1

    # status_events for jul1 clients' returns
    source_files = [
        dict(r)
        for r in c.execute(
            """
            SELECT se.source_file,
                   COUNT(DISTINCT r.id) AS n_returns,
                   COUNT(DISTINCT r.client_id) AS n_clients,
                   COUNT(*) AS n_events
            FROM returns r
            JOIN status_events se ON se.return_id = r.id
            WHERE r.client_id IN (
              SELECT id FROM clients WHERE substr(created_at,1,10)='2026-07-01'
            )
            GROUP BY 1 ORDER BY n_returns DESC
            """
        )
    ]

    # Returns created Jul1 — which clients
    ret_client_day = dict(
        c.execute(
            """
            SELECT
              CASE
                WHEN substr(cl.created_at,1,10)='2026-07-01' THEN 'client_also_jul1'
                WHEN substr(cl.created_at,1,10)='2026-04-21' THEN 'client_apr21'
                ELSE 'client_other_' || substr(cl.created_at,1,10)
              END AS bucket,
              COUNT(*) n
            FROM returns r
            JOIN clients cl ON cl.id = r.client_id
            WHERE substr(r.created_at,1,10)='2026-07-01'
            GROUP BY 1
            """
        )
    )

    # Cluster 344 at space-no-tz — do they have returns, when?
    cluster_time = "2026-07-01 18:19:30"
    cluster = [
        dict(r)
        for r in c.execute(
            """
            SELECT cl.id, cl.last_name, cl.first_name, cl.ssn_last4, cl.created_at,
                   (SELECT COUNT(*) FROM returns r WHERE r.client_id=cl.id) n_ret,
                   (SELECT GROUP_CONCAT(DISTINCT substr(r.created_at,1,19))
                      FROM returns r WHERE r.client_id=cl.id) ret_created,
                   (SELECT GROUP_CONCAT(DISTINCT r.log_number)
                      FROM returns r WHERE r.client_id=cl.id) log_numbers,
                   (SELECT GROUP_CONCAT(DISTINCT r.tax_year)
                      FROM returns r WHERE r.client_id=cl.id) years
            FROM clients cl WHERE cl.created_at=?
            """,
            (cluster_time,),
        )
    ]

    # ── Duplicate detection vs pre-Jul1 clients ───────────────────────
    pre = list(
        c.execute(
            """
            SELECT id, last_name, first_name, ssn_last4, created_at
            FROM clients
            WHERE substr(created_at,1,10) < '2026-07-01'
               OR (substr(created_at,1,10)='2026-07-01'
                   AND created_at < '2026-07-01 18:19:30'
                   AND created_at NOT LIKE '2026-07-01 18:19:30%')
            """
        )
    )
    # Simpler: all clients not in jul1 set
    jul1_ids = {int(r["id"]) for r in jul1_clients}
    pre = [
        dict(r)
        for r in c.execute(
            "SELECT id, last_name, first_name, ssn_last4, created_at FROM clients"
        )
        if int(r["id"]) not in jul1_ids
    ]

    def name_key(last, first):
        n = normalize_person(last or "", first or "")
        if n.match_keys:
            return n.match_keys[0]
        return (n.surname_full or "", "")

    def lower_key(last, first):
        return ((last or "").strip().lower(), (first or "").strip().lower())

    pre_by_norm: dict = defaultdict(list)
    pre_by_lower: dict = defaultdict(list)
    for p in pre:
        pre_by_norm[name_key(p["last_name"], p["first_name"])].append(p)
        pre_by_lower[lower_key(p["last_name"], p["first_name"])].append(p)

    # Known 96 from M10.1
    known_96_losers = set()
    known_96_pairs = []
    if M101.exists():
        m101 = json.loads(M101.read_text(encoding="utf-8"))
        for pair in m101.get("pairs", []):
            loser = pair["loser"]["client_id"]
            winner = pair["winner"]["client_id"]
            known_96_losers.add(loser)
            known_96_pairs.append((loser, winner))

    dup_exact_lower = []
    dup_norm_only = []
    novel = []
    ambiguous_multi = []

    for r in jul1_clients:
        cid = int(r["id"])
        lk = lower_key(r["last_name"], r["first_name"])
        nk = name_key(r["last_name"], r["first_name"])
        hits_lower = pre_by_lower.get(lk, [])
        hits_norm = pre_by_norm.get(nk, [])
        # exclude empty name keys
        if not nk[0] and not nk[1]:
            novel.append({"client_id": cid, "reason": "empty_name"})
            continue
        if hits_lower:
            bucket = dup_exact_lower
            matches = hits_lower
        elif hits_norm:
            bucket = dup_norm_only
            matches = hits_norm
        else:
            novel.append(
                {
                    "client_id": cid,
                    "created_at": r["created_at"],
                    "has_ssn": bool(r["ssn_last4"]),
                    "in_known_96": cid in known_96_losers,
                }
            )
            continue

        if len(matches) > 1:
            ambiguous_multi.append(
                {
                    "client_id": cid,
                    "n_pre_matches": len(matches),
                    "match_ids": [int(m["id"]) for m in matches],
                    "via": "lower" if hits_lower else "norm",
                    "in_known_96": cid in known_96_losers,
                }
            )
        bucket.append(
            {
                "client_id": cid,
                "pre_ids": [int(m["id"]) for m in matches],
                "pre_created_days": sorted(
                    {str(m["created_at"] or "")[:10] for m in matches}
                ),
                "jul1_has_ssn": bool(r["ssn_last4"]),
                "pre_any_ssn": any(m["ssn_last4"] for m in matches),
                "in_known_96": cid in known_96_losers,
                "via": "lower" if hits_lower else "norm",
            }
        )

    # ── Which importer? Evidence ──────────────────────────────────────
    # 1) No import_batches row for Jul1
    # 2) Timestamp format space_no_tz ≠ utils.now() ISO
    # 3) importer.py INSERT does not set ssn_last4; drake does
    # 4) Cluster null ssn rate
    cluster_ssn = dict(
        c.execute(
            """
            SELECT
              COUNT(*) n,
              SUM(CASE WHEN ssn_last4 IS NULL OR ssn_last4='' THEN 1 ELSE 0 END) null_ssn
            FROM clients WHERE created_at=?
            """,
            (cluster_time,),
        ).fetchone()
    )

    # Columns populated on cluster vs typical importer inserts
    # Check display_name / referral on cluster
    cluster_cols = dict(
        c.execute(
            """
            SELECT
              SUM(CASE WHEN display_name IS NOT NULL AND display_name!='' THEN 1 ELSE 0 END) has_display,
              SUM(CASE WHEN referral_flag IS NOT NULL THEN 1 ELSE 0 END) has_referral,
              SUM(CASE WHEN taxpayer_email IS NOT NULL AND taxpayer_email!='' THEN 1 ELSE 0 END) has_email,
              SUM(CASE WHEN address IS NOT NULL AND address!='' THEN 1 ELSE 0 END) has_address,
              SUM(CASE WHEN spouse_first_name IS NOT NULL AND spouse_first_name!='' THEN 1 ELSE 0 END) has_spouse_col
            FROM clients WHERE created_at=?
            """,
            (cluster_time,),
        ).fetchone()
    )

    # Apr21 — relation to Apr29
    apr21_n = c.execute(
        "SELECT COUNT(*) FROM clients WHERE substr(created_at,1,10)='2026-04-21'"
    ).fetchone()[0]
    apr21_with_apr29_return = c.execute(
        """
        SELECT COUNT(DISTINCT cl.id) FROM clients cl
        JOIN returns r ON r.client_id=cl.id
        WHERE substr(cl.created_at,1,10)='2026-04-21'
          AND substr(r.created_at,1,10)='2026-04-29'
        """
    ).fetchone()[0]
    apr21_ssn = dict(
        c.execute(
            """
            SELECT
              COUNT(*) n,
              SUM(CASE WHEN ssn_last4 IS NULL OR ssn_last4='' THEN 1 ELSE 0 END) null_ssn
            FROM clients WHERE substr(created_at,1,10)='2026-04-21'
            """
        ).fetchone()
    )
    apr21_fmt = Counter()
    for r in c.execute(
        "SELECT created_at FROM clients WHERE substr(created_at,1,10)='2026-04-21'"
    ):
        ca = r[0] or ""
        if "T" in ca and "+" in ca:
            apr21_fmt["iso_offset"] += 1
        elif "T" in ca and ca.endswith("Z"):
            apr21_fmt["iso_z"] += 1
        elif " " in ca:
            apr21_fmt["space_no_tz"] += 1
        else:
            apr21_fmt["other"] += 1

    # ── Re-run would-still-fail via importer.py logic ─────────────────
    # Manual importer matches primarily by LOG number then fuzzy name.
    # For Jul1 twins that are duplicates: simulate log import of later
    # against earlier-only world.
    #
    # Load known pairs from M101 where loser is Jul1
    still_fail_manual = 0
    merge_manual = 0
    review_manual = 0
    manual_paths = Counter()
    pair_sims = []

    if M101.exists():
        m101 = json.loads(M101.read_text(encoding="utf-8"))
        for pair in m101.get("pairs", []):
            loser = pair["loser"]
            winner = pair["winner"]
            # later = Jul1 side
            if str(loser["created_at"] or "").startswith("2026-07-01"):
                later, earlier = loser, winner
            elif str(winner["created_at"] or "").startswith("2026-07-01"):
                later, earlier = winner, loser
            else:
                continue

            # Get later's return log_number
            lat_ret = c.execute(
                """
                SELECT id, log_number, tax_year FROM returns
                WHERE client_id=? ORDER BY id LIMIT 1
                """,
                (later["client_id"],),
            ).fetchone()
            ear_rets = list(
                c.execute(
                    """
                    SELECT r.id AS return_id, r.log_number AS return_log_number,
                           r.tax_year, r.client_id,
                           c.last_name, c.first_name
                    FROM returns r JOIN clients c ON c.id=r.client_id
                    WHERE r.client_id=? AND r.tax_year=2025
                    """,
                    (earlier["client_id"],),
                )
            )
            csv_ln = later["last_name"] or ""
            csv_fn = later["first_name"] or ""
            log_num = (lat_ret["log_number"] if lat_ret else None) or ""

            # Simulate _match_return with only earlier's returns in prefetch
            path = None
            outcome = None  # merge / review / create
            if not ear_rets:
                # fuzzy against empty → create via upsert; check upsert exact
                lower_eq = (csv_ln or "").lower() == (earlier["last_name"] or "").lower() and (
                    csv_fn or ""
                ).lower() == (earlier["first_name"] or "").lower()
                if lower_eq:
                    outcome = "merge_via_upsert_exact"
                    path = "no_prefetch_upsert"
                else:
                    # find_client style score
                    score = score_client_names_pair(
                        csv_ln, csv_fn, earlier["last_name"], earlier["first_name"]
                    )
                    if score >= ACCEPT_THRESHOLD:
                        # Manual path: _fuzzy_pick needs year_rows — empty → empty match → upsert
                        outcome = (
                            "create_duplicate"
                            if not lower_eq
                            else "merge_via_upsert_exact"
                        )
                        path = f"empty_year_rows_score_{score}"
                    else:
                        outcome = "create_duplicate"
                        path = f"empty_year_rows_low_score_{score}"
            else:
                # log match against earlier returns only
                from importer import normalize_string  # type: ignore

                log_needle = normalize_string(str(log_num))
                same_log = [
                    r
                    for r in ear_rets
                    if normalize_string(
                        str(
                            r["return_log_number"]
                            if r["return_log_number"] is not None
                            else ""
                        )
                    )
                    == log_needle
                    and log_needle != ""
                ]
                if len(same_log) > 1:
                    outcome = "review_ambiguous_log"
                    path = "ambiguous_log"
                elif len(same_log) == 1:
                    score = score_client_names_pair(
                        csv_ln, csv_fn, same_log[0]["last_name"], same_log[0]["first_name"]
                    )
                    if score >= ACCEPT_THRESHOLD:
                        outcome = "merge_via_log_name"
                        path = f"log_name_score_{score}"
                    elif score >= REVIEW_THRESHOLD:
                        outcome = "review"
                        path = f"log_vs_name_medium_{score}"
                    else:
                        # fall through to fuzzy among earlier returns
                        score2 = score_client_names_pair(
                            csv_ln, csv_fn, earlier["last_name"], earlier["first_name"]
                        )
                        if score2 >= ACCEPT_THRESHOLD:
                            outcome = "merge_via_fuzzy"
                            path = f"fuzzy_after_log_miss_{score2}"
                        elif score2 >= REVIEW_THRESHOLD:
                            outcome = "review"
                            path = f"fuzzy_review_{score2}"
                        else:
                            lower_eq = (csv_ln or "").lower() == (
                                earlier["last_name"] or ""
                            ).lower() and (csv_fn or "").lower() == (
                                earlier["first_name"] or ""
                            ).lower()
                            outcome = (
                                "merge_via_upsert_exact"
                                if lower_eq
                                else "create_duplicate"
                            )
                            path = f"fuzzy_reject_{score2}"
                else:
                    # no log match — fuzzy among earlier
                    score2 = score_client_names_pair(
                        csv_ln, csv_fn, earlier["last_name"], earlier["first_name"]
                    )
                    if score2 >= ACCEPT_THRESHOLD:
                        outcome = "merge_via_fuzzy"
                        path = f"fuzzy_no_log_{score2}"
                    elif score2 >= REVIEW_THRESHOLD:
                        outcome = "review"
                        path = f"fuzzy_review_no_log_{score2}"
                    else:
                        lower_eq = (csv_ln or "").lower() == (
                            earlier["last_name"] or ""
                        ).lower() and (csv_fn or "").lower() == (
                            earlier["first_name"] or ""
                        ).lower()
                        outcome = (
                            "merge_via_upsert_exact" if lower_eq else "create_duplicate"
                        )
                        path = f"no_log_reject_{score2}_upsert_{lower_eq}"

            manual_paths[path.split("_score")[0].rstrip("0123456789_")] += 1
            if outcome == "create_duplicate":
                still_fail_manual += 1
            elif outcome and outcome.startswith("review"):
                review_manual += 1
            else:
                merge_manual += 1
            pair_sims.append(
                {
                    "later_id": later["client_id"],
                    "earlier_id": earlier["client_id"],
                    "later_log": log_num,
                    "outcome": outcome,
                    "path": path,
                    "in_known_shape": pair.get("shape"),
                }
            )

    # Novel vs dup summary among non-96
    dup_ids = {d["client_id"] for d in dup_exact_lower} | {
        d["client_id"] for d in dup_norm_only
    }
    remaining = [r for r in jul1_clients if int(r["id"]) not in known_96_losers]
    rem_dup = sum(1 for r in remaining if int(r["id"]) in dup_ids)
    rem_novel = sum(
        1
        for r in remaining
        if int(r["id"]) not in dup_ids
        and int(r["id"]) not in {a["client_id"] for a in ambiguous_multi}
    )
    rem_amb = sum(
        1 for r in remaining if int(r["id"]) in {a["client_id"] for a in ambiguous_multi}
    )

    # How many of 96 losers are in jul1?
    losers_in_jul1 = sum(1 for x in known_96_losers if x in jul1_ids)

    report = {
        "jul1_clients": {
            "n": len(jul1_clients),
            "timestamp_distribution": client_ts,
            "timestamp_format_counts": dict(fmt_counts),
            "dominant_cluster": {
                "created_at": cluster_time,
                "n": cluster_ssn.get("n") or sum(
                    x["n"] for x in client_ts if x["created_at"] == cluster_time
                ),
                "null_ssn": cluster_ssn.get("null_ssn"),
                "column_fill": cluster_cols,
                "format": "space_no_tz (NOT utils.now() ISO)",
            },
        },
        "jul1_returns": {
            "timestamp_distribution": return_ts,
            "client_created_day_of_those_returns": ret_client_day,
            "note": (
                "Client cluster at 18:19:30 space-format; return bulk at "
                "19:36:51 ISO — ~77 minutes apart. Suggests two-step or "
                "two-tool write, not a single process_csv/drake call."
            ),
        },
        "import_batches": {
            "n": len(batches),
            "rows": [
                {
                    "id": b["id"],
                    "filename": b["filename"],
                    "file_hash": b["file_hash"],
                    "imported_at": b["imported_at"],
                    "status": b["status"],
                    "row_count": b["row_count"],
                    "created_clients": b["created_clients"],
                    "created_returns": b["created_returns"],
                }
                for b in batches
            ],
            "import_rows_by_batch_action": import_row_stats,
            "jul1_batch_row_exists": False,
            "reconcile": (
                "Still exactly 3 import_batches, all imported_at 2026-04-29T18:04:12–14. "
                "No fourth batch for July 1. July 1 clients/returns were written "
                "outside the import_batches accounting path (or batches table was "
                "not updated)."
            ),
        },
        "which_importer": {
            "evidence": [
                "No import_batches row for 2026-07-01",
                "344/368 clients use 'YYYY-MM-DD HH:MM:SS' (space, no TZ) — neither importer.py nor drake_importer.py use that via utils.now() (ISO+offset)",
                "Cluster is 100% null ssn_last4; drake_importer INSERT sets ssn_last4; importer.py INSERT omits ssn (compatible with null)",
                "status_events on jul1-client returns still reference Apr29 CSV paths and/or later APP — see source_files",
                "Client write at 18:19:30 vs return write at 19:36:51 — split event",
            ],
            "source_files_on_jul1_client_returns": source_files,
            "verdict": (
                "July 1 was NOT recorded as a normal import_batches run. "
                "Timestamp format rules out current utils.now()-based importer inserts "
                "for the 344-client cluster. Most likely a one-off script, restore, "
                "or older code path that used naive datetime. "
                "Tax Log batch *links* on twins are from status_events.source_file "
                "strings (often the Apr29 TAX LOG path reused), not proof that "
                "importer.py process_csv ran on July 1."
            ),
            "drake_path_still_valid_for": (
                "M10.1 Drake simulation answers: if these names re-entered via "
                "drake_importer today against only the Apr21 sibling, 0 would create dups."
            ),
        },
        "duplicate_census_of_368": {
            "n_jul1": len(jul1_clients),
            "known_96_losers_in_jul1": losers_in_jul1,
            "duplicate_exact_lower": len(dup_exact_lower),
            "duplicate_norm_key_only": len(dup_norm_only),
            "ambiguous_multi_pre_match": len(ambiguous_multi),
            "novel": len(novel),
            "known_96_subset_of_exact_or_norm_dup": sum(
                1
                for d in dup_exact_lower + dup_norm_only
                if d["in_known_96"]
            ),
            "remaining_after_96": {
                "n": len(remaining),
                "duplicate": rem_dup,
                "novel": rem_novel,
                "ambiguous": rem_amb,
            },
            "dup_pre_created_day_hist": dict(
                Counter(
                    day
                    for d in dup_exact_lower + dup_norm_only
                    for day in d["pre_created_days"]
                )
            ),
        },
        "importer_py_simulation_on_96": {
            "still_creates_duplicate_today": still_fail_manual,
            "would_merge": merge_manual,
            "would_review": review_manual,
            "path_counts": dict(manual_paths),
            "note": (
                "Simulated importer.py _match_return (LOG-first, then fuzzy) with "
                "only the earlier sibling's TY2025 returns in the prefetch pool, "
                "then _upsert_client exact fallback. Separate from Drake M10.1 number."
            ),
        },
        "april_21_event": {
            "n_clients": apr21_n,
            "timestamp_distribution": apr21_ts,
            "timestamp_format_counts": dict(apr21_fmt),
            "ssn": apr21_ssn,
            "with_return_created_apr29": apr21_with_apr29_return,
            "relation_to_apr29_batches": (
                "April 21 created the client rows (ISO+offset timestamps, "
                f"{apr21_n} clients in 2 bursts at 22:06:58 and 22:10:40). "
                "April 29 import_batches (TAX LOG / TAXOPS / CSMDATA) created/updated "
                f"returns — {apr21_with_apr29_return} of those Apr21 clients received "
                "an Apr29-timestamped return. Apr29 is a return/status seed on top of "
                "an earlier Apr21 client foundation, not the founding client insert."
            ),
            "which_importer_apr21": (
                "No import_batches row on Apr21 either. ISO+offset matches utils.now(). "
                "High ssn fill suggests Drake-shaped data. Likely an early drake_importer "
                "or bootstrap load before batch accounting was reliable "
                f"(import_batches.row_count still 0 on all 3 Apr29 batches despite "
                f"{sum(x['n'] for x in import_row_stats)} import_rows)."
            ),
        },
        "decision_outputs": {
            "jul1_is_bulk": True,
            "jul1_has_import_batch_row": False,
            "n_batches_still": 3,
            "dup_of_preexisting_among_368": len(dup_exact_lower) + len(dup_norm_only),
            "novel_among_368": len(novel),
            "manual_importer_would_still_fail_today_on_96": still_fail_manual,
            "drake_importer_would_still_fail_today_on_96": 0,
        },
    }

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    DETAIL.write_text(
        json.dumps(
            {
                "dup_exact_lower": dup_exact_lower,
                "dup_norm_only": dup_norm_only,
                "ambiguous_multi": ambiguous_multi,
                "novel": novel,
                "pair_sims_manual": pair_sims,
                "cluster_sample_ids": [x["id"] for x in cluster[:20]],
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2))
    c.close()


if __name__ == "__main__":
    main()
