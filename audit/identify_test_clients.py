"""Full test-client identification, cross-ref 147, rate impact."""
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from audit.db import connect_audit
from audit.match import _load_sides, _taxops_keys, match_sides
from audit.normalizer import keys_with_transposition, normalize_person

SNAP = Path(r"T:\audit\snapshots\taxops_snapshot_20260731.sqlite")
OUT = Path(r"T:\audit\output\test_client_id.json")
DIAG = Path(r"T:\audit\output\matcher_defect_diag.json")
TASK2 = Path(r"T:\audit\output\task2_log_churn.json")

SEED_RETURN_DAY = "2026-04-29"
BATCH_FILENAMES = ("TAX LOG 2025 Live.csv", "TAXOPS.csv", "CSMDATA.csv")
KEYWORD_RE = re.compile(
    r"(?:^|[^A-Z0-9])(TEST|DEMO|SAMPLE|ASDF|XXX|AAAA+|FOO|BAR|BAZ|DUMMY|FAKE|QWERTY|ZXCV)(?:[^A-Z0-9]|$)",
    re.I,
)
MASH_TOKENS = {
    "ASDF",
    "ASDFG",
    "QWERTY",
    "QWER",
    "ZXCV",
    "ZXCVB",
    "AAAA",
    "XXXX",
    "ABCDEF",
    "ABC",
    "ABCD",
}


def connect():
    c = sqlite3.connect(f"file:{SNAP}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def tokens(blob: str) -> list[str]:
    return [t for t in re.split(r"[^A-Za-z0-9]+", (blob or "").upper()) if t]


def score_name(last, first, display) -> list[str]:
    blob = f"{last or ''} {first or ''} {display or ''}".strip()
    hits = []
    if not blob:
        hits.append("name_blank")
        return hits
    if KEYWORD_RE.search(blob):
        hits.append("name_keyword_testish")
    toks = tokens(blob)
    # whole-name is single char
    if (last or "").strip() and len((last or "").strip()) == 1:
        hits.append("name_single_char_last")
    if (first or "").strip() and len((first or "").strip()) == 1:
        hits.append("name_single_char_first")
    for t in toks:
        if len(t) >= 4 and len(set(t)) == 1:
            hits.append("name_repeated_char")
        if t in MASH_TOKENS:
            hits.append("name_keyboard_mash")
        # 6+ consonants mashed without vowel
        if len(t) >= 6 and not re.search(r"[AEIOU]", t) and t.isalpha():
            hits.append("name_no_vowel_mash")
    return sorted(set(hits))


def main():
    c = connect()

    # ── Reference sets ────────────────────────────────────────────────
    # import-batch-linked returns via status_events.source_file
    batch_returns = set()
    for r in c.execute(
        """
        SELECT DISTINCT return_id, source_file FROM status_events
        WHERE source_file IS NOT NULL AND source_file != ''
        """
    ):
        sf = r["source_file"] or ""
        if any(b in sf for b in BATCH_FILENAMES):
            batch_returns.add(int(r["return_id"]))

    c.execute("CREATE TEMP TABLE _batch_returns (id INTEGER PRIMARY KEY)")
    c.executemany(
        "INSERT OR IGNORE INTO _batch_returns(id) VALUES (?)",
        [(i,) for i in batch_returns],
    )
    clients_with_batch_return = {
        int(r[0])
        for r in c.execute(
            """
            SELECT DISTINCT r.client_id FROM returns r
            JOIN _batch_returns b ON b.id = r.id
            """
        )
    }

    # ssn_last4 frequency
    ssn_freq = Counter()
    for r in c.execute("SELECT ssn_last4 FROM clients WHERE ssn_last4 IS NOT NULL AND ssn_last4 != ''"):
        ssn_freq[str(r[0])] += 1
    # "repeated across many" — threshold: appear on ≥5 clients (1111 has 5)
    repeated_ssn = {s for s, n in ssn_freq.items() if n >= 5}

    # enrichment sets
    def _int_ids(sql: str) -> set[int]:
        out = set()
        for r in c.execute(sql):
            try:
                out.add(int(r[0]))
            except (TypeError, ValueError):
                continue
        return out

    has_spouse = _int_ids("SELECT DISTINCT client_id FROM spouses")
    has_dep = _int_ids("SELECT DISTINCT client_id FROM client_dependents")
    has_bill = _int_ids("SELECT DISTINCT client_id FROM client_billing")
    has_doc = _int_ids(
        """
        SELECT DISTINCT ret.client_id FROM return_documents d
        JOIN returns ret ON ret.id = d.return_id
        WHERE coalesce(d.is_deleted,0)=0
        """
    )
    has_ft = _int_ids(
        """
        SELECT DISTINCT ret.client_id FROM filetrack_status_history f
        JOIN returns ret ON ret.id = f.return_id
        WHERE f.return_id IS NOT NULL
        """
    )

    # returns info
    returns_by_client: dict[int, list] = defaultdict(list)
    for r in c.execute(
        "SELECT id, client_id, log_number, tax_year, created_at FROM returns"
    ):
        returns_by_client[int(r["client_id"])].append(dict(r))

    # INTAKE / APP sourced returns (manual)
    manual_source_returns = {
        int(r[0])
        for r in c.execute(
            """
            SELECT DISTINCT return_id FROM status_events
            WHERE upper(source_file) IN ('INTAKE','APP')
            """
        )
    }
    c.execute("CREATE TEMP TABLE _manual_returns (id INTEGER PRIMARY KEY)")
    c.executemany(
        "INSERT OR IGNORE INTO _manual_returns(id) VALUES (?)",
        [(i,) for i in manual_source_returns],
    )
    manual_clients = {
        int(r[0])
        for r in c.execute(
            """
            SELECT DISTINCT r.client_id FROM returns r
            JOIN _manual_returns m ON m.id = r.id
            """
        )
    }

    # audit_log user activity on returns → map to clients for "single user narrow window"
    # Prefer INTAKE events; also POST intake http (weak). Use status_events INTAKE timestamps.
    intake_events = list(
        c.execute(
            """
            SELECT se.return_id, se.event_timestamp, r.client_id
            FROM status_events se
            JOIN returns r ON r.id = se.return_id
            WHERE upper(se.source_file)='INTAKE'
            ORDER BY se.event_timestamp
            """
        )
    )
    intake_by_day = Counter()
    intake_clients_by_day: dict[str, set] = defaultdict(set)
    for r in intake_events:
        day = str(r["event_timestamp"] or "")[:10]
        intake_by_day[day] += 1
        intake_clients_by_day[day].add(int(r["client_id"]))

    # Clients created outside seed with no batch link:
    # Seed for clients is messy (04-21); user said 2026-04-29 seed batches.
    # Interpret: client has ZERO returns linked to batch source files,
    # AND client.created_at day is not in {2026-04-21, 2026-04-22} early bulk
    # AND not solely justified by 2026-07-01 Drake-like bulk (that HAS batch links).
    # Strict reading: created_at not on return-seed day (clients aren't on that day anyway)
    # → "outside seed batches + no import_batch link" = no batch-linked return.
    no_batch_link = set()
    for r in c.execute("SELECT id, created_at FROM clients"):
        cid = int(r["id"])
        if cid not in clients_with_batch_return:
            no_batch_link.add(cid)

    # Manual-created clients histogram (no batch link, not early 04-21/04-22 bulk without returns?)
    # User asked: created_at histogram for manually created clients
    # Define manual = INTAKE/APP source OR (no batch link AND created after 2026-04-29)
    early_bulk_days = {"2026-04-21", "2026-04-22"}
    july_bulk_day = "2026-07-01"

    clients = list(c.execute("SELECT * FROM clients"))
    scores: dict[int, dict] = {}

    signal_hits: Counter = Counter()

    for r in clients:
        cid = int(r["id"])
        last, first, display = r["last_name"], r["first_name"], r["display_name"]
        ssn = r["ssn_last4"]
        created = str(r["created_at"] or "")
        created_day = created[:10]
        rets = returns_by_client.get(cid, [])
        signals = []

        # 1) Name patterns
        for s in score_name(last, first, display):
            signals.append(s)

        # 2) ssn_last4
        if ssn is None or str(ssn).strip() == "":
            signals.append("ssn_null")
        else:
            s = str(ssn).strip()
            if s in {"0000", "1234"}:
                signals.append(f"ssn_sentinel_{s}")
            if s in repeated_ssn:
                signals.append("ssn_repeated_across_many")
            if len(set(s)) == 1 and len(s) == 4 and s.isdigit():
                signals.append("ssn_repeated_digit")  # 1111, 0000, etc.

        # 3) outside seed batches, no import_batch link
        if cid not in clients_with_batch_return:
            signals.append("no_import_batch_link")
            if created_day not in early_bulk_days and created_day != july_bulk_day:
                signals.append("created_outside_seed_and_bulk_no_batch")

        # 4) no returns / returns with no log_number
        if not rets:
            signals.append("no_returns")
        else:
            if all(
                (x["log_number"] in (None, "", "0", 0)) for x in rets
            ):
                signals.append("all_returns_no_log_number")
            if any(
                (x["log_number"] in (None, "", "0", 0)) for x in rets
            ) and not all(
                (x["log_number"] in (None, "", "0", 0)) for x in rets
            ):
                signals.append("some_returns_no_log_number")

        # 5) manual intake / APP
        if cid in manual_clients:
            signals.append("manual_intake_or_app_source")

        # 6) hollow enrichment
        hollow_bits = []
        if cid not in has_spouse:
            hollow_bits.append("no_spouses")
        if cid not in has_dep:
            hollow_bits.append("no_dependents")
        if cid not in has_bill:
            hollow_bits.append("no_billing")
        if cid not in has_doc:
            hollow_bits.append("no_documents")
        if cid not in has_ft:
            hollow_bits.append("no_filetrack")
        if len(hollow_bits) == 5:
            signals.append("hollow_no_spouse_dep_bill_doc_ft")
        # also record individual for transparency but don't count each as separate
        # "signal" for ranking — user asked them as one bullet

        signals = sorted(set(signals))
        for s in signals:
            signal_hits[s] += 1

        scores[cid] = {
            "client_id": cid,
            "created_at": created,
            "created_day": created_day,
            "ssn_last4_present": bool(ssn),
            "n_returns": len(rets),
            "signals": signals,
            "signal_count": len(signals),
            # PII-free name evidence
            "name_signals_only": [s for s in signals if s.startswith("name_")],
            "last_len": len((last or "").strip()),
            "first_len": len((first or "").strip()),
            "display_len": len((display or "").strip()),
        }

    # Manual-created histogram
    manual_hist = Counter()
    for cid, sc in scores.items():
        if "manual_intake_or_app_source" in sc["signals"] or (
            "no_import_batch_link" in sc["signals"]
            and sc["created_day"] not in early_bulk_days
            and sc["created_day"] != july_bulk_day
        ):
            manual_hist[sc["created_day"]] += 1

    # Probable test set: ≥2 signals, BUT ssn_null + hollow is extremely common
    # (735 null ssn). Need smarter combined rule.
    # User: "Score every client... report each signal's hit count separately as well as the combined set"
    # Combined = flagged by the scoring — I'll define probable as:
    #   A) any hard name_keyword / mash / sentinel ssn / repeated digit ssn
    #   OR B) ≥2 of {created_outside_seed_and_bulk_no_batch, no_returns, all_returns_no_log_number,
    #                manual_intake_or_app_source, name_*} 
    #   OR C) ≥3 signals excluding the ultra-common pair (ssn_null, hollow) alone
    #
    # Actually user said: report combined set — and "Cross-reference... flagged by ≥2 signals"
    # So combined = signal_count >= 2. But that's huge because ssn_null+hollow covers most.
    #
    # Report BOTH:
    #  - combined_ge2: all with ≥2 signals (raw)
    #  - probable_test: stricter — hard signals OR (≥2 excluding {ssn_null, hollow_...}))

    WEAK = {"ssn_null", "hollow_no_spouse_dep_bill_doc_ft", "some_returns_no_log_number"}
    HARD = {
        "name_keyword_testish",
        "name_keyboard_mash",
        "name_repeated_char",
        "name_no_vowel_mash",
        "name_blank",
        "ssn_sentinel_0000",
        "ssn_sentinel_1234",
        "ssn_repeated_digit",
    }

    combined_ge2 = []
    probable = []
    for cid, sc in scores.items():
        sigs = set(sc["signals"])
        if sc["signal_count"] >= 2:
            combined_ge2.append(cid)
        strong = sigs - WEAK
        if sigs & HARD:
            probable.append(cid)
        elif len(strong) >= 2:
            probable.append(cid)
        elif "created_outside_seed_and_bulk_no_batch" in sigs and (
            "no_returns" in sigs or "all_returns_no_log_number" in sigs
        ):
            probable.append(cid)

    probable_set = set(probable)

    # Rank probable by signal count
    ranked = sorted(
        [scores[cid] for cid in probable_set],
        key=lambda x: (-x["signal_count"], x["client_id"]),
    )

    # ── Cross-ref against 147 / 96 / 47 ────────────────────────────────
    # Rebuild 96 and 47 from prior methodology using audit DB + snapshot
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

    stage = {
        int(r["client_id"]): int(r["id"])
        for r in aconn.execute(
            "SELECT id, client_id FROM stage_taxops_client WHERE run_id=?", (run_id,)
        )
    }
    fail_clients = []
    for f in fails:
        row = c.execute(
            "SELECT client_id FROM returns WHERE id=?", (int(f["return_id"]),)
        ).fetchone()
        fail_clients.append(int(row["client_id"]))

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

    sides = _load_sides(aconn, run_id)
    log_indiv = [x for x in sides["log"] if not x.is_entity]
    from audit.match import _log_keys

    idx = defaultdict(list)
    for s in log_indiv:
        for k in _log_keys(s):
            idx[k].append(s)

    sibling_96 = set()
    ambiguous_47 = set()
    for cid in fail_clients:
        sid = client_to_stage.get(cid)
        if sid is None or sid in matched_to:
            continue
        sc = aconn.execute(
            "SELECT last_name, first_name FROM stage_taxops_client WHERE id=?", (sid,)
        ).fetchone()
        keys = set(keys_with_transposition(sc["last_name"] or "", sc["first_name"] or ""))
        cands, seen = [], set()
        for k in keys:
            for s in idx.get(k, []):
                if s.id not in seen:
                    seen.add(s.id)
                    cands.append(s)
        used = [s for s in cands if s.id in matched_log]
        unused = [s for s in cands if s.id not in matched_log]
        if used:
            other_stage = log_consumed_by[used[0].id]
            osc = aconn.execute(
                "SELECT last_name, first_name FROM stage_taxops_client WHERE id=?",
                (other_stage,),
            ).fetchone()
            same = ((osc["last_name"] or "").upper() == (sc["last_name"] or "").upper()) and (
                (osc["first_name"] or "").upper().split()[:1]
                == (sc["first_name"] or "").upper().split()[:1]
            )
            if same:
                sibling_96.add(cid)
        if unused:
            tkeys = normalize_person(sc["last_name"] or "", sc["first_name"] or "").match_keys
            if tkeys:
                from audit.match import _index_by_keys, _taxops_keys as tk

                taxops = sides["taxops"]
                same_key = _index_by_keys(taxops, tk).get(tkeys[0], [])
                unmatched_same = [t for t in same_key if t.id not in matched_to]
                if len(unmatched_same) > 1:
                    ambiguous_47.add(cid)

    def xref(cids: set[int], label: str):
        flagged_ge2 = [cid for cid in cids if scores[cid]["signal_count"] >= 2]
        flagged_probable = [cid for cid in cids if cid in probable_set]
        # also: the sibling/consumer partner
        return {
            "n": len(cids),
            "flagged_ge2_signals": len(flagged_ge2),
            "flagged_probable": len(flagged_probable),
            "signal_count_hist": dict(
                Counter(scores[cid]["signal_count"] for cid in cids)
            ),
            "top_signals": dict(
                Counter(
                    s for cid in cids for s in scores[cid]["signals"]
                ).most_common(12)
            ),
        }

    # Also check whether the WINNING sibling (consumer) is the test one
    consumers_of_96 = set()
    for cid in sibling_96:
        sid = client_to_stage[cid]
        sc = aconn.execute(
            "SELECT last_name, first_name FROM stage_taxops_client WHERE id=?", (sid,)
        ).fetchone()
        keys = set(keys_with_transposition(sc["last_name"] or "", sc["first_name"] or ""))
        for k in keys:
            for s in idx.get(k, []):
                if s.id in matched_log:
                    consumers_of_96.add(stage_to_client[log_consumed_by[s.id]])
                    break

    # ── Rate impact: exclude probable from TaxOps side ────────────────
    taxops_all = sides["taxops"]
    taxops_excl = [
        t
        for t in taxops_all
        if stage_to_client.get(t.id) not in probable_set
    ]
    # Baseline stored
    baseline_matched = len(stored)
    baseline_left = len(log_indiv)
    baseline_rate = round(baseline_matched / baseline_left, 4)

    m_excl, _, _ = match_sides(
        log_indiv, taxops_excl, _taxops_keys, require_same_entity_flag=False
    )
    # Also exclude combined_ge2 (too aggressive) for comparison
    ge2_set = set(combined_ge2)
    taxops_ge2 = [
        t for t in taxops_all if stage_to_client.get(t.id) not in ge2_set
    ]
    m_ge2, _, _ = match_sides(
        log_indiv, taxops_ge2, _taxops_keys, require_same_entity_flag=False
    )

    # Fresh baseline rerun for fair compare (same matcher code)
    m_base, _, _ = match_sides(
        log_indiv, taxops_all, _taxops_keys, require_same_entity_flag=False
    )

    # Hard-name-only set
    hard_only = {
        cid
        for cid, sc in scores.items()
        if set(sc["signals"]) & HARD
    }
    taxops_hard = [
        t for t in taxops_all if stage_to_client.get(t.id) not in hard_only
    ]
    m_hard, _, _ = match_sides(
        log_indiv, taxops_hard, _taxops_keys, require_same_entity_flag=False
    )

    report = {
        "universe": {"clients": len(clients), "returns": sum(len(v) for v in returns_by_client.values())},
        "signal_hit_counts": dict(signal_hits),
        "combined_ge2": {
            "n": len(combined_ge2),
            "note": (
                "Raw ≥2 signals. Inflated by common pair ssn_null + hollow "
                f"(ssn_null={signal_hits.get('ssn_null')}, "
                f"hollow={signal_hits.get('hollow_no_spouse_dep_bill_doc_ft')})."
            ),
        },
        "probable_test": {
            "n": len(probable_set),
            "definition": (
                "HARD name/ssn sentinel OR ≥2 non-weak signals OR "
                "(created_outside_seed_and_bulk_no_batch AND (no_returns OR all_returns_no_log_number)). "
                "Weak alone: ssn_null, hollow enrichment."
            ),
            "hard_signal_only_n": len(hard_only),
            "ranked_by_signal_count": [
                {
                    "client_id": r["client_id"],
                    "signal_count": r["signal_count"],
                    "signals": r["signals"],
                    "created_day": r["created_day"],
                    "n_returns": r["n_returns"],
                    "ssn_last4_present": r["ssn_last4_present"],
                }
                for r in ranked
            ],
        },
        "manual_created_histogram": {
            "definition": (
                "INTAKE/APP source OR (no batch link AND created outside "
                "2026-04-21/22 and 2026-07-01 bulks)"
            ),
            "by_day": dict(sorted(manual_hist.items())),
            "intake_events_by_day": dict(sorted(intake_by_day.items())),
            "intake_clients_by_day": {
                d: len(s) for d, s in sorted(intake_clients_by_day.items())
            },
            "note": (
                "clients.created_by does not exist; manual attribution uses "
                "status_events.source_file IN ('INTAKE','APP') and created_at/batch gaps. "
                "audit_log action=TEST (48) is user_id='test' against return_id=1 only — "
                "fixture traffic, not a client population."
            ),
        },
        "seed_context": {
            "return_created_2026_04_29": c.execute(
                "SELECT COUNT(*) FROM returns WHERE substr(created_at,1,10)='2026-04-29'"
            ).fetchone()[0],
            "clients_with_batch_linked_return": len(clients_with_batch_return),
            "clients_without_batch_link": len(no_batch_link),
            "client_created_day_hist": {
                str(r[0]): int(r[1])
                for r in c.execute(
                    "SELECT substr(created_at,1,10) d, COUNT(*) c FROM clients GROUP BY 1 ORDER BY c DESC"
                )
            },
            "july_01_note": (
                "368 clients stamped 2026-07-01 18:19:30 — real-looking names, all have returns, "
                "339/368 batch-linked. Treat as second import bulk, not test data."
            ),
        },
        "cross_ref_147": {
            "fail_returns": len(fails),
            "fail_unique_clients": len(set(fail_clients)),
            "sibling_96_same_name_consumer": xref(sibling_96, "96"),
            "ambiguous_47": xref(ambiguous_47, "47"),
            "consumers_of_96": xref(consumers_of_96, "consumers"),
            "fail_clients_all": xref(set(fail_clients), "all_fail"),
            "interpretation": (
                "If probable_test ∩ (96∪47) is small, the contention is genuine "
                "DUPLICATE_CLIENT pairs among production records, not seeded tests."
            ),
        },
        "rate_impact_one_to_one": {
            "stored_baseline": {
                "left_log": baseline_left,
                "matched": baseline_matched,
                "rate": baseline_rate,
            },
            "fresh_rerun_all_taxops": {
                "left_log": baseline_left,
                "matched": len(m_base),
                "rate": round(len(m_base) / baseline_left, 4),
                "taxops_right": len(taxops_all),
            },
            "exclude_probable_test": {
                "taxops_right": len(taxops_excl),
                "excluded": len(taxops_all) - len(taxops_excl),
                "matched": len(m_excl),
                "rate": round(len(m_excl) / baseline_left, 4),
                "delta_matched_vs_fresh": len(m_excl) - len(m_base),
                "delta_rate_vs_fresh": round(
                    len(m_excl) / baseline_left - len(m_base) / baseline_left, 4
                ),
            },
            "exclude_hard_signals_only": {
                "taxops_right": len(taxops_hard),
                "excluded": len(hard_only),
                "matched": len(m_hard),
                "rate": round(len(m_hard) / baseline_left, 4),
                "delta_matched_vs_fresh": len(m_hard) - len(m_base),
            },
            "exclude_combined_ge2_too_aggressive": {
                "taxops_right": len(taxops_ge2),
                "excluded": len(ge2_set),
                "matched": len(m_ge2),
                "rate": round(len(m_ge2) / baseline_left, 4),
                "note": "Shown for honesty — ge2 includes most null-ssn hollow clients.",
            },
        },
        "do_not_delete": True,
        "proposed_next": "Add clients.is_test via proposed_migration.sql; exclude from audits.",
    }

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    # Console summary without dumping full ranked list twice
    summary = {
        k: report[k]
        for k in (
            "universe",
            "signal_hit_counts",
            "combined_ge2",
            "manual_created_histogram",
            "seed_context",
            "cross_ref_147",
            "rate_impact_one_to_one",
        )
    }
    summary["probable_test_n"] = report["probable_test"]["n"]
    summary["probable_test_hard_only_n"] = report["probable_test"]["hard_signal_only_n"]
    summary["probable_top_20"] = report["probable_test"]["ranked_by_signal_count"][:20]
    print(json.dumps(summary, indent=2))
    aconn.close()
    c.close()


if __name__ == "__main__":
    main()
