"""M4 — spouse parse, provenance, and internal store divergence (report only)."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Optional

from audit.db import connect_audit
from audit.match import _log_keys, _load_sides, match_sides
from audit.normalizer import (
    clean_tokens,
    is_single_letter,
    normalize_drake_client_name,
    split_drake_client_name,
    split_log_names,
    strip_trailing_initials,
)
from audit.util import dumps


@dataclass
class SpouseParse:
    cls: str  # SHARED | SHARED_MI | EXPLICIT | TRUNCATED | NONE
    spouse_first: str
    spouse_last: str
    needs_review: bool = False


def classify_drake_spouse(client_name: str) -> SpouseParse:
    n = normalize_drake_client_name(client_name)
    if not n.is_joint:
        return SpouseParse("NONE", "", "")
    chunk = n.spouse_chunk
    toks = clean_tokens(chunk)
    trunc = n.truncated
    if trunc and n.truncation_unreliable_tail:
        # May still parse what we have
        pass

    if not toks:
        return SpouseParse("TRUNCATED" if trunc else "NONE", "", "", needs_review=True)

    if len(toks) == 1:
        # Single token spouse → shares taxpayer surname
        return SpouseParse(
            "SHARED",
            toks[0],
            n.surname_full,
            needs_review=bool(trunc),
        )

    # If last token is single letter → middle initial on spouse first name
    if is_single_letter(toks[-1]) and len(toks) == 2:
        return SpouseParse("SHARED_MI", toks[0], n.surname_full, needs_review=bool(trunc))

    # Spouse with own LAST, FIRST shape already handled by comma in chunk? rare
    if "," in chunk:
        sl, sf, _ = split_drake_client_name(chunk)
        return SpouseParse("EXPLICIT", sf, sl, needs_review=bool(trunc))

    # Multiple tokens: first = given, rest = surname (EXPLICIT) unless only MI
    given = toks[0]
    rest = toks[1:]
    rest = strip_trailing_initials(rest)
    if not rest:
        return SpouseParse("SHARED_MI", given, n.surname_full, needs_review=bool(trunc))
    return SpouseParse(
        "TRUNCATED" if trunc else "EXPLICIT",
        given,
        " ".join(rest),
        needs_review=bool(trunc and len(client_name.rstrip()) >= 39),
    )


def classify_log_spouse(last_raw: str, first_raw: str) -> SpouseParse:
    last, first, spouse = split_log_names(last_raw, first_raw)
    if not spouse:
        return SpouseParse("NONE", "", "")
    toks = clean_tokens(spouse)
    if not toks:
        return SpouseParse("NONE", "", "")
    # Dual surname in LAST: "SUR1 & SUR2"
    last_parts = [p.strip() for p in re.split(r"\s*&\s*", last_raw or "") if p.strip()]
    if len(toks) == 1:
        # Spouse first only; surname from shared last or second last
        sp_last = clean_tokens(last_parts[-1]) if last_parts else clean_tokens(last)
        return SpouseParse("SHARED", toks[0], " ".join(sp_last))
    return SpouseParse("EXPLICIT", toks[0], " ".join(toks[1:]))


def run_spouse(audit_db, run_id: int) -> dict:
    conn = connect_audit(audit_db)
    try:
        # Build Drake↔Log individual matches for corroboration
        sides = _load_sides(conn, run_id)
        d_indiv = [x for x in sides["drake"] if not x.is_entity]
        l_indiv = [x for x in sides["log"] if not x.is_entity]
        matches, _, _ = match_sides(d_indiv, l_indiv, _log_keys, require_same_entity_flag=False)
        log_by_drake = {a.id: b for a, b, _, _ in matches}

        class_counts = {"SHARED": 0, "SHARED_MI": 0, "EXPLICIT": 0, "TRUNCATED": 0}
        recovered = 0
        couples = 0
        for d in d_indiv:
            parsed = classify_drake_spouse(d.display)
            if parsed.cls == "NONE":
                continue
            couples += 1
            class_counts[parsed.cls] = class_counts.get(parsed.cls, 0) + 1

            provenance = "INFERRED"
            needs = 1 if parsed.needs_review else 0
            notes = None
            log_id = None
            log_side = log_by_drake.get(d.id)
            if log_side:
                log_id = log_side.id
                log_p = classify_log_spouse(log_side.last, log_side.first)
                if log_p.cls != "NONE":
                    # Corroboration: first token agreement
                    if log_p.spouse_first and parsed.spouse_first:
                        if log_p.spouse_first == parsed.spouse_first:
                            provenance = "OBSERVED"
                        else:
                            provenance = "OBSERVED"
                            notes = "spouse_first_conflict"
                            needs = 1
                    # Truncated recovery from log
                    if parsed.cls == "TRUNCATED" and log_p.spouse_last:
                        parsed = SpouseParse(
                            "EXPLICIT",
                            parsed.spouse_first or log_p.spouse_first,
                            log_p.spouse_last,
                            needs_review=False,
                        )
                        provenance = "OBSERVED"
                        needs = 0
                elif parsed.cls in ("SHARED", "SHARED_MI"):
                    provenance = "INFERRED"
                else:
                    provenance = "OBSERVED" if parsed.cls == "EXPLICIT" else "INFERRED"
            else:
                if parsed.cls == "EXPLICIT":
                    provenance = "OBSERVED"
                elif parsed.cls in ("SHARED", "SHARED_MI"):
                    provenance = "INFERRED"
                else:
                    provenance = "INFERRED"
                    needs = 1

            if parsed.spouse_first and (parsed.spouse_last or parsed.cls in ("SHARED", "SHARED_MI", "EXPLICIT")):
                if parsed.cls != "TRUNCATED" or (parsed.spouse_last and not needs):
                    recovered += 1
            if parsed.cls == "TRUNCATED" and needs:
                notes = (notes or "") + "|unrecovered_truncation"

            conn.execute(
                """
                INSERT INTO audit_spouse (
                  run_id, drake_stage_id, log_stage_id, spouse_class,
                  spouse_first, spouse_last, provenance, needs_review, notes
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id, d.id, log_id, parsed.cls,
                    parsed.spouse_first or None, parsed.spouse_last or None,
                    provenance, needs, notes,
                ),
            )

        # Internal divergence: clients.spouse_* vs spouses table
        div_n = 0
        rows = conn.execute(
            """
            SELECT c.client_id,
                   c.spouse_first_name AS cf, c.spouse_last_name AS cl,
                   s.first_name AS sf, s.last_name AS sl
            FROM stage_taxops_client c
            LEFT JOIN stage_taxops_spouse s
              ON s.run_id = c.run_id AND s.client_id = c.client_id
            WHERE c.run_id = ?
              AND (
                (c.spouse_first_name IS NOT NULL AND c.spouse_first_name != '')
                OR (c.spouse_last_name IS NOT NULL AND c.spouse_last_name != '')
                OR s.spouse_row_id IS NOT NULL
              )
            """,
            (run_id,),
        ).fetchall()
        for r in rows:
            cf, cl = (r["cf"] or "").strip(), (r["cl"] or "").strip()
            sf, sl = (r["sf"] or "").strip(), (r["sl"] or "").strip()
            has_c = bool(cf or cl)
            has_s = bool(sf or sl)
            if has_c and has_s:
                if cf.upper() == sf.upper() and cl.upper() == sl.upper():
                    continue  # agree — skip
                better = "spouses"
                note = "both_present_disagree"
            elif has_s and not has_c:
                better = "spouses"
                note = "only_spouses_table"
                # Still record for completeness of divergence inventory? Spec:
                # "which rows disagree" — only_spouses is divergence of representation
                # Include only when clients has partial or conflict. Skip pure only_spouses
                # to avoid 169 noise? Spec: "Report which rows disagree between the two
                # representations". only_spouses means clients empty — that's divergence.
                pass
            elif has_c and not has_s:
                better = "clients.spouse_*"
                note = "only_clients_columns"
            else:
                continue
            # Record all representation gaps
            conn.execute(
                """
                INSERT INTO audit_spouse_store_div (
                  run_id, client_id, clients_first, clients_last,
                  spouses_first, spouses_last, better_store, notes
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (run_id, int(r["client_id"]), cf or None, cl or None,
                 sf or None, sl or None, better, note),
            )
            div_n += 1

        conn.commit()
        return {
            "couples": couples,
            "class_counts": class_counts,
            "recovered": recovered,
            "baseline_target_recovered": 332,
            "baseline_couples": 345,
            "store_divergence_rows": div_n,
        }
    finally:
        conn.close()
