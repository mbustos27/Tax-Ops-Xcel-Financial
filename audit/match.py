"""M3 — pluggable deterministic matching tiers (no last-4-alone matches)."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Optional

from audit.db import connect_audit
from audit.normalizer import (
    full_normalized_string,
    keys_with_transposition,
    normalize_drake_client_name,
    normalize_log_row,
    normalize_person,
    transposed_interpretation,
)
from audit.util import dumps


@dataclass
class Side:
    kind: str
    id: int
    last: str
    first: str
    display: str = ""
    last4: Optional[str] = None
    is_entity: bool = False


TierFn = Callable[[Side, list[Side]], Optional[tuple[Side, str, float]]]


def _index_by_keys(sides: list[Side], key_fn) -> dict[tuple[str, str], list[Side]]:
    idx: dict[tuple[str, str], list[Side]] = {}
    for s in sides:
        for k in key_fn(s):
            idx.setdefault(k, []).append(s)
    return idx


def _drake_keys(s: Side) -> list[tuple[str, str]]:
    n = normalize_drake_client_name(s.display or f"{s.last}, {s.first}")
    return n.match_keys


def _log_keys(s: Side) -> list[tuple[str, str]]:
    return keys_with_transposition(s.last, s.first)


def _taxops_keys(s: Side) -> list[tuple[str, str]]:
    return normalize_person(s.last or "", s.first or "").match_keys


def _break_ties(cands: list[Side], probe: Side) -> Optional[Side]:
    if len(cands) == 1:
        return cands[0]
    if not probe.last4:
        return None
    narrowed = [c for c in cands if c.last4 and c.last4 == probe.last4]
    if len(narrowed) == 1:
        return narrowed[0]
    return None


def tier_surname_first_token(probe: Side, pool: list[Side], pool_keys_fn) -> Optional[tuple[Side, str, float]]:
    """Tier 1: normalized surname + first token (exact key)."""
    pkeys = (
        _drake_keys(probe)
        if probe.kind == "drake"
        else _log_keys(probe)
        if probe.kind == "log"
        else _taxops_keys(probe)
    )
    if not pkeys:
        return None
    idx = _index_by_keys(pool, pool_keys_fn)
    # Prefer full surname variant (first in list)
    key = pkeys[0]
    cands = idx.get(key, [])
    hit = _break_ties(cands, probe) if cands else None
    if hit:
        return hit, "surname_first_token", 1.0 if len(cands) == 1 else 0.9
    return None


def tier_paternal_only(probe: Side, pool: list[Side], pool_keys_fn) -> Optional[tuple[Side, str, float]]:
    """Tier 2: paternal-only surname variant + first token."""
    if probe.kind == "drake":
        pkeys = _drake_keys(probe)
    elif probe.kind == "log":
        pkeys = _log_keys(probe)
    else:
        pkeys = _taxops_keys(probe)
    if len(pkeys) < 2:
        return None
    idx = _index_by_keys(pool, pool_keys_fn)
    for key in pkeys[1:]:
        cands = idx.get(key, [])
        hit = _break_ties(cands, probe) if cands else None
        if hit:
            return hit, "paternal_surname_first_token", 0.85 if len(cands) == 1 else 0.75
    return None


def tier_full_transposition(probe: Side, pool: list[Side], pool_keys_fn) -> Optional[tuple[Side, str, float]]:
    """Tier 3: full normalized name, transposition-tolerant."""
    if probe.kind == "drake":
        pn = normalize_drake_client_name(probe.display or f"{probe.last}, {probe.first}")
        pfull = full_normalized_string(pn)
    elif probe.kind == "log":
        pn = normalize_log_row(probe.last, probe.first)
        pfull = full_normalized_string(pn)
        pfull_t = full_normalized_string(transposed_interpretation(probe.last, probe.first))
    else:
        pn = normalize_person(probe.last or "", probe.first or "")
        pfull = full_normalized_string(pn)
        pfull_t = pfull

    for cand in pool:
        if cand.kind == "drake":
            cfull = full_normalized_string(
                normalize_drake_client_name(cand.display or f"{cand.last}, {cand.first}")
            )
            alts = [cfull]
        elif cand.kind == "log":
            alts = [
                full_normalized_string(normalize_log_row(cand.last, cand.first)),
                full_normalized_string(transposed_interpretation(cand.last, cand.first)),
            ]
        else:
            alts = [full_normalized_string(normalize_person(cand.last or "", cand.first or ""))]
        targets = [pfull]
        if probe.kind == "log":
            targets.append(pfull_t)
        if any(t and t in alts for t in targets):
            return cand, "full_normalized_transposition", 0.8
    return None


# Pluggable tier list — insert exact-key tier at index 0 later without restructuring.
DEFAULT_TIERS: list[tuple[str, callable]] = [
    ("surname_first_token", tier_surname_first_token),
    ("paternal_surname_first_token", tier_paternal_only),
    ("full_normalized_transposition", tier_full_transposition),
]


def _load_sides(conn: sqlite3.Connection, run_id: int) -> dict[str, list[Side]]:
    drake: list[Side] = []
    for r in conn.execute(
        "SELECT id, client_name_raw, id_last4, is_entity FROM stage_drake WHERE run_id=? AND dropped=0",
        (run_id,),
    ):
        n = normalize_drake_client_name(r["client_name_raw"])
        drake.append(
            Side(
                kind="drake",
                id=int(r["id"]),
                last=n.last_raw,
                first=n.first_raw,
                display=r["client_name_raw"],
                last4=r["id_last4"],
                is_entity=bool(r["is_entity"]),
            )
        )

    log: list[Side] = []
    for r in conn.execute(
        """
        SELECT id, last_raw, first_raw, is_entity_sheet, sheet_name
        FROM stage_log WHERE run_id=? AND dropped=0
        """,
        (run_id,),
    ):
        last = r["last_raw"] or ""
        first = r["first_raw"] or ""
        is_ent = bool(r["is_entity_sheet"])
        # XCEL rows with blank FIRST are entity-shaped — route to entity matching
        blank_first_entity = (
            (not is_ent)
            and bool(last.strip())
            and not first.strip()
            and (r["sheet_name"] or "").strip().upper().startswith("XCEL")
        )
        if blank_first_entity:
            is_ent = True
        log.append(
            Side(
                kind="log",
                id=int(r["id"]),
                last=last,
                first=first,
                display=last if is_ent else f"{last}, {first}",
                is_entity=is_ent,
            )
        )

    taxops: list[Side] = []
    for r in conn.execute(
        "SELECT id, client_id, last_name, first_name, ssn_last4 FROM stage_taxops_client WHERE run_id=?",
        (run_id,),
    ):
        taxops.append(
            Side(
                kind="taxops",
                id=int(r["id"]),
                last=r["last_name"] or "",
                first=r["first_name"] or "",
                last4=r["ssn_last4"],
            )
        )
    return {"drake": drake, "log": log, "taxops": taxops}


def match_sides(
    lefts: list[Side],
    rights: list[Side],
    right_key_fn,
    *,
    require_same_entity_flag: bool = True,
    tiers=None,
) -> tuple[list[tuple[Side, Side, str, float]], list[Side], list[Side]]:
    tiers = tiers or DEFAULT_TIERS
    used_right: set[int] = set()
    matches: list[tuple[Side, Side, str, float]] = []

    for left in lefts:
        pool = [
            r
            for r in rights
            if r.id not in used_right
            and (not require_same_entity_flag or r.is_entity == left.is_entity)
        ]
        hit = None
        for _name, fn in tiers:
            # tier functions expect (probe, pool, pool_keys_fn)
            if fn in (tier_surname_first_token, tier_paternal_only):
                hit = fn(left, pool, right_key_fn)
            else:
                hit = fn(left, pool, right_key_fn)
            if hit:
                break
        if not hit:
            continue
        right, tier, conf = hit
        # Guard: never accept a match that used last4 alone (no name key).
        # All tiers here are name-based; last4 only in _break_ties.
        matches.append((left, right, tier, conf))
        used_right.add(right.id)

    matched_l = {m[0].id for m in matches}
    matched_r = {m[1].id for m in matches}
    residual_l = [x for x in lefts if x.id not in matched_l]
    residual_r = [x for x in rights if x.id not in matched_r]
    return matches, residual_l, residual_r


def _entity_key(text: str) -> str:
    """Primary entity key via normalize_entity (separate from person path)."""
    from audit.normalizer import normalize_entity

    keys = normalize_entity(text or "").keys
    return keys[0] if keys else ""


def _entity_all_keys(text: str) -> list[str]:
    from audit.normalizer import normalize_entity

    return list(normalize_entity(text or "").keys)


def match_entities(lefts: list[Side], rights: list[Side]) -> tuple[list, list, list]:
    """Entity-name match using normalize_entity keys.

    Strategy (no last-4-alone):
      1. Unique hit on suffix-preserved key
      2. Unique hit on stripped key
      3. Multi stripped-key candidates → unique among those sharing a suffix key
      4. last-4 tie-break only among remaining multi-candidate sets
    """
    from audit.normalizer import normalize_entity

    idx_suf: dict[str, list[Side]] = {}
    idx_strip: dict[str, list[Side]] = {}
    meta: dict[int, object] = {}
    for r in rights:
        ne = normalize_entity(r.display or r.last)
        meta[r.id] = ne
        for k in ne.keys_with_suffix:
            idx_suf.setdefault(k, []).append(r)
        for k in ne.keys:
            idx_strip.setdefault(k, []).append(r)

    used: set[int] = set()
    matches = []

    def _take(left: Side, cand: Side, tier: str, conf: float) -> None:
        matches.append((left, cand, tier, conf))
        used.add(cand.id)

    for left in lefts:
        ne = normalize_entity(left.display or left.last)
        # 1) suffix-preserved unique
        hit = None
        for k in ne.keys_with_suffix:
            cands = [c for c in idx_suf.get(k, []) if c.id not in used]
            if len(cands) == 1:
                hit = (cands[0], "entity_key_with_suffix", 1.0)
                break
        if hit:
            _take(left, hit[0], hit[1], hit[2])
            continue

        # 2) stripped unique
        cand_map: dict[int, Side] = {}
        for k in ne.keys:
            for c in idx_strip.get(k, []):
                if c.id not in used:
                    cand_map[c.id] = c
        cands = list(cand_map.values())
        if len(cands) == 1:
            _take(left, cands[0], "entity_key_stripped", 0.95)
            continue
        if not cands:
            continue

        # 3) disambiguate via suffix key overlap
        if ne.keys_with_suffix:
            narrowed = []
            for c in cands:
                cne = meta[c.id]
                if set(ne.keys_with_suffix) & set(cne.keys_with_suffix):
                    narrowed.append(c)
            if len(narrowed) == 1:
                _take(left, narrowed[0], "entity_key_stripped_suffix_disambig", 0.9)
                continue
            if narrowed:
                cands = narrowed

        # 4) last4 tie-break only
        if left.last4 and len(cands) > 1:
            by4 = [c for c in cands if c.last4 == left.last4]
            if len(by4) == 1:
                _take(left, by4[0], "entity_key_last4_tiebreak", 0.85)
                continue

    matched_l = {m[0].id for m in matches}
    matched_r = {m[1].id for m in matches}
    return (
        matches,
        [x for x in lefts if x.id not in matched_l],
        [x for x in rights if x.id not in matched_r],
    )


def run_match(audit_db, run_id: int) -> dict:
    conn = connect_audit(audit_db)
    try:
        sides = _load_sides(conn, run_id)
        # Individuals vs XCEL; entities vs business sheets — already flagged on sides
        pairs_spec = [
            ("DRAKE_LOG", sides["drake"], sides["log"], _log_keys, True),
            ("DRAKE_TAXOPS", sides["drake"], sides["taxops"], _taxops_keys, False),
            (
                "LOG_TAXOPS",
                [x for x in sides["log"] if not x.is_entity],
                sides["taxops"],
                _taxops_keys,
                False,
            ),
        ]
        summary = {}
        for pair, lefts, rights, rk, ent in pairs_spec:
            # For DRAKE_LOG split entity/individual for cleaner rates
            if pair == "DRAKE_LOG":
                for label, lf, rf, matcher in (
                    (
                        "indiv",
                        [x for x in lefts if not x.is_entity],
                        [x for x in rights if not x.is_entity],
                        "person",
                    ),
                    (
                        "entity",
                        [x for x in lefts if x.is_entity],
                        [x for x in rights if x.is_entity],
                        "entity",
                    ),
                ):
                    if matcher == "entity":
                        matches, res_l, res_r = match_entities(lf, rf)
                    else:
                        matches, res_l, res_r = match_sides(
                            lf, rf, rk, require_same_entity_flag=False
                        )
                    for a, b, tier, conf in matches:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO audit_match (
                              run_id, pair, left_kind, left_id, right_kind, right_id,
                              tier, confidence, sources_agree
                            ) VALUES (?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                run_id, f"{pair}:{label}", a.kind, a.id, b.kind, b.id,
                                tier, conf, dumps({"pair": pair, "segment": label}),
                            ),
                        )
                    denom = len(lf) or 1
                    summary[f"{pair}:{label}"] = {
                        "left": len(lf),
                        "matched": len(matches),
                        "rate": round(len(matches) / denom, 4),
                        "residual_left": len(res_l),
                        "residual_right": len(res_r),
                    }
                    for r in res_l:
                        conn.execute(
                            """
                            INSERT INTO audit_residual (
                              run_id, pair, left_kind, left_id, failure_cause, detail_json
                            ) VALUES (?,?,?,?,?,?)
                            """,
                            (
                                run_id, f"{pair}:{label}", r.kind, r.id,
                                "no_deterministic_tier", dumps({"segment": label}),
                            ),
                        )
            else:
                matches, res_l, res_r = match_sides(
                    lefts, rights, rk, require_same_entity_flag=False
                )
                for a, b, tier, conf in matches:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO audit_match (
                          run_id, pair, left_kind, left_id, right_kind, right_id,
                          tier, confidence, sources_agree
                        ) VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            run_id, pair, a.kind, a.id, b.kind, b.id,
                            tier, conf, dumps({"pair": pair}),
                        ),
                    )
                denom = len(lefts) or 1
                summary[pair] = {
                    "left": len(lefts),
                    "matched": len(matches),
                    "rate": round(len(matches) / denom, 4),
                    "residual_left": len(res_l),
                    "residual_right": len(res_r),
                }
                for r in res_l:
                    conn.execute(
                        """
                        INSERT INTO audit_residual (
                          run_id, pair, left_kind, left_id, failure_cause, detail_json
                        ) VALUES (?,?,?,?,?,?)
                        """,
                        (run_id, pair, r.kind, r.id, "no_deterministic_tier", dumps({})),
                    )
        conn.commit()
        return summary
    finally:
        conn.close()


def drake_log_match_rate(drake_names: list[str], log_rows: list[tuple[str, str]]) -> dict:
    """
    Harness for M1 acceptance: Drake Client Name list vs Log (last, first) pairs.
    Returns rate of Drake rows matched to some log row.
    """
    lefts = [
        Side(kind="drake", id=i, last="", first="", display=name)
        for i, name in enumerate(drake_names, 1)
    ]
    # populate last/first from parser
    for s in lefts:
        n = normalize_drake_client_name(s.display)
        s.last, s.first = n.last_raw, n.first_raw
    rights = [
        Side(kind="log", id=i, last=a, first=b)
        for i, (a, b) in enumerate(log_rows, 1)
    ]
    matches, res_l, res_r = match_sides(lefts, rights, _log_keys, require_same_entity_flag=False)
    return {
        "drake": len(lefts),
        "log": len(rights),
        "matched": len(matches),
        "rate": (len(matches) / len(lefts)) if lefts else 0.0,
        "residual": len(res_l),
    }
