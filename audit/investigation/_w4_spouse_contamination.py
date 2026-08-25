"""Step 1 — cross-client spouse contamination (read-only).

Scans:
  1. All ``spouses`` rows (every source) — spouse name vs other clients / other spouses
  2. ``wave4_clients_fold`` subset (user-requested rate)
  3. ``clients.spouse_*`` pre-fold columns
  4. Drake-import smoking gun: ``spouses.taxpayer_name`` tokens disjoint from owner client name

No TaxOps / disposition writes.
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TAXOPS = Path(r"T:\taxops\taxops.db")
DISP = Path(r"T:\audit\audit_disposition.sqlite")
OUT_JSON = Path(r"T:\audit\investigation\W4-spouse-contamination.json")
OUT_MD = Path(r"T:\audit\investigation\W4-spouse-contamination.md")


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tokens(s: str) -> frozenset[str]:
    return frozenset(t for t in re.findall(r"[A-Z0-9]+", _norm(s)) if len(t) > 1)


def _person_key(last: str, first: str) -> str:
    return f"{_norm(last)}|{_norm(first)}"


def _bag_key(last: str, first: str) -> frozenset[str]:
    return _tokens(f"{last} {first}")


def _is_sentinel(last: str, first: str) -> bool:
    if not (_norm(last) or _norm(first)):
        return True
    return _norm(last) in ("UNKNOWN", "NONE", "TAXPAYER") and _norm(first) in (
        "",
        "UNKNOWN",
        "NONE",
        "SPOUSE",
    )


def main() -> None:
    conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    clients = list(
        conn.execute(
            "SELECT id, last_name, first_name, spouse_last_name, spouse_first_name, ssn_last4 "
            "FROM clients"
        )
    )
    # Include taxpayer_name if present
    cols = {r[1] for r in conn.execute("PRAGMA table_info(spouses)")}
    has_tp = "taxpayer_name" in cols
    sql = (
        "SELECT id, client_id, first_name, last_name, source"
        + (", taxpayer_name" if has_tp else ", NULL AS taxpayer_name")
        + " FROM spouses"
    )
    spouses = list(conn.execute(sql))
    by_id = {c["id"]: c for c in clients}
    fold = [s for s in spouses if (s["source"] or "") == "wave4_clients_fold"]
    by_source = Counter((s["source"] or "(blank)") for s in spouses)

    client_by_pk: dict[str, list[int]] = defaultdict(list)
    client_by_bag: dict[frozenset[str], list[int]] = defaultdict(list)
    for c in clients:
        pk = _person_key(c["last_name"] or "", c["first_name"] or "")
        if pk != "|":
            client_by_pk[pk].append(c["id"])
        bag = _bag_key(c["last_name"] or "", c["first_name"] or "")
        if len(bag) >= 2:
            client_by_bag[bag].append(c["id"])

    spouse_pk_owners: dict[str, list[dict]] = defaultdict(list)
    spouse_bag_owners: dict[frozenset[str], list[dict]] = defaultdict(list)

    def _index_spouse(owner_id: int, s_last: str, s_first: str, *, via: str) -> None:
        if _is_sentinel(s_last, s_first):
            return
        meta = {
            "owner_id": owner_id,
            "spouse_last": s_last,
            "spouse_first": s_first,
            "via": via,
        }
        pk = _person_key(s_last, s_first)
        if pk != "|":
            spouse_pk_owners[pk].append(meta)
        bag = _bag_key(s_last, s_first)
        if len(bag) >= 2:
            spouse_bag_owners[bag].append(meta)

    for s in spouses:
        _index_spouse(
            s["client_id"],
            s["last_name"] or "",
            s["first_name"] or "",
            via=f"spouses:{s['source'] or ''}",
        )
    for c in clients:
        cl = (c["spouse_last_name"] or "").strip()
        cf = (c["spouse_first_name"] or "").strip()
        if cl or cf:
            _index_spouse(c["id"], cl, cf, via="clients.spouse_*")

    def lookup_other_clients(owner_id: int, s_last: str, s_first: str) -> list[dict]:
        out, seen = [], set()
        pk = _person_key(s_last, s_first)
        bag = _bag_key(s_last, s_first)
        ids = list(client_by_pk.get(pk, []))
        if len(bag) >= 2:
            ids.extend(client_by_bag.get(bag, []))
        # also: spouse first+last appear as tokens inside a joint client name
        # (FOUZIA inside "ZULQARNIAN & FOUZIA") — surname match + given in bag
        if _tokens(s_last) and _tokens(s_first):
            for c in clients:
                if c["id"] == owner_id:
                    continue
                cbag = _bag_key(c["last_name"] or "", c["first_name"] or "")
                if _tokens(s_last) <= cbag and not _tokens(s_first).isdisjoint(cbag):
                    ids.append(c["id"])
        for oid in ids:
            if oid == owner_id or oid in seen:
                continue
            seen.add(oid)
            oc = by_id[oid]
            out.append(
                {
                    "kind": "matches_other_client",
                    "other_client_id": oid,
                    "other_name": f"{oc['last_name']}, {oc['first_name']}",
                    "other_last4": oc["ssn_last4"],
                }
            )
        return out

    def lookup_other_spouses(owner_id: int, s_last: str, s_first: str) -> list[dict]:
        out, seen = [], set()
        pk = _person_key(s_last, s_first)
        bag = _bag_key(s_last, s_first)
        metas = list(spouse_pk_owners.get(pk, []))
        if len(bag) >= 2:
            metas.extend(spouse_bag_owners.get(bag, []))
        for m in metas:
            if m["owner_id"] == owner_id:
                continue
            key = (m["owner_id"], m["via"], m["spouse_last"], m["spouse_first"])
            if key in seen:
                continue
            seen.add(key)
            oc = by_id.get(m["owner_id"])
            out.append(
                {
                    "kind": "matches_other_spouse",
                    "other_client_id": m["owner_id"],
                    "other_client_name": (
                        f"{oc['last_name']}, {oc['first_name']}" if oc else None
                    ),
                    "other_spouse": f"{m['spouse_last']}, {m['spouse_first']}",
                    "via": m["via"],
                }
            )
        return out

    def taxpayer_name_mismatch(owner_id: int, taxpayer_name: str | None) -> dict | None:
        if not taxpayer_name or not str(taxpayer_name).strip():
            return None
        owner = by_id.get(owner_id)
        if not owner:
            return None
        obag = _bag_key(owner["last_name"] or "", owner["first_name"] or "")
        tbag = _tokens(str(taxpayer_name))
        if not obag or not tbag:
            return None
        # Mis-attached if owner surname tokens are disjoint from taxpayer_name
        owner_last = _tokens(owner["last_name"] or "")
        if owner_last and owner_last.isdisjoint(tbag):
            return {
                "kind": "taxpayer_name_disjoint_from_owner",
                "taxpayer_name": taxpayer_name,
                "owner_name": f"{owner['last_name']}, {owner['first_name']}",
                "shared_tokens": sorted(obag & tbag),
            }
        return None

    def analyze_spouse_row(s: sqlite3.Row) -> dict[str, Any] | None:
        owner_id = s["client_id"]
        s_last, s_first = s["last_name"] or "", s["first_name"] or ""
        if _is_sentinel(s_last, s_first):
            return None
        owner = by_id.get(owner_id)
        if not owner:
            return None
        other_client = lookup_other_clients(owner_id, s_last, s_first)
        other_spouse = lookup_other_spouses(owner_id, s_last, s_first)
        tp_mis = taxpayer_name_mismatch(owner_id, s["taxpayer_name"])
        if not other_client and not other_spouse and not tp_mis:
            return None
        return {
            "spouse_row_id": s["id"],
            "source": s["source"],
            "owner_client_id": owner_id,
            "owner_name": f"{owner['last_name']}, {owner['first_name']}",
            "owner_last4": owner["ssn_last4"],
            "spouse_name": f"{s_last}, {s_first}",
            "taxpayer_name": s["taxpayer_name"],
            "matches_other_client_n": len(other_client),
            "matches_other_spouse_n": len(other_spouse),
            "matches_other_client": other_client[:6],
            "matches_other_spouse": other_spouse[:6],
            "taxpayer_name_mismatch": tp_mis,
            "personnel": bool(other_client) or bool(tp_mis),
            "severity_reasons": [
                x
                for x in (
                    "matches_other_client" if other_client else None,
                    "taxpayer_name_mismatch" if tp_mis else None,
                )
                if x
            ],
        }

    all_hits = []
    for s in spouses:
        hit = analyze_spouse_row(s)
        if hit:
            all_hits.append(hit)

    fold_hits = [h for h in all_hits if (h["source"] or "") == "wave4_clients_fold"]
    drake_hits = [h for h in all_hits if "drake" in (h["source"] or "").lower()]
    tp_mismatch_hits = [h for h in all_hits if h.get("taxpayer_name_mismatch")]
    personnel_all = [h for h in all_hits if h["personnel"]]
    personnel_fold = [h for h in fold_hits if h["personnel"]]
    personnel_drake = [h for h in drake_hits if h["personnel"]]

    # clients.spouse_* column scan
    col_hits = []
    for c in clients:
        cl = (c["spouse_last_name"] or "").strip()
        cf = (c["spouse_first_name"] or "").strip()
        if _is_sentinel(cl, cf):
            continue
        other_client = lookup_other_clients(c["id"], cl, cf)
        other_spouse = lookup_other_spouses(c["id"], cl, cf)
        if not other_client and not other_spouse:
            continue
        col_hits.append(
            {
                "owner_client_id": c["id"],
                "owner_name": f"{c['last_name']}, {c['first_name']}",
                "owner_last4": c["ssn_last4"],
                "spouse_name": f"{cl}, {cf}",
                "matches_other_client": other_client[:6],
                "matches_other_spouse": other_spouse[:6],
                "personnel": bool(other_client),
            }
        )
    col_personnel = [h for h in col_hits if h["personnel"]]
    filled_cols = sum(
        1
        for c in clients
        if (c["spouse_last_name"] or "").strip() or (c["spouse_first_name"] or "").strip()
    )

    # Divergence overlap on personnel last4
    divs = []
    div_contam = []
    if DISP.exists():
        dconn = sqlite3.connect(f"file:{DISP}?mode=ro", uri=True)
        dconn.row_factory = sqlite3.Row
        divs = list(
            dconn.execute(
                "SELECT finding_id, entity_key, status FROM audit_disposition "
                "WHERE finding_type='SPOUSE_STORE_DIVERGENCE' AND status IN ('OPEN','ACKED')"
            )
        )
        dconn.close()
        by_last4: dict[str, list] = defaultdict(list)
        for h in personnel_all:
            if h.get("owner_last4") is not None:
                by_last4[str(h["owner_last4"]).zfill(4)].append(h)
        for d in divs:
            ek = d["entity_key"] or ""
            last4 = next((p for p in ek.split("|") if re.fullmatch(r"\d{4}", p or "")), None)
            hits = by_last4.get(last4 or "") or []
            if hits:
                div_contam.append({"finding_id": d["finding_id"], "entity_key": ek, "n": len(hits)})

    # Explicit W5 trio
    w5_last4 = {"5949", "5180", "3527"}
    w5_examples = [h for h in all_hits if str(h.get("owner_last4") or "").zfill(4) in w5_last4]

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "read_only": True,
        "universe": {
            "spouses_total": len(spouses),
            "spouses_by_source": dict(by_source),
            "wave4_fold_rows": len(fold),
            "clients_with_spouse_cols": filled_cols,
            "spouse_store_divergence_open": len(divs),
        },
        "all_spouses": {
            "n_hits": len(all_hits),
            "n_personnel": len(personnel_all),
            "n_taxpayer_name_mismatch": len(tp_mismatch_hits),
            "rate_personnel": round(len(personnel_all) / len(spouses), 4) if spouses else 0,
        },
        "wave4_fold": {
            "n_rows": len(fold),
            "n_hits": len(fold_hits),
            "n_personnel": len(personnel_fold),
            "rate_personnel": round(len(personnel_fold) / len(fold), 4) if fold else 0,
        },
        "drake_import": {
            "n_personnel": len(personnel_drake),
            "n_taxpayer_name_mismatch": sum(
                1 for h in drake_hits if h.get("taxpayer_name_mismatch")
            ),
            "note": (
                "Mis-attached Drake spouse rows: spouses.taxpayer_name belongs to a "
                "different household than spouses.client_id."
            ),
        },
        "clients_spouse_cols": {
            "n_personnel": len(col_personnel),
            "rate_personnel_of_filled": round(len(col_personnel) / filled_cols, 4)
            if filled_cols
            else 0,
        },
        "divergence_overlap_personnel": {
            "n": len(div_contam),
            "open_total": len(divs),
        },
        "w5_known_examples": w5_examples,
        "personnel_hits": personnel_all,
        "taxpayer_name_mismatch_hits": tp_mismatch_hits,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# W4 — cross-client spouse contamination",
        "",
        f"_Generated: {payload['generated_at']} · **read-only** · no TaxOps writes_",
        "",
        "## Verdict",
        "",
        "Two distinct defect classes:",
        "",
        "1. **Drake-import mis-attachment** — `spouses.taxpayer_name` names a *different* "
        "household than `spouses.client_id`. This is how MARTINEZ/QUINTANA/MUNGUIA got "
        "someone else's spouse. Source = `TY2025 Drake import`, **not** the Wave 4 fold.",
        "2. **Fold propagation** — `wave4_clients_fold` copied pre-existing `clients.spouse_*` "
        "values; fold personnel rate is low vs Drake-import.",
        "",
        f"| Universe | Rows | Personnel (wrong person) | Rate |",
        f"|---|---:|---:|---:|",
        f"| All `spouses` | {len(spouses)} | {len(personnel_all)} | {payload['all_spouses']['rate_personnel']:.1%} |",
        f"| `wave4_clients_fold` | {len(fold)} | {len(personnel_fold)} | {payload['wave4_fold']['rate_personnel']:.1%} |",
        f"| Drake import (personnel subset) | — | {len(personnel_drake)} | — |",
        f"| `taxpayer_name` disjoint from owner | — | {len(tp_mismatch_hits)} | — |",
        f"| `clients.spouse_*` filled | {filled_cols} | {len(col_personnel)} | {payload['clients_spouse_cols']['rate_personnel_of_filled']:.1%} |",
        "",
        f"Open `SPOUSE_STORE_DIVERGENCE` overlapping personnel contam: "
        f"**{len(div_contam)}** / {len(divs)}.",
        "",
        "## W5 known examples (confirm class)",
        "",
        "| Owner | Spouse on record | Source | taxpayer_name | Signal |",
        "|---|---|---|---|---|",
    ]
    for h in w5_examples:
        sig = ", ".join(h.get("personnel_reasons") or []) or "spouse-share"
        lines.append(
            f"| {h['owner_name']} (`{h['owner_client_id']}`, last4 `{h['owner_last4']}`) | "
            f"{h['spouse_name']} | `{h['source']}` | {h.get('taxpayer_name') or '—'} | {sig} |"
        )

    lines += [
        "",
        f"## All personnel hits ({len(personnel_all)})",
        "",
        "| Owner id | Owner | Spouse | Source | Reasons |",
        "|---:|---|---|---|---|",
    ]
    for h in sorted(personnel_all, key=lambda x: (x.get("source") or "", x["owner_client_id"])):
        lines.append(
            f"| {h['owner_client_id']} | {h['owner_name']} | {h['spouse_name']} | "
            f"`{h['source']}` | {', '.join(h.get('personnel_reasons') or [])} |"
        )
    lines += ["", f"Machine: `{OUT_JSON}`"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote {OUT_MD}")
    print(
        "all_personnel",
        len(personnel_all),
        "fold_personnel",
        len(personnel_fold),
        "drake_personnel",
        len(personnel_drake),
        "tp_mismatch",
        len(tp_mismatch_hits),
        "cols_personnel",
        len(col_personnel),
        "w5",
        len(w5_examples),
    )
    conn.close()


if __name__ == "__main__":
    main()
