"""
Merge one client into another, including same tax-year return consolidation.
"""
from __future__ import annotations

import sqlite3
from typing import Any


def _row_score(returns_dict: dict) -> int:
    s = 0
    if returns_dict.get("log_number") not in (None, "", 0, "0"):
        s += 2000
    for k, v in returns_dict.items():
        if k in ("id", "client_id", "created_at", "updated_at"):
            continue
        if v is not None and v != "" and v != 0 and v != 0.0:
            s += 1
    return s


def _ty_key(ty: int | None) -> str:
    return "__null__" if ty is None else str(ty)


def _merge_filler(w: Any, l: Any) -> Any:
    if w in (None, ""):
        return l
    if isinstance(w, (int, float)) and w in (0, 0.0) and l not in (None, "", 0, 0.0):
        return l
    return w


def _merge_dict_rows(a: dict, b: dict, skip: frozenset) -> dict:
    out = {**a}
    for k, lv in b.items():
        if k in skip:
            continue
        out[k] = _merge_filler(out.get(k), lv)
    return out


def merge_return_into(
    conn: sqlite3.Connection,
    winner_id: int,
    loser_id: int,
    keep_client_id: int,
    updated_ts: str,
) -> None:
    w = dict(conn.execute("SELECT * FROM returns WHERE id=?", (winner_id,)).fetchone() or {})
    l = dict(conn.execute("SELECT * FROM returns WHERE id=?", (loser_id,)).fetchone() or {})
    if not w or not l or winner_id == loser_id:
        return

    merged = _merge_dict_rows(w, l, frozenset({"id"}))
    merged["id"] = winner_id
    merged["client_id"] = keep_client_id
    merged["updated_at"] = updated_ts
    skip_update = frozenset({"id"})
    cols = [c for c in merged if c not in skip_update]
    set_sql = ", ".join(f'"{c}"=?' for c in cols)
    conn.execute(
        f"UPDATE returns SET {set_sql} WHERE id=?",
        [merged[c] for c in cols] + [winner_id],
    )

    # return_forms
    fw = conn.execute("SELECT * FROM return_forms WHERE return_id=?", (winner_id,)).fetchone()
    fl = conn.execute("SELECT * FROM return_forms WHERE return_id=?", (loser_id,)).fetchone()
    if fw and fl:
        fwm, flm = dict(fw), dict(fl)
        for c in fwm:
            if c in ("id", "return_id"):
                continue
            if c in flm:
                a, b = fwm.get(c) or 0, flm.get(c) or 0
                try:
                    fwm[c] = 1 if (int(a) or int(b)) else 0
                except (TypeError, ValueError):
                    fwm[c] = _merge_filler(fwm.get(c), flm.get(c))
        up_cols = [c for c in fwm if c not in ("id", "return_id")]
        conn.execute(
            "UPDATE return_forms SET " + ", ".join(f'"{c}"=?' for c in up_cols) + " WHERE return_id=?",
            [fwm[c] for c in up_cols] + [winner_id],
        )
    elif not fw and fl:
        conn.execute("UPDATE return_forms SET return_id=? WHERE return_id=?", (winner_id, loser_id))
    conn.execute("DELETE FROM return_forms WHERE return_id=?", (loser_id,))

    # payments
    pw = conn.execute("SELECT * FROM payments WHERE return_id=?", (winner_id,)).fetchone()
    pl = conn.execute("SELECT * FROM payments WHERE return_id=?", (loser_id,)).fetchone()
    if pw and pl:
        pwa, pla = dict(pw), dict(pl)
        merged_p = _merge_dict_rows(pwa, pla, frozenset({"id", "return_id"}))
        merged_p["id"] = pwa["id"]
        merged_p["return_id"] = winner_id
        pcols = [c for c in merged_p if c not in ("id", "return_id")]
        conn.execute(
            "UPDATE payments SET " + ", ".join(f'"{c}"=?' for c in pcols) + " WHERE id=?",
            [merged_p[c] for c in pcols] + [pwa["id"]],
        )
    elif not pw and pl:
        conn.execute("UPDATE payments SET return_id=? WHERE return_id=?", (winner_id, loser_id))
    conn.execute("DELETE FROM payments WHERE return_id=?", (loser_id,))

    for t in ("notes", "status_events", "dependents"):
        try:
            conn.execute(
                f"UPDATE {t} SET return_id=? WHERE return_id=?", (winner_id, loser_id)
            )
        except sqlite3.OperationalError:
            pass

    conn.execute("DELETE FROM returns WHERE id=?", (loser_id,))


def merge_client_into(
    conn: sqlite3.Connection, keep_id: int, discard_id: int, updated_ts: str
) -> None:
    """
    Re-point all `discard` data to `keep`, merge returns that share a tax year.
    """
    rets_keep = conn.execute(
        "SELECT * FROM returns WHERE client_id=?", (keep_id,)
    ).fetchall()
    rets_dis = conn.execute(
        "SELECT * FROM returns WHERE client_id=?", (discard_id,)
    ).fetchall()
    by_key: dict[str, dict] = {}
    for r in rets_keep:
        d = dict(r)
        by_key[_ty_key(d.get("tax_year"))] = d

    for rdisc in rets_dis:
        d = dict(rdisc)
        d_id = d["id"]
        k = _ty_key(d.get("tax_year"))
        if k in by_key:
            krow = by_key[k]
            kid, did = krow["id"], d_id
            sk, sd = _row_score(krow), _row_score(d)
            if sk >= sd:
                merge_return_into(conn, kid, did, keep_id, updated_ts)
                surv = kid
            else:
                merge_return_into(conn, did, kid, keep_id, updated_ts)
                surv = did
            row = dict(conn.execute("SELECT * FROM returns WHERE id=?", (surv,)).fetchone() or {})
            by_key[k] = row
        else:
            conn.execute(
                "UPDATE returns SET client_id=?, updated_at=? WHERE id=?",
                (keep_id, updated_ts, d_id),
            )
            nxt = dict(
                conn.execute("SELECT * FROM returns WHERE id=?", (d_id,)).fetchone() or {}
            )
            by_key[k] = nxt

    conn.execute(
        "UPDATE review_queue SET proposed_client_id=? WHERE proposed_client_id=?",
        (keep_id, discard_id),
    )
    conn.execute(
        "UPDATE review_queue SET resolved_client_id=? WHERE resolved_client_id=?",
        (keep_id, discard_id),
    )
    conn.execute("DELETE FROM clients WHERE id=?", (discard_id,))
