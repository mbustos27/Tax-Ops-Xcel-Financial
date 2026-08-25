"""
Merge one client into another, including same tax-year return consolidation.

Wave 2A: every merge writes ``client_merge_history`` in the same transaction
as the discard DELETE, with a full pre-delete snapshot of the discarded client
(and related return / status / filetrack rows) so merges are reconstructable.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional


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


def _json_dump(obj: Any) -> str:
    return json.dumps(obj, default=str, sort_keys=True)


def _rows_as_dicts(rows: list) -> list[dict]:
    return [dict(r) for r in rows]


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

    # UNIQUE(log_number, tax_year): clear loser's log before any handoff.
    conn.execute("UPDATE returns SET log_number = NULL WHERE id = ?", (loser_id,))

    # If the merged log would still clash with a third return, keep the winner's.
    merged_log = merged.get("log_number")
    if merged_log not in (None, ""):
        clash = conn.execute(
            """
            SELECT id FROM returns
             WHERE log_number = ? AND tax_year = ? AND id NOT IN (?, ?)
             LIMIT 1
            """,
            (merged_log, merged.get("tax_year"), winner_id, loser_id),
        ).fetchone()
        if clash:
            merged["log_number"] = w.get("log_number")

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

    for t in ("notes", "status_events", "dependents", "missing_docs"):
        try:
            conn.execute(
                f"UPDATE {t} SET return_id=? WHERE return_id=?", (winner_id, loser_id)
            )
        except sqlite3.OperationalError:
            pass

    # Delete loser BEFORE updating winner.client_id — otherwise UNIQUE(client_id,
    # tax_year) fires when the discard return "wins" and is reassigned onto keep
    # while keep's same-year row still exists.
    conn.execute("DELETE FROM returns WHERE id=?", (loser_id,))

    skip_update = frozenset({"id"})
    cols = [c for c in merged if c not in skip_update]
    set_sql = ", ".join(f'"{c}"=?' for c in cols)
    conn.execute(
        f"UPDATE returns SET {set_sql} WHERE id=?",
        [merged[c] for c in cols] + [winner_id],
    )


def _merge_client_contact_fields(
    conn: sqlite3.Connection, keep_id: int, discard_id: int, updated_ts: str
) -> None:
    """Fill NULL keep-client fields from discard (names/SSN stay on keep)."""
    skip = {"id", "created_at", "updated_at", "last_name", "first_name", "ssn_last4"}
    keep = dict(conn.execute("SELECT * FROM clients WHERE id=?", (keep_id,)).fetchone() or {})
    discard = dict(conn.execute("SELECT * FROM clients WHERE id=?", (discard_id,)).fetchone() or {})
    updates: dict[str, Any] = {}
    for col, val in discard.items():
        if col in skip or val is None or val == "":
            continue
        if keep.get(col) in (None, ""):
            updates[col] = val
    # Prefer a real SSN last4 on keep when keep is blank
    if keep.get("ssn_last4") in (None, "") and discard.get("ssn_last4") not in (None, ""):
        updates["ssn_last4"] = discard["ssn_last4"]
    if updates:
        set_sql = ", ".join(f'"{c}"=?' for c in updates)
        conn.execute(
            f"UPDATE clients SET {set_sql}, updated_at=? WHERE id=?",
            list(updates.values()) + [updated_ts, keep_id],
        )


def _repoint_client_children(
    conn: sqlite3.Connection, keep_id: int, discard_id: int
) -> None:
    """Move or drop client-scoped rows so DELETE clients(discard) is FK-safe."""
    # Many rows OK — dependents / billing history
    for tbl in ("client_dependents", "client_billing"):
        try:
            # Avoid UNIQUE(client_id, drake_dependent_id) clashes by dropping
            # discard rows that already exist on keep for the same Drake id.
            if tbl == "client_dependents":
                conn.execute(
                    """
                    DELETE FROM client_dependents
                     WHERE client_id = ?
                       AND drake_dependent_id IS NOT NULL
                       AND drake_dependent_id IN (
                         SELECT drake_dependent_id FROM client_dependents
                          WHERE client_id = ?
                            AND drake_dependent_id IS NOT NULL
                       )
                    """,
                    (discard_id, keep_id),
                )
            if tbl == "client_billing":
                conn.execute(
                    """
                    DELETE FROM client_billing
                     WHERE client_id = ?
                       AND balance_as_of IS NOT NULL
                       AND balance_as_of IN (
                         SELECT balance_as_of FROM client_billing
                          WHERE client_id = ?
                            AND balance_as_of IS NOT NULL
                       )
                    """,
                    (discard_id, keep_id),
                )
            conn.execute(
                f"UPDATE {tbl} SET client_id = ? WHERE client_id = ?",
                (keep_id, discard_id),
            )
        except sqlite3.OperationalError:
            pass

    # At-most-one-per-client tables: keep wins
    for tbl in ("spouses", "client_spouse_import"):
        try:
            kept_has = conn.execute(
                f"SELECT 1 FROM {tbl} WHERE client_id = ?", (keep_id,)
            ).fetchone()
            if kept_has:
                conn.execute(f"DELETE FROM {tbl} WHERE client_id = ?", (discard_id,))
            else:
                conn.execute(
                    f"UPDATE {tbl} SET client_id = ? WHERE client_id = ?",
                    (keep_id, discard_id),
                )
        except sqlite3.OperationalError:
            pass

    # Soft links — prefer re-pointing to keep so history stays attached
    for tbl, col in (
        ("work_orders", "client_id"),
        ("billing_requests", "client_id"),
    ):
        try:
            conn.execute(
                f"UPDATE {tbl} SET {col} = ? WHERE {col} = ?",
                (keep_id, discard_id),
            )
        except sqlite3.OperationalError:
            pass


def _snapshot_related(
    conn: sqlite3.Connection, discard_return_ids: list[int]
) -> tuple[list[dict], list[dict]]:
    """Pre-merge snapshots of status_events + filetrack_status_history for discard returns."""
    status_events: list[dict] = []
    filetrack: list[dict] = []
    if not discard_return_ids:
        return status_events, filetrack
    placeholders = ",".join("?" * len(discard_return_ids))
    try:
        status_events = _rows_as_dicts(
            conn.execute(
                f"SELECT * FROM status_events WHERE return_id IN ({placeholders})",
                discard_return_ids,
            ).fetchall()
        )
    except sqlite3.OperationalError:
        pass
    try:
        filetrack = _rows_as_dicts(
            conn.execute(
                f"SELECT * FROM filetrack_status_history WHERE return_id IN ({placeholders})",
                discard_return_ids,
            ).fetchall()
        )
    except sqlite3.OperationalError:
        pass
    return status_events, filetrack


def _write_merge_history(
    conn: sqlite3.Connection,
    *,
    keep_id: int,
    discard_id: int,
    merged_at: str,
    discard_client: dict,
    discard_returns: list[dict],
    returns_actions: list[dict],
    status_events: list[dict],
    filetrack_history: list[dict],
    operator: Optional[str],
    reason_code: Optional[str],
    note: Optional[str],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO client_merge_history (
          keep_id, discard_id, operator, reason_code, note, merged_at,
          discard_client_json, discard_returns_json, returns_actions_json,
          status_events_json, filetrack_history_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            keep_id,
            discard_id,
            operator,
            reason_code,
            note,
            merged_at,
            _json_dump(discard_client),
            _json_dump(discard_returns),
            _json_dump(returns_actions),
            _json_dump(status_events),
            _json_dump(filetrack_history),
        ),
    )
    return int(cur.lastrowid)


def merge_client_into(
    conn: sqlite3.Connection,
    keep_id: int,
    discard_id: int,
    updated_ts: str,
    *,
    operator: Optional[str] = None,
    reason_code: Optional[str] = None,
    note: Optional[str] = None,
) -> int:
    """
    Re-point all ``discard`` data to ``keep``, merge returns that share a tax year,
    write ``client_merge_history``, then delete the discard client (FK-safe).

    Returns the new ``client_merge_history.id``.
    """
    if keep_id == discard_id:
        raise ValueError("keep_id and discard_id must differ")

    discard_row = conn.execute(
        "SELECT * FROM clients WHERE id=?", (discard_id,)
    ).fetchone()
    if not discard_row:
        raise ValueError(f"discard client {discard_id} not found")
    discard_client = dict(discard_row)

    discard_returns = _rows_as_dicts(
        conn.execute("SELECT * FROM returns WHERE client_id=?", (discard_id,)).fetchall()
    )
    discard_return_ids = [int(r["id"]) for r in discard_returns]
    status_events, filetrack_history = _snapshot_related(conn, discard_return_ids)

    _merge_client_contact_fields(conn, keep_id, discard_id, updated_ts)

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

    returns_actions: list[dict] = []
    for rdisc in rets_dis:
        d = dict(rdisc)
        d_id = d["id"]
        k = _ty_key(d.get("tax_year"))
        action: dict[str, Any] = {
            "return_id": d_id,
            "log_number": d.get("log_number"),
            "tax_year": d.get("tax_year"),
        }
        if k in by_key:
            krow = by_key[k]
            kid, did = krow["id"], d_id
            sk, sd = _row_score(krow), _row_score(d)
            if sk >= sd:
                merge_return_into(conn, kid, did, keep_id, updated_ts)
                surv = kid
                action["action"] = "merged_into_keep_return"
            else:
                merge_return_into(conn, did, kid, keep_id, updated_ts)
                surv = did
                action["action"] = "discard_return_won_then_kept"
            action["survivor_return_id"] = surv
            row = dict(conn.execute("SELECT * FROM returns WHERE id=?", (surv,)).fetchone() or {})
            by_key[k] = row
        else:
            conn.execute(
                "UPDATE returns SET client_id=?, updated_at=? WHERE id=?",
                (keep_id, updated_ts, d_id),
            )
            action["action"] = "repointed"
            action["survivor_return_id"] = d_id
            nxt = dict(
                conn.execute("SELECT * FROM returns WHERE id=?", (d_id,)).fetchone() or {}
            )
            by_key[k] = nxt
        returns_actions.append(action)

    conn.execute(
        "UPDATE review_queue SET proposed_client_id=? WHERE proposed_client_id=?",
        (keep_id, discard_id),
    )
    conn.execute(
        "UPDATE review_queue SET resolved_client_id=? WHERE resolved_client_id=?",
        (keep_id, discard_id),
    )
    # Re-point any remaining returns that weren't caught above (edge case)
    conn.execute(
        "UPDATE returns SET client_id=?, updated_at=? WHERE client_id=?",
        (keep_id, updated_ts, discard_id),
    )

    _repoint_client_children(conn, keep_id, discard_id)

    # History BEFORE delete — same transaction as callers' commit/rollback.
    history_id = _write_merge_history(
        conn,
        keep_id=keep_id,
        discard_id=discard_id,
        merged_at=updated_ts,
        discard_client=discard_client,
        discard_returns=discard_returns,
        returns_actions=returns_actions,
        status_events=status_events,
        filetrack_history=filetrack_history,
        operator=operator,
        reason_code=reason_code,
        note=note,
    )

    conn.execute("DELETE FROM clients WHERE id=?", (discard_id,))
    return history_id
