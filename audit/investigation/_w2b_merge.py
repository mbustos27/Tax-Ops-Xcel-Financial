"""Wave 2B — verify + execute planned merges with merge trail.

Default: dry-run. Pass --apply to write. Pass --include-dre for the 7th pair.
Uses plain sqlite3 (avoids get_connection/init_db lock hangs on live share).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(r"T:\taxops")
DB_PATH = ROOT / "taxops.db"
OUT = Path(r"T:\audit\investigation\W2B-merge-results.json")

GROUP_A = [
    {"key": "ORMA SERVICES INC", "keep": 754, "discard": 8, "expect_keep_last4": "2100"},
    {"key": "MIAMAR FUTURE LLC", "keep": 654, "discard": 32, "expect_keep_last4": "4898"},
    {"key": "KLEAN SOLAR SOLUTIONS LLC", "keep": 517, "discard": 854, "expect_keep_last4": "7813"},
]
GROUP_B_CANDIDATES = [
    {"key": "SANDOVAL AUTO SERVICE TOW", "ids": (107, 1780)},
    {"key": "PADILLA GARCIA|ABEL CLAUDIA E", "ids": (479, 1801)},
    {"key": "GIMENEZ GARCIA|ENRIQUE P LUVIA", "ids": (543, 1803)},
]
GROUP_A_EXTRA = [
    {"key": "D R E AND ASSOCIATES", "keep": 220, "discard": 762, "expect_keep_last4": "9030"},
]


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_merge_history(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_merge_history (
          id                      INTEGER PRIMARY KEY AUTOINCREMENT,
          keep_id                 INTEGER NOT NULL,
          discard_id              INTEGER NOT NULL,
          operator                TEXT,
          reason_code             TEXT,
          note                    TEXT,
          merged_at               TEXT NOT NULL,
          discard_client_json     TEXT NOT NULL,
          discard_returns_json    TEXT NOT NULL DEFAULT '[]',
          returns_actions_json    TEXT NOT NULL DEFAULT '[]',
          status_events_json      TEXT NOT NULL DEFAULT '[]',
          filetrack_history_json  TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_client_merge_history_keep "
        "ON client_merge_history(keep_id, merged_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_client_merge_history_discard "
        "ON client_merge_history(discard_id, merged_at)"
    )
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='schema_version'"
        ).fetchone()
        cur = int(row[0]) if row and str(row[0]).isdigit() else 0
        if cur < 25:
            from datetime import datetime, timezone

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            cols = {r[1] for r in conn.execute("PRAGMA table_info(app_settings)")}
            if "updated_at" in cols:
                conn.execute(
                    "INSERT INTO app_settings (key, value, updated_at) VALUES ('schema_version', '25', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value='25', updated_at=excluded.updated_at",
                    (ts,),
                )
            else:
                conn.execute(
                    "INSERT INTO app_settings (key, value) VALUES ('schema_version', '25') "
                    "ON CONFLICT(key) DO UPDATE SET value='25'"
                )
    except sqlite3.OperationalError as e:
        print("schema_version stamp skipped:", e)
    conn.commit()


def _client(conn: sqlite3.Connection, cid: int) -> dict | None:
    row = conn.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    return dict(row) if row else None


def _returns(conn: sqlite3.Connection, cid: int) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, log_number, tax_year, client_status FROM returns WHERE client_id=?",
            (cid,),
        ).fetchall()
    ]


def _name(row: dict) -> str:
    return f"{row.get('last_name')}, {row.get('first_name')}"


def build_plan(conn: sqlite3.Connection, include_dre: bool) -> list[dict]:
    plan: list[dict] = []
    items = list(GROUP_A)
    if include_dre:
        items.extend(GROUP_A_EXTRA)
    for item in items:
        k, d = item["keep"], item["discard"]
        kr, dr = _client(conn, k), _client(conn, d)
        entry = {
            "key": item["key"],
            "group": "A7" if item in GROUP_A_EXTRA else "A",
            "keep": k,
            "discard": d,
            "ok": False,
            "keep_name": None,
            "discard_name": None,
            "keep_last4": None,
            "discard_last4": None,
            "keep_returns": [],
            "discard_returns": [],
            "skip_reason": None,
        }
        if not kr or not dr:
            entry["skip_reason"] = "client missing (already merged?)"
            plan.append(entry)
            continue
        entry["keep_name"] = _name(kr)
        entry["discard_name"] = _name(dr)
        entry["keep_last4"] = (kr.get("ssn_last4") or "").strip() or None
        entry["discard_last4"] = (dr.get("ssn_last4") or "").strip() or None
        entry["keep_returns"] = _returns(conn, k)
        entry["discard_returns"] = _returns(conn, d)
        expect = item.get("expect_keep_last4")
        if expect and entry["keep_last4"] != expect:
            entry["skip_reason"] = f"keep last4 {entry['keep_last4']!r} != {expect!r}"
        elif (
            entry["keep_last4"]
            and entry["discard_last4"]
            and entry["keep_last4"] != entry["discard_last4"]
        ):
            entry["skip_reason"] = "both have different last4 — refuse"
        else:
            entry["ok"] = True
        plan.append(entry)

    for item in GROUP_B_CANDIDATES:
        a, b = item["ids"]
        ca, cb = _client(conn, a), _client(conn, b)
        entry = {
            "key": item["key"],
            "group": "B",
            "keep": None,
            "discard": None,
            "ok": False,
            "keep_name": None,
            "discard_name": None,
            "keep_last4": None,
            "discard_last4": None,
            "keep_returns": [],
            "discard_returns": [],
            "skip_reason": None,
        }
        if not ca or not cb:
            entry["skip_reason"] = "client missing"
            plan.append(entry)
            continue
        ta, tb = ca.get("created_at") or "", cb.get("created_at") or ""
        keep, discard = (a, b) if ta <= tb else (b, a)
        kr, dr = (_client(conn, keep), _client(conn, discard))
        entry["keep"], entry["discard"] = keep, discard
        entry["keep_name"], entry["discard_name"] = _name(kr), _name(dr)
        entry["keep_last4"] = (kr.get("ssn_last4") or "").strip() or None
        entry["discard_last4"] = (dr.get("ssn_last4") or "").strip() or None
        entry["keep_created"] = kr.get("created_at")
        entry["discard_created"] = dr.get("created_at")
        entry["keep_returns"] = _returns(conn, keep)
        entry["discard_returns"] = _returns(conn, discard)
        if (
            entry["keep_last4"]
            and entry["discard_last4"]
            and entry["keep_last4"] != entry["discard_last4"]
        ):
            entry["skip_reason"] = "different last4 — refuse"
        else:
            entry["ok"] = True
        plan.append(entry)
    return plan


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--include-dre", action="store_true")
    args = ap.parse_args()

    print("connecting", DB_PATH)
    conn = connect()
    print("ensuring client_merge_history…")
    ensure_merge_history(conn)
    has = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='client_merge_history'"
    ).fetchone()
    print("client_merge_history", bool(has))

    n_clients_before = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    n_returns_before = conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0]

    plan = build_plan(conn, include_dre=args.include_dre)
    print("\n=== PLAN ===")
    for p in plan:
        print(
            f"[{p['group']}] {p['key']}: keep={p['keep']} discard={p['discard']} "
            f"ok={p['ok']} last4={p['keep_last4']}/{p['discard_last4']} "
            f"rets={len(p['keep_returns'])}/{len(p['discard_returns'])} "
            f"{p.get('skip_reason') or ''}"
        )
        print(f"       keep={p['keep_name']} | discard={p['discard_name']}")

    runnable = [p for p in plan if p["ok"]]
    print(f"\nrunnable={len(runnable)} / planned={len(plan)}")
    print(f"before clients={n_clients_before} returns={n_returns_before}")

    if not args.apply:
        print("\nDRY-RUN only. Re-run with --apply to write.")
        conn.close()
        return

    sys.path.insert(0, str(ROOT))
    from merge_ops import merge_client_into
    from utils import now

    results = []
    ts = now()
    for p in runnable:
        try:
            hist_id = merge_client_into(
                conn,
                int(p["keep"]),
                int(p["discard"]),
                ts,
                operator="wave2b",
                reason_code=f"wave2b_group_{p['group']}",
                note=p["key"],
            )
            conn.commit()
            results.append(
                {
                    "key": p["key"],
                    "keep": p["keep"],
                    "discard": p["discard"],
                    "hist_id": hist_id,
                    "ok": True,
                }
            )
            print(f"MERGED {p['key']} hist_id={hist_id}")
        except Exception as e:
            conn.rollback()
            results.append({"key": p["key"], "ok": False, "error": str(e)})
            print(f"FAILED {p['key']}: {e}")

    n_clients_after = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    n_returns_after = conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0]
    n_hist = conn.execute("SELECT COUNT(*) FROM client_merge_history").fetchone()[0]
    print(
        f"\nafter clients={n_clients_after} returns={n_returns_after} history_rows={n_hist}"
    )
    print(
        f"delta clients={n_clients_after - n_clients_before} "
        f"returns={n_returns_after - n_returns_before}"
    )
    OUT.write_text(
        json.dumps(
            {
                "results": results,
                "before": {"clients": n_clients_before, "returns": n_returns_before},
                "after": {
                    "clients": n_clients_after,
                    "returns": n_returns_after,
                    "history": n_hist,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("wrote", OUT)
    conn.close()


if __name__ == "__main__":
    main()
