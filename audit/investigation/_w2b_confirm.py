import json
import sqlite3

conn = sqlite3.connect(r"T:\taxops\taxops.db")
conn.row_factory = sqlite3.Row
print("history", conn.execute("SELECT COUNT(*) FROM client_merge_history").fetchone()[0])
for row in conn.execute(
    "SELECT id, keep_id, discard_id, note, discard_client_json, returns_actions_json "
    "FROM client_merge_history ORDER BY id"
):
    snap = json.loads(row["discard_client_json"])
    actions = json.loads(row["returns_actions_json"])
    keep_alive = conn.execute(
        "SELECT COUNT(*) FROM clients WHERE id=?", (row["keep_id"],)
    ).fetchone()[0]
    discard_alive = conn.execute(
        "SELECT COUNT(*) FROM clients WHERE id=?", (row["discard_id"],)
    ).fetchone()[0]
    keep_rets = conn.execute(
        "SELECT COUNT(*) FROM returns WHERE client_id=?", (row["keep_id"],)
    ).fetchone()[0]
    print(
        f"#{row['id']} {row['note']}: keep={row['keep_id']}(alive={keep_alive},rets={keep_rets}) "
        f"discard={row['discard_id']}(alive={discard_alive}) "
        f"snap={snap.get('last_name')!r} actions={len(actions)}"
    )
# spot-check gone discards
for d in (8, 32, 854, 762, 107, 479, 543):
    n = conn.execute("SELECT COUNT(*) FROM clients WHERE id=?", (d,)).fetchone()[0]
    print(f"discard {d} present={n}")
conn.close()
