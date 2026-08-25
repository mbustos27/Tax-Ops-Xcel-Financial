import sqlite3
from pathlib import Path

db = Path(r"T:\taxops\taxops.db")
conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
print(
    "client_merge_history_exists",
    conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='client_merge_history'"
    ).fetchone()[0],
)
try:
    print("merge_history_n", conn.execute("SELECT COUNT(*) FROM client_merge_history").fetchone()[0])
    print("cols", [r[1] for r in conn.execute("PRAGMA table_info(client_merge_history)")])
    print(
        "sample",
        conn.execute(
            "SELECT keeper_client_id, discarded_client_id, reason, operator FROM client_merge_history LIMIT 3"
        ).fetchall(),
    )
except Exception as e:
    print("merge_err", e)
print(
    "spouses_wave4",
    conn.execute("SELECT COUNT(*) FROM spouses WHERE source='wave4_clients_fold'").fetchone()[0],
)
print("spouses_total", conn.execute("SELECT COUNT(*) FROM spouses").fetchone()[0])
# any fold snapshot / backup table?
for name in (
    "spouse_fold_history",
    "clients_spouse_backup",
    "wave4_spouse_fold",
):
    n = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()[0]
    print(f"table_{name}", n)
conn.close()
