import sqlite3
from pathlib import Path

conn = sqlite3.connect(f"file:{Path(r'T:\audit\audit_disposition.sqlite')}?mode=ro", uri=True)
print(
    "NEEDS_HUMAN by status",
    list(
        conn.execute(
            "SELECT status, COUNT(*) FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' GROUP BY status"
        )
    ),
)
print(
    "wave5 resolved",
    conn.execute(
        "SELECT COUNT(*) FROM audit_disposition WHERE finding_type='NEEDS_HUMAN' "
        "AND resolved_by='wave5'"
    ).fetchone()[0],
)
print(
    "wave5 export_gap acked",
    conn.execute(
        "SELECT COUNT(*) FROM audit_disposition WHERE finding_type='NEEDS_HUMAN' "
        "AND resolved_by='wave5_export_gap'"
    ).fetchone()[0],
)
print(
    "still OPEN spouse_unrecovered",
    conn.execute(
        "SELECT COUNT(*) FROM audit_disposition WHERE finding_type='NEEDS_HUMAN' "
        "AND status='OPEN' AND entity_key LIKE 'spouse_unrecovered|%'"
    ).fetchone()[0],
)
conn.close()
