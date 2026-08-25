"""A2 probe helpers — RO."""
from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

p = Path(r"T:\audit\audit_20260731.sqlite")
c = sqlite3.connect(str(p))
c.row_factory = sqlite3.Row
print("tables", [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")])
print("finding types", dict(c.execute("SELECT finding_type, COUNT(*) FROM audit_finding GROUP BY 1")))
for r in c.execute(
    """
    SELECT f.finding_type, f.subject_id, f.detail_json, f.source_refs,
           d.client_name_raw, d.id_last4
    FROM audit_finding f
    LEFT JOIN stage_drake d ON d.run_id=f.run_id AND d.id=f.subject_id
    WHERE f.finding_type='NAME_TRUNCATED' LIMIT 2
    """
):
    print("NT", dict(r))
for r in c.execute(
    """
    SELECT f.finding_type, f.detail_json, f.source_refs, t.log_number, t.client_id
    FROM audit_finding f
    LEFT JOIN stage_taxops_return t ON t.run_id=f.run_id AND t.return_id=CAST(json_extract(f.source_refs,'$.return_id') AS INT)
    WHERE f.finding_type='PHANTOM_IN_TAXOPS' LIMIT 3
    """
):
    print("PH", dict(r))
print("runs", [dict(r) for r in c.execute("SELECT id, started_at, notes FROM audit_run")])
c.close()

# TaxOps audit_log merge?
from audit.db import connect_taxops_readonly
from audit.baseline import load_baseline_memory

mem = load_baseline_memory()
tc = connect_taxops_readonly(Path(mem["authoritative_taxops_path"]))
try:
    actions = list(
        tc.execute(
            "SELECT action, COUNT(*) n FROM audit_log GROUP BY 1 ORDER BY n DESC LIMIT 30"
        )
    )
    print("audit_log actions:", [dict(a) for a in actions])
    mergeish = list(
        tc.execute(
            "SELECT action, entity_type, COUNT(*) n FROM audit_log "
            "WHERE lower(action) LIKE '%merge%' OR lower(entity_type) LIKE '%merge%' "
            "GROUP BY 1,2"
        )
    )
    print("mergeish:", [dict(a) for a in mergeish])
    # any table with merge in name?
    tabs = [
        r[0]
        for r in tc.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%merge%'"
        )
    ]
    print("merge tables:", tabs)
finally:
    tc.close()
