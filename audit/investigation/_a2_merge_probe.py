"""Probe merge audit_log payloads. RO."""
from __future__ import annotations
import json
from pathlib import Path
from audit.db import connect_taxops_readonly
from audit.baseline import load_baseline_memory

mem = load_baseline_memory()
tc = connect_taxops_readonly(Path(mem["authoritative_taxops_path"]))
rows = list(
    tc.execute(
        "SELECT id, action, entity_type, entity_id, before_json, after_json, created_at, user_id "
        "FROM audit_log WHERE action LIKE '%merge_clients%' ORDER BY id DESC LIMIT 5"
    )
)
for r in rows:
    d = dict(r)
    for k in ("before_json", "after_json"):
        raw = d.get(k)
        if raw:
            try:
                d[k] = json.loads(raw)
            except Exception:
                pass
    print(json.dumps(d, default=str)[:800])
    print("---")
# count with usable keep/discard
n = 0
usable = 0
for r in tc.execute(
    "SELECT before_json, after_json, entity_id FROM audit_log WHERE action LIKE '%merge_clients%'"
):
    n += 1
    blob = " ".join(str(x or "") for x in r)
    if "keep" in blob.lower() or "discard" in blob.lower():
        usable += 1
print("merge rows", n, "mention keep/discard", usable)
tc.close()
