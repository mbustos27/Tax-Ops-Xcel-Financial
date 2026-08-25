from audit.disposition import connect_disposition
from audit.report import WORKFLOW_TYPES, collect_worklist_rows

c = connect_disposition()
for t in ["PREPARED_NOT_LOGGED", "LOGGED_NOT_PREPARED", "PHANTOM_IN_TAXOPS"]:
    n = c.execute(
        "SELECT COUNT(*) FROM audit_disposition WHERE finding_type=? AND status IN ('OPEN','ACKED')",
        (t,),
    ).fetchone()[0]
    print(t, n)
c.close()
extra = collect_worklist_rows(max_rows=800)
print("extra total", len(extra))
print("workflow in extra", sum(1 for r in extra if r["type"] in WORKFLOW_TYPES))
print("phantom in extra", sum(1 for r in extra if r["type"] == "PHANTOM_IN_TAXOPS"))
print("types", {r["type"] for r in extra})
