import sqlite3
c = sqlite3.connect(r"T:\audit\audit_disposition.sqlite")
for r in c.execute(
    "SELECT entity_key, status, resolved_by FROM audit_disposition "
    "WHERE entity_key IN (?,?,?)",
    (
        "spouse_unrecovered|1313|HUERTA|DANNY",
        "spouse_unrecovered|1697|RAMON RODRIGUEZ|CRESCENCIANO",
        "spouse_unrecovered|6789|MONTANEZ|PABLO",
    ),
):
    print(r)
print("--- FP count ---")
print(c.execute(
    "SELECT count(*) FROM audit_disposition WHERE finding_type='NEEDS_HUMAN' "
    "AND entity_key LIKE 'spouse_unrecovered|%' AND status='FALSE_POSITIVE'"
).fetchone())
