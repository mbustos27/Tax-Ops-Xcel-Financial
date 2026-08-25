import sqlite3
c = sqlite3.connect(r"T:\audit\audit_disposition.sqlite")
for r in c.execute(
    "select finding_type, count(*) from audit_disposition group by 1 order by 2 desc"
):
    print(r)
print("--- MALFORMED sample ---")
for r in c.execute(
    "select entity_key, salient, status from audit_disposition where finding_type='MALFORMED_LOG_NUMBER' limit 5"
):
    print(r)
print("--- COLLISION sample ---")
for r in c.execute(
    "select entity_key, salient, status from audit_disposition where finding_type='LOG_NUMBER_COLLISION' limit 5"
):
    print(r)
