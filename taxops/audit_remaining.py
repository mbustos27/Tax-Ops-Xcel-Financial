from db import get_connection
import json
conn = get_connection()
rows = conn.execute(
    "SELECT row_number, error, raw_json FROM import_rows WHERE error LIKE 'Invalid date%'"
).fetchall()
for r in rows:
    d = {}
    try: d = json.loads(r["raw_json"] or "{}")
    except: pass
    date_fields = {k: v for k, v in d.items() if any(x in k.lower() for x in ["date", "int", "pick", "log out"])}
    print("row=%s  err=%s  dates=%s" % (r["row_number"], r["error"], date_fields))
conn.close()
