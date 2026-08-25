from db import get_connection
from app import _enrich_reintake_from_prefill
conn = get_connection(r"T:\taxops\taxops.db")
row = conn.execute("SELECT * FROM clients WHERE id=1544").fetchone()
data = dict(row); data.pop("ssn_last4", None)
# wipe names to prove enrichment
data["last_name"] = ""; data["first_name"] = None
data["spouse_first_name"] = None; data["spouse_last_name"] = None
enriched, hints, spouse = _enrich_reintake_from_prefill(conn, 1544, data)
print("enriched", enriched.get("last_name"), enriched.get("first_name"), enriched.get("spouse_first_name"))
print("hints", hints)
print("spouse", spouse)
print("dob", enriched.get("taxpayer_dob"), "cell", enriched.get("taxpayer_cell"))
conn.close()
