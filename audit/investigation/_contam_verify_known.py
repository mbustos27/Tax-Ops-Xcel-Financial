import sqlite3
from pathlib import Path

conn = sqlite3.connect(f"file:{Path(r'T:/taxops/taxops.db')}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row

print("=== owners by last4 ===")
for last4 in ("5949", "5180", "3527"):
    for r in conn.execute(
        "SELECT id, last_name, first_name, spouse_last_name, spouse_first_name, ssn_last4 "
        "FROM clients WHERE ssn_last4=?",
        (last4,),
    ):
        print(dict(r))
        for s in conn.execute(
            "SELECT * FROM spouses WHERE client_id=?", (r["id"],)
        ):
            print("  spouse", dict(s))

print("=== TAREEN / NELSON / HONORINA as clients ===")
for r in conn.execute(
    """
    SELECT id, last_name, first_name, ssn_last4 FROM clients
    WHERE upper(last_name) LIKE '%TAREEN%'
       OR upper(first_name) LIKE '%FOUZIA%'
       OR (upper(last_name) LIKE '%NELSON%' AND upper(first_name) LIKE '%PATRICIA%')
       OR upper(last_name) LIKE '%HONORINA%'
       OR upper(first_name) LIKE '%HONORINA%'
       OR (upper(last_name) LIKE '%RUIZ%' AND upper(first_name) LIKE '%HONORINA%')
       OR upper(last_name||' '||first_name) LIKE '%RUIZ DE PEREZ%'
    """
):
    print(dict(r))

print("=== spouses named TAREEN/FOUZIA/NELSON/HONORINA ===")
for r in conn.execute(
    """
    SELECT s.client_id, s.last_name, s.first_name, s.source,
           c.last_name AS cl, c.first_name AS cf, c.ssn_last4
    FROM spouses s JOIN clients c ON c.id=s.client_id
    WHERE upper(s.last_name||' '||s.first_name) LIKE '%TAREEN%'
       OR upper(s.last_name||' '||s.first_name) LIKE '%FOUZIA%'
       OR upper(s.last_name||' '||s.first_name) LIKE '%NELSON%PATRICIA%'
       OR upper(s.last_name||' '||s.first_name) LIKE '%PATRICIA%NELSON%'
       OR upper(s.last_name||' '||s.first_name) LIKE '%HONORINA%'
       OR upper(s.last_name||' '||s.first_name) LIKE '%RUIZ DE PEREZ%'
    """
):
    print(dict(r))
conn.close()
