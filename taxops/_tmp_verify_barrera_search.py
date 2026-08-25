from db import get_connection
conn = get_connection(r"T:\taxops\taxops.db")
link = conn.execute(
    "SELECT * FROM drake_prefill_links WHERE csm_name_raw LIKE 'BARRERA, ADRIAN%'"
).fetchone()
print("link client_id", link["client_id"], "status", link["prefill_status"], "purple", link["purple_name"])
c = conn.execute("SELECT * FROM clients WHERE id=?", (link["client_id"],)).fetchone()
print("client", {k: c[k] for k in ("id","last_name","first_name","spouse_first_name","spouse_last_name","ssn_last4")})

# Simulate search blanca barrera
tokens = ["blanca", "barrera"]
clauses, params = [], []
for tok in tokens:
    qp = f"%{tok}%"
    clauses.append(
        "(lower(c.last_name) LIKE ? OR lower(c.first_name) LIKE ? OR "
        "lower(COALESCE(c.display_name,'')) LIKE ? OR "
        "lower(COALESCE(c.spouse_first_name,'')) LIKE ? OR "
        "lower(COALESCE(c.spouse_last_name,'')) LIKE ? OR "
        "lower(COALESCE(dpl.csm_name_raw,'')) LIKE ? OR "
        "lower(COALESCE(dpl.purple_name,'')) LIKE ?)"
    )
    params.extend([qp]*7)
rows = conn.execute(
    f"""
    SELECT c.id, c.last_name, c.first_name, c.spouse_first_name, c.spouse_last_name,
           MAX(CASE WHEN dpl.prefill_status='PRIOR_YEAR_FORMS_AVAILABLE' THEN 1 ELSE 0 END) AS py
    FROM clients c
    LEFT JOIN drake_prefill_links dpl ON dpl.client_id = c.id
    WHERE {' AND '.join(clauses)}
    GROUP BY c.id
    """,
    params,
).fetchall()
print(f"search 'blanca barrera' → {len(rows)}")
for r in rows:
    print(f"  id={r['id']} {r['last_name']}, {r['first_name']} & {r['spouse_first_name']} {r['spouse_last_name']} prior_forms={r['py']}")
conn.close()
