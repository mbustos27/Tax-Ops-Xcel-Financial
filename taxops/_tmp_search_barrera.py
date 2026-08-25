from db import get_connection
conn = get_connection(r"T:\taxops\taxops.db")
# Simulate search for adrian / barrera / blanca
for q in ["barrera", "adrian barrera", "blanca barrera", "adrian", "blanca"]:
    tokens = [t for t in q.lower().split() if t]
    clauses, params = [], []
    for tok in tokens:
        qp = f"%{tok}%"
        clauses.append(
            "(lower(c.last_name) LIKE ? OR lower(c.first_name) LIKE ? OR "
            "lower(COALESCE(c.display_name,'')) LIKE ? OR "
            "lower(COALESCE(c.spouse_first_name,'')) LIKE ? OR "
            "lower(COALESCE(c.spouse_last_name,'')) LIKE ?)"
        )
        params.extend([qp]*5)
    rows = conn.execute(
        f"SELECT c.id, c.last_name, c.first_name, c.spouse_first_name, c.spouse_last_name, "
        f"c.ssn_last4 FROM clients c WHERE {' AND '.join(clauses)} LIMIT 20",
        params,
    ).fetchall()
    print(f"q={q!r} → {len(rows)} hits")
    for r in rows:
        print(f"  id={r['id']} {r['last_name']!r},{r['first_name']!r} spouse={r['spouse_first_name']!r} {r['spouse_last_name']!r} ssn={r['ssn_last4']!r}")

# How many PRIOR_YEAR prefill links have no client_id?
n = conn.execute("SELECT COUNT(*) n FROM drake_prefill_links WHERE tax_year=2024 AND prefill_status='PRIOR_YEAR_FORMS_AVAILABLE'").fetchone()["n"]
linked = conn.execute("SELECT COUNT(*) n FROM drake_prefill_links WHERE tax_year=2024 AND client_id IS NOT NULL").fetchone()["n"]
print(f"\nPRIOR_YEAR links: {n}, with client_id: {linked}")

# Can we match Barrera client 1544 to prefill by name?
c = conn.execute("SELECT * FROM clients WHERE id=1544").fetchone()
print("client 1544:", dict(c) if c else None)
link = conn.execute("SELECT * FROM drake_prefill_links WHERE csm_name_raw LIKE '%BARRERA, ADRIAN%'").fetchone()
print("link:", {k: link[k] for k in link.keys()} if link else None)

# Returns for client 1544?
rets = conn.execute("SELECT id, tax_year, client_status, log_number FROM returns WHERE client_id=1544").fetchall()
print("returns:", [dict(r) for r in rets])
conn.close()
