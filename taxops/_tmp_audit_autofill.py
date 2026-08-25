"""Audit preintake autofill: clients with PRIOR_YEAR prefill but thin/empty name fields."""
from db import get_connection
from name_matcher import parse_name
from drake_prefill_importer import _spouse_from_csm_name

conn = get_connection(r"T:\taxops\taxops.db")  # share view of prod
# Prefer C if that's what server uses - try both
for db in [r"C:\TaxOps\taxops\taxops.db", r"T:\taxops\taxops.db", r"\\Xcel-server\taxops\taxops.db"]:
    try:
        from pathlib import Path
        if Path(db).is_file():
            conn.close()
            conn = get_connection(db)
            print("using", db)
            break
    except Exception as e:
        print("skip", db, e)

# Clients linked to PRIOR_YEAR with missing/blank first or last
rows = conn.execute("""
SELECT c.id, c.last_name, c.first_name, c.display_name,
       c.spouse_first_name, c.spouse_last_name,
       d.csm_name_raw, d.purple_name, d.prefill_status
FROM clients c
JOIN drake_prefill_links d ON d.client_id = c.id
WHERE d.prefill_status = 'PRIOR_YEAR_FORMS_AVAILABLE'
  AND d.tax_year = 2024
""").fetchall()
print("PRIOR_YEAR linked clients", len(rows))

empty_last = [r for r in rows if not (r["last_name"] or "").strip()]
empty_first = [r for r in rows if not (r["first_name"] or "").strip() and not (r["display_name"] or "").strip()]
# Business: first null OK if last present
biz_ok = [r for r in rows if (r["last_name"] or "").strip() and not (r["first_name"] or "").strip()]
print("empty last_name:", len(empty_last))
print("empty first AND display:", len(empty_first))
print("business-like (last, no first):", len(biz_ok))

# Would parse_name from csm fill them?
need_name_fix = []
for r in rows:
    ln = (r["last_name"] or "").strip()
    fn = (r["first_name"] or "").strip()
    if not ln:
        need_name_fix.append(r)
    elif not fn and not (r["display_name"] or "").strip():
        # check if CSM has a person first name
        pl, pf = parse_name(r["csm_name_raw"] or "")
        if pf:
            need_name_fix.append(r)

print("would benefit from CSM name backfill:", len(need_name_fix))
for r in need_name_fix[:15]:
    print(f"  id={r['id']} client=({r['last_name']!r},{r['first_name']!r}) csm={r['csm_name_raw']!r}")

# Simulate reintake payload name fields for Barrera
b = conn.execute("""
SELECT c.*, d.csm_name_raw, d.purple_name
FROM clients c
JOIN drake_prefill_links d ON d.client_id=c.id
WHERE c.last_name='BARRERA' AND c.first_name='ADRIAN'
""").fetchone()
if b:
    print("\nBarrera client fields:")
    for k in ("last_name","first_name","spouse_first_name","spouse_last_name","display_name"):
        print(f"  {k}={b[k]!r}")
    print("  csm", b["csm_name_raw"])

# Sample of newly created stubs - any with weird names?
stubs = conn.execute("""
SELECT c.id, c.last_name, c.first_name, c.spouse_first_name, d.csm_name_raw
FROM clients c
JOIN drake_prefill_links d ON d.client_id=c.id
WHERE d.prefill_status='PRIOR_YEAR_FORMS_AVAILABLE'
ORDER BY c.id DESC LIMIT 10
""").fetchall()
print("\nRecent PRIOR_YEAR linked clients:")
for r in stubs:
    print(f"  {r['id']} {r['last_name']!r}, {r['first_name']!r} spouse={r['spouse_first_name']!r} csm={r['csm_name_raw']!r}")

conn.close()
