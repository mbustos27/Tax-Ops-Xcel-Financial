import sqlite3
conn = sqlite3.connect('taxops.db')
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT c.id, c.last_name, c.first_name, c.display_name,
           COUNT(r.id) as return_count,
           GROUP_CONCAT(r.tax_year || '/' || COALESCE(r.log_number,'—') || '/' || r.client_status, '  |  ') as returns
    FROM clients c
    LEFT JOIN returns r ON r.client_id = c.id
    WHERE lower(c.last_name) = 'velasquez'
    GROUP BY c.id
    ORDER BY c.last_name, c.first_name
""").fetchall()

for r in rows:
    print(f"ID={r['id']}  display={r['display_name']}  last={r['last_name']}  first={r['first_name']}")
    print(f"   returns ({r['return_count']}): {r['returns']}")
    print()

conn.close()
