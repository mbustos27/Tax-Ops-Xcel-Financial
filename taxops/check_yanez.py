import sqlite3
conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT c.last_name, c.first_name, r.log_number, r.tax_year, r.client_status, r.processor "
    "FROM returns r JOIN clients c ON c.id=r.client_id "
    "WHERE upper(c.last_name)='YANEZ' ORDER BY r.tax_year"
).fetchall()
for r in rows:
    print(dict(r))
conn.close()
