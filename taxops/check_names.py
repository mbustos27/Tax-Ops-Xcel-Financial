import sqlite3
conn = sqlite3.connect('taxops.db')
conn.row_factory = sqlite3.Row

empty = conn.execute(
    "SELECT COUNT(*) as n FROM clients WHERE last_name IS NULL OR last_name = ''"
).fetchone()
print(f"Clients with no last_name: {empty['n']}")

samples = conn.execute(
    "SELECT id, last_name, first_name, display_name FROM clients WHERE last_name IS NULL OR last_name = '' LIMIT 10"
).fetchall()
for r in samples:
    print(dict(r))

total = conn.execute("SELECT COUNT(*) as n FROM clients").fetchone()
print(f"Total clients: {total['n']}")
conn.close()
