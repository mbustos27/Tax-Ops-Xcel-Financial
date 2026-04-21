import sqlite3
conn = sqlite3.connect("taxops.db")
conn.row_factory = sqlite3.Row
fin = conn.execute("SELECT COUNT(*) n FROM returns WHERE client_status = 'FINALIZE'").fetchone()
print("Returns still at FINALIZE:", fin["n"])
print("\nAll status counts:")
for r in conn.execute("SELECT client_status, COUNT(*) n FROM returns GROUP BY client_status ORDER BY n DESC").fetchall():
    print(f"  {r['client_status']}: {r['n']}")
conn.close()
