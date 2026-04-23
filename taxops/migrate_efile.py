import sqlite3
conn = sqlite3.connect("taxops.db")
r = conn.execute("UPDATE returns SET client_status='EFILE READY' WHERE client_status='EFILE'")
print("Migrated EFILE -> EFILE READY:", r.rowcount)
conn.commit()
conn.close()
