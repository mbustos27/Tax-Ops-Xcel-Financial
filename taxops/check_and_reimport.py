import sqlite3, shutil, os

db = "taxops.db"
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row

batch = conn.execute("SELECT id, filename FROM import_batches WHERE filename = 'drake_2025.csv'").fetchone()
print("Batch record:", dict(batch) if batch else None)

fin = conn.execute("SELECT COUNT(*) n FROM returns WHERE client_status = 'FINALIZE'").fetchone()
print("Returns at FINALIZE:", fin["n"])

if batch:
    conn.execute("DELETE FROM import_batches WHERE id = ?", (batch["id"],))
    conn.commit()
    print("Deleted batch record — hash cleared for re-import.")

conn.close()

src = os.path.join("data", "processed", "drake_2025.csv")
dst = os.path.join("data", "incoming", "drake_2025.csv")
if os.path.exists(src):
    shutil.copy2(src, dst)
    print(f"Copied {src} -> {dst}")
else:
    print("ERROR: drake_2025.csv not found in processed/")
