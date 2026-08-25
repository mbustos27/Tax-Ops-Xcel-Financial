import sqlite3, os, json, csv
from pathlib import Path
from datetime import datetime

SNAP = Path(r"T:\audit\investigation\snapshot")
OUT = Path(r"T:\audit\investigation")

dbs = sorted(SNAP.glob("*"))
report = []

def ro_connect(path):
    uri = f"file:{path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)

for dbpath in dbs:
    if not dbpath.is_file():
        continue
    info = {
        "path": str(dbpath),
        "size": dbpath.stat().st_size,
        "mtime": datetime.fromtimestamp(dbpath.stat().st_mtime).isoformat(sep=" ", timespec="seconds"),
    }
    try:
        conn = ro_connect(dbpath)
        cur = conn.cursor()
        page_count = cur.execute("PRAGMA page_count").fetchone()[0]
        page_size = cur.execute("PRAGMA page_size").fetchone()[0]
        journal = cur.execute("PRAGMA journal_mode").fetchone()[0]
        info.update({
            "page_count": page_count,
            "page_size": page_size,
            "bytes_calc": page_count * page_size,
            "journal_mode": journal,
        })
        master = cur.execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name").fetchall()
        info["sqlite_master"] = [{"type": t, "name": n, "sql": s} for t, n, s in master]
        # table row counts for identity-ish names
        tables = [n for t,n,s in master if t == "table" and not n.startswith("sqlite_")]
        counts = {}
        for t in tables:
            try:
                counts[t] = cur.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except Exception as e:
                counts[t] = f"ERR:{e}"
        info["row_counts"] = counts
        conn.close()
        info["ok"] = True
    except Exception as e:
        info["ok"] = False
        info["error"] = str(e)
    report.append(info)
    print(f"OK {dbpath.name} pages={info.get('page_count')} journal={info.get('journal_mode')} tables={len(info.get('row_counts',{}))}")

(OUT / "I0-schema-dump.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
print("Wrote I0-schema-dump.json")

# Also probe live path metadata without opening for write
live_candidates = [
    Path(r"T:\taxops\taxops.db"),
    Path(r"C:\TaxOps\taxops\taxops.db"),
    Path(r"T:\taxops\taxops_demo.db"),
    Path(r"T:\taxops\taxops_prefill_apply_test.db"),
    Path(r"T:\taxops\taxops_rebuilt.db"),
    Path(r"T:\taxops\taxops_recovered.db"),
    Path(r"T:\taxops\taxops_test.db"),
    Path(r"T:\taxops\taxops_test_ext.db"),
    Path(r"T:\taxops\.codegraph\codegraph.db"),
    Path(r"T:\taxops\backups\taxops_backup_20260701T182519Z.sqlite"),
    Path(r"T:\audit\audit_20260731.sqlite"),
    Path(r"T:\audit\audit_202607311100.sqlite"),
    Path(r"T:\audit\snapshots\taxops_snapshot_20260731.sqlite"),
    Path(r"T:\punchbridge\punchbridge.db"),
    Path(r"T:\punchbridge\data\punchbridge.db"),
]
live_meta = []
for p in live_candidates:
    if p.exists():
        st = p.stat()
        live_meta.append({
            "path": str(p),
            "size": st.st_size,
            "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(sep=" ", timespec="seconds"),
            "wal": (p.parent / (p.name + "-wal")).exists(),
            "shm": (p.parent / (p.name + "-shm")).exists(),
        })
        # read-only pragma for live files too (except empty 0-byte)
        if st.st_size > 0:
            try:
                c = ro_connect(p)
                live_meta[-1]["page_count"] = c.execute("PRAGMA page_count").fetchone()[0]
                live_meta[-1]["journal_mode"] = c.execute("PRAGMA journal_mode").fetchone()[0]
                c.close()
            except Exception as e:
                live_meta[-1]["pragma_error"] = str(e)
    else:
        live_meta.append({"path": str(p), "exists": False})

(OUT / "I0-live-db-meta.json").write_text(json.dumps(live_meta, indent=2), encoding="utf-8")
print("Wrote I0-live-db-meta.json")
