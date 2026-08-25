import sqlite3

conn = sqlite3.connect(r"T:\taxops\taxops.db")
conn.row_factory = sqlite3.Row
# Drake W5 staff diff spellings
fixes = {
    610: ("MARTINEZ", "SUZANNA"),
    145: ("MUNOZ", "MARIBEL"),
    819: ("QUINTANA", "KAYLA"),
}
for cid, (last, first) in fixes.items():
    before = conn.execute(
        "SELECT last_name, first_name, source, taxpayer_name FROM spouses WHERE client_id=?",
        (cid,),
    ).fetchone()
    conn.execute(
        "UPDATE spouses SET last_name=?, first_name=?, source=?, taxpayer_name=NULL WHERE client_id=?",
        (last, first, "contam_fix:drake_spouse_export:2026-08-12", cid),
    )
    after = conn.execute(
        "SELECT last_name, first_name, source, taxpayer_name FROM spouses WHERE client_id=?",
        (cid,),
    ).fetchone()
    print(cid, "before", dict(before) if before else None, "after", dict(after))
conn.commit()
conn.close()
