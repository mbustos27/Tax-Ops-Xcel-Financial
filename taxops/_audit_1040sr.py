"""Audit 1040SR presence across CSM, purple, match, and DB."""
from collections import Counter
from drake_prefill_importer import (
    load_csm_xlsx, composite_dedupe_csm, join_purple_chunks, match_csm_to_purple,
)
from db import get_connection

CSM = r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx"
CHUNKS = [
    r"T:\taxops\CSVFILES\TY2024S.csv",
    r"T:\taxops\CSVFILES\TY2024S2439-5405.csv",
    r"T:\taxops\CSVFILES\TY2024S8867-end.csv",
]

rows = load_csm_xlsx(CSM)
print("=== CSM raw Type distribution (named rows only) ===")
named = [r for r in rows if r.name_norm]
print(Counter((r.return_type or "(null)").strip().upper() for r in named).most_common())
sr_raw = [r for r in named if (r.return_type or "").strip().upper() in ("1040SR", "1040-SR", "1040 SR")]
print(f"named rows with Type~1040SR: {len(sr_raw)}")
# also case variants containing SR
sr_like = [r for r in named if "SR" in (r.return_type or "").upper()]
print(f"named rows Type containing 'SR': {len(sr_like)}")
print("sample SR-like types:", Counter((r.return_type or "(null)") for r in sr_like).most_common())

dedupe = composite_dedupe_csm(rows)
print("\n=== After dedupe ===")
print(Counter((c.get("return_type") or "(null)").strip().upper() for c in dedupe.clients).most_common())
sr_csm = [c for c in dedupe.clients if "SR" in (c.get("return_type") or "").upper()]
print(f"deduped clients Type containing SR: {len(sr_csm)}")
for c in sr_csm[:20]:
    print(f"  {c['csm_ssn_last4']} {c['csm_name_raw']!r} type={c['return_type']!r} disp={c['disposition_status']}")

purple, notes, rc, cc = join_purple_chunks(CHUNKS)
print("\n=== Purple Return Type ===")
print(Counter((p.return_type or "(null)") for p in purple).most_common())
sr_p = [p for p in purple if (p.return_type or "").upper().replace(" ", "") in ("1040SR", "1040-SR") or (p.return_type or "").upper() == "1040SR"]
sr_p2 = [p for p in purple if p.return_type and "SR" in p.return_type.upper()]
print(f"purple Return Type containing SR: {len(sr_p2)}")
print("samples:", [(p.taxpayer_name, p.return_type) for p in sr_p2[:15]])

links, *_ = match_csm_to_purple(dedupe.clients, purple)
print("\n=== Matched links by purple return_type ===")
print(Counter((L.return_type or "(none)") for L in links).most_common())
sr_links = [L for L in links if L.return_type and "SR" in L.return_type.upper()]
print(f"links with purple return_type SR: {len(sr_links)}")
print("status of SR links:", Counter(L.prefill_status for L in sr_links).most_common())

# CSM SR clients — did they get links?
print("\n=== CSM Type=1040SR clients → prefill status ===")
print(Counter(next(L.prefill_status for L in links if L.csm['csm_ssn_last4']==c['csm_ssn_last4'] and L.csm['csm_name_norm']==c['csm_name_norm']) for c in sr_csm).most_common() if sr_csm else "none")
for c in sr_csm:
    L = next(L for L in links if L.csm['csm_ssn_last4']==c['csm_ssn_last4'] and L.csm['csm_name_norm']==c['csm_name_norm'])
    print(f"  {c['csm_name_raw']!r} csm_type={c['return_type']!r} → {L.prefill_status} purple_rt={L.return_type!r} purple={L.purple_name!r}")

# DB check
for db in [r"T:\taxops\taxops_prefill_apply_test.db", r"T:\taxops\taxops.db"]:
    try:
        conn = get_connection(db)
        has = conn.execute("SELECT name FROM sqlite_master WHERE name='drake_form_prefill'").fetchone()
        if not has:
            print(f"\n{db}: no form_prefill")
            conn.close()
            continue
        rows_db = conn.execute(
            "SELECT return_type, COUNT(*) n FROM drake_form_prefill WHERE tax_year=2024 GROUP BY return_type ORDER BY n DESC"
        ).fetchall()
        print(f"\n{db} form_prefill return_type:")
        for r in rows_db:
            print(f"  {r['n']:5d}  {r['return_type']!r}")
        n_sr = conn.execute(
            "SELECT COUNT(*) n FROM drake_form_prefill WHERE tax_year=2024 AND UPPER(REPLACE(return_type,' ','')) LIKE '%1040SR%'"
        ).fetchone()["n"]
        print(f"  1040SR-ish rows: {n_sr}")
        conn.close()
    except Exception as e:
        print(db, e)
