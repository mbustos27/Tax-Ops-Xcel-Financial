"""Explicit lookup: Adrian Barrera + spouse Blanca across CSM / purple / DBs."""
import re
import sqlite3
from pathlib import Path

from drake_prefill_importer import (
    load_csm_xlsx,
    composite_dedupe_csm,
    join_purple_chunks,
    match_csm_to_purple,
    normalize_csm_name,
    normalize_purple_name,
    csm_name_variants,
)
from db import get_connection

CSM = r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx"
CHUNKS = [
    r"T:\taxops\CSVFILES\TY2024S.csv",
    r"T:\taxops\CSVFILES\TY2024S2439-5405.csv",
    r"T:\taxops\CSVFILES\TY2024S8867-end.csv",
]

def hit(s: str) -> bool:
    u = (s or "").upper()
    # Barrera / Barrerra / Barerra typos
    return bool(re.search(r"BAR+E?RR+A", u)) and "ADRIAN" in u

print("=== CSM raw rows matching Adrian + Barrera* ===")
rows = load_csm_xlsx(CSM)
csm_hits = [r for r in rows if hit(r.name_raw)]
# also Blanca + Barrera
blanca_hits = [
    r for r in rows
    if re.search(r"BAR+E?RR+A", (r.name_raw or "").upper())
    and "BLANCA" in (r.name_raw or "").upper()
]
print(f"Adrian+Barrera* rows: {len(csm_hits)}")
for r in csm_hits:
    print(
        f"  ssn={r.ssn_last4!r} name={r.name_raw!r} type={r.return_type!r} "
        f"status={r.status!r} started={r.started!r} completed={r.completed!r} "
        f"changed={r.last_change_raw!r} norm={r.name_norm!r}"
    )
print(f"\nBlanca+Barrera* rows: {len(blanca_hits)}")
for r in blanca_hits:
    print(f"  ssn={r.ssn_last4!r} name={r.name_raw!r} status={r.status!r} norm={r.name_norm!r}")

# Broader: any BARRERA with Adrian or Blanca in name
print("\n=== Any CSM name containing Barrera* (all) ===")
all_b = [r for r in rows if re.search(r"BAR+E?RR+A", (r.name_raw or "").upper())]
print(f"count: {len(all_b)}")
for r in sorted(all_b, key=lambda x: (x.ssn_last4, x.last_change_raw or "")):
    print(f"  {r.ssn_last4}  {r.name_raw!r}  status={r.status!r}  type={r.return_type!r}  changed={r.last_change_raw!r}")

print("\n=== After composite dedupe ===")
dedupe = composite_dedupe_csm(rows)
for c in dedupe.clients:
    if hit(c["csm_name_raw"]) or (
        re.search(r"BAR+E?RR+A", c["csm_name_raw"].upper())
        and ("ADRIAN" in c["csm_name_raw"].upper() or "BLANCA" in c["csm_name_raw"].upper())
    ):
        print(
            f"  ssn={c['csm_ssn_last4']} name={c['csm_name_raw']!r} "
            f"norm={c['csm_name_norm']!r} type={c.get('return_type')!r} "
            f"disp={c['disposition_status']} status={c['csm_status_raw']!r}"
        )
        print("  variants:")
        for label, v in csm_name_variants(c["csm_name_raw"]):
            print(f"    [{label}] {v}")

print("\n=== Purple Taxpayer Name ===")
purple, *_ = join_purple_chunks(CHUNKS)
p_hits = [
    p for p in purple
    if re.search(r"BAR+E?RR+A", p.taxpayer_name.upper())
    and ("ADRIAN" in p.taxpayer_name.upper() or "BLANCA" in p.taxpayer_name.upper())
]
print(f"hits: {len(p_hits)}")
for p in p_hits:
    print(
        f"  name={p.taxpayer_name!r} norm={p.name_norm!r} "
        f"return_type={p.return_type!r} dup={p.ambiguous_duplicate_name} "
        f"forms_keys={len(p.form_counts)}"
    )
    # show a few form counts
    for k in ("Schedule A", "Schedule C", "Form 8879", "Return Type"):
        if k in p.form_counts:
            print(f"    {k}: {p.form_counts[k]!r}")

# All purple Barrera*
print("\n=== All purple Barrera* ===")
for p in purple:
    if re.search(r"BAR+E?RR+A", p.taxpayer_name.upper()):
        print(f"  {p.taxpayer_name!r}  rt={p.return_type!r}")

print("\n=== Match result for this couple ===")
links, tiers, variants, stats = match_csm_to_purple(dedupe.clients, purple)
for L in links:
    raw = L.csm["csm_name_raw"]
    pn = L.purple_name or ""
    if hit(raw) or hit(pn) or (
        re.search(r"BAR+E?RR+A", (raw + " " + pn).upper())
        and ("ADRIAN" in (raw + " " + pn).upper() or "BLANCA" in (raw + " " + pn).upper())
    ):
        print(
            f"  status={L.prefill_status} tier={L.match_tier} score={L.match_score} "
            f"variant={L.matched_variant!r} reason={L.reason!r}"
        )
        print(f"    CSM: {raw!r} ssn={L.csm['csm_ssn_last4']} disp={L.csm['disposition_status']}")
        print(f"    purple: {pn!r} return_type={L.return_type!r}")

print("\n=== DB lookup ===")
for db in [r"T:\taxops\taxops.db", r"T:\taxops\taxops_prefill_apply_test.db"]:
    if not Path(db).exists():
        print(f"{db}: missing")
        continue
    conn = get_connection(db)
    print(f"\n--- {db} ---")
    # clients table
    clients = conn.execute(
        """
        SELECT id, ssn_last4, last_name, first_name, display_name
        FROM clients
        WHERE UPPER(COALESCE(display_name,'') || ' ' || COALESCE(last_name,'') || ' ' || COALESCE(first_name,''))
              LIKE '%BARRER%'
           OR UPPER(COALESCE(display_name,'') || ' ' || COALESCE(last_name,'') || ' ' || COALESCE(first_name,''))
              LIKE '%BARERRA%'
        """
    ).fetchall()
    print(f"clients Barrera*: {len(clients)}")
    for c in clients:
        blob = f"{c['display_name'] or ''} {c['last_name'] or ''} {c['first_name'] or ''}".upper()
        if "ADRIAN" in blob or "BLANCA" in blob or True:
            print(f"  id={c['id']} ssn={c['ssn_last4']!r} display={c['display_name']!r} "
                  f"last={c['last_name']!r} first={c['first_name']!r}")
    has = conn.execute(
        "SELECT name FROM sqlite_master WHERE name='drake_prefill_links'"
    ).fetchone()
    if has:
        links_db = conn.execute(
            """
            SELECT * FROM drake_prefill_links
            WHERE UPPER(csm_name_raw) LIKE '%BARRER%'
               OR UPPER(COALESCE(purple_name,'')) LIKE '%BARRER%'
               OR UPPER(csm_name_raw) LIKE '%ADRIAN%BARR%'
               OR UPPER(COALESCE(purple_name,'')) LIKE '%ADRIAN%BARR%'
            """
        ).fetchall()
        print(f"prefill links: {len(links_db)}")
        for L in links_db:
            print(
                f"  id={L['id']} status={L['prefill_status']} tier={L['match_tier']} "
                f"ssn={L['csm_ssn_last4']} csm={L['csm_name_raw']!r} purple={L['purple_name']!r} "
                f"variant={L['matched_variant']!r} client_id={L['client_id']}"
            )
            fp = conn.execute(
                "SELECT return_type, form_counts FROM drake_form_prefill WHERE link_id=?",
                (L["id"],),
            ).fetchone()
            if fp:
                print(f"    form return_type={fp['return_type']!r} json_len={len(fp['form_counts'])}")
            else:
                print("    (no form_prefill row)")
    else:
        print("no prefill tables")
    conn.close()
