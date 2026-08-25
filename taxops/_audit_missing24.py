from collections import Counter, defaultdict
from pathlib import Path
import hashlib
from drake_prefill_importer import (
    load_csm_xlsx, composite_dedupe_csm, join_purple_chunks, match_csm_to_purple,
    normalize_purple_name, csm_name_variants,
)
from db import get_connection, get_schema_version

def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(1<<20), b''):
            h.update(chunk)
    return h.hexdigest()[:16]

files = [
    (r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx", r"C:\Users\Windows 10\Desktop\2024 CLIENTS.xlsx"),
    (r"T:\taxops\CSVFILES\TY2024S.csv", r"C:\Users\Windows 10\Desktop\TY2024S.csv"),
]
print("=== File identity CSVFILES vs Desktop ===")
for a,b in files:
    pa, pb = Path(a), Path(b)
    print(f"{pa.name}: csvfiles={pa.stat().st_size if pa.exists() else None} desktop={pb.stat().st_size if pb.exists() else None}")
    if pa.exists() and pb.exists():
        print(f"  sha256-16 equal? {sha(a)==sha(b)}  ({sha(a)} vs {sha(b)})")

CSM = r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx"
rows = load_csm_xlsx(CSM)
dedupe = composite_dedupe_csm(rows)
print("\n=== CSM identity stats ===")
print(f"rows={len(rows)} clients={dedupe.clients_after}")
print(f"unique ssn_last4 among kept: {len({c['csm_ssn_last4'] for c in dedupe.clients})}")
# same SSN, multiple names
by_ssn = defaultdict(list)
for c in dedupe.clients:
    by_ssn[c['csm_ssn_last4']].append(c['csm_name_raw'])
multi = {k:v for k,v in by_ssn.items() if len(v)>1}
print(f"SSNs with multiple distinct name_norms kept: {len(multi)}")
if multi:
    for k,v in list(multi.items())[:15]:
        print(f"  {k}: {v}")

purple, notes, rc, cc = join_purple_chunks([
    r"T:\taxops\CSVFILES\TY2024S.csv",
    r"T:\taxops\CSVFILES\TY2024S2439-5405.csv",
    r"T:\taxops\CSVFILES\TY2024S8867-end.csv",
])
links, tiers, variants, stats = match_csm_to_purple(dedupe.clients, purple)

# Purple without PRIOR_YEAR — who are they?
matched_p = {
    normalize_purple_name(L.purple_name)
    for L in links
    if L.prefill_status == "PRIOR_YEAR_FORMS_AVAILABLE" and L.purple_name
}
# also purple attached to manual/lowconf as candidate
cand_p = {
    normalize_purple_name(L.purple_name)
    for L in links
    if L.purple_name and L.prefill_status in ("NEEDS_MANUAL_LINK", "LOW_CONFIDENCE_NO_MATCH")
}
all_p = {p.name_norm: p.taxpayer_name for p in purple}
unlinked = sorted(n for n in all_p if n not in matched_p)
print(f"\n=== Purple without PRIOR_YEAR_FORMS link: {len(unlinked)} ===")
print(f"of those also a manual/lowconf candidate: {len([n for n in unlinked if n in cand_p])}")
print(f"true purple orphans (no CSM link of any kind): {len([n for n in unlinked if n not in cand_p])}")
print("sample unlinked purple (40):")
for n in unlinked[:40]:
    tag = "candidate" if n in cand_p else "ORPHAN"
    print(f"  [{tag}] {all_p[n]}")

# Live DB state
print("\n=== Live DB prefill gap ===")
for db in [r"T:\taxops\taxops.db", r"T:\taxops\taxops_prefill_apply_test.db"]:
    c = get_connection(db)
    ver = get_schema_version(c)
    has = c.execute("SELECT name FROM sqlite_master WHERE name='drake_prefill_links'").fetchone()
    n = c.execute("SELECT COUNT(*) n FROM drake_prefill_links").fetchone()["n"] if has else 0
    print(f"{db}: schema={ver} links={n}")
    c.close()

# Blank-name summary (finish earlier audit)
empty = [r for r in rows if not r.name_norm]
named_ssns = {r.ssn_last4 for r in rows if r.name_norm}
only_empty = {r.ssn_last4 for r in empty} - named_ssns
print("\n=== Blank Client Name skip summary ===")
print(f"blank-name rows: {len(empty)}")
print(f"SSNs with named twin (not lost): {len({r.ssn_last4 for r in empty} & named_ssns)}")
print(f"SSNs truly nameless / LOST: {only_empty}")
for r in empty:
    if r.ssn_last4 in only_empty:
        print(f"  LOST: ssn={r.ssn_last4} status={r.status} type={r.return_type} started={r.started} changed={r.last_change_raw} by={r.changed_by}")
