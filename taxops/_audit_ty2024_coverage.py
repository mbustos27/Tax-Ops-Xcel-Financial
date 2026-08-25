"""Audit TY2024 client coverage: CSM raw → dedupe → purple → prefill DB → TaxOps clients/returns."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from db import get_connection, get_schema_version
from drake_prefill_importer import (
    composite_dedupe_csm,
    join_purple_chunks,
    load_csm_xlsx,
    match_csm_to_purple,
    normalize_csm_name,
    extract_ssn_last4,
    _cell_str,
)

CSM = r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx"
CHUNKS = [
    r"T:\taxops\CSVFILES\TY2024S.csv",
    r"T:\taxops\CSVFILES\TY2024S2439-5405.csv",
    r"T:\taxops\CSVFILES\TY2024S8867-end.csv",
]
DBS = [
    r"T:\taxops\taxops_prefill_apply_test.db",
    r"T:\taxops\taxops.db",
]

print("=== 1. CSM raw load ===")
rows = load_csm_xlsx(CSM)
print(f"rows_loaded: {len(rows)}")
skip_empty_ssn = sum(1 for r in rows if not r.ssn_last4)
skip_empty_name = sum(1 for r in rows if not r.name_norm)
skip_both = sum(1 for r in rows if not r.ssn_last4 and not r.name_norm)
skip_ssn_only = sum(1 for r in rows if not r.ssn_last4 and r.name_norm)
skip_name_only = sum(1 for r in rows if r.ssn_last4 and not r.name_norm)
print(f"empty ssn (any): {skip_empty_ssn}")
print(f"empty name (any): {skip_empty_name}")
print(f"  both empty: {skip_both}")
print(f"  ssn empty, name present: {skip_ssn_only}")
print(f"  name empty, ssn present: {skip_name_only}")

# Sample skipped with name but no ssn
print("\n--- Sample: name present, SSN missing (up to 25) ---")
n = 0
for r in rows:
    if not r.ssn_last4 and r.name_norm:
        print(f"  {r.name_raw!r}  status={r.status!r}  last_change={r.last_change_raw!r}")
        n += 1
        if n >= 25:
            break
print(f"(showing {n})")

dedupe = composite_dedupe_csm(rows)
print(f"\n=== 2. After composite dedupe ===")
print(f"skipped empty name/ssn: {dedupe.empty_name_or_ssn_skipped}")
print(f"clients_after: {dedupe.clients_after}")
print(f"disposition: {dict(dedupe.disposition_counts)}")

purple, notes, rc, cc = join_purple_chunks(CHUNKS)
print(f"\n=== 3. Purple ===")
for note in notes:
    print(f"  OK {note}")
print(f"purple rows: {len(purple)}  unique norms: {len({p.name_norm for p in purple})}")

links, tiers, variants, stats = match_csm_to_purple(dedupe.clients, purple)
status_counts = Counter(L.prefill_status for L in links)
print(f"\n=== 4. Prefill match status ===")
for k, v in status_counts.most_common():
    print(f"  {v:5d}  {k}")

csm_keys = {(c["csm_ssn_last4"], c["csm_name_norm"]) for c in dedupe.clients}
link_keys = {(L.csm["csm_ssn_last4"], L.csm["csm_name_norm"]) for L in links}
print(f"CSM keys without link draft: {len(csm_keys - link_keys)}")  # should be 0

# Purple names not matched to any PRIOR_YEAR CSM
matched_purple = {
    L.purple_name for L in links if L.prefill_status == "PRIOR_YEAR_FORMS_AVAILABLE" and L.purple_name
}
# normalize compare
from drake_prefill_importer import normalize_purple_name
matched_pnorms = {
    normalize_purple_name(L.purple_name)
    for L in links
    if L.prefill_status == "PRIOR_YEAR_FORMS_AVAILABLE" and L.purple_name
}
all_pnorms = {p.name_norm for p in purple}
orphan_purple = all_pnorms - matched_pnorms
print(f"purple norms with no PRIOR_YEAR link: {len(orphan_purple)}")

print("\n=== 5. TaxOps DB comparison ===")
for db_path in DBS:
    p = Path(db_path)
    if not p.exists():
        print(f"\n{db_path}: MISSING")
        continue
    conn = get_connection(str(p))
    ver = get_schema_version(conn)
    has_prefill = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='drake_prefill_links'"
    ).fetchone()
    print(f"\n--- {db_path} (schema {ver}) ---")

    # Returns / clients for tax_year 2024
    try:
        r2024 = conn.execute(
            "SELECT COUNT(*) n FROM returns WHERE tax_year = 2024"
        ).fetchone()["n"]
        c2024 = conn.execute(
            """
            SELECT COUNT(DISTINCT client_id) n FROM returns WHERE tax_year = 2024
            """
        ).fetchone()["n"]
        print(f"returns ty2024: {r2024}  distinct clients: {c2024}")
    except Exception as e:
        print(f"returns query failed: {e}")
        r2024 = c2024 = None

    if has_prefill:
        n_links = conn.execute(
            "SELECT COUNT(*) n FROM drake_prefill_links WHERE tax_year = 2024"
        ).fetchone()["n"]
        by_st = conn.execute(
            """
            SELECT prefill_status, COUNT(*) n FROM drake_prefill_links
            WHERE tax_year = 2024 GROUP BY prefill_status ORDER BY n DESC
            """
        ).fetchall()
        print(f"prefill links ty2024: {n_links}")
        for row in by_st:
            print(f"  {row['n']:5d}  {row['prefill_status']}")
        linked = conn.execute(
            """
            SELECT COUNT(*) n FROM drake_prefill_links
            WHERE tax_year = 2024 AND client_id IS NOT NULL
            """
        ).fetchone()["n"]
        print(f"prefill with client_id set: {linked}")
    else:
        print("no drake_prefill_links table")

    # TaxOps 2024 clients whose ssn_last4+name don't appear in CSM dedupe
    # Build CSM index by ssn_last4
    csm_by_ssn = defaultdict(list)
    for c in dedupe.clients:
        csm_by_ssn[c["csm_ssn_last4"]].append(c)

    if c2024 is not None:
        rows_db = conn.execute(
            """
            SELECT c.id, c.ssn_last4, c.last_name, c.first_name, c.display_name,
                   r.id AS return_id, r.log_number, r.client_status
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE r.tax_year = 2024
            """
        ).fetchall()
        missing_from_csm = []
        ssn_hit_name_miss = []
        in_csm = 0
        no_ssn = 0
        for r in rows_db:
            ssn = (r["ssn_last4"] or "").strip()
            if not ssn or ssn == "0000":
                no_ssn += 1
                missing_from_csm.append(r)
                continue
            cands = csm_by_ssn.get(ssn, [])
            if not cands:
                missing_from_csm.append(r)
                continue
            # name soft check
            dn = normalize_csm_name(
                r["display_name"]
                or f"{r['last_name'] or ''}, {r['first_name'] or ''}"
            )
            if any(c["csm_name_norm"] == dn for c in cands):
                in_csm += 1
            else:
                # still count as present in CSM by SSN (possible name format diff)
                in_csm += 1
                ssn_hit_name_miss.append((r, [c["csm_name_raw"] for c in cands]))

        print(f"TaxOps ty2024 returns matched to CSM by ssn_last4: {in_csm}")
        print(f"TaxOps ty2024 returns with no CSM ssn_last4: {len(missing_from_csm)} (no_ssn_in_taxops={no_ssn})")
        print(f"SSN hit but display_name != csm_name_norm: {len(ssn_hit_name_miss)}")
        print("\n--- TaxOps 2024 clients NOT in CSM (up to 40) ---")
        for r in missing_from_csm[:40]:
            print(
                f"  client_id={r['id']} log={r['log_number']} "
                f"ssn={r['ssn_last4']!r} "
                f"name={r['display_name'] or (str(r['last_name'])+', '+str(r['first_name']))!r} "
                f"status={r['client_status']!r}"
            )
        if len(missing_from_csm) > 40:
            print(f"  ... +{len(missing_from_csm)-40} more")

    conn.close()

# CSM clients not in TaxOps at all (by ssn)
print("\n=== 6. CSM clients with no TaxOps client (ssn_last4) — using apply_test if present else taxops.db ===")
db_path = next((d for d in DBS if Path(d).exists()), None)
if db_path:
    conn = get_connection(db_path)
    taxops_ssns = {
        (r["ssn_last4"] or "").strip()
        for r in conn.execute("SELECT DISTINCT ssn_last4 FROM clients").fetchall()
        if (r["ssn_last4"] or "").strip()
    }
    # Also ty2024-only
    taxops_ssns_2024 = {
        (r["ssn_last4"] or "").strip()
        for r in conn.execute(
            """
            SELECT DISTINCT c.ssn_last4 FROM clients c
            JOIN returns r ON r.client_id = c.id
            WHERE r.tax_year = 2024
            """
        ).fetchall()
        if (r["ssn_last4"] or "").strip()
    }
    csm_not_in_taxops = [c for c in dedupe.clients if c["csm_ssn_last4"] not in taxops_ssns]
    csm_not_in_ty2024 = [c for c in dedupe.clients if c["csm_ssn_last4"] not in taxops_ssns_2024]
    print(f"DB: {db_path}")
    print(f"CSM clients whose ssn_last4 absent from clients table: {len(csm_not_in_taxops)}")
    print(f"CSM clients whose ssn_last4 absent from ty2024 returns: {len(csm_not_in_ty2024)}")
    disp = Counter(c["disposition_status"] for c in csm_not_in_ty2024)
    print("CSM-not-in-ty2024-returns by disposition:")
    for k, v in disp.most_common():
        print(f"  {v:5d}  {k}")
    print("\n--- Sample CSM not in ty2024 returns (30) ---")
    for c in csm_not_in_ty2024[:30]:
        print(
            f"  {c['csm_ssn_last4']}  {c['csm_name_raw']!r}  "
            f"{c['disposition_status']}  status={c['csm_status_raw']!r}"
        )
    conn.close()

print("\nDONE")
