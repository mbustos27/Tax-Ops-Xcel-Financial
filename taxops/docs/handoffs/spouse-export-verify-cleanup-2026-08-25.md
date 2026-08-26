# Handoff: Spouse export verify + wrong-attach cleanup (2026-08-25)

**Branch:** `reception-agents-hardening`  
**DB:** live `T:\taxops\taxops.db` ≡ `C:\TaxOps\taxops\taxops.db` (same share; service uses local `C:\` path)  
**Audience:** next agent / ops picking up spouse data quality  
**Related runbook:** [Spouse import](../runbooks/spouse-import.md)

---

## What happened (this session)

1. **Problem context (prior):** Drake MFJ spouse rows had been matched on the **spouse-side** name, landing data on the wrong TaxLog client. Primary-taxpayer matching was restored; 28 bad pending rows were cleared and re-imported earlier on this branch.
2. **Export verification:** Confirmed spouses were checked against Drake exports:
   - `CSVFILES/TAXPAYERspouse25.csv` (split: taxpayer first/last + spouse name)
   - `CSVFILES/TY2024Spouses.csv` (combined MFJ line)
3. **Cleanup applied:** Deleted **18** export-backed bad `spouses` rows (`WRONG_ATTACH` + `WRONG_SINGLE_HAS_SPOUSE`).
4. **Post-check:** Re-ran verify → **0** wrong-attach / single-with-spouse flags remaining.

### Counts

| When | Confirmed spouses | Pending | Notes |
|------|-------------------|---------|--------|
| Before cleanup | 463 | 0 | After earlier pending remediation |
| After `--apply` | **445** | 0 | −18 wrong attaches |
| Post re-verify | 445 | 0 | Only TEST junk still “problem” |

---

## Scripts added (not yet committed)

| Script | Role |
|--------|------|
| `scripts/verify_spouses_vs_export.py` | Read-only: every confirmed spouse vs exports; flags wrong attach, single+spouse, spouse-also-client, name variants |
| `scripts/cleanup_wrong_spouse_attaches.py` | Dry-run by default; `--apply` deletes rows from latest `reports/spouse_export_verify_*.csv` with bad verdicts. Skips `confirmed_at_intake=1` |

Earlier on branch (already committed in `8477139` / prior): `audit_confirmed_spouses.py`, `reject_wave4_orphans.py`, `docs/runbooks/spouse-import.md`.

### Re-run commands

```powershell
cd T:\taxops

python scripts\verify_spouses_vs_export.py --db T:\taxops\taxops.db
# Writes reports\spouse_export_verify_YYYYMMDD_HHMMSS.csv (gitignored)

python scripts\cleanup_wrong_spouse_attaches.py --db T:\taxops\taxops.db
# dry-run; then --apply only after review
```

---

## Key findings to preserve

### Generational suffixes are distinct people
`normalize_name` / `find_client` strip `JR`. Do **not** collapse:

| Export | Profile | Status after cleanup |
|--------|---------|----------------------|
| AGUSTIN DURAN DOB **1992-06-09**, no spouse | **#1670** AGUSTIN | Spouse **removed** (was wrong wave4 Guadalupe) |
| AGUSTIN DURAN DOB **1955-04-26**, spouse GUADALUPE | **#852** AGUSTIN JR | **Kept** — Drake import `AGUSTIN & GUADALUPE DURAN` |

### Spouses who are also clients
Export-confirmed MFJ where the spouse name also matches another TaxLog **primary** profile is **OK** (e.g. NIEVES, LEDESMA, BALBOA). That is not contamination.

### Name variants (same household)
Compound / joint client names vs Drake primary (e.g. `PADILLA GARCIA` vs `PADILLA`, `DE LA CRUZ` vs `CRUZ`) classified `OK_EXPORT_NAME_VARIANT` — not deleted.

### Do nots
- Do **not** Confirm-All on `/admin/spouses-review` after a suspicious import.
- Do **not** revive spouse-side-only `find_client` matching.
- Do **not** `--apply` Wave4 orphans (`reject_wave4_orphans.py`) without human sign-off (~187 candidates from earlier dry-run).
- Do **not** commit `taxops.db`, `.env`, spouse CSVs, or `reports/*` PII.

---

## Remaining work (open)

1. **`NO_EXPORT` (~91 after cleanup)** — mostly `wave4_clients_fold` with first-name-only spouse (`PAMELA A`) and empty `taxpayer_name`. Exports cannot confirm; needs DOB/name enrichment or Wave4 orphan pass with review.
2. **TEST clients (#2155–#2188)** — seven `UNKNOWN TEST` spouse rows flagged `SPOUSE_IS_OTHER_CLIENT_NO_EXPORT`. Safe to delete as junk when desired.
3. **#38 AJRAB** — earlier flag: spouse `NATALIE M KDEISS` matches client **#2247**; no export pair in the verify index. Re-check on next verify if it reappears.
4. **Commit** untracked scripts when ready:
   - `scripts/verify_spouses_vs_export.py`
   - `scripts/cleanup_wrong_spouse_attaches.py`
   - this handoff doc  
   Link them from `docs/runbooks/spouse-import.md` audit section.
5. **Wave4 orphans** — `reject_wave4_orphans.py` still dry-run only; separate decision from this cleanup.
6. **Optional:** clear `clients.spouse_*` columns where they still mirror deleted wrong `spouses` rows (verify script only touched `spouses` table).

---

## Latest artifacts (local, gitignored)

- `reports/spouse_export_verify_20260825_104210.csv` — post-cleanup verify  
- `reports/spouse_export_verify_20260825_103707.csv` — pre-cleanup source for the 18 deletes  
- `reports/spouse_mismatch_verified.csv` — earlier MISMATCH×export crosswalk  

---

## Quick health check for next person

```powershell
cd T:\taxops
python -c "import sqlite3; c=sqlite3.connect('taxops.db'); print(c.execute('select sum(needs_review=0), sum(needs_review=1) from spouses').fetchone())"
# Expect: (445, 0) or nearby if further edits

python scripts\verify_spouses_vs_export.py --db T:\taxops\taxops.db
# Expect: WRONG_ATTACH=0, WRONG_SINGLE_HAS_SPOUSE=0
```

Admin: `/admin/spouses-review` should stay ~0 pending.
