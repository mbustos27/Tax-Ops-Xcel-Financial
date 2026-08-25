# Spouse import runbook

**Audience:** Ops / staff running Drake MFJ spouse imports  
**Related:** [Operations Runbook](../RUNBOOK.md) · `scripts/import_spouse_info.py` · `scripts/remediate_spouse_pending.py`

> **Workstation vs server:** `T:\taxops\...` (this workstation) and `C:\TaxOps\taxops\...` (TaxOpsService host) are the **same share**, not two copies. Prefer `--db T:\taxops\taxops.db` from the workstation; the service uses `C:\TaxOps\taxops\taxops.db` (local path — SQLite WAL over SMB fails).

---

## Sanctioned import path (only)

Use **primary-taxpayer matching** only:

```powershell
cd T:\taxops   # or C:\TaxOps\taxops on the server

# Dry-run first
python scripts\import_spouse_info.py "f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv" --db T:\taxops\taxops.db --dry-run

# Apply
python scripts\import_spouse_info.py "f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv" --db T:\taxops\taxops.db
```

Optional TY2024-style CSV (`Taxpayer Name` + `Spouse Name` columns):

```powershell
python scripts\import_spouse_info.py CSVFILES\TY2024Spouses.csv --combined-format --db T:\taxops\taxops.db --dry-run
```

To clear bad **pending** review rows then re-import:

```powershell
python scripts\remediate_spouse_pending.py --db T:\taxops\taxops.db --csv "f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv"
# review dry-run, then:
python scripts\remediate_spouse_pending.py --apply --db T:\taxops\taxops.db --csv "f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv"
```

Admin UI: `/admin/spouses-review` — prefer per-row ✕ Wrong over **Confirm All** after a bad import.

---

## Correct model

Drake MFJ line:

```text
PRIMARY [& SPOUSE]
```

Example: `ARGELIS ORTIZ & SANDRA CANIZALES`

1. Parse **primary** with `name_matcher.parse_mfj_primary_taxpayer()` → `(ORTIZ, ARGELIS)`.
2. Match that pair to a TaxLog **client** (`find_client`, thresholds below).
3. Store spouse first / last / DOB on **that** client’s `spouses` row (one row per `client_id`).

Shared-surname lines like `PEDRO & MARIA CARDONA` resolve primary as `(CARDONA, PEDRO)`.

Thresholds (from `name_matcher.py`):

| Constant | Value | Meaning |
|---|---|---|
| `ACCEPT_THRESHOLD` | **88** | Auto-accept primary match (`needs_review=0` when unambiguous) |
| `REVIEW_THRESHOLD` | **70** | Below this → no client attach / review CSV |

Import also **skips self-spouse** matches (TaxLog client name ≈ spouse name) so data does not land on the spouse’s client record.

---

## Historical bug (deprecated approach)

**Do not** match the Drake **spouse-side** name (e.g. `SANDRA CANIZALES`) to TaxLog via `find_client()` and attach the row there.

That pattern often scored **90** (same last name + first-token heuristics ≥ 88), so spouse data landed on the **wrong household** (e.g. `CANIZALES, ROSA` instead of `ORTIZ, ARGELIS`). Review queued some cases (`r1_spouse_sot:drake_no_spouse`); many “90% Conf.” pending rows were systematically wrong.

The spouse-side matcher is **deprecated**. The only sanctioned scripts are `import_spouse_info.py` (primary match) and `remediate_spouse_pending.py`.

---

## Live source file

Purple-sheet / Drake spouse export (preferred):

```text
f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv
```

Skip first 2 metadata lines; header includes `Taxpayer Name`, `Spouse Last Name`, `Spouse First Name`, `Spouse Date of Birth`, etc. Prefer **Spouse Date of Birth** over “Spouse Birthday” (current-year reminder).

`CSVFILES\TY2024Spouses.csv` is an alternate combined format — use `--combined-format`.

---

## Audit / cleanup helpers (read-only by default)

```powershell
# Confirmed-row primary mismatch report (no DB writes)
python scripts\audit_confirmed_spouses.py --db T:\taxops\taxops.db

# Wave4 0% orphans with no Drake spouse line (dry-run; --apply only after sign-off)
python scripts\reject_wave4_orphans.py --db T:\taxops\taxops.db
```

CSV audits under `reports/` are gitignored (may contain names). Do not commit `taxops.db`, spouse CSVs, or PII.
