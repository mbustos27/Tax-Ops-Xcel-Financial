# A4 — Invariant guards

_Generated: 2026-08-12T22:50:47Z_

**Overall: 5 PASS / 1 MODIFIED / 1 FAIL**

| Guard | Title | Result |
|---|---|---|
| I1 | Zero duplicate (log_number, tax_year) in returns | `PASS` |
| I2 | Zero entity_link with sole evidence last-4 | `PASS` |
| I3 | No two clients share (norm last, norm first, ssn_last4) non-null | `PASS` |
| I4 | Every drake_prefill_links.client_id resolves to a client | `PASS` |
| I5 | ux_returns_log_year and idx_returns_unique_client_year present+UNIQUE | `PASS` |
| I6 | TaxOps DB unchanged across audit run (mtime + size) | `MODIFIED` |
| I7 | Tax Log CSV import idempotency (throwaway copy) | `FAIL` |

## I1 — Zero duplicate (log_number, tax_year) in returns

- **Result:** `PASS`
- **Detail:** `{"duplicate_groups": 0, "samples": []}`

## I2 — Zero entity_link with sole evidence last-4

- **Result:** `PASS`
- **Detail:** `{"samples": [], "violations": 0}`

## I3 — No two clients share (norm last, norm first, ssn_last4) non-null

- **Result:** `PASS`
- **Note:** Hard uniqueness on identity triple; distinct from exact-name-only twins without SSN.
- **Detail:** `{"duplicate_groups": 0, "samples": {}}`

## I4 — Every drake_prefill_links.client_id resolves to a client

- **Result:** `PASS`
- **Detail:** `{"linked_rows": 1264, "orphan_ids": [], "orphans": 0}`

## I5 — ux_returns_log_year and idx_returns_unique_client_year present+UNIQUE

- **Result:** `PASS`
- **Note:** db.py skips creating ux_returns_log_year when duplicate pairs exist.
- **Detail:** `{"blocking_log_year_dups": 0, "idx_client_year_unique": true, "idx_returns_unique_client_year": true, "index_names": ["idx_returns_client_year", "idx_returns_proc_year", "idx_returns_status_year", "idx_returns_unique_client_year", "idx_returns_updated_at", "ux_returns_log_year"], "ux_returns_log_year": true, "ux_unique": true}`

## I6 — TaxOps DB unchanged across audit run (mtime + size)

- **Result:** `MODIFIED`
- **Note:** PASS = mtime+size stable. MODIFIED = mtime drifted (NSSM/Flask) but size unchanged (RO URI + throwaway copy only). FAIL = size changed.
- **Detail:** `{"external_mtime_touch": true, "mtime_after_ns": 1786575009948095500, "mtime_before_ns": 1786574888938988500, "mtime_unchanged": false, "original_condition": "mtime_unchanged", "original_condition_held": false, "size_after": 10461184, "size_before": 10461184, "size_unchanged": true, "weaker_condition": "size_unchanged", "weaker_condition_held": true}`

## I7 — Tax Log CSV import idempotency (throwaway copy)

- **Result:** `FAIL`
- **Note:** Second pass must create 0 clients AND 0 returns (catches log#-match and _upsert_return gaps). See pass2_return_diagnosis when FAIL.
- **Detail:** `{"clients_after_pass1": 1526, "clients_after_pass2": 1526, "clients_before": 1521, "clients_idempotent": true, "csv": "C:\\Users\\Windows 10\\OneDrive - Xcel Financial Services LLC\\Shared\\Logs\\TAX LOG 2025 Live.csv", "pass1": {"created_clients": 5, "created_returns": 2, "errors": 326, "review": 604, "success": 321, "updated_clients": 41, "updated_returns": 275}, "pass2": {"created_clients": 0, "created_returns": 1, "errors": 325, "review": 604, "success": 322, "updated_clients": 4, "updated_returns": 7}, "pass2_created_clients": 0, "pass2_created_returns": 1, "pass2_new_returns": [{"client_id": 2441, "client_status": "LOG OUT", "created_at": "2026-08-12T22:50:26+00:00", "first_name": "ANDRES", "id": 2843, "last_name": "PEREZ & GARCIA VILLAREAL", "log_number": "141", "tax_year": 2025}], "pass2_return_diagnosis": [{"class": "no_prior_match_on_log_year \u2014 pass1 missed or assigned log during upsert; or non-determinism in _upsert_return / matcher", "prior_same_log_year": [], "return": {"client_id": 2441, "client_status": "LOG OUT", "created_at": "2026-08-12T22:50:26+00:00", "first_name": "ANDRES", "id": 2843, "last_name": "PEREZ & GARCIA VILLAREAL", "log_number": "141", "tax_year": 2025}}], "returns_after_pass1": 1609, "returns_after_pass2": 1610, "returns_before": 1607, "returns_idempotent": false, "throwaway": "T:\\audit\\tmp\\a4_idempotency_throwaway.sqlite"}`

## Scope

- Live TaxOps is read-only for guards I1–I6.
- I7 writes only to `T:\audit\tmp\a4_idempotency_throwaway.sqlite`.
- Violations are hard alerts — separate from A2/A3 findings.
- Status semantics (Amendment 2 C3): `PASS` = original condition held; `MODIFIED` = original failed but documented weaker condition held; `FAIL` = neither held.
