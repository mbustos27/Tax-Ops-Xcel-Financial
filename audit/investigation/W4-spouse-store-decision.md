# W4 — Spouse store decision

_Decided: 2026-08-11. Input: I1 spouse fill rates + live counts (clients spouse names 406/375; spouses 166; clients-only 354)._

## Decision

| Store | Role |
|---|---|
| **`spouses`** | **Canonical** relational spouse record (one row per client via `idx_spouses_one_per_client`) |
| **`clients.spouse_first_name` / `spouse_last_name`** | **Deprecated read-through** during transition; frozen against *new* semantic writes where practical; kept populated for UI that still reads clients columns |
| **`clients.spouse_dob` / `spouse_cell` / `spouse_work_phone` / `spouse_email`** | **Dead** (0–1 filled rows) — do not write; leave columns in place (SQLite drop is a rebuild; defer) |
| **`drake_household_prefill`** | Staging only — never write-through to production spouse identity |

## Reconciliation (one-time)

Fold every client that has `spouse_last_name` (or first) and **no** `spouses` row into `spouses` with `source='wave4_clients_fold'`. Do not overwrite an existing `spouses` row (keep wins).

`SPOUSE_AMBIGUOUS` findings stay human — folding names does not resolve genuine ambiguity.

## Acceptance

- `spouses` row count rises by ~354 (clients-only set)
- New intake / prefill spouse fills prefer `spouses` upsert
- Priority worklist can drop pure `SPOUSE_STORE_DIVERGENCE` rows that were clients-only vs empty spouses after A2 re-run
