# A2 — Stable findings & disposition delta

_Generated: 2026-08-12T22:49:56Z_
_Run label: `lucy-gate-remeasure`_
_Disposition DB: `T:\audit\audit_disposition.sqlite`_

## Headline (NEW + REGRESSED)

**0**  *(not total open findings)*

| Bucket | Count |
|---|---:|
| NEW | 0 |
| RECURRING | 133 |
| RESOLVED (absent this run) | 0 |
| REGRESSED | 0 |

### NEW by type


### RECURRING by type

- `NAME_TRUNCATED`: 72
- `L0_DRAKE_ONLY`: 33
- `LOG_NUMBER_COLLISION`: 19
- `MALFORMED_LOG_NUMBER`: 9

## Fingerprint recipe (per type)

`finding_id = sha256(finding_type || entity_key || salient_payload)`

Canonical entity keys **exclude** `created_at`, TaxOps row ids (merge-volatile), and raw untruncated names.

- **CONFIRMED_MATCH:** Skipped for disposition worklist (noise). Fingerprint exists for completeness: bare_log|last4|norm_name.
- **DUPLICATE_CLIENT:** entity_key = sorted(norm_a, norm_b) joined by '||'. Client ids excluded — merges would churn fingerprints.
- **L0_DRAKE_ONLY:** entity_key = bare_log|tax_year for L0-eligible Drake invoice with no TaxOps and no Log hit.
- **L0_KEY_PRESENT:** entity_key = bare_log|tax_year for successful invoice L0 links (informational / coverage; usually not worklist).
- **L5_UNMATCHED:** entity_key = bare_log|norm_invoice_name for leftover L0-eligible invoice taxpayers after ladder.
- **LOGGED_NOT_PREPARED:** entity_key = bare_log|tax_year (Tax Log col B normalized). Sheet row numbers churn; bare log does not.
- **LOG_NUMBER_COLLISION:** entity_key = bare_log (Amendment 2 C2). Salient = n_taxpayers. Pre-C2 collisions keyed on raw invoice are a different fingerprint space.
- **MALFORMED_LOG_NUMBER:** entity_key = raw invoice string (digits as exported). Salient unused. Bare form lives in detail only — do not put bare in entity_key (would churn when normalize rules tighten).
- **MERGE_UNTRACEABLE:** Singleton: entity_key='global'. Emitted when there is no durable merge history table. May coexist with MERGE_PARTIAL_TRAIL (audit_log keep_id/discard_id only; no discarded-identity snapshot).
- **MISSING_IN_TAXOPS:** entity_key = last4|' '|norm_drake_name or bare invoice/log if known. No Drake stage_id.
- **NAME_TRUNCATED:** entity_key = last4|' '|normalized_csm_name_prefix (folded, no stage_id). Survives CSM re-export row order and TaxOps merges; changes only if CSM last4 or truncated display string changes. Amendment 2 C6: informational — CSM 40-char display artifact; L0 invoice / L1 carry identity; not Priority.
- **NEEDS_HUMAN:** entity_key = subtype|' '|stable subject key (last4/name/bare_log). Subtype included so distinct review reasons don't collapse.
- **PHANTOM_IN_TAXOPS:** entity_key = bare_log|tax_year if log present, else norm_name(last|first). Avoids TaxOps client_id (merge attrition).
- **PREPARED_NOT_LOGGED:** entity_key = last4|' '|norm_drake_name (Drake prepared, no log link). Prefer bare_log when invoice available.
- **SPOUSE_AMBIGUOUS:** entity_key = last4|' '|norm_drake_name.
- **SPOUSE_STORE_DIVERGENCE:** entity_key = norm_client_name|tax_year (or last4 when present). No client_id.

## Jul31 backfill

`{"c1_legacy_malformed_seed": {"inserted": 0, "legacy_malformed": 22, "skipped": 22}, "existing_rows": 1850, "skipped": true}`

Weak reconstructions use `first_seen_run='pre-baseline'` (not faked as jul31).

## Merge attrition trail

- **Verdict:** `MERGE_TRAIL_COMPLETE`
- **Rationale:** client_merge_history present with discard identity snapshot columns; 7 history row(s). Merges are reconstructable from the trail.
- audit_log merge API rows: 89
- payloads with keep/discard: 86
- merge_* tables: `['client_merge_history']`

## Sample NEW findings (≤15)


## Disposition schema

```
audit_disposition(finding_id PK, finding_type, entity_key, status,
  resolved_by, resolved_at, note, first_seen_run, last_seen_run, ...)
status ∈ OPEN | ACKED | WONTFIX | RESOLVED | FALSE_POSITIVE
```

DB path is durable across runs and **never truncated** by the audit tool.
