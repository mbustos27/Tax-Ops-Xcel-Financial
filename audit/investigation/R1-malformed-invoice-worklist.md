# R1 — Non-canonical invoice worklist (office)

_Generated: 2026-08-13T20:20:36Z · rule: only `^25\d{4}$` joins; no lenient bare-parse_

**23** distinct non-canonical invoices (27 rows).

## Hazard

- **`25141`** (PEREZ, ANDRES) — lenient→`141` — **COLLIDES_CLEAN_BARE_141_PEREZ**. Never parse.

## By class

| Class | n rows | Proposed reading |
|---|---:|---|
| `25_plus_3` | 11 | lenient bare = last 3 digits; likely missing zero-pad — confirm then rewrite as 25XXXX |
| `7_digit` | 6 | extra digit; position ambiguous — confirm |
| `stub` | 5 | never parse (`25`/`250`/`251`) |
| `not_25_prefix` | 5 | prior season or typo — reject for TY2025 join |

## Full list

| Invoice | Class | Lenient bare | Verdict |
|---|---|---|---|
| `210273` | `not_25_prefix` | `210273` | prior season or typo — reject |
| `210729` | `not_25_prefix` | `210729` | prior season or typo — reject |
| `23229` | `not_25_prefix` | `23229` | prior season or typo — reject |
| `25` | `stub` | `—` | never parse |
| `250` | `stub` | `—` | never parse |
| `2500786` | `7_digit` | `786` | extra digit; position ambiguous — confirm |
| `2500892` | `7_digit` | `892` | extra digit; position ambiguous — confirm |
| `2501016` | `7_digit` | `1016` | extra digit; position ambiguous — confirm |
| `2501018` | `7_digit` | `1018` | extra digit; position ambiguous — confirm |
| `2501177` | `7_digit` | `1177` | extra digit; position ambiguous — confirm |
| `25037` | `25_plus_3` | `37` | plausible missing zero-pad — confirm |
| `25055` | `25_plus_3` | `55` | plausible missing zero-pad — confirm |
| `25063` | `25_plus_3` | `63` | plausible missing zero-pad — confirm |
| `2507598` | `7_digit` | `7598` | extra digit; position ambiguous — confirm |
| `251` | `stub` | `1` | never parse |
| `25141` | `25_plus_3` | `141` | NEVER parse — manufactures I7 collision on PEREZ/141 **HAZARD** |
| `25392` | `25_plus_3` | `392` | plausible missing zero-pad — confirm |
| `25488` | `25_plus_3` | `488` | plausible missing zero-pad — confirm |
| `25537` | `25_plus_3` | `537` | plausible missing zero-pad — confirm |
| `25549` | `25_plus_3` | `549` | plausible missing zero-pad — confirm |
| `25678` | `25_plus_3` | `678` | plausible missing zero-pad — confirm |
| `25735` | `25_plus_3` | `735` | plausible missing zero-pad — confirm |
| `50688` | `not_25_prefix` | `50688` | prior season or typo — reject |
