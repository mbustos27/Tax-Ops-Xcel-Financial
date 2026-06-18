## Parent epic
Epic: Email watcher stability (`mail_watcher.py`)

## Problem statement
Suspected unsolicited rows in **`known_sender_rules`** attributed to `'from-llm-suggestion'` / `'auto-suggested'` semantics.

## Required audit
```
rg -n "INSERT .*known_sender_rules|into known_sender_rules" taxops/
```
Enumerate **every writer** besides:
1. Admin UI add route (**`/api/email-suggestions` family / known sender endpoints** — verify exact handlers in **`app.py`**).
2. Staff **Accept suggestion** pathway (explicit POST).

Also review triggers from **`mail_watcher._update_domain_cache`** & graduation helpers referencing **`rule_suggestions`** ONLY (never direct insert).

## Fix strategy
Strip rogue inserts; optionally data cleanup migration script:
```sql
DELETE FROM known_sender_rules WHERE note IN ('from-llm-suggestion','auto-suggested');
```

Optional guardrail: audit log WARN on unexpected insert site.

## Definition of done
- `rg`/code search shows writer list matches expectation.
- 24-hour soak: counts stable without surprise domain rows.

