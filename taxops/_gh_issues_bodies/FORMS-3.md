## Parent epic
Epic: Form data extraction verification

## Schema note
`**wages_tips_other`** & friends remain for legacy rows (**no DROP**) per `.cursor/rules.md` migration discipline.

## Code changes
Eliminate population of legacy mirrored columns inside **`extractor`** + **`ai_routes._save_form_data`** mapping tables — write **only canonical `box*`** fields (+ tax_year meta).

Verification:
```
rg wages_tips_other taxops/extractor.py taxops/ai_routes.py
```
→ should show **reads/migrations only**, not INSERT field maps.

## Tests
SQLite fixture proving NULL legacy mirror + populated canonical fields.

