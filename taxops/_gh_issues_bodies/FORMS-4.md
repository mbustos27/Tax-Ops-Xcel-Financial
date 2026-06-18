## Parent epic
Epic: Form data extraction verification

## Target UI
`t taxops/templates/return_detail.html` — JS builders:
- **`nonzeroOrderedKeys`**, **`fdFieldLabel`**, **`FORM_GROUP_DEFS`** map

## Enhancements
- Expand label dictionary / IRS wording for ALL visible keys.
- Group sections (Fed / FICA / State / Local).
- Maintain inline edit **`/return/<id>/form-data/.../update`** behavior.

## Definition of done
No raw snake_case bleed in staff-facing DOM for shipped keys; empties suppressed.

