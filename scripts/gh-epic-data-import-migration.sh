#!/usr/bin/env bash
# One-shot: create epic + DoD child issues for data import & migration.
# Run:  bash scripts/gh-epic-data-import-migration.sh
set -euo pipefail
REPO="${REPO:-mbustos27/Tax-Ops-Xcel-Financial}"

EPIC_BODY=$(cat <<'MD'
## Problem
We cannot reliably migrate existing data from Drake CSV and the manual Excel log into TaxOps without:
- duplicate clients being created
- inconsistent log numbers
- weak linking between records

This prevents TaxOps from replacing the current spreadsheet workflow.

## Why it matters
If import is unreliable, staff will not trust or adopt the system.
This is the gateway feature for real usage.

## Goal
Successfully import Drake CSV + manual Excel log into TaxOps with:
- correct client matching
- no duplicate client creation (or flagged for review)
- consistent log number assignment per return

## Scope (Minimum version)
- Import Drake CSV (drake_2025.csv)
- Import manual Excel log (TAX LOG 2025 Live.csv)
- Use existing fuzzy matcher to attempt client matching
- If match confidence is high → link to existing client
- If match confidence is low → send to Review Queue
- Assign log numbers to all imported returns
- Prevent exact duplicate return creation (same client + tax year)

## Out of Scope (not in this task)
- Perfect matching accuracy
- UI improvements to Review Queue
- Multi-year client merging logic
- Automated duplicate cleanup tools
- OpenClaw / LLM matching

## Notes / Context
- Fuzzy matching already exists (name_matcher.py)
- Review Queue already exists for low-confidence matches
- Log number system must align with existing workflow
- Manual log is currently source of truth

## Definition of Done
Track each item as a child issue in this repo (see linked issues).
MD
)

EPIC_URL=$(gh issue create --repo "$REPO" --title "EPIC: Data import & client matching (Drake + manual log)" --body "$EPIC_BODY")
# URL like https://github.com/org/repo/issues/22
EPIC_NUM=$(echo "$EPIC_URL" | sed -n 's#.*/issues/\([0-9]*\).*#\1#p')
if [[ -z "$EPIC_NUM" ]]; then
  echo "Could not parse epic number from: $EPIC_URL" >&2
  exit 1
fi

echo "Created epic #$EPIC_NUM: $EPIC_URL"
echo

child() {
  local title="$1" labels="$2" body="$3"
  gh issue create --repo "$REPO" --title "$title" --label "$labels" --body "$body"
}

ref="Related to epic #$EPIC_NUM.

"

child "DoD: Drake CSV imports without breaking" "P1 – Workflow,S,feature" "${ref}## Acceptance
- [ ] \`drake_2025.csv\` (or equivalent) processes end-to-end
- [ ] No uncaught import errors; import batch recorded
- [ ] Data lands in the expected returns/client tables
"

child "DoD: Manual Excel log imports without breaking" "P1 – Workflow,S,feature" "${ref}## Acceptance
- [ ] \`TAX LOG 2025 Live.csv\` (or exported CSV) processes end-to-end
- [ ] Column mapping matches office log workflow; import batch recorded
- [ ] No uncaught import errors
"

child "DoD: High-confidence client matching" "P1 – Workflow,M,feature" "${ref}## Acceptance
- [ ] Fuzzy / deterministic matcher (name_matcher) used on import
- [ ] When confidence is above the agreed threshold, records link to the correct existing \`client\` row
- [ ] New clients are only created when no acceptable match exists
"

child "DoD: Low-confidence matches in Review Queue" "P1 – Workflow,M,feature" "${ref}## Acceptance
- [ ] Low-confidence or ambiguous matches create \`review_queue\` entries (or equivalent)
- [ ] Staff can see them in the Review UI and resolve (link / new client)
- [ ] No silent wrong links for ambiguous cases
"

child "DoD: No duplicate returns (client + tax year)" "P1 – Workflow,M,feature" "${ref}## Acceptance
- [ ] Re-importing the same source row does not create a second return for the same \`(client_id, tax_year)\`
- [ ] Idempotent or upsert behavior documented and tested
"

child "DoD: Log numbers assigned and visible" "P1 – Workflow,S,feature" "${ref}## Acceptance
- [ ] Every imported return has a stable office log # where required by workflow
- [ ] Shown in dashboard, return list, and return detail
- [ ] Consistent with how the manual log assigns numbers
"

child "DoD: Imported data usable in real workflow" "P1 – Workflow,M,feature" "${ref}## Acceptance
- [ ] After import, key fields appear correctly on the dashboard
- [ ] Return detail and status workflow reflect imported data
- [ ] Staff can use TaxOps for the same day-to-day steps as the spreadsheet for imported clients
"

echo
echo "Done. Epic: $EPIC_URL"
echo "Child issues created under repo $REPO (see issue list; titles start with 'DoD:')."
