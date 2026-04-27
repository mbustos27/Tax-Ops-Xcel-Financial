#!/usr/bin/env bash
# Bootstrap GitHub labels and issues for the TaxOps dev board.
# Requires: GitHub CLI (gh) authenticated: gh auth login
# Run: Git Bash or WSL — not cmd/PowerShell (heredocs).
#
#   chmod +x scripts/gh-taxops-board-bootstrap.sh
#   REPO=owner/repo ./scripts/gh-taxops-board-bootstrap.sh
#
# Or from repo root with defaults:
#   ./scripts/gh-taxops-board-bootstrap.sh

set -euo pipefail

# ---------------------------
# CONFIG
# ---------------------------
# Default: this repo (override: REPO=other/other ./script.sh)
REPO="${REPO:-mbustos27/Tax-Ops-Xcel-Financial}"
PROJECT_NAME="${PROJECT_NAME:-TaxOps Dev Board}"
readonly REPO
readonly PROJECT_NAME

echo "Using repo: $REPO  (project name: $PROJECT_NAME — informational only)"
echo

# ---------------------------
# CREATE LABELS
# ---------------------------
gh label create "P1 – Workflow" --color FF0000 --repo "$REPO" 2>/dev/null || true
gh label create "P2 – Ops" --color FF9900 --repo "$REPO" 2>/dev/null || true
gh label create "P3 – Feature" --color 0066FF --repo "$REPO" 2>/dev/null || true
gh label create "P4 – Future" --color 999999 --repo "$REPO" 2>/dev/null || true

gh label create "S" --color 0E8A16 --repo "$REPO" 2>/dev/null || true
gh label create "M" --color FBCA04 --repo "$REPO" 2>/dev/null || true
gh label create "L" --color D93F0B --repo "$REPO" 2>/dev/null || true

gh label create "feature" --color 1D76DB --repo "$REPO" 2>/dev/null || true
gh label create "bug" --color D73A4A --repo "$REPO" 2>/dev/null || true
gh label create "improvement" --color A2EEEF --repo "$REPO" 2>/dev/null || true

# ---------------------------
# CREATE EPIC ISSUE
# ---------------------------
EPIC_URL=$(gh issue create --repo "$REPO" \
  --title "EPIC: Initial Data Import & Client Matching System" \
  --body "Tracks all work related to importing data and resolving client identity across sources.

**Board:** $PROJECT_NAME")

echo "Epic created: $EPIC_URL"
echo

# ---------------------------
# CREATE ISSUES
# ---------------------------

gh issue create --repo "$REPO" \
  --title "Drake CSV Import" \
  --label "P1 – Workflow,S,feature" \
  --body "$(cat <<'EOF'
## Problem
We cannot import Drake data into the system.

## Goal
Successfully import Drake CSV into returns table.

## Scope (Minimum version)
- Parse drake_2025.csv
- Map fields to returns table
- Create return records

## Definition of Done
- [ ] CSV loads without runtime errors
- [ ] All rows are parsed
- [ ] Returns inserted into DB
- [ ] Visible in dashboard
- [ ] Import batch recorded
EOF
)"

gh issue create --repo "$REPO" \
  --title "Manual Excel Log Import" \
  --label "P1 – Workflow,S,feature" \
  --body "$(cat <<'EOF'
## Problem
Manual log data is not in the system.

## Goal
Import Excel log into TaxOps.

## Scope
- Parse TAX LOG 2025 Live.csv
- Map fields correctly
- Insert records

## Definition of Done
- [ ] File loads without errors
- [ ] Records inserted correctly
- [ ] Visible in UI
- [ ] Import batch recorded
EOF
)"

gh issue create --repo "$REPO" \
  --title "Fuzzy Matching Integration" \
  --label "P1 – Workflow,M,feature" \
  --body "$(cat <<'EOF'
## Goal
Link imported records to existing clients.

## Definition of Done
- [ ] Confidence threshold defined
- [ ] High-confidence matches auto-link
- [ ] No duplicate clients created
EOF
)"

gh issue create --repo "$REPO" \
  --title "Review Queue Wiring" \
  --label "P2 – Ops,M,feature" \
  --body "$(cat <<'EOF'
## Goal
Handle low-confidence matches.

## Definition of Done
- [ ] Low-confidence matches sent to review_queue
- [ ] Visible in UI
- [ ] Can resolve manually
EOF
)"

gh issue create --repo "$REPO" \
  --title "Duplicate Return Prevention" \
  --label "P1 – Workflow,M,feature" \
  --body "$(cat <<'EOF'
## Goal
Prevent duplicate returns.

## Definition of Done
- [ ] Check (client_id + tax_year)
- [ ] No duplicates inserted
- [ ] Re-import safe
EOF
)"

gh issue create --repo "$REPO" \
  --title "Log Number Assignment" \
  --label "P1 – Workflow,S,feature" \
  --body "$(cat <<'EOF'
## Goal
Assign log numbers to returns.

## Definition of Done
- [ ] Unique log numbers generated
- [ ] Stored in DB
- [ ] Visible in UI
EOF
)"

echo
echo "Done. Epic: $EPIC_URL"
