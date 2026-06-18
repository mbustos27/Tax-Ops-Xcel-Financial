# Creates labels (idempotent-ish), epic issues, and child issues for TaxOps delivery roadmap.
# Usage: pwsh scripts/gh_create_delivery_epics.ps1
# Repo: mbustos27/Tax-Ops-Xcel-Financial

$ErrorActionPreference = "Stop"
$Repo = "mbustos27/Tax-Ops-Xcel-Financial"

function Ensure-Label {
    param([string]$Name, [string]$Color, [string]$Description)
    gh label create $Name --color $Color --description $Description --repo $Repo 2>$null | Out-Null
    # ignore exit code when label exists
}

$labels = @(
    @{ Name = "epic";        Color = "7F77DD"; Description = "Top-level epic issue" },
    @{ Name = "bulk";        Color = "378ADD"; Description = "Bulk status update" },
    @{ Name = "filters";    Color = "1D9E75"; Description = "Saved dashboard filters" },
    @{ Name = "profile";     Color = "EF9F27"; Description = "Client profile" },
    @{ Name = "multi-year";  Color = "D85A30"; Description = "Multi-year view" },
    @{ Name = "rollover";    Color = "639922"; Description = "Season rollover" },
    @{ Name = "production"; Color = "888780"; Description = "Production hardening" },
    @{ Name = "audit";      Color = "4B1528"; Description = "Audit log" }
)
foreach ($l in $labels) {
    Ensure-Label -Name $l.Name -Color $l.Color -Description $l.Description
}

function New-GhIssue {
    param(
        [string]$Title,
        [string[]]$Labels,
        [string]$Body
    )
    $ghArgs = @("issue", "create", "--repo", $Repo, "--title", $Title, "--body", $Body)
    foreach ($lb in $Labels) {
        $ghArgs += "--label"
        $ghArgs += $lb
    }
    $url = & gh @ghArgs
    if ($url -match "issues/(\d+)") {
        return $Matches[1]
    }
    if ($LASTEXITCODE -ne 0) {
        throw "gh issue create failed: $Title"
    }
    throw "Could not parse issue number from: $url"
}

Write-Host "--- Epics ---"
$eProd = New-GhIssue -Title "Epic: Production Hardening — Office Deployment" `
    -Labels @("epic", "production") `
    -Body @"
Goal: Make the app stable, observable, and recoverable for daily office use across multiple workstations on the local network.

Child issues: PROD-1 through PROD-8
"@
Write-Host "PROD epic #$eProd"

$eAudit = New-GhIssue -Title "Epic: Multi-User Audit Log" `
    -Labels @("epic", "audit") `
    -Body @"
Goal: Record every data-modifying action by staff with who, what changed, and when — queryable by admin for accountability and error recovery.

Child issues: AUDIT-1 through AUDIT-8
Dependency: Epic Production Hardening (#$eProd) should be underway or deployed first.
"@
Write-Host "AUDIT epic #$eAudit"

$eBulk = New-GhIssue -Title "Epic: Bulk Status Update" `
    -Labels @("epic", "bulk") `
    -Body @"
Goal: Allow staff to select multiple returns and apply a status change in one action.

Child issues: BULK-1 through BULK-6
Depends on audit trail: Epic #$eAudit
"@
Write-Host "BULK epic #$eBulk"

$eFilters = New-GhIssue -Title "Epic: Saved Dashboard Filters" `
    -Labels @("epic", "filters") `
    -Body @"
Goal: Let staff save named filter presets so they can switch views without re-entering criteria each session.

Child issues: FILTER-1 through FILTER-6
Dashboard surface aligns with Bulk (#$eBulk).
"@
Write-Host "FILTER epic #$eFilters"

$eProfile = New-GhIssue -Title "Epic: Client Profile Page — Unified History" `
    -Labels @("epic", "profile") `
    -Body @"
Goal: Replace per-return view with a client-level profile showing all returns, documents, notes, and status history in one place.

Child issues: PROFILE-1 through PROFILE-7
Stable data / audit expectations: #$eAudit
"@
Write-Host "PROFILE epic #$eProfile"

$eMulti = New-GhIssue -Title "Epic: Multi-Year Client View" `
    -Labels @("epic", "multi-year") `
    -Body @"
Goal: Let staff see a client's return data side-by-side across multiple tax years.

Child issues: MULTIYEAR-1 through MULTIYEAR-6
Dependency: Client Profile epic (#$eProfile) must be complete.
"@
Write-Host "MULTIYEAR epic #$eMulti"

$eRoll = New-GhIssue -Title "Epic: Next Season Rollover" `
    -Labels @("epic", "rollover") `
    -Body @"
Goal: Carry forward client records, preparer assignments, and prior-year metadata to seed a new tax year without manual re-entry.

Child issues: ROLLOVER-1 through ROLLOVER-6
Dependency: Client Profile (#$eProfile) and Multi-Year (#$eMulti) should land first.
"@
Write-Host "ROLLOVER epic #$eRoll"

function New-Child {
    param([string]$Title, [string]$Label, [string]$EpicNum, [string]$EpicSlug)
    $body = "Part of #$EpicNum (Epic: $EpicSlug)"
    New-GhIssue -Title $Title -Labels @($Label) -Body $body | Out-Null
}

Write-Host "--- PROD children ---"
@(
    "PROD-1: Process manager — systemd or NSSM, auto-restart on crash, start on boot",
    "PROD-2: Structured logging — JSON output to rotating file, configurable log level via env var",
    "PROD-3: Health check endpoint GET /health — status, db, uptime, version",
    "PROD-4: Nightly database backup script, 30-day retention, alert on failure",
    "PROD-5: Env config — all secrets in .env, env validation on startup with clear errors",
    "PROD-6: Frontend error boundary — friendly message, log to server, no full crash",
    "PROD-7: Local network access — document LAN IP setup, firewall rules, staff onboarding",
    "PROD-8: Smoke test script — hits key endpoints post-deploy, exits non-zero on failure"
) | ForEach-Object { New-Child -Title $_ -Label "production" -EpicNum $eProd -EpicSlug "Production Hardening" }

Write-Host "--- AUDIT children ---"
@(
    "AUDIT-1: Schema — audit_log table (id, user_id, action, entity_type, entity_id, before_json, after_json, ip_address, created_at)",
    "AUDIT-2: Audit middleware — wraps all POST/PATCH/DELETE, captures before/after, writes to audit_log, non-blocking",
    "AUDIT-3: Audit log admin UI — filterable by user, action type, entity, date range",
    "AUDIT-4: Audit detail view — full before/after JSON diff per entry",
    "AUDIT-5: Sensitive field masking — SSN and financial fields redacted in before/after JSON",
    "AUDIT-6: Audit log CSV export for filtered results",
    "AUDIT-7: Retention policy — configurable auto-purge, default 7 years, admin UI to set",
    "AUDIT-8: Read-only enforcement — audit_log rows never updated or deleted by app code"
) | ForEach-Object { New-Child -Title $_ -Label "audit" -EpicNum $eAudit -EpicSlug "Multi-User Audit Log" }

Write-Host "--- BULK children ---"
@(
    "BULK-1: Add multi-select checkboxes to returns dashboard table",
    "BULK-2: Bulk action toolbar — appears when ≥1 row selected, actions: change status, assign preparer",
    "BULK-3: API endpoint POST /returns/bulk-status with transaction rollback on partial failure",
    "BULK-4: Audit log entry per return changed (who, from_status, to_status, timestamp)",
    "BULK-5: Confirmation modal showing count and target status before commit",
    "BULK-6: Toast feedback on success/partial failure with per-return error details"
) | ForEach-Object { New-Child -Title $_ -Label "bulk" -EpicNum $eBulk -EpicSlug "Bulk Status Update" }

Write-Host "--- FILTER children ---"
@(
    "FILTER-1: Schema — filters table (id, user_id, name, filter_json, created_at)",
    "FILTER-2: API — GET/POST/DELETE /filters scoped to authenticated user",
    "FILTER-3: UI — Save current filter button in filter bar, prompts for name",
    "FILTER-4: UI — saved filter pill list below filter bar, click to apply, x to delete",
    "FILTER-5: Default filter support — user marks one preset as default, loads on dashboard open",
    "FILTER-6: Shared filters — admin can publish a filter visible to all users"
) | ForEach-Object { New-Child -Title $_ -Label "filters" -EpicNum $eFilters -EpicSlug "Saved Dashboard Filters" }

Write-Host "--- PROFILE children ---"
@(
    "PROFILE-1: Client profile route /clients/:id — shell page with header (name, contact, ID)",
    "PROFILE-2: Returns timeline component — all returns sorted by tax year with status badges",
    "PROFILE-3: Documents panel — all documents across all returns, filterable by year and doc type",
    "PROFILE-4: Notes/activity feed — chronological log of status changes, assignments, notes",
    "PROFILE-5: Edit client info inline — name, address, contact, filing status",
    "PROFILE-6: Link from every return detail page back to client profile",
    "PROFILE-7: Search results link directly to client profile, not individual return"
) | ForEach-Object { New-Child -Title $_ -Label "profile" -EpicNum $eProfile -EpicSlug "Client Profile Page" }

Write-Host "--- MULTIYEAR children ---"
@(
    "MULTIYEAR-1: Multi-year comparison UI — select 2–3 years, render columns side by side",
    "MULTIYEAR-2: Key field comparison rows — AGI, filing status, refund/balance due, preparer",
    "MULTIYEAR-3: Highlight year-over-year changes exceeding configurable thresholds",
    "MULTIYEAR-4: Document availability indicator per year — received vs missing",
    "MULTIYEAR-5: Export comparison as PDF for preparer notes or client summary",
    "MULTIYEAR-6: API GET /clients/:id/years?years=2022,2023,2024 returning normalized comparison payload"
) | ForEach-Object { New-Child -Title $_ -Label "multi-year" -EpicNum $eMulti -EpicSlug "Multi-Year Client View" }

Write-Host "--- ROLLOVER children ---"
@(
    "ROLLOVER-1: Admin UI — Start new season action, target year input, confirmation gate",
    "ROLLOVER-2: Rollover job — create new return per active client, status=pending_intake, copy preparer from prior year",
    "ROLLOVER-3: Carry-forward fields config — admin selects which fields roll over vs reset",
    "ROLLOVER-4: Rollover preview — count of clients to roll, list of exceptions before commit",
    "ROLLOVER-5: Idempotency — re-running rollover for same year is a no-op, not a duplicate",
    "ROLLOVER-6: Post-rollover report — CSV of created returns, skipped clients, and reasons"
) | ForEach-Object { New-Child -Title $_ -Label "rollover" -EpicNum $eRoll -EpicSlug "Next Season Rollover" }

Write-Host ""
Write-Host "Done. Epic issue numbers:"
Write-Host "  Production:  #$eProd"
Write-Host "  Audit:       #$eAudit"
Write-Host "  Bulk:        #$eBulk"
Write-Host "  Filters:     #$eFilters"
Write-Host "  Profile:     #$eProfile"
Write-Host "  Multi-year:  #$eMulti"
Write-Host "  Rollover:    #$eRoll"
