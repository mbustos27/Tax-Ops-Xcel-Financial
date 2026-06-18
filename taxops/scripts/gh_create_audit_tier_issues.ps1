# Creates labels + Tier 1/2/3 GitHub issues using body files in _gh_issues_bodies/audit_tiers_2026/
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$Bodies = Join-Path $RepoRoot "_gh_issues_bodies\audit_tiers_2026"
$urls = New-Object System.Collections.Generic.List[string]

function Add-Issue([string]$Title, [string]$Labels, [string]$BodyRel) {
    $path = Join-Path $Bodies $BodyRel
    if (-not (Test-Path $path)) { throw "Missing body file: $path" }
    $url = gh issue create --title $Title --label $Labels -F $path
    Write-Host $url
    $script:urls.Add($url)
}

# Labels (suppress stderr if duplicate)
$labels = @(
    @{ n = "security"; c = "d73a4a"; d = "Security fixes" },
    @{ n = "reliability"; c = "e4e669"; d = "Reliability and data integrity" },
    @{ n = "performance"; c = "0e8a16"; d = "Performance improvements" },
    @{ n = "tech-debt"; c = "cfd3d7"; d = "Technical debt and refactoring" },
    @{ n = "testing"; c = "7057ff"; d = "Test coverage" },
    @{ n = "critical"; c = "b60205"; d = "Must fix before production use" },
    @{ n = "epic"; c = "0075ca"; d = "Parent epic issue" }
)
foreach ($l in $labels) {
    gh label create $l.n --color $l.c --description $l.d --force 2>$null | Out-Null
}

# Tier issues (order preserves narrative)
Add-Issue "Epic: Security hardening - critical fixes before multi-user production use" "epic,security,critical" "epic-tier1-security.md"
Add-Issue "SEC-1: Add CSRF protection to all state-changing endpoints" "security,critical" "sec-1-csrf.md"
Add-Issue "SEC-2: Per-user accounts with hashed passwords - replace shared credential" "security,critical" "sec-2-auth-users.md"
Add-Issue "SEC-3: Session cookie security flags and lifetime" "security,critical" "sec-3-session-cookies.md"
Add-Issue "SEC-4: SQLite WAL mode and busy_timeout in db.get_connection()" "security,reliability,critical" "sec-4-sqlite-wal.md"
Add-Issue "SEC-5: MAX_CONTENT_LENGTH and upload validation" "security,critical" "sec-5-upload-limits.md"
Add-Issue "SEC-6: Add missing database indexes for hot query paths" "security,performance,critical" "sec-6-indexes.md"
Add-Issue "SEC-7: Login rate limiting and account lockout" "security,critical" "sec-7-login-rate-limit.md"

Add-Issue "Epic: Reliability hardening - data integrity and production stability" "epic,reliability" "epic-tier2-reliability.md"
Add-Issue "REL-1: Move LLM calls out of request handlers - async or 202+poll pattern" "reliability" "rel-1-llm-async.md"
Add-Issue "REL-2: Replace per-request audit thread with single long-lived writer queue" "reliability" "rel-2-audit-writer-queue.md"
Add-Issue "REL-3: Worker startup independent of WSGI entry point" "reliability" "rel-3-worker-startup.md"
Add-Issue "REL-4: Standardize DB connection lifecycle - context manager pattern" "reliability" "rel-4-db-context.md"
Add-Issue "REL-5: Extraction worker event signaling - eliminate 60-second poll delay" "reliability,performance" "rel-5-extraction-event.md"
Add-Issue "REL-6: api_field type validation per column" "reliability" "rel-6-api-field-validate.md"

Add-Issue "Epic: Technical debt - refactoring, test coverage, and performance" "epic,tech-debt" "epic-tier3-tech-debt.md"
Add-Issue "DEBT-1: Refactor app.py into blueprints - split 5800-line god module" "tech-debt" "debt-1-blueprints.md"
Add-Issue "DEBT-2: Test coverage - auth, upload, CSRF, field validation" "tech-debt,testing" "debt-2-tests-gap.md"
Add-Issue "DEBT-3: name_matcher.py unit tests - fuzzy match thresholds and edge cases" "tech-debt,testing" "debt-3-name-matcher.md"
Add-Issue "DEBT-4: Dashboard query optimization - batch fetches eliminate N+1 pattern" "tech-debt,performance" "debt-4-dashboard-batch.md"
Add-Issue "DEBT-5: Cache-Control fix - long cache for versioned static assets" "tech-debt,performance" "debt-5-static-cache.md"
Add-Issue "DEBT-6: Schema version tracking for migrations" "tech-debt" "debt-6-schema-version.md"
Add-Issue "DEBT-7: _processed_uids LRU cap in mail_watcher - prevent unbounded memory growth" "tech-debt" "debt-7-mail-uids-cap.md"
Add-Issue "DEBT-8: Extraction confidence normalization and accuracy improvements" "tech-debt" "debt-8-extraction-confidence.md"

Write-Host ""
Write-Host "--- Created issue URLs ---"
$urls | ForEach-Object { Write-Host $_ }

$outLog = Join-Path $RepoRoot "_gh_issues_bodies\audit_tiers_2026\_created_issue_urls.txt"
$urls | Set-Content -Path $outLog -Encoding UTF8
Write-Host ""
Write-Host "Wrote:"
Write-Host $outLog
