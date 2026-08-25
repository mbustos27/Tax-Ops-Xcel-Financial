"""Minimal README for the findings-only client audit."""

# TaxOps Client Audit (findings-only)

Never writes to TaxOps. Creates `audit/audit_YYYYMMDD.sqlite` and a gitignored workbook under `audit/output/`.

## Run

From repo root `T:\` (needs openpyxl + rapidfuzz):

```powershell
$py = "\\Xcel-server\taxops\punchbridge\.venv\Scripts\python.exe"
& $py -m audit `
  --operator "yourname" `
  --backup-verified `
  --csm-unfiltered `
  --drake "C:\Users\Windows 10\Desktop\CLIENTS.xlsx" `
  --tax-log "C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\Shared\Logs\TAX LOG 2025 Live.xlsx"
```

M0 refuses without `--backup-verified` and `--csm-unfiltered`.
If source counts differ from the locked baselines, M0 halts unless you also pass `--acknowledge-baseline-drift`.

Normalizer-only check:

```powershell
& $py -m audit --operator "x" --m1-only --drake "..." --tax-log "..."
```

## Notes

- Do not modify `taxops/name_matcher.py`.
- No Anthropic calls.
- `proposed_migration.sql` is text-only — do not execute it.
