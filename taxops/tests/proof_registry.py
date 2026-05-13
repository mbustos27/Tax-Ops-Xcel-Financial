"""Human-readable proof lines for pytest terminal summary and text logs.

Maps each test function name to lines describing what a PASS proves (ASCII-friendly)."""

from __future__ import annotations

# Keys must match test function names in test_github_cards.py
PROOF_BY_TEST_NAME: dict[str, list[str]] = {
    "test_health_ok": [
        "GET /health returns 200 JSON with ok status when SQLite responds to SELECT 1.",
        "Response includes uptime_seconds and version strings for probes and dashboards.",
        "SQLite round-trip exposes latency_ms (non-negative) alongside db.ok=true.",
    ],
    "test_health_no_login_required": [
        "/health succeeds without logging in (distinct from authenticated HTML routes).",
    ],
    "test_health_version_from_env": [
        "TAXOPS_VERSION environment variable replaces git-derived version in /health response.",
        "Resetting the release-version cache picks up NSSM/ci injects between tests reliably.",
    ],
    "test_health_db_failure_returns_503": [
        "When SQLite open/query fails, /health responds HTTP 503 with status degraded.",
        "Error detail is surfaced only under JSON db.error (no HTML login redirect).",
    ],
    "test_retention_cleanup_deletes_old_backups": [
        "Retention removes files matching taxops_backup_*.sqlite when mtime exceeds window.",
        "Files newer than cutoff days survive so recent nightly copies are kept.",
    ],
    "test_backup_sqlite_live_copies": [
        "SQLite Connection.backup clones schema + rows without stopping the Flask service pattern.",
        "Copied DB row round-trips so backup content is coherent (not truncated zero-byte files).",
    ],
    "test_run_once_success_subprocess": [
        "scripts/nightly_backup_db.py exits 0 on success and writes UTC-stamped backups under BACKUP_DIR.",
        "Backed-up DB remains queryable identical to live source for sampled row content.",
    ],
    "test_run_once_missing_db_alerts_optional_webhook[False]": [
        "Backup run fails loudly (exit semantics) when TAXOPS_DB path is absent.",
        "Without TAXOPS_BACKUP_ALERT_WEBHOOK configured, notifier is skipped (pure exit code semantics).",
    ],
    "test_run_once_missing_db_alerts_optional_webhook[True]": [
        "Backup run fails loudly (exit semantics) when TAXOPS_DB path is absent.",
        "TAXOPS_BACKUP_ALERT_WEBHOOK triggers notifier when webhook URL configured.",
    ],
    "test_relaxed_profile_empty_errors": [
        "Relaxed TAXOPS_ENV (e.g. test) skips PROD-5 strict secret checks so CI/pytest imports app safely.",
    ],
    "test_strict_requires_secret_user_pass": [
        "Strict profile rejects missing explicit TAXOPS_SECRET / TAXOPS_USER / TAXOPS_PASS (no silent dev fallbacks).",
    ],
    "test_strict_imap_requires_mailbox_secrets": [
        "Whenever IMAP_HOST enables the mail watcher, strict mode requires mailbox user + password populated.",
    ],
    "test_skip_validation_flag": [
        "TAXOPS_SKIP_ENV_VALIDATION=1 escapes strict checks without editing code paths (bootstrap only).",
    ],
    "test_validate_and_exit_raises": [
        "validate_taxops_environment_and_exit maps validation failures into SystemExit(1) visible to NSSM/Task Scheduler logs.",
    ],
    "test_probe_health_ok": [
        "/health returning JSON status=200 with status ok passes smoke probe_health with no diagnostics.",
    ],
    "test_probe_health_requires_ok_status": [
        "probe_health rejects JSON 200 payloads where status is degraded so smoke tests surface DB regressions.",
    ],
    "test_probe_health_503_reports_payload": [
        "Smoke script maps HTTPError 503 + JSON body into readable failure lines instead of crashing urllib.",
    ],
    "test_run_checks_detects_bad_static": [
        "run_checks fetches static/app.js and fails when content lacks JS signatures (catch broken static deploy).",
    ],
    "test_client_error_anonymous_returns_401_json": [
        "Unauthenticated POST /api/client-error returns JSON 401 (same contract as other /api/* routes).",
    ],
    "test_client_error_invalid_json_logged_in": [
        "Client error endpoint rejects non-JSON bodies with 400 so scrapers cannot spam arbitrary payloads.",
    ],
    "test_client_error_logs_warning": [
        "Authenticated client error reports hit logger taxops.frontend at WARNING with kind + message for PROD-6 triage.",
    ],
    "test_dashboard_reachable_logged_in_lists_returns_header": [
        "Authenticated GET / returns HTTP 200 (not redirect to login).",
        "Dashboard HTML includes TaxOps branding so staff see the correct app shell.",
        "Page includes the returns table (.data-table) and/or quick-filter controls (core workflow UI).",
    ],
    "test_return_missing_id_returns_not_found_when_logged_in": [
        "Authenticated GET /return/<id> for a non-existent id returns 404 (route works with login).",
        "Does not leak the detail page for bogus IDs.",
    ],
    "test_anonymous_dashboard_redirects_to_login": [
        "Unauthenticated GET / responds with 302 redirect (guests cannot open the dashboard).",
        "Redirect targets the login page so session gate matches production behavior.",
    ],
    "test_anonymous_return_detail_redirects_to_login": [
        "Unauthenticated GET /return/<id> responds with 302 to login (return detail is behind auth).",
    ],
    "test_save_attachments_writes_hash_and_counts": [
        "Email attachment save persists SHA-256 hex in return_documents.file_hash for ingest rows.",
        "Inserted row filenames and byte counts match MIME payload.",
    ],
    "test_duplicate_two_identical_parts_skipped": [
        "Two MIME parts identical under same sanitized name/size hash only persist one DB row.",
        "Duplicate skips log INFO with hash prefix only—not full fingerprint.",
    ],
    "test_strong_dedupe_different_filenames": [
        "Duplicate payload is skipped by file_hash match even when MIME filename differs from stored row.",
    ],
    "test_cheap_dedupe_legacy_row_without_hash": [
        "Legacy rows with NULL file_hash still dedupe via filename + byte size.",
    ],
    "test_worker_tags_unknown_w2_after_save": [
        "Extractor happy path: synthetic W-2 fields + high confidence persist w2_records and set doc_type W-2.",
        "Queue row ends in status completed when form save and tagging succeed.",
    ],
    "test_worker_does_not_overwrite_staff_doc_type": [
        "After _save_form_data, doc_type is only updated when still unknown/empty (staff 1099 unchanged).",
    ],
    "test_manual_extract_route_tags_unknown": [
        "POST /ai/documents/<id>/extract tags unknown documents W-2 after save (same guard as worker).",
    ],
    "test_classify_shim_respects_only_unknown": [
        "_classify_document_using_row(only_if_still_unknown=True) returns inferred type but does not overwrite non-unknown doc_type.",
    ],
    "test_verify_w2_harness_passes": [
        "verify_w2 canonical fixture: INSERT columns listed in extractor allow-list & W-2 prompt, PRAGMA has columns, DB row matches sentinel values.",
    ],
    "test_legacy_mirror_w2_records": [
        "w2_records: DDL mirrors (wages_tips_other etc.) stay NULL; box1/box2 and tax_year saved via canonical keys only.",
        "form_schema import guard: FORM_TABLE_INSERT_COLUMNS disjoint from FORM_LEGACY_MIRROR_COLUMNS.",
    ],
    "test_legacy_mirror_f1099_nec": [
        "f1099_nec_records: legacy nonemployee_compensation / federal mirror columns untouched on insert.",
    ],
    "test_legacy_mirror_f1099_misc": [
        "f1099_misc_records: legacy rents/royalties/other_income mirrors untouched on insert.",
    ],
    "test_legacy_mirror_f1099_int": [
        "f1099_int_records: legacy interest_income mirrors untouched on insert.",
    ],
    "test_legacy_mirror_f1099_div": [
        "f1099_div_records: legacy dividends mirrors untouched on insert.",
    ],
    "test_parse_years_param_accepts_three": [
        "parse_years_param normalizes unordered CSV into sorted unique ascending year list.",
    ],
    "test_parse_years_param_rejects_one": [
        "Fewer than two distinct tax years returns a descriptive error instead of acceptance.",
    ],
    "test_compare_json_two_years": [
        "GET /api/clients/:id/years?years=y1,y2 returns column data per tax_year with refund_amount from payments.",
        "YoY highlight flags mark large refund and IRS balance deltas on the newer year column.",
    ],
    "test_compare_pdf_returns_bytes": [
        "render_year_comparison_pdf produces a valid PDF (%PDF-) from a minimal normalized payload stub.",
    ],
    "test_rollover_preview_counts": [
        "Season rollover preview counts eligible clients (new target year) separately from skipped duplicates.",
    ],
    "test_rollover_commit_idempotent": [
        "rollover_commit seeds PENDING INTAKE returns with optional prior log carry; a second run skips without creating duplicates.",
    ],
    "test_rollover_preview_api_forbidden": [
        "POST /api/admin/season-rollover/preview returns 403 when the operator is not allowed to run season rollover.",
    ],
}
