"""Human-readable proof lines for pytest terminal summary and text logs.

Maps each test function name to lines describing what a PASS proves (ASCII-friendly)."""

from __future__ import annotations

# Keys must match test function names in test_github_cards.py
PROOF_BY_TEST_NAME: dict[str, list[str]] = {
    "test_max_content_length_is_configured": [
        "MAX_CONTENT_LENGTH is set to a non-trivial value — raw uploads are capped.",
    ],
    "test_max_content_length_default_is_50mb": [
        "Default upload limit is exactly 50 MB.",
    ],
    "test_413_handler_returns_json": [
        "Oversized upload returns HTTP 413 with a JSON body containing an 'error' key.",
    ],
    "test_sec6_indexes_exist_after_init": [
        "All 15 SEC-6 hot-path indexes are present after init_db runs on a fresh database.",
    ],
    "test_return_documents_return_id_uses_index": [
        "EXPLAIN QUERY PLAN shows SEARCH USING INDEX for return_documents(return_id).",
    ],
    "test_payments_return_id_uses_index": [
        "EXPLAIN shows index scan for payments(return_id).",
    ],
    "test_notes_return_id_uses_index": [
        "EXPLAIN shows index scan for notes(return_id).",
    ],
    "test_missing_docs_return_id_uses_index": [
        "EXPLAIN shows index scan for missing_docs(return_id, is_resolved).",
    ],
    "test_extraction_queue_status_uses_index": [
        "EXPLAIN shows index scan for extraction_queue(status, created_at).",
    ],
    "test_returns_status_year_uses_index": [
        "EXPLAIN shows index scan for returns(client_status, tax_year) — dashboard filter.",
    ],
    "test_returns_updated_at_uses_index": [
        "EXPLAIN shows index scan for returns ORDER BY updated_at.",
    ],
    "test_login_max_attempts_default": [
        "_LOGIN_MAX_ATTEMPTS defaults to 5.",
    ],
    "test_login_lockout_minutes_default": [
        "_LOGIN_LOCKOUT_MINUTES defaults to 15.",
    ],
    "test_failed_attempts_increments_on_bad_password": [
        "failed_attempts counter increments on each wrong-password attempt.",
    ],
    "test_failed_attempts_resets_on_success": [
        "failed_attempts resets to 0 on a successful login.",
    ],
    "test_account_locks_after_max_attempts": [
        "Account returns _AUTH_LOCKED after _LOGIN_MAX_ATTEMPTS consecutive failures.",
    ],
    "test_locked_account_rejects_correct_password": [
        "A locked account returns _AUTH_LOCKED even when the correct password is supplied.",
    ],
    "test_expired_lockout_allows_login": [
        "A lockout whose locked_until is in the past allows a successful login.",
    ],
    "test_login_route_returns_same_error_for_locked_and_bad_credentials": [
        "Locked account and wrong password both return HTTP 200 with the same error string — no info leak.",
    ],
    "test_sixth_rapid_failure_triggers_lockout": [
        "The 6th consecutive failure returns _AUTH_LOCKED when max_attempts=5 (issue requirement).",
    ],
    "test_journal_mode_is_wal": [
        "PRAGMA journal_mode=WAL is set by get_connection() — concurrent readers never block each other.",
    ],
    "test_synchronous_is_normal": [
        "PRAGMA synchronous=NORMAL (1) gives WAL durability without full-sync overhead.",
    ],
    "test_busy_timeout_is_set": [
        "PRAGMA busy_timeout >= 10000 ms — locked-DB OperationalError is not raised immediately.",
    ],
    "test_temp_store_is_memory": [
        "PRAGMA temp_store=MEMORY (2) keeps temp tables in RAM rather than temp files.",
    ],
    "test_foreign_keys_still_on": [
        "foreign_keys enforcement is still ON after the SEC-4 PRAGMAs are applied.",
    ],
    "test_concurrent_readers_do_not_block_each_other": [
        "Four concurrent readers all finish under 2 s — WAL snapshot isolation in effect.",
    ],
    "test_busy_timeout_allows_writer_to_finish": [
        "Second writer waits on busy_timeout rather than raising OperationalError immediately.",
    ],
    "test_session_cookie_httponly_is_true": [
        "SESSION_COOKIE_HTTPONLY=True prevents JavaScript access to the session cookie.",
    ],
    "test_session_cookie_samesite_is_lax": [
        "SESSION_COOKIE_SAMESITE=Lax blocks session cookie from being sent on cross-site POSTs.",
    ],
    "test_permanent_session_lifetime_is_12_hours": [
        "PERMANENT_SESSION_LIFETIME is 12 hours — sessions expire automatically after inactivity.",
    ],
    "test_templates_auto_reload_matches_debug": [
        "TEMPLATES_AUTO_RELOAD is False in non-debug mode — no unnecessary disk I/O in production.",
    ],
    "test_security_headers_present_on_health": [
        "X-Frame-Options, X-Content-Type-Options, Referrer-Policy, Cache-Control are set on all responses.",
    ],
    "test_csp_header_present_on_health": [
        "Content-Security-Policy with default-src self, object-src none, base-uri self, form-action self, frame-ancestors none.",
    ],
    "test_csp_header_present_on_login_page": [
        "CSP is enforced on the unauthenticated login page as well as authenticated routes.",
    ],
    "test_session_is_permanent_after_login": [
        "session.permanent=True is set on login so the cookie carries Max-Age/Expires for the 12-h window.",
    ],
    "test_auth_users_table_exists_after_init": [
        "auth_users table is created by init_db/migrate with all SEC-2 required columns.",
    ],
    "test_bootstrap_seeds_user_from_env": [
        "bootstrap_auth_user inserts one admin row from TAXOPS_USER/TAXOPS_PASS when table is empty.",
    ],
    "test_bootstrap_is_noop_when_users_exist": [
        "bootstrap_auth_user is a no-op if at least one user row exists — safe to call every startup.",
    ],
    "test_authenticate_correct_password": [
        "check_password_hash path returns a user dict with correct username on valid credentials.",
    ],
    "test_authenticate_wrong_password_returns_none": [
        "Wrong password returns None and increments failed_attempts counter in auth_users.",
    ],
    "test_authenticate_inactive_user_returns_none": [
        "is_active=0 rows are rejected even with a correct password.",
    ],
    "test_authenticate_unknown_user_returns_none": [
        "Unknown usernames return None without raising an exception.",
    ],
    "test_authenticate_updates_last_login_at": [
        "Successful authentication writes a non-null last_login_at timestamp to auth_users.",
    ],
    "test_login_route_accepts_hashed_user": [
        "POST /login with hashed credentials in auth_users redirects (HTTP 301/302).",
    ],
    "test_login_route_rejects_wrong_password": [
        "POST /login with wrong password re-renders the login page with error message (HTTP 200).",
    ],
    "test_plaintext_compare_never_fires_when_users_exist": [
        "Env-var plaintext fallback in _authenticate_user is blocked when auth_users has rows.",
        "An attacker who knows TAXOPS_USER/TAXOPS_PASS cannot bypass auth_users after bootstrap.",
    ],
    "test_post_without_csrf_token_is_rejected": [
        "POST to /api/return/<id>/status without X-CSRFToken returns HTTP 400 (CSRF blocked).",
        "Flask-WTF CSRFProtect is active on the main app and all registered blueprints.",
    ],
    "test_post_with_valid_csrf_token_passes_middleware": [
        "POST with a valid X-CSRFToken header is not rejected by CSRF middleware.",
        "Token generated via generate_csrf() satisfies Flask-WTF validation.",
    ],
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
        "run_checks parses cache-busting ``v`` from login HTML then fetches /static/app.js?v=… "
        "and fails when JS lacks expected signatures.",
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
    "test_run_checks_requires_versioned_css_hrefs_on_login": [
        "run_checks fails when /login omits ``?v=`` on bundled CSS URLs so stale UI shells are caught automatically.",
    ],
    "test_taxops_asset_cache_version_is_non_empty_string": [
        "taxops_asset_cache_version() yields a usable non-empty ``?v=`` token for static filenames.",
    ],
    "test_waitress_package_available": [
        "The waitress WSGI dependency is installed so production `python app.py` can serve outside FLASK_DEBUG mode.",
    ],
}
