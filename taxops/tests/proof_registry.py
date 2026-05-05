"""Human-readable proof lines for pytest terminal summary and text logs.

Maps each test function name to lines describing what a PASS proves (ASCII-friendly)."""

from __future__ import annotations

# Keys must match test function names in test_github_cards.py
PROOF_BY_TEST_NAME: dict[str, list[str]] = {
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
}
