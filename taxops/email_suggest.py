"""
email_suggest.py
~~~~~~~~~~~~~~~~~
Phase 3.1 — non-binding client-match suggestions for the email-inbox holding
area. Suggestions only: the staff member always makes the final call via the
existing ``/api/email-inbox/<id>/assign`` endpoint. There is no unconfirmed
state introduced by this module.

Computed on demand by ``/api/email-inbox/items`` and cached on the
``email_inbox`` row (``suggested_return_id``/``suggestion_method``/
``suggestion_score``). Never called from the IMAP poll cycle — mail_watcher.py
stays match-free and keeps its <=1-rules-SELECT-per-cycle invariant.

Matching chain (first hit wins; a wrong suggestion is worse than none):
  1. exact_email — sender_email matches exactly one client's
     taxpayer_email/spouse_email (case-insensitive).
  2. domain_hint — sender_domain is in config.CLIENT_HINT_DOMAINS and exactly
     one client has a taxpayer_email/spouse_email on that domain. Multiple
     clients sharing the domain -> no suggestion (ambiguous).
  3. fuzzy — sender display name against client names via the existing
     name_matcher.find_client(), gated at FUZZY_SUGGEST_THRESHOLD (90, the
     established TaxOps auto-accept bar). Below threshold -> no suggestion.

Reuses name_matcher.py's normalization/scoring utilities (parse_name,
find_client) verbatim — no new fuzzy-matching or normalization code here.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from config import CLIENT_HINT_DOMAINS
from name_matcher import parse_name, find_client

# TaxOps' established auto-accept bar (see name_matcher.ACCEPT_THRESHOLD=88 for
# CSV-import matching). Suggestions use a stricter 90: a wrong suggestion in a
# staff-facing queue is worse than showing nothing.
FUZZY_SUGGEST_THRESHOLD = 90

SuggestResult = dict  # keys: return_id, method, score (score may be None)


def _most_recent_return_for_client(conn: sqlite3.Connection, client_id: int) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM returns WHERE client_id=? ORDER BY tax_year DESC, id DESC LIMIT 1",
        (client_id,),
    ).fetchone()
    return row["id"] if row else None


def _exact_email_match(conn: sqlite3.Connection, sender_email: str) -> Optional[int]:
    """Return the single client_id whose taxpayer/spouse email matches, or None
    if there is no match or the match is ambiguous across multiple clients."""
    if not sender_email:
        return None
    rows = conn.execute(
        "SELECT DISTINCT id FROM clients "
        "WHERE lower(taxpayer_email)=lower(?) OR lower(spouse_email)=lower(?)",
        (sender_email, sender_email),
    ).fetchall()
    if len(rows) != 1:
        return None
    return rows[0]["id"]


def _domain_hint_match(conn: sqlite3.Connection, sender_domain: str) -> Optional[int]:
    """Return the single client_id with an email on sender_domain, if
    sender_domain is a configured client-hint domain and exactly one client
    matches. Multiple clients sharing the domain -> ambiguous -> None."""
    if not sender_domain or sender_domain.lower() not in CLIENT_HINT_DOMAINS:
        return None
    like_pat = "%@" + sender_domain.lower()
    rows = conn.execute(
        "SELECT DISTINCT id FROM clients "
        "WHERE lower(taxpayer_email) LIKE ? OR lower(spouse_email) LIKE ?",
        (like_pat, like_pat),
    ).fetchall()
    if len(rows) != 1:
        return None
    return rows[0]["id"]


def _fuzzy_name_match(conn: sqlite3.Connection, sender_name: str) -> Optional[tuple[int, int]]:
    """Return (client_id, score) for a fuzzy display-name match at or above
    FUZZY_SUGGEST_THRESHOLD, or None. Reuses name_matcher's normalization and
    scoring — sender_name is passed through parse_name()/find_client() exactly
    like an imported CSV name string."""
    if not sender_name or not sender_name.strip():
        return None
    last, first = parse_name(sender_name)
    if not last:
        return None
    result = find_client(conn, last, first)
    if result is None or result["score"] < FUZZY_SUGGEST_THRESHOLD:
        return None
    return result["client_id"], result["score"]


def compute_suggestion(
    conn: sqlite3.Connection,
    *,
    sender_email: Optional[str],
    sender_domain: Optional[str],
    sender_name: Optional[str],
) -> Optional[SuggestResult]:
    """Run the matching chain and return the first hit, or None.

    Never raises for "no match found" — only returns None. Privacy: only
    returns return_id/method/score; callers are responsible for keeping
    client names/log numbers as the only additional display data (no SSN/
    EIN/TIN/file_path ever flow through this module).
    """
    client_id = _exact_email_match(conn, sender_email or "")
    if client_id is not None:
        return_id = _most_recent_return_for_client(conn, client_id)
        if return_id is not None:
            return {"return_id": return_id, "method": "exact_email", "score": None}

    client_id = _domain_hint_match(conn, sender_domain or "")
    if client_id is not None:
        return_id = _most_recent_return_for_client(conn, client_id)
        if return_id is not None:
            return {"return_id": return_id, "method": "domain_hint", "score": None}

    fuzzy = _fuzzy_name_match(conn, sender_name or "")
    if fuzzy is not None:
        client_id, score = fuzzy
        return_id = _most_recent_return_for_client(conn, client_id)
        if return_id is not None:
            return {"return_id": return_id, "method": "fuzzy", "score": score}

    return None


def enrich_items_with_suggestions(conn: sqlite3.Connection, items: list[dict]) -> None:
    """Mutate ``items`` in place: compute+cache a suggestion for any item that
    doesn't have one yet (``suggested_return_id IS NULL``), then attach
    display-only fields (client name, log number) for the UI.

    Called from app.py's /email-inbox and /api/email-inbox/items handlers —
    never from the IMAP poll cycle. Commits the connection (UPDATE writes for
    newly-computed suggestions).

    Privacy: only client display name + log number are attached — never SSN/
    EIN/TIN/file_path.
    """
    dirty = False
    for item in items:
        if item.get("suggested_return_id") is None:
            suggestion = compute_suggestion(
                conn,
                sender_email=item.get("sender_email"),
                sender_domain=item.get("sender_domain"),
                sender_name=item.get("sender_name"),
            )
            if suggestion is not None:
                conn.execute(
                    "UPDATE email_inbox SET suggested_return_id=?, suggestion_method=?, "
                    "suggestion_score=? WHERE id=?",
                    (suggestion["return_id"], suggestion["method"], suggestion["score"], item["id"]),
                )
                dirty = True
                item["suggested_return_id"] = suggestion["return_id"]
                item["suggestion_method"] = suggestion["method"]
                item["suggestion_score"] = suggestion["score"]

        item["suggested_client_name"] = None
        item["suggested_log_number"] = None
        if item.get("suggested_return_id") is not None:
            row = conn.execute(
                "SELECT r.log_number, c.display_name, c.last_name, c.first_name "
                "FROM returns r JOIN clients c ON c.id = r.client_id WHERE r.id = ?",
                (item["suggested_return_id"],),
            ).fetchone()
            if row:
                name = row["display_name"] or (
                    f"{row['last_name']}, {row['first_name']}" if row["first_name"] else (row["last_name"] or "")
                )
                item["suggested_client_name"] = name
                item["suggested_log_number"] = row["log_number"]
            else:
                # Suggested return no longer exists (deleted after caching) —
                # drop the stale suggestion rather than show a dangling link.
                item["suggested_return_id"] = None
                item["suggestion_method"] = None
                item["suggestion_score"] = None

    if dirty:
        conn.commit()
