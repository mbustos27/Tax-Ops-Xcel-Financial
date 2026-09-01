"""Mass-email audience resolution and Jinja2 template rendering (Prompt K).

Preview / dry-run only — no outbound send. Recipients are deduplicated per
client_id even when multiple returns or missing-doc rows qualify.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Optional

from jinja2 import Environment, select_autoescape

import config
from client_email import resolve_client_email
from db import get_active_intake_tax_year
from engagement_status_rules import DRAKE_EFILE_COMPLETE_STATUSES
from return_deadlines import sql_upcoming_deadline_clause
from preparer import preparer_filter_match_values

_log = logging.getLogger(__name__)

_SLOW_CYCLE_DAYS = 21
_STALE_PROCESSING_DAYS = 60

_NEEDS_ATTENTION_DRAKE_DONE = (
    "EF Accepted",
    "E-Filed: YES",
    "Printed",
)

_IN_PROGRESS_STATUSES = (
    "PENDING INTAKE",
    "PROCESSING",
    "HOLD",
    "FINALIZE",
)

_ALLOWED_STATUSES = frozenset(
    {
        "PENDING INTAKE",
        "PROCESSING",
        "HOLD",
        "FINALIZE",
        "PICKUP",
        "EFILE READY",
        "LOG OUT",
        "REJECTED",
    }
)

_ALLOWED_FORM_FIELDS = frozenset(
    {
        "form_1040",
        "sched_a_d",
        "sched_c",
        "sched_e",
        "form_1120",
        "form_1120s",
        "form_1065_llc",
        "corp_officer",
        "business_owner",
        "form_990_1041",
        "is_amended",
        "has_w7",
        "is_extension",
    }
)

_CONTACT_STATUS_VALUES = frozenset(
    {"not_contacted", "contacted", "follow_up_needed", "resolved"}
)

# Campaign audience categories — maps to return_forms flags (OR within category).
CAMPAIGN_CATEGORIES: dict[str, dict[str, Any]] = {
    "personal": {
        "label": "Personal income tax (1040)",
        "forms": ["form_1040"],
    },
    "business_1120": {
        "label": "C corporation (Form 1120)",
        "forms": ["form_1120"],
    },
    "business_1120s": {
        "label": "S corporation (Form 1120-S)",
        "forms": ["form_1120s"],
    },
    "business_partnership": {
        "label": "Partnership / LLC (Form 1065)",
        "forms": ["form_1065_llc"],
    },
    "business_all": {
        "label": "All business returns (1120 / 1120-S / 1065)",
        "forms": ["form_1120", "form_1120s", "form_1065_llc"],
    },
}
ALLOWED_CAMPAIGN_CATEGORIES = frozenset(CAMPAIGN_CATEGORIES.keys())


def campaign_categories_for_ui() -> list[dict[str, str]]:
    """Category picker payload for admin UI."""
    return [
        {"id": key, "label": meta["label"]}
        for key, meta in CAMPAIGN_CATEGORIES.items()
    ]


def forms_for_campaign_category(category: str) -> list[str]:
    meta = CAMPAIGN_CATEGORIES.get(category) or {}
    forms = meta.get("forms") or []
    return [f for f in forms if f in _ALLOWED_FORM_FIELDS]


def _apply_form_clauses(
    clauses: list[str],
    forms: list[str],
) -> None:
    if not forms:
        return
    parts: list[str] = []
    for form_col in forms:
        if form_col in ("is_amended", "has_w7", "is_extension"):
            parts.append(f"r.{form_col} = 1")
        else:
            parts.append(f"rf.{form_col} = 1")
    if parts:
        clauses.append("(" + " OR ".join(parts) + ")")


_JINJA_ENV = Environment(autoescape=select_autoescape(["html", "xml"]))

_UNRENDERED_SYNTAX = re.compile(r"\{\{.*?\}\}|\{%.*?%\}")


@dataclass
class AudienceResolutionStats:
    """Counts only — safe for logs and reports (no PII)."""

    audience_return_rows: int = 0
    unique_clients_matched: int = 0
    dropped_do_not_email: int = 0
    dropped_no_email: int = 0
    resolved_recipients: int = 0
    deadline_window_active: bool = False
    deadline_window_blocked: bool = False
    deadline_window_reason: str = ""
    days_until_deadline: Optional[int] = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "audience_return_rows": self.audience_return_rows,
            "unique_clients_matched": self.unique_clients_matched,
            "dropped_do_not_email": self.dropped_do_not_email,
            "dropped_no_email": self.dropped_no_email,
            "resolved_recipients": self.resolved_recipients,
        }
        if self.deadline_window_active:
            out["deadline_window_active"] = True
            out["deadline_window_blocked"] = self.deadline_window_blocked
            if self.deadline_window_reason:
                out["deadline_window_reason"] = self.deadline_window_reason
            if self.days_until_deadline is not None:
                out["days_until_deadline"] = self.days_until_deadline
        return out


@dataclass
class CampaignRecipient:
    client_id: int
    return_id: int
    resolved_email: str
    template_variables: dict[str, Any] = field(default_factory=dict)


@dataclass
class RenderedEmail:
    subject: str
    body_html: str
    body_text: Optional[str] = None


def sanitize_audience_definition(raw: object) -> dict[str, Any]:
    """Whitelist audience JSON — dashboard_saved_filters shape plus campaign keys."""
    if not isinstance(raw, dict):
        return {}

    out: dict[str, Any] = {}

    ty = raw.get("tax_year")
    if ty is not None:
        try:
            out["tax_year"] = int(ty)
        except (TypeError, ValueError):
            pass

    statuses = raw.get("status")
    filtered_status: list[str] = []
    if isinstance(statuses, list):
        for s in statuses:
            ss = str(s).strip().upper()
            if ss in _ALLOWED_STATUSES:
                filtered_status.append(ss)
    elif isinstance(statuses, str) and statuses.strip():
        ss = statuses.strip().upper()
        if ss in _ALLOWED_STATUSES:
            filtered_status.append(ss)
    if filtered_status:
        out["status"] = filtered_status

    processor = raw.get("processor")
    if processor is not None and str(processor).strip():
        out["processor"] = str(processor).strip()

    for flag in ("balance_due", "late_intake", "slow_cycle", "stale_processing", "in_progress_only", "upcoming_deadline"):
        val = raw.get(flag)
        if val in ("1", 1, True, "true", "yes", "on"):
            out[flag] = "1"

    form_col = raw.get("form")
    if isinstance(form_col, str) and form_col.strip():
        fk = form_col.strip()
        if fk in _ALLOWED_FORM_FIELDS:
            out["form"] = fk

    forms_raw = raw.get("forms")
    forms_clean: list[str] = []
    if isinstance(forms_raw, list):
        for f in forms_raw:
            fk = str(f).strip()
            if fk in _ALLOWED_FORM_FIELDS:
                forms_clean.append(fk)
    if forms_clean:
        out["forms"] = forms_clean

    cat = raw.get("campaign_category")
    if isinstance(cat, str) and cat.strip() in ALLOWED_CAMPAIGN_CATEGORIES:
        out["campaign_category"] = cat.strip()
        if "forms" not in out:
            out["forms"] = forms_for_campaign_category(cat.strip())

    rc = raw.get("reject_contact")
    if isinstance(rc, str) and rc.strip():
        rcv = rc.strip().lower()
        if rcv == "needs_followup" or rcv in _CONTACT_STATUS_VALUES:
            out["reject_contact"] = rcv

    qq = raw.get("q")
    if isinstance(qq, str) and qq.strip():
        out["q"] = qq.strip()[:500]

    contact_statuses = raw.get("contact_status")
    cs_clean: list[str] = []
    if isinstance(contact_statuses, list):
        for cs in contact_statuses:
            c = str(cs).strip().lower()
            if c in _CONTACT_STATUS_VALUES:
                cs_clean.append(c)
    elif isinstance(contact_statuses, str) and contact_statuses.strip():
        c = contact_statuses.strip().lower()
        if c in _CONTACT_STATUS_VALUES:
            cs_clean.append(c)
    if cs_clean:
        out["contact_status"] = cs_clean

    omd = raw.get("open_missing_docs")
    if omd in (True, 1, "1", "true", "yes", "on"):
        out["open_missing_docs"] = True

    ext_flag = raw.get("is_extension")
    if ext_flag in (True, 1, "1", "true", "yes", "on"):
        out["is_extension"] = True

    within = raw.get("extension_due_within_days")
    if within is not None:
        try:
            days = int(within)
            if days >= 0:
                out["extension_due_within_days"] = days
        except (TypeError, ValueError):
            pass

    td_id = raw.get("tax_deadline_id")
    if td_id is not None:
        try:
            out["tax_deadline_id"] = int(td_id)
        except (TypeError, ValueError):
            pass

    for window_key in ("deadline_due_within_days", "deadline_due_min_days"):
        val = raw.get(window_key)
        if val is not None:
            try:
                days = int(val)
                if days >= 0:
                    out[window_key] = days
            except (TypeError, ValueError):
                pass

    return out


def _apply_quick_filter_q(clauses: list[str], params: list[Any], q_raw: object) -> None:
    if not q_raw:
        return
    q = str(q_raw).strip()
    if not q:
        return
    tokens = [t for t in q.lower().split() if t]
    if not tokens:
        return
    for tok in tokens:
        if tok.isdigit():
            clauses.append("(r.log_number = ? OR r.log_number LIKE ?)")
            params.extend([tok, f"{tok}%"])
        else:
            qp = f"%{tok}%"
            clauses.append(
                "("
                "lower(COALESCE(r.log_number,'')) LIKE ? OR "
                "lower(c.last_name) LIKE ? OR "
                "lower(c.first_name) LIKE ? OR "
                "lower(COALESCE(c.display_name,'')) LIKE ?"
                ")"
            )
            params.extend([qp, qp, qp, qp])


def _apply_exclude_drake_efile_complete(
    clauses: list[str],
    params: list[Any],
) -> None:
    """Skip returns Drake marks fully e-file accepted — office rule: LOG OUT."""
    placeholders = ",".join("?" for _ in DRAKE_EFILE_COMPLETE_STATUSES)
    clauses.append(
        f"(r.drake_status_raw IS NULL OR TRIM(r.drake_status_raw) = '' "
        f"OR r.drake_status_raw NOT IN ({placeholders}))"
    )
    params.extend(DRAKE_EFILE_COMPLETE_STATUSES)


def _build_audience_clauses(
    audience: dict[str, Any],
    conn,
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    clauses.append("UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'")
    clauses.append("COALESCE(c.is_test, 0) = 0")

    tax_year = audience.get("tax_year")
    if tax_year is None:
        tax_year = get_active_intake_tax_year(conn)
    clauses.append("r.tax_year = ?")
    params.append(int(tax_year))

    if audience.get("status"):
        statuses = audience["status"]
        clauses.append(f"r.client_status IN ({','.join('?' for _ in statuses)})")
        params.extend(statuses)

    if audience.get("processor"):
        pvals = preparer_filter_match_values(audience["processor"])
        if pvals:
            clauses.append("r.processor IN (" + ",".join("?" for _ in pvals) + ")")
            params.extend(pvals)

    if audience.get("balance_due"):
        clauses.append(
            "(p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee)"
        )

    if audience.get("late_intake"):
        clauses.append(
            "(r.intake_date IS NOT NULL AND ("
            "CAST(substr(r.intake_date,6,2) AS INTEGER) > 4 OR "
            "(CAST(substr(r.intake_date,6,2) AS INTEGER) = 4 AND "
            "CAST(substr(r.intake_date,9,2) AS INTEGER) >= 1)"
            "))"
        )

    if audience.get("slow_cycle"):
        # Open returns still in the office — intake at least N days ago (not dashboard
        # "completed slowly" which requires logout_date/ack_date).
        cutoff = (date.today() - timedelta(days=_SLOW_CYCLE_DAYS)).isoformat()
        clauses.append("r.intake_date IS NOT NULL")
        clauses.append("r.intake_date <= ?")
        params.append(cutoff)
        clauses.append(
            "UPPER(COALESCE(r.client_status, '')) NOT IN ('LOG OUT', 'REJECTED', 'CANCELLED')"
        )
        _apply_exclude_drake_efile_complete(clauses, params)

    if audience.get("stale_processing"):
        cutoff = (date.today() - timedelta(days=_STALE_PROCESSING_DAYS)).isoformat()
        placeholders = ",".join("?" for _ in _NEEDS_ATTENTION_DRAKE_DONE)
        clauses.append("r.client_status = 'PROCESSING'")
        clauses.append(
            "(c.display_name IS NOT NULL AND TRIM(c.display_name) != '')"
        )
        clauses.append("r.intake_date IS NOT NULL")
        clauses.append("r.intake_date < ?")
        params.append(cutoff)
        clauses.append(
            f"(r.drake_status_raw IS NULL OR TRIM(r.drake_status_raw) = '' "
            f"OR r.drake_status_raw NOT IN ({placeholders}))"
        )
        params.extend(_NEEDS_ATTENTION_DRAKE_DONE)

    if audience.get("upcoming_deadline"):
        clauses.append(sql_upcoming_deadline_clause("r"))

    if audience.get("in_progress_only"):
        clauses.append(
            f"r.client_status IN ({','.join('?' for _ in _IN_PROGRESS_STATUSES)})"
        )
        params.extend(_IN_PROGRESS_STATUSES)

    if audience.get("form"):
        form_col = audience["form"]
        _apply_form_clauses(clauses, [form_col])
    elif audience.get("forms"):
        _apply_form_clauses(clauses, list(audience["forms"]))
    elif audience.get("campaign_category"):
        _apply_form_clauses(
            clauses,
            forms_for_campaign_category(str(audience["campaign_category"])),
        )

    if audience.get("reject_contact"):
        st_list = audience.get("status") or []
        if not (st_list and "REJECTED" not in st_list):
            rc = (audience["reject_contact"] or "").strip().lower()
            clauses.append("r.client_status = 'REJECTED'")
            if rc == "needs_followup":
                clauses.append(
                    "(r.contact_status IS NULL OR r.contact_status = '' OR "
                    "r.contact_status IN ('not_contacted','follow_up_needed'))"
                )
            elif rc in _CONTACT_STATUS_VALUES:
                if rc == "not_contacted":
                    clauses.append(
                        "(r.contact_status IS NULL OR r.contact_status = '' OR "
                        "r.contact_status = 'not_contacted')"
                    )
                else:
                    clauses.append("r.contact_status = ?")
                    params.append(rc)

    if audience.get("contact_status"):
        cs_list = audience["contact_status"]
        parts: list[str] = []
        bind: list[Any] = []
        for cs in cs_list:
            if cs == "not_contacted":
                parts.append(
                    "(r.contact_status IS NULL OR r.contact_status = '' OR "
                    "r.contact_status = 'not_contacted')"
                )
            else:
                parts.append("r.contact_status = ?")
                bind.append(cs)
        if parts:
            clauses.append("(" + " OR ".join(parts) + ")")
            params.extend(bind)

    if audience.get("open_missing_docs"):
        clauses.append(
            "EXISTS ("
            "SELECT 1 FROM missing_docs md "
            "WHERE md.return_id = r.id AND md.is_resolved = 0"
            ")"
        )
        _apply_exclude_drake_efile_complete(clauses, params)

    if audience.get("is_extension"):
        clauses.append(
            "(COALESCE(r.is_extension, 0) = 1 OR COALESCE(r.extension_requested, 0) = 1)"
        )

    within = audience.get("extension_due_within_days")
    if within is not None:
        today = date.today()
        end = today + timedelta(days=int(within))
        clauses.append("r.extension_due_date IS NOT NULL")
        clauses.append("r.extension_due_date >= ?")
        clauses.append("r.extension_due_date <= ?")
        params.extend([today.isoformat(), end.isoformat()])

    _apply_quick_filter_q(clauses, params, audience.get("q"))

    return clauses, params


def _open_missing_count(conn, return_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM missing_docs "
        "WHERE return_id = ? AND is_resolved = 0",
        (return_id,),
    ).fetchone()
    return int(row["n"] or 0)


def _log_number_sort_key(log_number: Optional[str]) -> tuple[int, str]:
    if log_number and str(log_number).strip().isdigit():
        return (0, f"{int(log_number):010d}")
    return (1, str(log_number or ""))


def _pick_best_return_row(rows: list[dict], conn) -> dict:
    """One return per client — prefer more open missing docs, then log number."""
    scored: list[tuple[tuple[int, tuple], dict]] = []
    for row in rows:
        rid = int(row["return_id"])
        open_n = _open_missing_count(conn, rid)
        key = (-open_n, _log_number_sort_key(row.get("log_number")))
        scored.append((key, row))
    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def _build_name_full(
    first: str,
    last: str,
    display_name: str,
    spouse_first: str = "",
    spouse_last: str = "",
) -> str:
    if display_name:
        return display_name
    base = f"{last}, {first}".strip(", ") if first else last
    sp_first = (spouse_first or "").strip()
    if sp_first:
        return f"{base} & {sp_first}"
    return base


def _office_contact_defaults() -> dict[str, str]:
    phone = (getattr(config, "OFFICE_PHONE", "") or "").strip()
    reply_to = (config.SMTP_FROM or config.IMAP_USER or "").strip()
    return {"office_phone": phone, "reply_to": reply_to}


def _tax_deadline_row(conn, tax_deadline_id: Optional[int]) -> Optional[dict]:
    if not tax_deadline_id:
        return None
    row = conn.execute(
        """
        SELECT id, label, due_date, tax_year, applies_to
        FROM tax_deadlines
        WHERE id = ? AND is_active = 1
        """,
        (int(tax_deadline_id),),
    ).fetchone()
    return dict(row) if row else None


def _tax_deadline_date(conn, tax_deadline_id: Optional[int]) -> Optional[str]:
    row = _tax_deadline_row(conn, tax_deadline_id)
    if row and row.get("due_date"):
        return str(row["due_date"])[:10]
    return None


def list_tax_deadlines(conn, tax_year: Optional[int] = None) -> list[dict[str, Any]]:
    """Active tax_deadlines rows for admin UI / campaign setup."""
    ty = tax_year
    if ty is None:
        ty = get_active_intake_tax_year(conn)
    rows = conn.execute(
        """
        SELECT id, label, applies_to, due_date, tax_year
        FROM tax_deadlines
        WHERE is_active = 1 AND tax_year = ?
        ORDER BY due_date, label
        """,
        (int(ty),),
    ).fetchall()
    return [dict(r) for r in rows]


def _deadline_window_check(
    conn,
    audience: dict[str, Any],
) -> tuple[bool, AudienceResolutionStats]:
    """Gate campaigns tied to tax_deadline_id + window keys."""
    stats = AudienceResolutionStats()
    td_id = audience.get("tax_deadline_id")
    within = audience.get("deadline_due_within_days")
    if td_id is None or within is None:
        return True, stats

    stats.deadline_window_active = True
    row = _tax_deadline_row(conn, td_id)
    if not row or not row.get("due_date"):
        stats.deadline_window_blocked = True
        stats.deadline_window_reason = "deadline_not_found"
        return False, stats

    due = date.fromisoformat(str(row["due_date"])[:10])
    today = date.today()
    days_left = (due - today).days
    stats.days_until_deadline = days_left
    min_days = int(audience.get("deadline_due_min_days") or 0)
    if days_left < min_days or days_left > int(within):
        stats.deadline_window_blocked = True
        stats.deadline_window_reason = "outside_deadline_window"
        return False, stats
    return True, stats


def _cycle_days(intake_date: Optional[str], end_date: Optional[str]) -> Optional[int]:
    if not intake_date or not end_date:
        return None
    try:
        d0 = date.fromisoformat(str(intake_date)[:10])
        d1 = date.fromisoformat(str(end_date)[:10])
        return (d1 - d0).days
    except ValueError:
        return None


def _open_missing_items(conn, return_id: int) -> list[str]:
    rows = conn.execute(
        "SELECT item_text FROM missing_docs "
        "WHERE return_id = ? AND is_resolved = 0 "
        "ORDER BY item_text",
        (return_id,),
    ).fetchall()
    return [str(r["item_text"]) for r in rows if r["item_text"]]


def build_template_variables(
    conn,
    client_id: int,
    return_id: int,
    audience: dict[str, Any],
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            r.log_number,
            r.tax_year,
            r.client_status,
            r.contact_status,
            r.extension_due_date,
            r.intake_date,
            r.logout_date,
            r.ack_date,
            c.display_name,
            c.first_name,
            c.last_name,
            c.spouse_first_name,
            c.spouse_last_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE r.id = ? AND c.id = ?
        """,
        (return_id, client_id),
    ).fetchone()
    if not row:
        return {}

    missing_items = _open_missing_items(conn, return_id)
    deadline = ""
    deadline_label = ""
    days_until: Optional[int] = None

    td_row = _tax_deadline_row(conn, audience.get("tax_deadline_id"))
    if td_row and td_row.get("due_date"):
        deadline = str(td_row["due_date"])[:10]
        deadline_label = str(td_row.get("label") or "")
        try:
            days_until = (date.fromisoformat(deadline) - date.today()).days
        except ValueError:
            days_until = None
    elif row["extension_due_date"]:
        deadline = str(row["extension_due_date"])[:10]
        try:
            days_until = (date.fromisoformat(deadline) - date.today()).days
        except ValueError:
            days_until = None

    cycle_end = row["logout_date"] or row["ack_date"]
    if cycle_end:
        cycle_days = _cycle_days(row["intake_date"], cycle_end)
    else:
        cycle_days = _cycle_days(row["intake_date"], date.today().isoformat())
    intake_display = str(row["intake_date"])[:10] if row["intake_date"] else ""

    name_full = _build_name_full(
        row["first_name"] or "",
        row["last_name"] or "",
        row["display_name"] or "",
        row["spouse_first_name"] or "",
        row["spouse_last_name"] or "",
    )
    contacts = _office_contact_defaults()
    return {
        "client_name": name_full,
        "name": name_full,
        "display_name": row["display_name"] or name_full,
        "log_number": row["log_number"] or "",
        "tax_year": row["tax_year"],
        "client_status": row["client_status"] or "",
        "contact_status": row["contact_status"] or "",
        "missing_items": missing_items,
        "missing_items_text": "\n".join(f"• {t}" for t in missing_items),
        "deadline_date": deadline,
        "deadline_label": deadline_label,
        "days_until_deadline": days_until if days_until is not None else "",
        "intake_date": intake_display,
        "cycle_days": cycle_days if cycle_days is not None else "",
        "office_phone": contacts["office_phone"],
        "reply_to": contacts["reply_to"],
    }


def resolve_audience(
    conn,
    audience_definition: object,
) -> tuple[list[CampaignRecipient], AudienceResolutionStats]:
    """Return deduplicated recipients with template variable dicts (no rendering)."""
    audience = sanitize_audience_definition(audience_definition)
    window_ok, window_stats = _deadline_window_check(conn, audience)
    if not window_ok:
        return [], window_stats

    clauses, params = _build_audience_clauses(audience, conn)
    where = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT
            r.id AS return_id,
            r.client_id,
            r.log_number,
            c.do_not_email
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        LEFT JOIN payments p ON p.return_id = r.id
        LEFT JOIN return_forms rf ON rf.return_id = r.id
        {where}
        ORDER BY r.client_id, r.log_number
    """
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    stats = AudienceResolutionStats(audience_return_rows=len(rows))
    if window_stats.deadline_window_active:
        stats.deadline_window_active = True
        stats.days_until_deadline = window_stats.days_until_deadline

    by_client: dict[int, list[dict]] = {}
    for row in rows:
        cid = int(row["client_id"])
        by_client.setdefault(cid, []).append(row)

    stats.unique_clients_matched = len(by_client)
    recipients: list[CampaignRecipient] = []

    for cid, client_rows in by_client.items():
        if int(client_rows[0].get("do_not_email") or 0):
            stats.dropped_do_not_email += 1
            continue

        email = resolve_client_email(conn, cid)
        if not email:
            stats.dropped_no_email += 1
            continue

        best = _pick_best_return_row(client_rows, conn)
        return_id = int(best["return_id"])
        variables = build_template_variables(conn, cid, return_id, audience)
        recipients.append(
            CampaignRecipient(
                client_id=cid,
                return_id=return_id,
                resolved_email=email,
                template_variables=variables,
            )
        )

    stats.resolved_recipients = len(recipients)
    return recipients, stats


def load_email_template(conn, template_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT id, key, subject, body_html, body_text, variables_schema, is_active
        FROM email_templates
        WHERE id = ? AND is_active = 1
        """,
        (int(template_id),),
    ).fetchone()
    return dict(row) if row else None


def render_email_template(template: dict, variables: dict[str, Any]) -> RenderedEmail:
    subject = _JINJA_ENV.from_string(template["subject"]).render(**variables)
    body_html = _JINJA_ENV.from_string(template["body_html"]).render(**variables)
    body_text = None
    raw_text = template.get("body_text")
    if raw_text:
        body_text = _JINJA_ENV.from_string(raw_text).render(**variables)
    return RenderedEmail(subject=subject, body_html=body_html, body_text=body_text)


def preview_campaign(
    conn,
    audience_definition: object,
    template_id: Optional[int] = None,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Preview audience + optional template render. Response may include PII for staff UI."""
    recipients, stats = resolve_audience(conn, audience_definition)
    limit = max(0, min(int(sample_limit), 50))
    samples: list[dict[str, Any]] = []

    template = None
    if template_id is not None:
        template = load_email_template(conn, int(template_id))

    for rec in recipients[:limit]:
        item: dict[str, Any] = {
            "client_id": rec.client_id,
            "return_id": rec.return_id,
            "log_number": rec.template_variables.get("log_number"),
            "client_name": rec.template_variables.get("client_name"),
            "resolved_email": rec.resolved_email,
            "template_variables": rec.template_variables,
        }
        if template:
            rendered = render_email_template(template, rec.template_variables)
            item["subject"] = rendered.subject
            item["body_html"] = rendered.body_html
            item["body_text"] = rendered.body_text
            if _UNRENDERED_SYNTAX.search(rendered.subject):
                item["render_warning"] = "unrendered_syntax_in_subject"
            elif _UNRENDERED_SYNTAX.search(rendered.body_html):
                item["render_warning"] = "unrendered_syntax_in_body"
        samples.append(item)

    return {
        "stats": stats.as_dict(),
        "resolved_recipients": stats.resolved_recipients,
        "samples": samples,
        "template_found": template is not None if template_id else None,
    }


def audience_definition_from_json(text: str) -> dict[str, Any]:
    try:
        raw = json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid audience JSON: {exc}") from exc
    return sanitize_audience_definition(raw)
