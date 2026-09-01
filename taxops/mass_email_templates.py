"""Canonical mass-email template definitions.

Office: Xcel Financial Services, LLC
"""

from __future__ import annotations

from typing import Any

from mass_email_template_shell import (
    email_footer_text_block,
    email_html_document,
    paragraph,
    reply_cta,
    sign_off,
)

_COMMON_VARS = [
    "client_name",
    "name",
    "display_name",
    "log_number",
    "tax_year",
    "office_phone",
    "reply_to",
]

_DEADLINE_VARS = _COMMON_VARS + [
    "deadline_date",
    "deadline_label",
    "days_until_deadline",
    "missing_items",
    "missing_items_text",
]

_OPS_VARS = _COMMON_VARS + [
    "intake_date",
    "cycle_days",
    "missing_items",
    "missing_items_text",
]


def _missing_docs_body_html() -> str:
    return (
        paragraph("Hello {{ display_name }},")
        + paragraph(
            "We're working on your {{ tax_year }} tax return (Log #{{ log_number }}), "
            "but a few documents are still needed before we can move forward."
        )
        + "{% if missing_items %}"
        + paragraph("<strong>Still needed:</strong>")
        + '<ul style="margin:0 0 16px 0; padding-left:20px; font-size:15px; color:#222222; line-height:1.6;">'
        + "{% for item in missing_items %}"
        + "<li>{{ item }}</li>"
        + "{% endfor %}"
        + "</ul>"
        + "{% endif %}"
        + paragraph(
            "You can simply reply to this email with the documents attached{% if office_phone %}, "
            "or call us at {{ office_phone }}{% endif %}."
        )
        + sign_off()
    )


def _missing_docs_body_text() -> str:
    return """Hello {{ display_name }},

We're working on your {{ tax_year }} tax return (Log #{{ log_number }}), but a few documents are still needed before we can move forward.
{% if missing_items %}
Still needed:
{{ missing_items_text }}
{% endif %}
You can simply reply to this email with the documents attached{% if office_phone %}, or call us at {{ office_phone }}{% endif %}.

Thank you,
Xcel Financial Services, LLC""" + email_footer_text_block()


TEMPLATE_MISSING_DOCS_REMINDER: dict[str, Any] = {
    "key": "missing_docs_reminder",
    "category": "all",
    "subject": (
        "Documents still needed for your {{ tax_year }} return — Log #{{ log_number }}"
    ),
    "body_html": email_html_document(
        "A few documents are still needed to finish your {{ tax_year }} tax return.",
        _missing_docs_body_html(),
        "Documents Needed",
    ),
    "body_text": _missing_docs_body_text(),
    "variables_schema": _COMMON_VARS + ["missing_items", "missing_items_text"],
}

TEMPLATE_EXTENSION_DEADLINE_REMINDER: dict[str, Any] = {
    "key": "extension_deadline_reminder",
    "category": "all",
    "subject": (
        "{% if deadline_date %}Your extended {{ tax_year }} return is due "
        "{{ deadline_date }} — Log #{{ log_number }}{% else %}Reminder: your extended "
        "{{ tax_year }} return — Log #{{ log_number }}{% endif %}"
    ),
    "body_html": email_html_document(
        "Your extended tax return deadline is approaching — here's what we still need.",
        "{% if deadline_date %}"
        + paragraph("Hello {{ display_name }},")
        + paragraph(
            "This is a reminder that your extended {{ tax_year }} tax return "
            "(Log #{{ log_number }}) is due on <strong>{{ deadline_date }}</strong>."
        )
        + "{% else %}"
        + paragraph("Hello {{ display_name }},")
        + paragraph(
            "This is a reminder about your extended {{ tax_year }} tax return "
            "(Log #{{ log_number }})."
        )
        + "{% endif %}"
        + "{% if missing_items %}"
        + paragraph("<strong>To complete and file your return, we still need:</strong>")
        + '<ul style="margin:0 0 16px 0; padding-left:20px; font-size:15px; color:#222222; line-height:1.6;">'
        + "{% for item in missing_items %}"
        + "<li>{{ item }}</li>"
        + "{% endfor %}"
        + "</ul>"
        + "{% endif %}"
        + paragraph(
            "Please send any remaining signed forms or documents as soon as possible so we "
            "can complete and file your return. " + reply_cta()
        )
        + sign_off(),
        "Extension Deadline Reminder",
    ),
    "body_text": """Hello {{ display_name }},
{% if deadline_date %}
This is a reminder that your extended {{ tax_year }} tax return (Log #{{ log_number }}) is due on {{ deadline_date }}.
{% else %}
This is a reminder about your extended {{ tax_year }} tax return (Log #{{ log_number }}).
{% endif %}
{% if missing_items %}
To complete and file your return, we still need:
{{ missing_items_text }}
{% endif %}
Please send any remaining signed forms or documents as soon as possible so we can complete and file your return. """
    + reply_cta()
    + """

Thank you,
Xcel Financial Services, LLC"""
    + email_footer_text_block(),
    "variables_schema": _DEADLINE_VARS,
}

TEMPLATE_DEADLINE_REMINDER_EARLY: dict[str, Any] = {
    "key": "deadline_reminder_early",
    "category": "all",
    "subject": (
        "{% if deadline_label %}{{ deadline_label }} on {{ deadline_date }} — "
        "Log #{{ log_number }}{% else %}Upcoming tax deadline — Log #{{ log_number }}{% endif %}"
    ),
    "body_html": email_html_document(
        "A tax deadline is coming up — here's what we still need from you.",
        paragraph("Hello {{ display_name }},")
        + "{% if deadline_date and deadline_label %}"
        + paragraph(
            "This is an early reminder that <strong>{{ deadline_label }}</strong> for your "
            "{{ tax_year }} tax return (Log #{{ log_number }}) is "
            "<strong>{{ deadline_date }}</strong>"
            "{% if days_until_deadline %} "
            "({{ days_until_deadline }} day{% if days_until_deadline != 1 %}s{% endif %} "
            "from today){% endif %}."
        )
        + "{% else %}"
        + paragraph(
            "This is an early reminder about an upcoming deadline for your "
            "{{ tax_year }} tax return (Log #{{ log_number }})."
        )
        + "{% endif %}"
        + "{% if missing_items %}"
        + paragraph("<strong>To stay on track, we still need:</strong>")
        + '<ul style="margin:0 0 16px 0; padding-left:20px; font-size:15px; color:#222222; line-height:1.6;">'
        + "{% for item in missing_items %}"
        + "<li>{{ item }}</li>"
        + "{% endfor %}"
        + "</ul>"
        + "{% else %}"
        + paragraph(
            "If we have already received everything from you, no action is needed — "
            "thank you for staying in touch with our office."
        )
        + "{% endif %}"
        + paragraph(
            "Please send any remaining documents or signed forms at your earliest convenience. "
            + reply_cta()
        )
        + sign_off(),
        "Tax Deadline Reminder",
    ),
    "body_text": """Hello {{ display_name }},
{% if deadline_date and deadline_label %}
This is an early reminder that {{ deadline_label }} for your {{ tax_year }} tax return (Log #{{ log_number }}) is {{ deadline_date }}{% if days_until_deadline %} ({{ days_until_deadline }} days from today){% endif %}.
{% else %}
This is an early reminder about an upcoming deadline for your {{ tax_year }} tax return (Log #{{ log_number }}).
{% endif %}
{% if missing_items %}
To stay on track, we still need:
{{ missing_items_text }}
{% else %}
If we have already received everything from you, no action is needed — thank you for staying in touch with our office.
{% endif %}
Please send any remaining documents or signed forms at your earliest convenience. """
    + reply_cta()
    + """

Thank you,
Xcel Financial Services, LLC"""
    + email_footer_text_block(),
    "variables_schema": _DEADLINE_VARS,
}

TEMPLATE_DEADLINE_REMINDER_FINAL: dict[str, Any] = {
    "key": "deadline_reminder_final",
    "category": "all",
    "subject": (
        "{% if deadline_date %}Final reminder: due {{ deadline_date }} — "
        "Log #{{ log_number }}{% else %}Final deadline reminder — Log #{{ log_number }}{% endif %}"
    ),
    "body_html": email_html_document(
        "Your tax deadline is very soon — please send remaining items right away.",
        paragraph("Hello {{ display_name }},")
        + "{% if deadline_date and deadline_label %}"
        + paragraph(
            "<strong>{{ deadline_label }}</strong> for your {{ tax_year }} tax return "
            "(Log #{{ log_number }}) is <strong>{{ deadline_date }}</strong>"
            "{% if days_until_deadline %} — "
            "<strong>{{ days_until_deadline }} day{% if days_until_deadline != 1 %}s{% endif %} "
            "remaining</strong>{% endif %}."
        )
        + "{% else %}"
        + paragraph(
            "This is a final reminder about an approaching deadline for your "
            "{{ tax_year }} tax return (Log #{{ log_number }})."
        )
        + "{% endif %}"
        + "{% if missing_items %}"
        + paragraph("<strong>We still need the following to complete your return:</strong>")
        + '<ul style="margin:0 0 16px 0; padding-left:20px; font-size:15px; color:#222222; line-height:1.6;">'
        + "{% for item in missing_items %}"
        + "<li>{{ item }}</li>"
        + "{% endfor %}"
        + "</ul>"
        + "{% endif %}"
        + paragraph(
            "Please reply to this email with any outstanding documents or signed forms "
            "<strong>as soon as possible</strong> so we can finish before the deadline. "
            + reply_cta()
        )
        + sign_off(),
        "Final Tax Deadline Reminder",
    ),
    "body_text": """Hello {{ display_name }},
{% if deadline_date and deadline_label %}
{{ deadline_label }} for your {{ tax_year }} tax return (Log #{{ log_number }}) is {{ deadline_date }}{% if days_until_deadline %} — {{ days_until_deadline }} days remaining{% endif %}.
{% else %}
This is a final reminder about an approaching deadline for your {{ tax_year }} tax return (Log #{{ log_number }}).
{% endif %}
{% if missing_items %}
We still need the following to complete your return:
{{ missing_items_text }}
{% endif %}
Please reply to this email with any outstanding documents or signed forms as soon as possible so we can finish before the deadline. """
    + reply_cta()
    + """

Thank you,
Xcel Financial Services, LLC"""
    + email_footer_text_block(),
    "variables_schema": _DEADLINE_VARS,
}

TEMPLATE_STALE_PROCESSING_REMINDER: dict[str, Any] = {
    "key": "stale_processing_reminder",
    "category": "all",
    "subject": (
        "Checking in on your {{ tax_year }} return — Log #{{ log_number }}"
    ),
    "body_html": email_html_document(
        "We're still working on your return and may need a few items from you.",
        paragraph("Hello {{ display_name }},")
        + paragraph(
            "We're following up on your {{ tax_year }} tax return (Log #{{ log_number }}). "
            "Your file has been in progress with us for a while, and we want to make sure "
            "nothing is holding us back from completing your return."
        )
        + "{% if missing_items %}"
        + paragraph("<strong>Items we still need from you:</strong>")
        + '<ul style="margin:0 0 16px 0; padding-left:20px; font-size:15px; color:#222222; line-height:1.6;">'
        + "{% for item in missing_items %}"
        + "<li>{{ item }}</li>"
        + "{% endfor %}"
        + "</ul>"
        + "{% else %}"
        + paragraph(
            "If you've sent everything already, a quick reply to confirm is helpful. "
            "If anything changed since we last spoke, please let us know."
        )
        + "{% endif %}"
        + paragraph(
            "You can reply to this email with documents attached{% if office_phone %}, "
            "or call us at {{ office_phone }}{% endif %}."
        )
        + sign_off(),
        "Return Status Check-in",
    ),
    "body_text": """Hello {{ display_name }},

We're following up on your {{ tax_year }} tax return (Log #{{ log_number }}). Your file has been in progress with us for a while, and we want to make sure nothing is holding us back from completing your return.
{% if missing_items %}
Items we still need from you:
{{ missing_items_text }}
{% else %}
If you've sent everything already, a quick reply to confirm is helpful. If anything changed since we last spoke, please let us know.
{% endif %}
You can reply to this email with documents attached{% if office_phone %}, or call us at {{ office_phone }}{% endif %}.

Thank you,
Xcel Financial Services, LLC"""
    + email_footer_text_block(),
    "variables_schema": _OPS_VARS,
}

TEMPLATE_SLOW_CYCLE_REMINDER: dict[str, Any] = {
    "key": "slow_cycle_reminder",
    "category": "all",
    "subject": (
        "Update on your {{ tax_year }} return — Log #{{ log_number }}"
    ),
    "body_html": email_html_document(
        "Your return is taking longer than usual — here's how you can help us finish.",
        paragraph("Hello {{ display_name }},")
        + paragraph(
            "We're reaching out about your {{ tax_year }} tax return (Log #{{ log_number }}). "
            "This return has been open longer than our typical processing window, and we want "
            "to keep things moving for you."
        )
        + "{% if missing_items %}"
        + paragraph("<strong>Outstanding items:</strong>")
        + '<ul style="margin:0 0 16px 0; padding-left:20px; font-size:15px; color:#222222; line-height:1.6;">'
        + "{% for item in missing_items %}"
        + "<li>{{ item }}</li>"
        + "{% endfor %}"
        + "</ul>"
        + "{% endif %}"
        + paragraph(
            "If you have documents or questions, please reply to this email{% if office_phone %} "
            "or call us at {{ office_phone }}{% endif %} so we can wrap up your return."
        )
        + sign_off(),
        "Return Processing Update",
    ),
    "body_text": """Hello {{ display_name }},

We're reaching out about your {{ tax_year }} tax return (Log #{{ log_number }}). This return has been open longer than our typical processing window, and we want to keep things moving for you.
{% if missing_items %}
Outstanding items:
{{ missing_items_text }}
{% endif %}
If you have documents or questions, please reply to this email{% if office_phone %} or call us at {{ office_phone }}{% endif %} so we can wrap up your return.

Thank you,
Xcel Financial Services, LLC"""
    + email_footer_text_block(),
    "variables_schema": _OPS_VARS,
}

ALL_TEMPLATES: tuple[dict[str, Any], ...] = (
    TEMPLATE_MISSING_DOCS_REMINDER,
    TEMPLATE_EXTENSION_DEADLINE_REMINDER,
    TEMPLATE_DEADLINE_REMINDER_EARLY,
    TEMPLATE_DEADLINE_REMINDER_FINAL,
    TEMPLATE_STALE_PROCESSING_REMINDER,
    TEMPLATE_SLOW_CYCLE_REMINDER,
)

# Default campaign audience JSON — tax_year adjusted at seed/runtime.
CAMPAIGN_AUDIENCE_MISSING_DOCS: dict[str, Any] = {
    "tax_year": 2026,
    "status": ["PROCESSING", "HOLD"],
    "open_missing_docs": True,
}

CAMPAIGN_AUDIENCE_EXTENSION_DEADLINE: dict[str, Any] = {
    "tax_year": 2026,
    "campaign_category": "personal",
    "is_extension": True,
    "extension_due_within_days": 14,
}

CAMPAIGN_AUDIENCE_STALE_PROCESSING: dict[str, Any] = {
    "tax_year": 2026,
    "stale_processing": True,
}

CAMPAIGN_AUDIENCE_SLOW_CYCLE: dict[str, Any] = {
    "tax_year": 2026,
    "slow_cycle": True,
    "status": ["PROCESSING", "HOLD", "FINALIZE", "PICKUP"],
}

# Early reminder: 8–30 days before deadline; final: 0–7 days.
CAMPAIGN_AUDIENCE_DEADLINE_EARLY: dict[str, Any] = {
    "tax_year": 2026,
    "campaign_category": "personal",
    "in_progress_only": True,
    "deadline_due_within_days": 30,
    "deadline_due_min_days": 8,
}

CAMPAIGN_AUDIENCE_DEADLINE_FINAL: dict[str, Any] = {
    "tax_year": 2026,
    "campaign_category": "personal",
    "in_progress_only": True,
    "deadline_due_within_days": 7,
    "deadline_due_min_days": 0,
}

# Federal / common deadlines for active intake tax years.
def _business_deadline_seeds(tax_year: int, due_year: int) -> tuple[dict[str, Any], ...]:
    """Entity return due dates for calendar-year filers (due in due_year)."""
    return (
        {
            "label": "C corporation return due",
            "applies_to": "federal/1120",
            "due_date": f"{due_year}-04-15",
            "tax_year": tax_year,
        },
        {
            "label": "C corporation extended return due",
            "applies_to": "federal/1120/extension",
            "due_date": f"{due_year}-10-15",
            "tax_year": tax_year,
        },
        {
            "label": "S corporation return due",
            "applies_to": "federal/1120s",
            "due_date": f"{due_year}-03-15",
            "tax_year": tax_year,
        },
        {
            "label": "S corporation extended return due",
            "applies_to": "federal/1120s/extension",
            "due_date": f"{due_year}-09-15",
            "tax_year": tax_year,
        },
        {
            "label": "Partnership / LLC return due",
            "applies_to": "federal/1065",
            "due_date": f"{due_year}-03-15",
            "tax_year": tax_year,
        },
        {
            "label": "Partnership / LLC extended return due",
            "applies_to": "federal/1065/extension",
            "due_date": f"{due_year}-09-15",
            "tax_year": tax_year,
        },
        {
            "label": "990 / 1041 return due",
            "applies_to": "federal/990_1041",
            "due_date": f"{due_year}-05-15",
            "tax_year": tax_year,
        },
        {
            "label": "990 / 1041 extended return due",
            "applies_to": "federal/990_1041/extension",
            "due_date": f"{due_year}-11-15",
            "tax_year": tax_year,
        },
    )


_TY2025_CORE_DEADLINES: tuple[dict[str, Any], ...] = (
    {
        "label": "Federal individual return due",
        "applies_to": "federal/1040",
        "due_date": "2026-04-15",
        "tax_year": 2025,
    },
    {
        "label": "Federal extension request due",
        "applies_to": "federal/extension_request",
        "due_date": "2026-04-15",
        "tax_year": 2025,
    },
    {
        "label": "Federal extended return due",
        "applies_to": "federal/extension",
        "due_date": "2026-10-15",
        "tax_year": 2025,
    },
)

_TY2026_CORE_DEADLINES: tuple[dict[str, Any], ...] = (
    {
        "label": "Federal individual return due",
        "applies_to": "federal/1040",
        "due_date": "2027-04-15",
        "tax_year": 2026,
    },
    {
        "label": "Federal extension request due",
        "applies_to": "federal/extension_request",
        "due_date": "2027-04-15",
        "tax_year": 2026,
    },
    {
        "label": "Federal extended return due",
        "applies_to": "federal/extension",
        "due_date": "2027-10-15",
        "tax_year": 2026,
    },
    {
        "label": "Q1 estimated tax payment",
        "applies_to": "federal/estimated/q1",
        "due_date": "2026-04-15",
        "tax_year": 2026,
    },
    {
        "label": "Q2 estimated tax payment",
        "applies_to": "federal/estimated/q2",
        "due_date": "2026-06-15",
        "tax_year": 2026,
    },
    {
        "label": "Q3 estimated tax payment",
        "applies_to": "federal/estimated/q3",
        "due_date": "2026-09-15",
        "tax_year": 2026,
    },
    {
        "label": "Q4 estimated tax payment",
        "applies_to": "federal/estimated/q4",
        "due_date": "2027-01-15",
        "tax_year": 2026,
    },
)

TAX_DEADLINE_SEEDS: tuple[dict[str, Any], ...] = (
    *_TY2025_CORE_DEADLINES,
    *_business_deadline_seeds(2025, 2026),
    *_TY2026_CORE_DEADLINES,
    *_business_deadline_seeds(2026, 2027),
)

# Draft campaigns seeded per deadline label (early + final windows).
DEADLINE_CAMPAIGN_LABELS: tuple[str, ...] = (
    "Federal individual return due",
    "Federal extension request due",
    "Federal extended return due",
    "C corporation return due",
    "S corporation return due",
    "Partnership / LLC return due",
    "990 / 1041 return due",
    "Q1 estimated tax payment",
    "Q2 estimated tax payment",
    "Q3 estimated tax payment",
    "Q4 estimated tax payment",
)
