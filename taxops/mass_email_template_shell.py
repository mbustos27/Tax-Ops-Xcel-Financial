"""Shared HTML shell for Xcel mass-email templates."""

from __future__ import annotations

OFFICE_NAME = "Xcel Financial Services, LLC"
ACCENT = "#6B2233"


def email_html_document(preheader: str, body_inner: str, title: str = "Message") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
</head>
<body style="margin:0; padding:0; background-color:#f4f4f4; font-family: Arial, Helvetica, sans-serif;">
<span style="display:none; font-size:1px; color:#f4f4f4; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">{preheader}</span>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f4; padding:24px 0;">
<tr>
<td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; max-width:600px; width:100%; border-collapse:collapse;">
<tr>
<td style="background-color:{ACCENT}; padding:20px 24px;">
<span style="color:#ffffff; font-size:18px; font-weight:bold; font-family: Arial, Helvetica, sans-serif;">{OFFICE_NAME}</span>
</td>
</tr>
<tr>
<td style="padding:24px;">
{body_inner}
</td>
</tr>
<tr>
<td style="padding:16px 24px; border-top:1px solid #eeeeee;">
<p style="margin:0; font-size:12px; color:#888888; line-height:1.5;">This message is regarding tax return Log #{{{{ log_number }}}} ({{{{ tax_year }}}}). If you'd prefer not to receive email reminders like this, please contact our office and we'll update your file.</p>
</td>
</tr>
</table>
</td>
</tr>
</table>
</body>
</html>"""


def email_footer_text_block() -> str:
    return (
        "\n---\n"
        "This message is regarding tax return Log #{{ log_number }} ({{ tax_year }}). "
        "If you'd prefer not to receive email reminders like this, please contact our office "
        "and we'll update your file."
    )


def paragraph(text: str) -> str:
    return (
        f'<p style="margin:0 0 16px 0; font-size:15px; color:#222222; line-height:1.5;">'
        f"{text}</p>"
    )


def sign_off() -> str:
    return (
        paragraph("Thank you,")
        + f'<p style="margin:0; font-size:15px; color:{ACCENT}; font-weight:bold;">'
        f"{OFFICE_NAME}</p>"
    )


def reply_cta() -> str:
    return (
        "You can simply reply to this email{% if office_phone %}, or call us at "
        "{{ office_phone }}{% endif %}."
    )
