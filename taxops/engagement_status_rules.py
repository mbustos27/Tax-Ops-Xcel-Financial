"""Drake engagement status invariants (client export / workflow).

Office rules (confirmed Aug 2026):
- E-File Accepted → TaxOps client_status must be LOG OUT
- Extension E-File Accepted → TaxOps client_status must be at least PROCESSING
  (extension filed; return work continues — not case-closed)
"""

from __future__ import annotations

# Drake client-export Engagement Status column values
ENGAGEMENT_EFILE_ACCEPTED = "E-File Accepted"
ENGAGEMENT_EXTENSION_EFILE_ACCEPTED = "Extension E-File Accepted"

# Map Drake client-export Engagement Status → drake_status_raw (CSM Status column).
ENGAGEMENT_TO_DRAKE_STATUS: dict[str, str] = {
    ENGAGEMENT_EFILE_ACCEPTED: "EF Accepted",
    ENGAGEMENT_EXTENSION_EFILE_ACCEPTED: "EF Ext Accepted",
    "E-File Rejected": "EF Rejected",
    "Extension E-File Rejected": "EF Rejected",
    "Pending E-File Acceptance": "EF Pending",
    "Pending Extension E-File Acceptance": "EF Pending",
    "Pending Signature": "Printed",
    "Rolled from prior year": "In Progress",
    "New Return": "In Progress",
}


def engagement_to_drake_status_raw(engagement_status: str | None) -> str | None:
    """Translate client-export Engagement Status to drake_status_raw, or None if unknown/blank."""
    eng = (engagement_status or "").strip()
    if not eng:
        return None
    return ENGAGEMENT_TO_DRAKE_STATUS.get(eng)


# Drake client-export Client Type → primary entity form flag.
CLIENT_TYPE_ENTITY_FORM: dict[str, str] = {
    "Corporate": "form_1120",
    "Business 1120-S": "form_1120s",
    "Partnership": "form_1065_llc",
    "Exempt": "form_990_1041",
    "Fiduciary": "form_990_1041",
    "Individual": "form_1040",
}

# drake_status_raw values that mean the full return was e-file accepted (case closed).
DRAKE_EFILE_COMPLETE_STATUSES = frozenset(
    {
        "EF Accepted",
        "E-Filed: YES",
    }
)

# drake_status_raw for extension-only acceptance — still in progress.
DRAKE_EXTENSION_ACCEPTED_STATUSES = frozenset({"EF Ext Accepted"})

_CLIENT_STATUS_ORDER = (
    "PENDING INTAKE",
    "PROCESSING",
    "HOLD",
    "FINALIZE",
    "PICKUP",
    "EFILE READY",
    "EFILE",
    "LOG OUT",
)


def client_status_at_least_processing(status: str | None) -> bool:
    st = (status or "").strip().upper()
    if not st:
        return False
    try:
        idx = _CLIENT_STATUS_ORDER.index(st)
        min_idx = _CLIENT_STATUS_ORDER.index("PROCESSING")
    except ValueError:
        return False
    return idx >= min_idx


def engagement_status_violation(
    engagement_status: str | None,
    client_status: str | None,
) -> str | None:
    """Return violation code if TaxOps status breaks engagement rules, else None."""
    eng = (engagement_status or "").strip()
    if eng == ENGAGEMENT_EFILE_ACCEPTED:
        if (client_status or "").strip().upper() != "LOG OUT":
            return "efile_accepted_not_logout"
    elif eng == ENGAGEMENT_EXTENSION_EFILE_ACCEPTED:
        if not client_status_at_least_processing(client_status):
            return "extension_accepted_before_processing"
    return None


def drake_indicates_efile_complete(drake_status_raw: str | None) -> bool:
    return (drake_status_raw or "").strip() in DRAKE_EFILE_COMPLETE_STATUSES


def drake_indicates_extension_accepted(drake_status_raw: str | None) -> bool:
    return (drake_status_raw or "").strip() in DRAKE_EXTENSION_ACCEPTED_STATUSES
