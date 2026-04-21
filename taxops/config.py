from pathlib import Path

# All paths are absolute, anchored to the taxops/ directory itself.
# This ensures the importer works regardless of which directory you run
# `python main.py` from.
_HERE = Path(__file__).parent

DB_PATH      = str(_HERE / "taxops.db")
INCOMING_DIR = str(_HERE / "data" / "incoming")
PROCESSED_DIR = str(_HERE / "data" / "processed")
ERROR_DIR    = str(_HERE / "data" / "error")

MANUAL_LOG_SOURCE = "MANUAL_LOG_IMPORT"
DRAKE_SOURCE = "DRAKE_IMPORT"
CSMDATA_SOURCE = "CSMDATA_IMPORT"

# Drake / CSM status → internal workflow status
# Keys must match the uppercased value from the Status column exactly.
DRAKE_STATUS_MAP: dict[str, str] = {
    # Generic in-progress states
    "IN PROGRESS":                  "PROCESSING",
    "WAITING ON INFO":              "PROCESSING",
    "WAITING FOR INFO":             "PROCESSING",
    "HOLD":                         "PROCESSING",
    "ON EXTENSION":                 "PROCESSING",
    "EF EXTENSION":                 "PROCESSING",
    "EXTENSION":                    "PROCESSING",
    "EF REJECTED":                  "PROCESSING",
    "EF REJECT":                    "PROCESSING",
    # Finalized / printed
    "READY TO FILE":                "FINALIZE",
    "READY TO PRINT":               "FINALIZE",
    "PRINTED":                      "FINALIZE",
    # E-filed / acknowledged (CSM uses these exact strings)
    "E-FILED":                      "EFILE",
    "EFILED":                       "EFILE",
    "EF ACCEPTED":                  "EFILE",
    "EF EXT ACCEPTED":              "EFILE",
    "EF ACCEPTED - STATE ONLY":     "EFILE",
    "EF ACCEPTED STATE ONLY":       "EFILE",
    "EF ACCEPT":                    "EFILE",
    "ACCEPTED":                     "EFILE",
    # Complete / mailed / picked up
    "MAILED":                       "LOG OUT",
    "COMPLETE":                     "LOG OUT",
    "COMPLETED":                    "LOG OUT",
}

# Drake return type code → return_forms boolean flags
DRAKE_TYPE_FORMS: dict[str, dict[str, int]] = {
    "1040":    {"form_1040": 1},
    "1040SR":  {"form_1040": 1},
    "1040-SR": {"form_1040": 1},
    "1120":    {"form_1120": 1},
    "1120S":   {"form_1120s": 1},
    "1065":    {"form_1065_llc": 1},
    "990":     {"form_990_1041": 1},
    "1041":    {"form_990_1041": 1},
}

EXPECTED_HEADERS = [
    "LOG 2025",
    "LAST",
    "FIRST",
    "TAX PAYER NAME (S)",
    "YR",
    "PROCESSOR",
    "VERIFIED",
    "CLIENT STATUS",
    "INT'D",
    "25 TRANSF",
    "26 TRANSF",
    "EMAIL",
    "DATE EMAILED",
    "PICK UP",
    "LOG OUT",
    "TOTAL FEE",
    "RECEIPT #",
    "FEE PAID",
    "CC Fee",
    "Zelle or CK #",
    "Cash, Q Pay",
    "1040",
    "SCH A & D",
    "SCHED C",
    "SCHED E",
    "1120",
    "1120S",
    "1065/LLC",
    "Corp Officer",
    "Bus Owner",
    "1040X",
    "W7",
    "990/1041",
    "EXT",
    "TRANSFER",
    "UPDATED",
    "NOTES",
    "Referral",
    "Referred By",
]
