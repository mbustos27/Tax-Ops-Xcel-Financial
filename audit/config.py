"""Constants and expected baselines for the client audit (findings-only)."""

from __future__ import annotations

from pathlib import Path

from audit import __version__

TOOL_VERSION = __version__
AUDIT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = AUDIT_ROOT / "output"
SNAPSHOT_DIR = AUDIT_ROOT / "snapshots"

# Default source paths (operator may override via CLI).
# A0: prefer OneDrive TY2025 CSM (later Last Change; 1159 rows). Desktop is older fallback.
DEFAULT_DRAKE_PATH = Path(
    r"C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\CLIENTS.xlsx"
)
DEFAULT_TAX_LOG_PATH = Path(
    r"C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC"
    r"\Shared\Logs\TAX LOG 2025 Live.xlsx"
)
# Share path — samefile as T:\taxops\taxops.db on the workstation (A0).
# Server NSSM may use C:\TaxOps\taxops\taxops.db; that pairing is AUTHORITATIVE_DB_UNRESOLVED
# from the workstation (see audit/baseline.py).
DEFAULT_TAXOPS_DB = Path(r"\\Xcel-server\taxops\taxops\taxops.db")

# Tax Log sheet routing
SHEET_INDIVIDUALS = "XCEL 2025"
SHEET_BUSINESS = (
    "1120 CORP LIST",
    "1120 S LIST",
    "1065 & LLC LIST",
    "EXT 1120",
    "EXT 1120S, 1065'S",  # exact match may need strip; loader fuzzy-matches
)
ENTITY_DRAKE_TYPES = frozenset({"1120", "1120S", "1065", "1041", "990"})
INDIVIDUAL_DRAKE_TYPES = frozenset({"1040"})

# ---------------------------------------------------------------------------
# HISTORICAL REFERENCE ONLY (A0) — no longer preflight crash gates.
# Live drift = previous run in audit/baseline_memory.json → finding BASELINE_DRIFT.
# Jul31 investigation lock values retained for narrative comparison in reports.
# ---------------------------------------------------------------------------
HISTORICAL_DRAKE_ROWS = 1159
HISTORICAL_DRAKE_TYPES = {
    "1040": 1044,
    "1120": 55,
    "1065": 29,
    "1120S": 28,
    "1041": 2,
    "990": 1,
}
HISTORICAL_LOG_NAMED_ROWS = 1252
HISTORICAL_TAXOPS_CLIENTS = 1514
HISTORICAL_TAXOPS_RETURNS = 1748
HISTORICAL_TAXOPS_TY2025 = 1413
HISTORICAL_TAXOPS_SPOUSES = 169
HISTORICAL_TAXOPS_SCHEMA = 23

# Back-compat aliases (deprecated). Prefer HISTORICAL_* or baseline_memory.
EXPECTED_DRAKE_ROWS = HISTORICAL_DRAKE_ROWS
EXPECTED_DRAKE_TYPES = HISTORICAL_DRAKE_TYPES
EXPECTED_LOG_NAMED_ROWS = HISTORICAL_LOG_NAMED_ROWS
EXPECTED_TAXOPS_CLIENTS = HISTORICAL_TAXOPS_CLIENTS
EXPECTED_TAXOPS_RETURNS = HISTORICAL_TAXOPS_RETURNS
EXPECTED_TAXOPS_TY2025 = HISTORICAL_TAXOPS_TY2025
EXPECTED_TAXOPS_SPOUSES = HISTORICAL_TAXOPS_SPOUSES
EXPECTED_TAXOPS_SCHEMA = HISTORICAL_TAXOPS_SCHEMA

# Log layout
LOG_DATA_START_ROW = 6  # 1-based
LOG_LAST_COL = 2  # 0-based C
LOG_FIRST_COL = 3  # 0-based D
LOG_YR_COL = 4  # 0-based E (header on row 3)

PRIMARY_TAX_YEAR = 2025
