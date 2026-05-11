from pathlib import Path

# All paths are absolute, anchored to the taxops/ directory itself.
# This ensures the importer works regardless of which directory you run
# `python main.py` from.
import os
from collections.abc import MutableMapping

_HERE = Path(__file__).parent

# Environment keys already present before any `.env` merge (NSSM service, user profile, systemd, etc.).
# Repo-root `.env` uses `setdefault` against this snapshot (never overrides NSSM exports).
# `taxops/.env` then overlays defaults for keys that were NOT in this snapshot — so deployment-specific
# Ollama/DB overrides in `taxops/.env` can win over a repo-root `.env` template containing localhost.
_DOTENV_READONLY_KEYS = frozenset(os.environ.keys())


def _normalize_env_key(key: str) -> str:
    """Strip UTF-8 BOM / accidental leading ':' (NSSM copy-paste typos)."""
    k = key.strip().lstrip("\ufeff")
    if len(k) > 1 and k[0] == ":" and (k[1].isalpha() or k[1] == "_"):
        k = k[1:]
    return k


def _apply_env_file(path: Path) -> None:
    """Load KEY=VALUE from `.env`; never overrides vars already set in the OS/process."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return
    raw = raw.lstrip("\ufeff")
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.lower().startswith("export "):
            s = s[7:].strip()
        if "=" not in s:
            continue
        key, _, val = s.partition("=")
        key = _normalize_env_key(key)
        val = val.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, val)


def _apply_env_file_overlay(path: Path) -> None:
    """Load `.env`; override vars from repo-root `.env` / builtins but never NSSM/OS process exports.

    Call after :func:`_apply_env_file` on the repo root so ``taxops/.env`` wins for deployments.
    Keys in :data:`_DOTENV_READONLY_KEYS` are left unchanged.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return
    raw = raw.lstrip("\ufeff")
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.lower().startswith("export "):
            s = s[7:].strip()
        if "=" not in s:
            continue
        key, _, val = s.partition("=")
        key = _normalize_env_key(key)
        val = val.strip().strip('"').strip("'")
        if key and key not in _DOTENV_READONLY_KEYS:
            os.environ[key] = val


def _migrate_leading_colon_process_env_inplace(environ: MutableMapping[str, str]) -> None:
    """If NSSM/registry stored ':VAR=value', copy into VAR when VAR is not already set."""
    extra: dict[str, str] = {}
    for k, v in environ.items():
        if not isinstance(k, str) or len(k) <= 1 or k[0] != ":":
            continue
        if not (k[1].isalpha() or k[1] == "_"):
            continue
        canon = k[1:]
        if canon and canon not in environ:
            extra[canon] = v if isinstance(v, str) else str(v)
    if extra:
        environ.update(extra)


def _migrate_leading_colon_process_env() -> None:
    _migrate_leading_colon_process_env_inplace(os.environ)


# Repo-root `.env` first (shared defaults); `taxops/.env` overlays for keys not from OS/NSSM.
_apply_env_file(_HERE.parent / ".env")
_apply_env_file_overlay(_HERE / ".env")
_migrate_leading_colon_process_env()

# Override DB path via env var — used for demo mode
DB_PATH       = os.environ.get("TAXOPS_DB") or str(_HERE / "taxops.db")
INCOMING_DIR  = str(_HERE / "data" / "incoming")
PROCESSED_DIR = str(_HERE / "data" / "processed")
ERROR_DIR     = str(_HERE / "data" / "error")

# "demo" shows a banner in the UI; anything else is production
APP_ENV = os.environ.get("TAXOPS_ENV", "production").lower()

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

# Default Ollama tag for any code path that does not pick a model explicitly
# (document extraction JSON, mail watcher, draft-email, IRS code JSON fallback).
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2")

# POST /ai/chat only — separate models so you can use a smaller/faster router and a stronger writer.
OLLAMA_ROUTER_MODEL = os.environ.get("OLLAMA_ROUTER_MODEL") or OLLAMA_MODEL
OLLAMA_CHAT_MODEL = os.environ.get("OLLAMA_CHAT_MODEL") or OLLAMA_MODEL

OLLAMA_CHAT_ROUTER_TIMEOUT_SEC = int(os.environ.get("OLLAMA_CHAT_ROUTER_TIMEOUT", "30"))
OLLAMA_CHAT_ANSWER_TIMEOUT_SEC = int(os.environ.get("OLLAMA_CHAT_ANSWER_TIMEOUT", "60"))
# Planner + confidence abstain — below this, row tools cleared (aggregates-first path).
_CHAT_CONF = os.environ.get("CHAT_ROUTER_CONFIDENCE_MIN", "0.55").strip()
try:
    CHAT_ROUTER_CONFIDENCE_MIN = float(_CHAT_CONF)
except ValueError:
    CHAT_ROUTER_CONFIDENCE_MIN = 0.55
CHAT_ROUTER_CONFIDENCE_MIN = max(0.0, min(1.0, CHAT_ROUTER_CONFIDENCE_MIN))
# Router /api/generate transport failure → continue with aggregates-only path (deterministic KPIs +
# dataplane markdown) instead of returning HTTP 503. Set "false" to keep strict failures.
_CHAT_RTF_AGG = os.environ.get("CHAT_ROUTER_TRANSPORT_FAIL_AGGREGATES", "true").lower()
CHAT_ROUTER_TRANSPORT_FAIL_AGGREGATES = _CHAT_RTF_AGG in ("true", "1", "yes", "on")
# After router degradation, skip the answer LLM and return structured roll-up (+ optional snapshot markdown).
_CHAT_DEGRADED_SKIP_LLM = os.environ.get("CHAT_ANSWER_ON_ROUTER_TIMEOUT_SKIP_LLM", "true").lower()
CHAT_ANSWER_ON_ROUTER_TIMEOUT_SKIP_LLM = _CHAT_DEGRADED_SKIP_LLM in ("true", "1", "yes", "on")

# Successful /ai/chat answers → SQLite table for repeat lookup across restarts (TTL hours).
_AI_CHAT_DISK_H = os.environ.get("AI_CHAT_DISK_CACHE_HOURS", "24").strip()
AI_CHAT_DISK_CACHE_ENABLE = os.environ.get("AI_CHAT_DISK_CACHE_ENABLE", "true").lower() == "true"
try:
    AI_CHAT_DISK_CACHE_HOURS = float(_AI_CHAT_DISK_H)
except ValueError:
    AI_CHAT_DISK_CACHE_HOURS = 24.0

# Optional: append staff /ai/chat questions to JSONL for export → chat_scope_examples.tsv training.
# Logs may include client names — use only on trusted machines; default path is gitignored.
CHAT_TRAINING_LOG_ENABLE = os.environ.get("CHAT_TRAINING_LOG_ENABLE", "false").lower() == "true"
CHAT_TRAINING_LOG_CACHE_HITS = os.environ.get("CHAT_TRAINING_LOG_CACHE_HITS", "false").lower() == "true"
CHAT_TRAINING_LOG_PATH = Path(
    os.environ.get("CHAT_TRAINING_LOG_PATH", str(_HERE / "data" / "ai_chat_staff_queries.jsonl"))
)

# Document extraction — model tags must exist on `ollama list` (see `.env.example`).
OLLAMA_EXTRACT_MODEL_TEXT = os.environ.get(
    "OLLAMA_EXTRACT_MODEL_TEXT",
) or OLLAMA_MODEL
OLLAMA_EXTRACT_MODEL_VISION = os.environ.get(
    "OLLAMA_EXTRACT_MODEL_VISION",
    "llama3.2-vision",
)

# Document extraction (long prompts + slow local models). Read timeout = full generate time.
OLLAMA_EXTRACT_TIMEOUT_TEXT = int(
    os.environ.get("OLLAMA_EXTRACT_TIMEOUT_TEXT", "90")
)
OLLAMA_EXTRACT_TIMEOUT_VISION = int(
    os.environ.get("OLLAMA_EXTRACT_TIMEOUT_VISION", "240")
)

DOCUMENTS_BASE_PATH = os.environ.get("DOCUMENTS_BASE_PATH", str(_HERE / "documents"))

# DOC-6 — Drake Documents staging root on this machine (Working/Archive Cabinet data path).
# Blank = POST /api/return/<id>/sync-to-drake returns 400. File copy only; no Drake API.
DRAKE_DOCUMENTS_PATH = (os.environ.get("DRAKE_DOCUMENTS_PATH") or "").strip()

# Email watcher (IMAP) — leave IMAP_HOST blank to disable
IMAP_HOST          = os.environ.get("IMAP_HOST", "")
IMAP_PORT          = int(os.environ.get("IMAP_PORT", 993))
IMAP_USER          = os.environ.get("IMAP_USER", "")
IMAP_PASS          = os.environ.get("IMAP_PASS", "")
IMAP_POLL_INTERVAL = int(os.environ.get("IMAP_POLL_INTERVAL", 120))
IMAP_FOLDER        = os.environ.get("IMAP_FOLDER", "INBOX")
# Set to true to log what would be marked as read without touching Gmail
IMAP_DRY_RUN: bool = os.environ.get("IMAP_DRY_RUN", "false").lower() == "true"
# When false, mail watcher never adds \\Seen — emails stay unread in the mailbox (still processes).
IMAP_MARK_AS_READ: bool = os.environ.get("IMAP_MARK_AS_READ", "true").lower() == "true"

# Ollama HTTP timeout for mail-watcher LLM calls (name extraction, domain classify)
MAIL_WATCHER_LLM_TIMEOUT = int(os.environ.get("MAIL_WATCHER_LLM_TIMEOUT", "45"))

# Fuzzy client match minimum for routing email attachments (name_matcher ACCEPT_THRESHOLD is 88).
# Lower values attach more aggressively — verify Office tolerance before lowering below ~80.
MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE = int(os.environ.get("MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE", "82"))

# Gmail exposes its tab categories as IMAP folders.
# Each folder is mapped to a handling strategy:
#   'full_processing' — classify + attach (real clients may live here)
#   'skip'            — Gmail already knows it's promotional; mark as seen, zero LLM calls
# These are checked in order during each poll cycle.
GMAIL_CATEGORY_FOLDERS: dict = {
    # INBOX already contains personal-tab messages in Gmail IMAP — do NOT add
    # CATEGORY_PERSONAL here; it is a mirror of INBOX and causes every personal
    # email to be fetched and classified twice.
    "INBOX":                "full_processing",   # everything, including personal tab
    "CATEGORY_PROMOTIONS":  "skip",              # Gmail already sorted this
    "CATEGORY_UPDATES":     "skip",              # automated updates
    "CATEGORY_FORUMS":      "skip",              # mailing lists
    "CATEGORY_SOCIAL":      "skip",              # social notifications
}

# Set to False for non-Gmail IMAP servers (Exchange, Fastmail, etc.)
USE_GMAIL_CATEGORIES: bool = os.environ.get("USE_GMAIL_CATEGORIES", "true").lower() == "true"

# ── Email classification helpers ──────────────────────────────────────────────
# Domains that are always promotional for a tax office — never client documents.
# Checked before the LLM is called to save time and improve accuracy.
KNOWN_PROMOTIONAL_DOMAINS: frozenset = frozenset({
    # Tax-software and payroll vendors
    "adp.com",
    "intuit.com",
    "drakesoftware.com",
    "gruntworx.com",
    "pdffiller.com",
    "myfilingservices.com",
    "countingworkspro.com",
    "irstaxforum.com",
    "nelcosolutions.com",
    "minespress.com",
    "inktechnologies.com",
    "procrm.ai",
    # Software / SaaS
    "adobe.com",
    "canva.com",
    "microsoft.com",
    "udemy.com",
    "avery.com",
    "ccsend.com",
    "sevengrav.com",
    "tminetwork.com",
    # Shopping / retail
    "poshmark.com",
    "airbnb.com",
    "amazon.com",
    "officedepot.com",
    "pens.com",
    "hertz.com",
    "mycheapoair.com",
    "redvinebc.com",
    "truckers-insurance.com",
    "lifetouch.com",
    # Financial institutions
    "bankofamerica.com",
    "chase.com",
    "americanexpress.com",
    "scif.com",
    # Government / regulatory bodies (newsletters and bulletins only — not direct notices)
    "ca.gov",          # covers cdtfa.ca.gov, doj.ca.gov, state.ca.gov, ftb.ca.gov, etc.
    "sba.gov",
    "lacity.org",
    "lasvbbb.org",
    # Professional associations
    "csea.org",
    "nationalnotary.org",
    "cpshr.us",
    # Real estate / business
    "compass.com",
    "lendistry.com",
    "calsavers.com",
    "spectrumemails.com",
    "stthom.edu",
})

# Subdomain prefixes that indicate mass-mailing infrastructure — always promotional.
MASS_MAILING_PREFIXES: tuple = (
    "em.",
    "e.",
    "email.",
    "mails.",
    "engage.",
    "mail.",
    "info.",
    "news.",
    "newsletter.",
    "noreply.",
    "notifications.",
    "marketing.",
    "offers.",
    "deals.",
    "promo.",
    "updates.",
    "clientfeedback.",
    "welcome.",
    "mcmap.",
    "p.",
    "paymentsolutions.",
    "shared1.",
    "listserv.",
)

# Personal / consumer email domains — these senders are almost always real clients.
# Emails from these domains must NEVER be auto-classified as promotional;
# they always go to the LLM for proper classification.
# xcelfinancial.com is included so internal staff emails get full LLM treatment.
PERSONAL_EMAIL_DOMAINS: frozenset = frozenset({
    "gmail.com",
    "yahoo.com",
    "icloud.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "live.com",
    "me.com",
    "mac.com",
    "protonmail.com",
    "ymail.com",
    "att.net",
    "comcast.net",
    "sbcglobal.net",
    "verizon.net",
    "xcelfinancial.com",
})

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
    # Drake "ready/printed" — prep done, client needs to sign before efiling
    "READY TO FILE":                "PICKUP",
    "READY TO PRINT":               "PICKUP",
    "PRINTED":                      "PICKUP",
    # Cleared to transmit (client signed)
    "READY TO EFILE":               "EFILE READY",
    # Transmitted, awaiting ack
    "E-FILED":                      "EFILE READY",
    "EFILED":                       "EFILE READY",
    # Acknowledged / accepted → case closed
    "EF ACCEPTED":                  "LOG OUT",
    "EF EXT ACCEPTED":              "LOG OUT",
    "EF ACCEPTED - STATE ONLY":     "LOG OUT",
    "EF ACCEPTED STATE ONLY":       "LOG OUT",
    "EF ACCEPT":                    "LOG OUT",
    "ACCEPTED":                     "LOG OUT",
    # Complete / mailed
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
