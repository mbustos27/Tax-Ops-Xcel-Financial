from pathlib import Path

# All paths are absolute, anchored to the taxops/ directory itself.
# This ensures the importer works regardless of which directory you run
# `python main.py` from.
import logging
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

APP_NAME = "Tax Log"

# Staff who conduct client interviews at intake.
# Order is displayed as-is in the dropdown.
INTERVIEWERS: list[str] = ["Marlin", "Lorena", "Armida", "Sandra", "Lucy", "Moises"]

# "demo" shows a banner in the UI; anything else is production
APP_ENV = os.environ.get("TAXOPS_ENV", "production").lower()

# PROD-2 — rotating JSON logs + level (see logging_config.configure_logging).
_ll = os.environ.get("TAXOPS_LOG_LEVEL", "INFO").strip().upper()
LOG_LEVEL_STR = _ll if _ll in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL") else "INFO"
LOG_LEVEL_INT = getattr(logging, LOG_LEVEL_STR, logging.INFO)

_log_json_raw = (os.environ.get("TAXOPS_LOG_JSON_PATH") or "").strip()
LOG_JSON_PATH = Path(_log_json_raw) if _log_json_raw else None

try:
    _log_mb = int(os.environ.get("TAXOPS_LOG_JSON_MAX_MB", "50"))
except ValueError:
    _log_mb = 50
LOG_JSON_MAX_BYTES = max(1, _log_mb) * 1024 * 1024

try:
    LOG_JSON_BACKUP_COUNT = int(os.environ.get("TAXOPS_LOG_JSON_BACKUPS", "10"))
except ValueError:
    LOG_JSON_BACKUP_COUNT = 10
LOG_JSON_BACKUP_COUNT = max(0, LOG_JSON_BACKUP_COUNT)

_lc = os.environ.get("TAXOPS_LOG_CONSOLE", "true").lower()
LOG_CONSOLE_ENABLED = _lc in ("true", "1", "yes", "on")

# PROD-3 — GET /health version string (CI/NSSM can set explicit release label).
_RELEASE_VERSION_CACHED: str | None = None


def taxops_release_version() -> str:
    """``TAXOPS_VERSION`` overrides; otherwise short git SHA when ``.git`` exists; else ``unknown``."""
    global _RELEASE_VERSION_CACHED
    if _RELEASE_VERSION_CACHED is not None:
        return _RELEASE_VERSION_CACHED
    tagged = os.environ.get("TAXOPS_VERSION", "").strip()
    if tagged:
        _RELEASE_VERSION_CACHED = tagged
        return _RELEASE_VERSION_CACHED
    if not (_HERE.parent / ".git").exists():
        _RELEASE_VERSION_CACHED = "unknown"
        return _RELEASE_VERSION_CACHED
    import subprocess

    try:
        proc = subprocess.run(
            ["git", "-C", str(_HERE.parent), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        out = proc.stdout.strip() if proc.stdout else ""
        _RELEASE_VERSION_CACHED = out if proc.returncode == 0 and out else "unknown"
    except (OSError, subprocess.TimeoutExpired):
        _RELEASE_VERSION_CACHED = "unknown"
    return _RELEASE_VERSION_CACHED


def taxops_asset_cache_version() -> str:
    """Token for ``?v=`` on static JS/CSS URLs (CACHE bust / GitHub #141).

    Precedence for the base label:

    1. ``TAXOPS_APP_VERSION`` — bump this on each deploy when shipping static-only
       changes without changing ``TAXOPS_VERSION`` or git revision.
    2. ``taxops_release_version()`` — ``TAXOPS_VERSION`` env, else short git SHA.

    Always suffix the max mtime of bundled static files so uncommitted edits to
    ``app.js`` / ``app.css`` still change ``?v=`` (otherwise the button HTML can
    update while the browser keeps a stale handler-less ``app.js``).
    """
    tag = os.environ.get("TAXOPS_APP_VERSION", "").strip()
    base = tag or taxops_release_version()
    try:
        mt = 0
        for rel in ("static/app.js", "static/app.css", "static/tw.min.css"):
            p = _HERE / rel
            if p.is_file():
                mt = max(mt, p.stat().st_mtime_ns)
        if mt:
            return f"m{mt}" if not base or base == "unknown" else f"{base}-m{mt}"
    except OSError:
        pass
    return base if base and base != "unknown" else "0"


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
    os.environ.get("OLLAMA_EXTRACT_TIMEOUT_VISION", "180")
)

DOCUMENTS_BASE_PATH = os.environ.get("DOCUMENTS_BASE_PATH", str(_HERE / "documents"))
EMAIL_INBOX_DIR = os.environ.get(
    "EMAIL_INBOX_DIR",
    str(_HERE / "documents" / "email_inbox")
)

# MULTIYEAR-3 — YoY highlight thresholds for GET /api/clients/<id>/years
def _mf_env(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


MULTIYEAR_AGI_PERCENT_THRESHOLD = _mf_env("TAXOPS_MULTIYEAR_AGI_PCT", "10")
MULTIYEAR_REFUND_ABS_THRESHOLD = _mf_env("TAXOPS_MULTIYEAR_REFUND_ABS", "500")
MULTIYEAR_BALANCE_ABS_THRESHOLD = _mf_env("TAXOPS_MULTIYEAR_BALANCE_ABS", "500")

# DOC-6 — Drake Documents staging root on this machine (Working/Archive Cabinet data path).
# Blank = POST /api/return/<id>/sync-to-drake returns 400. File copy only; no Drake API.
DRAKE_DOCUMENTS_PATH = (os.environ.get("DRAKE_DOCUMENTS_PATH") or "").strip()

# Drake Documents folder structure prep — DOC-6 (mirror TaxYear / LastName_ReturnID for future sync).
# Separate from ``DRAKE_DOCUMENTS_PATH`` (bulk staging + manifest copy).
DRAKE_DOCUMENTS_BASE = (os.environ.get("DRAKE_DOCUMENTS_BASE") or "").strip()
_DRAKE_FOLDER_EN = os.environ.get("DRAKE_FOLDER_STRUCTURE_ENABLED", "false").lower()
DRAKE_FOLDER_STRUCTURE_ENABLED = _DRAKE_FOLDER_EN == "true"

# Email watcher (IMAP) — leave IMAP_HOST blank to disable
IMAP_HOST          = os.environ.get("IMAP_HOST", "")
IMAP_PORT          = int(os.environ.get("IMAP_PORT", 993))
IMAP_USER          = os.environ.get("IMAP_USER", "")
IMAP_PASS          = os.environ.get("IMAP_PASS", "")
IMAP_POLL_INTERVAL = int(os.environ.get("IMAP_POLL_INTERVAL", 120))
IMAP_FOLDER        = os.environ.get("IMAP_FOLDER", "INBOX")

# Comma-separated list of folders to poll when USE_GMAIL_CATEGORIES is false.
# Folder names with spaces are supported (e.g. "TAX DOCUMENTS FROM CLIENTS").
IMAP_FOLDERS: list[str] = [
    f.strip()
    for f in os.environ.get("IMAP_FOLDERS", IMAP_FOLDER).split(",")
    if f.strip()
]

# Domain(s) the office sends from — emails arriving from these are self-sent.
# Auto-derived from IMAP_USER; extend with OWN_EMAIL_DOMAINS env var (comma-separated).
_imap_own_domain   = IMAP_USER.split("@")[-1].lower() if "@" in IMAP_USER else ""
_extra_own         = os.environ.get("OWN_EMAIL_DOMAINS", "")
OWN_EMAIL_DOMAINS: frozenset = frozenset(
    d.strip().lower()
    for d in ([_imap_own_domain] + _extra_own.split(","))
    if d.strip()
)
# Dry-run: log what the mail watcher would do without touching any IMAP state.
IMAP_DRY_RUN: bool = os.environ.get("IMAP_DRY_RUN", "false").lower() == "true"
# POLICY: TaxOps never sets or clears \Seen on any message.  IMAP_MARK_AS_READ
# and IMAP_PRESERVE_UNREAD are retained for config-file backward compatibility
# but have no effect — _mark_read() is never called anywhere in the codebase.
IMAP_MARK_AS_READ: bool = os.environ.get("IMAP_MARK_AS_READ", "true").lower() == "true"
# Part 5: max consecutive retry attempts before an email is permanently skipped.
IMAP_MAX_RETRIES: int = int(os.environ.get("IMAP_MAX_RETRIES", "3"))
# Retained for backward compat — no longer gates any behavior.  DB log dedup
# (email_processing_log) runs unconditionally on every poll.
IMAP_PRESERVE_UNREAD: bool = os.environ.get("IMAP_PRESERVE_UNREAD", "false").lower() == "true"

# Ollama HTTP timeout for mail-watcher LLM calls (name extraction, domain classify)
MAIL_WATCHER_LLM_TIMEOUT = int(os.environ.get("MAIL_WATCHER_LLM_TIMEOUT", "45"))

# Fuzzy client match minimum for routing email attachments (name_matcher ACCEPT_THRESHOLD is 88).
# Lower values attach more aggressively — verify Office tolerance before lowering below ~80.
MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE = int(os.environ.get("MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE", "82"))

# When false (default): image files (.jpg, .jpeg, .png) skip the vision model entirely.
# Images are saved to the return and staff tag them manually.
# Set true only when Ollama has enough resources for concurrent vision requests.
EXTRACTOR_VISION_ENABLED: bool = os.environ.get("EXTRACTOR_VISION_ENABLED", "false").lower() == "true"

# Scan Agent (reception workstation WIA → PDF). Browser calls this host; TaxOps
# server does not need the agent, but templates use these defaults for the UI.
SCAN_AGENT_URL: str = (os.environ.get("SCAN_AGENT_URL") or "http://127.0.0.1:8766").rstrip("/")
SCAN_AGENT_TOKEN: str = os.environ.get("SCAN_AGENT_TOKEN", "")

# ── Claude OCR (scan-agent documents only) — PunchBridge-ported pattern ──────
ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")
# Model strings verified against docs.claude.com (same family as PunchBridge).
CLAUDE_OCR_MODEL_FAST: str = os.environ.get("CLAUDE_OCR_MODEL_FAST", "claude-haiku-4-5")
CLAUDE_OCR_MODEL_READ: str = os.environ.get("CLAUDE_OCR_MODEL_READ", "claude-sonnet-5")
CLAUDE_OCR_MODEL_HARD: str = os.environ.get("CLAUDE_OCR_MODEL_HARD", "claude-opus-4-8")
# Escalate Haiku → Sonnet when confidence is below this (or missing).
try:
    CLAUDE_OCR_ESCALATE_THRESHOLD: float = float(
        os.environ.get("CLAUDE_OCR_ESCALATE_THRESHOLD", "0.85")
    )
except ValueError:
    CLAUDE_OCR_ESCALATE_THRESHOLD = 0.85
# Escalate Sonnet → Opus below this (PunchBridge HARD_REREAD_THRESHOLD default).
try:
    CLAUDE_OCR_HARD_REREAD_THRESHOLD: float = float(
        os.environ.get("CLAUDE_OCR_HARD_REREAD_THRESHOLD", "0.75")
    )
except ValueError:
    CLAUDE_OCR_HARD_REREAD_THRESHOLD = 0.75

# EMAIL-7: matches with score >= MIN_SCORE but < LOW_CONF_THRESHOLD go to pending_review
# instead of auto-attaching; staff confirms/rejects from Email Review → Pending Review.
MAIL_LOW_CONF_THRESHOLD = int(os.environ.get("MAIL_LOW_CONF_THRESHOLD", "88"))

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

OFFICE_PHONE: str = os.environ.get("OFFICE_PHONE", "")

# Outbound client email (mass campaigns) — Prompt J placeholders; wired in Prompt L.
# Leave SMTP_HOST blank until ops configures send method (see docs/investigations/mass-email-send-method.md).
SMTP_HOST: str = os.environ.get("SMTP_HOST", "")
SMTP_PORT: int = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER: str = os.environ.get("SMTP_USER", "")
SMTP_PASS: str = os.environ.get("SMTP_PASS", "")
SMTP_FROM: str = os.environ.get("SMTP_FROM", "")
SMTP_USE_TLS: bool = os.environ.get("SMTP_USE_TLS", "true").lower() == "true"
# Default true until human completes test-mode send (Prompt L).
SMTP_DRY_RUN: bool = os.environ.get("SMTP_DRY_RUN", "true").lower() == "true"
# Second gate: must be explicitly true before any SMTP socket is opened (tests stay dry).
EMAIL_CAMPAIGN_ALLOW_SMTP: bool = (
    os.environ.get("EMAIL_CAMPAIGN_ALLOW_SMTP", "false").lower() == "true"
)
# Live sends per second cap when throttling (Prompt L / Workspace limits).
EMAIL_CAMPAIGN_SEND_DELAY_SEC: float = float(
    os.environ.get("EMAIL_CAMPAIGN_SEND_DELAY_SEC", "1.0")
)

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

# Client business domains — unique domains that belong to real clients (e.g. a
# client's own company). Used by email_suggest.py's "domain_hint" suggestion:
# an inbox item from one of these domains is suggested to the single client
# who has an email on file at that domain (ambiguous if more than one).
#
# Populated via IMAP_CLIENT_HINT_DOMAINS env var (comma-separated).
# Example: IMAP_CLIENT_HINT_DOMAINS=olavictory.org,coronabrosinstall.com,orealtyllc.com
CLIENT_HINT_DOMAINS: frozenset = frozenset(
    d.strip().lower()
    for d in os.environ.get("IMAP_CLIENT_HINT_DOMAINS", "").split(",")
    if d.strip()
)

# Asymmetric classifier confidence thresholds.
# The penalty for a false-promotional classification (silently dropping a client
# email) is much worse than a false-client classification (staff reviews an extra
# email).  Promotional therefore requires higher confidence before being accepted.
# Below either threshold the result is discarded and the email falls through to LLM.
CLASSIFIER_PROMOTIONAL_THRESHOLD: float = float(
    os.environ.get("CLASSIFIER_PROMOTIONAL_THRESHOLD", "0.80")
)
CLASSIFIER_CLIENT_THRESHOLD: float = float(
    os.environ.get("CLASSIFIER_CLIENT_THRESHOLD", "0.65")
)

# ── INTAKE-8: Auto-discount for new client intakes ───────────────────────────
# Dollar amount automatically applied as discount_amount on every new intake.
# Set to 0 to disable. Configurable without a code change.
INTAKE_AUTO_DISCOUNT: int = int(os.environ.get("INTAKE_AUTO_DISCOUNT", "20"))
INTAKE_SUGGESTED_UPCHARGE_PCT: int = int(os.environ.get("INTAKE_SUGGESTED_UPCHARGE_PCT", "8"))

# ── ACCOUNTING-2: Receipt OCR → QuickBooks categorization ────────────────────
# Ollama vision model for receipt OCR (defaults to the existing extraction vision model).
ACCOUNTING_VISION_MODEL: str = (
    os.environ.get("OLLAMA_VISION_MODEL") or os.environ.get("OLLAMA_EXTRACT_MODEL_VISION", "llama3.2-vision")
)
ACCOUNTING_OCR_TIMEOUT: int = int(os.environ.get("ACCOUNTING_OCR_TIMEOUT", "180"))

# QuickBooks export format: "csv" (QB Online) or "iif" (QB Desktop legacy).
QB_EXPORT_MODE: str = (os.environ.get("QB_EXPORT_MODE") or "csv").lower()

# Path to Chart of Accounts CSV (required for COA matching; optional at startup).
COA_CSV_PATH: str = (os.environ.get("COA_CSV_PATH") or "").strip()

# Path to historical transactions CSV (optional; used to seed embedding quality).
HISTORY_CSV_PATH: str = (os.environ.get("HISTORY_CSV_PATH") or "").strip()

# Embedding confidence bands: score >= HIGH → "high"; >= MEDIUM → "medium"; else "low".
def _acc_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)

ACCOUNTING_CONFIDENCE_HIGH: float   = _acc_float("ACCOUNTING_CONFIDENCE_HIGH", "0.80")
ACCOUNTING_CONFIDENCE_MEDIUM: float = _acc_float("ACCOUNTING_CONFIDENCE_MEDIUM", "0.50")

# Max retry attempts before receipt_queue item is permanently failed.
ACCOUNTING_MAX_ATTEMPTS: int = int(os.environ.get("ACCOUNTING_MAX_ATTEMPTS", "3"))

# ── RBAC ─────────────────────────────────────────────────────────────────────
ROLE_HIERARCHY: dict[str, int] = {"receptionist": 0, "preparer": 1, "admin": 2}

# Named feature-level permissions that allow targeted cross-role access.
# Each key maps to the set of roles that may exercise that feature.
# Admin and preparer are always included; receptionist gains the two expansions below.
# Add new entries here — never scatter role-string checks across routes.
ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    # Receptionist may view and work the pickup queue and e-file queue.
    "can_manage_efile_queue":      frozenset({"receptionist", "preparer", "admin"}),
    # Extension queue: preparers and admin file extensions; receptionist view-only via batch.
    "can_manage_extension_queue":  frozenset({"preparer", "admin"}),
    # Email inbox is admin-only until the workflow is fully hardened.
    "can_use_email_tools":         frozenset({"admin"}),
    # Mass-email campaigns: preview/test/live send (admin-only; Prompt L).
    "can_send_client_email":       frozenset({"admin"}),
    # Compliance Tracker: day-to-day filing period work (status updates,
    # roll-forward, correspondence notes) is preparer+admin — the actual
    # CDTFA/city filing work, not front-desk. Client/account/credential
    # CRUD stays role_required("admin") only (Compliance Tracker security
    # requirements #1-#4) — deliberately NOT a ROLE_PERMISSIONS entry, so
    # it can never be loosened by editing this dict alone.
    "can_manage_compliance_filings": frozenset({"preparer", "admin"}),
    # Intake document scanning via the reception Scan Agent.
    "can_scan_intake_docs": frozenset({"receptionist", "preparer", "admin"}),
    # Return documents: list/view/upload/tag/soft-delete (reception desk + preparers).
    # Drake sync stays separate (login + Drake flag) — not front-desk critical.
    "can_manage_return_documents": frozenset({"receptionist", "preparer", "admin"}),
    # Client profile contact fields + filing status on latest return (intake desk).
    "can_edit_client_profile":     frozenset({"receptionist", "preparer", "admin"}),
}


def _parse_taxops_users_map() -> dict[str, dict]:
    """Parse TAXOPS_USERS into a lookup dict keyed by username.

    Format: ``user:password:role;user2:password2:role2``
    Falls back to TAXOPS_USER / TAXOPS_PASS as a single admin account so
    existing single-user deployments that have not set TAXOPS_USERS continue
    to work unchanged.
    """
    raw = (os.environ.get("TAXOPS_USERS") or "").strip()
    result: dict[str, dict] = {}
    if raw:
        for entry in raw.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            parts = entry.split(":", 2)
            if len(parts) != 3:
                continue
            username, password, role = parts[0].strip(), parts[1].strip(), parts[2].strip().lower()
            if role not in ROLE_HIERARCHY:
                role = "receptionist"
            if username:
                result[username] = {"password": password, "role": role}
    if not result:
        u = (os.environ.get("TAXOPS_USER") or "").strip()
        p = (os.environ.get("TAXOPS_PASS") or "").strip()
        if u and p:
            result[u] = {"password": p, "role": "admin"}
    return result


TAXOPS_USERS_MAP: dict[str, dict] = _parse_taxops_users_map()

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
    "EF PENDING":                   "PROCESSING",
    # Prior-year carryforward — data rolled from prior season, needs work
    "UPDATED FROM 2024":            "PROCESSING",
    "UPDATED FROM 2023":            "PROCESSING",
    "UPDATED FROM 2022":            "PROCESSING",
    # Drake "ready/printed" — prep done, client needs to sign before efiling
    "READY TO FILE":                "PICKUP",
    "READY TO PRINT":               "PICKUP",
    "PRINTED":                      "PICKUP",
    # Cleared to transmit (client signed)
    "READY TO EFILE":               "EFILE READY",
    # Transmitted, awaiting ack
    "E-FILED":                      "EFILE READY",
    "EFILED":                       "EFILE READY",
    # Acknowledged / accepted → case closed (full return e-filed)
    "EF ACCEPTED":                  "LOG OUT",
    # Extension-only ack — return still open until extended due date
    "EF EXT ACCEPTED":              "PROCESSING",
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

# ── AUDIT (AUDIT-2…AUDIT-7) ─────────────────────────────────────────────────
_audit_on = (os.environ.get("AUDIT_LOGGING_ENABLED", "true") or "").lower()
AUDIT_LOGGING_ENABLED = _audit_on in ("true", "1", "yes", "on")
try:
    AUDIT_RETENTION_YEARS_DEFAULT = int(os.environ.get("AUDIT_RETENTION_YEARS", "7"))
except ValueError:
    AUDIT_RETENTION_YEARS_DEFAULT = 7
AUDIT_RETENTION_YEARS_DEFAULT = max(1, min(50, AUDIT_RETENTION_YEARS_DEFAULT))

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
