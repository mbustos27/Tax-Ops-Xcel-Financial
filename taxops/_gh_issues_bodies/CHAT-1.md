## Parent epic
Epic: Chat performance — eliminate LLM calls for common questions

## Depends on
Coordinated with merged LLM-6 `/ai/chat` work (`ai_routes.py`, `POST /ai/chat`, `CHAT_TOOL_ALLOWLIST`).

## What to build / extend
**Note:** `taxops/chat_cache.py` already exists (refresh snapshots, KPI material, deterministic routing). Extend it — do not blindly replace wholesale.

Implement or strengthen **structured in-process indexes** rebuilt on a cadence:

- **`STATUS_COUNTS`**-style aggregates (often sourced from refreshed cache / `get_system_context`).
- **`PROCESSOR`/preparer rollup** maps for workload questions.
- **Financial roll-ups** coherent with dashboard math.
- **`BALANCE_DUE_RETURN_IDS` or equivalent** membership only if Privacy rules satisfied (never store/export `ssn_last4`; no new PII vectors).

Suggested API shapes (adapt to existing module layout):
```python
def build_cache(...)  # startup + periodic
def refresh_cache(...)  # on-demand/admin
def get_status_count(status: str) -> int  # backed by aggregates
```

## Privacy rules (non-negotiable)
- Matches `.cursor/rules.md`: no **full** SSN; treat `ssn_last4` with extreme care — not in caches exposed to prompts/exports/logs.
- In-process aggregates only unless product decision says otherwise.

## Wire into routing
Before `extract_json(...)` router in `_ai_chat_submit()`, exploit `classify_intent`, `try_deterministic_response`, and snapshot helpers already imported from `chat_cache.py`.

## Definition of done
- Measurable reduction in `/ai/chat` Ollama round-trips on golden questions.
- `python -m pytest tests/ -v` green.
- `/ai/status` reports useful cache freshness if applicable.

