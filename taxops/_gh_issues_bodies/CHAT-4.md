## Parent epic
Epic: Chat performance

## Depends on
CHAT-1–CHAT-3 baseline.

## Current flow (`_ai_chat_submit` in **`ai_routes.py`**)
Typical latency stack:
1. `extract_json(...)` — tool router (**Ollama**)
2. `db_tools.<tool>`
3. `chat(...)` — narrative answer (**Ollama**)

Environmental toggles (`CHAT_ROUTER_TRANSPORT_FAIL_AGGREGATES`, `CHAT_ANSWER_ON_ROUTER_TIMEOUT_SKIP_LLM`) already degrade when router dies.

## What to build
When intent + tool + args resolved **deterministically** (no speculative router creativity required), skip `extract_json` entirely and annotate metadata (`telemetry` / payload field) distinguishing **deterministic-router** vs **llm-router**.

## Definition of done
- Log evidence: deterministic path → **single** downstream model call maximum (often zero when cache wins).
- No regression on ambiguous prompts where router genuinely needed.

