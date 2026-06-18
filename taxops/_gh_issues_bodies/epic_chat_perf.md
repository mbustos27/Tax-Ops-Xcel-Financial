## Goal
The current /ai/chat route makes 2 LLM calls per question. For common staff questions like 'how many returns are in PROCESSING?' the LLM is unnecessary — the answer is often a deterministic path. This epic pushes more traffic to **instant** answers (`try_deterministic_response`, `chat_cache.ensure_chat_cache_for_year`, and related helpers wired from `POST /ai/chat` in `ai_routes.py` alongside `CHAT_TOOLS` / `CHAT_TOOL_ALLOWLIST`).

## Current code touchpoints (read before changing)
- `ai_routes.py`: `POST /ai/chat` → `_ai_chat_submit()`, `extract_json(...)` router, `chat(...)` answer path.
- Existing `chat_cache.py` already holds intent routing, KPI snapshot material, deterministic fast paths — extend rather than reinvent.

## Current state (typical LAN)
- Tool selection (`extract_json`): often tens of seconds on remote Ollama.
- Answer generation (`chat`): another chunk of latency unless short-circuit or cache wins.

## Goal state
- Count/status-class questions: sub-100ms where classified + cache allows.
- Repeated questions: answer cache hit (already partially implemented — extend/TTL strategy per team).
- Complex/novel questions: minimize redundant router work where safe.

## Child issues
- CHAT-1: In-memory hash tables and stats cache
- CHAT-2: Intent classifier — regex pattern routing
- CHAT-3: Answer cache with TTL
- CHAT-4: Reduce to single LLM call for complex questions

## Non-goals
- No external cache (Redis, Memcached) — prefer in-process + existing SQLite patterns where already used.
- No schema changes unless a child issue explicitly adds one.
- No new pip dependencies beyond team approval.

