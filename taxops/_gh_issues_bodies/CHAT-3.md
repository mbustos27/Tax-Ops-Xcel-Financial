## Parent epic
Epic: Chat performance

## Depends on
Coordinate with CHAT-1 / CHAT-2 to avoid duplicate cache layers.

## Current code
SQLite-backed answer cache paths already exist (`get_cached_answer`, `set_cached_answer`, TTL env). Verify behavior vs epic requirements (TTL, invalidation hooks, `normalize_question` keyed).

Locations: **`chat_cache.py`**, exercised from `_ai_chat_submit()` in **`ai_routes.py`**.

## What to extend
- TTL / invalidation semantics after **payments**, **status edits**, **`refresh_chat_cache`**.
- Telemetry: **`/ai/status`** already surfaces extraction queue counts — add **answer cache** subsection if Product wants parity with prompt spec (`size`, `ttl_seconds`).
- Explicit `cached: true` payloads where appropriate.

## Definition of done
- Repeat question within TTL: **no** redundant Ollama work.
- Stale TTL or invalidation clears bad answers deterministically.

