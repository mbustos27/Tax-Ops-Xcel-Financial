## Parent epic
Epic: Chat performance

## Depends on
CHAT-1 (indexes/cache foundations) landed or parallel-safe.

## Existing code (`chat_cache.py`)
`classify_intent()`, `normalize_question`, and deterministic helpers feed `try_deterministic_response(...)`. Prefer **extend** regex/heuristic buckets here rather than a second classifier.

## What to build
Tight pattern routing documented in-module:
- **`count_by_status`**, **`balance_due`**, **`processor` workload**, **`search_client`**, **`financial_totals`**, etc.
- Return `(intent, entities)` tuples compatible with downstream `try_deterministic_response(...)`.

Wire order in `_ai_chat_submit()` (**`ai_routes.py`**):
```
Question → (existing cache hit) → classify_intent / try_deterministic_response
       → optional scope gate (`chat_scope_classifier.should_block_tool_router_llm`)
       → extract_json(...) tool router only when needed
```

## Definition of done
- Golden prompts hit deterministic lane without router call when classified.
- Unmatched prompts still behave as today (fallback to router).
- Tests extended for classifier edges.

