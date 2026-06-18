## Parent epic
Technical debt epic

## Problem
Per-row enrichment / lookups can multiply DB roundtrips during large dashboard pages.

## Fix
Batch loaders (`forms_for_returns`, payment summaries keyed by `return_id`), then map locally.

## Definition of done
- EXPLAIN/trace shows fewer statements per dashboard render on large corpuses OR measured latency improvement
- python -m pytest tests/ -v passes
