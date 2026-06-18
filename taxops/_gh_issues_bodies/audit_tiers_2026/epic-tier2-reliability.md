## Goal
Fix reliability issues visible under multi-user tax-season load — async bottlenecks, audit writer churn, startup coupling, extraction latency, typed field updates.

## Child issues
- REL-1: Move LLM calls out of request handlers
- REL-2: Replace per-request audit thread with single writer queue
- REL-3: Worker startup independent of WSGI entry point
- REL-4: Standardize DB connection lifecycle with context manager / teardown
- REL-5: Extraction worker event signaling (wake on enqueue)
- REL-6: api_field type validation per column

## Definition of done
REL-1 through REL-6 closed.
