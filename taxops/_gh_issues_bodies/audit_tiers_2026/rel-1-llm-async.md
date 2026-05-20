## Parent epic
Reliability hardening epic

## Problem
Synchronous LLM HTTP in `/ai/*` routes blocks Waitress workers; aligns with codebase review HIGH finding.

## Fix direction
Prefer 202 + job polling or dedicated worker queue; alternatively raise threadpool + timeouts as interim mitigation. Coordinate with extraction_queue semantics.

## Definition of done
- Documented SLA: no single request holds a worker beyond N seconds except streaming chat (if any)
- python -m pytest tests/ -v passes
