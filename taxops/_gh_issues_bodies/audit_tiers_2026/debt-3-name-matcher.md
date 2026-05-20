## Parent epic
Technical debt epic

## Problem
`name_matcher.py` thresholds drive mail watcher routing risk — no pinned unit expectations.

## Fix
Parameterized tests covering exact match, accented names, ambiguity, MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE boundary behaviors.

## Definition of done
- Tests lock threshold semantics with fixtures
- python -m pytest tests/ -v passes
