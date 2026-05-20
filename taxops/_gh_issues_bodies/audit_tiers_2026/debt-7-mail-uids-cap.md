## Parent epic
Technical debt epic

## Problem
`mail_watcher._processed_uids` is an unbounded set (`taxops/mail_watcher.py` module comment ~37–41) guarded by `_processed_uids_lock`.

## Fix
LRU/order capped structure (OrderedDict/pop oldest) preserving correctness for duplicate suppression window.

## Definition of done
- Memory bounded across long-lived process
- python -m pytest tests/ -v passes
