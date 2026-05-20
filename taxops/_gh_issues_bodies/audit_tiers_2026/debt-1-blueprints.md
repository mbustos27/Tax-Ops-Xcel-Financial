## Parent epic
Technical debt epic

## Problem
Single ~5.8 kLOC `taxops/app.py` mixes routing, queries, masking, importer glue, lifecycle.

## Fix
Incremental blueprints (`routes/` packages) extracting documents + email review first, then payments, etc.; keep zero behavior change PRs.

## Definition of done
- app.py shrunk measurably; at least documents + email review routed via blueprints
- python -m pytest tests/ -v passes
