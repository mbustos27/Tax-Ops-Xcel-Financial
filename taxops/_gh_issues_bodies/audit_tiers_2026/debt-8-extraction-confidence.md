## Parent epic
Technical debt epic

## Problem
Confidence tuple semantics drift between `_extract_fields` return value and `_compute_confidence` in worker flow; PDF vision path may truncate multi-page stubs/forms.

## Fix
- Normalize API return shape (either drop bogus constant confidence or compute once)
- Document scoring per `detected_type`
- Optionally render capped multi-page thumbnails for vision model

## Definition of done
- Tests updated for semantics
- Documented rationale in extractor module docstring
- python -m pytest tests/ -v passes
