## Parent epic
Epic: Form data extraction verification

## Problem
Suspected truncation to employer + coarse wage summaries — compare against PDF ground truth boxes (Box1..Box17 etc.).

## Implementation review
Audit **`extractor._build_text_prompt` / `_build_vision_prompt`**, **`_DOCUMENT_EXTRACT_ALLOWED_KEYS` whitelist**, **`ai_routes._save_form_data`** column mapping (`FORM_TABLE_INSERT_COLUMNS` in **`form_schema.py`**).

Deliver **`verify_w2.py`** regression harness dumping DB row vs expected canonical fixture.

## Definition of done
All enumerated IRS boxes persisted to **`box*_...` TEXT columns** for happy path fixtures.

