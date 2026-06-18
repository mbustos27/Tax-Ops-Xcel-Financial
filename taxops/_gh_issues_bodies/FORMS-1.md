## Parent epic
Epic: Form data extraction verification

## Code paths (`extractor.py` + `ai_routes.py`)
- Background: **`extractor._process_item` → `_save_form_data` → guarded `UPDATE return_documents SET doc_type = ?`** (only unknown → staff tag untouched).
- Manual: **`ai_routes.ai_document_extract` (`POST /ai/documents/<id>/extract`)** after **`_save_form_data`** commits.
- Classify shim: **`_classify_document_using_row`**.

## Acceptance
Synthetic W‑2 ingest: **`doc_type`** flips **`unknown` → `'W‑2'`** without manual tagging when thresholds satisfied.

## Tests
Exercise worker happy-path fixture + UI smoke instructions.

