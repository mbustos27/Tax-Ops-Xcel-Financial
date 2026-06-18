## Goal
Finalize **accuracy + visibility** for extracted IRS rows stored in **`w2_records`**, **`f1099_*_records`** (DDL originates from **`form_schema.CREATE_TABLE_FRAGMENTS_DOC7`** aggregated during **`db.init_db`** + migrations).

## Current pipeline pillars
| Component | Responsibility |
|-----------|----------------|
| **`extractor._extract_fields`** | pdfplumber / vision prompts |
| **`extractor._process_item`** | queue consume, **`_compute_confidence`**, `UPDATE return_documents.doc_type`, queue status |
| **`ai_routes` extract/classify helpers** (`_save_form_data`, `_detect_form_type`, `_form_table_to_doc_type`) | Manual + batch saves |
| **`return_detail.html` JS** (`loadFormData`, `fdFieldLabel`, ...) | Rendering & inline edit |

Open issues surfaced in QA: incomplete field parity, lingering **legacy mirrored columns**.

## Child issues
- FORMS-1 **`doc_type` auto-tag lag**
- FORMS-2 **W-2 box fidelity**
- FORMS-3 **stop writes to legacy columns**
- FORMS-4 **UI completeness / labels**

## Non-goals
No Drake coupling, IRS electronic file generation, alterations to extractor queue concurrency model absent perf incident.

