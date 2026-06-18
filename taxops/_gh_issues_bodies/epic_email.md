## Goal
Harden **`mail_watcher.py`** ingestion so each UNSEEN message is handled once per cycle, **`known_sender_rules`** only grows via staff-approved inserts, and duplicate attachments stop polluting **`return_documents`**.

## Canonical functions to audit
| Function | Responsibility |
|---------|----------------|
| **`_poll_lock`** (`threading.Lock`) | `blocking=False` skip when prior cycle active |
| **`_poll_once` / `_poll_once_inner`** | Full IMAP pass + classify + dispatch |
| **`_check_known_sender_rule`** (`sender_domain`) | Layer-1 deterministic domain rule |
| **`_record_classification`** | Persist `email_classifications` meta (never whole email bodies) |

## Bug themes
1. **Duplicate processing / multi-save** despite guard rails (`_processed_uids`, dedup helpers).
2. **Auto-rules** creeping into **`known_sender_rules`** without UX acceptance.

## Child issues
- EMAIL-1 — `_poll_lock` + UID parsing correctness
- EMAIL-2 — audit every **`INSERT`** into **`known_sender_rules`** (API + migrations)
- EMAIL-3 — attachment dedupe hashes / size checks inside **`_save_attachments()`**

## Non-goals (per sprint planning)
- No redesign of reviewer UI shells unless blocking fix.
- No swap from **sklearn** email classifier (`classifier.py` — replaces legacy fastText path) unless product requests.

