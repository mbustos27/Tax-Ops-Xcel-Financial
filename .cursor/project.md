# TaxOps – System Overview

## 1. Purpose

TaxOps replaces manual Excel logs, paper intake sheets, and e-file tracking spreadsheets used in a tax preparation office.

Primary goal:

- Track returns from intake → processing → pickup → e-file → logout
- Replace manual workflows with structured UI flows

---

## 2. Core Workflow (IMPORTANT)

Statuses:

- PROCESSING → return being worked on
- HOLD → waiting on missing client documents
- FINALIZE → ready for final review
- PICKUP → client must sign and pay
- EFILE READY → ready to be transmitted
- LOG OUT → completed
- REJECTED → IRS rejected return

Rules:

- HOLD means work is paused (not active)
- PICKUP requires signatures + payment
- EFILE READY requires pickup complete
- LOG OUT only happens after e-file or final completion

---

## 3. Real-World Processes (SOURCE OF TRUTH)

### Pickup Flow

- Client signs forms
- Payment collected (cash, Zelle, card)
- Card adds 3% fee
- Receipt created in QuickBooks
- Receipt number recorded
- Pickup date recorded
- Return becomes EFILE READY

### E-file Flow

- Returns move to EFILE READY queue
- Staff selects multiple returns
- Creates batch
- Transmission date = today
- Submit in Drake (manual)
- Later mark:
  - Accepted
  - Rejected

### Rejection Flow

- When rejected:
  - Must record rejection code
  - Must record reason
  - Must mark for client contact

---

## 4. Data Model / Current Technical Reality (IMPORTANT)

Authoritative schema: `taxops/db.py` (init_db + *migrate*existing_tables). Below is a summary aligned with the live app.

### clients

Core:

- id
- last_name
- first_name
- display_name
- ssn_last4
- referral_flag
- referred_by
- created_at
- updated_at

Also includes spouse fields, contact info, address, intake demographics, prior_year_log, etc.

---

### returns

Keys:

- id
- client_id
- tax_year

Workflow:

- client_status (NOT a column named "status")
- Values come from STATUS_FLOW in [app.py](http://app.py) and may be normalized via normalizer

Important:

- CANCELLED is terminal and import-locked

Office / dates:

- log_number
- processor
- intake_date
- pickup_date
- logout_date
- updated_date
- date_emailed
- email_marker
- transfer flags
- efile_date
- ack_date (IRS ACK date stored on return)
- drake_status_raw
- verified

Pickup UI:

- signatures_given
- signatures_received

IMPORTANT:

- No transmission_date on returns
- No ack_status on returns
- These belong to efile batch tables

---

### return_forms

- One row per return_id
- Boolean flags for 1040, schedules, entity forms, etc.

---

### payments

- return_id (FK)
- total_fee
- fee_paid
- receipt_number
- cc_fee
- payment_method

Also includes:

- refund_amount
- balance_due
- bank_deposit
- zelle/check/cash references

---

### Supporting Tables

notes:

- per-return notes

missing_docs:

- per-return missing document tracker

status_events:

- status change history

import_batches / import_rows:

- CSV import audit trail

review_queue:

- ambiguous import matches

dependents:

- dependent records per return

---

### efile_batches

- id
- transmission_date
- notes
- status (open / closed)
- created_at

---

### efile_batch_items

- id
- batch_id (FK)
- return_id (FK)

Snapshot fields:

- log_number
- client_name
- ssn_last4
- tax_year
- receipt_number
- fee_paid
- pickup_date
- transmission_date

ACK tracking:

- ack_status (pending / accepted / rejected)
- ack_date
- rejection_code
- rejection_reason

Operations:

- needs_calculation
- created_at

Constraint:

- UNIQUE (batch_id, return_id)

---

### Rules (technical reality)

- One logical return per (client_id, tax_year) is intended, but not strictly enforced
- Log numbers are not globally unique
- Each return typically has one payments row
- Batch items store denormalized data for spreadsheet-style use

---

## 5. System Principles (VERY IMPORTANT)

- Do not duplicate data across tables unless necessary
- Always reuse existing fields before adding new ones
- UI should reflect real-world workflow, not abstract design
- Prefer simple solutions over complex automation
- When uncertain, ask rather than guessing

---

## 6. Constraints

- No direct Drake integration
- No direct QuickBooks integration (manual entry only)
- System must work with existing workflow
- Must not break dashboard or return detail page
- Must support re-importing data safely

---

## 7. Naming Conventions

- status values: ALL_CAPS (PROCESSING, HOLD, etc.)
- stored in returns.client_status
- database fields: snake_case
- UI labels: human readable
- IDs: integer primary keys

---

## 8. How to Approach New Features

When implementing a feature:

1. Check existing schema first
2. Reuse existing tables/fields if possible
3. Add minimal new fields
4. Avoid overengineering
5. Ensure feature maps to real workflow

---

## 9. Definition of Done (Global)

A feature is done when:

- It works in real workflow
- Data persists correctly
- UI reflects correct state
- Does not break existing features

---

## 10. AI Coding Rules

Before coding:

- Read [db.py](http://db.py), [app.py](http://app.py), and relevant templates first
- Do not assume fields exist because they are described here
- Verify actual column names before writing queries
- Reuse existing status helpers and normalizers
- Do not create duplicate tables for existing workflows
- Prefer small migrations over large rewrites
- Keep changes compatible with:
  - import system
  - dashboard
  - return detail page
  - payment flow