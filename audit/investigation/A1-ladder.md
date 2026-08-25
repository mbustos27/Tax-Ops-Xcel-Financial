# A1 — Identity resolution ladder

_Generated: 2026-08-12T22:49:52Z_
_Audit DB: `T:\audit\audit_20260810.sqlite` run_id=5_

## Threshold policy

- Audit auto-accept fuzzy: **90** (aligned with prefill)
- Import accept (reference): **88**
- Human review band: **85–89**
- Pairs in disagreement band 88–89 this run: **0**

## L0 precondition funnel (Drake invoice keys)

| Stage | Distinct invoices |
|---|---:|
| Full-width raw keys | 844 |
| After bare-log format/range (C1) | 835 |
| After normalize (season strip in bare) | 835 |
| After bare-log collision exclusion (L0-eligible raw) | 815 |

## L0 three-way Venn (distinct invoice/log keys)

| Cell | Count |
|---|---:|
| Drake ∩ TaxOps ∩ Log | 571 |
| Drake ∩ TaxOps \ Log | 150 |
| Drake ∩ Log \ TaxOps | 52 |
| Drake only (neither) | 42 |
| TaxOps ∩ Log \ Drake L0-eligible | 194 |
| TaxOps TY keys with log# | 1017 |
| Tax Log named digit keys | 870 |

Jul31 name-based CSM Venn (Desktop 1155): all3=1009, taxops_not_log=126, log_not_taxops=13, neither=7. A1 L0 is bare-log three-way (Drake invoice `25xxxx` ↔ TaxOps/Log `xxxx`) on L0-eligible=815 bare_keys=815; three_way=571, drake∩taxops\log=150, drake∩log\taxops=52, drake_neither=42.

## CSM export lag

Authoritative CSM `C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\CLIENTS.xlsx` rows=1159; invoice export full-width=1062 / data=1063. CSM is later by Last Change through 07/30; TAXPAYER.csv As-of 08-10 — invoice export is newer for keys/names; CSM still supplies last4 for L1.

## Per-tier link counts

| Tier | Rule | Links |
|---|---|---:|
| L0 Drake↔TaxOps | (invoice, tax_year) | 721 |
| L0 Drake↔Log | (invoice, tax_year) | 623 |
| L0 TaxOps↔Log | (log_number, tax_year) | 768 |
| Log index (info) | cut=772; all=1041→YR25=870 | — |
| L1 last4+surname | CSM ↔ TaxOps | 387 |
| L2 exact match_keys / high fuzzy≥90 | | 7 |
| L3 truncation prefix | CSM↔invoice only | 0 |
| L4 human review | | 1 |
| L5 unmatched Drake invoice (L0-eligible leftover) | | 35 |

## C6 — L3 truncation diagnosis

- **Wired sources:** authoritative CSM (≥39-char display) ↔ `TAXPAYER.csv` invoice first/last only. **Not** purple CSM, **not** Tax Log.
- **Verdict:** L3 is wired but ineffective for joint/truncated CSM strings (invoice names are shorter or differently ordered; first/last max 35/17, zero rows at 39–40). Identity for these rows is carried by **L0 invoice keys** + L1 last4+surname. `NAME_TRUNCATED` is reclassified **informational** (dropped from Priority worklist).
- L3 link count this run: **0**

## Samples (≤10 per tier)

### L0

- `drake_invoice:250449|2025|4` → `taxops_return:557` score=1.0 human=False
  evidence: `{"bare_log": "449", "client_id": 511, "drake_name": "C EST LA VIE APPAREL INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250449", "log_number": "449", "tax_year": 2025, "taxops_na`
- `drake_invoice:250795|2025|5` → `taxops_return:536` score=1.0 human=False
  evidence: `{"bare_log": "795", "client_id": 490, "drake_name": "J & H MECHANICAL INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250795", "log_number": "795", "tax_year": 2025, "taxops_name":`
- `drake_invoice:250545|2025|6` → `taxops_return:495` score=1.0 human=False
  evidence: `{"bare_log": "545", "client_id": 451, "drake_name": "CREATIVE 28 STUDIO", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250545", "log_number": "545", "tax_year": 2025, "taxops_name": "`
- `drake_invoice:250814|2025|11` → `taxops_return:306` score=1.0 human=False
  evidence: `{"bare_log": "814", "client_id": 269, "drake_name": "EMMANUEL AND DAVANI LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250814", "log_number": "814", "tax_year": 2025, "taxops_nam`
- `drake_invoice:250398|2025|12` → `taxops_return:237` score=1.0 human=False
  evidence: `{"bare_log": "398", "client_id": 201, "drake_name": "CORONA BROS INSTALLATION LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250398", "log_number": "398", "tax_year": 2025, "taxop`
- `drake_invoice:250570|2025|14` → `taxops_return:534` score=1.0 human=False
  evidence: `{"bare_log": "570", "client_id": 488, "drake_name": "INSTACINCH INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250570", "log_number": "570", "tax_year": 2025, "taxops_name": "INST`
- `drake_invoice:250744|2025|20` → `taxops_return:874` score=1.0 human=False
  evidence: `{"bare_log": "744", "client_id": 815, "drake_name": "PRO WOOD FRAMING CONTRACTOR INC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250744", "log_number": "744", "tax_year": 2025, "ta`
- `drake_invoice:250512|2025|22` → `taxops_return:505` score=1.0 human=False
  evidence: `{"bare_log": "512", "client_id": 461, "drake_name": "INDUSTRIAL MACHINERY AUTOMATION SVC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250512", "log_number": "512", "tax_year": 2025,`
- `drake_invoice:250358|2025|30` → `taxops_return:302` score=1.0 human=False
  evidence: `{"bare_log": "358", "client_id": 264, "drake_name": "EERIE LANE COLLECTIVE LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "250358", "log_number": "358", "tax_year": 2025, "taxops_n`
- `drake_invoice:251240|2025|31` → `taxops_return:941` score=1.0 human=False
  evidence: `{"bare_log": "1240", "client_id": 874, "drake_name": "ROCKET TCG LLC", "guards": ["format", "season_prefix", "not_collision", "tax_year", "bare_log_normalize"], "invoice": "251240", "log_number": "1240", "tax_year": 2025, "taxops_name": "RO`

### L1

- `drake_csm:1743|ADAME, DEMETRIO L & ROSALINDA HERNANDEZ` → `taxops_client:1349` score=1.0 human=False
  evidence: `{"csm_name": "ADAME, DEMETRIO L & ROSALINDA HERNANDEZ", "last4": "1743", "rule": "last4+surname", "surname": "ADAME", "taxops_name": "ADAME, DEMETRIO"}`
- `drake_csm:5907|AGUAYO CORTES, JULIO G` → `taxops_client:12` score=1.0 human=False
  evidence: `{"csm_name": "AGUAYO CORTES, JULIO G", "last4": "5907", "rule": "last4+surname", "surname": "AGUAYO CORTES", "taxops_name": "AGUAYO CORTES, JULIO G"}`
- `drake_csm:5599|AGUAYO, MARGARITA P` → `taxops_client:14` score=1.0 human=False
  evidence: `{"csm_name": "AGUAYO, MARGARITA P", "last4": "5599", "rule": "last4+surname", "surname": "AGUAYO", "taxops_name": "AGUAYO, MARGARITA P"}`
- `drake_csm:9371|AGUILAR FLORES G, MONICA` → `taxops_client:17` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR FLORES G, MONICA", "last4": "9371", "rule": "last4+surname", "surname": "AGUILAR FLORES", "taxops_name": "AGUILAR FLORES G, MONICA"}`
- `drake_csm:7644|AGUILAR REYNA, ABUNDIO` → `taxops_client:22` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR REYNA, ABUNDIO", "last4": "7644", "rule": "last4+surname", "surname": "AGUILAR REYNA", "taxops_name": "AGUILAR REYNA, ABUNDIO"}`
- `drake_csm:8695|AGUILAR, AMBER A` → `taxops_client:304` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR, AMBER A", "last4": "8695", "rule": "last4+surname", "surname": "AGUILAR", "taxops_name": "AGUILAR, AMBER"}`
- `drake_csm:6720|AGUILAR, JEREMIAH & SOLEDAD` → `taxops_client:28` score=1.0 human=False
  evidence: `{"csm_name": "AGUILAR, JEREMIAH & SOLEDAD", "last4": "6720", "rule": "last4+surname", "surname": "AGUILAR", "taxops_name": "AGUILAR, JEREMIAH & SOLEDAD"}`
- `drake_csm:1344|AGUIRRE JIMENEZ, ANTONIO` → `taxops_client:35` score=1.0 human=False
  evidence: `{"csm_name": "AGUIRRE JIMENEZ, ANTONIO", "last4": "1344", "rule": "last4+surname", "surname": "AGUIRRE JIMENEZ", "taxops_name": "AGUIRRE JIMENEZ, ANTONIO"}`
- `drake_csm:1593|AGUIRRE, JOSE E & IRMA` → `taxops_client:1316` score=1.0 human=False
  evidence: `{"csm_name": "AGUIRRE, JOSE E & IRMA", "last4": "1593", "rule": "last4+surname", "surname": "AGUIRRE", "taxops_name": "AGUIRRE, JOSE"}`
- `drake_csm:9208|ALDAMA, MAYRA A` → `taxops_client:1634` score=1.0 human=False
  evidence: `{"csm_name": "ALDAMA, MAYRA A", "last4": "9208", "rule": "last4+surname", "surname": "ALDAMA", "taxops_name": "ALDAMA, MAYRA"}`

### L2

- `drake_invoice:251195|327` → `taxops_client:1706` score=1.0 human=False
  evidence: `{"bare_log": "1195", "drake": "MEDINA, YOLANDA", "keys": [["MEDINA", "YOLANDA"]], "method": "exact_match_keys", "taxops": "MEDINA, YOLANDA"}`
- `drake_invoice:251062|465` → `taxops_client:2191` score=1.0 human=False
  evidence: `{"bare_log": "1062", "drake": "ANDRADE RAZO, MIGUEL", "keys": [["ANDRADE RAZO", "MIGUEL"], ["ANDRADE", "MIGUEL"]], "method": "exact_match_keys", "taxops": "ANDRADE, MIGUEL"}`
- `drake_invoice:251270|546` → `taxops_client:2130` score=1.0 human=False
  evidence: `{"bare_log": "1270", "drake": "GONZALEZ, BRIANNA", "keys": [["GONZALEZ", "BRIANNA"]], "method": "exact_match_keys", "taxops": "GONZALEZ LOPEZ, BRIANNA S"}`
- `drake_invoice:250489|905` → `taxops_client:1440` score=1.0 human=False
  evidence: `{"bare_log": "489", "drake": "HERNANDEZ, ABEL", "keys": [["HERNANDEZ", "ABEL"]], "method": "exact_match_keys", "taxops": "HERNANDEZ, ABEL"}`
- `drake_invoice:250510|entity` → `taxops_client:503` score=100.0 human=False
  evidence: `{"bare_log": "510", "entity_exactish": "JORGE DE LA OSA DENTAL CORPORATION", "score": 100.0}`
- `drake_invoice:251189|entity` → `taxops_client:45` score=100.0 human=False
  evidence: `{"bare_log": "1189", "entity_exactish": "ALL IN ONE CONCRETE LLC", "score": 100.0}`
- `drake_invoice:251257|475` → `taxops_client:2259` score=92.85714285714286 human=False
  evidence: `{"bare_log": "1257", "drake": "MARTINEZ, MARIA", "method": "order_normalized_token_sort", "score": 92.85714285714286, "taxops": "MARTINEZ, MARIO"}`

### L4

- `drake_csm:1945|SORIA, NORMA O` → `taxops_client:972,2199` score=None human=True
  evidence: `{"last4": "1945", "n": 2, "reason": "last4+surname_ambiguous", "surname": "SORIA"}`

## Notes

- L0 compares **bare log numbers**: Drake Invoice `250141` ≡ TaxOps/Log `141` (strip season prefix `25` + leading zeros). Raw invoice still recorded in evidence.
- L0 never matches across tax years; tax_year is required on the TaxOps side.
- Malformed / collision / proforma-stale invoices are excluded from L0 (drop to name tiers).
- No link uses last-4 as sole evidence (L1 always pairs last4 with surname).
- Name precedence: TAXPAYER.csv first/last over CSM joint for identity; CSM for last4 + display.
- Fuzzy threshold chosen: **90** (prefill-aligned). Import's 88 is reported in the 88–89 band only.
