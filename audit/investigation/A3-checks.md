# A3 — Failure-mode checks (F1–F12)

_Generated: 2026-08-12T22:50:01Z_
_Disposition DB: `T:\audit\audit_disposition.sqlite`_
_Delta after checks: `{"NEW": 0, "RECURRING": 855, "REGRESSED": 0, "RESOLVED": 0, "findings_emitted": 855, "headline": 0}`_

| Check | Title | Deviation | Baseline highlight | Actual highlight |
|---|---|---|---|---|
| F1 | Drake 40-char name truncation | `INFORMATIONAL` | `{"desktop_at_40": 54, "jul31_NAME_TRUNCATED": 71, "prefill_at_40": 74}` | `{"csm_at_39": 17, "csm_at_40_plus": 55, "csm_truncated_total": 72, "prefill_csm_name_raw_at_40": 74}` |
| F2 | Name-order / joint-format divergence | `INFORMATIONAL` | `{"csm_with_amp": 442, "csm_with_comma": 1254, "purple_with_comma": 0}` | `{"csm_n": 1159, "csm_with_amp": 351, "csm_with_comma": 1044, "taxops_with_amp": 249}` |
| F3 | Duplicate TaxOps clients (twins) | `INFORMATIONAL` | `{"jul31_DUPLICATE_CLIENT": 142, "live_exact_groups": 8, "normalized_multi_buckets": 14}` | `{"exact_extra_rows": 8, "exact_groups": 8, "fuzzy_88_95_pairs_surname_blocked": 37, "normalized_multi_buckets": 8}` |
| F4 | Jul1 created_at rewrite provenance | `INFORMATIONAL` | `{"jul31_cluster_approx": 344, "jul31_jul1_dated": 368, "live_jul1_dated_prior": 191}` | `{"iso_format": 12, "jul1_dated": 184, "space_format": 172, "stamp_18_19_30": 172}` |
| F5 | Asymmetric SSN twins | `INFORMATIONAL` | `{"note": "Common in Jul1 cluster (0/368 SSN on stamp set)"}` | `{"asymmetric_groups": 0, "samples": []}` |
| F6 | Shells without log numbers | `INFORMATIONAL` | `{"approx_pct_returns_without_log": 26, "fill_rate_log": 0.74}` | `{"pct_without_log": 25.33, "pending_intake_total": 117, "returns_total": 1607, "returns_without_log": 407}` |
| F7 | Last-4 collisions | `INFORMATIONAL` | `{"client_keys_surplus_approx": 78, "csm_surplus_approx": 52, "prefill_surplus_approx": 130}` | `{"csm_collision_keys": 51, "csm_surplus": 52, "taxops_collision_keys": 78, "taxops_surplus": 82}` |
| F8 | Phantom / unmatched TaxOps | `INFORMATIONAL` | `{"jul31_PHANTOM_IN_TAXOPS": 227}` | `{"by_bucket": {"closed_or_logout": 150, "unmatched_other": 183}, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "named_rows": 1251, "restart_cut": 772}, "note": "Log keys = YR=25 canonical per bare (restart cut); not all named rows", "phantom_returns": 333}` |
| F9 | Logged-not-prepared / prepared-not-logged | `INFORMATIONAL` | `{"LOGGED_NOT_PREPARED": 141, "PREPARED_NOT_LOGGED": 88}` | `{"csm_rows_with_status_field": 1158, "invoice_L0_not_in_log": 192, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "restart_cut": 772}, "log_not_in_invoice_L0": 247}` |
| F10 | Prefill stub proliferation (--link-clients) | `INFORMATIONAL` | `{"all_ssn_and_prefill_linked": true, "aug7_burst": 244}` | `{"burst_count": 244, "exact_name_older_twin": 8, "prefill_linked": 244, "ssn_bearing": 243}` |
| F11 | Spouse triple-store divergence | `INFORMATIONAL` | `{"SPOUSE_STORE_DIVERGENCE": 198, "household_prefill": 1302, "spouses": 166}` | `{"clients_with_spouse_cols": 406, "drake_household_prefill": 1302, "only_clients_cols": 0, "spouses_rows": 442}` |
| F12 | Cross-system key coverage (Amendment 1) | `SUPERSEDED` | `{"I4_claim": "no shared key", "a1_three_way": 586, "amendment1": "Invoice Number = Tax Log number (season-prefixed in Drake)"}` | `{"client_external_ids_exists": false, "client_external_ids_rows": 0, "l0_eligible_invoices": 815, "three_way_bare_log": 571}` |

## F1 — Drake 40-char name truncation

- **Query:** COUNT CSM Client Name WHERE len>=39; prefill csm_name_raw len=40; TaxOps longer bag sharing surname prefix
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"desktop_at_40": 54, "jul31_NAME_TRUNCATED": 71, "onedrive_expected_near": "\u226554 (OneDrive 1159 vs Desktop 1155)", "prefill_at_40": 74}`
- **Actual:** `{"cross_source_superstring_hits": 0, "csm_at_39": 17, "csm_at_40_plus": 55, "csm_truncated_total": 72, "prefill_csm_name_raw_at_40": 74}`
- **Note:** Cap is upstream Drake CSM, not TaxOps VARCHAR.
- **Findings emitted:** 72

## F2 — Name-order / joint-format divergence

- **Query:** Classify CSM/TaxOps for comma, ampersand, blank-first (entity)
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"clients_blank_first": 151, "csm_with_amp": 442, "csm_with_comma": 1254, "purple_with_comma": 0}`
- **Actual:** `{"clients_blank_first": 146, "csm_n": 1159, "csm_with_amp": 351, "csm_with_comma": 1044, "order_hard_pairs_sampled": 0, "taxops_name_fields_with_comma": 5, "taxops_with_amp": 249}`
- **Note:** Format classifiers only; L3/L4 resolution volume deferred to ladder stats.
- **Findings emitted:** 0

## F3 — Duplicate TaxOps clients (twins)

- **Query:** GROUP BY exact last|first; punct-stripped norm; fuzzy 88–95 surname-blocked
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"jul31_DUPLICATE_CLIENT": 142, "jul31_stamped_collisions_approx": 254, "live_exact_groups": 8, "normalized_multi_buckets": 14}`
- **Actual:** `{"exact_extra_rows": 8, "exact_groups": 8, "fuzzy_88_95_pairs_surname_blocked": 37, "normalized_multi_buckets": 8, "top_blast": [{"blast": 1, "ids": [659, 2275], "key": "HERNANDEZ|ISMAEL", "n": 2}, {"blast": 1, "ids": [731, 2276], "key": "NUNO|JUAN", "n": 2}, {"blast": 1, "ids": [1215, 2272], "key": "ALVARADO|OSCAR", "n": 2}, {"blast": 2, "ids": [1278, 2393], "key": "LUNA|ESTEBAN", "n": 2}, {"blast": 1, "ids": [1344, 2404], "key": "VALDEZ|SANDRA", "n": 2}, {"blast": 3, "ids": [1440, 2273], "key": "HERNANDEZ|ABEL", "n": 2}, {"blast": 1, "ids": [1506, 2329], "key": "TASHAYOD|ALEX", "n": 2}, {"blast": 1, "ids": [1709, 2429], "key": "SOLOMON|LAUREN", "n": 2}]}`
- **Note:** Ranked by blast radius (returns + log numbers at risk).
- **Findings emitted:** 8

## F4 — Jul1 created_at rewrite provenance

- **Query:** clients.created_at LIKE '2026-07-01%'; space vs ISO; id<=1753; pre-Jul1 returns
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"jul31_cluster_approx": 344, "jul31_jul1_dated": 368, "live_jul1_dated_prior": 191, "mechanism": "CREATED_AT_REWRITE_NOT_INSERT"}`
- **Actual:** `{"id_le_1753": 167, "iso_format": 12, "jul1_dated": 184, "space_format": 172, "stamp_18_19_30": 172, "with_pre_jul1_return": 172}`
- **Note:** Provenance flag, not a repair target.
- **Findings emitted:** 1

## F5 — Asymmetric SSN twins

- **Query:** Normalized name dup groups where some have ssn_last4 and others NULL
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"note": "Common in Jul1 cluster (0/368 SSN on stamp set)"}`
- **Actual:** `{"asymmetric_groups": 0, "samples": []}`
- **Note:** Merge direction should favor SSN-bearing row.
- **Findings emitted:** 0

## F6 — Shells without log numbers

- **Query:** returns WHERE log_number IS NULL; PENDING INTAKE subset
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"approx_pct_returns_without_log": 26, "fill_rate_log": 0.74}`
- **Actual:** `{"by_status": {"CANCELLED": {"n": 2, "no_log": 0}, "EFILE": {"n": 2, "no_log": 0}, "EFILE READY": {"n": 1, "no_log": 0}, "FINALIZE": {"n": 25, "no_log": 1}, "HOLD": {"n": 3, "no_log": 0}, "LOG OUT": {"n": 840, "no_log": 117}, "OLD PICKUP": {"n": 1, "no_log": 1}, "PENDING INTAKE": {"n": 117, "no_log": 116}, "PICK UP": {"n": 1, "no_log": 1}, "PICKUP": {"n": 75, "no_log": 13}, "PRIOR HOLD": {"n": 3, "no_log": 2}, "PRIOR PROC": {"n": 2, "no_log": 2}, "PROCESSING": {"n": 535, "no_log": 154}}, "pct_without_log": 25.33, "pending_intake_total": 117, "pending_intake_without_log": 116, "returns_total": 1607, "returns_without_log": 407}`
- **Findings emitted:** 0

## F7 — Last-4 collisions

- **Query:** GROUP BY last4 HAVING COUNT>1 on CSM, clients, prefill; assert L1 not last4-alone
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"client_keys_surplus_approx": 78, "csm_surplus_approx": 52, "prefill_surplus_approx": 130}`
- **Actual:** `{"csm_collision_keys": 51, "csm_surplus": 52, "l1_last4_only_violations": 0, "prefill_collision_keys": 130, "taxops_collision_keys": 78, "taxops_surplus": 82}`
- **Note:** Collisions are expected; last4-alone L1 would be alarming.
- **Findings emitted:** 0

## F8 — Phantom / unmatched TaxOps

- **Query:** TY2025 returns whose bare log ∉ Tax Log (YR=25 canonical) and ∉ Drake L0-eligible invoices
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"jul31_PHANTOM_IN_TAXOPS": 227}`
- **Actual:** `{"by_bucket": {"closed_or_logout": 150, "unmatched_other": 183}, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "named_rows": 1251, "restart_cut": 772}, "note": "Log keys = YR=25 canonical per bare (restart cut); not all named rows", "phantom_returns": 333}`
- **Note:** Split test/closed before worklist; name-only phantoms need A3+ ladder residual.
- **Findings emitted:** 333

## F9 — Logged-not-prepared / prepared-not-logged

- **Query:** bare_log: invoice L0 vs Tax Log XCEL 2025 YR=25 canonical (workflow routing)
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"LOGGED_NOT_PREPARED": 141, "PREPARED_NOT_LOGGED": 88}`
- **Actual:** `{"csm_rows_with_status_field": 1158, "invoice_L0_not_in_log": 192, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "restart_cut": 772}, "log_not_in_invoice_L0": 247, "method": "invoice_key_proxy_canonical_yr25_log"}`
- **Note:** Marked workflow findings — different worklist from data findings.
- **Findings emitted:** 439

## F10 — Prefill stub proliferation (--link-clients)

- **Query:** clients.created_at LIKE '2026-08-07T22:27:47%'; join drake_prefill_links
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"all_ssn_and_prefill_linked": true, "aug7_burst": 244}`
- **Actual:** `{"burst_count": 244, "exact_name_older_twin": 8, "prefill_linked": 244, "ssn_bearing": 243}`
- **Note:** Classify twin-of-older vs legitimately new before merge.
- **Findings emitted:** 1

## F11 — Spouse triple-store divergence

- **Query:** clients.spouse_* vs spouses vs drake_household_prefill
- **Deviation class:** `INFORMATIONAL`
- **Baseline:** `{"SPOUSE_STORE_DIVERGENCE": 198, "household_prefill": 1302, "spouses": 166}`
- **Actual:** `{"clients_with_spouse_cols": 406, "drake_household_prefill": 1302, "only_clients_cols": 0, "only_spouses_table": 36, "spouses_rows": 442}`
- **Findings emitted:** 1

## F12 — Cross-system key coverage (Amendment 1)

- **Query:** bare_log three-way on TAXPAYER.csv L0 ∩ TaxOps ∩ Log(YR=25 canonical); client_external_ids exists?
- **Deviation class:** `SUPERSEDED`
- **Baseline:** `{"I4_claim": "no shared key", "a1_three_way": 586, "amendment1": "Invoice Number = Tax Log number (season-prefixed in Drake)"}`
- **Actual:** `{"client_external_ids_exists": false, "client_external_ids_rows": 0, "drake_taxops": 721, "invoice_full_width_coverage_pct": 99.91, "l0_eligible_invoices": 815, "log_index": {"all_bares": 1041, "canonical_yr25_bares": 870, "restart_cut": 772}, "pct_l0_not_three_way": 29.94, "three_way_bare_log": 571}`
- **Note:** I4 F12 'no key' superseded by invoice/log bare-key. client_external_ids not minted yet.
- **Findings emitted:** 0

## Loud corrections vs I4

- **F12:** I4 'no cross-system key' is **SUPERSEDED** by Amendment 1 (Invoice Number / bare log).
- **F8/F9:** Counts use invoice-key proxies; Jul31 used name-match residuals — not 1:1 comparable.
