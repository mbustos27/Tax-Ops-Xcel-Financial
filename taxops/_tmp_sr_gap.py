from drake_prefill_importer import join_purple_chunks, load_csm_xlsx, composite_dedupe_csm, match_csm_to_purple, normalize_purple_name
purple, *_ = join_purple_chunks([
    r"T:\taxops\CSVFILES\TY2024S.csv",
    r"T:\taxops\CSVFILES\TY2024S2439-5405.csv",
    r"T:\taxops\CSVFILES\TY2024S8867-end.csv",
])
sr = [p for p in purple if (p.return_type or "") == "1040SR"]
print("purple 1040SR", len(sr))
rows = load_csm_xlsx(r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx")
dedupe = composite_dedupe_csm(rows)
links, *_ = match_csm_to_purple(dedupe.clients, purple)
matched_sr = {normalize_purple_name(L.purple_name) for L in links if L.prefill_status=="PRIOR_YEAR_FORMS_AVAILABLE" and L.return_type=="1040SR"}
all_sr = {p.name_norm for p in sr}
missing = sorted(all_sr - matched_sr)
print("matched PRIOR 1040SR", len(matched_sr))
print("unmatched 1040SR", len(missing))
for n in missing:
    p = next(x for x in sr if x.name_norm==n)
    cands = [L for L in links if L.purple_name and normalize_purple_name(L.purple_name)==n]
    print(f"  {p.taxpayer_name!r} candidates={[(L.prefill_status, L.csm['csm_name_raw'], L.reason, L.match_score) for L in cands]}")
