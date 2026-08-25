"""I1-I3 quantitative analysis — read-only against snapshot DBs."""
from __future__ import annotations
import json, sqlite3, re, csv, io, hashlib
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

OUT = Path(r"T:\audit\investigation")
LIVE = OUT / "snapshot" / "taxops.db"
JUL31 = OUT / "snapshot" / "taxops_snapshot_20260731.sqlite"
AUDIT = OUT / "snapshot" / "audit_202607311100.sqlite"

def ro(p):
    c = sqlite3.connect(f"file:{Path(p).as_posix()}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c

def norm_key(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def person_key(last, first) -> str:
    return norm_key(f"{last or ''}|{first or ''}")

def display_key(last, first, display=None) -> str:
    if display and str(display).strip():
        return norm_key(str(display))
    return norm_key(f"{last or ''}, {first or ''}".strip(" ,"))

# ── I1 column stats ─────────────────────────────────────────────────
conn = ro(LIVE)
i1 = {"db": str(LIVE), "tables": {}}

IDENTITY_TABLES = {
    "clients": [
        "last_name","first_name","display_name","ssn_last4",
        "spouse_last_name","spouse_first_name","taxpayer_dob","spouse_dob",
        "taxpayer_phone","taxpayer_cell","taxpayer_work_phone","spouse_cell","spouse_work_phone",
        "taxpayer_email","spouse_email","address","prior_year_log","referred_by",
        "created_at","updated_at","is_new_client","id_type",
    ],
    "returns": [
        "log_number","tax_year","client_status","processor","intake_date",
        "filing_status","bank_name","bank_routing","bank_account","bank_account_type",
        "notes_intake","drake_status_raw","created_at","updated_at",
    ],
    "spouses": [
        "drake_spouse_id","last_name","first_name","middle_initial","date_of_birth",
        "derived_last_name","source","taxpayer_name","match_confidence",
    ],
    "client_dependents": [
        "drake_dependent_id","first_name","last_name","relationship","date_of_birth",
        "ssn_last4","taxpayer_name",
    ],
    "drake_prefill_links": [
        "csm_ssn_last4","csm_name_raw","csm_name_norm","purple_name","purple_name_norm",
        "client_id","prefill_status","match_tier","match_score","match_variant",
        "csm_status_raw",
    ],
}

for table, cols in IDENTITY_TABLES.items():
    total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    tinfo = {"total_rows": total, "columns": {}}
    for col in cols:
        try:
            row = conn.execute(f"""
                SELECT
                  COUNT(*) AS n,
                  SUM(CASE WHEN {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT))!='' THEN 1 ELSE 0 END) AS filled,
                  COUNT(DISTINCT CASE WHEN {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT))!='' THEN {col} END) AS distinct_filled,
                  MAX(LENGTH(CAST({col} AS TEXT))) AS max_len
                FROM {table}
            """).fetchone()
        except Exception as e:
            tinfo["columns"][col] = {"error": str(e)}
            continue
        filled = row["filled"] or 0
        # length histogram near truncation boundaries
        bounds = [39, 40, 50, 64, 100, 255]
        bound_counts = {}
        for b in bounds:
            n = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE LENGTH(CAST({col} AS TEXT)) = ?",
                (b,),
            ).fetchone()[0]
            if n:
                bound_counts[str(b)] = n
        # also >=39 for name-like
        ge39 = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE LENGTH(CAST({col} AS TEXT)) >= 39"
        ).fetchone()[0]
        tinfo["columns"][col] = {
            "fill_rate": round(filled / total, 4) if total else None,
            "filled": filled,
            "distinct_filled": row["distinct_filled"],
            "max_len": row["max_len"],
            "exact_len_counts": bound_counts,
            "len_ge_39": ge39,
        }
    i1["tables"][table] = tinfo

# Truncation suspects: max_len exactly equals a common width
suspects = []
for table, tinfo in i1["tables"].items():
    for col, st in tinfo["columns"].items():
        ml = st.get("max_len")
        if ml in (39, 40, 50, 64, 100, 255) and st.get("exact_len_counts", {}).get(str(ml), 0) > 0:
            suspects.append({"table": table, "column": col, "max_len": ml,
                             "n_at_max": st["exact_len_counts"].get(str(ml)),
                             "len_ge_39": st.get("len_ge_39")})
i1["truncation_suspects"] = suspects

(OUT/"I1-stats.json").write_text(json.dumps(i1, indent=2), encoding="utf-8")
print("I1 done suspects", len(suspects))

# ── I2 July 1 + Aug 7 characterization ──────────────────────────────
i2 = {"queries": {}}

for label, path in [("live", LIVE), ("jul31", JUL31)]:
    c = ro(path)
    jul1_n = c.execute("SELECT COUNT(*) FROM clients WHERE substr(created_at,1,10)='2026-07-01'").fetchone()[0]
    dist = [dict(r) for r in c.execute(
        "SELECT created_at, COUNT(*) n FROM clients WHERE substr(created_at,1,10)='2026-07-01' GROUP BY 1 ORDER BY n DESC"
    )]
    fill = dict(c.execute("""
        SELECT
          SUM(CASE WHEN ssn_last4 IS NOT NULL AND TRIM(ssn_last4)!='' THEN 1 ELSE 0 END) has_ssn,
          SUM(CASE WHEN display_name IS NOT NULL AND TRIM(display_name)!='' THEN 1 ELSE 0 END) has_display,
          SUM(CASE WHEN address IS NOT NULL AND TRIM(address)!='' THEN 1 ELSE 0 END) has_address,
          SUM(CASE WHEN taxpayer_email IS NOT NULL AND TRIM(taxpayer_email)!='' THEN 1 ELSE 0 END) has_email,
          SUM(CASE WHEN taxpayer_cell IS NOT NULL AND TRIM(taxpayer_cell)!='' THEN 1 ELSE 0 END) has_cell,
          COUNT(*) n
        FROM clients WHERE substr(created_at,1,10)='2026-07-01'
    """).fetchone())
    # name collisions within jul1 set vs pre-jul1
    jul1 = list(c.execute(
        "SELECT id, last_name, first_name, display_name, ssn_last4, created_at FROM clients WHERE substr(created_at,1,10)='2026-07-01'"
    ))
    pre = list(c.execute(
        "SELECT id, last_name, first_name, display_name, ssn_last4, created_at FROM clients WHERE substr(created_at,1,10)<'2026-07-01'"
    ))
    pre_by_key = defaultdict(list)
    for r in pre:
        pre_by_key[person_key(r["last_name"], r["first_name"])].append(r)
    exact_pre = 0
    no_pre = 0
    multi_pre = 0
    samples_exact = []
    for r in jul1:
        k = person_key(r["last_name"], r["first_name"])
        hits = pre_by_key.get(k, [])
        if not hits:
            no_pre += 1
        elif len(hits) == 1:
            exact_pre += 1
            if len(samples_exact) < 5:
                samples_exact.append({"jul1_id": r["id"], "name": f"{r['last_name']}, {r['first_name']}",
                                      "pre_id": hits[0]["id"], "pre_created": hits[0]["created_at"]})
        else:
            multi_pre += 1
    # display_name collisions among jul1 themselves
    disp = Counter(norm_key(r["display_name"] or f"{r['last_name']}, {r['first_name']}") for r in jul1)
    self_dups = sum(1 for k,v in disp.items() if v>1 and k)
    batches = [dict(r) for r in c.execute("SELECT * FROM import_batches ORDER BY id")]
    i2[label] = {
        "jul1_client_count": jul1_n,
        "timestamp_dist": dist,
        "column_fill": fill,
        "exact_name_match_to_pre_jul1": exact_pre,
        "no_pre_match": no_pre,
        "multi_pre_match": multi_pre,
        "samples_exact_pre": samples_exact,
        "jul1_internal_display_dup_keys": self_dups,
        "import_batches": batches,
    }
    c.close()

# Aug 7 burst on live
c = ro(LIVE)
aug7 = list(c.execute(
    "SELECT id, last_name, first_name, ssn_last4, created_at, display_name FROM clients WHERE substr(created_at,1,10)='2026-08-07'"
))
aug7_ssn = sum(1 for r in aug7 if r["ssn_last4"])
# how many linked in prefill
linked = c.execute("""
  SELECT COUNT(*) FROM clients c
  JOIN drake_prefill_links d ON d.client_id=c.id
  WHERE substr(c.created_at,1,10)='2026-08-07'
""").fetchone()[0]
i2["aug7"] = {
    "n": len(aug7),
    "with_ssn": aug7_ssn,
    "linked_in_prefill": linked,
    "single_timestamp": Counter(r["created_at"] for r in aug7).most_common(3),
    "sample": [{"id": r["id"], "name": f"{r['last_name']}, {r['first_name']}", "ssn4": r["ssn_last4"]} for r in aug7[:5]],
}

# Duplicate name groups live
dups = [dict(r) for r in c.execute("""
  SELECT UPPER(TRIM(last_name)) ln, UPPER(TRIM(COALESCE(first_name,''))) fn, COUNT(*) n,
         GROUP_CONCAT(id) ids
  FROM clients
  WHERE COALESCE(TRIM(last_name),'')!=''
  GROUP BY 1,2 HAVING n>1
  ORDER BY n DESC LIMIT 40
""")]
i2["live_exact_name_duplicate_groups"] = {"n_groups": len(dups), "top": dups[:25]}
# count all groups
n_all = c.execute("""
  SELECT COUNT(*) FROM (
    SELECT 1 FROM clients
    WHERE COALESCE(TRIM(last_name),'')!=''
    GROUP BY UPPER(TRIM(last_name)), UPPER(TRIM(COALESCE(first_name,'')))
    HAVING COUNT(*)>1
  )
""").fetchone()[0]
extra_rows = c.execute("""
  SELECT COALESCE(SUM(n-1),0) FROM (
    SELECT COUNT(*) n FROM clients
    WHERE COALESCE(TRIM(last_name),'')!=''
    GROUP BY UPPER(TRIM(last_name)), UPPER(TRIM(COALESCE(first_name,'')))
    HAVING COUNT(*)>1
  )
""").fetchone()[0]
i2["live_exact_name_duplicate_groups"]["n_groups_all"] = n_all
i2["live_exact_name_duplicate_groups"]["extra_rows"] = extra_rows
c.close()

(OUT/"I2-stats.json").write_text(json.dumps(i2, indent=2, default=str), encoding="utf-8")
print("I2 done jul31", i2["jul31"]["jul1_client_count"], "live", i2["live"]["jul1_client_count"], "aug7", i2["aug7"]["n"])

# ── I3 linkage ──────────────────────────────────────────────────────
i3 = {}

# Candidate identifier scoring on live TaxOps
c = ro(LIVE)
cand = {}
for col, expr in [
    ("ssn_last4", "ssn_last4"),
    ("taxpayer_cell", "taxpayer_cell"),
    ("taxpayer_phone", "taxpayer_phone"),
    ("taxpayer_email", "taxpayer_email"),
    ("address", "address"),
    ("taxpayer_dob", "taxpayer_dob"),
    ("prior_year_log", "prior_year_log"),
]:
    total = c.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    filled = c.execute(f"SELECT COUNT(*) FROM clients WHERE {expr} IS NOT NULL AND TRIM(CAST({expr} AS TEXT))!=''").fetchone()[0]
    distinct = c.execute(f"SELECT COUNT(DISTINCT {expr}) FROM clients WHERE {expr} IS NOT NULL AND TRIM(CAST({expr} AS TEXT))!=''").fetchone()[0]
    # collision: distinct keys with >1 row
    collisions = c.execute(f"""
      SELECT COUNT(*) FROM (
        SELECT {expr} FROM clients
        WHERE {expr} IS NOT NULL AND TRIM(CAST({expr} AS TEXT))!=''
        GROUP BY {expr} HAVING COUNT(*)>1
      )
    """).fetchone()[0]
    cand[col] = {
        "source": "taxops.clients",
        "fill_rate": round(filled/total,4),
        "filled": filled,
        "distinct": distinct,
        "uniqueness_ratio": round(distinct/filled,4) if filled else None,
        "collision_keys": collisions,
    }

# log_number on returns
total_r = c.execute("SELECT COUNT(*) FROM returns").fetchone()[0]
filled_log = c.execute("SELECT COUNT(*) FROM returns WHERE log_number IS NOT NULL AND TRIM(log_number)!=''").fetchone()[0]
dist_log = c.execute("SELECT COUNT(DISTINCT log_number) FROM returns WHERE log_number IS NOT NULL AND TRIM(log_number)!=''").fetchone()[0]
# uniqueness of (log_number, tax_year)
uy = c.execute("""
  SELECT COUNT(*) FROM (
    SELECT log_number, tax_year FROM returns
    WHERE log_number IS NOT NULL AND TRIM(log_number)!=''
    GROUP BY log_number, tax_year HAVING COUNT(*)>1
  )
""").fetchone()[0]
cand["log_number"] = {
    "source": "taxops.returns",
    "fill_rate": round(filled_log/total_r,4),
    "filled": filled_log,
    "distinct": dist_log,
    "uniqueness_ratio": round(dist_log/filled_log,4) if filled_log else None,
    "duplicate_log_year_pairs": uy,
}

# CSM last4 from prefill
total_l = c.execute("SELECT COUNT(*) FROM drake_prefill_links").fetchone()[0]
filled_csm = c.execute("SELECT COUNT(*) FROM drake_prefill_links WHERE csm_ssn_last4 IS NOT NULL AND TRIM(csm_ssn_last4)!=''").fetchone()[0]
dist_csm = c.execute("SELECT COUNT(DISTINCT csm_ssn_last4) FROM drake_prefill_links WHERE csm_ssn_last4 IS NOT NULL AND TRIM(csm_ssn_last4)!=''").fetchone()[0]
coll_csm = c.execute("""
  SELECT COUNT(*) FROM (
    SELECT csm_ssn_last4 FROM drake_prefill_links
    WHERE csm_ssn_last4 IS NOT NULL AND TRIM(csm_ssn_last4)!=''
    GROUP BY csm_ssn_last4 HAVING COUNT(*)>1
  )
""").fetchone()[0]
cand["csm_ssn_last4"] = {
    "source": "drake_prefill_links",
    "fill_rate": round(filled_csm/total_l,4) if total_l else None,
    "filled": filled_csm, "distinct": dist_csm,
    "uniqueness_ratio": round(dist_csm/filled_csm,4) if filled_csm else None,
    "collision_keys": coll_csm,
}
i3["candidate_ids_taxops"] = cand

# Name length at 39/40 in clients and prefill
name_len = {}
for label, sql in [
    ("clients_display", "SELECT LENGTH(TRIM(display_name)) L FROM clients WHERE display_name IS NOT NULL AND TRIM(display_name)!=''"),
    ("clients_last", "SELECT LENGTH(TRIM(last_name)) L FROM clients WHERE last_name IS NOT NULL"),
    ("csm_name_raw", "SELECT LENGTH(TRIM(csm_name_raw)) L FROM drake_prefill_links"),
    ("csm_name_norm", "SELECT LENGTH(TRIM(csm_name_norm)) L FROM drake_prefill_links"),
    ("purple_name", "SELECT LENGTH(TRIM(purple_name)) L FROM drake_prefill_links WHERE purple_name IS NOT NULL"),
]:
    lens = [r[0] for r in c.execute(sql)]
    name_len[label] = {
        "n": len(lens),
        "max": max(lens) if lens else None,
        "n_eq_39": sum(1 for x in lens if x==39),
        "n_eq_40": sum(1 for x in lens if x==40),
        "n_ge_39": sum(1 for x in lens if x>=39),
        "n_ge_40": sum(1 for x in lens if x>=40),
    }
i3["name_length_boundaries"] = name_len

# Normalized name collisions in clients
buckets = defaultdict(list)
for r in c.execute("SELECT id, last_name, first_name, display_name, ssn_last4 FROM clients"):
    k = person_key(r["last_name"], r["first_name"])
    if k and k != "|":
        buckets[k].append({"id": r["id"], "last": r["last_name"], "first": r["first_name"],
                           "ssn4": (r["ssn_last4"] or "")[-4:] if r["ssn_last4"] else None})
multi = sorted([(k,v) for k,v in buckets.items() if len(v)>1], key=lambda x: -len(x[1]))
i3["normalized_name_collisions"] = {
    "n_multi_buckets": len(multi),
    "extra_rows": sum(len(v)-1 for _,v in multi),
    "largest_25": [{"key": k, "n": len(v), "ids": [x["id"] for x in v], "ssn4s": [x["ssn4"] for x in v]} for k,v in multi[:25]],
}

# Truncated vs full: csm_name that is prefix of another
names = [r[0] for r in c.execute("SELECT csm_name_norm FROM drake_prefill_links WHERE csm_name_norm IS NOT NULL")]
names_set = set(names)
prefix_pairs = []
# only check names with len>=39 as truncated candidates
cands39 = [n for n in names if len(n)>=39]
for short in cands39:
    for long in names:
        if long != short and long.startswith(short) and len(long) > len(short):
            prefix_pairs.append({"short": short, "long": long})
            break
i3["truncated_vs_full_csm"] = {"n_short_ge39": len(cands39), "n_with_longer_prefix_hit": len(prefix_pairs),
                              "samples": prefix_pairs[:10]}

# Format divergence samples
fmt = {
    "csm_has_amp": c.execute("SELECT COUNT(*) FROM drake_prefill_links WHERE csm_name_raw LIKE '%&%'").fetchone()[0],
    "csm_has_comma": c.execute("SELECT COUNT(*) FROM drake_prefill_links WHERE csm_name_raw LIKE '%,%'").fetchone()[0],
    "purple_has_amp": c.execute("SELECT COUNT(*) FROM drake_prefill_links WHERE purple_name LIKE '%&%'").fetchone()[0],
    "purple_has_comma": c.execute("SELECT COUNT(*) FROM drake_prefill_links WHERE purple_name LIKE '%,%'").fetchone()[0],
    "clients_blank_first": c.execute("SELECT COUNT(*) FROM clients WHERE first_name IS NULL OR TRIM(first_name)=''").fetchone()[0],
    "sample_joint_csm": [r[0] for r in c.execute("SELECT csm_name_raw FROM drake_prefill_links WHERE csm_name_raw LIKE '%&%' LIMIT 5")],
    "sample_purple_order": [r[0] for r in c.execute("SELECT purple_name FROM drake_prefill_links WHERE purple_name IS NOT NULL LIMIT 5")],
}
i3["name_format"] = fmt
c.close()

# Three-way from audit DB if available
if AUDIT.exists():
    a = ro(AUDIT)
    # stage counts
    stages = {}
    for t in ["stage_drake","stage_log","stage_taxops_client","audit_match","audit_finding"]:
        try:
            stages[t] = a.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception as e:
            stages[t] = str(e)
    # match pair kinds
    try:
        pairs = [dict(r) for r in a.execute(
            "SELECT left_kind, right_kind, COUNT(*) n FROM audit_match GROUP BY 1,2 ORDER BY n DESC"
        )]
    except Exception:
        # try alternate schema
        cols = [r[1] for r in a.execute("PRAGMA table_info(audit_match)")]
        pairs = {"columns": cols}
        try:
            pairs = [dict(r) for r in a.execute(
                f"SELECT * FROM audit_match LIMIT 1"
            )]
        except Exception as e:
            pairs = str(e)
    findings = []
    try:
        findings = [dict(r) for r in a.execute(
            "SELECT subtype, COUNT(*) n FROM audit_finding GROUP BY 1 ORDER BY n DESC"
        )]
    except Exception:
        try:
            findings = [dict(r) for r in a.execute(
                "SELECT finding_type, COUNT(*) n FROM audit_finding GROUP BY 1 ORDER BY n DESC"
            )]
        except Exception as e:
            findings = [{"error": str(e), "cols": [r[1] for r in a.execute("PRAGMA table_info(audit_finding)")]}]
    i3["audit_db"] = {"stages": stages, "match_pairs": pairs, "findings": findings,
                      "pragma_match": [dict(zip(["cid","name","type","notnull","dflt","pk"], r)) for r in a.execute("PRAGMA table_info(audit_match)")],
                      "pragma_finding": [dict(zip(["cid","name","type","notnull","dflt","pk"], r)) for r in a.execute("PRAGMA table_info(audit_finding)")]}
    a.close()

# CSM xlsx name lengths (Desktop 1155 + CSVFILES 2024)
from openpyxl import load_workbook
csm_stats = {}
for label, path in [
    ("desktop_1155", Path(r"C:\Users\Windows 10\Desktop\CLIENTS.xlsx")),
    ("csvfiles_2024", Path(r"T:\taxops\CSVFILES\2024 CLIENTS.xlsx")),
]:
    if not path.exists():
        continue
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    header = next(rows)
    # Client Name col
    try:
        idx = list(header).index("Client Name")
        id_idx = list(header).index("ID (Last 4)")
    except ValueError:
        idx, id_idx = 1, 0
    names=[]; last4s=[]; types=[]
    for row in rows:
        nm = row[idx] if idx < len(row) else None
        if nm is None or str(nm).strip()=="":
            continue
        s = str(nm).strip()
        names.append(s)
        last4s.append(str(row[id_idx]).strip() if id_idx < len(row) and row[id_idx] is not None else "")
        if len(header)>2:
            types.append(str(row[2]).strip() if row[2] else "")
    wb.close()
    lens=[len(n) for n in names]
    # uniqueness last4
    filled4=[x for x in last4s if x and x.lower()!="none"]
    csm_stats[label] = {
        "n": len(names),
        "max_name_len": max(lens) if lens else None,
        "n_eq_39": sum(1 for x in lens if x==39),
        "n_eq_40": sum(1 for x in lens if x==40),
        "n_ge_39": sum(1 for x in lens if x>=39),
        "last4_filled": len(filled4),
        "last4_distinct": len(set(filled4)),
        "last4_collision_keys": len(filled4)-len(set(filled4)),
        "sample_len40": [n for n in names if len(n)==40][:5],
        "sample_len39": [n for n in names if len(n)==39][:5],
    }
i3["csm_xlsx"] = csm_stats

# Tax log XCEL 2025 — sample name lengths via openpyxl (data start row 6)
log_path = Path(r"C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC\Shared\Logs\TAX LOG 2025 Live.xlsx")
if log_path.exists():
    wb = load_workbook(log_path, read_only=True, data_only=True)
    ws = wb["XCEL 2025"]
    # Per audit config: LOG_LAST_COL=2 (C), LOG_FIRST_COL=3 (D), LOG_YR_COL=4, data from row 6
    named=0; logs=[]; lasts=[]; firsts=[]; lens=[]
    for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if i < 6:
            continue
        last = row[2] if len(row)>2 else None
        first = row[3] if len(row)>3 else None
        logn = row[1] if len(row)>1 else None  # [INFERRED] col B often log — verify
        # Actually config says LOG_LAST_COL=2, so col index 2 is last name. Log # might be col 0 or 1.
        if last is None or str(last).strip()=="":
            continue
        named += 1
        ls=str(last).strip(); fs=str(first).strip() if first else ""
        lasts.append(ls); firsts.append(fs)
        lens.append(len(ls)+ (1+len(fs) if fs else 0))
        if logn is not None and str(logn).strip() not in ("", "None"):
            logs.append(str(logn).strip())
    wb.close()
    i3["tax_log_xcel2025"] = {
        "named_rows": named,
        "log_values_nonempty": len(logs),
        "log_distinct": len(set(logs)),
        "last_max_len": max(len(x) for x in lasts) if lasts else None,
        "last_n_eq_40": sum(1 for x in lasts if len(x)==40),
        "last_n_eq_39": sum(1 for x in lasts if len(x)==39),
        "note": "Column mapping per audit/config.py LOG_LAST_COL=2 LOG_FIRST_COL=3; log number column inferred from col B presence",
    }

(OUT/"I3-stats.json").write_text(json.dumps(i3, indent=2, default=str), encoding="utf-8")
print("I3 done")
print("truncation suspects", json.dumps(suspects[:15], indent=2))
print("jul31 exact_pre", i2["jul31"]["exact_name_match_to_pre_jul1"], "no_pre", i2["jul31"]["no_pre_match"])
