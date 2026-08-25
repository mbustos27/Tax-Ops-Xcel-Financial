import csv
from pathlib import Path
from collections import Counter
from db import get_connection, get_schema_version

for path in [
    r"C:\Users\Windows 10\Desktop\TY2024Spouses.csv",
    r"C:\Users\Windows 10\Desktop\TAXPAYERspouse25.csv",
]:
    print("===", path, "===")
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))
    print("total lines", len(rows))
    for i, r in enumerate(rows[:5]):
        print(f"  row{i+1} width={len(r)}: {r[:12]}")
    # header is row 3 (index 2)
    header = rows[2]
    print("header", header)
    data = rows[3:]
    # skip empty
    data = [r for r in data if any(c.strip() for c in r)]
    widths = Counter(len(r) for r in data)
    print("data rows", len(data), "width dist", widths.most_common())
    print("max width", max(widths), "header cols", len(header))
    wider = sum(1 for r in data if len(r) > len(header))
    print("wider than header", wider)

c = get_connection(r"T:\taxops\taxops.db")
print("\nDB schema", get_schema_version(c))
print("status", [dict(r) for r in c.execute("select prefill_status, count(*) n from drake_prefill_links group by 1 order by n desc")])
print("total", c.execute("select count(*) n from drake_prefill_links").fetchone()["n"])
print("forms", c.execute("select count(*) n from drake_form_prefill").fetchone()["n"])
c.close()
