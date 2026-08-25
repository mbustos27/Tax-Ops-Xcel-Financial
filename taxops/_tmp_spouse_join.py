import csv
from collections import Counter, defaultdict
from drake_prefill_importer import normalize_purple_name, join_purple_chunks

path = r"C:\Users\Windows 10\Desktop\TY2024Spouses.csv"
with open(path, newline="", encoding="utf-8-sig") as f:
    rows = list(csv.reader(f))
header = rows[2]
data = rows[3:]
# pad
H = len(header)
padded = 0
taxpayers = defaultdict(list)
for r in data:
    if not any(c.strip() for c in r):
        continue
    if len(r) > H:
        raise SystemExit("wider")
    if len(r) < H:
        padded += 1
        r = r + [""]*(H-len(r))
    d = dict(zip(header, r))
    name = d["Taxpayer Name"].strip()
    if not name:
        continue
    taxpayers[normalize_purple_name(name)].append(d)
print("grouped", len(taxpayers), "padded", padded, "raw data", len(data))

purple, *_ = join_purple_chunks([
    r"T:\taxops\CSVFILES\TY2024S.csv",
    r"T:\taxops\CSVFILES\TY2024S2439-5405.csv",
    r"T:\taxops\CSVFILES\TY2024S8867-end.csv",
])
pset = {p.name_norm for p in purple}
sset = set(taxpayers)
print("exact", len(pset & sset), "spouse only", len(sset-pset), "purple only", len(pset-sset))
print("spouse only sample", list(sset-pset)[:5])
print("purple only sample", list(pset-sset)[:5])
