from csv_analyzer import analyze

with open("data/incoming/TAX LOG 2025 Live.csv", "rb") as f:
    data = analyze(f.read())

print(f"Header row:  {data.header_row_index}")
print(f"Data starts: {data.data_start_index}")
print(f"Total rows:  {data.total_rows}")
print(f"Warnings:    {data.warnings}")
print()
mapped = [c for c in data.columns if not c.skip and c.field]
for c in mapped:
    key = f"{c.table}.{c.field}"
    print(f"  col {c.col_index:2d}  [{key:35s}]  conf={c.confidence}%  header={c.raw_header}")
print()
print(f"Unmapped cols: {sum(1 for c in data.columns if c.skip)}")
