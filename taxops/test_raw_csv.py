"""Look at raw CSV rows around log numbers that have no name to understand structure."""
import csv
import io

with open("data/incoming/TAX LOG 2025 Live.csv", "rb") as f:
    raw = f.read()

text = raw.decode("utf-8-sig", errors="replace")
reader = csv.reader(io.StringIO(text))
rows = list(reader)

# Print the first 30 rows to see the structure
print("=== First 30 rows (raw) ===")
for i, row in enumerate(rows[:30]):
    print(f"Row {i:3d}: {row}")

print()
# Now find rows where the log numbers 510, 772, 782 appear
target_logs = {"510", "772", "782", "382", "846"}
print("=== Rows containing target log numbers ===")
for i, row in enumerate(rows):
    for cell in row:
        if cell.strip() in target_logs:
            # Print context: 2 rows before and 2 after
            start = max(0, i-2)
            end   = min(len(rows), i+3)
            print(f"--- Found '{cell.strip()}' at row {i} ---")
            for j in range(start, end):
                print(f"  Row {j}: {rows[j]}")
            break
