"""
Precise check: for no-log 2025 returns, does the SAME client_id have
any other return WITH a log number? That would indicate a genuine missed link.
"""
from db import get_connection
conn = get_connection()

# --- No-log 2025 returns where client has OTHER returns WITH a log number ---
same_client_has_log = conn.execute("""
    SELECT
        c.last_name, c.first_name, r.tax_year,
        (SELECT r2.log_number || ' (yr ' || r2.tax_year || ')'
         FROM returns r2
         WHERE r2.client_id = r.client_id
           AND r2.log_number IS NOT NULL AND r2.log_number != ''
         ORDER BY r2.tax_year DESC LIMIT 1) AS other_log
    FROM returns r
    JOIN clients c ON c.id = r.client_id
    WHERE (r.log_number IS NULL OR r.log_number = '')
      AND r.tax_year = 2025
      AND EXISTS (
          SELECT 1 FROM returns r2
          WHERE r2.client_id = r.client_id
            AND r2.log_number IS NOT NULL AND r2.log_number != ''
      )
    ORDER BY c.last_name
    LIMIT 50
""").fetchall()

print("No-log 2025 returns where SAME client has a log# on another year: %d" % len(same_client_has_log))
for r in same_client_has_log[:30]:
    print("  %-40s  other_log=%s" % (
        ("%s, %s" % (r["last_name"], r["first_name"] or ""))[:40],
        r["other_log"]
    ))

total_same = conn.execute("""
    SELECT COUNT(*) FROM returns r
    WHERE (r.log_number IS NULL OR r.log_number = '')
      AND r.tax_year = 2025
      AND EXISTS (
          SELECT 1 FROM returns r2
          WHERE r2.client_id = r.client_id
            AND r2.log_number IS NOT NULL AND r2.log_number != ''
      )
""").fetchone()[0]
print("(total: %d)" % total_same)

# --- No-log 2025 returns where client has NO returns with a log number at all ---
no_log_anywhere = conn.execute("""
    SELECT COUNT(*) FROM returns r
    WHERE (r.log_number IS NULL OR r.log_number = '')
      AND r.tax_year = 2025
      AND NOT EXISTS (
          SELECT 1 FROM returns r2
          WHERE r2.client_id = r.client_id
            AND r2.log_number IS NOT NULL AND r2.log_number != ''
      )
""").fetchone()[0]
print()
print("No-log 2025 returns whose client has ZERO log# ever: %d  (Drake-only clients)" % no_log_anywhere)

# --- Check if any 2025 no-log return client also has a 2025 LOGGED return ---
# (That would mean same client imported twice for 2025)
both = conn.execute("""
    SELECT c.last_name, c.first_name,
           r_no.log_number AS no_log,
           r_yes.log_number AS yes_log
    FROM returns r_no
    JOIN clients c ON c.id = r_no.client_id
    JOIN returns r_yes ON r_yes.client_id = r_no.client_id
    WHERE (r_no.log_number IS NULL OR r_no.log_number = '')
      AND r_no.tax_year = 2025
      AND r_yes.tax_year = 2025
      AND r_yes.log_number IS NOT NULL AND r_yes.log_number != ''
    LIMIT 20
""").fetchall()
print()
print("No-log 2025 returns where SAME client ALSO has a logged 2025 return: %d" % len(both))
for r in both[:10]:
    print("  %-40s  (dup? log=%s vs no-log)" % (
        ("%s, %s" % (r["last_name"], r["first_name"] or ""))[:40],
        r["yes_log"]
    ))

# --- Summary ---
print()
print("=== SUMMARY ===")
print("Total no-log 2025 returns    : 833")
print("Same client has log elsewhere: %d  (prior-year returns, Drake matched different year)" % total_same)
print("Client never had any log#    : %d  (pure Drake-only clients, never in manual log)" % no_log_anywhere)

conn.close()
