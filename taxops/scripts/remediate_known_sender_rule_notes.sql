-- One-time remediation (EMAIL-2): clarify notes on known_sender_rules.
-- Apply to a SQLite backup or maintenance window after deploying code that uses note
-- staff-accepted-rule-suggestion on Accept POST.
--
-- Remap misleading note from the staff Accept path (recommended — keeps blocking rules active).
UPDATE known_sender_rules SET note = 'staff-accepted-rule-suggestion' WHERE note = 'from-llm-suggestion';

-- Optional: drop rows wrongly tagged auto-suggested (no current code path emits this).
-- DELETE FROM known_sender_rules WHERE note = 'auto-suggested';

-- Optional nuclear DELETE from epic wording (drops every matching rule row, legitimate or not).
-- DELETE FROM known_sender_rules WHERE note IN ('from-llm-suggestion','auto-suggested');
