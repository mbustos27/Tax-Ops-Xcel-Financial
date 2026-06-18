"""Remove document and email-review route blocks from app.py (DEBT-1)."""
content = open('app.py', encoding='utf-8').read()

# ── Block 1: document routes ───────────────────────────────────────────────
START1 = '\n\n@app.route("/return/<int:return_id>/documents/upload", methods=["POST"])'
END1_ANCHOR = '\n\ndef _serialize_form_row'
i1 = content.find(START1)
i1_end = content.find(END1_ANCHOR, i1)
assert i1 > 0 and i1_end > i1, f"doc block not found: {i1} / {i1_end}"
note1 = '\n\n# DEBT-1: document routes moved to routes/documents.py (Blueprint).'
content = content[:i1] + note1 + content[i1_end:]
print("doc block removed")

# ── Block 2: email review routes ───────────────────────────────────────────
# The section starts with the comment line and ends before Email sender rules
import re
m_start = re.search(r'\n\n# ── Email classification review', content)
m_end   = re.search(r'\n\n# ── Email sender rules', content)
assert m_start and m_end and m_end.start() > m_start.start(), "email block not found"
note2 = '\n\n# DEBT-1: email review routes moved to routes/email_review.py (Blueprint).'
content = content[:m_start.start()] + note2 + content[m_end.start():]
print("email block removed")

open('app.py', 'w', encoding='utf-8').write(content)
print(f"done, lines: {content.count(chr(10))}")
