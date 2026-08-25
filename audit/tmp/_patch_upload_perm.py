from pathlib import Path

p = Path(r"T:\taxops\routes\documents.py")
t = p.read_text(encoding="utf-8")
old = (
    '@documents_bp.route("/return/<int:return_id>/documents/upload", methods=["POST"])\n'
    "@login_required\n"
    "def return_documents_upload(return_id: int):"
)
new = (
    '@documents_bp.route("/return/<int:return_id>/documents/upload", methods=["POST"])\n'
    "@login_required\n"
    '@permission_required("can_manage_return_documents")\n'
    "def return_documents_upload(return_id: int):"
)
if old not in t:
    raise SystemExit("pattern not found")
p.write_text(t.replace(old, new, 1), encoding="utf-8")
print("ok")
