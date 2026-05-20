"""JS-2: Wrap every bare inline <script> block in templates with an IIFE.

Functions referenced by onclick=/onchange= attributes are exposed on window
so inline event handlers keep working after the scope change.

Run from taxops/:
    python scripts/wrap_template_scripts.py [--dry-run]
"""
from __future__ import annotations
import re, sys, pathlib

# Functions that must remain reachable from inline event handlers.
# Derived from: python scripts/find_onclick.py
WINDOW_EXPORTS: dict[str, list[str]] = {
    "dashboard.html":       ["applyFilter", "setStatus", "toggleStatusMenu"],
    "efile_batch.html":     ["closeAcceptModal", "closeRejectModal", "markLogOut",
                             "markTransmitted", "setAck", "showAcceptModal",
                             "showRejectDetail", "submitAccept", "submitReject",
                             "toggleExportMenu", "toggleFlag"],
    "email_review.html":    ["acceptSuggestion", "deleteRule",
                             "finalizeEmailClassification", "loadDigest",
                             "markMissedReviewed", "rejectSuggestion"],
    "import_audit.html":    ["filterTable", "mergeClient", "showTab"],
    "logout_queue.html":    ["quickStatus"],
    "merge_clients.html":   ["mergePair", "skipPair", "swapKeepDiscard"],
    "pickup_workflow.html": ["onMethodChange", "recalc"],
    "return_detail.html":   ["addMissingDoc", "confirmExtraction", "copyDraft",
                             "deleteDocument", "deleteMissingDoc", "draftEmail",
                             "extractFields", "printSticker", "quickToggle",
                             "saveContactDate", "saveContactStatus", "saveField",
                             "setStatus", "setStatusDetail", "submitNote",
                             "syncDocToDrake", "toggleDocFilter",
                             "toggleExtractionReview", "toggleMissingDoc",
                             "toggleStatusMenu", "updateDocType"],
    "review_queue.html":    ["hideLinkSearch", "resolve", "searchClients",
                             "showLinkSearch"],
    "source_compare.html":  ["applyAllFrom", "applyField", "applyRecommended"],
    "upload.html":          ["confirmImport", "resetUpload"],
}

# Inline-script pattern: <script> … </script> WITHOUT a src= attribute
_SCRIPT_RE = re.compile(
    r'(<script(?:\s+(?!src=)[^>]*)?>)(.*?)(</script>)',
    re.DOTALL | re.IGNORECASE,
)


def _already_iife(body: str) -> bool:
    """True if the body is already fully wrapped in a single IIFE."""
    stripped = body.strip()
    return bool(re.match(r'^\(function\s*\(', stripped)
                or re.match(r'^\(\s*\(\s*\)\s*=>', stripped))


def _is_blank(body: str) -> bool:
    return not body.strip()


def _wrap(body: str, exports: list[str]) -> str:
    """Wrap body in an IIFE, appending window exports for any function
    that is actually defined in body."""
        # Only export functions that are defined in this specific script block
    defined = set(re.findall(r'^\s*(?:async\s+)?function\s+(\w+)', body, re.MULTILINE))
    needed  = [f for f in exports if f in defined]

    indent = "  "
    indented_body = "\n".join(
        (indent + line if line.strip() else line)
        for line in body.rstrip().splitlines()
    )
    export_lines = "\n".join(f"{indent}window.{fn} = {fn};" for fn in needed)
    if export_lines:
        return f"\n(function () {{\n{indented_body}\n\n{export_lines}\n}})();\n"
    return f"\n(function () {{\n{indented_body}\n}})();\n"


def transform(html: str, exports: list[str]) -> tuple[str, int]:
    """Return (new_html, count_of_wraps_applied)."""
    count = 0

    def replace(m: re.Match) -> str:
        nonlocal count
        open_tag, body, close_tag = m.group(1), m.group(2), m.group(3)
        if _is_blank(body) or _already_iife(body):
            return m.group(0)
        count += 1
        return open_tag + _wrap(body, exports) + close_tag

    new_html = _SCRIPT_RE.sub(replace, html)
    return new_html, count


def main(argv: list[str]) -> None:
    dry_run = "--dry-run" in argv
    root = pathlib.Path("templates")
    total = 0
    for p in sorted(root.rglob("*.html")):
        exports = WINDOW_EXPORTS.get(p.name, [])
        html = p.read_text(encoding="utf-8")
        new_html, n = transform(html, exports)
        if n == 0:
            continue
        total += n
        print(f"  {'(dry)' if dry_run else 'WRAP '} {p}  ({n} block{'s' if n>1 else ''})")
        if not dry_run:
            p.write_text(new_html, encoding="utf-8")
    print(f"\nTotal blocks wrapped: {total}")


if __name__ == "__main__":
    main(sys.argv)
