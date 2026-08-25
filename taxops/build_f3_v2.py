"""Build clean Feature 3 commit on top of HEAD (F2)."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(r"T:\taxops")
TMP = Path(r"C:\Users\Windows 10\AppData\Local\Temp\f3-wip")


def show(path: str) -> str:
    return subprocess.check_output(["git", "show", f"HEAD:./{path}"], cwd=ROOT).decode("utf-8")


def patch_app(clean: str) -> str:
    # Intake print: pass log_in_date + names where possible
    old = (
        "                if FILETRACK_PRINT_MODE == \"relay\":\n"
        "                    from filetrack.labels.relay_client import print_label_via_relay\n"
        "                    print_label_via_relay(log_number)\n"
        "                else:\n"
        "                    from filetrack.labels.print_label import print_label as _filetrack_print_label\n"
        "                    _filetrack_print_label(log_number)"
    )
    new = (
        "                _label_kwargs = dict(\n"
        "                    last_name=last_name or \"\",\n"
        "                    first_name=(f.get(\"first_name\") or \"\").strip(),\n"
        "                    log_in_date=_v(\"intake_date\") or today_iso,\n"
        "                )\n"
        "                if FILETRACK_PRINT_MODE == \"relay\":\n"
        "                    from filetrack.labels.relay_client import print_label_via_relay\n"
        "                    print_label_via_relay(log_number, **_label_kwargs)\n"
        "                else:\n"
        "                    from filetrack.labels.print_label import print_label as _filetrack_print_label\n"
        "                    _filetrack_print_label(log_number, **_label_kwargs)"
    )
    if old not in clean:
        raise SystemExit("HEAD intake print block not found")
    clean = clean.replace(old, new, 1)

    # print-label API — prefer replacing whole function if present
    m = re.search(
        r'@app\.post\("/api/return/<int:return_id>/print-label"\).*?(?=\n\n# ──|\n\n@app\.)',
        clean,
        re.S,
    )
    if not m:
        raise SystemExit("HEAD print-label API not found")
    api = '''@app.post("/api/return/<int:return_id>/print-label")
@login_required
def api_return_print_label(return_id: int):
    """Reprint the physical LOG label via the same path as intake.

    Optional JSON ``log_in_date`` (YYYY-MM-DD) overrides the LOG-IN date printed
    on the sticker; blank/missing falls back to the return's intake_date, then today.
    """
    from filetrack.config import FILETRACK_ENABLED, FILETRACK_PRINT_MODE
    from filetrack.labels.relay_client import RelayError

    if not FILETRACK_ENABLED:
        return jsonify({
            "success": False,
            "error": "Filetrack printing is disabled (FILETRACK_ENABLED=false).",
        }), 400

    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT r.log_number, r.intake_date, c.last_name, c.first_name, c.display_name
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE r.id = ?
            """,
            (return_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"success": False, "error": "Return not found"}), 404
    log_number = row["log_number"]
    if not log_number:
        return jsonify({"success": False, "error": "Return has no log number yet."}), 400

    data = _get_json_safe() or {}
    log_in_date = (data.get("log_in_date") or "").strip() or (row["intake_date"] or None)
    kwargs = dict(
        last_name=row["last_name"] or "",
        first_name=row["first_name"] or "",
        display_name=row["display_name"] or "",
        log_in_date=log_in_date,
    )

    try:
        if (FILETRACK_PRINT_MODE or "local").strip().lower() == "relay":
            from filetrack.labels.relay_client import print_label_via_relay
            print_label_via_relay(log_number, **kwargs)
        else:
            from filetrack.labels.print_label import print_label
            print_label(log_number, **kwargs)
    except RelayError as exc:
        return jsonify({"success": False, "error": str(exc)}), 502
    except Exception as exc:
        logging.getLogger("filetrack").warning(
            "print-label API failed return_id=%s log=%s", return_id, log_number, exc_info=True,
        )
        return jsonify({"success": False, "error": str(exc)}), 500

    return jsonify({"success": True, "log_number": str(log_number)})


'''
    clean = clean[: m.start()] + api + clean[m.end() :]

    if '"today":          date.today().isoformat(),' not in clean:
        needle = (
            '        "prompt_scan":    request.args.get("scan") == "1",\n'
            "    })\n"
            '    return render_template("return_detail.html", **ctx)'
        )
        repl = (
            '        "prompt_scan":    request.args.get("scan") == "1",\n'
            '        "today":          date.today().isoformat(),\n'
            "    })\n"
            '    return render_template("return_detail.html", **ctx)'
        )
        if needle not in clean:
            raise SystemExit("today needle missing on HEAD return_detail ctx")
        clean = clean.replace(needle, repl, 1)
    return clean


def patch_return_detail(rd: str, rd_wip: str) -> str:
    if "print-sticker-date" not in rd:
        mw = re.search(
            r'(<label class="inline-flex items-center gap-1\.5 text-xs text-slate-600">'
            r".*?print-sticker-date.*?</label>\s*"
            r'<button type="button" onclick="printSticker\(\)".*?</button>)',
            rd_wip,
            re.S,
        )
        if not mw:
            raise SystemExit("wip print sticker block missing")
        mo = re.search(
            r'<button type="button" onclick="printSticker\(\)".*?</button>',
            rd,
            re.S,
        )
        if not mo:
            raise SystemExit("head print sticker button missing")
        rd = rd[: mo.start()] + mw.group(1) + rd[mo.end() :]

    mwf = re.search(r"async function printSticker\(\) \{.*?^\s{2}\}", rd_wip, re.S | re.M)
    mhf = re.search(r"async function printSticker\(\) \{.*?^\s{2}\}", rd, re.S | re.M)
    if not mwf or not mhf:
        raise SystemExit(f"printSticker fn missing wip={bool(mwf)} head={bool(mhf)}")
    return rd[: mhf.start()] + mwf.group(0) + rd[mhf.end() :]


def main() -> None:
    # Keep current F3 filetrack modules already edited in workspace; rebuild app/return_detail only.
    app_clean = patch_app(show("app.py"))
    (TMP / "app_clean.py").write_text(app_clean, encoding="utf-8")
    print("app_clean", app_clean.count("log_in_date"))

    rd_wip = (TMP / "return_detail.html").read_text(encoding="utf-8")
    rd_clean = patch_return_detail(show("templates/return_detail.html"), rd_wip)
    (TMP / "return_detail_clean.html").write_text(rd_clean, encoding="utf-8")
    print("rd_clean", "print-sticker-date" in rd_clean)


if __name__ == "__main__":
    main()
