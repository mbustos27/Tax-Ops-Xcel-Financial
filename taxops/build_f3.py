"""Build Feature-3-only clean copies of app.py and return_detail.html."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

TMP = Path(r"C:\Users\Windows 10\AppData\Local\Temp\f3-wip")


def main() -> None:
    wip = (TMP / "app.py").read_text(encoding="utf-8")
    clean = subprocess.check_output(["git", "show", "HEAD:./app.py"]).decode("utf-8")

    old_intake = (
        "        from filetrack.labels.dispatch import try_print_log_label\n"
        "        try_print_log_label(\n"
        "            log_number,\n"
        '            last_name=last_name or "",\n'
        '            first_name=(f.get("first_name") or "").strip(),\n'
        "        )"
    )
    new_intake = (
        "        from filetrack.labels.dispatch import try_print_log_label\n"
        "        try_print_log_label(\n"
        "            log_number,\n"
        '            last_name=last_name or "",\n'
        '            first_name=(f.get("first_name") or "").strip(),\n'
        '            log_in_date=_v("intake_date") or today_iso,\n'
        "        )"
    )
    if 'log_in_date=_v("intake_date")' not in clean:
        if old_intake not in clean:
            raise SystemExit("intake anchor missing")
        clean = clean.replace(old_intake, new_intake, 1)

    m = re.search(
        r'@app\.post\("/api/return/<int:return_id>/print-label"\).*?'
        r'return jsonify\(\{"success": True, "log_number": str\(log_number\)\}\)',
        wip,
        re.S,
    )
    m2 = re.search(
        r'@app\.post\("/api/return/<int:return_id>/print-label"\).*?'
        r'return jsonify\(\{"success": True, "log_number": str\(log_number\)\}\)',
        clean,
        re.S,
    )
    if not m or not m2:
        raise SystemExit(f"print-label blocks missing wip={bool(m)} clean={bool(m2)}")
    clean = clean[: m2.start()] + m.group(0) + clean[m2.end() :]

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
            raise SystemExit("return_detail today needle missing")
        clean = clean.replace(needle, repl, 1)

    (TMP / "app_clean.py").write_text(clean, encoding="utf-8")
    print("app_clean ok", clean.count("log_in_date"))

    rd_wip = (TMP / "return_detail.html").read_text(encoding="utf-8")
    rd = subprocess.check_output(
        ["git", "show", "HEAD:./templates/return_detail.html"]
    ).decode("utf-8")

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
    rd = rd[: mhf.start()] + mwf.group(0) + rd[mhf.end() :]
    (TMP / "return_detail_clean.html").write_text(rd, encoding="utf-8")
    print("return_detail_clean ok", "print-sticker-date" in rd)


if __name__ == "__main__":
    main()
