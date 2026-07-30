"""Scan Agent prerequisite + runtime self-test.

Run on the reception PC (Epson attached)::

    cd /d T:\\taxops
    python -m scan_agent.selftest
    python -m scan_agent.selftest --fix   # attempt pip / pywin32 postinstall
    python -m scan_agent.selftest --json

Exit codes:
  0 = all required checks passed
  1 = one or more required checks failed
  2 = usage / environment error
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from typing import Any


from scan_agent.server import REQUIRED_CODE_REV

DEFAULT_PORT = int(os.environ.get("SCAN_AGENT_PORT", "8766"))


@dataclass
class CheckResult:
    id: str
    title: str
    ok: bool
    required: bool = True
    detail: str = ""
    fix: str = ""


@dataclass
class SuiteReport:
    checks: list[CheckResult] = field(default_factory=list)
    python: str = ""
    port: int = DEFAULT_PORT
    token_present: bool = False

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks if c.required)

    @property
    def failed_required(self) -> list[CheckResult]:
        return [c for c in self.checks if c.required and not c.ok]


def _add(
    report: SuiteReport,
    *,
    id: str,
    title: str,
    ok: bool,
    required: bool = True,
    detail: str = "",
    fix: str = "",
) -> None:
    report.checks.append(
        CheckResult(
            id=id,
            title=title,
            ok=ok,
            required=required,
            detail=detail,
            fix=fix,
        )
    )


def _load_token() -> str:
    env = (os.environ.get("SCAN_AGENT_TOKEN") or "").strip()
    if env:
        return env
    candidates = [
        r"C:\TaxOps\ScanAgent\token.env",
        os.path.join(os.path.dirname(__file__), "..", ".env"),
        os.path.join(os.path.dirname(__file__), "..", "..", ".env"),
    ]
    for path in candidates:
        path = os.path.abspath(path)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("SCAN_AGENT_TOKEN="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return ""


def check_python(report: SuiteReport) -> None:
    report.python = sys.executable
    ver = sys.version_info
    ok = ver.major == 3 and ver.minor >= 10
    _add(
        report,
        id="python",
        title="Python 3.10+",
        ok=ok,
        detail=f"{sys.version.split()[0]} @ {sys.executable}",
        fix="Install Python 3.12+ from python.org (check Add to PATH), then re-run the wizard.",
    )
    if "WindowsApps" in (sys.executable or ""):
        _add(
            report,
            id="python_store",
            title="Not Windows Store stub python",
            ok=False,
            detail=sys.executable,
            fix="Uninstall Store Python alias or install python.org build; Store stubs break pywin32/WIA.",
        )


def check_imports(report: SuiteReport) -> dict[str, bool]:
    mods = {
        "flask": "flask",
        "win32com": "win32com.client",
        "pythoncom": "pythoncom",
        "PIL": "PIL",
        "pymupdf": "fitz",
    }
    found: dict[str, bool] = {}
    for label, modname in mods.items():
        try:
            __import__(modname)
            found[label] = True
            _add(report, id=f"import_{label}", title=f"Import {label}", ok=True, detail=modname)
        except Exception as exc:
            found[label] = False
            required = label != "pymupdf"  # Pillow PDF fallback exists
            _add(
                report,
                id=f"import_{label}",
                title=f"Import {label}",
                ok=False,
                required=required,
                detail=str(exc),
                fix=(
                    f'"{sys.executable}" -m pip install flask "pywin32>=306" Pillow pymupdf'
                    if label in ("flask", "win32com", "pythoncom", "PIL", "pymupdf")
                    else ""
                ),
            )
    return found


def run_pywin32_postinstall() -> tuple[bool, str]:
    """Register COM bits — common fix when CoInitialize/WIA fails after pip install."""
    try:
        import win32api  # type: ignore  # noqa: F401
    except Exception as exc:
        return False, f"pywin32 not importable: {exc}"
    # Locate postinstall script next to this interpreter
    candidates = [
        os.path.join(os.path.dirname(sys.executable), "Scripts", "pywin32_postinstall.py"),
        os.path.join(sys.prefix, "Scripts", "pywin32_postinstall.py"),
    ]
    script = next((p for p in candidates if os.path.isfile(p)), None)
    if not script:
        # Newer pywin32: python -m pywin32_postinstall
        try:
            completed = subprocess.run(
                [sys.executable, "-m", "pywin32_postinstall", "-install"],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            out = (completed.stdout or "") + (completed.stderr or "")
            return completed.returncode == 0, out.strip() or f"exit {completed.returncode}"
        except Exception as exc:
            return False, f"pywin32_postinstall module missing: {exc}"
    try:
        completed = subprocess.run(
            [sys.executable, script, "-install"],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        out = (completed.stdout or "") + (completed.stderr or "")
        return completed.returncode == 0, out.strip() or f"exit {completed.returncode}"
    except Exception as exc:
        return False, str(exc)


def check_com_wia(report: SuiteReport, *, do_fix: bool) -> None:
    if do_fix:
        ok_fix, detail_fix = run_pywin32_postinstall()
        _add(
            report,
            id="pywin32_postinstall",
            title="pywin32 post-install (COM registration)",
            ok=ok_fix,
            required=False,
            detail=detail_fix[:500],
            fix='Run as Admin: python -m pywin32_postinstall -install',
        )

    try:
        if sys.platform == "win32":
            sys.coinit_flags = 2  # STA before pythoncom import
        import pythoncom  # type: ignore
        import win32com.client  # type: ignore
    except Exception as exc:
        _add(
            report,
            id="com_import",
            title="pythoncom + win32com available",
            ok=False,
            detail=str(exc),
            fix='pip install "pywin32>=306" then python -m pywin32_postinstall -install',
        )
        return

    inited = False
    try:
        pythoncom.CoInitializeEx(pythoncom.COINIT_APARTMENTTHREADED)
        inited = True
    except Exception:
        try:
            pythoncom.CoInitialize()
            inited = True
        except Exception as exc:
            _add(
                report,
                id="com_init",
                title="CoInitialize (COM apartment)",
                ok=False,
                detail=str(exc),
                fix="Run wizard as the logged-in user (not only SYSTEM). Re-run pywin32_postinstall as Admin.",
            )
            return

    _add(report, id="com_init", title="CoInitialize (COM apartment)", ok=True, detail="STA ok")

    try:
        mgr = win32com.client.Dispatch("WIA.DeviceManager")
        count = int(mgr.DeviceInfos.Count)
        names: list[str] = []
        for i in range(1, count + 1):
            info = mgr.DeviceInfos(i)
            try:
                if int(info.Type) != 1:
                    continue
            except Exception:
                continue
            try:
                names.append(str(info.Properties("Name").Value))
            except Exception:
                names.append(f"Scanner-{i}")
        if names:
            _add(
                report,
                id="wia_scanners",
                title="WIA scanners visible",
                ok=True,
                detail=" | ".join(names),
            )
        else:
            _add(
                report,
                id="wia_scanners",
                title="WIA scanners visible",
                ok=False,
                detail=f"DeviceManager ok but 0 scanners (DeviceInfos={count})",
                fix=(
                    "Power on Epson, fix USB (Device Manager status OK, not phantom), "
                    "install Epson Scan 2, unplug/replug, then re-run selftest."
                ),
            )
    except Exception as exc:
        msg = str(exc)
        fix = "pip install pywin32 + python -m pywin32_postinstall -install (Admin), then restart."
        if "CoInitialize" in msg:
            fix = (
                "CoInitialize missing on this thread — use scan_agent com_sta_v4 build, "
                "and run python -m pywin32_postinstall -install as Admin."
            )
        _add(
            report,
            id="wia_dispatch",
            title="WIA.DeviceManager Dispatch",
            ok=False,
            detail=msg,
            fix=fix,
        )
    finally:
        if inited:
            try:
                pythoncom.CoUninitialize()
            except Exception:
                pass

    # Package path uses the same helper the HTTP agent uses
    try:
        from scan_agent.server import list_wia_scanners

        devices = list_wia_scanners()
        _add(
            report,
            id="list_wia_helper",
            title="scan_agent.list_wia_scanners()",
            ok=True,
            required=False,
            detail=f"{len(devices)} device(s)",
        )
    except Exception as exc:
        _add(
            report,
            id="list_wia_helper",
            title="scan_agent.list_wia_scanners()",
            ok=False,
            detail=str(exc),
            fix="Ensure you run from taxops folder: python -m scan_agent.selftest",
        )


def check_token(report: SuiteReport) -> str:
    token = _load_token()
    report.token_present = bool(token)
    _add(
        report,
        id="token",
        title="SCAN_AGENT_TOKEN present",
        ok=bool(token),
        detail="set" if token else "missing",
        fix="Run scan_agent_wizard.bat and enter the same token as TaxOps SCAN_AGENT_TOKEN.",
    )
    return token


def check_code_on_disk(report: SuiteReport) -> None:
    try:
        from scan_agent import server as sa

        src_path = getattr(sa, "__file__", "") or ""
        with open(src_path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        has_rev = REQUIRED_CODE_REV in text
        has_com = "CoInitializeEx" in text and "_com_sta" in text
        _add(
            report,
            id="code_rev_disk",
            title=f"server.py has {REQUIRED_CODE_REV} + _com_sta",
            ok=has_rev and has_com,
            detail=src_path,
            fix="Update share T:\\taxops\\scan_agent\\server.py from server, clear __pycache__, restart agent.",
        )
    except Exception as exc:
        _add(
            report,
            id="code_rev_disk",
            title="server.py readable",
            ok=False,
            detail=str(exc),
            fix="Map T: to \\\\Xcel-server\\taxops and cd taxops before running.",
        )


def check_port_free_or_ours(report: SuiteReport, port: int) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    try:
        result = sock.connect_ex(("127.0.0.1", port))
        in_use = result == 0
    finally:
        sock.close()
    _add(
        report,
        id="port_listen",
        title=f"TCP {port} listener",
        ok=True,
        required=False,
        detail="in use (agent may already be running)" if in_use else "free",
    )


def check_http_health(report: SuiteReport, token: str, port: int) -> None:
    url = f"http://127.0.0.1:{port}/health"
    if not token:
        _add(
            report,
            id="http_health",
            title="HTTP /health",
            ok=False,
            required=False,
            detail="skipped — no token",
        )
        return
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"X-Scan-Agent-Token": token},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body)
    except urllib.error.HTTPError as exc:
        _add(
            report,
            id="http_health",
            title="HTTP /health",
            ok=False,
            required=False,
            detail=f"HTTP {exc.code}",
            fix="Token mismatch or agent not ready — re-run wizard / restart_scan_agent.",
        )
        return
    except Exception as exc:
        _add(
            report,
            id="http_health",
            title="HTTP /health",
            ok=False,
            required=False,
            detail=str(exc),
            fix="Start the agent with the wizard (step Start), leave the window open.",
        )
        return

    rev = data.get("code_rev")
    tips = data.get("tips") or []
    err = data.get("scanner_error") or ""
    blob = f"{err} {' '.join(str(t) for t in tips)}"
    if rev not in ("com_sta_v1", "com_sta_v2", "com_sta_v3", "com_sta_v4"):
        _add(
            report,
            id="http_code_rev",
            title=f"Running agent is {REQUIRED_CODE_REV}",
            ok=False,
            detail=f"code_rev={rev!r} — OLD process still running",
            fix="On reception: run GO_SCAN_AGENT.bat — must kill PID on 8766.",
        )
    elif rev != REQUIRED_CODE_REV:
        _add(
            report,
            id="http_code_rev",
            title=f"Running agent is {REQUIRED_CODE_REV}",
            ok=False,
            detail=f"code_rev={rev!r} — restart to load {REQUIRED_CODE_REV}",
            fix="On reception: run GO_SCAN_AGENT.bat — must kill PID on 8766.",
        )
    else:
        _add(
            report,
            id="http_code_rev",
            title=f"Running agent is {REQUIRED_CODE_REV}",
            ok=True,
            detail=f"code_rev={rev}",
        )

    if "CoInitialize" in blob:
        _add(
            report,
            id="http_no_coinit_error",
            title="No CoInitialize error from /health",
            ok=False,
            detail=blob[:300],
            fix="Kill old agent, run pywin32_postinstall, start wizard again.",
        )
    else:
        _add(
            report,
            id="http_no_coinit_error",
            title="No CoInitialize error from /health",
            ok=True,
            required=False,
            detail="clean",
        )

    if data.get("scanner_found"):
        _add(
            report,
            id="http_scanner",
            title="Agent reports scanner_found",
            ok=True,
            detail=str(data.get("scanner_names") or data.get("scanners")),
        )
    else:
        _add(
            report,
            id="http_scanner",
            title="Agent reports scanner_found",
            ok=False,
            required=False,
            detail=blob[:300] or "scanner_found=false",
            fix="Fix Epson USB/power/drivers; agent HTTP can be up while WIA still empty.",
        )


def check_firewall_hint(report: SuiteReport, port: int) -> None:
    if sys.platform != "win32":
        return
    try:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"Get-NetFirewallRule -DisplayName 'TaxOps Scan Agent' -EA SilentlyContinue | "
                f"Where-Object {{ $_.Enabled -eq 'True' }} | Select-Object -First 1 -ExpandProperty DisplayName",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        name = (completed.stdout or "").strip()
        _add(
            report,
            id="firewall",
            title=f"Firewall rule for TCP {port}",
            ok=bool(name),
            required=False,
            detail=name or "missing",
            fix="Run scan_agent_wizard.bat as Admin (adds inbound TCP 8766).",
        )
    except Exception as exc:
        _add(
            report,
            id="firewall",
            title="Firewall rule check",
            ok=False,
            required=False,
            detail=str(exc),
        )


def run_suite(*, port: int = DEFAULT_PORT, do_fix: bool = False, http: bool = True) -> SuiteReport:
    report = SuiteReport(port=port)
    # Ensure taxops root is on path when run as python -m scan_agent.selftest
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)

    check_python(report)
    check_imports(report)
    check_code_on_disk(report)
    check_com_wia(report, do_fix=do_fix)
    token = check_token(report)
    check_port_free_or_ours(report, port)
    check_firewall_hint(report, port)
    if http:
        check_http_health(report, token, port)
    return report


def print_report(report: SuiteReport) -> None:
    print("")
    print("=" * 60)
    print("  TaxOps Scan Agent — selftest")
    print("=" * 60)
    print(f"  Python: {report.python}")
    print(f"  Port:   {report.port}")
    print("")
    for c in report.checks:
        mark = "OK  " if c.ok else ("FAIL" if c.required else "WARN")
        print(f"  [{mark}] {c.title}")
        if c.detail:
            print(f"         {c.detail}")
        if (not c.ok) and c.fix:
            print(f"         FIX: {c.fix}")
    print("")
    if report.passed:
        print("  RESULT: PASS (required checks)")
    else:
        print("  RESULT: FAIL")
        print("  Failed required:")
        for c in report.failed_required:
            print(f"    - {c.id}: {c.title}")
    print("=" * 60)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TaxOps Scan Agent selftest")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--fix", action="store_true", help="Run pywin32 postinstall")
    parser.add_argument("--json", action="store_true", help="Machine-readable JSON")
    parser.add_argument("--no-http", action="store_true", help="Skip live /health check")
    args = parser.parse_args(argv)

    report = run_suite(port=args.port, do_fix=args.fix, http=not args.no_http)
    if args.json:
        payload: dict[str, Any] = {
            "passed": report.passed,
            "python": report.python,
            "port": report.port,
            "token_present": report.token_present,
            "checks": [asdict(c) for c in report.checks],
        }
        print(json.dumps(payload, indent=2))
    else:
        print_report(report)
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
