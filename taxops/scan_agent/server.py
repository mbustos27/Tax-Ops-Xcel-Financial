"""TaxOps Scan Agent — local WIA scanner service for the reception workstation.

Modeled on filetrack.relay.server: small Flask app, NSSM-friendly, token auth.

Run (on the PC with the Epson scanner):
    set SCAN_AGENT_TOKEN=secret
    python -m scan_agent.server --port 8766

NSSM service name: ScanAgent (see filetrack/DEPLOYMENT.md § Scan Agent).
"""
from __future__ import annotations

import sys

# WIA.DeviceManager requires an STA apartment. pywin32 defaults to MTA unless
# this is set BEFORE the first pythoncom import on a thread.
if sys.platform == "win32":
    sys.coinit_flags = 2  # COINIT_APARTMENTTHREADED

import argparse
import contextlib
import hmac
import io
import logging
import os
import queue
import subprocess
import tempfile
import threading
from typing import Any, Callable, TypeVar

logger = logging.getLogger("scan_agent")

REQUIRED_CODE_REV = "com_sta_v4"

DEFAULT_HOST = os.environ.get("SCAN_AGENT_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("SCAN_AGENT_PORT", "8766"))
SCAN_AGENT_TOKEN = os.environ.get("SCAN_AGENT_TOKEN", "")

_T = TypeVar("_T")


class ScanError(RuntimeError):
    """Scanner hardware / WIA failure."""


def _require_token(token_header: str, expected: str) -> tuple[int, dict] | None:
    """Mandatory token — reject if unset or mismatch. Returns error body or None."""
    if not expected:
        return 401, {
            "error": "SCAN_AGENT_TOKEN is not configured on this agent — refusing all scans"
        }
    if not hmac.compare_digest(token_header or "", expected):
        return 401, {"error": "Unauthorized"}
    return None


class _StaWiaPump:
    """Long-lived STA thread: CoInitializeEx once, run all WIA work here.

    Flask's threaded workers must NOT call CoInitialize themselves — pywin32/win32com
    often leave those threads without a usable apartment, which surfaces as
    "CoInitialize has not been called" on WIA.DeviceManager.
    """

    _instance: "_StaWiaPump | None" = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self._jobs: queue.Queue = queue.Queue()
        self._ready = threading.Event()
        self._init_error: BaseException | None = None
        self._init_detail = ""
        self._thread = threading.Thread(
            target=self._loop, name="wia-sta-pump", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(20):
            raise ScanError("WIA STA thread did not start within 20s")
        if self._init_error is not None:
            raise ScanError(
                f"WIA STA CoInitializeEx failed: {self._init_error} ({self._init_detail})"
            )

    @classmethod
    def get(cls) -> "_StaWiaPump":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def _loop(self) -> None:
        if sys.platform == "win32":
            sys.coinit_flags = 2  # COINIT_APARTMENTTHREADED
        pythoncom = None
        try:
            import pythoncom as _pythoncom  # type: ignore

            pythoncom = _pythoncom
            # Prefer explicit STA. S_OK (0) and S_FALSE (1) are both success.
            hr = pythoncom.CoInitializeEx(pythoncom.COINIT_APARTMENTTHREADED)
            self._init_detail = f"CoInitializeEx hr={hr!r}"
            self._ready.set()
        except Exception as exc:
            # Last resort: ole32 directly, then pythoncom.CoInitialize
            try:
                import ctypes

                ole_hr = int(ctypes.windll.ole32.CoInitializeEx(None, 0x2))
                if ole_hr not in (0, 1):
                    raise RuntimeError(f"ole32.CoInitializeEx HRESULT=0x{ole_hr & 0xFFFFFFFF:08X}")
                import pythoncom as _pythoncom  # type: ignore

                pythoncom = _pythoncom
                try:
                    pythoncom.CoInitializeEx(pythoncom.COINIT_APARTMENTTHREADED)
                except Exception:
                    pythoncom.CoInitialize()
                self._init_detail = f"ole32+pythoncom ok (after {exc})"
                self._ready.set()
            except Exception as exc2:
                self._init_error = exc2
                self._init_detail = f"primary={exc!r}"
                self._ready.set()
                return

        try:
            while True:
                job = self._jobs.get()
                if job is None:
                    break
                fn, args, kwargs, box, done = job
                try:
                    box["result"] = fn(*args, **kwargs)
                except BaseException as exc:  # noqa: BLE001 — deliver to caller
                    box["error"] = exc
                finally:
                    done.set()
        finally:
            if pythoncom is not None:
                try:
                    pythoncom.CoUninitialize()
                except Exception:
                    pass

    def call(self, fn: Callable[..., _T], *args: Any, timeout: float = 120.0, **kwargs: Any) -> _T:
        box: dict[str, Any] = {}
        done = threading.Event()
        self._jobs.put((fn, args, kwargs, box, done))
        if not done.wait(timeout):
            raise ScanError(
                f"WIA STA call timed out after {timeout:.0f}s "
                "(often phantom Epson USB — unplug, power-cycle, Device Manager OK)."
            )
        if "error" in box:
            err = box["error"]
            if isinstance(err, ScanError):
                raise err
            raise ScanError(str(err)) from err
        return box.get("result")  # type: ignore[return-value]


def _wia_call(fn: Callable[..., _T], *args: Any, timeout: float = 120.0, **kwargs: Any) -> _T:
    """Run callable on the process-wide WIA STA thread (CoInitializeEx already done)."""
    return _StaWiaPump.get().call(fn, *args, timeout=timeout, **kwargs)


@contextlib.contextmanager
def _com_sta():
    """Compat wrapper: ensure STA pump is up (tests look for this name)."""
    _StaWiaPump.get()
    yield


def _epson_device_tips() -> list[str]:
    """Windows PnP tips when WIA sees nothing (phantom USB / powered off)."""
    tips: list[str] = []
    if sys.platform != "win32":
        return tips
    try:
        ps = (
            "$devs = Get-PnpDevice -ErrorAction SilentlyContinue | "
            "Where-Object { $_.FriendlyName -match 'EPSON|Epson|ES-500|ES-400' }; "
            "if (-not $devs) { 'NONE'; exit 0 }; "
            "$devs | ForEach-Object { "
            "  $prob = if ($_.Problem) { [string]$_.Problem } else { '' }; "
            "  '{0}|{1}|{2}' -f $_.FriendlyName, $_.Status, $prob "
            "}"
        )
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        out = (completed.stdout or "").strip()
        if not out or out == "NONE":
            tips.append(
                "No Epson in Device Manager — power on the scanner, use a data USB cable, "
                "try another port, then restart ScanAgent."
            )
            return tips
        phantom = False
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) < 2:
                continue
            name, status = parts[0], parts[1]
            prob = parts[2] if len(parts) > 2 else ""
            bad = status.upper() not in ("OK",) or "PHANTOM" in prob.upper() or "CM_PROB" in prob.upper()
            if bad:
                phantom = True
                tips.append(
                    f"Epson '{name}' shows {status}"
                    + (f" ({prob})" if prob else "")
                    + " — unplug USB 10s, power-cycle scanner, then Scan for hardware changes "
                    "(or re-run setup_reception_pc.ps1 Epson step) and restart ScanAgent."
                )
        if not phantom:
            tips.append(
                "Epson is listed in Device Manager but not in WIA — reinstall Epson Scan 2, "
                "reboot the reception PC, restart ScanAgent."
            )
    except Exception as exc:
        logger.debug("epson device tip probe failed: %s", exc)
    return tips


def _list_wia_scanners_impl() -> list[dict[str, Any]]:
    """Must run on the STA pump thread."""
    try:
        import win32com.client  # type: ignore
    except ImportError as exc:
        raise ScanError(
            "pywin32 is not installed — pip install pywin32 on this workstation"
        ) from exc

    devices: list[dict[str, Any]] = []
    try:
        mgr = win32com.client.Dispatch("WIA.DeviceManager")
        for i in range(1, mgr.DeviceInfos.Count + 1):
            info = mgr.DeviceInfos(i)
            try:
                dtype = int(info.Type)
            except Exception:
                dtype = -1
            if dtype != 1:
                continue
            name = ""
            try:
                name = str(info.Properties("Name").Value)
            except Exception:
                name = f"Scanner-{i}"
            devices.append({"name": name, "device_id": str(info.DeviceID), "type": dtype})
    except ScanError:
        raise
    except Exception as exc:
        raise ScanError(f"WIA DeviceManager failed: {exc}") from exc
    return devices


def list_wia_scanners() -> list[dict[str, Any]]:
    """Return Present WIA scanner devices (empty list if pywin32/WIA unavailable)."""
    return _wia_call(_list_wia_scanners_impl, timeout=30.0)


def list_wia_scanners_timed(timeout_sec: float = 4.0) -> list[dict[str, Any]]:
    """WIA DeviceManager can hang forever on phantom USB — never block health that long."""
    return _wia_call(_list_wia_scanners_impl, timeout=timeout_sec)


def _pick_wia_scanner_info(mgr: Any):
    """Prefer Epson ES-500 / ES-400; otherwise first WIA scanner."""
    preferred = []
    others = []
    for i in range(1, mgr.DeviceInfos.Count + 1):
        info = mgr.DeviceInfos(i)
        try:
            if int(info.Type) != 1:
                continue
        except Exception:
            continue
        name = ""
        try:
            name = str(info.Properties("Name").Value)
        except Exception:
            name = ""
        upper = name.upper()
        if "EPSON" in upper or "ES-500" in upper or "ES-400" in upper:
            preferred.append(info)
        else:
            others.append(info)
    if preferred:
        return preferred[0]
    if others:
        return others[0]
    return None


def _set_wia_prop(item: Any, prop_id: int, value: Any) -> None:
    try:
        item.Properties(prop_id).Value = value
    except Exception:
        pass


def _adf_feed_ready(device: Any) -> bool | None:
    """Return True/False if Document Handling Status is readable; None if unknown.

    WIA_DPS_DOCUMENT_HANDLING_STATUS (3087): FEED_READY = 0x1.
    """
    for key in (3087, "Document Handling Status"):
        try:
            status = int(device.Properties(key).Value)
            return bool(status & 0x1)
        except Exception:
            continue
    return None


def _is_adf_empty_error(exc: BaseException) -> bool:
    """WIA_ERROR_PAPER_EMPTY / 'no documents left in the document feeder'."""
    blob = str(exc).lower()
    if "no documents left" in blob or "paper empty" in blob:
        return True
    if "document feeder" in blob and ("no document" in blob or "empty" in blob):
        return True
    # HRESULT WIA_ERROR_PAPER_EMPTY = 0x80210003 = -2145320957
    if "-2145320957" in blob or "80210003" in blob:
        return True
    return False


def _wait_for_adf_pages(device: Any, *, timeout_sec: float = 25.0, poll_sec: float = 0.5) -> None:
    """Give staff time to load the feeder before the first Transfer."""
    import time

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        ready = _adf_feed_ready(device)
        if ready is True:
            logger.info("scan_agent: ADF FEED_READY")
            return
        if ready is False:
            logger.debug("scan_agent: waiting for pages in ADF…")
        time.sleep(poll_sec)
    logger.info(
        "scan_agent: ADF wait timed out after %.0fs — attempting Transfer anyway",
        timeout_sec,
    )


def _scan_pages_wia_impl(*, handwriting: bool = False, max_pages: int = 50) -> list[bytes]:
    """Must run on the STA pump thread."""
    import time

    try:
        import win32com.client  # type: ignore
    except ImportError as exc:
        raise ScanError(
            "pywin32 is not installed — pip install pywin32 on this workstation"
        ) from exc

    mgr = win32com.client.Dispatch("WIA.DeviceManager")
    scanner_info = _pick_wia_scanner_info(mgr)
    if scanner_info is None:
        raise ScanError(
            "No WIA scanner found. Power on the Epson, fix USB (status OK in Device Manager), "
            "install Epson Scan 2, then restart the ScanAgent service."
        )

    try:
        scanner_name = str(scanner_info.Properties("Name").Value)
    except Exception:
        scanner_name = "scanner"
    logger.info("scan_agent: using WIA device %r", scanner_name)

    device = scanner_info.Connect()
    # Prefer ADF / feeder when the driver exposes Document Handling Select (3088).
    # 1 = Feeder, 2 = Flatbed (WIA_DPS_DOCUMENT_HANDLING_SELECT).
    try:
        _set_wia_prop(device, 3088, 1)
    except Exception:
        pass

    # Staff often open the modal before pages are seated — wait for FEED_READY.
    _wait_for_adf_pages(device, timeout_sec=25.0, poll_sec=0.5)

    if device.Items.Count < 1:
        raise ScanError("Scanner connected but exposes no WIA items")

    item = device.Items(1)

    _set_wia_prop(item, 6147, 300)  # horizontal dpi
    _set_wia_prop(item, 6148, 300)  # vertical dpi
    if handwriting:
        _set_wia_prop(item, 6146, 2)  # grayscale
    else:
        _set_wia_prop(item, 6146, 1)  # color / text

    jpeg_fmt = "{B96B3CAE-0728-11D3-9D7B-0000F81EF32E}"
    pages: list[bytes] = []

    empty_retries = 8
    empty_delay_sec = 2.0

    for _ in range(max_pages):
        image = None
        last_exc: BaseException | None = None
        for attempt in range(empty_retries):
            try:
                image = item.Transfer(jpeg_fmt)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if pages:
                    # End of batch after at least one page — normal ADF empty.
                    break
                if _is_adf_empty_error(exc) and attempt < empty_retries - 1:
                    logger.info(
                        "scan_agent: ADF empty on Transfer, retry %d/%d in %.1fs",
                        attempt + 1,
                        empty_retries,
                        empty_delay_sec,
                    )
                    time.sleep(empty_delay_sec)
                    continue
                break
        if pages and image is None:
            break
        if image is None:
            raise ScanError(
                f"WIA Transfer failed on {scanner_name}: {last_exc}. "
                "Load pages in the ADF, close the cover, and try again."
            ) from last_exc

        tmp_path = None
        try:
            # Never create the file first — WIA ImageFile.SaveFile fails with
            # "The file exists" (ERROR_FILE_EXISTS / -2147024816) if the path
            # already exists (mkstemp creates an empty file).
            import uuid

            tmp_path = os.path.join(
                tempfile.gettempdir(),
                f"taxops_wia_{os.getpid()}_{uuid.uuid4().hex}.jpg",
            )
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            try:
                image.SaveFile(tmp_path)
            except Exception as save_exc:
                # One retry on a fresh path if somehow still colliding.
                if "file exists" in str(save_exc).lower() or "-2147024816" in str(save_exc):
                    tmp_path = os.path.join(
                        tempfile.gettempdir(),
                        f"taxops_wia_{os.getpid()}_{uuid.uuid4().hex}.jpg",
                    )
                    image.SaveFile(tmp_path)
                else:
                    raise
            with open(tmp_path, "rb") as fh:
                pages.append(fh.read())
        finally:
            if tmp_path and os.path.isfile(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    if not pages:
        raise ScanError("Scan produced zero pages — load the ADF and try again")
    return pages


def _scan_pages_wia(*, handwriting: bool = False, max_pages: int = 50) -> list[bytes]:
    """Acquire page image bytes (JPEG) from WIA (Epson preferred, ADF when available)."""
    return _wia_call(
        _scan_pages_wia_impl,
        handwriting=handwriting,
        max_pages=max_pages,
        # Allow ADF wait (~25s) + empty-feeder retries (~16s) + multi-page scan.
        timeout=300.0,
    )


def pages_to_pdf(page_jpegs: list[bytes]) -> bytes:
    """Assemble JPEG page bytes into a single PDF (pymupdf)."""
    try:
        import fitz  # pymupdf
    except ImportError:
        # Fallback: Pillow multipage PDF
        from PIL import Image

        images = [Image.open(io.BytesIO(b)).convert("RGB") for b in page_jpegs]
        out = io.BytesIO()
        images[0].save(out, format="PDF", save_all=True, append_images=images[1:])
        return out.getvalue()

    doc = fitz.open()
    try:
        for data in page_jpegs:
            img = fitz.open(stream=data, filetype="jpeg")
            try:
                pdf_bytes = img.convert_to_pdf()
            finally:
                img.close()
            page_pdf = fitz.open("pdf", pdf_bytes)
            try:
                doc.insert_pdf(page_pdf)
            finally:
                page_pdf.close()
        return doc.tobytes()
    finally:
        doc.close()


def run_scan(*, handwriting: bool = False) -> tuple[bytes, int]:
    """Run a scan and return (pdf_bytes, page_count)."""
    pages = _scan_pages_wia(handwriting=handwriting)
    pdf = pages_to_pdf(pages)
    return pdf, len(pages)


def handle_scan_request(
    *,
    token_header: str,
    expected_token: str,
    handwriting: bool = False,
) -> tuple[int, dict | bytes, dict | None]:
    """
    Returns (status, body, headers).
    body is dict for JSON errors, or raw PDF bytes on success.
    """
    auth_err = _require_token(token_header, expected_token)
    if auth_err:
        return auth_err[0], auth_err[1], {"Content-Type": "application/json"}

    try:
        pdf, page_count = run_scan(handwriting=handwriting)
    except ScanError as exc:
        logger.error("scan_agent: %s", exc)
        return 500, {"error": str(exc)}, {"Content-Type": "application/json"}
    except Exception:
        logger.exception("scan_agent: unexpected scan failure")
        return 500, {"error": "internal scan error"}, {"Content-Type": "application/json"}

    headers = {
        "Content-Type": "application/pdf",
        "X-Scan-Page-Count": str(page_count),
        "Content-Disposition": 'attachment; filename="scan.pdf"',
    }
    return 200, pdf, headers


def create_app(*, token: str | None = None):
    from flask import Flask, jsonify, request, Response

    expected = token if token is not None else SCAN_AGENT_TOKEN
    app = Flask(__name__)

    # Do NOT start the WIA STA pump here — CoInitialize / device probe can stall
    # for many seconds and delay READY /health. Pump starts lazily on first WIA use.

    @app.after_request
    def _cors(resp):
        # Browser on Tax Log (taxlog / server) calls this agent on the reception PC.
        origin = request.headers.get("Origin") or "*"
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Headers"] = (
            "Content-Type, X-Scan-Agent-Token"
        )
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Expose-Headers"] = "X-Scan-Page-Count, Content-Disposition"
        return resp

    @app.route("/health", methods=["OPTIONS"])
    @app.route("/scan", methods=["OPTIONS"])
    def options_preflight():
        return ("", 204)

    @app.get("/health")
    def health():
        # Health still requires token when configured (plan: mandatory auth).
        auth_err = _require_token(
            request.headers.get("X-Scan-Agent-Token", ""), expected
        )
        if auth_err:
            return jsonify(auth_err[1]), auth_err[0]

        # Fast path: prove the agent process is alive WITHOUT touching WIA.
        # WIA.DeviceManager hangs for minutes on phantom Epson USB nodes.
        probe_wia = (request.args.get("wia") or "0").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        info: dict[str, Any] = {
            "status": "ok",
            "service": "scan_agent",
            "code_rev": REQUIRED_CODE_REV,
            "agent_ok": True,
            "scanner_ok": False,
            "scanner_found": False,
            "wia_probed": probe_wia,
            "com_sta": False,
            "wia_save": "uuid_path",  # SaveFile must not use mkstemp (file-exists bug)
        }
        # Prove CoInitializeEx ran on the dedicated STA thread (no WIA yet).
        try:
            info["com_sta"] = bool(
                _wia_call(lambda: True, timeout=3.0) is True
            )
            info["com_detail"] = getattr(_StaWiaPump.get(), "_init_detail", "") or "ok"
        except Exception as exc:
            info["com_sta"] = False
            info["com_error"] = str(exc)
            info["tips"] = [
                f"STA CoInitializeEx failed: {exc}. "
                "On reception: close all Scan Agent windows, run GO_SCAN_AGENT.bat."
            ]
            return jsonify(info)

        if not probe_wia:
            # No staff-facing tips here — TaxOps status must call ?wia=1.
            # A tip on this path was previously shown as "Scanner not ready."
            info["tips"] = []
            return jsonify(info)

        try:
            scanners = list_wia_scanners_timed(4.0)
            info["scanners"] = scanners
            info["scanner_found"] = len(scanners) > 0
            info["scanner_ok"] = len(scanners) > 0
            tips: list[str] = []
            if not scanners:
                tips.append(
                    "No WIA scanner — power on Epson, fix USB (Device Manager status OK), "
                    "install Epson Scan 2, then restart via GO_SCAN_AGENT.bat."
                )
                # PnP tip probe is optional and can also hang — keep short.
                try:
                    tips.extend(_epson_device_tips())
                except Exception:
                    pass
            else:
                names = " | ".join(s.get("name") or "?" for s in scanners)
                info["scanner_names"] = names
                if not any(
                    "EPSON" in (s.get("name") or "").upper()
                    or "ES-500" in (s.get("name") or "").upper()
                    for s in scanners
                ):
                    tips.append(
                        f"WIA sees {names!r} but not Epson — check Scan 2 / USB; "
                        "Scan Agent will use the first WIA scanner."
                    )
            info["tips"] = tips
        except ScanError as exc:
            info["scanner_found"] = False
            info["scanner_ok"] = False
            info["scanner_error"] = str(exc)
            tips = [str(exc)]
            if "CoInitialize" in str(exc):
                tips.insert(
                    0,
                    "Close this agent window and run \\\\Xcel-server\\taxops\\GO_SCAN_AGENT.bat "
                    "so com_sta_v4 loads (dedicated STA thread).",
                )
            info["tips"] = tips
        except Exception as exc:
            info["scanner_found"] = False
            info["scanner_ok"] = False
            info["scanner_error"] = str(exc)
            info["tips"] = [str(exc)]
        return jsonify(info)

    @app.post("/scan")
    def do_scan():
        body = request.get_json(silent=True) or {}
        handwriting = bool(body.get("handwriting") or request.args.get("handwriting"))
        status, payload, headers = handle_scan_request(
            token_header=request.headers.get("X-Scan-Agent-Token", ""),
            expected_token=expected,
            handwriting=handwriting,
        )
        if isinstance(payload, (bytes, bytearray)):
            return Response(bytes(payload), status=status, headers=headers or {})
        return jsonify(payload), status

    return app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TaxOps Scan Agent (WIA → PDF)")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--token", default=None, help="Overrides SCAN_AGENT_TOKEN")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    token = args.token if args.token is not None else SCAN_AGENT_TOKEN
    if not token:
        logger.error(
            "SCAN_AGENT_TOKEN is required — set the env var or pass --token. Refusing to start."
        )
        return 2

    app = create_app(token=token)
    # Bind first, THEN print READY — older log said "listening" before the socket opened,
    # which looked like a hang on 0.0.0.0 (0.0.0.0 only means "all interfaces").
    from werkzeug.serving import make_server

    server = make_server(args.host, args.port, app, threaded=True)
    logger.info(
        "READY build=%s  health=http://127.0.0.1:%d/health  (LAN bind %s:%d)",
        REQUIRED_CODE_REV,
        args.port,
        args.host,
        args.port,
    )
    print(
        f"READY {REQUIRED_CODE_REV}  http://127.0.0.1:{args.port}/health  "
        f"(accepting LAN on {args.host}:{args.port})",
        flush=True,
    )
    # Warm STA pump in the background after we are already accepting /health.
    if sys.platform == "win32":

        def _warm_sta() -> None:
            try:
                _StaWiaPump.get()
                logger.info("WIA STA pump ready (%s)", REQUIRED_CODE_REV)
            except Exception as exc:
                logger.error("WIA STA pump failed to start: %s", exc)

        threading.Thread(target=_warm_sta, name="wia-sta-warm", daemon=True).start()
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
