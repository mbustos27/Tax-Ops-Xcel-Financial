"""filetrack.labels.printer — send raw ZPL to a Windows printer via win32print.

Single clean attempt, no retry logic (per M1 spec — retries are explicitly
out of scope for M1). `pywin32` is imported lazily so this module (and the
rest of filetrack.labels) still imports cleanly on non-Windows dev machines;
only calling send_zpl()/list_printers() there raises PrinterUnavailableError.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    import win32print
    _HAVE_WIN32PRINT = True
    _IMPORT_ERROR: Exception | None = None
except ImportError as exc:  # pragma: no cover — exercised on non-Windows only
    win32print = None  # type: ignore
    _HAVE_WIN32PRINT = False
    _IMPORT_ERROR = exc


class PrinterUnavailableError(RuntimeError):
    """Raised when pywin32 is not installed / not usable on this platform."""


class PrinterNotFoundError(RuntimeError):
    """Raised when the requested (or default) printer name is not registered
    with Windows — i.e. win32print.OpenPrinter() would fail."""


class SpoolerError(RuntimeError):
    """Raised when the Windows print spooler rejects the job."""


def _require_win32print() -> None:
    if not _HAVE_WIN32PRINT:
        raise PrinterUnavailableError(
            "pywin32 is not installed or not usable on this platform. "
            "filetrack.labels.printer requires Windows + pywin32 "
            "(`pip install -r filetrack/requirements.txt`). "
            f"Original import error: {_IMPORT_ERROR}"
        )


def list_printers() -> list[str]:
    """Return the names of all printers Windows currently knows about
    (local + connected network printers) — same set win32print.OpenPrinter()
    would accept as `printer_name`."""
    _require_win32print()
    # PRINTER_ENUM_LOCAL | PRINTER_ENUM_CONNECTIONS == 2. At the default
    # info level, win32print.EnumPrinters returns a list of
    # (flags, description, name, comment) tuples — NOT dicts (there is no
    # "pPrinterName" key to index into; that was this function's bug before
    # this fix — see filetrack/hardware_validation/print_test.py for the
    # tuple-unpacking form this mirrors).
    return [name for (_flags, _description, name, _comment) in win32print.EnumPrinters(2)]


def send_zpl(zpl: str, printer_name: str | None = None) -> None:
    """Send raw ZPL to a Windows printer via StartDocPrinter(..., 'RAW') +
    WritePrinter, with a clean StartPagePrinter/EndPagePrinter/EndDocPrinter/
    ClosePrinter teardown in `finally` no matter what fails.

    printer_name defaults to filetrack.config.DEFAULT_PRINTER_NAME
    (FILETRACK_PRINTER env var) when not given explicitly.
    """
    _require_win32print()

    if printer_name is None:
        from filetrack.config import DEFAULT_PRINTER_NAME
        printer_name = DEFAULT_PRINTER_NAME
    if not printer_name:
        raise PrinterNotFoundError(
            "No printer name given and FILETRACK_PRINTER is not set. "
            "Pass printer_name= explicitly or set the FILETRACK_PRINTER env var. "
            "Use list_printers() to see available names."
        )

    try:
        handle = win32print.OpenPrinter(printer_name)
    except Exception as exc:
        raise PrinterNotFoundError(
            f"Could not open printer {printer_name!r}. "
            f"Available printers: {list_printers()}. Original error: {exc}"
        ) from exc

    try:
        try:
            job_id = win32print.StartDocPrinter(handle, 1, ("filetrack label", None, "RAW"))
        except Exception as exc:
            raise SpoolerError(f"StartDocPrinter failed for {printer_name!r}: {exc}") from exc

        try:
            try:
                win32print.StartPagePrinter(handle)
            except Exception as exc:
                raise SpoolerError(f"StartPagePrinter failed for {printer_name!r}: {exc}") from exc

            try:
                win32print.WritePrinter(handle, zpl.encode("utf-8"))
            except Exception as exc:
                raise SpoolerError(f"WritePrinter failed for {printer_name!r}: {exc}") from exc
            finally:
                try:
                    win32print.EndPagePrinter(handle)
                except Exception:
                    logger.exception("EndPagePrinter cleanup failed for %r", printer_name)
        finally:
            try:
                win32print.EndDocPrinter(handle)
            except Exception:
                logger.exception("EndDocPrinter cleanup failed for %r", printer_name)
    finally:
        try:
            win32print.ClosePrinter(handle)
        except Exception:
            logger.exception("ClosePrinter cleanup failed for %r", printer_name)

    logger.info("Sent %d bytes of ZPL to printer %r (job %s)", len(zpl.encode("utf-8")), printer_name, job_id)
