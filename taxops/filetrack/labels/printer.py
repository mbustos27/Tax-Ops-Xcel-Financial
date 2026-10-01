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


def send_pdf(
    pdf_bytes: bytes,
    printer_name: str | None = None,
    *,
    doc_name: str = "taxops letter",
) -> None:
    """Silently print a PDF to a Windows letter printer (no browser dialog).

    Writes bytes to a temp ``.pdf`` and uses ``ShellExecute`` ``printto`` so
    the installed printer driver renders the job (Ricoh / PCL / etc.). This
    path is for 8.5×11 docs — never send these to the ZPL label queue.

    ``printer_name`` defaults to ``LETTER_PRINTER_NAME`` (TAXOPS_LETTER_PRINTER
    / FILETRACK_LETTER_PRINTER). Raises PrinterNotFoundError if unset.
    """
    import os
    import tempfile
    import time

    _require_win32print()
    import win32api  # type: ignore

    if printer_name is None:
        from filetrack.config import LETTER_PRINTER_NAME
        printer_name = LETTER_PRINTER_NAME
    if not printer_name:
        raise PrinterNotFoundError(
            "No letter printer configured. Set TAXOPS_LETTER_PRINTER "
            "(or FILETRACK_LETTER_PRINTER) on the print-relay machine to the "
            "exact Windows queue name for the 8.5×11 printer "
            "(e.g. \"RICOH C5502 Printer\"). "
            f"Available printers: {list_printers()}."
        )
    if not pdf_bytes or not pdf_bytes.startswith(b"%PDF"):
        raise SpoolerError("send_pdf requires PDF bytes starting with %PDF")

    known = list_printers()
    if printer_name not in known:
        raise PrinterNotFoundError(
            f"Letter printer {printer_name!r} not found. Available: {known}."
        )

    fd, path = tempfile.mkstemp(prefix="taxops_letter_", suffix=".pdf")
    try:
        os.write(fd, pdf_bytes)
        os.close(fd)
        fd = -1
        # printto: silent path through the Windows driver — no browser dialog.
        win32api.ShellExecute(0, "printto", path, f'"{printer_name}"', ".", 0)
        # Give the spooler a moment to open the temp file before we delete it.
        time.sleep(2.0)
        logger.info(
            "Sent %d-byte PDF (%s) to letter printer %r",
            len(pdf_bytes), doc_name, printer_name,
        )
    except PrinterNotFoundError:
        raise
    except Exception as exc:
        raise SpoolerError(
            f"ShellExecute printto failed for {printer_name!r}: {exc}"
        ) from exc
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except Exception:
                pass
        try:
            os.remove(path)
        except Exception:
            logger.warning("Could not remove temp PDF %s (spooler still holding it)", path)
