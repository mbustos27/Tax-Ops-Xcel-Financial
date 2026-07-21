# M0 — Hardware validation harness

**Status: NOT YET RUN.** No `FINDINGS.md` exists in this directory. M1's
`filetrack.labels` and M2's `filetrack.listener` were built with reasonable,
fully-overridable defaults (see the "⚠ PENDING physical confirmation" block
in `filetrack/config.py`) but neither has been confirmed against the actual
Arkscan 2054A printer or ScanAvenger scanner. **Run this before trusting
filetrack in production**, and before wiring the listener into TaxOps (M3).

This directory has no dependency on the rest of `filetrack/` — it's meant to
be run standalone, on the Windows box that has the printer/dongle physically
attached, by whoever has the hardware in front of them.

## Procedure

### 1. Confirm the printer sends/receives ZPL correctly

```
python -m filetrack.hardware_validation.print_test --printer "Arkscan 2054A"
```

This prints 3 small test labels (list printers with `--list-printers` first
if you don't know the exact Windows printer name):

1. A label whose barcode encodes `LOG:00123` (the exact scheme M1 will use).
2. A label whose barcode encodes `STATUS:FINALIZE` (the exact scheme M3's
   status labels will use).
3. A label with a few likely-troublesome characters in a barcode payload
   (colon, hyphen) to double-check the colon survives.

Use `--dry-run` to print the ZPL to stdout instead of sending it, if you want
to eyeball it first.

### 2. Confirm what the scanner actually sends

Plug in the ScanAvenger dongle. It ships in HID keyboard-wedge mode by
default — scanning anything "types" the payload into whatever window has
focus, followed by a terminator key (usually Enter).

```
python -m filetrack.hardware_validation.scan_probe --mode hid
```

Then scan the three labels printed in step 1, one at a time, in order.
`scan_probe.py` prints (and appends to `scan_probe_log.jsonl`) for each scan:

- the **exact repr()** of what was received, including any terminator
  characters, so you can see if it was CR (`\r`), LF (`\n`), CRLF, or TAB;
- whether the `:` delimiter survived intact (`LOG:00123` vs. e.g. `LOG00123`
  or `LOG;00123` if the scanner's keyboard layout mapping mangled it);
- the wall-clock gap since the previous scan, so you can also do a burst
  test: hold several file labels' worth of scans in the gun's buffer
  (storage mode) and release them all at once, then compare the timing
  pattern against slow, deliberate single scans.

If your scanner is configured for serial (COM port) output instead of HID,
use `--mode serial --port COM3` (adjust the port).

### 3. Fill in FINDINGS.md

Copy `FINDINGS_TEMPLATE.md` to `FINDINGS.md` in this same directory and fill
in the blanks from what `scan_probe.py` showed you. This is the file M1/M2
were written to read defaults from (`filetrack/config.py` — update the
`DEFAULT_SCAN_SUFFIX` / colon-survival notes there once you know for sure,
and flip `HARDWARE_FINDINGS_CONFIRMED = True`).

**If the colon was stripped, split, or mangled**, do not just pick a new
delimiter and move on — flag it back so `filetrack.labels.template` (the
barcode payload builder) and `filetrack.listener.parser` (the barcode payload
reader) both get updated consistently, together, in the same change.
