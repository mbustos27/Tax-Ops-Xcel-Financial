# M0 Hardware Validation — FINDINGS

Copy this file to `FINDINGS.md` (same directory) and fill it in while running
`print_test.py` + `scan_probe.py` against the real Arkscan 2054A + ScanAvenger,
per `README.md`. Leave `[ ]` unchecked items visibly unchecked rather than
guessing — M1/M2 are written to fail loudly on an unconfirmed assumption
rather than silently using the wrong one.

Date run: __________
Operator: __________
Printer Windows name used: __________
Scanner output mode confirmed: [ ] HID keyboard-wedge   [ ] Serial (COM___)

## 1. Barcode payload survival

- [ ] `LOG:00123` label printed, scanned, and compared byte-for-byte.
  - Exact string received (paste `repr()` output from scan_probe.py):
    `______________________`
  - Colon survived intact? [ ] YES  [ ] NO — if NO, what came through instead
    of `:`? ______________
- [ ] `STATUS:FINALIZE` label printed, scanned, and compared byte-for-byte.
  - Exact string received: `______________________`

## 2. Terminator / suffix character

- [ ] Terminator after each scan (check the one observed):
  [ ] CR (`\r`)   [ ] LF (`\n`)   [ ] CRLF (`\r\n`)   [ ] TAB (`\t`)   [ ] other: ______

## 3. Burst vs. live timing

- [ ] Buffered several scans in the gun's storage mode, released them at once.
  - Did all scans arrive, in the same order they were scanned? [ ] YES  [ ] NO
  - Approx. inter-record gap during burst replay: ______ ms
- [ ] Same scans repeated slowly, one at a time, several seconds apart.
  - Results identical (same records, same order) to the burst test? [ ] YES  [ ] NO

## 4. Label geometry sanity check

- [ ] Printed label physically measures 2.625" x 1" (or matches the roll on
  hand). [ ] YES  [ ] NO — actual size: __________
- [ ] Barcode is fully within the printable area, not clipped, not
  overlapping any text field. [ ] YES  [ ] NO

## 5. Decisions to feed back into `filetrack/config.py`

- `DEFAULT_SCAN_SUFFIX` should be: __________ (update the constant)
- `DEFAULT_SCAN_MODE` should be: __________ (hid / serial)
- Delimiter (`PREFIX_DELIMITER`) still `:` ? [ ] YES — no change needed
  [ ] NO — STOP, do not change silently, update `LOG_PREFIX`/`STATUS_PREFIX`/
  `PREFIX_DELIMITER` in `filetrack/config.py` AND regenerate any already-
  printed labels.
- Once all of the above is filled in, set `HARDWARE_FINDINGS_CONFIRMED = True`
  in `filetrack/config.py`.
