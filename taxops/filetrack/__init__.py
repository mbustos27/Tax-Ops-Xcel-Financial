"""filetrack — physical file-tracking module for TaxOps.

Barcode-driven tracking of paper files: a Code128 label on each file folder
(`LOG:<number>`) and Code128 labels at each workstation (`STATUS:<NAME>`).
A wireless scanner feeds both prefixes through the same stream; the listener
(filetrack.listener) tells them apart by the `PREFIX:VALUE` scheme and
maintains "sticky" status so a whole stack of files can be scanned against
one status scan.

Submodules:
  filetrack.config             — single canonical source for the allowed
                                  status set and hardware defaults, shared by
                                  the listener (M2) and the TaxOps service
                                  layer (M3).
  filetrack.hardware_validation — M0: standalone scripts an operator runs
                                  against the real Arkscan/ScanAvenger to
                                  populate FINDINGS.md. Not imported by
                                  anything else.
  filetrack.labels             — M1: pure ZPL rendering + win32print RAW
                                  sending. No Flask, no DB.
  filetrack.listener           — M2: scan parsing + sticky-status state
                                  machine + pluggable sink. No TaxOps import.
  filetrack.service / .routes  — M3: TaxOps-integrated service layer +
                                  internal endpoint (added once M1/M2 land).

This module is entirely inert until wired up in M3 — importing filetrack.*
submodules from app.py is what turns any of this "on".
"""
