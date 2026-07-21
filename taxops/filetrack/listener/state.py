"""filetrack.listener.state — sticky-status state machine.

Pure logic, no I/O, no wall-clock reads (callers/run_listener attach a
timestamp when they actually submit to a sink) — this is what makes it
trivially unit-testable and what proves burst vs. live replay are
equivalent (order is all that matters, never timing).

Behavior (locked, from the M2 spec):
  - active_status starts None.
  - A 'status' record with a NAME in the allowed set sets active_status =
    NAME. An unknown NAME is rejected and the PRIOR active_status is left
    untouched (not cleared).
  - A 'log' record, while active_status is set, produces an Assignment
    (log_number, active_status). active_status is NOT consumed/cleared by
    this — it stays sticky across many LOG scans (a whole stack of files
    scanned against one status) until a different STATUS scan changes it.
  - A 'log' record while active_status is None is rejected — no default is
    ever assigned.
  - An 'unknown' record (unrecognized prefix / malformed) is rejected —
    the caller (run_listener) logs it and moves on; it never raises.
"""
from __future__ import annotations

from dataclasses import dataclass

from filetrack.config import ALLOWED_STATUSES
from filetrack.listener.parser import RECORD_KIND_LOG, RECORD_KIND_STATUS, RECORD_KIND_UNKNOWN


@dataclass(frozen=True)
class Assignment:
    """A file (log_number) should be recorded at `status`. Carries no
    timestamp — pure logic; the caller stamps ts when it reaches the sink."""
    log_number: str
    status: str


@dataclass(frozen=True)
class Rejection:
    """Something did not produce an assignment. `reason` is a short,
    human-readable, log-friendly explanation. `kind`/`value` are the raw
    classified record that was rejected, for structured logging."""
    reason: str
    kind: str
    value: str


class StatusState:
    """Holds the single sticky `active_status` and applies scan records to it.

    One instance per listener process (or per test) — this is exactly the
    state a burst of scans mutates in order, which is why order (not timing)
    is what correctness depends on.
    """

    def __init__(self, allowed_statuses: tuple[str, ...] = ALLOWED_STATUSES):
        self._allowed = set(allowed_statuses)
        self.active_status: str | None = None

    def apply(self, kind: str, value: str) -> Assignment | Rejection | None:
        """Apply one classified (kind, value) record (see
        filetrack.listener.parser.classify) to the state machine.

        Returns:
          Assignment  — a LOG scan while a status was active.
          Rejection   — an unknown STATUS name, a LOG scan with no active
                        status, or an unrecognized prefix.
          None        — a STATUS scan that successfully changed
                        active_status. This is not an error and not an
                        assignment; run_listener still logs the scan itself
                        (every scan is logged per the M2 spec), it just has
                        no Assignment/Rejection outcome to route to a sink.
        """
        if kind == RECORD_KIND_STATUS:
            if value not in self._allowed:
                # Unknown status name: error signal, leave prior state (locked behavior).
                return Rejection(
                    reason=f"unknown status {value!r} — active_status left unchanged "
                           f"({self.active_status!r})",
                    kind=kind,
                    value=value,
                )
            self.active_status = value
            return None

        if kind == RECORD_KIND_LOG:
            if self.active_status is None:
                return Rejection(
                    reason="no active status — scan a STATUS label before any LOG label",
                    kind=kind,
                    value=value,
                )
            return Assignment(log_number=value, status=self.active_status)

        # RECORD_KIND_UNKNOWN or anything unrecognized — never crash the loop.
        return Rejection(reason="unrecognized scan prefix", kind=RECORD_KIND_UNKNOWN, value=value)
