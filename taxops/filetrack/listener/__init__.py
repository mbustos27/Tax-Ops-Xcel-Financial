"""filetrack.listener — M2: scan parsing + sticky-status state machine +
pluggable sink. No TaxOps import (no app.py/db.py), no DB, no routes — the
sink is how M3 wires this into TaxOps without this package knowing it."""
