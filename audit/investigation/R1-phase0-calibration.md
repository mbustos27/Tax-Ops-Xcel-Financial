# R1 Phase 0 — export defects (calibration)

_Generated: 2026-08-13T20:20:36Z_
_Canonical ingest: `T:\audit\investigation\exports\TAXPAYERspouseaddressstatus.csv` · sha256 `024cc87af2275246682ca4e4498ddf708b151ab293051cddb36743829e87b415`_
_Drake source (fragile): `F:\DRAKE25\DT\5\4F24A262\Documents\TAXPAYERspouseaddressstatus.csv`_

| Check | Result |
|---|---|
| Data rows | 1118 |
| Ragged | **110** `{17: 75, 19: 35, 25: 1008}` |
| City/State/ZIP cols | False/False/False |
| Street max / state-token | 32 / **0** |
| Invoice fill | 912 / 1118 (81.6%) |
| Strict `^25\d{4}$` | **885** |
| Non-canonical (office worklist) | **23** distinct / 27 rows |
| Hazards (lenient→141) | **1** |

See `R1-malformed-invoice-worklist.md`. **Do not bare-parse non-canonical invoices.**
