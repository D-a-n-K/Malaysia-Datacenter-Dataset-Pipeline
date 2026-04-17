# v7 → v7.1 fix-pass report

**Date:** 2026-04-17

- v7 rows: 134
- v7.1 rows: **133** (expected 132–133)
- Deletions: 1
- ✅ Row count within expected range.

## Per-fix outcomes

- **Fix 1 — AWS double-pin**: applied (2 change-log entries)
- **Fix 2 — DayOne/GDS naming collision**: flagged_for_user (keep-both per building-granularity default) (2 change-log entries)
- **Fix 3 — NTT CBJ rename + coord-fill**: partial (rename applied, geocode failed) (4 change-log entries)
- **Fix 4 — Google/i-City collision flag**: flagged_for_user (1 change-log entries)
- **Fix 5 — Port Dickson pair flag**: flagged_for_user (2 change-log entries)
- **Fix 6 — Racks Central / YTL / NTP documentation**: applied (doc-only) (9 change-log entries)

## Fix-2 decision record

v7_row_21 (`DayOne (Johor)`, empty source/note, specific Gelang Patah street address) and v7_row_33 (`GDS Nusajaya NTP1 (Johor)`, NTP1-specific name, GDS press-release provenance) sit at identical coords. v7_row_21's name/note do not explicitly name NTP1, but neither do they name NTP2 or NTP3. The task's decision rules make this an ambiguous case. Per the user's building-level-granularity preference and the explicit guardrail `"Do NOT auto-delete if this is ambiguous"`, both rows were kept: v7_row_21 was renamed to `DayOne Nusajaya Tech Park Campus` to signal the campus-level role, and both rows carry `review:campus_vs_building_decision` flags for user triage.

## Fix-3 geocode outcome

- Geocode failed; coords left blank, coord_confidence=unknown — outcome: `skipped_missing_data`

## Rows whose `v7_pending_review` was cleared

- v7_row_73 → v7.1_row_72: Cleared NTT-specific pending_review flags (not_duplicate_per_dedup_5 + rename recommendation)

## Rows whose `v7_pending_review` was appended

- v7_row_33 → v7.1_row_33 (fix 2): Cross-ref flag on NTP1 row pointing to campus pin (v7_row_21)
- v7_row_69 → v7.1_row_68 (fix 4): Flagged Google Selangor coord as likely Nominatim city-centroid artifact colliding with i-City cluster
- v7_row_127 → v7.1_row_126 (fix 5): Flagged Google/Gamuda Port Dickson coord-sharing ambiguity
- v7_row_128 → v7.1_row_127 (fix 5): Flagged Google/Gamuda Port Dickson coord-sharing ambiguity

## Sanity: rows modified outside the 6 fixes

✅ All 16 touched rows are within scope: [0, 21, 33, 34, 35, 53, 54, 55, 68, 69, 73, 104, 105, 106, 127, 128]

## Outcome tally

- `applied`: 14
- `flagged_for_user`: 5
- `skipped_missing_data`: 1
