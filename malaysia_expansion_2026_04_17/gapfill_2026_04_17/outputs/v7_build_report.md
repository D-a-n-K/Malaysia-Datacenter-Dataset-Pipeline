# v7 Master Build Report

**Date:** 2026-04-17
**Mode:** Option 1 (Step 4 `rejected_resolution` skipped — file does not yet exist)

## Known gaps carried forward to v8

5 rows rejected in `v6_gapfill_candidates` with `promotion_action = reject:sole_source_tos_blocked` were NOT included in v7. Their resolution (via SSM, MCMC, planning portals, and operator sources) is a separate pass not yet run. These are known-unknowns: the facilities likely exist but lack legitimate independent sourcing in the current pipeline.

- **VADS Semarak Data Centre** — operator: `VADS Berhad` — locality: ``
- **VADS Bayan Baru Data Centre (Penang)** — operator: `VADS Berhad` — locality: ``
- **VADS Labuan Data Centre** — operator: `VADS Berhad` — locality: ``
- **AWS KUL Dengkil Site (ap-southeast-5 AZ)** — operator: `Amazon Data Services Malaysia Sdn Bhd` — locality: ``
- **AWS KUL Bukit Jalil Site (ap-southeast-5 AZ)** — operator: `Amazon Data Services Malaysia Sdn Bhd` — locality: ``

## Pre-flight vs actual row count

```
v7_estimate =  101  v6 master
            +  30  gap add_new
            +  4  gap review:*
            +  1  gap hold:*
            -  2  dedup merge_confirmed
            -  0  dedup merge_reversed
            = 134

v7_actual   = 134
```

✅ Match.

**Note on user-supplied stub formula:** the task message gave `count(gapfill.review:*) = 3` as the stub value. The actual file has 4 rows whose `promotion_action` starts with `review:` — `review:cotenant_of_HDC…`, `review:possible_dup_AIMS Bangunan AIMS / KL2`, `review:possible_dup_Open DC CJ1`, `review:possible_dup_Open DC PE1`. I used the file's actual count; if only 3 were expected, one of these should be reclassified before v8.

## Row-count summary

- v6 master: **101** rows
- v7 master: **134** rows
- Net delta: **+33**

### v7_status distribution
- `inherited`: 60
- `pending_review`: 54
- `promoted`: 18
- `updated`: 2

## Per-category outcomes

### Gapfill
- `add_new` promoted: **30** rows appended to v7
- `review:*` flagged: **4** rows appended as `pending_review`
- `hold:*` flagged: **1** rows appended as `pending_review`
- `update_existing:*` applied: **1** rows merged into base
- `skip:*` logged: **1** (no v7 row produced)
- `reject:*` carried to v8 gap: **5** (no v7 row produced)

### Dedup
- `merge_confirmed`: 2
- `not_duplicate_keep_both`: 2
- `review`: 1
- `review_campus`: 1

### Rejected resolution (Step 4)
- **Skipped** — `v6_rejected_resolution.csv` does not yet exist. See "Known gaps carried forward to v8" above for the 5 facilities awaiting resolution.

## Final dedup sweep

- Potential duplicates surfaced (same operator, <150m): **8**
- Cotenant pairs surfaced (different operator, <150m): **42**

### Potential duplicates
- v7_row_0 `AWS Asia Pacific (Malaysia)` ↔ v7_row_68 `AWS facility (Kuala Lumpur)` @ 11m
- v7_row_33 `GDS Nusajaya NTP1 (Johor)` ↔ v7_row_34 `GDS Nusajaya NTP2 (Johor)` @ 111m
- v7_row_34 `GDS Nusajaya NTP2 (Johor)` ↔ v7_row_35 `GDS Nusajaya NTP3 (Johor)` @ 111m
- v7_row_53 `YTL Johor Data Center 1 (Kulai)` ↔ v7_row_54 `YTL Johor Data Center 2 (Kulai)` @ 111m
- v7_row_54 `YTL Johor Data Center 2 (Kulai)` ↔ v7_row_55 `YTL Johor Data Center 3 (Kulai)` @ 111m
- v7_row_104 `Racks Central RCJM1 (Pasir Gudang)` ↔ v7_row_105 `Racks Central RCJM2A (Pasir Gudang)` @ 0m
- v7_row_104 `Racks Central RCJM1 (Pasir Gudang)` ↔ v7_row_106 `Racks Central RCJM2B (Pasir Gudang)` @ 0m
- v7_row_105 `Racks Central RCJM2A (Pasir Gudang)` ↔ v7_row_106 `Racks Central RCJM2B (Pasir Gudang)` @ 0m

### Cotenant pairs
- v7_row_0 `AWS Asia Pacific (Malaysia)` (AWS) ↔ v7_row_2 `Azure Malaysia West` (Azure) @ 0m
- v7_row_2 `Azure Malaysia West` (Azure) ↔ v7_row_68 `AWS facility (Kuala Lumpur)` (AWS) @ 11m
- v7_row_6 `Bridge Data Centres MY02 (Cyberjaya)` (Bridge Data Centres) ↔ v7_row_42 `VADS CBJ6 (Cyberjaya)` (VADS (TM subsidiary)) @ 0m
- v7_row_6 `Bridge Data Centres MY02 (Cyberjaya)` (Bridge Data Centres) ↔ v7_row_46 `Hitachi Sunway DC CX2 (Cyberjaya)` (Hitachi Sunway) @ 24m
- v7_row_6 `Bridge Data Centres MY02 (Cyberjaya)` (Bridge Data Centres) ↔ v7_row_62 `PLTPro CX2 (Cyberjaya)` (PLTPro) @ 27m
- v7_row_8 `AIMS Bangunan AIMS / KL2 (Kuala Lumpur)` (AIMS) ↔ v7_row_131 `Exabytes International Data Center (Kuala Lumpur)` (Exabytes) @ 43m
- v7_row_14 `Telekom Malaysia IPDC (Iskandar Puteri)` (Telekom Malaysia) ↔ v7_row_34 `GDS Nusajaya NTP2 (Johor)` (GDS / DayOne) @ 144m
- v7_row_17 `AirTrunk JHB1 (Johor)` (AirTrunk) ↔ v7_row_23 `EdgeConneX Johor` (EdgeConneX) @ 0m
- v7_row_19 `Vantage Cyberjaya Campus 2` (Vantage Data Centers) ↔ v7_row_31 `Equinix KL1 (Cyberjaya)` (Equinix) @ 77m
- v7_row_19 `Vantage Cyberjaya Campus 2` (Vantage Data Centers) ↔ v7_row_43 `VADS CBJ8 (Cyberjaya)` (VADS (TM subsidiary)) @ 47m
- v7_row_19 `Vantage Cyberjaya Campus 2` (Vantage Data Centers) ↔ v7_row_59 `Open DC CJ1 (Cyberjaya)` (Open DC) @ 46m
- v7_row_19 `Vantage Cyberjaya Campus 2` (Vantage Data Centers) ↔ v7_row_132 `Exabytes CJ1 Data Centre (Cyberjaya)` (Exabytes) @ 54m
- v7_row_21 `DayOne (Johor)` (DayOne) ↔ v7_row_28 `Empyrion Digital MY1 (Nusajaya, Johor)` (Empyrion Digital) @ 0m
- v7_row_21 `DayOne (Johor)` (DayOne) ↔ v7_row_33 `GDS Nusajaya NTP1 (Johor)` (GDS / DayOne) @ 0m
- v7_row_21 `DayOne (Johor)` (DayOne) ↔ v7_row_34 `GDS Nusajaya NTP2 (Johor)` (GDS / DayOne) @ 111m
- v7_row_22 `EdgeConneX Cyberjaya` (EdgeConneX) ↔ v7_row_37 `STT Cyberjaya DC.2` (STT GDC / Basis Bay) @ 0m
- v7_row_28 `Empyrion Digital MY1 (Nusajaya, Johor)` (Empyrion Digital) ↔ v7_row_33 `GDS Nusajaya NTP1 (Johor)` (GDS / DayOne) @ 0m
- v7_row_28 `Empyrion Digital MY1 (Nusajaya, Johor)` (Empyrion Digital) ↔ v7_row_34 `GDS Nusajaya NTP2 (Johor)` (GDS / DayOne) @ 111m
- v7_row_31 `Equinix KL1 (Cyberjaya)` (Equinix) ↔ v7_row_43 `VADS CBJ8 (Cyberjaya)` (VADS (TM subsidiary)) @ 89m
- v7_row_31 `Equinix KL1 (Cyberjaya)` (Equinix) ↔ v7_row_51 `Vantage KUL1 Campus (Cyberjaya)` (Vantage Data Centers) @ 142m
- v7_row_31 `Equinix KL1 (Cyberjaya)` (Equinix) ↔ v7_row_59 `Open DC CJ1 (Cyberjaya)` (Open DC) @ 89m
- v7_row_31 `Equinix KL1 (Cyberjaya)` (Equinix) ↔ v7_row_132 `Exabytes CJ1 Data Centre (Cyberjaya)` (Exabytes) @ 42m
- v7_row_38 `STT Johor 1 (Nusa Cemerlang)` (STT GDC) ↔ v7_row_50 `Microsoft Nusa Cemerlang (Johor)` (Microsoft) @ 0m
- v7_row_42 `VADS CBJ6 (Cyberjaya)` (VADS (TM subsidiary)) ↔ v7_row_46 `Hitachi Sunway DC CX2 (Cyberjaya)` (Hitachi Sunway) @ 24m
- v7_row_42 `VADS CBJ6 (Cyberjaya)` (VADS (TM subsidiary)) ↔ v7_row_62 `PLTPro CX2 (Cyberjaya)` (PLTPro) @ 27m
- v7_row_43 `VADS CBJ8 (Cyberjaya)` (VADS (TM subsidiary)) ↔ v7_row_51 `Vantage KUL1 Campus (Cyberjaya)` (Vantage Data Centers) @ 146m
- v7_row_43 `VADS CBJ8 (Cyberjaya)` (VADS (TM subsidiary)) ↔ v7_row_59 `Open DC CJ1 (Cyberjaya)` (Open DC) @ 1m
- v7_row_43 `VADS CBJ8 (Cyberjaya)` (VADS (TM subsidiary)) ↔ v7_row_132 `Exabytes CJ1 Data Centre (Cyberjaya)` (Exabytes) @ 48m
- v7_row_46 `Hitachi Sunway DC CX2 (Cyberjaya)` (Hitachi Sunway) ↔ v7_row_62 `PLTPro CX2 (Cyberjaya)` (PLTPro) @ 5m
- v7_row_49 `Microsoft Kulai (Johor)` (Microsoft) ↔ v7_row_53 `YTL Johor Data Center 1 (Kulai)` (YTL Data Center Holdings) @ 0m
- v7_row_49 `Microsoft Kulai (Johor)` (Microsoft) ↔ v7_row_54 `YTL Johor Data Center 2 (Kulai)` (YTL Data Center Holdings) @ 111m
- v7_row_51 `Vantage KUL1 Campus (Cyberjaya)` (Vantage Data Centers) ↔ v7_row_59 `Open DC CJ1 (Cyberjaya)` (Open DC) @ 147m
- v7_row_51 `Vantage KUL1 Campus (Cyberjaya)` (Vantage Data Centers) ↔ v7_row_132 `Exabytes CJ1 Data Centre (Cyberjaya)` (Exabytes) @ 129m
- v7_row_59 `Open DC CJ1 (Cyberjaya)` (Open DC) ↔ v7_row_132 `Exabytes CJ1 Data Centre (Cyberjaya)` (Exabytes) @ 48m
- v7_row_69 `Google facility (Selangor)` (Google) ↔ v7_row_103 `Maxis i-City Data Centre (Shah Alam)` (Maxis Berhad) @ 30m
- v7_row_69 `Google facility (Selangor)` (Google) ↔ v7_row_114 `HDC High Performance Data Centre (Shah Alam)` (HDC Data Centre Sdn Bhd) @ 30m
- v7_row_91 `Open DC PE1 - Suntech Penang Cybercity` (OPEN DC SDN BHD) ↔ v7_row_130 `Exabytes Penang Suntech Cybercity` (Exabytes) @ 46m
- v7_row_101 `VADS Iskandar Puteri Data Centre (IPDC)` (VADS Berhad) ↔ v7_row_119 `TM Global IPDC (Iskandar Puteri) Data Centre` (TM Global) @ 0m
- v7_row_102 `VADS Klang Valley Data Centre (KVDC)` (VADS Berhad) ↔ v7_row_118 `TM Global KVDC (Klang Valley Core) Data Centre` (TM Global) @ 0m
- v7_row_103 `Maxis i-City Data Centre (Shah Alam)` (Maxis Berhad) ↔ v7_row_114 `HDC High Performance Data Centre (Shah Alam)` (HDC Data Centre Sdn Bhd) @ 0m
- v7_row_116 `TM Global BFDC (Brickfields) Data Centre` (TM Global) ↔ v7_row_123 `VADS Brickfields Data Centre` (VADS Berhad) @ 0m
- v7_row_127 `Google Port Dickson Data Centre (planned)` (Google) ↔ v7_row_128 `Gamuda DC Springhill (Port Dickson)` (Gamuda DC Infrastructure Sdn Bhd) @ 0m

## Special-case flags

### AIMS three-way (v6 rows 9, 74, 91)
- v6_row_9 → v7_row_9: **AIMS Cyberjaya Block 2** (AIMS) — `v7_pending_review`: review_per_dedup_3: AIMS has multiple Cyberjaya buildings (CJ1, Block 2, possibly more). PeeringDB labels one 'CJ1 Cyberjaya'; v5.1 has 'Block 2'. These may or may not be the same building — AIMS uses both conventions. Without an authoritative AIMS f
- v6_row_74 → v7_row_74: **AIMS CJ1 Cyberjaya** (AIMS Data Centre Sdn Bhd) — `v7_pending_review`: review_per_dedup_3: AIMS has multiple Cyberjaya buildings (CJ1, Block 2, possibly more). PeeringDB labels one 'CJ1 Cyberjaya'; v5.1 has 'Block 2'. These may or may not be the same building — AIMS uses both conventions. Without an authoritative AIMS f
- v6_row_91 → v7_row_90: **AIMS Cyberjaya** (AIMS Data Centre Sdn Bhd) — `v7_pending_review`: not_duplicate_per_dedup_4: Evidence reversal: the two PeeringDB records have DIFFERENT addresses, so they are two distinct buildings — not a duplicate. PDB fac/3168 (AIMS CJ1 Cyberjaya) is at Jalan Cyber Point 4, Cyber 8, Cyberjaya 63000. PDB fac/131

### Keppel coord caveat (v6 row 44)
- v6_row_44 → v7_row_44: **Keppel DC Johor 1 (Kulai)** — `v7_pending_review`: merge_confirmed but coord (from Dgtl Infra XLSX per v6 note) not operator-verified; Keppel does not publish street-level coord for this BTS facility. Flag for coord corroboration in v8.

### NTT CBJ rename + coord-fill recommendation (v6 row 73)
- v6_row_73 → v7_row_73: **NTT Cyberjaya Data Center (CBJ)** — `v7_pending_review`: not_duplicate_per_dedup_5: NTT's own newsroom confirms its Cyberjaya site is a campus with numbered buildings (CBJ1 through CBJ6); CBJ6 is a specific 2023 build (7 MW, 4,890 m²) that forms a 'combined facility' with CBJ5. The PeeringDB record fac/190

## Errors

- None

## Pending-review queue

Total rows with `v7_status` ∈ {`pending_review`, `unresolved`}: **54**

### `hold` — 1 row(s)
- v7_row_120 | v6_row_None | **TM Global SJDC (Subterranean Penang) Data Centre** (TM Global)
  - `v7_pending_review`: hold:needs_penang_address

### `merge_confirmed but coord (from Dgtl Infra XLSX per v6 note)` — 1 row(s)
- v7_row_44 | v6_row_44 | **Keppel DC Johor 1 (Kulai)** (Keppel Data Centres)
  - `v7_pending_review`: merge_confirmed but coord (from Dgtl Infra XLSX per v6 note) not operator-verified; Keppel does not publish street-level coord for this BTS facility. Flag for coord corroboration in v8.

### `not_duplicate_per_dedup_4` — 1 row(s)
- v7_row_90 | v6_row_91 | **AIMS Cyberjaya** (AIMS Data Centre Sdn Bhd)
  - `v7_pending_review`: not_duplicate_per_dedup_4: Evidence reversal: the two PeeringDB records have DIFFERENT addresses, so they are two distinct buildings — not a duplicate. PDB fac/3168 (AIMS CJ1 Cyberjaya) is at Jalan Cyber Point 4, Cyber 8, Cyberjaya 63000. PDB fac/13120 (AIMS Cyberjaya) is at Jalan Teknokrat 1/2, Cyb

### `not_duplicate_per_dedup_5` — 2 row(s)
- v7_row_11 | v6_row_11 | **NTT Cyberjaya CBJ6** (NTT DATA)
  - `v7_pending_review`: not_duplicate_per_dedup_5: NTT's own newsroom confirms its Cyberjaya site is a campus with numbered buildings (CBJ1 through CBJ6); CBJ6 is a specific 2023 build (7 MW, 4,890 m²) that forms a 'combined facility' with CBJ5. The PeeringDB record fac/1902 (low ID, dating to early PDB era ~2010) labels j
- v7_row_73 | v6_row_73 | **NTT Cyberjaya Data Center (CBJ)** (NTT DATA's Global Data Centers division)
  - `v7_pending_review`: not_duplicate_per_dedup_5: NTT's own newsroom confirms its Cyberjaya site is a campus with numbered buildings (CBJ1 through CBJ6); CBJ6 is a specific 2023 build (7 MW, 4,890 m²) that forms a 'combined facility' with CBJ5. The PeeringDB record fac/1902 (low ID, dating to early PDB era ~2010) labels j

### `review` — 4 row(s)
- v7_row_103 | v6_row_None | **Maxis i-City Data Centre (Shah Alam)** (Maxis Berhad)
  - `v7_pending_review`: review:cotenant_of_HDC High Performance Data Centre (Shah Alam) || sweep_cotenant_pair:paired_with_v7_row_69 (haversine=30m)
- v7_row_130 | v6_row_None | **Exabytes Penang Suntech Cybercity** (Exabytes)
  - `v7_pending_review`: review:possible_dup_Open DC PE1 - Suntech Penang Cybercity || sweep_cotenant_pair:paired_with_v7_row_91 (haversine=46m)
- v7_row_131 | v6_row_None | **Exabytes International Data Center (Kuala Lumpur)** (Exabytes)
  - `v7_pending_review`: review:possible_dup_AIMS Bangunan AIMS / KL2 (Kuala Lumpur) || sweep_cotenant_pair:paired_with_v7_row_8 (haversine=43m)
- v7_row_132 | v6_row_None | **Exabytes CJ1 Data Centre (Cyberjaya)** (Exabytes)
  - `v7_pending_review`: review:possible_dup_Open DC CJ1 (Cyberjaya) || sweep_cotenant_pair:paired_with_v7_row_19 (haversine=54m)

### `review_campus_per_dedup_2` — 3 row(s)
- v7_row_53 | v6_row_53 | **YTL Johor Data Center 1 (Kulai)** (YTL Data Center Holdings)
  - `v7_pending_review`: review_campus_per_dedup_2: Three rows at (1.6206, 103.5216), (1.6216, 103.5216), (1.6226, 103.5216) — a perfect 0.001°-lat stagger on identical lon. That pattern is not how a real campus lays out. YTL Green DC Park publishes a single campus in Kulai on 111ha; if these are distinct buildings they nee
- v7_row_54 | v6_row_54 | **YTL Johor Data Center 2 (Kulai)** (YTL Data Center Holdings)
  - `v7_pending_review`: review_campus_per_dedup_2: Three rows at (1.6206, 103.5216), (1.6216, 103.5216), (1.6226, 103.5216) — a perfect 0.001°-lat stagger on identical lon. That pattern is not how a real campus lays out. YTL Green DC Park publishes a single campus in Kulai on 111ha; if these are distinct buildings they nee
- v7_row_55 | v6_row_55 | **YTL Johor Data Center 3 (Kulai)** (YTL Data Center Holdings)
  - `v7_pending_review`: review_campus_per_dedup_2: Three rows at (1.6206, 103.5216), (1.6216, 103.5216), (1.6226, 103.5216) — a perfect 0.001°-lat stagger on identical lon. That pattern is not how a real campus lays out. YTL Green DC Park publishes a single campus in Kulai on 111ha; if these are distinct buildings they nee

### `review_per_dedup_3` — 2 row(s)
- v7_row_9 | v6_row_9 | **AIMS Cyberjaya Block 2** (AIMS)
  - `v7_pending_review`: review_per_dedup_3: AIMS has multiple Cyberjaya buildings (CJ1, Block 2, possibly more). PeeringDB labels one 'CJ1 Cyberjaya'; v5.1 has 'Block 2'. These may or may not be the same building — AIMS uses both conventions. Without an authoritative AIMS facility list, flag for human review rather than au
- v7_row_74 | v6_row_74 | **AIMS CJ1 Cyberjaya** (AIMS Data Centre Sdn Bhd)
  - `v7_pending_review`: review_per_dedup_3: AIMS has multiple Cyberjaya buildings (CJ1, Block 2, possibly more). PeeringDB labels one 'CJ1 Cyberjaya'; v5.1 has 'Block 2'. These may or may not be the same building — AIMS uses both conventions. Without an authoritative AIMS facility list, flag for human review rather than au

### `sweep_cotenant_pair` — 35 row(s)
- v7_row_0 | v6_row_0 | **AWS Asia Pacific (Malaysia)** (AWS)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_2 (haversine=0m) || sweep_potential_dup:paired_with_v7_row_68 (haversine=11m)
- v7_row_2 | v6_row_2 | **Azure Malaysia West** (Azure)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_0 (haversine=0m)
- v7_row_6 | v6_row_6 | **Bridge Data Centres MY02 (Cyberjaya)** (Bridge Data Centres)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_42 (haversine=0m)
- v7_row_8 | v6_row_8 | **AIMS Bangunan AIMS / KL2 (Kuala Lumpur)** (AIMS)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_131 (haversine=43m)
- v7_row_14 | v6_row_14 | **Telekom Malaysia IPDC (Iskandar Puteri)** (Telekom Malaysia)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_34 (haversine=144m)
- v7_row_17 | v6_row_17 | **AirTrunk JHB1 (Johor)** (AirTrunk)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_23 (haversine=0m)
- v7_row_19 | v6_row_19 | **Vantage Cyberjaya Campus 2** (Vantage Data Centers)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_31 (haversine=77m)
- v7_row_21 | v6_row_21 | **DayOne (Johor)** (DayOne)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_28 (haversine=0m)
- v7_row_22 | v6_row_22 | **EdgeConneX Cyberjaya** (EdgeConneX)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_37 (haversine=0m)
- v7_row_23 | v6_row_23 | **EdgeConneX Johor** (EdgeConneX)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_17 (haversine=0m)
- v7_row_28 | v6_row_28 | **Empyrion Digital MY1 (Nusajaya, Johor)** (Empyrion Digital)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_21 (haversine=0m)
- v7_row_31 | v6_row_31 | **Equinix KL1 (Cyberjaya)** (Equinix)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_19 (haversine=77m)
- v7_row_33 | v6_row_33 | **GDS Nusajaya NTP1 (Johor)** (GDS / DayOne)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_21 (haversine=0m) || sweep_potential_dup:paired_with_v7_row_34 (haversine=111m)
- v7_row_34 | v6_row_34 | **GDS Nusajaya NTP2 (Johor)** (GDS / DayOne)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_14 (haversine=144m) || sweep_potential_dup:paired_with_v7_row_33 (haversine=111m)
- v7_row_37 | v6_row_37 | **STT Cyberjaya DC.2** (STT GDC / Basis Bay)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_22 (haversine=0m)
- v7_row_38 | v6_row_38 | **STT Johor 1 (Nusa Cemerlang)** (STT GDC)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_50 (haversine=0m)
- v7_row_42 | v6_row_42 | **VADS CBJ6 (Cyberjaya)** (VADS (TM subsidiary))
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_6 (haversine=0m)
- v7_row_43 | v6_row_43 | **VADS CBJ8 (Cyberjaya)** (VADS (TM subsidiary))
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_19 (haversine=47m)
- v7_row_46 | v6_row_46 | **Hitachi Sunway DC CX2 (Cyberjaya)** (Hitachi Sunway)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_6 (haversine=24m)
- v7_row_49 | v6_row_49 | **Microsoft Kulai (Johor)** (Microsoft)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_53 (haversine=0m)
- v7_row_50 | v6_row_50 | **Microsoft Nusa Cemerlang (Johor)** (Microsoft)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_38 (haversine=0m)
- v7_row_51 | v6_row_51 | **Vantage KUL1 Campus (Cyberjaya)** (Vantage Data Centers)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_31 (haversine=142m)
- v7_row_59 | v6_row_59 | **Open DC CJ1 (Cyberjaya)** (Open DC)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_19 (haversine=46m)
- v7_row_62 | v6_row_62 | **PLTPro CX2 (Cyberjaya)** (PLTPro)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_6 (haversine=27m)
- v7_row_69 | v6_row_69 | **Google facility (Selangor)** (Google)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_103 (haversine=30m)
- v7_row_91 | v6_row_92 | **Open DC PE1 - Suntech Penang Cybercity** (OPEN DC SDN BHD)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_130 (haversine=46m)
- v7_row_101 | v6_row_None | **VADS Iskandar Puteri Data Centre (IPDC)** (VADS Berhad)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_119 (haversine=0m)
- v7_row_102 | v6_row_None | **VADS Klang Valley Data Centre (KVDC)** (VADS Berhad)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_118 (haversine=0m)
- v7_row_114 | v6_row_None | **HDC High Performance Data Centre (Shah Alam)** (HDC Data Centre Sdn Bhd)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_69 (haversine=30m)
- v7_row_116 | v6_row_None | **TM Global BFDC (Brickfields) Data Centre** (TM Global)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_123 (haversine=0m)
- v7_row_118 | v6_row_None | **TM Global KVDC (Klang Valley Core) Data Centre** (TM Global)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_102 (haversine=0m)
- v7_row_119 | v6_row_None | **TM Global IPDC (Iskandar Puteri) Data Centre** (TM Global)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_101 (haversine=0m)
- v7_row_123 | v6_row_None | **VADS Brickfields Data Centre** (VADS Berhad)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_116 (haversine=0m)
- v7_row_127 | v6_row_None | **Google Port Dickson Data Centre (planned)** (Google)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_128 (haversine=0m)
- v7_row_128 | v6_row_None | **Gamuda DC Springhill (Port Dickson)** (Gamuda DC Infrastructure Sdn Bhd)
  - `v7_pending_review`: sweep_cotenant_pair:paired_with_v7_row_127 (haversine=0m)

### `sweep_potential_dup` — 5 row(s)
- v7_row_35 | v6_row_35 | **GDS Nusajaya NTP3 (Johor)** (GDS / DayOne)
  - `v7_pending_review`: sweep_potential_dup:paired_with_v7_row_34 (haversine=111m)
- v7_row_68 | v6_row_68 | **AWS facility (Kuala Lumpur)** (AWS)
  - `v7_pending_review`: sweep_potential_dup:paired_with_v7_row_0 (haversine=11m) || sweep_cotenant_pair:paired_with_v7_row_2 (haversine=11m)
- v7_row_104 | v6_row_None | **Racks Central RCJM1 (Pasir Gudang)** (Racks Central)
  - `v7_pending_review`: sweep_potential_dup:paired_with_v7_row_105 (haversine=0m)
- v7_row_105 | v6_row_None | **Racks Central RCJM2A (Pasir Gudang)** (Racks Central)
  - `v7_pending_review`: sweep_potential_dup:paired_with_v7_row_104 (haversine=0m)
- v7_row_106 | v6_row_None | **Racks Central RCJM2B (Pasir Gudang)** (Racks Central)
  - `v7_pending_review`: sweep_potential_dup:paired_with_v7_row_104 (haversine=0m)

## datacentermap.com holdover check

**21** v7 row(s) still reference datacentermap / dcm (inherited from pre-TOS-enforcement v6). These are NOT silently stripped — flag for remediation in v8:

- v7_row_39 (v6_row_39): **VADS CBJ1 (Cyberjaya)** — `note` contains: `Lingkaran Usahawan 1 Timur; Tier 3; ISO 27001; datacentermap.com snippet`
- v7_row_40 (v6_row_40): **VADS CBJ2 (Cyberjaya)** — `note` contains: `Bangunan TM CBJ Unit 2, Jalan Lingkaran Fauna; datacentermap.com snippet`
- v7_row_41 (v6_row_41): **VADS CBJ5 (Cyberjaya)** — `note` contains: `CSF Computer Exchange, 3552 Jalan Teknokrat 6, 3rd Floor; datacentermap.com snippet`
- v7_row_42 (v6_row_42): **VADS CBJ6 (Cyberjaya)** — `note` contains: `CX2 Computer Exchange, 7118 Jalan Impact, 4th Floor; datacentermap.com snippet`
- v7_row_45 (v6_row_45): **STACK JHB01 (Johor Bahru)** — `note` contains: `10.8 hectares; cloud and AI/ML workloads; Jalan Bioteknologi; datacentermap.com snippet`
- v7_row_46 (v6_row_46): **Hitachi Sunway DC CX2 (Cyberjaya)** — `note` contains: `7118 CX2 Computer Exchange, Jalan Impact; includes former Jalan Teknologi 1 site; datacentermap.com snippet`
- v7_row_47 (v6_row_47): **Hitachi Sunway DC Century Square (Cyberjaya)** — `note` contains: `Block 2310, Jalan Usahawan, Century Square; datacentermap.com snippet`
- v7_row_58 (v6_row_58): **Shinsei Malaysia 1 (Labu, Negeri Sembilan)** — `note` contains: `Labu, Negeri Sembilan; also known as Regal Orion; single facility; datacentermap.com`
- v7_row_59 (v6_row_59): **Open DC CJ1 (Cyberjaya)** — `note` contains: `Jalan Cyber Point 4, Cyber 8; Cyberjaya; datacentermap.com`
- v7_row_60 (v6_row_60): **Open DC JB1 (Johor Bahru)** — `note` contains: `Menara MSC Cyberport, No 5 Jalan Bukit Meldrum; Johor Bahru; datacentermap.com`
- v7_row_61 (v6_row_61): **Infinaxis Cyberjaya 1** — `note` contains: `Multimedia Super Corridor; Tier III target; datacentermap.com snippet`
- v7_row_62 (v6_row_62): **PLTPro CX2 (Cyberjaya)** — `note` contains: `CX2 Computer Exchange, West Wing; Cyberjaya; datacentermap.com snippet`
- v7_row_111 (v6_row_None): **Progenet Data Center (Petaling Jaya)** — `note` contains: `Tier-3. 7000sqft/floor. Single-source-ish — two trade directories. | geocoded via Nominatim (full_address): match='Menara Lien Hoe, Persiaran Tropicana, Tropica`
- v7_row_112 (v6_row_None): **Teliti Datacentre (Bandar Enstek)** — `note` contains: `Tier-3, 120,000sqft NLA. ETP key project under Business Services sector. Targeting Tier-4. | geocoded via Nominatim (city_centroid): match='Bandar Enstek, Nilai`
- v7_row_113 (v6_row_None): **SKALI Serdang Internet Data Center** — `note` contains: `Tier-2, 6500sqft, 600-1000 servers. SKALI IDC operating since 2000. | geocoded via Nominatim (city_centroid): match='Seri Kembangan, Majlis Bandaraya Subang Jay`
- v7_row_114 (v6_row_None): **HDC High Performance Data Centre (Shah Alam)** — `note` contains: `First Uptime Tier-III-certified DC in Malaysia (2012). Co-located with Maxis i-City DC in same i-City Block M building (M-04-3A vs Block M No.6 main). | geocode`
- v7_row_123 (v6_row_None): **VADS Brickfields Data Centre** — `note` contains: `VADS-branded DC in TM Brickfields building. Dual-brand with TM Global BFDC. | geocoded via Nominatim (city_centroid): match='Brickfields, Kuala Lumpur, 50470, M`
- v7_row_124 (v6_row_None): **VADS Ipoh Data Centre (Perak)** — `note` contains: `Tier 2 managed DC inside the Ipoh Telekom exchange. VADS regional site. | geocoded via Nominatim (city_centroid): match='Majlis Bandaraya Ipoh, Kinta, Perak, Ma`
- v7_row_126 (v6_row_None): **Mah Sing DC Hub @ Southville City (Bangi)** — `note` contains: `JV: 150-acre site earmarked, up to 500MW planned. Initial 17.55-acre phase with Bridge DC up to 100MW. | geocoded via Nominatim (full_address): match='Southvill`
- v7_row_129 (v6_row_None): **ZDATA GP3 Gelang Patah (Computility Technology Sdn Bhd)** — `note` contains: `RM8bn hyperscale AI DC. 300MW per datacentermap. First GreenRE Platinum DC in Malaysia. Operational phase-1 Mar 2026. | geocoded via Nominatim (full_address): m`
- v7_row_130 (v6_row_None): **Exabytes Penang Suntech Cybercity** — `note` contains: `CO-TENANCY QUESTION: this candidate's coords fall within 150 m of v6 row 'Open DC PE1 - Suntech Penang Cybercity'. Different operators → not auto-duplicate, but`

## Recommendations for v8

1. **Run the rejected-resolution pass** on the 5 `reject:sole_source_tos_blocked` rows (VADS ×3, AWS KUL ×2) using SSM / MCMC / planning-portal / operator-IR sources.
2. **AIMS three-way consolidation** (v6 rows 9, 74, 91) — decide whether the Cyber 3 building is one physical facility (currently represented twice: row 9 and row 91) or two.
3. **Rename NTT CBJ → CBJ1** and coord-fill from `43000 Jalan APEC, Cyberjaya 63000`.
4. **Keppel DC Johor 1 coord verification** — pull Keppel DC REIT property schedules (Bursa/SGX) or reverse-geocode (1.6686, 103.5224) against OSM to confirm the park name.
5. **Resolve the dedup-sweep findings** — review each `sweep_potential_dup` and `sweep_cotenant_pair` pair flagged in this pass.
6. **Remediate the datacentermap holdover rows** listed above — replace `source` with operator-direct or PeeringDB/OSM URLs.
