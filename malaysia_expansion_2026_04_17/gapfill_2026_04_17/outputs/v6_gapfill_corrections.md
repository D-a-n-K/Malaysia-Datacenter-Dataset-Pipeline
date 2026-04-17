# v6 gapfill corrections report

Applied by `apply_corrections.py` on 2026-04-17 in response to the
six-issue review of `v6_gapfill_candidates.csv` and `v6_dedup_fixes.csv`.

## Issue 1 — datacentermap.com TOS violation (the big one)

The parent project's `project_datacentermap_tos.md` memory and the
original gapfill brief both forbid datacentermap.com as a source. My
original pass cited it on 14 rows anyway — apologies. Corrections:

### §1a — sole-source rejections (fully blanked)

| Row | Facility name |
|---|---|
| 27 | VADS Semarak Data Centre |
| 28 | VADS Bayan Baru Data Centre (Penang) |
| 30 | VADS Labuan Data Centre |
| 40 | AWS KUL Dengkil Site (ap-southeast-5 AZ) |
| 41 | AWS KUL Bukit Jalil Site (ap-southeast-5 AZ) |

Each row had `source`, `lat`, `lon`, `address` blanked; `n_sources=0`; 
`coord_confidence=unknown`; `promotion_action=reject:sole_source_tos_blocked`. 
The `name` and `operator` fields are kept so a follow-up investigator 
knows what to re-source.

### §1b — multi-source downgrades

| Row | Facility | dcm removed | new n_sources | outcome |
|---|---|---|---|---|
| 13 | Progenet Data Center (Petaling Jaya) | 1 | 1 | single-source warning |
| 14 | Teliti Datacentre (Bandar Enstek) | 1 | 1 | single-source warning |
| 15 | SKALI Serdang Internet Data Center | 1 | 1 | single-source warning |
| 16 | HDC High Performance Data Centre (Shah Alam) | 1 | 1 | single-source warning |
| 26 | VADS Brickfields Data Centre | 1 | 1 | single-source warning |
| 29 | VADS Ipoh Data Centre (Perak) | 1 | 1 | single-source warning |
| 32 | Mah Sing DC Hub @ Southville City (Bangi) | 1 | 2 | multi-source intact |
| 35 | ZDATA GP3 Gelang Patah (Computility Technology Sdn Bhd) | 1 | 2 | multi-source intact |
| 36 | Exabytes Penang Suntech Cybercity | 1 | 1 | single-source warning |

### §1c — cache cleanup

Scanned `/tmp/gapfill_cache/` for files containing `datacentermap.com` 
in their content or filename. Deleted **0** file(s).

## Issue 2 — coord_confidence relabels

### §2a — row 22 (TM Global SJDC Penang)

Was `exact` with a state-centroid geocode; note admitted rough-locality. 
Corrected to `approximate_locality` with blanked coords (state centroid is 
worse than no coord — suggests false precision). Action changed to 
`hold:needs_penang_address`.

### §2b — audit of other rows

Relabeled 13 row(s):

| Row | Facility | Old | New |
|---|---|---|---|
| 3 | VADS Iskandar Puteri Data Centre (IPDC) | building | approximate_locality |
| 4 | VADS Klang Valley Data Centre (KVDC) | building | approximate_locality |
| 11 | SILVERSTREAMS Data Centre (Bagan Datuk) | building | approximate_locality |
| 17 | MaNaDr Bintulu AI Data Centre (planned) | building | approximate_locality |
| 18 | TM Global BFDC (Brickfields) Data Centre | building | approximate_locality |
| 19 | TM Global KJDC (Kelana Jaya) Data Centre | building | approximate_locality |
| 20 | TM Global KVDC (Klang Valley Core) Data Centr | building | approximate_locality |
| 21 | TM Global IPDC (Iskandar Puteri) Data Centre | building | approximate_locality |
| 23 | YTL Green Data Center Park (Kulai) | building | approximate_locality |
| 32 | Mah Sing DC Hub @ Southville City (Bangi) | building | approximate_locality |
| 33 | Google Port Dickson Data Centre (planned) | building | approximate_locality |
| 34 | Gamuda DC Springhill (Port Dickson) | building | approximate_locality |
| 35 | ZDATA GP3 Gelang Patah (Computility Technolog | building | approximate_locality |

## Issue 3 — row 23 YTL Green Data Center Park

Self-contradicting (note said 'already in v5.1' while action was 
`add_new`). Changed to `update_existing:<row_idx>` pointing at the 
matching v6_master row. The candidate's sources enrich the existing 
v5.1 row's provenance rather than add a new facility.

## Issue 4 — co-tenancy flagging

### §4a — rows 5 (Maxis i-City) and 16 (HDC)

Both sit in the same i-City Block M building. Row 5 previously had no 
co-tenancy note; now flagged `review:cotenant_of_HDC...` and its note 
mirrors row 16's. Row 16's note updated symmetrically to reference row 5.

### §4b — existing review rows 36/37/38/41

Rewrote the note on each to explicit open-question form: 'reviewer to 
decide: (a) same building co-tenant, (b) rebrand-merge, or (c) distinct 
buildings.' No preemptive conclusion.

## Issue 5 — TM Global ↔ VADS pair cross-references

| TM Global row | VADS row | Tag | Status |
|---|---|---|---|
| 21 | 3 | IPDC | linked |
| 20 | 4 | KVDC | linked |
| 18 | 26 | BFDC | linked |

## Issue 6 — v6_dedup_fixes.csv

* NTT CBJ/CBJ6: Distance undefined — v6_review 'NTT Cyberjaya Data Center (CBJ)' has null coords (PeeringDB row carried no lat/lon). Cannot compute haversine; `review` stands.
* Row 0 'Keppel DC Johor 1 (Kulai)': merge→merge_pending_verification
* Row 1 'Princeton Digital Group JH1 (Johor)': merge→merge_pending_verification
* Row 4 'AIMS CJ1 Cyberjaya': merge→merge_pending_verification

## Final promotion_action distribution

| promotion_action | count |
|---|---|
| `add_new` | 30 |
| `reject:sole_source_tos_blocked` | 5 |
| `skip:duplicate_of_CX5 / MY02 Cyberjaya Malaysia` | 1 |
| `review:cotenant_of_HDC High Performance Data Centre (Shah Alam)` | 1 |
| `hold:needs_penang_address` | 1 |
| `update_existing:12` | 1 |
| `review:possible_dup_Open DC PE1 - Suntech Penang Cybercity` | 1 |
| `review:possible_dup_AIMS Bangunan AIMS / KL2 (Kuala Lumpur)` | 1 |
| `review:possible_dup_Open DC CJ1 (Cyberjaya)` | 1 |

## Success criteria check

* datacentermap.com URL still in any `source` field: NO
* datacentermap.com cache files remaining: 0
* Row 23 promotion_action: `update_existing:12`
* Row 5 references row 16 in note: yes
* Row 16 references row 5 in note: yes

