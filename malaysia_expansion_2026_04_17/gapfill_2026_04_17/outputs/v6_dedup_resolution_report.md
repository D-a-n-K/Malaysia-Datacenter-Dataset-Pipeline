# v6 Dedup Fixes — Resolution Report

**Date:** 2026-04-17
**Scope:** Resolution of the 4 pending-verification entries in `v6_dedup_fixes.csv` via operator-direct fetches.
**Entries resolved:** 4 of 4. The 2 human-judgment entries (YTL campus review, AIMS CJ1 vs Block 2) were left untouched per scope.

---

## Entry 0 — Keppel DC Johor 1 (Kulai)

**Outcome:** `merge_pending_verification → merge_confirmed`

**Operator source:** https://www.keppeldatacentres.com/locations/asia-pacific/malaysia/keppel-dc-johor-1/

**Key evidence:**
- Keppel publishes a single dedicated page for "Keppel DC Johor 1" describing it as a BTS (built-to-suit) colocation facility for a leading tech company, completed 2020, 261,000 sq ft land / 100,495 sq ft GFA, jointly owned by Keppel Data Centres and Alpha Data Centre Fund.
- The operator page does **not** publish a street address or GPS coordinates — location is described only as "within an industrial park in Johor, Malaysia."

**Why the merge is confirmed regardless:** the "drop" row (86) is the PeeringDB record for the same named facility ("Keppel Data Centres | Keppel DC Johor 1") with null coords. Since both rows are unambiguously the same physical facility and the drop row contributes no competing coord, the merge decision is sound. The v5.1 coord (1.6686, 103.5224) originates from the Dgtl Infra XLSX cited in the existing `note` — its precision is a separate gapfill concern, not a dedup concern.

**Fetch results:**
- `keppeldatacentres.com` facility page — 200 OK, confirmed facility identity.
- `kepinfra.com` press-release index — 404 (site was consolidated under keppel.com/infrastructure/).
- `keppel.com/infrastructure/media/press-release/` — 404 on that path; no Johor announcement surfaced through accessible navigation.
- `datacenters.com`, `datacenterdynamics.com`, `baxtel.com` — 403/404 to our User-Agent; not pursued further within budget.

**Flag for human:** Coord is not independently confirmed at operator street-address level. If the land-transformation analysis needs precise lat/lon, a future pass should try to (a) pull Keppel DC REIT's Bursa/SGX property schedules, (b) reverse-geocode the coord against OSM to identify the industrial park name.

---

## Entry 1 — Princeton Digital JH1 (Johor)

**Outcome:** `merge_pending_verification → merge_confirmed`

**Operator source:** https://princetondg.com/locations/malaysia/

**Key evidence (verbatim from operator page):**
> "JH1 — City: Kulai, Johor. Park: Sedenak Tech Park (STeP). Land area: 128,000 m². Capacity: 200 MW. Colocation area: 80,000 m². Configuration: 4 buildings."

Sedenak Tech Park's approximate centroid is (1.69°N, 103.41°E). The v5.1 coord (1.6923, 103.4145) falls inside STeP — consistent with the operator's published location. The v6_review PeeringDB coord (1.6642, 103.5298) is ~15 km east, in the Kulai town / Iskandar area, and is inconsistent with STeP.

**Conclusion:** v5.1 row is geolocationally correct; v6_review (PeeringDB fac/14647) is geocoding-drifted. Merge is correct — keep v5.1 with STeP coord, drop PeeringDB row, carry pdb_id into the note.

**Fetch results:**
- `princetondg.com/locations/malaysia/` — 200 OK, yielded direct "Kulai / Sedenak Tech Park" confirmation.

---

## Entry 4 — AIMS CJ1 Cyberjaya vs. AIMS Cyberjaya

**Outcome:** `merge_pending_verification → not_duplicate_keep_both` (evidence reversed the original merge decision)

**Operator sources:**
- https://www.peeringdb.com/fac/3168 (AIMS CJ1 Cyberjaya)
- https://www.peeringdb.com/fac/13120 (AIMS Cyberjaya)

**Key evidence:** The two PeeringDB records — both operator-registered by "AIMS Data Centre Sdn Bhd" — have **distinct street addresses in different Cyberjaya zones**:

| Record | Address | Cyber zone |
|---|---|---|
| fac/3168 (AIMS CJ1 Cyberjaya) | Jalan Cyber Point 4, Cyber 8, Cyberjaya 63000 | Cyber 8 |
| fac/13120 (AIMS Cyberjaya)    | Jalan Teknokrat 1/2, Cyber 3, Cyberjaya 63000 | Cyber 3 |

Cyber 3 and Cyber 8 are separate named zones within Cyberjaya with distinct street grids. The prior merge assumption (that "AIMS Cyberjaya" was a generic alias for CJ1) was therefore wrong — AIMS operates at least two distinct Cyberjaya buildings registered in PeeringDB.

**Fetch results:**
- `aims.com.my` — TLS certificate verification failed under our fetcher; could not retrieve the operator's own facility list. The two PeeringDB addresses are sufficient evidence for the keep-both decision.

**Flag for human (important):** The "drop" row (91, AIMS Cyberjaya, Cyber 3 / Jalan Teknokrat) is coord-less. It plausibly corresponds to the historical Menara AIMS / Block 2 facility in Cyber 3 — which overlaps with the **separate** review entry (row 4 in this file: AIMS Cyberjaya Block 2 vs. AIMS CJ1 Cyberjaya). Before filling coords for row 91, a human should decide whether row 91 and the existing v5.1 "AIMS Cyberjaya Block 2" row (row_idx 9) describe the same Cyber 3 building — otherwise Cyber 3 may end up double-counted.

---

## Entry 5 — NTT Cyberjaya CBJ vs. CBJ6

**Outcome:** `review → not_duplicate_keep_both`

**Operator sources:**
- https://services.global.ntt/en-US/newsroom/ntt-strengthens-commitment-to-malaysia-with-the-launch-of-cyberjaya-6-data-center
- https://www.peeringdb.com/fac/1902

**Key evidence:**
- NTT's own newsroom for the CBJ6 launch explicitly describes Cyberjaya as a **campus** and CBJ6 as "the sixth data center on NTT's campus" (7 MW critical IT load, 4,890 m², 2× 33 kV substations). NTT quotes its own MD describing the campus as having "evolved… over the past two decades." CBJ5 (2021) and CBJ6 (2023) are explicitly noted as forming a 20,000 m² / 22 MW "combined facility" — i.e., CBJ1–CBJ6 are **distinct buildings on one campus**.
- The PeeringDB record fac/1902 carries a very low PDB ID (consistent with registration in PDB's early years, ~2010–2012), labels the facility just "CBJ" (no number), and lists the campus address "43000 Jalan APEC, Cyberjaya, 63000" with null GPS. The unnumbered label and the age of the record align with what was the original single building (now CBJ1), registered before NTT's naming convention added numeric suffixes.

**Interpretation:** "CBJ" (PDB fac/1902) is effectively the original CBJ1 — a different physical building from the v5.1 "NTT Cyberjaya CBJ6" row (row 11). Merging would collapse two distinct buildings into one, which is wrong for the land-transformation analysis.

**Fetch results:**
- NTT global.ntt newsroom article — 200 OK via DuckDuckGo surfacing; provided campus description and CBJ6 specs.
- `services.global.ntt/en-us/services-and-products/global-data-centers/global-locations` — page present but lacked per-building detail.

**Flag for human (action needed):** The v6_review row 73 ("NTT Cyberjaya Data Center (CBJ)") should be retained and, in a subsequent gapfill pass, (a) **renamed** to "NTT Cyberjaya CBJ1" to disambiguate from CBJ6, (b) **coord-filled** from the published campus address (43000 Jalan APEC, Cyberjaya) — CBJ1 is on the same campus parcel as CBJ6 but is a physically distinct building. Scope of this task explicitly excluded coord modifications, so that work is deferred.

---

## Source-fetchability summary

| Site | Status during this pass |
|---|---|
| keppeldatacentres.com | ✅ 200 on facility page |
| princetondg.com | ✅ 200 on Malaysia locations page |
| services.global.ntt (newsroom) | ✅ 200 via DDG-surfaced URL |
| peeringdb.com | ✅ 200 on fac records |
| aims.com.my | ❌ TLS cert validation failed — operator page not retrievable; PDB substituted |
| kepinfra.com / keppel.com press index | ❌ 404 on expected paths |
| datacenters.com / datacenterdynamics.com / baxtel.com | ❌ 403/404 to our UA |

No Wayback bypass was attempted (no site was genuinely down; all were TLS/UA-related). No datacentermap.com citations appear in `evidence_urls`.

---

## Summary table

| # | Entry | Before | After | Evidence class |
|---|---|---|---|---|
| 0 | Keppel DC Johor 1 | merge_pending_verification | merge_confirmed | operator-direct (identity only; no coord published) |
| 1 | Princeton Digital JH1 | merge_pending_verification | merge_confirmed | operator-direct (confirms STeP location, invalidates PDB-drifted coord) |
| 4 | AIMS CJ1 vs AIMS Cyberjaya | merge_pending_verification | not_duplicate_keep_both | PDB (operator-registered) — two distinct Cyberjaya addresses |
| 5 | NTT CBJ vs CBJ6 | review | not_duplicate_keep_both | NTT newsroom (campus has distinct numbered buildings) |

All 4 pending entries resolved within budget. No entries remain in `merge_pending_verification` state.
