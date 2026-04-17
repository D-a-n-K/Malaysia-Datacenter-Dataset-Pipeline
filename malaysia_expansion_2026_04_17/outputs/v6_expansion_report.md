# Malaysia DC Corpus Expansion — v6 Report

Generated 2026-04-17 by `merge_v6.py` as part of the single-day sprint.

## Rows per source

| Source file | Candidates |
|---|---|
| `peeringdb_candidates.csv` | 27 |
| `mida_candidates.csv` | 11 |
| `st_candidates.csv` | 5 |
| `operator_ir_candidates.csv` | 1 |
| **Total before sibling-collapse** | **44** |
| **Unique facilities after collapse** | **35** |

## Confidence-tier breakdown

| Tier | Rows | Definition |
|---|---|---|
| High | 4 | confidence ≥ 0.7 AND ≥2 sibling sources |
| Medium | 0 | 0.4 ≤ confidence < 0.7, or single-source ≥ 0.4 |
| Needs review | 31 | confidence < 0.4 |

## Operator-tier breakdown

| Tier | Rows | Share |
|---|---|---|
| hyperscaler | 3 | 8.6% |
| tier1_colo | 9 | 25.7% |
| tier2_colo | 16 | 45.7% |
| sovereign_telco | 7 | 20.0% |
| unknown | 0 | 0.0% |

## Coverage by city (top 10)

| City | Rows |
|---|---|
| Jalan Cyber Point 4, Cyberjaya, 63000 | 2 |
| Johor | 1 |
| Cyberjaya | 1 |
| Kuala Lumpur | 1 |
| Klang | 1 |
| Selangor | 1 |
| Petaling Jaya | 1 |
| Technology Park Malaysia, Kuala Lumpur, 57000 | 1 |
| 43000 Jalan APEC, Cyberjaya, 63000 | 1 |
| MY02, Cyberjaya, 63000 | 1 |

## Dedup verdict against v5

| Verdict | Rows |
|---|---|
| new | 34 |
| duplicate_proximity | 1 |

## Negative results worth recording

* **MDEC + DNB**: 0 candidates. MDEC's /media-release corpus is about MDEC-internal programs, not facility announcements; DNB's 5G-wholesaler role was wound down in Jan 2025 and its site uses flat-slug articles. Not a productive automated source.
* **Bursa Malaysia**: HTTP 403 at the CDN before robots.txt is even served — no automated scraping possible. Pivoted to operator-native IR sites; YTL Power's robots.txt disallows `*`, so that IR feed is also skipped.
* **Wikipedia (prior session)**: 0 MY-tagged DC entities on Wikidata (confirmed via SPARQL). Retained as a documented negative result; recommend not re-running for future Southeast-Asian country pilots.

## Caveats

* MIDA and operator-IR rows use city-centroid coordinates with `coord_confidence='geocoded_with_campus_offset'`. Street-level geocoding was deliberately deferred to avoid invoking Google/Geocoder APIs inside the sprint — any high-confidence facility the user approves for merge should be hand-located against satellite imagery.
* ST (Energy Commission) candidates pass a loose `centre` keyword filter that surfaces shopping malls, hospitals, and office towers alongside real DCs. Human review is mandatory for anything from this source.
* The confidence score treats any candidate sitting within 300 m of a v5 row (or name-matching one) as 2-sources-agree, so PeeringDB entries that re-discover an existing v5 facility pick up credit for that agreement. Single-source PeeringDB rows with no v5 neighbour stay in the review bucket — by design, since the sprint's definition of high-confidence requires multi-source corroboration.
