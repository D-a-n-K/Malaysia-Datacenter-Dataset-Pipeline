# Building a Global Datacenter Dataset: Malaysia Pilot

## Overview

This project constructs a multi-source datacenter dataset for Malaysia using openly-licensed data and text analytics methods, demonstrating a replicable pipeline that can scale to any country or region. Commercial datacenter directories like Data Center Map prohibit both programmatic scraping and manual extraction for database integration in their terms of service; this pipeline sidesteps that constraint entirely by drawing from three complementary source layers: OpenStreetMap (OSM) via the Overpass API, Wikidata via SPARQL, cloud provider region endpoints, and trade press corpus analysis using spaCy Named Entity Recognition (NER). The result is a 50-facility dataset with operator attribution, geographic coordinates, source provenance, and country-level metadata, all produced through methods that are legally defensible, academically citable, and reproducible from a single Quarto document.

Malaysia was chosen deliberately as a pilot. Johor has become one of the fastest-growing hyperscale datacenter markets in Southeast Asia since 2023, driven by Singapore's 2019–2022 moratorium on new datacenter capacity and the subsequent overflow of hyperscaler investment across the border. The country is small enough to run the full pipeline end-to-end in seconds but large enough to produce an analytically interesting dataset, one that reveals systematic coverage gaps across open data sources and demonstrates how text analytics methods can close those gaps.

## Key Findings

The pipeline evolved through three versions, each adding a source layer and exposing a new coverage dynamic:

**Version 1** queried OSM and Wikidata, returning 20 facilities from OSM and zero from Wikidata. Fifteen of the twenty OSM entries were unnamed building polygons clustered in Johor's Sedenak Tech Park area, traced by OSM contributors from satellite imagery but lacking operator tags. Wikidata's structured coverage of Southeast Asian datacenter infrastructure proved to be near-zero as of April 2026, a finding that itself documents a significant gap in the linked-data ecosystem.

**Version 2** added five cloud provider regions (Amazon Web Services (AWS), Microsoft Azure, Google Cloud Platform (GCP), Oracle Cloud, and Alibaba Cloud), all of which have announced or activated Malaysia regions since 2022 but none of which appeared in OSM or Wikidata. AWS was queried via its machine-readable regional services JSON endpoint; the remaining four were compiled from each provider's publicly announced infrastructure pages, with source attribution preserved. This brought the dataset to 25 facilities and introduced the first operator-attributed entries.

**Version 3** introduced a trade press NER pipeline, constructing a 16-document corpus from search engine result snippets, accessible press releases, and industry analyses sourced from DataCenterDynamics, MIDA, Mordor Intelligence, EdgeConneX, Empyrion Digital, NTT DATA, and TechNode Global. DataCenterDynamics blocks programmatic article fetches (returning HTTP 403), so the corpus was built from publicly available search result snippets rather than full article scrapes, a methodological constraint documented explicitly in the pipeline rather than hidden. SpaCy's `en_core_web_sm` model extracted 102 named entities (50 ORG, 47 GPE, 3 FAC), which were cross-referenced against the v2 dataset to identify 25 additional facilities operated by Bridge Data Centres, NTT DATA, AirTrunk, Vantage Data Centers, EdgeConneX, DayOne, Princeton Digital Group, K2 Strategic, Telekom Malaysia, YTL Power, Empyrion Digital, Open DC, and others. The final dataset contains 50 facilities with operator attribution for 30 of them, spanning all four of Malaysia's datacenter hubs: Johor, Klang Valley / Cyberjaya, Penang, and Kedah.

## Methodology

The pipeline mixes R and Python via reticulate in a single Quarto (`.qmd`) literate programming document rendered to PDF. The geospatial engineering stages (API queries, spatial joins, deduplication) run in R using `httr`, `jsonlite`, `sf`, `dplyr`, and `leaflet`. The text analytics stage (NER) runs in Python using `spaCy` via reticulate, with entity results passed back to R for cross-referencing and visualization. This R-Python bridge pattern follows the same approach used in ITEC/SIS 724 Advanced Text Analytics coursework at American University.

The source layers, their access methods, and their licenses are as follows:

| Source | Method | License | Facilities |
|--------|--------|---------|------------|
| OpenStreetMap | Overpass API (REST, JSON) | ODbL | 20 |
| Wikidata | SPARQL endpoint | CC0 | 0 |
| AWS | Regional services JSON | Public documentation | 1 |
| Azure, GCP, Oracle, Alibaba | Curated from public announcements | Public documentation | 4 |
| Trade press corpus | Search snippets + press releases, spaCy NER | Publicly available text | 25 |

## Coverage Gap Analysis

The most analytically significant finding is the systematic divergence between open geospatial data and the actual landscape of Malaysian datacenter infrastructure. OSM captures community-mapped colocation facilities and satellite-traced building footprints but lacks operator attribution for 100% of its entries; Wikidata's structured datacenter coverage in Southeast Asia is effectively zero; cloud provider APIs capture the hyperscale layer but at region-level granularity rather than facility-level; and trade press is the only source that identifies the colocation operators (Bridge Data Centres, AirTrunk, DayOne, NTT, EdgeConneX, Vantage) that collectively account for the majority of Malaysia's built IT capacity. No single source is sufficient. The multi-source reconciliation approach is the contribution.

## Expansion (April 2026): v4 → v7.1

The v3 dataset grew from 50 to 133 facilities through a series of targeted expansion, correction, and consolidation passes. Each pass is reproducible from cached HTTP responses and logged decisions.

**v4 / v5 / v5.1 — operator-tier scoring and postprocessing.** v4 added explicit operator-tier classification (`hyperscale`, `tier1_colo`, `tier2_colo`, `cloud_region`, etc.) and per-row confidence scores; v5 extended the dataset to the `datacenter-malaysia-v4.qmd` Quarto document. v5.1 was a postprocessing pass that reconciled duplicate entries, normalized operator names, and produced `malaysia_datacenters_v5_1.csv` as the merge baseline for v6.

**v6 — open-source expansion with five new scrapers.** After Data Center Map's terms of service were found to prohibit database integration, the v6 sprint added five scrapers targeting openly-licensed or publicly-disclosed sources: `peeringdb_scraper.py` (PeeringDB facility API), `mida_scraper.py` (MIDA press-release corpus), `st_scraper.py` (Suruhanjaya Tenaga licensee filings), `mdec_dnb_scraper.py` (MDEC Digital Nusantara Berhad records), and `operator_ir_scraper.py` (operator investor-relations pages, ~40 operators). All scrapers use stdlib-only HTTP (no third-party libraries), a 1-second inter-request delay, URL-hash file cache, and a descriptive research User-Agent. `merge_v6.py` consolidated the five candidate files with the v5.1 baseline into `malaysia_datacenters_v6_master.csv` (101 rows × 38 columns), stratified into three confidence layers (`v6_high_confidence.csv`, `v6_medium_confidence.csv`, `v6_needs_review.csv`).

**Gapfill sprint (2026-04-17) — v6 candidates + corrections + dedup.** A focused gapfill pass emitted 42 candidate rows (`v6_gapfill_candidates.csv`) with each row carrying a `promotion_action` (`add_new`, `update_existing:<idx>`, `review:*`, `hold:*`, `skip:*`, or `reject:*`). A separate dedup pass produced 6 decisions (`v6_dedup_fixes.csv`) covering suspected duplicates. `build_dedup_fixes.py`, `emit_candidates.py`, `apply_corrections.py`, and `build_report.py` implement the pipeline.

**Dedup resolution pass — 4 pending-verification entries resolved via operator sources.** Four dedup decisions that had been downgraded to `merge_pending_verification` (no independent coordinate corroboration in cache) were resolved by fetching operator-direct pages: Keppel DC Johor 1 (keppeldatacentres.com confirmed facility identity), Princeton Digital JH1 (princetondg.com confirmed Sedenak Tech Park location, invalidating a 15 km-drifted PeeringDB coord), AIMS CJ1 vs. AIMS Cyberjaya (reversed to `not_duplicate_keep_both` — PeeringDB records have distinct Cyberjaya zone addresses), and NTT CBJ vs. CBJ6 (resolved to `not_duplicate_keep_both` — NTT's newsroom confirms a multi-building numbered campus where CBJ is historical shorthand for the original CBJ1 building).

**v7 master — 134 rows.** `build_v7.py` applied all finalized decisions to produce `malaysia_datacenters_v7_master.csv`, adding three audit columns (`v7_status`, `v7_pending_review`, `v7_change_log`) without altering the 38 inherited columns. A final dedup sweep surfaced 8 potential same-operator duplicates and 42 cotenancy pairs as `pending_review` flags rather than auto-merging.

**v7.1 — 6 targeted fixes → 133 rows.** `build_v7_1.py` applied a narrow correction pass: an AWS double-pin merger, a DayOne/GDS naming collision flag (kept both, building-level granularity preserved), NTT CBJ renamed to CBJ1 with source corroboration added, a Google/i-City geocoder-artifact flag, a Google/Gamuda Port Dickson ambiguity flag, and documentation of shared-geocode status for the Racks Central, YTL Johor, and DayOne NTP cluster phases. One deletion (the duplicate AWS pin); all other fixes were documentation-only or flag-only.

**Known gaps carried to v8.** Five facilities with sole-source TOS-blocked provenance (VADS Semarak, VADS Bayan Baru, VADS Labuan, AWS KUL Dengkil, AWS KUL Bukit Jalil) are listed in the v7 build report for a future SSM/MCMC/planning-portal resolution pass. The NTT CBJ1 coordinate remains blank — Nominatim has no entry for "Jalan APEC, Cyberjaya" under any of five query variants tried.

## Repository Contents

| File | Description |
|------|-------------|
| `datacenter-malaysia-v3.qmd` / `.pdf` | Original v3 pipeline (50 facilities) |
| `datacenter-malaysia-v4.qmd` / `.pdf` | v4 pipeline with operator-tier scoring |
| `outputs/malaysia_datacenters_v3.csv` | v3 dataset (50 rows) |
| `malaysia_expansion_2026_04_17/` | v5–v7.1 expansion pipeline and outputs |
| `malaysia_expansion_2026_04_17/outputs/malaysia_datacenters_v6_master.csv` | v6 master (101 rows × 38 cols) |
| `malaysia_expansion_2026_04_17/gapfill_2026_04_17/outputs/malaysia_datacenters_v7_master.csv` | v7 master (134 rows × 41 cols) |
| `malaysia_expansion_2026_04_17/gapfill_2026_04_17/outputs/malaysia_datacenters_v7_1_master.csv` | v7.1 master (133 rows × 41 cols) |
| `malaysia_expansion_2026_04_17/gapfill_2026_04_17/outputs/v6_dedup_resolution_report.md` | Dedup pending-verification resolution report |
| `malaysia_expansion_2026_04_17/gapfill_2026_04_17/outputs/v7_build_report.md` | v6 → v7 build report |
| `malaysia_expansion_2026_04_17/gapfill_2026_04_17/outputs/v7_to_v7_1_fix_report.md` | v7 → v7.1 fix-pass report |
| `outputs/overpass_malaysia_raw.json` | Cached Overpass API response |

## Requirements

The pipeline requires R (4.3+) with `httr`, `jsonlite`, `dplyr`, `tidyr`, `stringr`, `purrr`, `sf`, `leaflet`, `ggplot2`, `knitr`, and `reticulate`; Python (3.11+) via Anaconda with `pandas`, `spacy`, and the `en_core_web_sm` model; and a Natural Earth 50m country shapefile (public domain, downloaded automatically if not staged locally). A Google Maps API key is not required for the current pipeline but would be needed to extend the geocoding stage.

## Scaling to Other Countries

The pipeline generalizes by changing two parameters: the Overpass `area["ISO3166-1"="MY"]` filter accepts any ISO 3166-1 country code, and the Wikidata `wdt:P17 wd:Q833` constraint accepts any Wikidata country entity ID. Cloud provider and trade press layers require per-country curation, though the methodology and corpus construction approach remain identical. A global-scale run would need to chunk the Overpass query by region to avoid server timeouts, a constraint documented in the pipeline's reflection section.

## Author

Daniel Chavez | Graduate Student, American University
