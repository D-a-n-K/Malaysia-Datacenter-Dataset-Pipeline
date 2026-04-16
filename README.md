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

## Repository Contents

| File | Description |
|------|-------------|
| `datacenter-malaysia-v3.qmd` | Complete Quarto pipeline, renders to PDF |
| `datacenter-malaysia-v3.pdf` | Rendered output (41 pages) |
| `outputs/malaysia_datacenters_v3.csv` | Final dataset (50 rows) |
| `outputs/malaysia_datacenters_v3.rds` | R-native serialized dataset |
| `outputs/overpass_malaysia_raw.json` | Cached Overpass API response |

## Requirements

The pipeline requires R (4.3+) with `httr`, `jsonlite`, `dplyr`, `tidyr`, `stringr`, `purrr`, `sf`, `leaflet`, `ggplot2`, `knitr`, and `reticulate`; Python (3.11+) via Anaconda with `pandas`, `spacy`, and the `en_core_web_sm` model; and a Natural Earth 50m country shapefile (public domain, downloaded automatically if not staged locally). A Google Maps API key is not required for the current pipeline but would be needed to extend the geocoding stage.

## Scaling to Other Countries

The pipeline generalizes by changing two parameters: the Overpass `area["ISO3166-1"="MY"]` filter accepts any ISO 3166-1 country code, and the Wikidata `wdt:P17 wd:Q833` constraint accepts any Wikidata country entity ID. Cloud provider and trade press layers require per-country curation, though the methodology and corpus construction approach remain identical. A global-scale run would need to chunk the Overpass query by region to avoid server timeouts, a constraint documented in the pipeline's reflection section.

## Author

Daniel Chavez | Graduate Student, American University
