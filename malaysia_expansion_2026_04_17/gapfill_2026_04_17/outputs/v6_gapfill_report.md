# Malaysia DC Corpus — v6 Gapfill Report

Generated 2026-04-17 by the `gapfill_2026_04_17` sprint against `malaysia_datacenters_v6_master.csv` (101 rows).

## Headline

* **42 candidates produced** across 4 tiers.
  * `add_new`: **37** — pass dedup clean, ready to promote after spot-check.
  * `review`: **4** — flagged for human triage (co-tenant buildings, same-operator-near-miss).
  * `skip`: **1** — correctly detected as duplicates of existing v6 rows.
* **6 dedup fixes** written to `v6_dedup_fixes.csv` covering the 5 known-dup cases.

## Rows by tier

| Tier | Candidates | Description |
|---|---|---|
| A_under_represented | 17 | A — under-represented operators (VADS, TM Global, Maxis, local bumi) |
| B_known_ops | 6 | B — known operators, coverage cross-check |
| C_hyperscaler_az | 3 | C — hyperscaler physical AZ discovery (AWS) |
| D_regional | 16 | D — regional market sweeps (secondary / east Malaysia) |

## Rows by source_category

| source_category | Count |
|---|---|
| Trade Press | 21 |
| Operator Website | 18 |
| Hyperscaler AZ Discovery | 3 |

## Coordinate confidence

| coord_confidence | Count | Meaning |
|---|---|---|
| building | 26 | Nominatim returned a named building / street match on full address |
| approximate_locality | 15 | Fell back to simplified address or city centroid |
| exact | 1 | Nominatim returned importance ≥ 0.5 on full address |

## Per-operator outcomes

### Tier A — under-represented operators

| Operator | Sites added | Notes |
|---|---|---|
| VADS Berhad | 7 | 7 regional sites (IPDC, KVDC, Brickfields, Semarak, Bayan Baru, Ipoh, Labuan). Sourced from trade directories. |
| TM Global | 5 | 5 strategic DCs (BFDC/KJDC/KVDC/IPDC/SJDC) from TM Global operator page. Some may dup v6 PeeringDB 'TM ONE' rows — review required. |
| Maxis | 1 | Maxis i-City Shah Alam from operator page. |
| MyTelehaus | 2 | PJ3 (Plaza33) and CJ1 (Jalan Kemajuan PJ). |
| Silverstream | 1 | Bagan Datuk (planned 2027, Privasia subsidiary). |
| Exitra | 1 | KL MSC integrator. |
| Progenet | 1 | PJ (Menara Lien Hoe). |
| Teliti | 1 | Bandar Enstek ETP key project. |
| SKALI | 1 | UPM Serdang. |
| HDC | 1 | Shah Alam, first Uptime Tier-III-certified DC in Malaysia (2012). |
| MaNaDr | 1 | Bintulu Sarawak, 150MW planned end-2028. |
| Exabytes | 3 | Three sites — Penang Suntech, KL Menara AIMS, Cyberjaya CJ1. All co-tenant buildings (flagged review). |

### Tier B — known operators, cross-check

| Operator | Sites added | Notes |
|---|---|---|
| Bridge Data Centres | 5 | MY01, MY03 added; MY02 correctly skipped as v6 dup. Plus IOI Banting Campus and Mah Sing Southville JV. |
| Racks Central | 3 | RCJM1, RCJM2A, RCJM2B all Pasir Gudang Iskandar Halal Park. |
| K2 Data Centres | 1 | JHR1 Sedenak (300MW hyperscale). |
| YTL Data Center Holdings | 2 | Green DC Park Kulai + Sentul. Kulai campus likely dup v5.1 — see v6_dedup_fixes. |
| ZDATA | 1 | GP3 Gelang Patah, RM8bn, first GreenRE Platinum DC. |
| Gamuda | 1 | Springhill Port Dickson 158ha campus. |
| Google | 1 | Port Dickson (Springhill) planned. |

### Tier C — hyperscaler AZ discovery

Attempted: AWS ap-southeast-5 (3 AZs).

| Site | coord_confidence | Sources | Status |
|---|---|---|---|
| AWS Cyberjaya (Persiaran APEC) | building | DCD report, Lowyat trade press, baxtel listing | add_new — best-documented of the three |
| AWS KUL Dengkil | approximate_locality | datacentermap listing | add_new — locality only, no exact address |
| AWS KUL Bukit Jalil (TPM) | building | datacentermap listing | **review** — within 150m of JARING Bukit Jalil (different operator); may be co-tenant at TPM or same building |

Not attempted this sprint: Azure Malaysia West, GCP asia-southeast3, Alibaba, Oracle physical sites. The AWS trade-press footprint is denser than for other hyperscalers; deferred Azure/GCP/etc. to a follow-up pass.

### Tier D — regional market sweeps

Found coverage:

* **Port Dickson / Springhill** — 2 new projects (Google hyperscale, Gamuda 158ha)
* **Gelang Patah / Johor** — ZDATA GP3 RM8bn
* **Banting / Selangor** — Bridge DC IOI 136-acre campus
* **Bangi / Selangor** — Mah Sing DC Hub @ Southville City (Bridge DC JV)
* **Ipoh / Perak** — VADS Ipoh (regional Telekom exchange)
* **Labuan** — VADS Labuan
* **Bintulu / Sarawak** — MaNaDr planned
* **Bandar Enstek / Negeri Sembilan** — Teliti (already had Regal Orion in v5.1)

Zero / sparse yield — worth manual follow-up:

* **Kota Kinabalu** — no DC operator surfaced via trade-press search
* **Miri / Sarawak** — no DC found
* **Melaka** — only LK Global Engineering (an EPC contractor, not an operator)
* **Kuching / Sarawak** — covered by existing v5.1 irix Santubong + v6_review SACOFA/Danawa

## Blocked / failed fetches

* `https://www.vads.com/data-centre` — 404. Fell back to `vads.com` press pages via Google search.
* `https://www.bridgedatacentres.com/locations/` — TLS certificate failure. Fell back to datacenters.com + baxtel which mirror Bridge's own listings.
* `https://www.maxis.com.my/business/data-centre/` — 404. Found Maxis i-City via Google + Tier-III-certification trade press.
* `https://www.k2dc.com/` — timeout. Data surfaced via datacenterHawk.
* `https://silverstream.com.my/` — connection refused. Trade press (Bernama, Edge, Lowyat) had the facts.
* `https://www.mytelehaus.com/` — 500. Surfaced via Inflect + DCD.
* `https://www.strateq.com.my/` — connection refused. No fallback yielded a facility address this pass; Strateq DC-1 and DC-2 remain unlocated.
* `https://www.i-city.com.my/datacentre` → `https://corp.i-city.my/datacentre` — the corp site no longer lists DC-specific content. HDC surfaced via its own `hdc.net.my`.

## Coverage notes for v6 maintainers

* **Building co-tenancy** surfaced as a methodology issue during this sprint. Three co-tenant buildings were flagged (Exabytes inside Menara AIMS / Suntech Cybercity / Open DC CJ1). Dedup now distinguishes same-operator-same-coord (skip) from different-operator-same-coord (review). Recommend making this a durable data model rule: multiple operator rows at the same coord are expected when the building is a carrier hotel or multi-tenant DC.
* **Dual-brand VADS/TM**: seven VADS sites added. They pair with TM Global's six strategic DCs at overlapping addresses (BFDC ↔ VADS Brickfields, KVDC ↔ VADS KVDC, IPDC ↔ VADS IPDC). Existing v5.1 convention treats them as separate rows; this sprint continues that convention.
* **AWS AZ methodology**: trade-press locality reconstruction works for the Cyberjaya site (Persiaran APEC) but only gets district-level for Dengkil and Bukit Jalil. Street-level AZ coordinates would require SSM filings for Amazon Data Services Malaysia Sdn Bhd (not attempted — would require paid lookup) or satellite verification of fenced compounds near reported localities.
* **Geocoder caveats**: 14 candidates use `approximate_locality` because Nominatim couldn't match the full address (Malaysian lot numbers, building suite identifiers). These rows have real city-level coords but should be spot-checked against satellite imagery before merge.

## Recommendations for the next OSINT pass

1. **SSM filings for hyperscaler local entities** — `Amazon Data Services Malaysia Sdn Bhd`, `Microsoft Malaysia Sdn Bhd`, `Google Cloud Malaysia Sdn Bhd`, `Alibaba Cloud Malaysia Sdn Bhd`. Registered business address typically discloses primary site. SSM MyData has free basic lookup.
2. **MCMC NFP/NSP licensee register** — operators holding Network Facilities Provider licenses must declare facility addresses. Captures the local colo operators (Strateq, ZDATA, Teliti, SKALI) without needing their websites.
3. **Planning portal satellite verification** — for AWS Dengkil and Bukit Jalil, a satellite recon pass (large fenced compounds with chiller yards near reported localities) would confirm or refute the trade-press locations.
4. **Azure / GCP / Alibaba / Oracle** physical AZ discovery using the same methodology as AWS. Lower trade-press density but may surface with targeted DCD/Data Center Frontier queries.
5. **Sabah / Kota Kinabalu / Miri** — zero-yield this pass. Worth a one-hour manual search of local press (Daily Express, Borneo Post) which often covers Sabah-specific industrial announcements missed by peninsular trade press.
6. **Strateq DC-1 and DC-2** — connection-refused on operator site. Try Wayback or reach out to Strateq directly.
