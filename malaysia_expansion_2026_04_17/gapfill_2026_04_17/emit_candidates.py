"""
emit_candidates.py — turn the structured research findings into candidate rows
against v6, with Nominatim geocoding + dedup.

Each CANDIDATE tuple below comes from an independent OSINT fetch (see
`source_urls`). Coordinates, if present, are geocoded via Nominatim from the
operator-supplied address — never copied from any reference file.
"""

from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from gapfill_common import (  # noqa: E402
    V6_SCHEMA,
    blank_candidate_row,
    dedup_check,
    geocode_address,
    load_v6,
)

# Each candidate: a dict describing one independent OSINT finding.
#   name, operator, operator_norm, address, city, source_urls, source_category,
#   note, physical_facility
CANDIDATES: list[dict] = [
    # ─── Tier A: Bridge Data Centres ────────────────────────────────────
    {
        "name": "Bridge Data Centres MY01 (Cyberjaya)",
        "operator": "Bridge Data Centres",
        "operator_norm": "Bridge Data Centres",
        "address": "7118 Jalan Impact, Cyber 6, Cyberjaya, Selangor",
        "city": "Cyberjaya",
        "source_urls": [
            "https://www.datacenters.com/providers/bridge-data-centres/data-center-locations",
            "https://baxtel.com/data-center/bridge-data-centres-cyberjaya-my01",
        ],
        "source_category": "Trade Press",
        "note": "Bridge DC's first Malaysia site. Operator-listed across baxtel + datacenters.com.",
        "physical_facility": True,
    },
    {
        "name": "Bridge Data Centres MY02 (Cyberjaya)",
        "operator": "Bridge Data Centres",
        "operator_norm": "Bridge Data Centres",
        "address": "Jalan Cyber Point 2, Cyber 12, Cyberjaya, Selangor",
        "city": "Cyberjaya",
        "source_urls": [
            "https://www.datacenters.com/bridge-data-centres-my02-cyberjaya-malaysia",
            "https://baxtel.com/data-center/bridge-data-centres-cyberjaya-my02",
        ],
        "source_category": "Trade Press",
        "note": "Bridge DC Cyberjaya second site. Already referenced via PeeringDB in v6_review; address is more specific here.",
        "physical_facility": True,
    },
    {
        "name": "Bridge Data Centres MY03 (Bukit Jalil)",
        "operator": "Bridge Data Centres",
        "operator_norm": "Bridge Data Centres",
        "address": "Jalan Inovasi 3, MRANTI Park, Bukit Jalil, Kuala Lumpur",
        "city": "Bukit Jalil",
        "source_urls": [
            "https://www.datacenters.com/bridge-data-centres-my03-bukit-jalil-malaysia",
            "https://www.datacenterdynamics.com/en/news/bridge-dc-expands-in-malaysia-with-data-center-in-cyberjaya/",
        ],
        "source_category": "Trade Press",
        "note": "Hyperscale campus in MRANTI Park, Bukit Jalil. 48MW IT power per DCD.",
        "physical_facility": True,
    },
    # ─── Tier A: VADS (dual-brand with TM Global) ──────────────────────
    {
        "name": "VADS Iskandar Puteri Data Centre (IPDC)",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Nusajaya Tech Park, Iskandar Puteri, Johor",
        "city": "Iskandar Puteri",
        "source_urls": [
            "http://www.vads.com/telekom-malaysia-berhad-and-nusajaya-tech-park-sdn-bhd-partner-to-develop-data-center-in-iskandar-malaysia-johor/",
            "http://www.vads.com/tm-develops-a-purpose-built-data-centre-and-invests-in-nusajaya-tech-park-3/",
        ],
        "source_category": "Operator Website",
        "note": "VADS-branded facility co-located with TM Global IPDC at Nusajaya Tech Park. Dual-brand convention per v5.1.",
        "physical_facility": True,
    },
    {
        "name": "VADS Klang Valley Data Centre (KVDC)",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Cyberjaya, Selangor",
        "city": "Cyberjaya",
        "source_urls": [
            "http://www.vads.com/vads-gears-up-its-twin-core-strategy-in-malaysia/",
            "http://www.vads.com/about-us/",
        ],
        "source_category": "Operator Website",
        "note": "VADS twin-core DC. Operator statement: 'IPDC and KVDC placed 320km apart with fast fibre link.'",
        "physical_facility": True,
    },
    # ─── Tier A: Maxis i-City ──────────────────────────────────────────
    {
        "name": "Maxis i-City Data Centre (Shah Alam)",
        "operator": "Maxis Berhad",
        "operator_norm": "Maxis",
        "address": "Block M, No 6, Persiaran Multimedia i-City, Seksyen 7, 40000 Shah Alam, Selangor",
        "city": "Shah Alam",
        "source_urls": [
            "https://www.business.maxis.com.my/en/cloud/data-centres/",
            "https://www.malaysianwireless.com/2012/10/maxis-data-centre-gets-tier-iii-certification/",
        ],
        "source_category": "Operator Website",
        "note": "First Tier-III-certified DC in Malaysia (2012). Located in i-City complex Shah Alam.",
        "physical_facility": True,
    },
    # ─── Tier A: Racks Central ─────────────────────────────────────────
    {
        "name": "Racks Central RCJM1 (Pasir Gudang)",
        "operator": "Racks Central",
        "operator_norm": "Racks Central",
        "address": "Iskandar Halal Park, 81700 Pasir Gudang, Johor",
        "city": "Pasir Gudang",
        "source_urls": [
            "https://www.rackscentral.com/locations/rcjm1",
            "https://www.rackscentral.com/news/racks-central-expands-data-center-capacity-with-landmark-acquisition-in-iskandar-halal-park-johor",
        ],
        "source_category": "Operator Website",
        "note": "Racks Central's first Malaysia site. 90MW IT load, 9200sqm white space.",
        "physical_facility": True,
    },
    {
        "name": "Racks Central RCJM2A (Pasir Gudang)",
        "operator": "Racks Central",
        "operator_norm": "Racks Central",
        "address": "Iskandar Halal Park, 81700 Pasir Gudang, Johor",
        "city": "Pasir Gudang",
        "source_urls": ["https://www.rackscentral.com/locations/rcjm2a"],
        "source_category": "Operator Website",
        "note": "Adjacent 12.9-acre expansion. 175MW target IT load.",
        "physical_facility": True,
    },
    {
        "name": "Racks Central RCJM2B (Pasir Gudang)",
        "operator": "Racks Central",
        "operator_norm": "Racks Central",
        "address": "Iskandar Halal Park, 81700 Pasir Gudang, Johor",
        "city": "Pasir Gudang",
        "source_urls": ["https://www.rackscentral.com/locations/rcjm2b"],
        "source_category": "Operator Website",
        "note": "Second Pasir Gudang phase. 175MW target, AI-native.",
        "physical_facility": True,
    },
    # ─── Tier A: MyTelehaus ────────────────────────────────────────────
    {
        "name": "MyTelehaus PJ3 Centre (Petaling Jaya)",
        "operator": "MyTelehaus",
        "operator_norm": "MyTelehaus",
        "address": "Plaza33, Petaling Jaya, Selangor",
        "city": "Petaling Jaya",
        "source_urls": [
            "http://mytelehaus.com/pj3-centre/",
            "https://www.datacenterdynamics.com/en/news/operator-mytelehaus-to-open-data-center-in-selangor-malaysia/",
        ],
        "source_category": "Operator Website",
        "note": "First Tier-3 DC in Malaysia in a high-rise. MSC status.",
        "physical_facility": True,
    },
    {
        "name": "MyTelehaus CJ1 (Petaling Jaya)",
        "operator": "MyTelehaus",
        "operator_norm": "MyTelehaus",
        "address": "No. 1 Jalan Kemajuan, 46200 Petaling Jaya, Selangor",
        "city": "Petaling Jaya",
        "source_urls": [
            "https://inflect.com/no-1-jalan-kemajuan-petaling-jaya/mytelehaus/datacenter/cj1",
            "https://baxtel.com/data-center/mytelehaus-cyberjaya-cj1-centre",
        ],
        "source_category": "Trade Press",
        "note": "Despite the 'CJ' naming, sited in Petaling Jaya per Inflect/baxtel.",
        "physical_facility": True,
    },
    # ─── Tier A: Silverstream (Privasia subsidiary) ────────────────────
    {
        "name": "SILVERSTREAMS Data Centre (Bagan Datuk)",
        "operator": "Silver Streams Technofarm Sdn Bhd",
        "operator_norm": "Silverstream",
        "address": "Bagan Datuk, Perak",
        "city": "Bagan Datuk",
        "source_urls": [
            "https://silverstreams.ai/",
            "https://www.bernama.com/misc/rss/news.php?id=2543321",
            "https://theedgemalaysia.com/node/779495",
        ],
        "source_category": "Trade Press",
        "note": "RM569M contract with Inspur Cloud. 10MW phase-1 by Q4 2027, 49-acre site, up to 25MW. Privasia subsidiary; Mara+Felcra backed.",
        "physical_facility": True,
    },
    # ─── Tier A: Exitra & Progenet ─────────────────────────────────────
    {
        "name": "Exitra Data Center (Kuala Lumpur)",
        "operator": "Exitra Sdn Bhd",
        "operator_norm": "Exitra",
        "address": "No 1, Building LGB, Jalan Wan Kadir, 60000 Kuala Lumpur",
        "city": "Kuala Lumpur",
        "source_urls": [
            "https://datacentercatalog.com/malaysia/exitra-data-center",
        ],
        "source_category": "Trade Press",
        "note": "MSC-status local integrator. Single source — needs a second corroboration before promotion.",
        "physical_facility": True,
    },
    {
        "name": "Progenet Data Center (Petaling Jaya)",
        "operator": "Progenet Innovations",
        "operator_norm": "Progenet",
        "address": "Menara Lien Hoe, 47410 Petaling Jaya, Selangor",
        "city": "Petaling Jaya",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/kuala-lumpur/progenet-innovations/",
            "https://datacentercatalog.com/malaysia/progenet-data-center-tier-31",
        ],
        "source_category": "Trade Press",
        "note": "Tier-3. 7000sqft/floor. Single-source-ish — two trade directories.",
        "physical_facility": True,
    },
    # ─── Tier A: Teliti ────────────────────────────────────────────────
    {
        "name": "Teliti Datacentre (Bandar Enstek)",
        "operator": "Teliti Datacentres",
        "operator_norm": "Teliti",
        "address": "Techpark@Enstek, Lot PT29470 & PT29471, Bandar Enstek, Mukim Labu, Daerah Seremban, Negeri Sembilan",
        "city": "Bandar Enstek",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/mukim-labu/teliti-datacentre/",
            "https://www.edgeprop.my/content/teliti-data-centre-named-key-project-under-business-services-sector-epp",
        ],
        "source_category": "Trade Press",
        "note": "Tier-3, 120,000sqft NLA. ETP key project under Business Services sector. Targeting Tier-4.",
        "physical_facility": True,
    },
    # ─── Tier A: SKALI ─────────────────────────────────────────────────
    {
        "name": "SKALI Serdang Internet Data Center",
        "operator": "SKALI",
        "operator_norm": "SKALI",
        "address": "Block 1, UPM-MTDC Server Farm Complex, Lebuh Silikon, Universiti Putra Malaysia, 43400 Serdang, Selangor",
        "city": "Serdang",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/selangor/skali-internet-data-center/",
            "https://datacentercatalog.com/malaysia/skali-internet-data-center1",
        ],
        "source_category": "Trade Press",
        "note": "Tier-2, 6500sqft, 600-1000 servers. SKALI IDC operating since 2000.",
        "physical_facility": True,
    },
    # ─── Tier A: HDC (co-tenant at Maxis i-City building) ─────────────
    {
        "name": "HDC High Performance Data Centre (Shah Alam)",
        "operator": "HDC Data Centre Sdn Bhd",
        "operator_norm": "HDC",
        "address": "M-04-3A, Block M, No 6, Persiaran Multimedia i-City, Seksyen 7, 40000 Shah Alam, Selangor",
        "city": "Shah Alam",
        "source_urls": [
            "https://www.hdc.net.my/datacenter.php",
            "https://www.datacentermap.com/malaysia/shah-alam/high-performance-data-centre/",
        ],
        "source_category": "Operator Website",
        "note": "First Uptime Tier-III-certified DC in Malaysia (2012). Co-located with Maxis i-City DC in same i-City Block M building (M-04-3A vs Block M No.6 main).",
        "physical_facility": True,
    },
    # ─── Tier A: MaNaDr (planned, Bintulu) ─────────────────────────────
    {
        "name": "MaNaDr Bintulu AI Data Centre (planned)",
        "operator": "Mobile-health Network Solutions",
        "operator_norm": "MaNaDr",
        "address": "Bintulu, Sarawak",
        "city": "Bintulu",
        "source_urls": [
            "https://www.datacenterdynamics.com/en/news/singapore-health-firm-manadr-pivots-to-ai-data-center-development/",
            "https://www.stocktitan.net/news/MNDR/mobile-health-network-solutions-signs-mou-to-secure-two-malaysian-ai-b49dgkp9mimn.html",
        ],
        "source_category": "Trade Press",
        "note": "Binding MOU Nov 2025. 150MW site in Bintulu area targeted end-2028. Separate 25MW MY site also in MOU.",
        "physical_facility": True,
    },
    # ─── Tier B: TM Global (cross-check coverage) ──────────────────────
    {
        "name": "TM Global BFDC (Brickfields) Data Centre",
        "operator": "TM Global",
        "operator_norm": "TM Global",
        "address": "Brickfields, Kuala Lumpur",
        "city": "Kuala Lumpur",
        "source_urls": [
            "https://tmglobal.com.my/products-and-solutions/hosting/data-centre",
            "https://www.tm.com.my/news/tm_global_expands_data_centres_cyberjaya_johor",
        ],
        "source_category": "Operator Website",
        "note": "Urban DC in KL city centre. One of TM Global's six strategic DCs (BFDC/KJDC/IPDC/KVDC/SJDC/HKDC).",
        "physical_facility": True,
    },
    {
        "name": "TM Global KJDC (Kelana Jaya) Data Centre",
        "operator": "TM Global",
        "operator_norm": "TM Global",
        "address": "Kelana Jaya, Petaling Jaya, Selangor",
        "city": "Kelana Jaya",
        "source_urls": [
            "https://tmglobal.com.my/products-and-solutions/hosting/data-centre",
        ],
        "source_category": "Operator Website",
        "note": "One of six TM Global strategic DCs.",
        "physical_facility": True,
    },
    {
        "name": "TM Global KVDC (Klang Valley Core) Data Centre",
        "operator": "TM Global",
        "operator_norm": "TM Global",
        "address": "Cyberjaya, Selangor",
        "city": "Cyberjaya",
        "source_urls": [
            "https://tm.com.my/news/tm-one-unveils-its-latest-state-of-the-art-klang-valley-core-data-centre",
            "https://www.datacenterdynamics.com/en/news/tm-one-opens-cyberjaya-malaysia-facility/",
        ],
        "source_category": "Operator Website",
        "note": "Hyperconnected DC in Cyberjaya. May duplicate v6 PeeringDB 'TM ONE KVDC' — dedup check required.",
        "physical_facility": True,
    },
    {
        "name": "TM Global IPDC (Iskandar Puteri) Data Centre",
        "operator": "TM Global",
        "operator_norm": "TM Global",
        "address": "Nusajaya Tech Park, Iskandar Puteri, Johor",
        "city": "Iskandar Puteri",
        "source_urls": [
            "https://tmglobal.com.my/products-and-solutions/hosting/data-centre",
            "http://www.vads.com/tm-develops-a-purpose-built-data-centre-and-invests-in-nusajaya-tech-park-3/",
        ],
        "source_category": "Operator Website",
        "note": "Co-located with VADS IPDC (dual-brand).",
        "physical_facility": True,
    },
    {
        "name": "TM Global SJDC (Subterranean Penang) Data Centre",
        "operator": "TM Global",
        "operator_norm": "TM Global",
        "address": "Penang",
        "city": "Penang",
        "source_urls": [
            "https://tmglobal.com.my/products-and-solutions/hosting/data-centre",
        ],
        "source_category": "Operator Website",
        "note": "SJDC = TM's Penang DC. Rough locality only; needs Penang address to pinpoint.",
        "physical_facility": True,
    },
    # ─── Tier B: YTL ───────────────────────────────────────────────────
    {
        "name": "YTL Green Data Center Park (Kulai)",
        "operator": "YTL Data Center Holdings",
        "operator_norm": "YTL Data Center Holdings",
        "address": "Kulai, Johor",
        "city": "Kulai",
        "source_urls": [
            "https://www.ytl.com/press-releases/ytl-green-data-center-park-launches-in-johor-the-first-integrated-data-center-park-powered-by-renewable-solar-energy-in-malaysia-2/",
            "https://www.ytl.com/sustainability/shownews.asp?newsid=4898&category=inthenews",
        ],
        "source_category": "Operator Website",
        "note": "111ha campus, 500MW capacity over 10 years. Already in v5.1 — dedup expected.",
        "physical_facility": True,
    },
    {
        "name": "YTL Sentul Data Centre (Kuala Lumpur)",
        "operator": "YTL Data Center Holdings",
        "operator_norm": "YTL Data Center Holdings",
        "address": "Sentul, Kuala Lumpur",
        "city": "Sentul",
        "source_urls": [
            "https://www.ytl.com/sustainability/feature-stories/green-data-centers-for-a-sustainable-future/",
        ],
        "source_category": "Operator Website",
        "note": "5MW, being upgraded to Tier-III. Urban KL site.",
        "physical_facility": True,
    },
    # ─── Tier B: K2DC ──────────────────────────────────────────────────
    {
        "name": "K2 Data Centres JHR1 (Sedenak Tech Park)",
        "operator": "K2 Data Centres",
        "operator_norm": "K2 Data Centres",
        "address": "Lot PTD 31493, Jalan Digital 8, Taman Teknologi Sedenak (Step), Mukim Bukit Batu, 82100 Kulai, Johor",
        "city": "Kulai",
        "source_urls": [
            "https://datacenterhawk.com/marketplace/providers/k2-data-centres/ptd-31493-jalan-digital-8-taman-teknologi-sedenak/jhr1",
            "https://baxtel.com/data-center/k2-johor-jhr1",
            "https://w.media/k2-and-tnb-secure-power-deal-for-k2s-hyperscale-data-center-campus-in-johor/",
        ],
        "source_category": "Trade Press",
        "note": "300MW hyperscale campus. Kuok Group subsidiary. TNB power deal signed.",
        "physical_facility": True,
    },
    # ─── Tier D: VADS regional network ─────────────────────────────────
    {
        "name": "VADS Brickfields Data Centre",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Level 6, Bangunan Telekom Brickfields, Jalan Tun Sambathan, 50470 Kuala Lumpur",
        "city": "Brickfields",
        "source_urls": [
            "https://tiaonline.org/942-datacenter/tm-one-vads-berhad-vads-brickfields-city-data-centre-facility-9th-floor-jalan-tun-sambanthan-kampung-attap-kuala-lumpur/",
            "https://www.datacentermap.com/malaysia/kuala-lumpur/vads-brickfields/",
        ],
        "source_category": "Trade Press",
        "note": "VADS-branded DC in TM Brickfields building. Dual-brand with TM Global BFDC.",
        "physical_facility": True,
    },
    {
        "name": "VADS Semarak Data Centre",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Level 19, Menara Celcom, Jalan Semarak, 54100 Kuala Lumpur",
        "city": "Kuala Lumpur",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/kuala-lumpur/vads-semarak/",
        ],
        "source_category": "Trade Press",
        "note": "VADS urban DC at Menara Celcom, Jalan Semarak.",
        "physical_facility": True,
    },
    {
        "name": "VADS Bayan Baru Data Centre (Penang)",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Level 1, Ibusawat Telekom Bayan Baru, Jalan Tengah, 11950 Bayan Baru, Penang",
        "city": "Bayan Baru",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/bayan-baru/vads-bayan-baru/",
        ],
        "source_category": "Trade Press",
        "note": "VADS Penang site at the Bayan Baru Telekom exchange.",
        "physical_facility": True,
    },
    {
        "name": "VADS Ipoh Data Centre (Perak)",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Level 5, Ibusawat Telekom Ipoh, Jalan Datuk Onn Jaafar, 30300 Ipoh, Perak",
        "city": "Ipoh",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/ipoh/vads-ipoh/",
            "https://baxtel.com/data-center/vads-ipoh",
        ],
        "source_category": "Trade Press",
        "note": "Tier 2 managed DC inside the Ipoh Telekom exchange. VADS regional site.",
        "physical_facility": True,
    },
    {
        "name": "VADS Labuan Data Centre",
        "operator": "VADS Berhad",
        "operator_norm": "VADS (TM subsidiary)",
        "address": "Level 2, Ibusawat Telekom Kg. Jawa, 87000 Labuan",
        "city": "Labuan",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/labuan/vads-labuan/",
        ],
        "source_category": "Trade Press",
        "note": "VADS Labuan site inside the Telekom Kg. Jawa exchange. Offshore financial zone support.",
        "physical_facility": True,
    },
    # ─── Tier D: Bridge DC IOI Banting Campus ──────────────────────────
    {
        "name": "Bridge Data Centres IOI Banting Campus",
        "operator": "Bridge Data Centres",
        "operator_norm": "Bridge Data Centres",
        "address": "IOI Industrial Park @ Banting, Selangor",
        "city": "Banting",
        "source_urls": [
            "https://www.ioiproperties.com.my/news/ioi-properties-sells-banting-industrial-land-bridge-data-centres-rm741m",
            "https://www.datacenterdynamics.com/en/news/malaysian-property-developer-ioi-sells-136-acres-of-land-to-singapores-bridge-data-centres/",
            "https://baxtel.com/data-center/bridge-dc-banting-campus",
        ],
        "source_category": "Trade Press",
        "note": "136 acres acquired Jan 2026 for RM741M. Adjacent to 500kV lines. Selangor 'AI-ready' corridor.",
        "physical_facility": True,
    },
    # ─── Tier D: Mah Sing DC Hub ───────────────────────────────────────
    {
        "name": "Mah Sing DC Hub @ Southville City (Bangi)",
        "operator": "Mah Sing / Bridge Data Centres JV",
        "operator_norm": "Bridge Data Centres",
        "address": "Southville City, 43900 Bangi, Selangor",
        "city": "Bangi",
        "source_urls": [
            "https://www.mahsing.com.my/news/mah-sing-launches-mah-sing-dc-hubsouthville-city-with-bridge-data-centres/",
            "https://www.datacenterdynamics.com/en/news/bridge-dc-signs-200mw-data-center-jv-with-malaysias-mah-sing/",
            "https://www.datacentermap.com/malaysia/kuala-lumpur/mah-sing-dc-hub-southville-city/",
        ],
        "source_category": "Trade Press",
        "note": "JV: 150-acre site earmarked, up to 500MW planned. Initial 17.55-acre phase with Bridge DC up to 100MW.",
        "physical_facility": True,
    },
    # ─── Tier D: Google Port Dickson ───────────────────────────────────
    {
        "name": "Google Port Dickson Data Centre (planned)",
        "operator": "Google",
        "operator_norm": "Google",
        "address": "Springhill, Port Dickson, Negeri Sembilan",
        "city": "Port Dickson",
        "source_urls": [
            "https://www.lowyat.net/2025/350820/google-port-dickson-data-centre-project/",
        ],
        "source_category": "Trade Press",
        "note": "Part of Google's US$2bn Malaysia infrastructure investment. Planned site in Springhill.",
        "physical_facility": True,
    },
    # ─── Tier D: Gamuda Springhill DC ──────────────────────────────────
    {
        "name": "Gamuda DC Springhill (Port Dickson)",
        "operator": "Gamuda DC Infrastructure Sdn Bhd",
        "operator_norm": "Gamuda",
        "address": "Springhill, Port Dickson, Negeri Sembilan",
        "city": "Port Dickson",
        "source_urls": [
            "https://www.malaymail.com/news/malaysia/2025/04/28/n-sembilan-to-host-two-data-centres-backed-by-gamuda-us-investors-says-mb/174749",
            "https://gamuda.com/2025/01/negri-sembilan-to-get-158ha-data-centre-campus/news/",
        ],
        "source_category": "Trade Press",
        "note": "158ha data-centre campus. 157.71ha freehold acquired for RM424.4M. AI-focused.",
        "physical_facility": True,
    },
    # ─── Tier D: ZDATA GP3 ─────────────────────────────────────────────
    {
        "name": "ZDATA GP3 Gelang Patah (Computility Technology Sdn Bhd)",
        "operator": "ZDATA Technologies",
        "operator_norm": "ZDATA",
        "address": "Gelang Patah, Johor",
        "city": "Gelang Patah",
        "source_urls": [
            "https://www.digitalnewsasia.com/sustainability-matters/zdata-groups-us2bil-johor-data-centre-first-platinum-certified-green-data",
            "https://www.thestar.com.my/business/business-news/2026/03/11/zdata-developing-rm8bil-greenre-platinum-hyperscale-data-centre-in-johor",
            "https://www.datacentermap.com/malaysia/johor-bahru/computility-technology-johor/",
        ],
        "source_category": "Trade Press",
        "note": "RM8bn hyperscale AI DC. 300MW per datacentermap. First GreenRE Platinum DC in Malaysia. Operational phase-1 Mar 2026.",
        "physical_facility": True,
    },
    # ─── Tier D: Exabytes ──────────────────────────────────────────────
    {
        "name": "Exabytes Penang Suntech Cybercity",
        "operator": "Exabytes",
        "operator_norm": "Exabytes",
        "address": "Suntech @ Penang Cybercity, Lintang Mayang Pasir 3, 11950 Bayan Baru, Penang",
        "city": "Bayan Baru",
        "source_urls": [
            "https://www.exabytes.my/about/datacenter/malaysia",
            "https://www.datacentermap.com/c/exabytes-network-sdn-bhd/",
        ],
        "source_category": "Operator Website",
        "note": "Exabytes Penang HQ + DC. PCI-DSS certified.",
        "physical_facility": True,
    },
    {
        "name": "Exabytes International Data Center (Kuala Lumpur)",
        "operator": "Exabytes",
        "operator_norm": "Exabytes",
        "address": "Level 7, Menara AIMS, Changkat Raja Chulan, 50200 Kuala Lumpur",
        "city": "Kuala Lumpur",
        "source_urls": [
            "https://www.exabytes.my/about/datacenter/malaysia",
        ],
        "source_category": "Operator Website",
        "note": "Co-located inside Menara AIMS (the AIMS operator's KL2 tower).",
        "physical_facility": True,
    },
    {
        "name": "Exabytes CJ1 Data Centre (Cyberjaya)",
        "operator": "Exabytes",
        "operator_norm": "Exabytes",
        "address": "Jalan Cyber Point 4, Cyber 8, 63000 Cyberjaya, Selangor",
        "city": "Cyberjaya",
        "source_urls": [
            "https://www.exabytes.my/about/datacenter/malaysia",
        ],
        "source_category": "Operator Website",
        "note": "Exabytes Cyberjaya facility at Jalan Cyber Point 4.",
        "physical_facility": True,
    },
    # ─── Tier C: Hyperscaler AZ discovery (AWS physical sites) ─────────
    {
        "name": "AWS Cyberjaya Data Centre (ap-southeast-5 AZ)",
        "operator": "Amazon Data Services Malaysia Sdn Bhd",
        "operator_norm": "AWS",
        "address": "Persiaran APEC, Cyberjaya, Selangor",
        "city": "Cyberjaya",
        "source_urls": [
            "https://www.datacenterdynamics.com/en/news/aws-first-malaysian-data-center-set-to-be-in-cyberjaya-report/",
            "https://www.lowyat.net/2024/329760/amazon-data-centre-msia-taking-shape/",
            "https://baxtel.com/data-center/amazon-cyberjaya-1",
        ],
        "source_category": "Hyperscaler AZ Discovery",
        "note": "First physical AWS site in Malaysia per DCD and Lowyat trade-press reconstruction. Part of the RM25.5bn / US$6.2bn ap-southeast-5 region build-out (3 AZs total).",
        "physical_facility": True,
    },
    {
        "name": "AWS KUL Dengkil Site (ap-southeast-5 AZ)",
        "operator": "Amazon Data Services Malaysia Sdn Bhd",
        "operator_norm": "AWS",
        "address": "Dengkil, Sepang, Selangor",
        "city": "Dengkil",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/kuala-lumpur/amazon-aws-kul-dengkil/",
        ],
        "source_category": "Hyperscaler AZ Discovery",
        "note": "Second AZ reported for ap-southeast-5, locality-only via trade directory. Street address not publicly disclosed by AWS. coord_confidence=approximate_locality.",
        "physical_facility": True,
    },
    {
        "name": "AWS KUL Bukit Jalil Site (ap-southeast-5 AZ)",
        "operator": "Amazon Data Services Malaysia Sdn Bhd",
        "operator_norm": "AWS",
        "address": "Technology Park Malaysia, Bukit Jalil, Kuala Lumpur",
        "city": "Bukit Jalil",
        "source_urls": [
            "https://www.datacentermap.com/malaysia/kuala-lumpur/amazon-aws-kul-bukit-jalil/",
        ],
        "source_category": "Hyperscaler AZ Discovery",
        "note": "Third AZ reported for ap-southeast-5. Reported locality (TPM) via trade directory. Street address not publicly disclosed by AWS.",
        "physical_facility": True,
    },
]


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger(__name__)

    v6 = load_v6()
    rows_out: list[dict] = []

    for cand in CANDIDATES:
        geo = geocode_address(cand["address"])
        geo_attempt = "full_address"

        # Fallback 1: strip lot numbers / suite numbers / building names
        if not geo:
            import re as _re
            simplified = _re.sub(
                r"\b(lot|pt|ptd|no\.?|block|unit|suite|level|floor)\s*[-\w/]+\b",
                "", cand["address"], flags=_re.IGNORECASE,
            )
            simplified = _re.sub(r"[,\s]+", " ", simplified).strip(", ")
            if simplified and simplified != cand["address"]:
                geo = geocode_address(simplified)
                geo_attempt = "simplified_address"

        # Fallback 2: city centroid
        if not geo and cand.get("city"):
            geo = geocode_address(cand["city"])
            geo_attempt = "city_centroid"

        lat = geo["lat"] if geo else None
        lon = geo["lon"] if geo else None
        if geo_attempt == "full_address" and geo and geo.get("importance", 0) > 0.5:
            coord_conf = "exact"
        elif geo_attempt == "full_address" and geo:
            coord_conf = "building"
        elif geo_attempt == "simplified_address" and geo:
            coord_conf = "approximate_locality"
        elif geo_attempt == "city_centroid" and geo:
            coord_conf = "approximate_locality"
        else:
            coord_conf = "unknown"
        geo_note = (
            f"geocoded via Nominatim ({geo_attempt}): match={geo['match'][:80]!r} "
            f"importance={geo.get('importance')}"
            if geo else "geocode returned no match on full/simplified/city"
        )

        hit = dedup_check(
            v6,
            candidate_name=cand["name"],
            candidate_operator=cand["operator"],
            candidate_lat=lat,
            candidate_lon=lon,
            candidate_address=cand["address"],
            candidate_city=cand.get("city", ""),
        )

        row = blank_candidate_row()
        row["name"] = cand["name"]
        row["operator"] = cand["operator"]
        row["operator_norm"] = cand["operator_norm"]
        row["lat"] = lat if lat is not None else ""
        row["lon"] = lon if lon is not None else ""
        row["source_category"] = cand["source_category"]
        row["source"] = " | ".join(cand["source_urls"])
        row["address"] = cand["address"]
        row["physical_facility"] = cand.get("physical_facility", True)
        row["coord_confidence"] = coord_conf
        row["n_sources"] = len(cand["source_urls"])
        row["note"] = cand["note"] + " | " + geo_note
        row["promotion_action"] = hit.status
        row["promotion_note"] = hit.reason
        row["facility_type"] = "physical_facility"
        row["status"] = "Operational"  # default; some are planned
        if "planned" in cand["note"].lower() or "mou" in cand["note"].lower():
            row["status"] = "Planned"
        rows_out.append(row)
        log.info(
            "%s → %s (%.4f, %.4f) [%s]",
            cand["name"][:50], hit.status[:40], lat or 0.0, lon or 0.0, coord_conf,
        )

    out_path = HERE / "outputs" / "v6_gapfill_candidates.csv"
    out_path.parent.mkdir(exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=V6_SCHEMA)
        writer.writeheader()
        for r in rows_out:
            writer.writerow({c: r.get(c, "") for c in V6_SCHEMA})
    log.info("Wrote %s (%d rows)", out_path, len(rows_out))
    # Status summary
    from collections import Counter
    status_counts = Counter(r["promotion_action"].split(":")[0] for r in rows_out)
    log.info("Action breakdown: %s", dict(status_counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
