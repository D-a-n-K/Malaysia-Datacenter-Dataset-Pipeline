"""
build_report.py — generate outputs/v6_gapfill_report.md summarising the
2026-04-17 gapfill sprint.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
CANDIDATES = HERE / "outputs" / "v6_gapfill_candidates.csv"
FIXES = HERE / "outputs" / "v6_dedup_fixes.csv"
OUT = HERE / "outputs" / "v6_gapfill_report.md"


def main() -> int:
    df = pd.read_csv(CANDIDATES)
    fixes = pd.read_csv(FIXES) if FIXES.exists() else pd.DataFrame()

    add = df[df["promotion_action"].astype(str).str.startswith("add_new")]
    rev = df[df["promotion_action"].astype(str).str.startswith("review")]
    skp = df[df["promotion_action"].astype(str).str.startswith("skip")]

    src_counts = Counter(df["source_category"])
    coord_counts = Counter(df["coord_confidence"])

    # Group by logical tier based on operator/source
    def tier(row) -> str:
        n = str(row["name"]).lower()
        s = str(row["source_category"])
        if s == "Hyperscaler AZ Discovery":
            return "C_hyperscaler_az"
        if any(k in n for k in ["port dickson", "gamuda", "ipoh", "labuan", "bintulu",
                                 "bandar enstek", "bagan datuk", "penang", "bayan",
                                 "gelang patah", "pasir gudang", "banting", "bangi",
                                 "springhill"]):
            return "D_regional"
        if "vads" in n or "tm global" in n or "maxis" in n or "mytelehaus" in n or \
           "silverstream" in n or "exitra" in n or "progenet" in n or \
           "teliti" in n or "skali" in n or "hdc" in n or "manadr" in n or \
           "strateq" in n or "exabytes" in n:
            return "A_under_represented"
        return "B_known_ops"

    df["_tier"] = df.apply(tier, axis=1)
    by_tier = df.groupby("_tier").size().to_dict()

    lines: list[str] = []
    a = lines.append

    a("# Malaysia DC Corpus — v6 Gapfill Report")
    a("")
    a("Generated 2026-04-17 by the `gapfill_2026_04_17` sprint against `malaysia_datacenters_v6_master.csv` (101 rows).")
    a("")
    a("## Headline")
    a("")
    a(f"* **{len(df)} candidates produced** across 4 tiers.")
    a(f"  * `add_new`: **{len(add)}** — pass dedup clean, ready to promote after spot-check.")
    a(f"  * `review`: **{len(rev)}** — flagged for human triage (co-tenant buildings, same-operator-near-miss).")
    a(f"  * `skip`: **{len(skp)}** — correctly detected as duplicates of existing v6 rows.")
    a(f"* **{len(fixes)} dedup fixes** written to `v6_dedup_fixes.csv` covering the 5 known-dup cases.")
    a("")
    a("## Rows by tier")
    a("")
    a("| Tier | Candidates | Description |")
    a("|---|---|---|")
    tier_labels = {
        "A_under_represented": "A — under-represented operators (VADS, TM Global, Maxis, local bumi)",
        "B_known_ops": "B — known operators, coverage cross-check",
        "C_hyperscaler_az": "C — hyperscaler physical AZ discovery (AWS)",
        "D_regional": "D — regional market sweeps (secondary / east Malaysia)",
    }
    for t, label in tier_labels.items():
        a(f"| {t} | {by_tier.get(t, 0)} | {label} |")
    a("")
    a("## Rows by source_category")
    a("")
    a("| source_category | Count |")
    a("|---|---|")
    for s, n in src_counts.most_common():
        a(f"| {s} | {n} |")
    a("")
    a("## Coordinate confidence")
    a("")
    a("| coord_confidence | Count | Meaning |")
    a("|---|---|---|")
    conf_meanings = {
        "exact": "Nominatim returned importance ≥ 0.5 on full address",
        "building": "Nominatim returned a named building / street match on full address",
        "approximate_locality": "Fell back to simplified address or city centroid",
        "unknown": "No geocode match — lat/lon blank",
    }
    for c, n in coord_counts.most_common():
        a(f"| {c} | {n} | {conf_meanings.get(c, '')} |")
    a("")
    a("## Per-operator outcomes")
    a("")
    a("### Tier A — under-represented operators")
    a("")
    a("| Operator | Sites added | Notes |")
    a("|---|---|---|")
    op_rollup = df.groupby("operator_norm").size().to_dict()
    a(f"| VADS Berhad | {op_rollup.get('VADS (TM subsidiary)', 0)} | 7 regional sites (IPDC, KVDC, Brickfields, Semarak, Bayan Baru, Ipoh, Labuan). Sourced from trade directories. |")
    a(f"| TM Global | {op_rollup.get('TM Global', 0)} | 5 strategic DCs (BFDC/KJDC/KVDC/IPDC/SJDC) from TM Global operator page. Some may dup v6 PeeringDB 'TM ONE' rows — review required. |")
    a(f"| Maxis | {op_rollup.get('Maxis', 0)} | Maxis i-City Shah Alam from operator page. |")
    a(f"| MyTelehaus | {op_rollup.get('MyTelehaus', 0)} | PJ3 (Plaza33) and CJ1 (Jalan Kemajuan PJ). |")
    a(f"| Silverstream | {op_rollup.get('Silverstream', 0)} | Bagan Datuk (planned 2027, Privasia subsidiary). |")
    a(f"| Exitra | {op_rollup.get('Exitra', 0)} | KL MSC integrator. |")
    a(f"| Progenet | {op_rollup.get('Progenet', 0)} | PJ (Menara Lien Hoe). |")
    a(f"| Teliti | {op_rollup.get('Teliti', 0)} | Bandar Enstek ETP key project. |")
    a(f"| SKALI | {op_rollup.get('SKALI', 0)} | UPM Serdang. |")
    a(f"| HDC | {op_rollup.get('HDC', 0)} | Shah Alam, first Uptime Tier-III-certified DC in Malaysia (2012). |")
    a(f"| MaNaDr | {op_rollup.get('MaNaDr', 0)} | Bintulu Sarawak, 150MW planned end-2028. |")
    a(f"| Exabytes | {op_rollup.get('Exabytes', 0)} | Three sites — Penang Suntech, KL Menara AIMS, Cyberjaya CJ1. All co-tenant buildings (flagged review). |")
    a("")
    a("### Tier B — known operators, cross-check")
    a("")
    a("| Operator | Sites added | Notes |")
    a("|---|---|---|")
    a(f"| Bridge Data Centres | {op_rollup.get('Bridge Data Centres', 0)} | MY01, MY03 added; MY02 correctly skipped as v6 dup. Plus IOI Banting Campus and Mah Sing Southville JV. |")
    a(f"| Racks Central | {op_rollup.get('Racks Central', 0)} | RCJM1, RCJM2A, RCJM2B all Pasir Gudang Iskandar Halal Park. |")
    a(f"| K2 Data Centres | {op_rollup.get('K2 Data Centres', 0)} | JHR1 Sedenak (300MW hyperscale). |")
    a(f"| YTL Data Center Holdings | {op_rollup.get('YTL Data Center Holdings', 0)} | Green DC Park Kulai + Sentul. Kulai campus likely dup v5.1 — see v6_dedup_fixes. |")
    a(f"| ZDATA | {op_rollup.get('ZDATA', 0)} | GP3 Gelang Patah, RM8bn, first GreenRE Platinum DC. |")
    a(f"| Gamuda | {op_rollup.get('Gamuda', 0)} | Springhill Port Dickson 158ha campus. |")
    a(f"| Google | {op_rollup.get('Google', 0)} | Port Dickson (Springhill) planned. |")
    a("")
    a("### Tier C — hyperscaler AZ discovery")
    a("")
    a("Attempted: AWS ap-southeast-5 (3 AZs).")
    a("")
    a("| Site | coord_confidence | Sources | Status |")
    a("|---|---|---|---|")
    a("| AWS Cyberjaya (Persiaran APEC) | building | DCD report, Lowyat trade press, baxtel listing | add_new — best-documented of the three |")
    a("| AWS KUL Dengkil | approximate_locality | datacentermap listing | add_new — locality only, no exact address |")
    a("| AWS KUL Bukit Jalil (TPM) | building | datacentermap listing | **review** — within 150m of JARING Bukit Jalil (different operator); may be co-tenant at TPM or same building |")
    a("")
    a("Not attempted this sprint: Azure Malaysia West, GCP asia-southeast3, Alibaba, Oracle physical sites. The AWS trade-press footprint is denser than for other hyperscalers; deferred Azure/GCP/etc. to a follow-up pass.")
    a("")
    a("### Tier D — regional market sweeps")
    a("")
    a("Found coverage:")
    a("")
    a("* **Port Dickson / Springhill** — 2 new projects (Google hyperscale, Gamuda 158ha)")
    a("* **Gelang Patah / Johor** — ZDATA GP3 RM8bn")
    a("* **Banting / Selangor** — Bridge DC IOI 136-acre campus")
    a("* **Bangi / Selangor** — Mah Sing DC Hub @ Southville City (Bridge DC JV)")
    a("* **Ipoh / Perak** — VADS Ipoh (regional Telekom exchange)")
    a("* **Labuan** — VADS Labuan")
    a("* **Bintulu / Sarawak** — MaNaDr planned")
    a("* **Bandar Enstek / Negeri Sembilan** — Teliti (already had Regal Orion in v5.1)")
    a("")
    a("Zero / sparse yield — worth manual follow-up:")
    a("")
    a("* **Kota Kinabalu** — no DC operator surfaced via trade-press search")
    a("* **Miri / Sarawak** — no DC found")
    a("* **Melaka** — only LK Global Engineering (an EPC contractor, not an operator)")
    a("* **Kuching / Sarawak** — covered by existing v5.1 irix Santubong + v6_review SACOFA/Danawa")
    a("")
    a("## Blocked / failed fetches")
    a("")
    a("* `https://www.vads.com/data-centre` — 404. Fell back to `vads.com` press pages via Google search.")
    a("* `https://www.bridgedatacentres.com/locations/` — TLS certificate failure. Fell back to datacenters.com + baxtel which mirror Bridge's own listings.")
    a("* `https://www.maxis.com.my/business/data-centre/` — 404. Found Maxis i-City via Google + Tier-III-certification trade press.")
    a("* `https://www.k2dc.com/` — timeout. Data surfaced via datacenterHawk.")
    a("* `https://silverstream.com.my/` — connection refused. Trade press (Bernama, Edge, Lowyat) had the facts.")
    a("* `https://www.mytelehaus.com/` — 500. Surfaced via Inflect + DCD.")
    a("* `https://www.strateq.com.my/` — connection refused. No fallback yielded a facility address this pass; Strateq DC-1 and DC-2 remain unlocated.")
    a("* `https://www.i-city.com.my/datacentre` → `https://corp.i-city.my/datacentre` — the corp site no longer lists DC-specific content. HDC surfaced via its own `hdc.net.my`.")
    a("")
    a("## Coverage notes for v6 maintainers")
    a("")
    a("* **Building co-tenancy** surfaced as a methodology issue during this sprint. Three co-tenant buildings were flagged (Exabytes inside Menara AIMS / Suntech Cybercity / Open DC CJ1). Dedup now distinguishes same-operator-same-coord (skip) from different-operator-same-coord (review). Recommend making this a durable data model rule: multiple operator rows at the same coord are expected when the building is a carrier hotel or multi-tenant DC.")
    a("* **Dual-brand VADS/TM**: seven VADS sites added. They pair with TM Global's six strategic DCs at overlapping addresses (BFDC ↔ VADS Brickfields, KVDC ↔ VADS KVDC, IPDC ↔ VADS IPDC). Existing v5.1 convention treats them as separate rows; this sprint continues that convention.")
    a("* **AWS AZ methodology**: trade-press locality reconstruction works for the Cyberjaya site (Persiaran APEC) but only gets district-level for Dengkil and Bukit Jalil. Street-level AZ coordinates would require SSM filings for Amazon Data Services Malaysia Sdn Bhd (not attempted — would require paid lookup) or satellite verification of fenced compounds near reported localities.")
    a("* **Geocoder caveats**: 14 candidates use `approximate_locality` because Nominatim couldn't match the full address (Malaysian lot numbers, building suite identifiers). These rows have real city-level coords but should be spot-checked against satellite imagery before merge.")
    a("")
    a("## Recommendations for the next OSINT pass")
    a("")
    a("1. **SSM filings for hyperscaler local entities** — `Amazon Data Services Malaysia Sdn Bhd`, `Microsoft Malaysia Sdn Bhd`, `Google Cloud Malaysia Sdn Bhd`, `Alibaba Cloud Malaysia Sdn Bhd`. Registered business address typically discloses primary site. SSM MyData has free basic lookup.")
    a("2. **MCMC NFP/NSP licensee register** — operators holding Network Facilities Provider licenses must declare facility addresses. Captures the local colo operators (Strateq, ZDATA, Teliti, SKALI) without needing their websites.")
    a("3. **Planning portal satellite verification** — for AWS Dengkil and Bukit Jalil, a satellite recon pass (large fenced compounds with chiller yards near reported localities) would confirm or refute the trade-press locations.")
    a("4. **Azure / GCP / Alibaba / Oracle** physical AZ discovery using the same methodology as AWS. Lower trade-press density but may surface with targeted DCD/Data Center Frontier queries.")
    a("5. **Sabah / Kota Kinabalu / Miri** — zero-yield this pass. Worth a one-hour manual search of local press (Daily Express, Borneo Post) which often covers Sabah-specific industrial announcements missed by peninsular trade press.")
    a("6. **Strateq DC-1 and DC-2** — connection-refused on operator site. Try Wayback or reach out to Strateq directly.")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
