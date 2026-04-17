"""
Wikipedia / DBpedia expansion pipeline for Malaysian data center dataset.

Purpose
-------
Close the coverage gap in malaysia_datacenters_v5.csv by adding a Wikipedia-
based discovery layer. This source is:
  - Openly licensed (CC BY-SA for text, CC0 for structured Wikidata metadata)
  - Queryable via a stable public API (no ToS issues like datacentermap.com)
  - Country-agnostic — the same pipeline works for the next country pilot

Strategy
--------
1. Seed with a curated list of operator + topical article titles.
2. For each seed, fetch the Wikipedia article's wikitext and the Wikidata
   Q-ID via the MediaWiki API.
3. Parse wikitext with mwparserfromhell to extract:
     - Coordinates from {{Coord}} templates in the article
     - Wikilinks to candidate facility articles (1-hop expansion)
     - Infobox fields (operator, inception_year, etc.)
4. For each linked article that passes a DC-relevance filter, resolve its
   Wikidata Q-ID and fetch structured properties via SPARQL:
     - P625 (coordinates), P137 (operator), P17 (country),
       P571 (inception), P31 (instance-of — filter for Q1149652/subclasses)
5. Run spaCy NER + pattern matching on prose to surface facility mentions
   that don't have their own Wikipedia article (the most common case for
   Malaysian facilities).
6. Emit rows matching the v5 CSV schema with source_category='Wikipedia'
   and deduplicate against v5 by (normalized_name, proximity<300m).

How to run (Positron)
---------------------
    pip install requests mwparserfromhell spacy pandas
    python -m spacy download en_core_web_sm
    python wikipedia_scrape.py \
        --v5-csv outputs/malaysia_datacenters_v5.csv \
        --out-csv outputs/malaysia_wikipedia_candidates.csv \
        --country-qid Q833 \
        --user-agent "ITEC724-DatacenterPilot/1.0 (daniel@american.edu)"

Output
------
A CSV of candidate facilities with provenance for each row. Designed to be
*reviewed* before merging — Wikipedia's Malaysia coverage is thin enough
that you'll want human eyes on new rows. Expected yield on this pilot:
5–15 new facilities plus enriched metadata (inception year, operator
canonicalization) for ~20 existing v5 rows.

Notes
-----
- Rate limiting: MediaWiki API tolerates ~200 req/min with a descriptive
  user-agent, but this script sleeps 0.3s between calls to be a good
  citizen. Total runtime ~2-3 minutes.
- Caching: responses are cached to ./cache/ so re-runs are free. Delete
  the cache dir to force refresh.
- Replicability: to run for Singapore, change --country-qid to Q334 and
  update SEED_TITLES. Everything else is country-agnostic.
"""

from __future__ import annotations
import argparse
import json
import re
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import requests
import pandas as pd
import mwparserfromhell as mwp


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MW_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData"

# Wikidata Q-IDs for "data center" and its subclasses. The subclass tree is
# traversed via wdt:P279* in the SPARQL query.
DATA_CENTER_QID = "Q1149652"

# Seed list — topical anchors + known operators. For a new country, rebuild
# this from: (a) the country's Wikipedia article linked via "Data centers in X",
# (b) any operator you know is active there, (c) the relevant tech-hub city
# articles (Cyberjaya is Malaysia's analog of Loudoun County).
SEED_TITLES = [
    # Topical anchor pages — these articles mention many facilities in prose
    "Cyberjaya",
    "Iskandar Puteri",
    "Kulai",
    "Sedenak",
    "Johor Bahru",
    "Shah Alam",
    "Bukit Jalil",
    "Bayan Lepas",
    "Multimedia Super Corridor",

    # Operator articles — infoboxes list subsidiaries, prose lists sites
    "YTL Corporation",
    "YTL Power International",
    "Telekom Malaysia",
    "Equinix",
    "Digital Realty",
    "NTT Ltd.",
    "Keppel Corporation",
    "AirTrunk",
    "Princeton Digital Group",
    "Vantage Data Centers",
    "Microsoft Azure",
    "Amazon Web Services",
    "Google Cloud Platform",
    "Oracle Cloud",
    "Alibaba Cloud",
    "ByteDance",
    "TikTok",

    # List / category pages that sometimes enumerate facilities
    "List of Internet exchange points in Asia",
]

# Terms whose presence in an article's wikitext mean we should treat it as a
# DC-candidate source document (even if not a DC article itself).
DC_RELEVANCE_TERMS = [
    "data center", "data centre", "datacenter", "datacentre",
    "colocation", "hyperscale", "cloud region", "availability zone",
]

# Malaysian place-name tokens used to confirm a mentioned facility is in MY.
MY_PLACE_TOKENS = {
    "malaysia", "johor", "selangor", "kuala lumpur", "cyberjaya",
    "iskandar", "kulai", "sedenak", "johor bahru", "shah alam",
    "subang", "penang", "bayan lepas", "melaka", "putrajaya",
    "nusajaya", "gelang patah", "pasir gudang", "kedah",
}


# ---------------------------------------------------------------------------
# Caching HTTP client
# ---------------------------------------------------------------------------

class CachedClient:
    """Thin wrapper over requests with disk cache + polite rate limit."""

    def __init__(self, user_agent: str, cache_dir: Path, sleep: float = 0.3):
        self.headers = {"User-Agent": user_agent}
        self.cache = cache_dir
        self.cache.mkdir(exist_ok=True, parents=True)
        self.sleep = sleep

    def get(self, url: str, params: dict, cache_key: str) -> dict:
        cf = self.cache / f"{cache_key}.json"
        if cf.exists():
            return json.loads(cf.read_text())
        params = {**params, "format": "json"}
        r = requests.get(url, params=params, headers=self.headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        cf.write_text(json.dumps(data))
        time.sleep(self.sleep)
        return data


# ---------------------------------------------------------------------------
# MediaWiki helpers
# ---------------------------------------------------------------------------

def mw_pageprops(client: CachedClient, titles: list[str]) -> dict[str, dict]:
    """Return {title: {exists, pageid, wikidata_id, url}} for each title.

    pageprops.wikibase_item is the Wikidata Q-ID — this is the cheapest way
    to cross-reference a Wikipedia article with Wikidata.
    """
    out: dict[str, dict] = {}
    for i in range(0, len(titles), 50):
        batch = titles[i:i + 50]
        data = client.get(MW_API, {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "pageprops|info",
            "inprop": "url",
            "formatversion": 2,
        }, cache_key=f"pageprops_{i}_{hash(tuple(batch)) & 0xffff:04x}")
        for p in data.get("query", {}).get("pages", []):
            title = p.get("title")
            if p.get("missing"):
                out[title] = {"exists": False}
                continue
            out[title] = {
                "exists": True,
                "pageid": p.get("pageid"),
                "wikidata_id": p.get("pageprops", {}).get("wikibase_item"),
                "url": p.get("fullurl"),
            }
    return out


def mw_wikitext(client: CachedClient, titles: list[str]) -> dict[str, str]:
    """Return {title: wikitext} for each existing title."""
    out: dict[str, str] = {}
    for i in range(0, len(titles), 20):
        batch = titles[i:i + 20]
        data = client.get(MW_API, {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
            "formatversion": 2,
        }, cache_key=f"wikitext_{i}_{hash(tuple(batch)) & 0xffff:04x}")
        for p in data.get("query", {}).get("pages", []):
            if p.get("missing"):
                continue
            revs = p.get("revisions", [])
            if revs:
                out[p["title"]] = revs[0]["slots"]["main"]["content"]
    return out


# ---------------------------------------------------------------------------
# Wikitext parsing
# ---------------------------------------------------------------------------

COORD_RE = re.compile(
    r"\{\{Coord\s*\|\s*([\d\.\-]+)\s*\|\s*([\d\.\-]+)", re.IGNORECASE
)
COORD_DMS_RE = re.compile(
    r"\{\{Coord\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d\.]+)?\s*\|\s*([NS])"
    r"\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d\.]+)?\s*\|\s*([EW])",
    re.IGNORECASE,
)


def parse_coords_from_wikitext(text: str) -> tuple[float, float] | None:
    """Extract the first {{Coord}} template as (lat, lon) or None.

    Handles both decimal form ({{Coord|3.14|101.69|...}}) and DMS form
    ({{Coord|2|55|30|N|101|39|45|E}}).
    """
    m = COORD_DMS_RE.search(text)
    if m:
        lat_d, lat_m, lat_s, lat_h, lon_d, lon_m, lon_s, lon_h = m.groups()
        lat = int(lat_d) + int(lat_m) / 60 + float(lat_s or 0) / 3600
        lon = int(lon_d) + int(lon_m) / 60 + float(lon_s or 0) / 3600
        if lat_h.upper() == "S":
            lat = -lat
        if lon_h.upper() == "W":
            lon = -lon
        return (round(lat, 6), round(lon, 6))
    m = COORD_RE.search(text)
    if m:
        try:
            return (float(m.group(1)), float(m.group(2)))
        except ValueError:
            return None
    return None


def extract_wikilinks(wikitext: str) -> list[str]:
    """Return unique wikilink target titles from an article."""
    parsed = mwp.parse(wikitext)
    titles = set()
    for link in parsed.filter_wikilinks():
        target = str(link.title).strip()
        # Skip files, categories, interwiki, section-only links
        if ":" in target and target.split(":", 1)[0].lower() in {
            "file", "image", "category", "wikt", "wikipedia",
        }:
            continue
        if target.startswith("#"):
            continue
        titles.add(target.split("#")[0])  # strip section anchors
    return sorted(titles)


def is_dc_relevant(wikitext: str) -> bool:
    """Does this article mention data center infrastructure?"""
    lower = wikitext[:50_000].lower()  # cap for speed; lead section is enough
    return any(term in lower for term in DC_RELEVANCE_TERMS)


def mentions_malaysia(wikitext: str) -> bool:
    """Does this article mention a Malaysian place?"""
    lower = wikitext[:50_000].lower()
    return any(tok in lower for tok in MY_PLACE_TOKENS)


# ---------------------------------------------------------------------------
# Wikidata SPARQL
# ---------------------------------------------------------------------------

SPARQL_DC_IN_COUNTRY = """
SELECT ?item ?itemLabel ?coords ?operator ?operatorLabel ?inception WHERE {{
  ?item wdt:P31/wdt:P279* wd:{dc_qid} .
  ?item wdt:P17 wd:{country_qid} .
  OPTIONAL {{ ?item wdt:P625 ?coords . }}
  OPTIONAL {{ ?item wdt:P137 ?operator . }}
  OPTIONAL {{ ?item wdt:P571 ?inception . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def query_wikidata_dcs(client: CachedClient, country_qid: str) -> pd.DataFrame:
    """Pull every Wikidata item that is an instance-of/subclass-of data center
    AND located in the given country."""
    query = SPARQL_DC_IN_COUNTRY.format(
        dc_qid=DATA_CENTER_QID, country_qid=country_qid,
    )
    cf = client.cache / f"wikidata_dc_{country_qid}.json"
    if cf.exists():
        data = json.loads(cf.read_text())
    else:
        r = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers={**client.headers, "Accept": "application/sparql-results+json"},
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        cf.write_text(json.dumps(data))

    rows = []
    for b in data["results"]["bindings"]:
        coords_wkt = b.get("coords", {}).get("value", "")
        m = re.match(r"Point\(([-\d\.]+)\s+([-\d\.]+)\)", coords_wkt)
        lon, lat = (float(m.group(1)), float(m.group(2))) if m else (None, None)
        rows.append({
            "wikidata_id": b["item"]["value"].rsplit("/", 1)[-1],
            "name": b.get("itemLabel", {}).get("value"),
            "operator": b.get("operatorLabel", {}).get("value"),
            "lat": lat,
            "lon": lon,
            "inception": b.get("inception", {}).get("value"),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Prose-level facility extraction
# ---------------------------------------------------------------------------
# Many Malaysian facilities don't have their own Wikipedia article but ARE
# named in the Cyberjaya / operator articles. We catch those with patterns.

FACILITY_PATTERNS = [
    # "Equinix JH1", "NEXTDC KL1" — single-token operator + short tag like JH1.
    # Lookbehind stops us matching mid-sentence text like "operates Equinix JH1".
    re.compile(
        r"(?<![A-Za-z])"
        r"(?P<operator>[A-Z][A-Za-z&\.]{1,15})"
        r"\s+"
        r"(?P<n>[A-Z]{2,4}\d{1,3}[A-Z]?)"
        r"\b"
    ),
    # "YTL Johor Data Center 1" — explicit "Data Center N" phrasing.
    re.compile(
        r"(?<![A-Za-z])"
        r"(?P<operator>[A-Z][A-Za-z&\.]{1,15}(?:\s+[A-Z][A-Za-z]+){0,3})"
        r"\s+Data\s+Cent(?:er|re)\s+(?P<n>\d+)"
    ),
    # "... the JHB1 data center ..." — standalone tag + DC keyword.
    re.compile(
        r"(?<![A-Za-z])"
        r"(?P<n>[A-Z]{2,5}\d{1,2}[A-Z]?)"
        r"\s+(?:data\s+cent(?:er|re)|facility|campus)\b",
        re.IGNORECASE,
    ),
]


def extract_prose_facilities(article_title: str, wikitext: str) -> list[dict]:
    """Pattern-match facility mentions in article prose. Low precision, high
    recall — output is CANDIDATES that a human reviews before promoting.
    """
    # Strip wikitext markup to cleaner text for pattern matching
    try:
        text = mwp.parse(wikitext).strip_code()
    except Exception:
        text = wikitext

    found = []
    for pat in FACILITY_PATTERNS:
        for m in pat.finditer(text):
            # Require a Malaysia token within 200 chars for geographic scoping
            window = text[max(0, m.start() - 200):m.end() + 200].lower()
            if not any(tok in window for tok in MY_PLACE_TOKENS):
                continue
            found.append({
                "source_article": article_title,
                "matched_text": m.group(0),
                "context_snippet": text[max(0, m.start() - 100):m.end() + 100],
                **m.groupdict(),
            })
    # Dedup by matched_text within article
    seen = set()
    unique = []
    for f in found:
        key = f["matched_text"].lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(f)
    return unique


# ---------------------------------------------------------------------------
# v5 schema alignment + dedup
# ---------------------------------------------------------------------------

V5_COLUMNS = [
    "name", "operator", "lat", "lon", "sources", "osm_id", "wikidata_id",
    "provider", "region_code", "facility_type", "n_sources", "ADMIN",
    "ISO_A3", "CONTINENT", "SUBREGION", "name_normalized", "has_operator",
    "in_both_sources", "source_category", "operator_norm", "status", "year",
    "coord_confidence", "cluster_id", "address", "source", "note", "osm_type",
]


def to_v5_row(name: str, operator: str | None, lat: float | None,
              lon: float | None, wikidata_id: str | None,
              source_article: str, note: str = "",
              inception: str | None = None) -> dict:
    """Build a single row aligned to the v5 CSV schema."""
    year = None
    if inception:
        m = re.match(r"(\d{4})", inception)
        if m:
            year = int(m.group(1))
    return {
        "name": name,
        "operator": operator,
        "lat": lat,
        "lon": lon,
        "sources": f"Wikipedia:{source_article}",
        "osm_id": None,
        "wikidata_id": wikidata_id,
        "provider": None,
        "region_code": None,
        "facility_type": "physical_facility",
        "n_sources": 1,
        "ADMIN": "Malaysia",
        "ISO_A3": "MYS",
        "CONTINENT": "Asia",
        "SUBREGION": "South-Eastern Asia",
        "name_normalized": name,
        "has_operator": operator is not None,
        "in_both_sources": False,
        "source_category": "Wikipedia",
        "operator_norm": operator,
        "status": "Unknown",
        "year": year,
        "coord_confidence": "wikidata" if wikidata_id else "article_coord",
        "cluster_id": None,
        "address": None,
        "source": "wikipedia_pipeline",
        "note": note,
        "osm_type": None,
    }


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    from math import radians, sin, cos, asin, sqrt
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    return 2 * R * asin(sqrt(a))


def dedup_against_v5(new_rows: list[dict], v5: pd.DataFrame,
                     distance_km: float = 0.3) -> pd.DataFrame:
    """Flag candidates that match an existing v5 row by (a) Wikidata Q-ID,
    (b) normalized-name exact match, or (c) <300m spatial proximity.

    Does NOT drop matches — it attaches a `merge_status` column so the
    human reviewer can decide.
    """
    df = pd.DataFrame(new_rows, columns=V5_COLUMNS)
    if df.empty:
        return df
    df["merge_status"] = "new"
    df["matched_v5_name"] = None

    v5_valid = v5.dropna(subset=["lat", "lon"]).copy()

    for i, row in df.iterrows():
        # Wikidata match
        if row["wikidata_id"]:
            hit = v5[v5["wikidata_id"].astype(str) == str(row["wikidata_id"])]
            if not hit.empty:
                df.at[i, "merge_status"] = "duplicate_wikidata"
                df.at[i, "matched_v5_name"] = hit.iloc[0]["name"]
                continue
        # Name match (case-insensitive)
        name_lower = str(row["name"]).lower()
        name_hit = v5[v5["name"].astype(str).str.lower() == name_lower]
        if not name_hit.empty:
            df.at[i, "merge_status"] = "duplicate_name"
            df.at[i, "matched_v5_name"] = name_hit.iloc[0]["name"]
            continue
        # Spatial proximity
        if pd.notna(row["lat"]) and pd.notna(row["lon"]):
            for _, v5row in v5_valid.iterrows():
                if haversine_km(row["lat"], row["lon"],
                                v5row["lat"], v5row["lon"]) < distance_km:
                    df.at[i, "merge_status"] = "proximity_match"
                    df.at[i, "matched_v5_name"] = v5row["name"]
                    break
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v5-csv", required=True, help="Path to malaysia_datacenters_v5.csv")
    ap.add_argument("--out-csv", required=True, help="Path to write candidates")
    ap.add_argument("--country-qid", default="Q833", help="Wikidata Q-ID of country (Q833=Malaysia)")
    ap.add_argument("--user-agent", required=True,
                    help="Required by MediaWiki ToS. Format: 'App/1.0 (email)'")
    ap.add_argument("--cache-dir", default="./cache")
    args = ap.parse_args()

    client = CachedClient(args.user_agent, Path(args.cache_dir))
    v5 = pd.read_csv(args.v5_csv)
    print(f"Loaded v5: {len(v5)} rows")

    # Step 1: verify seed titles exist, pull Wikidata IDs
    print("\n=== Step 1: Resolve seed titles ===")
    props = mw_pageprops(client, SEED_TITLES)
    ok_titles = [t for t, v in props.items() if v.get("exists")]
    print(f"Seed: {len(ok_titles)}/{len(SEED_TITLES)} titles exist")

    # Step 2: fetch wikitext
    print("\n=== Step 2: Fetch wikitext ===")
    wikitexts = mw_wikitext(client, ok_titles)
    print(f"Fetched {len(wikitexts)} articles")

    # Step 3: 1-hop link expansion — any wikilinked article whose title
    # contains a DC keyword OR an operator-like pattern
    print("\n=== Step 3: Expand wikilinks ===")
    dc_pat = re.compile(
        r"data\s*cent(?:er|re)|hyperscale|colocation", re.IGNORECASE,
    )
    candidate_links = set()
    for title, text in wikitexts.items():
        for link in extract_wikilinks(text):
            if dc_pat.search(link):
                candidate_links.add(link)
    print(f"Candidate linked articles (DC keyword in title): {len(candidate_links)}")

    link_props = mw_pageprops(client, sorted(candidate_links))
    link_ok = [t for t, v in link_props.items() if v.get("exists")]
    link_texts = mw_wikitext(client, link_ok)
    # Filter to Malaysia-relevant
    malaysia_linked = {
        t: x for t, x in link_texts.items() if mentions_malaysia(x)
    }
    print(f"Of those, {len(malaysia_linked)} mention Malaysia")

    # Step 4: Wikidata SPARQL for structured DCs in country
    print("\n=== Step 4: Wikidata SPARQL ===")
    wd = query_wikidata_dcs(client, args.country_qid)
    print(f"Wikidata structured data centers in {args.country_qid}: {len(wd)}")

    # Step 5: build rows
    print("\n=== Step 5: Build candidate rows ===")
    rows: list[dict] = []

    # 5a — structured Wikidata hits (highest confidence)
    for _, r in wd.iterrows():
        rows.append(to_v5_row(
            name=r["name"], operator=r.get("operator"),
            lat=r.get("lat"), lon=r.get("lon"),
            wikidata_id=r["wikidata_id"],
            source_article="Wikidata SPARQL",
            note="structured_wikidata",
            inception=r.get("inception"),
        ))

    # 5b — Wikipedia article has its own coords + DC relevance
    all_article_texts = {**wikitexts, **malaysia_linked}
    for title, text in all_article_texts.items():
        if not is_dc_relevant(text):
            continue
        coords = parse_coords_from_wikitext(text)
        if coords is None:
            continue
        wd_id = (props.get(title) or link_props.get(title) or {}).get("wikidata_id")
        # Skip if we already covered it in 5a
        if wd_id and any(r["wikidata_id"] == wd_id for r in rows):
            continue
        rows.append(to_v5_row(
            name=title, operator=None,
            lat=coords[0], lon=coords[1],
            wikidata_id=wd_id,
            source_article=title,
            note="article_coord_template",
        ))

    # 5c — prose-mentioned facilities (lowest confidence, needs review)
    prose_candidates = []
    for title, text in all_article_texts.items():
        prose_candidates.extend(extract_prose_facilities(title, text))
    print(f"Prose-extracted facility mentions: {len(prose_candidates)}")
    # Emit these WITHOUT coords — human review fills them in
    for pc in prose_candidates:
        rows.append(to_v5_row(
            name=pc["matched_text"], operator=pc.get("operator"),
            lat=None, lon=None, wikidata_id=None,
            source_article=pc["source_article"],
            note=f"prose_pattern|context={pc['context_snippet'][:200]}",
        ))

    # Step 6: dedup + write
    print(f"\n=== Step 6: Dedup against v5 ({len(rows)} raw candidates) ===")
    final = dedup_against_v5(rows, v5)
    print(final["merge_status"].value_counts().to_string())

    Path(args.out_csv).parent.mkdir(exist_ok=True, parents=True)
    final.to_csv(args.out_csv, index=False)
    print(f"\n✓ Wrote {len(final)} rows to {args.out_csv}")
    print("\nNext step: open the CSV, filter merge_status='new', and manually")
    print("verify before promoting into the v5 pipeline.")


if __name__ == "__main__":
    main()