"""
st_scraper.py — Task 5 of the 2026-04-17 expansion sprint.

Scrape the Energy Commission (Suruhanjaya Tenaga) public distribution
licensee register. The table lives at:

    https://www.st.gov.my/stakeholders/electricity/
    list-of-registered-electricity-industry-professionals-and-licensees/
    list-public-distribution-licensee

with pagination via ?page=0..N. Each row has: Company Name, Address,
Start Date, End Date, Installation Capacity (MW).

Filter
------
Retain rows where:
  (a) company name matches a v5 operator OR contains a DC-suggestive keyword
      (data, digital, cloud, hyperscale, cyber, centre/center), AND
  (b) Installation Capacity ≥ 5 MW (the clustering threshold mentioned in the
      sprint brief), AND
  (c) Address resolves to one of the Malaysian DC clusters in
      MY_LOCATION_CENTROIDS (so we can at least assign a city centroid).

Coordinates are city centroids — this is the rough signal intended to
*surface* facilities for manual review, not to produce merge-ready rows.

Run
---
    python st_scraper.py

Output
------
    outputs/st_candidates.csv
    logs/st_fetch.log
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from mida_scraper import configure_logging, load_v5  # noqa: E402
from scrape_common import (  # noqa: E402
    MY_LOCATION_CENTROIDS,
    V5_COLUMNS,
    blank_v5_row,
    dedupe_against_v5,
    fetch_cached,
    load_operators,
    normalize_name,
)

BASE_URL = (
    "https://www.st.gov.my/stakeholders/electricity/"
    "list-of-registered-electricity-industry-professionals-and-licensees/"
    "list-public-distribution-licensee"
)
MAX_PAGES = 60

DC_COMPANY_KEYWORDS = [
    "data", "digital", "cloud", "hyperscale", "cyber", "centre", "center",
    "telco", "telecom", "telekom", "network",
]

MIN_MW = 5.0

_ROW_RE = re.compile(r"<tr>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
_CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


def clean_cell(html: str) -> str:
    txt = _TAG_RE.sub(" ", html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def parse_licensee_table(html: str) -> list[dict]:
    """Yield dicts with keys: name, address, start_date, end_date, mw."""
    rows = []
    for row_html in _ROW_RE.findall(html):
        cells = [clean_cell(c) for c in _CELL_RE.findall(row_html)]
        if len(cells) < 5:
            continue
        name, address, start_date, end_date, cap = cells[:5]
        mw = None
        m = re.search(r"([\d]+(?:\.\d+)?)", cap.replace(",", ""))
        if m:
            try:
                mw = float(m.group(1))
            except ValueError:
                mw = None
        rows.append({
            "name": name,
            "address": address,
            "start_date": start_date,
            "end_date": end_date,
            "mw": mw,
            "capacity_raw": cap,
        })
    return rows


def looks_dc_licensee(name: str, operators: list[str]) -> tuple[bool, str]:
    low = name.lower()
    for kw in DC_COMPANY_KEYWORDS:
        if kw in low:
            return True, f"keyword:{kw}"
    for op in operators:
        if not op or len(op) < 4:
            continue
        if re.search(r"\b" + re.escape(op) + r"\b", name, flags=re.IGNORECASE):
            return True, f"operator:{op}"
    return False, ""


def match_location(address: str) -> tuple[str, float, float] | None:
    for city, (lat, lon) in MY_LOCATION_CENTROIDS.items():
        if re.search(rf"\b{re.escape(city)}\b", address, flags=re.IGNORECASE):
            return (city, lat, lon)
    return None


def main() -> int:
    configure_logging(HERE / "logs" / "st_fetch.log")
    log = logging.getLogger(__name__)

    operators = load_operators(HERE / "v5_operators.txt")
    v5_rows = load_v5(HERE.parent / "outputs" / "malaysia_datacenters_v5.csv")

    candidates: list[dict] = []
    seen_licensees: set[str] = set()
    stats = {"pages_fetched": 0, "rows_parsed": 0, "passed_name_filter": 0,
             "passed_mw_filter": 0, "passed_location_filter": 0}

    for page in range(MAX_PAGES):
        url = BASE_URL if page == 0 else f"{BASE_URL}?page={page}"
        try:
            html = fetch_cached(url)
        except Exception as exc:  # noqa: BLE001
            log.warning("Fetch fail page=%d: %s", page, exc)
            break
        stats["pages_fetched"] += 1
        rows = parse_licensee_table(html)
        if not rows:
            log.info("No more rows at page=%d; stopping", page)
            break
        stats["rows_parsed"] += len(rows)

        for r in rows:
            key = normalize_name(r["name"])
            if key in seen_licensees:
                continue
            seen_licensees.add(key)

            matched, reason = looks_dc_licensee(r["name"], operators)
            if not matched:
                continue
            stats["passed_name_filter"] += 1

            if r["mw"] is None or r["mw"] < MIN_MW:
                continue
            stats["passed_mw_filter"] += 1

            loc = match_location(r["address"])
            if loc is None:
                continue
            stats["passed_location_filter"] += 1

            city, lat, lon = loc
            out = blank_v5_row()
            out["name"] = f"{r['name']} ({city})"
            out["operator"] = r["name"]
            out["operator_norm"] = r["name"]
            out["lat"] = lat
            out["lon"] = lon
            out["sources"] = "Energy Commission License"
            out["source_category"] = "Energy Commission License"
            out["source"] = url
            out["facility_type"] = "physical_facility"
            out["coord_confidence"] = "geocoded_with_campus_offset"
            out["address"] = r["address"]
            out["name_normalized"] = normalize_name(out["name"])
            # Year = license start date's year if parseable
            ym = re.search(r"(20\d{2})", r["start_date"])
            out["year"] = ym.group(1) if ym else ""
            out["note"] = (
                f"match={reason} | capacity={r['capacity_raw']} | "
                f"licensed={r['start_date']}..{r['end_date']}"
            )

            hit = dedupe_against_v5(out["name"], lat, lon, v5_rows)
            out["merge_status"] = hit.status
            out["merge_matched_name"] = hit.matched_name or ""
            out["merge_distance_m"] = "" if hit.distance_m is None else hit.distance_m
            candidates.append(out)

    log.info(
        "ST: pages=%d rows=%d → name=%d mw=%d loc=%d → %d candidates",
        stats["pages_fetched"], stats["rows_parsed"],
        stats["passed_name_filter"], stats["passed_mw_filter"],
        stats["passed_location_filter"], len(candidates),
    )

    out_path = HERE / "outputs" / "st_candidates.csv"
    columns = V5_COLUMNS + ["merge_status", "merge_matched_name", "merge_distance_m"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in candidates:
            w.writerow({c: r.get(c, "") for c in columns})
    log.info("Wrote %s", out_path)

    status_counts: dict[str, int] = {}
    for r in candidates:
        status_counts[r["merge_status"]] = status_counts.get(r["merge_status"], 0) + 1
    log.info("Dedup verdict: %s", json.dumps(status_counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
