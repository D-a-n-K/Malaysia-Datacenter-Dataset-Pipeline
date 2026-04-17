"""
peeringdb_scraper.py — Task 6 of the 2026-04-17 expansion sprint.

Query the public PeeringDB facility API for Malaysia and emit any facility
not already represented in v5. PeeringDB IDs are stable (`fac_id`), so each
candidate carries one in its `sources` field for downstream cross-linking.

The v5 CSV was originally seeded from PeeringDB via the v4 R pipeline, but
the `source_category` column shows no PeeringDB rows — merge-out during
dedup dropped the provenance. Re-querying lets us (a) surface any facilities
added since v4 was built, and (b) emit verifiable PeeringDB URLs for every
row that matches, improving the confidence score on existing v5 rows.

Run
---
    python peeringdb_scraper.py

Output
------
    outputs/peeringdb_candidates.csv  — rows not in v5, in v5 schema
    outputs/peeringdb_matches.csv     — v5 rows with their PeeringDB match,
                                        for enrichment in the Task 7 merge
    logs/peeringdb_fetch.log
"""

from __future__ import annotations

import csv
import json
import logging
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from scrape_common import (  # noqa: E402
    USER_AGENT,
    V5_COLUMNS,
    blank_v5_row,
    dedupe_against_v5,
    normalize_name,
)

from mida_scraper import configure_logging, load_v5  # noqa: E402

API_URL = "https://www.peeringdb.com/api/fac?country=MY"


def fetch_api(url: str) -> list[dict]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.load(resp)
    return data.get("data", [])


def main() -> int:
    configure_logging(HERE / "logs" / "peeringdb_fetch.log")
    log = logging.getLogger(__name__)

    v5_rows = load_v5(HERE.parent / "outputs" / "malaysia_datacenters_v5.csv")

    log.info("GET %s", API_URL)
    fac_rows = fetch_api(API_URL)
    log.info("PeeringDB returned %d Malaysia facilities", len(fac_rows))

    candidates: list[dict] = []
    matches: list[dict] = []

    for fac in fac_rows:
        name = fac.get("name") or ""
        lat = fac.get("latitude")
        lon = fac.get("longitude")
        try:
            lat_f = float(lat) if lat not in (None, "") else None
            lon_f = float(lon) if lon not in (None, "") else None
        except (TypeError, ValueError):
            lat_f = lon_f = None

        hit = dedupe_against_v5(name, lat_f, lon_f, v5_rows)
        pdb_url = f"https://www.peeringdb.com/fac/{fac['id']}"
        if hit.status != "new":
            matches.append({
                "pdb_id": fac["id"],
                "pdb_name": name,
                "pdb_url": pdb_url,
                "pdb_lat": lat_f if lat_f is not None else "",
                "pdb_lon": lon_f if lon_f is not None else "",
                "v5_name": hit.matched_name or "",
                "merge_status": hit.status,
                "merge_distance_m": hit.distance_m if hit.distance_m is not None else "",
            })
            continue

        row = blank_v5_row()
        row["name"] = name
        row["operator"] = fac.get("org_name") or ""
        row["operator_norm"] = fac.get("org_name") or ""
        row["lat"] = lat_f if lat_f is not None else ""
        row["lon"] = lon_f if lon_f is not None else ""
        row["sources"] = f"PeeringDB fac {fac['id']}"
        row["source_category"] = "PeeringDB"
        row["source"] = pdb_url
        row["address"] = ", ".join(
            p for p in (fac.get("address1"), fac.get("city"), fac.get("zipcode")) if p
        )
        row["facility_type"] = "physical_facility"
        row["coord_confidence"] = "source_native" if lat_f is not None else ""
        row["name_normalized"] = normalize_name(name)
        row["note"] = (
            f"pdb_id={fac['id']} | "
            f"clli={fac.get('clli') or ''} | "
            f"region={fac.get('region_continent') or ''} | "
            f"status={fac.get('status') or ''}"
        )
        row["merge_status"] = "new"
        row["merge_matched_name"] = ""
        row["merge_distance_m"] = ""
        candidates.append(row)

    # Write candidates
    out_path = HERE / "outputs" / "peeringdb_candidates.csv"
    columns = V5_COLUMNS + ["merge_status", "merge_matched_name", "merge_distance_m"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in candidates:
            w.writerow({c: r.get(c, "") for c in columns})
    log.info("Wrote %s (%d new candidates)", out_path, len(candidates))

    # Write match file for v5 enrichment
    match_path = HERE / "outputs" / "peeringdb_matches.csv"
    with match_path.open("w", newline="", encoding="utf-8") as f:
        if matches:
            w = csv.DictWriter(f, fieldnames=list(matches[0].keys()))
            w.writeheader()
            w.writerows(matches)
        else:
            f.write("")
    log.info("Wrote %s (%d matches)", match_path, len(matches))
    return 0


if __name__ == "__main__":
    sys.exit(main())
