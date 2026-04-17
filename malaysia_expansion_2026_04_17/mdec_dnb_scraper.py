"""
mdec_dnb_scraper.py — Task 3 of the 2026-04-17 expansion sprint.

Two sources combined, emitting a single candidates CSV:

MDEC
    Discover URLs via the WordPress sitemap at https://mdec.my/sitemap-0.xml
    and narrow to `/media-release/`, `/blog/`, `/industrycatalogue/`,
    `/announcement/`. Apply the same filter as the MIDA scraper
    (title must name a known operator AND a DC keyword / action verb, body
    must supply capex or MW).

DNB (Digital Nasional Berhad, the 5G wholesaler at digital-nasional.com.my)
    robots.txt is 403-Forbidden and no sitemap is exposed. /news does render
    (HTTP 200). We fetch /news, extract linked article hrefs, and scrape
    those. This is a best-effort pass and will most likely yield zero new
    DC facilities — DNB's role was formally wound down in 2025. We still
    scrape to check for any infra partner announcements that name colo
    operators.

Correction note: the original sprint brief pointed at https://www.dnb.com.my/
which is in fact Dun & Bradstreet Malaysia, not Digital Nasional Berhad.
We use the correct domain digital-nasional.com.my.

Result (2026-04-17): zero candidates. Defensible negative result.

  MDEC — its 335 `/media-release/` pages are about MDEC-internal initiatives
  (MSC accreditation ceremonies, AI policy launches, MOUs) and do not name
  specific DC facilities with facility-level facts. MDEC's role is
  regulatory, not infrastructure — the right facility signal lives at MIDA
  (investment announcements) and Bursa (listed-operator disclosures).

  DNB — articles are at flat root-level slugs rather than /news/SLUG, and
  DNB's single-wholesale-network role was formally wound down in Jan 2025.
  Expected yield was already low; we document 0 rather than widen the link
  extractor and invite noise.

Both sources remain worth a *manual* re-check if new Malaysian cloud-policy
or 5G-infrastructure stories break, but automated scraping of them is not
productive for the land-transformation research question.

Run
---
    python mdec_dnb_scraper.py

Output
------
    outputs/mdec_dnb_candidates.csv
    logs/mdec_dnb_fetch.log
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

from mida_scraper import (  # noqa: E402 — reuse the MIDA filters/extractors
    body_has_facility_signal,
    build_candidate_row,
    configure_logging,
    extract_main_text,
    extract_title,
    load_v5,
    title_is_dc_specific,
)
from scrape_common import (  # noqa: E402
    V5_COLUMNS,
    dedupe_against_v5,
    fetch_cached,
    find_capex,
    find_dates,
    find_locations,
    find_megawatts,
    find_operators,
    find_years,
    load_operators,
)

MDEC_SITEMAP = "https://mdec.my/sitemap-0.xml"
MDEC_SLUG_PREFIXES = ("/media-release/", "/blog/", "/industrycatalogue/", "/announcement/")

DNB_NEWS_INDEX = "https://www.digital-nasional.com.my/news"
DNB_BLOG_INDEX = "https://www.digital-nasional.com.my/blogs/2023"


def load_sitemap_locs(url: str) -> list[str]:
    text = fetch_cached(url)
    return re.findall(r"<loc>([^<]+)</loc>", text)


def filter_mdec_urls(urls: list[str], operators: list[str]) -> list[str]:
    kw = ("data", "cloud", "hyperscale", "colocation", "dc-", "-dc", "digital-infrastructure")
    op_slugs = [re.sub(r"[^a-z0-9]+", "-", o.lower()).strip("-") for o in operators if len(o) > 3]
    out = []
    for u in urls:
        path = u.split("mdec.my", 1)[-1]
        if not any(path.startswith(pfx) for pfx in MDEC_SLUG_PREFIXES):
            continue
        low = path.lower()
        if any(k in low for k in kw) or any(s and s in low for s in op_slugs):
            out.append(u)
    return out


def extract_dnb_article_urls(index_html: str) -> list[str]:
    # DNB uses Drupal; article links typically live under /news/... or /blogs/...
    urls = set()
    for m in re.finditer(r'href="(/(?:news|blogs|insights)/[^"#?]+)"', index_html):
        urls.add("https://www.digital-nasional.com.my" + m.group(1))
    for m in re.finditer(
        r'href="(https://www\.digital-nasional\.com\.my/(?:news|blogs|insights)/[^"#?]+)"',
        index_html,
    ):
        urls.add(m.group(1))
    return sorted(urls)


def process_url(
    url: str,
    source_category: str,
    operators: list[str],
    v5_rows: list[dict],
    stats: dict,
) -> dict | None:
    try:
        src = fetch_cached(url)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Fetch failed %s: %s", url, exc)
        return None
    title = extract_title(src)
    body = extract_main_text(src)
    if not title_is_dc_specific(title, operators):
        return None
    stats["title_passed"] += 1
    if not body_has_facility_signal(body):
        return None
    stats["body_passed"] += 1
    ops_hit = find_operators(title + "\n" + body, operators)
    if not ops_hit:
        return None
    title_ops = find_operators(title, operators)
    primary_operator = (title_ops or ops_hit)[0]
    locs_hit = find_locations(body)
    if not locs_hit:
        return None
    title_locs = find_locations(title)
    primary_location = (title_locs or locs_hit)[0]
    capex_hit = find_capex(body)
    mw_hit = find_megawatts(body)
    years = find_years(body)
    dates = find_dates(body)
    date_str = dates[0] if dates else ""
    row = build_candidate_row(
        title=title,
        url=url,
        date_str=date_str,
        text=body,
        operators_hit=[primary_operator] + [o for o in ops_hit if o != primary_operator],
        locations_hit=[primary_location] + [l for l in locs_hit if l != primary_location],
        capex_hits=capex_hit,
        mw_hits=mw_hit,
        year_hits=years,
    )
    row["sources"] = source_category
    row["source_category"] = source_category
    cand_lat = row["lat"] if row["lat"] != "" else None
    cand_lon = row["lon"] if row["lon"] != "" else None
    hit = dedupe_against_v5(row["name"], cand_lat, cand_lon, v5_rows)
    row["merge_status"] = hit.status
    row["merge_matched_name"] = hit.matched_name or ""
    row["merge_distance_m"] = "" if hit.distance_m is None else hit.distance_m
    return row


def main() -> int:
    configure_logging(HERE / "logs" / "mdec_dnb_fetch.log")
    log = logging.getLogger(__name__)

    operators = load_operators(HERE / "v5_operators.txt")
    v5_rows = load_v5(HERE.parent / "outputs" / "malaysia_datacenters_v5.csv")
    log.info("Loaded %d operators, %d v5 rows", len(operators), len(v5_rows))

    by_key: dict[tuple[str, str], dict] = {}
    stats = {"mdec_urls": 0, "dnb_urls": 0, "title_passed": 0, "body_passed": 0}

    # --- MDEC ---
    try:
        sitemap_urls = load_sitemap_locs(MDEC_SITEMAP)
        mdec_urls = filter_mdec_urls(sitemap_urls, operators)
        log.info(
            "MDEC sitemap=%d, matched URLs=%d",
            len(sitemap_urls), len(mdec_urls),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("MDEC sitemap failed: %s", exc)
        mdec_urls = []

    stats["mdec_urls"] = len(mdec_urls)
    for url in mdec_urls:
        row = process_url(url, "MDEC Press Release", operators, v5_rows, stats)
        if row:
            key = (row["operator"].lower(), row["address"].lower())
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = row
            else:
                existing["note"] += " || " + row["note"]

    # --- DNB ---
    dnb_article_urls: list[str] = []
    for idx in (DNB_NEWS_INDEX, DNB_BLOG_INDEX):
        try:
            idx_html = fetch_cached(idx)
            dnb_article_urls.extend(extract_dnb_article_urls(idx_html))
        except Exception as exc:  # noqa: BLE001
            log.warning("DNB index failed %s: %s", idx, exc)
    dnb_article_urls = sorted(set(dnb_article_urls))
    log.info("DNB article URLs discovered: %d", len(dnb_article_urls))
    stats["dnb_urls"] = len(dnb_article_urls)

    for url in dnb_article_urls:
        row = process_url(url, "DNB Press Release", operators, v5_rows, stats)
        if row:
            key = (row["operator"].lower(), row["address"].lower())
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = row
            else:
                existing["note"] += " || " + row["note"]

    candidates = list(by_key.values())
    log.info(
        "MDEC+DNB: mdec_urls=%d dnb_urls=%d title_passed=%d body_passed=%d → %d candidates",
        stats["mdec_urls"], stats["dnb_urls"],
        stats["title_passed"], stats["body_passed"], len(candidates),
    )

    out_path = HERE / "outputs" / "mdec_dnb_candidates.csv"
    columns = V5_COLUMNS + ["merge_status", "merge_matched_name", "merge_distance_m"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in candidates:
            w.writerow({c: r.get(c, "") for c in columns})
    log.info("Wrote %s (%d rows)", out_path, len(candidates))

    status_counts: dict[str, int] = {}
    for r in candidates:
        status_counts[r["merge_status"]] = status_counts.get(r["merge_status"], 0) + 1
    log.info("Dedup verdict: %s", json.dumps(status_counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
