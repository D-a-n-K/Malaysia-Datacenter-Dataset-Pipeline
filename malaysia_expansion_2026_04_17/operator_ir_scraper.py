"""
operator_ir_scraper.py — Task 4 of the 2026-04-17 expansion sprint.

Originally scoped as "Bursa Malaysia filings for YTL Power, TIME dotCom,
Telekom Malaysia, Sunway REIT, Gamuda". That plan does not survive first
contact with reality:

  * bursamalaysia.com returns HTTP 403 to every request (CDN/WAF block)
    before robots.txt can even be evaluated. We do not attempt to evade
    it. The upstream announcement data is therefore not accessible via
    automated HTTP clients for this pilot.

  * YTL Power's robots.txt sets `User-agent: * Disallow: /` — all crawlers
    except a named allow-list are denied. Our pilot user-agent is not in
    that list, so we skip YTL Power entirely.

Pivot: scrape the investor-relations / news sections of the four operators
that *do* allow access, and emit candidate rows for any DC-specific
announcement surfaced.

    * TIME dotCom (www.time.com.my) — discover URLs via sitemap_index.xml.
    * Telekom Malaysia (www.tm.com.my) — scrape /news and /announcements.
    * Sunway (www.sunway.com.my) — scrape /news and /media.
    * Gamuda (gamuda.com) — scrape /news and /investor-relations.

All use the same filter as the MIDA scraper (operator-in-title + DC keyword
or action verb, body supplies capex or MW).

Run
---
    python operator_ir_scraper.py

Output
------
    outputs/operator_ir_candidates.csv
    logs/operator_ir_fetch.log
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from mida_scraper import (  # noqa: E402
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

SM_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

SITEMAP_SOURCES = {
    # operator_label, sitemap_index_url, link_pattern_hosts
    "TIME dotCom": "https://www.time.com.my/sitemap_index.xml",
}

INDEX_SOURCES = {
    # operator_label, list of index pages to extract article hrefs from
    "Telekom Malaysia": [
        "https://www.tm.com.my/news",
        "https://www.tm.com.my/announcements",
    ],
    "Sunway": [
        "https://www.sunway.com.my/news",
        "https://www.sunway.com.my/media",
    ],
    "Gamuda": [
        "https://gamuda.com/news",
        "https://gamuda.com/investor-relations",
    ],
}

ARTICLE_HREF_RE = re.compile(r'href="([^"#?]+)"')


def load_sitemap_urls(sitemap_url: str) -> list[str]:
    out: list[str] = []
    root = ET.fromstring(fetch_cached(sitemap_url).lstrip("\ufeff \t\r\n"))
    # Sitemap index or direct urlset?
    if root.tag.endswith("sitemapindex"):
        for sm in root.findall("sm:sitemap", SM_NS):
            loc = sm.findtext("sm:loc", namespaces=SM_NS)
            if loc:
                try:
                    sub = ET.fromstring(fetch_cached(loc).lstrip("\ufeff \t\r\n"))
                except Exception as exc:  # noqa: BLE001
                    logging.warning("Sub-sitemap fail %s: %s", loc, exc)
                    continue
                for url in sub.findall("sm:url", SM_NS):
                    u = url.findtext("sm:loc", namespaces=SM_NS)
                    if u:
                        out.append(u)
    else:
        for url in root.findall("sm:url", SM_NS):
            u = url.findtext("sm:loc", namespaces=SM_NS)
            if u:
                out.append(u)
    return out


def extract_article_urls(index_html: str, base_host: str) -> list[str]:
    urls = set()
    for m in ARTICLE_HREF_RE.finditer(index_html):
        href = m.group(1)
        if href.startswith("http"):
            url = href
        elif href.startswith("/"):
            url = base_host + href
        else:
            continue
        if base_host not in url:
            continue
        # Focus on article-shaped paths
        low = url.lower()
        if not any(k in low for k in ("/news", "/media", "/press", "/announcement", "/article", "/story", "/post")):
            continue
        # Exclude PDFs for the main pass (Bursa-style filings); we handle PDFs separately
        if low.endswith((".jpg", ".png", ".gif", ".mp4", ".zip")):
            continue
        urls.add(url)
    return sorted(urls)


def process_article(
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
    configure_logging(HERE / "logs" / "operator_ir_fetch.log")
    log = logging.getLogger(__name__)

    operators = load_operators(HERE / "v5_operators.txt")
    v5_rows = load_v5(HERE.parent / "outputs" / "malaysia_datacenters_v5.csv")
    log.info("Loaded %d operators, %d v5 rows", len(operators), len(v5_rows))

    by_key: dict[tuple[str, str], dict] = {}
    stats = {"title_passed": 0, "body_passed": 0, "urls_scanned": 0}

    # Source 1: sitemap-based (TIME dotCom)
    for label, sm_url in SITEMAP_SOURCES.items():
        try:
            urls = load_sitemap_urls(sm_url)
        except Exception as exc:  # noqa: BLE001
            log.warning("Sitemap fail %s: %s", sm_url, exc)
            continue
        # Filter to DC-relevant slugs to cap fetch volume
        dc_slugs = ("data", "cloud", "dc-", "-dc", "hyperscale", "colocation", "centre", "center")
        filtered = [u for u in urls if any(s in u.lower() for s in dc_slugs)]
        log.info("%s sitemap=%d dc-filtered=%d", label, len(urls), len(filtered))
        for u in filtered:
            stats["urls_scanned"] += 1
            row = process_article(u, f"{label} Press Release", operators, v5_rows, stats)
            if row:
                key = (row["operator"].lower(), row["address"].lower())
                if key in by_key:
                    by_key[key]["note"] += " || " + row["note"]
                else:
                    by_key[key] = row

    # Source 2: index-page extraction (TM, Sunway, Gamuda)
    for label, indexes in INDEX_SOURCES.items():
        article_urls: set[str] = set()
        for idx in indexes:
            try:
                html = fetch_cached(idx)
            except Exception as exc:  # noqa: BLE001
                log.warning("Index fail %s: %s", idx, exc)
                continue
            base_host = "/".join(idx.split("/", 3)[:3])
            article_urls.update(extract_article_urls(html, base_host))
        log.info("%s article URLs discovered: %d", label, len(article_urls))

        # Prioritize slugs hinting at DC content
        dc_slugs = ("data", "cloud", "dc", "hyperscale", "digital-infra", "centre", "center")
        ranked = sorted(
            article_urls,
            key=lambda u: (not any(s in u.lower() for s in dc_slugs), u),
        )
        for u in ranked[:80]:  # cap per source to avoid hammering
            stats["urls_scanned"] += 1
            row = process_article(u, f"{label} Press Release", operators, v5_rows, stats)
            if row:
                key = (row["operator"].lower(), row["address"].lower())
                if key in by_key:
                    by_key[key]["note"] += " || " + row["note"]
                else:
                    by_key[key] = row

    candidates = list(by_key.values())
    log.info(
        "Operator IR: urls_scanned=%d title_passed=%d body_passed=%d → %d candidates",
        stats["urls_scanned"], stats["title_passed"], stats["body_passed"],
        len(candidates),
    )

    out_path = HERE / "outputs" / "operator_ir_candidates.csv"
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
