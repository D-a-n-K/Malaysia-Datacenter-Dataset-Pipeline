"""
mida_scraper.py — Task 2 of the 2026-04-17 expansion sprint.

Fetches MIDA media releases via https://www.mida.gov.my/media-release-sitemap.xml
and the three mida-news-sitemap*.xml volumes. For each URL published 2022-01-01
or later, downloads the page, filters on DC keywords + known v5 operator names,
and extracts:

    title, date, operator(s), location (city + centroid lat/lon), capex,
    MW capacity, year mentions

Emits candidate rows in the v5 schema with source_category='MIDA Press Release'
and a per-row dedup verdict against the current v5 corpus.

Run
---
    python mida_scraper.py

Output
------
    outputs/mida_candidates.csv
    logs/mida_fetch.log
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from scrape_common import (  # noqa: E402
    DC_KEYWORDS,
    V5_COLUMNS,
    blank_v5_row,
    dedupe_against_v5,
    fetch_cached,
    find_capex,
    find_dates,
    find_locations,
    find_megawatts,
    find_operators,
    find_years,
    html_to_text,
    load_operators,
    normalize_name,
)


SITEMAPS = [
    "https://www.mida.gov.my/media-release-sitemap.xml",
    "https://www.mida.gov.my/mida-news-sitemap.xml",
    "https://www.mida.gov.my/mida-news-sitemap2.xml",
    "https://www.mida.gov.my/mida-news-sitemap3.xml",
]
MIN_DATE = datetime(2022, 1, 1)

SM_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def configure_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )


def parse_sitemap(xml_text: str) -> list[tuple[str, datetime | None]]:
    # MIDA's WordPress-SEO sitemap has two leading spaces before <?xml which
    # breaks strict XML parsers. Trim whitespace before the prolog.
    root = ET.fromstring(xml_text.lstrip("\ufeff \t\r\n"))
    entries: list[tuple[str, datetime | None]] = []
    for url in root.findall("sm:url", SM_NS):
        loc = url.findtext("sm:loc", namespaces=SM_NS)
        if not loc:
            continue
        lastmod_raw = url.findtext("sm:lastmod", default="", namespaces=SM_NS)
        dt: datetime | None = None
        if lastmod_raw:
            try:
                dt = datetime.fromisoformat(lastmod_raw.replace("Z", "+00:00"))
                dt = dt.replace(tzinfo=None)
            except ValueError:
                dt = None
        entries.append((loc, dt))
    return entries


_DC_TITLE_KEYWORDS = (
    "data centre", "data center", "hyperscale", "cloud region",
    "colocation", "co-location", "dc ", " dc", "cloud",
)
_FACILITY_ACTION_VERBS = (
    "launches", "opens", "breaks ground", "ground-?break", "unveils",
    "announces", "commissions", "completes", "expands", "establishes",
    "commences", "inaugurates", "reveals", "acquires", "invests",
)


def title_is_dc_specific(title: str, operators: list[str]) -> bool:
    """
    True when the title reads like a specific facility announcement, not a
    broader think-piece that happens to mention hyperscalers in the body.

    Rule: the title must name a known operator AND either
      (a) contain a DC/cloud keyword, or
      (b) contain a facility action verb (launches, breaks ground, opens...).

    This filters out market-analysis articles ("Data centre appeal", "Taking
    a hard look at data centres") while keeping specific announcements
    ("AWS announces RM25.5b to launch cloud region").
    """
    low = title.lower()
    ops_hit = find_operators(title, operators)
    if not ops_hit:
        return False
    has_dc_kw = any(kw in low for kw in _DC_TITLE_KEYWORDS)
    has_action = bool(
        re.search(r"\b(" + "|".join(_FACILITY_ACTION_VERBS) + r")\b", low)
    )
    return has_dc_kw or has_action


def body_has_facility_signal(text: str) -> bool:
    """Require at least one quantitative fact (capex or MW) in the body."""
    return bool(find_capex(text) or find_megawatts(text))


_TITLE_RE = re.compile(r"<title[^>]*>([^<]+)</title>", re.IGNORECASE)


def extract_title(html_src: str) -> str:
    m = _TITLE_RE.search(html_src)
    if not m:
        return ""
    return m.group(1).strip().replace(" – MIDA | Malaysian Investment Development Authority", "")


def extract_main_text(html_src: str) -> str:
    """MIDA uses a WordPress entry-content div. Prefer that if present."""
    m = re.search(
        r'<div[^>]+class="[^"]*(?:entry-content|post-content|the_content)[^"]*"[^>]*>(.*?)</div>\s*(?:<footer|<div\s+class="(?:post-tags|entry-footer))',
        html_src,
        flags=re.DOTALL | re.IGNORECASE,
    )
    fragment = m.group(1) if m else html_src
    return html_to_text(fragment)


def load_v5(v5_path: Path) -> list[dict]:
    import csv as _csv  # avoid shadow
    rows: list[dict] = []
    with v5_path.open(newline="", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            try:
                row["lat"] = float(row["lat"]) if row.get("lat") else None
                row["lon"] = float(row["lon"]) if row.get("lon") else None
            except ValueError:
                row["lat"] = row["lon"] = None
            rows.append(row)
    return rows


def build_candidate_row(
    title: str,
    url: str,
    date_str: str,
    text: str,
    operators_hit: list[str],
    locations_hit: list[tuple[str, float, float]],
    capex_hits: list[str],
    mw_hits: list[str],
    year_hits: list[str],
) -> dict:
    row = blank_v5_row()
    primary_location = locations_hit[0] if locations_hit else None
    primary_operator = operators_hit[0] if operators_hit else ""

    row["name"] = (
        f"{primary_operator} facility ({primary_location[0]})"
        if primary_operator and primary_location
        else title
    )
    row["operator"] = primary_operator
    row["operator_norm"] = primary_operator
    row["lat"] = primary_location[1] if primary_location else ""
    row["lon"] = primary_location[2] if primary_location else ""
    row["sources"] = "MIDA Press Release"
    row["source_category"] = "MIDA Press Release"
    row["source"] = url
    row["facility_type"] = "physical_facility"
    row["coord_confidence"] = "geocoded_with_campus_offset" if primary_location else ""
    row["address"] = primary_location[0] if primary_location else ""
    row["name_normalized"] = normalize_name(row["name"])
    row["year"] = sorted(year_hits)[-1] if year_hits else ""
    note_parts = []
    if operators_hit:
        note_parts.append(f"operators={', '.join(operators_hit)}")
    if locations_hit:
        note_parts.append(f"locations={', '.join(l[0] for l in locations_hit)}")
    if capex_hits:
        note_parts.append(f"capex={', '.join(capex_hits[:3])}")
    if mw_hits:
        note_parts.append(f"mw={', '.join(mw_hits[:3])}")
    if date_str:
        note_parts.append(f"published={date_str}")
    note_parts.append(f"title={title}")
    row["note"] = " | ".join(note_parts)
    return row


def main() -> int:
    configure_logging(HERE / "logs" / "mida_fetch.log")
    log = logging.getLogger(__name__)

    operators = load_operators(HERE / "v5_operators.txt")
    v5_rows = load_v5(HERE.parent / "outputs" / "malaysia_datacenters_v5.csv")
    log.info("Loaded %d operators, %d v5 rows", len(operators), len(v5_rows))

    # Phase 1 — collect URLs from sitemaps.
    all_urls: list[tuple[str, datetime | None]] = []
    for sm_url in SITEMAPS:
        try:
            text = fetch_cached(sm_url)
            entries = parse_sitemap(text)
            log.info("Sitemap %s → %d URLs", sm_url, len(entries))
            all_urls.extend(entries)
        except Exception as exc:  # noqa: BLE001
            log.warning("Sitemap failed %s: %s", sm_url, exc)

    # Filter by date.
    filtered: list[tuple[str, datetime | None]] = []
    for url, dt in all_urls:
        if dt is None or dt >= MIN_DATE:
            filtered.append((url, dt))
    log.info(
        "URLs total=%d, post-2022 (or undated)=%d",
        len(all_urls),
        len(filtered),
    )

    # Phase 2 — pre-filter by URL slug. MIDA slugs are descriptive; if a DC keyword
    # appears in the slug we fetch. Otherwise we still fetch a 10%-ish sample so
    # we don't completely miss releases that only reveal DC content in the body.
    keywords_in_slug = ("data", "cloud", "hyperscale", "digital", "dc", "colocation", "cyberjaya", "johor")
    operator_slugs = [normalize_name(o).replace(" ", "-") for o in operators if len(o) > 3]

    def slug_looks_interesting(url: str) -> bool:
        slug = url.rsplit("/media-release/", 1)[-1].rsplit("/mida-news/", 1)[-1].lower()
        if any(k in slug for k in keywords_in_slug):
            return True
        return any(op_slug and op_slug in slug for op_slug in operator_slugs)

    # Primary: slug-matched. Secondary: all the rest (capped to avoid hammering).
    primary = [(u, d) for (u, d) in filtered if slug_looks_interesting(u)]
    log.info("Slug-matched URLs to fetch: %d", len(primary))

    # Phase 3 — fetch, classify, extract. We keep only releases whose TITLE
    # reads as a specific facility announcement (DC keyword OR operator +
    # action verb) AND whose BODY supplies a capex or MW figure AND at least
    # one Malaysian location. Within this pass we dedupe by (operator,
    # location) so that multiple releases about the same investment collapse
    # into a single facility row (keeping the earliest date and the union of
    # facts in the note).
    by_key: dict[tuple[str, str], dict] = {}
    stats = {"fetched": 0, "title_passed": 0, "body_passed": 0, "no_location": 0, "no_operator": 0}

    for url, dt in primary:
        stats["fetched"] += 1
        try:
            src = fetch_cached(url)
        except Exception as exc:  # noqa: BLE001
            log.warning("Fetch failed %s: %s", url, exc)
            continue
        title = extract_title(src)
        body = extract_main_text(src)

        if not title_is_dc_specific(title, operators):
            continue
        stats["title_passed"] += 1

        if not body_has_facility_signal(body):
            continue
        stats["body_passed"] += 1

        ops_hit = find_operators(title + "\n" + body, operators)
        if not ops_hit:
            stats["no_operator"] += 1
            continue

        # Prefer operators that appear in the TITLE (much stronger signal than
        # a single body mention). Fall back to body hits if none in title.
        title_ops = find_operators(title, operators)
        primary_operator = (title_ops or ops_hit)[0]

        locs_hit = find_locations(body)
        if not locs_hit:
            stats["no_location"] += 1
            continue

        # Same preference: location in title wins over body-only.
        title_locs = find_locations(title)
        primary_location = (title_locs or locs_hit)[0]

        capex_hit = find_capex(body)
        mw_hit = find_megawatts(body)
        years = find_years(body)
        dates = find_dates(body)
        date_str = dates[0] if dates else (dt.date().isoformat() if dt else "")

        key = (primary_operator.lower(), primary_location[0].lower())
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
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
        else:
            existing["note"] = existing["note"] + " || " + row["note"]

    candidates = list(by_key.values())
    log.info(
        "MIDA phases: fetched=%d title_passed=%d body_passed=%d no_op=%d no_loc=%d → %d unique (op,loc)",
        stats["fetched"], stats["title_passed"], stats["body_passed"],
        stats["no_operator"], stats["no_location"], len(candidates),
    )

    # Now dedupe against v5.
    for row in candidates:
        cand_lat = row["lat"] if row["lat"] != "" else None
        cand_lon = row["lon"] if row["lon"] != "" else None
        hit = dedupe_against_v5(row["name"], cand_lat, cand_lon, v5_rows)
        row["merge_status"] = hit.status
        row["merge_matched_name"] = hit.matched_name or ""
        row["merge_distance_m"] = "" if hit.distance_m is None else hit.distance_m

    out_path = HERE / "outputs" / "mida_candidates.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    columns = V5_COLUMNS + ["merge_status", "merge_matched_name", "merge_distance_m"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in candidates:
            w.writerow({c: r.get(c, "") for c in columns})
    log.info("Wrote %s (%d rows)", out_path, len(candidates))

    # Summary for stdout visibility
    status_counts: dict[str, int] = {}
    for r in candidates:
        status_counts[r["merge_status"]] = status_counts.get(r["merge_status"], 0) + 1
    log.info("Dedup verdict: %s", json.dumps(status_counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
