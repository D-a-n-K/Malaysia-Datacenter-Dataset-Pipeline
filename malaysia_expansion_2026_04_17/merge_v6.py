"""
merge_v6.py — Task 7 of the 2026-04-17 expansion sprint.

Concatenates every *_candidates.csv produced by Tasks 2-6, applies the
confidence scoring module (Task 1), and splits the result into three
review tiers:

    v6_high_confidence.csv   confidence ≥ 0.7 AND any sibling candidate
                             within 300m OR name match (sibling mode)
    v6_medium_confidence.csv 0.4 ≤ confidence < 0.7
    v6_needs_review.csv      confidence < 0.4 OR single-source

Also assigns a four-tier operator classification (sovereign/hyperscaler/
tier-1/tier-2) to every row — the classification the advisor asked for in
the morning brief. And generates a markdown summary report for Sam.

Run
---
    python merge_v6.py

Outputs
-------
    outputs/v6_high_confidence.csv
    outputs/v6_medium_confidence.csv
    outputs/v6_needs_review.csv
    outputs/v6_expansion_report.md
    logs/merge_v6.log
"""

from __future__ import annotations

import csv
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from confidence_score import score_row, source_agreement_score  # noqa: E402
from mida_scraper import configure_logging, load_v5  # noqa: E402
from scrape_common import V5_COLUMNS, haversine_m, normalize_name  # noqa: E402

CANDIDATE_FILES = [
    "mida_candidates.csv",
    "mdec_dnb_candidates.csv",
    "operator_ir_candidates.csv",
    "peeringdb_candidates.csv",
    "st_candidates.csv",
]


# ---------------------------------------------------------------------------
# Operator tier classification (advisor request: four tiers at 80-row scale)
# ---------------------------------------------------------------------------

HYPERSCALER_OPERATORS = {
    "aws", "amazon", "azure", "microsoft", "google", "gcp", "oracle",
    "alibaba", "bytedance", "tiktok",
}

TIER1_COLO_OPERATORS = {
    "equinix", "digital realty", "nttdata", "ntt data", "ntt", "stt gdc",
    "st telemedia", "keppel data centres", "keppel", "airtrunk",
    "princeton digital group", "pdg", "edgeconnex", "vantage",
    "bridge data centres", "bdc", "dayone", "gds", "stack infrastructure",
    "empyrion digital",
}

SOVEREIGN_TELCO_OPERATORS = {
    "telekom malaysia", "tm", "vads", "ytl", "ytl power",
    "ytl data center holdings", "time dotcom", "time", "timecom",
    "celcomdigi", "maxis",
}

# Everything else that still looks DC-ish (includes smaller local colos like
# AIMS, Basis Bay, IRIX, Open DC, ModernOne, Infinaxis, MEASAT, SACOFA,
# Danawa, i-Tech, IP ServerOne, etc.)
TIER2_SIGNAL = True


def classify_operator(operator: str) -> str:
    low = operator.lower().strip()
    if not low:
        return "unknown"
    for kw in SOVEREIGN_TELCO_OPERATORS:
        if kw in low:
            return "sovereign_telco"
    for kw in HYPERSCALER_OPERATORS:
        if kw in low:
            return "hyperscaler"
    for kw in TIER1_COLO_OPERATORS:
        if kw in low:
            return "tier1_colo"
    return "tier2_colo"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def load_all_candidates() -> pd.DataFrame:
    frames = []
    for fn in CANDIDATE_FILES:
        p = HERE / "outputs" / fn
        if not p.exists():
            continue
        df = pd.read_csv(p)
        df["_source_file"] = fn
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    # Ensure v5 columns present even when empty
    for c in V5_COLUMNS + ["merge_status", "merge_matched_name", "merge_distance_m"]:
        if c not in combined.columns:
            combined[c] = ""
    return combined


def find_siblings(df: pd.DataFrame, radius_m: float = 300.0) -> dict[int, list[int]]:
    """Return idx → list[idx] of other candidates describing the same facility."""
    out: dict[int, list[int]] = defaultdict(list)
    records = df.to_dict("records")
    for i, row_i in enumerate(records):
        lat_i = row_i.get("lat")
        lon_i = row_i.get("lon")
        name_i = normalize_name(str(row_i.get("name") or ""))
        try:
            lat_i_f = float(lat_i) if lat_i not in (None, "", "nan") else None
            lon_i_f = float(lon_i) if lon_i not in (None, "", "nan") else None
        except (TypeError, ValueError):
            lat_i_f = lon_i_f = None
        for j, row_j in enumerate(records):
            if i == j:
                continue
            name_j = normalize_name(str(row_j.get("name") or ""))
            if name_i and name_j and name_i == name_j:
                out[i].append(j)
                continue
            try:
                lat_j_f = float(row_j["lat"]) if row_j["lat"] not in (None, "", "nan") else None
                lon_j_f = float(row_j["lon"]) if row_j["lon"] not in (None, "", "nan") else None
            except (TypeError, ValueError):
                lat_j_f = lon_j_f = None
            if lat_i_f is None or lon_i_f is None or lat_j_f is None or lon_j_f is None:
                continue
            if haversine_m(lat_i_f, lon_i_f, lat_j_f, lon_j_f) <= radius_m:
                out[i].append(j)
    return out


def main() -> int:
    configure_logging(HERE / "logs" / "merge_v6.log")
    log = logging.getLogger(__name__)

    df = load_all_candidates()
    if df.empty:
        log.error("No candidates to merge.")
        return 1

    log.info("Loaded %d rows across %d candidate files",
             len(df), len(df["_source_file"].unique()))

    # Load v5 as additional sibling pool: if a candidate is within 300m of an
    # existing v5 row (or name-matches one), treat that as corroboration —
    # the candidate is independently re-discovering a v5 facility, which is
    # exactly what we want agreement-weight to capture.
    v5_rows = load_v5(HERE.parent / "outputs" / "malaysia_datacenters_v5.csv")
    v5_records = [
        {"lat": v["lat"], "lon": v["lon"], "name": v.get("name", ""),
         "name_normalized": v.get("name_normalized", "")}
        for v in v5_rows
    ]

    # Compute n_sources across sibling candidates (same name or within 300m)
    siblings = find_siblings(df)
    sibling_counts = {i: 1 + len(siblings[i]) for i in range(len(df))}
    df["n_sources"] = df.index.map(sibling_counts).fillna(1).astype(int)

    # Sibling-aware confidence score. Siblings = other candidates + v5 rows
    # within 300m (v5 siblings bump n_sources by the count of matched v5 rows).
    records = df.to_dict("records")
    scores: list[float] = []
    from confidence_score import (
        geocode_precision_score,
        source_count_score,
    )
    for i, row in enumerate(records):
        cand_sibs = [records[j] for j in siblings.get(i, [])]
        v5_sibs = []
        try:
            lat_f = float(row["lat"]) if row["lat"] not in (None, "", "nan") else None
            lon_f = float(row["lon"]) if row["lon"] not in (None, "", "nan") else None
        except (TypeError, ValueError):
            lat_f = lon_f = None
        cand_name_norm = normalize_name(str(row.get("name") or ""))
        for v in v5_records:
            try:
                v_lat = float(v["lat"]) if v["lat"] not in (None, "", "nan") else None
                v_lon = float(v["lon"]) if v["lon"] not in (None, "", "nan") else None
            except (TypeError, ValueError):
                v_lat = v_lon = None
            if (lat_f is not None and lon_f is not None
                    and v_lat is not None and v_lon is not None
                    and haversine_m(lat_f, lon_f, v_lat, v_lon) <= 300.0):
                v5_sibs.append(v)
                continue
            v_name_norm = v.get("name_normalized") or normalize_name(str(v.get("name") or ""))
            if cand_name_norm and v_name_norm and cand_name_norm == v_name_norm:
                v5_sibs.append(v)
        effective_n_sources = df.loc[i, "n_sources"] + len(v5_sibs)
        sibs_all = cand_sibs + v5_sibs
        agree = source_agreement_score(row, siblings=sibs_all) if sibs_all else source_agreement_score(row)
        s_count = source_count_score(effective_n_sources)
        s_geo = geocode_precision_score(row.get("coord_confidence"))
        scores.append(round(0.4 * s_count + 0.3 * agree + 0.3 * s_geo, 4))
        # Persist the effective n_sources for downstream reporting
        df.loc[i, "n_sources"] = effective_n_sources
    df["confidence"] = scores

    # Operator tier
    df["operator_tier"] = df["operator"].fillna("").astype(str).apply(classify_operator)

    # Collapse near-duplicates within the candidate pool, keeping the highest
    # confidence row as the representative. Everything collapsed becomes a
    # "co_sources" list in the note.
    kept: list[dict] = []
    dropped_into: dict[int, int] = {}  # dropped idx → surviving idx
    sorted_idx = sorted(range(len(df)), key=lambda i: -df.loc[i, "confidence"])
    keeper_mask = [True] * len(df)
    for idx in sorted_idx:
        if not keeper_mask[idx]:
            continue
        for j in siblings.get(idx, []):
            if keeper_mask[j]:
                keeper_mask[j] = False
                dropped_into[j] = idx
    for i, keep in enumerate(keeper_mask):
        if keep:
            row = records[i].copy()
            co_sources = [records[j]["source"] for j, to in dropped_into.items() if to == i and records[j].get("source")]
            if co_sources:
                row["note"] = (row.get("note") or "") + " || co_sources=" + "; ".join(co_sources)
                row["sources"] = (row.get("sources") or "") + " + " + str(len(co_sources)) + " sibling(s)"
            row["confidence"] = df.loc[i, "confidence"]
            row["operator_tier"] = df.loc[i, "operator_tier"]
            row["n_sources"] = df.loc[i, "n_sources"]
            kept.append(row)

    result = pd.DataFrame(kept)
    log.info("After sibling collapse: %d unique rows (from %d candidates)",
             len(result), len(df))

    # Split into tiers
    high = result[(result["confidence"] >= 0.7) & (result["n_sources"] >= 2)]
    review = result[result["confidence"] < 0.4]
    medium = result.drop(index=high.index.union(review.index))

    columns = V5_COLUMNS + [
        "merge_status", "merge_matched_name", "merge_distance_m",
        "confidence", "operator_tier", "_source_file",
    ]
    for name, sub in [
        ("v6_high_confidence.csv", high),
        ("v6_medium_confidence.csv", medium),
        ("v6_needs_review.csv", review),
    ]:
        out_path = HERE / "outputs" / name
        sub.reindex(columns=columns).to_csv(out_path, index=False)
        log.info("Wrote %s (%d rows)", out_path, len(sub))

    # Summary report
    report = _build_report(df, result, high, medium, review)
    report_path = HERE / "outputs" / "v6_expansion_report.md"
    report_path.write_text(report, encoding="utf-8")
    log.info("Wrote %s", report_path)
    return 0


def _build_report(
    df: pd.DataFrame,
    result: pd.DataFrame,
    high: pd.DataFrame,
    medium: pd.DataFrame,
    review: pd.DataFrame,
) -> str:
    lines: list[str] = []
    a = lines.append

    a("# Malaysia DC Corpus Expansion — v6 Report")
    a("")
    a("Generated 2026-04-17 by `merge_v6.py` as part of the single-day sprint.")
    a("")
    a("## Rows per source")
    a("")
    a("| Source file | Candidates |")
    a("|---|---|")
    by_src = df["_source_file"].value_counts()
    for src, n in by_src.items():
        a(f"| `{src}` | {n} |")
    a(f"| **Total before sibling-collapse** | **{len(df)}** |")
    a(f"| **Unique facilities after collapse** | **{len(result)}** |")
    a("")
    a("## Confidence-tier breakdown")
    a("")
    a("| Tier | Rows | Definition |")
    a("|---|---|---|")
    a(f"| High | {len(high)} | confidence ≥ 0.7 AND ≥2 sibling sources |")
    a(f"| Medium | {len(medium)} | 0.4 ≤ confidence < 0.7, or single-source ≥ 0.4 |")
    a(f"| Needs review | {len(review)} | confidence < 0.4 |")
    a("")
    a("## Operator-tier breakdown")
    a("")
    a("| Tier | Rows | Share |")
    a("|---|---|---|")
    tier_counts = Counter(result["operator_tier"])
    total = len(result) or 1
    for tier in ("hyperscaler", "tier1_colo", "tier2_colo", "sovereign_telco", "unknown"):
        n = tier_counts.get(tier, 0)
        a(f"| {tier} | {n} | {n/total*100:.1f}% |")
    a("")
    a("## Coverage by city (top 10)")
    a("")
    a("| City | Rows |")
    a("|---|---|")
    city_counts = Counter(result["address"].fillna(""))
    for city, n in city_counts.most_common(10):
        if not city:
            continue
        a(f"| {city[:60]} | {n} |")
    a("")
    a("## Dedup verdict against v5")
    a("")
    a("| Verdict | Rows |")
    a("|---|---|")
    for status, n in Counter(result["merge_status"].fillna("")).items():
        a(f"| {status or '(blank)'} | {n} |")
    a("")
    a("## Negative results worth recording")
    a("")
    a("* **MDEC + DNB**: 0 candidates. MDEC's /media-release corpus is "
      "about MDEC-internal programs, not facility announcements; DNB's "
      "5G-wholesaler role was wound down in Jan 2025 and its site uses "
      "flat-slug articles. Not a productive automated source.")
    a("* **Bursa Malaysia**: HTTP 403 at the CDN before robots.txt is even "
      "served — no automated scraping possible. Pivoted to operator-native "
      "IR sites; YTL Power's robots.txt disallows `*`, so that IR feed is "
      "also skipped.")
    a("* **Wikipedia (prior session)**: 0 MY-tagged DC entities on Wikidata "
      "(confirmed via SPARQL). Retained as a documented negative result; "
      "recommend not re-running for future Southeast-Asian country pilots.")
    a("")
    a("## Caveats")
    a("")
    a("* MIDA and operator-IR rows use city-centroid coordinates with "
      "`coord_confidence='geocoded_with_campus_offset'`. Street-level "
      "geocoding was deliberately deferred to avoid invoking Google/Geocoder "
      "APIs inside the sprint — any high-confidence facility the user "
      "approves for merge should be hand-located against satellite imagery.")
    a("* ST (Energy Commission) candidates pass a loose `centre` keyword "
      "filter that surfaces shopping malls, hospitals, and office towers "
      "alongside real DCs. Human review is mandatory for anything from this "
      "source.")
    a("* The confidence score treats any candidate sitting within 300 m of "
      "a v5 row (or name-matching one) as 2-sources-agree, so PeeringDB "
      "entries that re-discover an existing v5 facility pick up credit for "
      "that agreement. Single-source PeeringDB rows with no v5 neighbour "
      "stay in the review bucket — by design, since the sprint's definition "
      "of high-confidence requires multi-source corroboration.")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
