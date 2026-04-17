"""
postprocess_v5.py — "easy wins" cleanup pass on the v5 corpus.

Three in-place fixes, none of which add rows from any external source. The
output is a cleaned-up v5.1 CSV that is a drop-in replacement for v5 in the
downstream Task 7 merge.

Fix 1 — OSM unnamed-way attribution / clustering
    v5 contains 15 rows whose names are `Unnamed facility (OSM way N)`.
    These are polygons pulled from OpenStreetMap without names. For each
    such row:
      (a) find the nearest *named* v5 row within 500 m. If one exists,
          attribute the OSM way to that operator (drop the placeholder row,
          append the OSM way id to the named row's note as provenance), or
      (b) otherwise, cluster the unassigned OSM rows by 500 m proximity
          using a naive single-link algorithm and emit one representative
          row per cluster named `OSM unnamed cluster near (lat, lon)`.

Fix 2 — Intra-corpus near-duplicate merge
    Group rows by `(operator_norm, name_root)` where `name_root` strips
    trailing building/phase numbers, "Data Centre", "Campus", and
    directional qualifiers. Within each group, merge rows that sit within
    1500 m of each other. Keeps the row with the most complete metadata;
    concatenates differing coord provenance into the survivor's note.

Fix 3 — Flag hyperscale cloud-region pins
    Rows with facility_type = "cloud_region" are logical region centroids,
    not physical facilities. They can't support land-transformation
    analysis (the parent project's downstream step). Set a new column
    `physical_facility` to FALSE on those rows and add a note that AZ-site
    discovery is deferred to a future enrichment pass.

Run
---
    python postprocess_v5.py

Output
------
    outputs/malaysia_datacenters_v5_1.csv   cleaned corpus
    outputs/postprocess_report.md           summary of changes
    logs/postprocess.log
"""

from __future__ import annotations

import csv
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import pandas as pd  # noqa: E402

from mida_scraper import configure_logging  # noqa: E402
from scrape_common import haversine_m, normalize_name  # noqa: E402

ATTRIBUTION_RADIUS_M = 500.0
CLUSTER_RADIUS_M = 500.0
DEDUP_RADIUS_M = 1500.0
# Coord-only pass: same operator sitting within this radius is treated as the
# same facility even if the names don't share a root. 150 m is tight enough
# to avoid collapsing genuinely separate buildings inside a single business
# park (Bridge MY02/MY03, STT Johor 1/2, etc. are kilometres apart) but loose
# enough to catch 30 m–100 m coord drift on re-entered rows.
SAME_OPERATOR_MERGE_RADIUS_M = 150.0


# ---------------------------------------------------------------------------
# Fix 1: OSM unnamed-way attribution / clustering
# ---------------------------------------------------------------------------

def _is_unnamed_osm(row: dict) -> bool:
    name = str(row.get("name") or "")
    return name.startswith("Unnamed facility (OSM way")


def _osm_way_id(row: dict) -> str | None:
    m = re.search(r"OSM way (\d+)", str(row.get("name") or ""))
    return m.group(1) if m else None


def _float(value) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    import math
    return None if math.isnan(f) else f


def attribute_or_cluster_osm(rows: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """
    Return (surviving_rows, dropped_unnamed_rows, stats).

    Named rows receive appended provenance in their `note` for any OSM way
    attributed to them. Unassigned OSM ways are clustered into aggregate
    rows.
    """
    named: list[dict] = [r for r in rows if not _is_unnamed_osm(r)]
    unnamed: list[dict] = [r for r in rows if _is_unnamed_osm(r)]

    stats = {"total_unnamed": len(unnamed), "attributed": 0, "clustered": 0,
             "aggregate_rows": 0}

    # Phase A — attribution
    unattributed: list[dict] = []
    for u in unnamed:
        u_lat = _float(u.get("lat"))
        u_lon = _float(u.get("lon"))
        if u_lat is None or u_lon is None:
            unattributed.append(u)
            continue

        best_named = None
        best_dist = ATTRIBUTION_RADIUS_M + 1.0
        for n in named:
            n_lat = _float(n.get("lat"))
            n_lon = _float(n.get("lon"))
            if n_lat is None or n_lon is None:
                continue
            d = haversine_m(u_lat, u_lon, n_lat, n_lon)
            if d <= ATTRIBUTION_RADIUS_M and d < best_dist:
                best_dist = d
                best_named = n
        if best_named is not None:
            way_id = _osm_way_id(u) or ""
            existing_note = str(best_named.get("note") or "")
            new_frag = f"osm_way={way_id}@{best_dist:.0f}m"
            best_named["note"] = (existing_note + " | " + new_frag).strip(" |")
            # Bump n_sources: this OSM polygon is additional independent
            # corroboration of the named row's footprint.
            try:
                best_named["n_sources"] = int(float(best_named.get("n_sources") or 1)) + 1
            except (TypeError, ValueError):
                best_named["n_sources"] = 2
            stats["attributed"] += 1
        else:
            unattributed.append(u)

    # Phase B — cluster unassigned OSM rows by 500 m single-link
    clusters: list[list[dict]] = []
    for u in unattributed:
        u_lat = _float(u.get("lat"))
        u_lon = _float(u.get("lon"))
        placed = False
        if u_lat is not None and u_lon is not None:
            for c in clusters:
                for other in c:
                    o_lat = _float(other.get("lat"))
                    o_lon = _float(other.get("lon"))
                    if o_lat is None or o_lon is None:
                        continue
                    if haversine_m(u_lat, u_lon, o_lat, o_lon) <= CLUSTER_RADIUS_M:
                        c.append(u)
                        placed = True
                        break
                if placed:
                    break
        if not placed:
            clusters.append([u])

    aggregate_rows: list[dict] = []
    for c in clusters:
        lats = [_float(r.get("lat")) for r in c if _float(r.get("lat")) is not None]
        lons = [_float(r.get("lon")) for r in c if _float(r.get("lon")) is not None]
        if not lats or not lons:
            continue
        mean_lat = sum(lats) / len(lats)
        mean_lon = sum(lons) / len(lons)
        way_ids = sorted(_osm_way_id(r) or "" for r in c)
        seed = dict(c[0])
        seed["name"] = (
            f"OSM unnamed cluster ({len(c)} ways) near "
            f"({mean_lat:.4f}, {mean_lon:.4f})"
        )
        seed["operator"] = ""
        seed["operator_norm"] = ""
        seed["lat"] = round(mean_lat, 6)
        seed["lon"] = round(mean_lon, 6)
        seed["n_sources"] = 1
        seed["coord_confidence"] = "source_native"
        seed["name_normalized"] = normalize_name(seed["name"])
        seed["note"] = (
            (str(seed.get("note") or "") + " | " if seed.get("note") else "")
            + f"osm_ways={','.join(way_ids)}"
        ).strip(" |")
        aggregate_rows.append(seed)
        stats["clustered"] += len(c)
    stats["aggregate_rows"] = len(aggregate_rows)

    return named + aggregate_rows, unnamed, stats


# ---------------------------------------------------------------------------
# Fix 2: intra-corpus near-duplicate merge
# ---------------------------------------------------------------------------

_NAME_ROOT_STRIP_RE = re.compile(
    r"\b("
    r"data\s*cent(?:er|re)|campus|"
    r"phase\s*\d+|building\s*\d+|block\s*\d+|"
    r"north|south|east|west|"
    r"\d+[a-z]?"  # trailing building numbers like 1, 2, 2A
    r")\b",
    re.IGNORECASE,
)


def name_root(name: str) -> str:
    n = normalize_name(name)
    # strip parenthetical city suffixes like "(Johor)"
    n = re.sub(r"\s*\([^)]*\)\s*", " ", n)
    n = _NAME_ROOT_STRIP_RE.sub(" ", n)
    n = " ".join(n.split())
    return n


# Pattern for building / phase identifiers inside a facility name:
#   "JH1", "JH2", "NTP1", "KL1", "CBJ6", "My01", "MY02", etc., and plain
#   "Data Center 2", "Building 3", "Phase 1", "Block 2A".
# We extract the *last* token matching the alphanumeric-suffix pattern so
# "Princeton Digital Group JH1" → "jh1" but "Princeton Digital JH1 Campus"
# also → "jh1" (the "Campus" suffix doesn't contain a digit).
_BUILDING_ID_RE = re.compile(
    r"\b("
    r"[A-Z]{2,4}\d{1,3}[A-Z]?"              # JH1, CBJ6, MY02, NTP2A
    r"|MY\d{2,3}|my\d{2,3}"                  # My01, MY02 (BDC-style)
    r"|(?:building|block|phase|dc|data\s*centre?)\s*\d+[a-z]?"
    r")\b",
    re.IGNORECASE,
)

_GENERIC_NUMBER_RE = re.compile(
    r"\b(\d{1,2}[a-z]?)\b"
)


def building_id(name: str) -> str | None:
    """
    Extract a canonical building/phase identifier from a facility name.
    Returns lowercase string (e.g. 'jh1', 'dc2', 'building3', '7') or None
    if no identifier can be found.
    """
    matches = _BUILDING_ID_RE.findall(name)
    if matches:
        last = matches[-1]
        # Collapse internal whitespace: "data centre 2" → "datacentre2"
        return re.sub(r"\s+", "", last).lower()
    # Fall back to trailing generic number if the name ends with one
    gm = _GENERIC_NUMBER_RE.findall(name)
    if gm:
        return gm[-1].lower()
    return None


def intra_dedup(rows: list[dict]) -> tuple[list[dict], list[dict], dict]:
    stats = {"merges": 0, "rows_in_merges": 0}
    # Key is (operator_norm_lower, name_root, building_id_or_empty). Including
    # building_id here stops "YTL Johor Data Center 1" and "YTL Johor Data
    # Center 2" from clustering together (same name_root but building_ids 1
    # and 2 are distinct). If neither row has a building_id the bucket is
    # keyed by name_root alone.
    groups: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for i, r in enumerate(rows):
        op = str(r.get("operator_norm") or r.get("operator") or "").lower().strip()
        root = name_root(str(r.get("name") or ""))
        if not op or not root:
            continue
        bid = building_id(str(r.get("name") or "")) or ""
        groups[(op, root, bid)].append(i)

    drop_ids: set[int] = set()
    merged_notes: dict[int, list[str]] = defaultdict(list)
    for key, idxs in groups.items():
        if len(idxs) < 2:
            continue
        # Single-link cluster within the group
        visited = [False] * len(idxs)
        clusters = []
        for i_off in range(len(idxs)):
            if visited[i_off]:
                continue
            stack = [i_off]
            current = []
            while stack:
                k = stack.pop()
                if visited[k]:
                    continue
                visited[k] = True
                current.append(idxs[k])
                for m_off in range(len(idxs)):
                    if visited[m_off]:
                        continue
                    a_lat = _float(rows[idxs[k]].get("lat"))
                    a_lon = _float(rows[idxs[k]].get("lon"))
                    b_lat = _float(rows[idxs[m_off]].get("lat"))
                    b_lon = _float(rows[idxs[m_off]].get("lon"))
                    if None in (a_lat, a_lon, b_lat, b_lon):
                        continue
                    if haversine_m(a_lat, a_lon, b_lat, b_lon) <= DEDUP_RADIUS_M:
                        stack.append(m_off)
            if len(current) > 1:
                clusters.append(current)

        for cluster in clusters:
            # Keeper = row with the most non-empty columns as a proxy for
            # metadata completeness.
            def completeness(idx: int) -> int:
                return sum(
                    1 for v in rows[idx].values()
                    if str(v).strip() not in ("", "NA", "nan", "None")
                )
            keeper = max(cluster, key=completeness)
            losers = [i for i in cluster if i != keeper]
            for l in losers:
                drop_ids.add(l)
                # Carry forward any OSM-way attribution the loser had picked
                # up in Fix 1 so provenance survives the merge.
                loser_note = str(rows[l].get("note") or "")
                osm_frags = [p for p in re.split(r"\s*\|\s*|\s*\|\|\s*", loser_note)
                             if p.startswith("osm_way=") or p.startswith("osm_ways=")]
                frag = (
                    f"merged_from={rows[l].get('name')!r}@"
                    f"({rows[l].get('lat')},{rows[l].get('lon')}) "
                    f"src={rows[l].get('source_category') or ''}"
                )
                if osm_frags:
                    frag += " [carries " + "; ".join(osm_frags) + "]"
                merged_notes[keeper].append(frag)
                # Bump n_sources to reflect the independent re-discovery
                try:
                    rows[keeper]["n_sources"] = int(float(rows[keeper].get("n_sources") or 1)) + 1
                except (TypeError, ValueError):
                    rows[keeper]["n_sources"] = 2
            stats["merges"] += 1
            stats["rows_in_merges"] += len(cluster)

    for kid, frags in merged_notes.items():
        rows[kid]["note"] = (
            (str(rows[kid].get("note") or "") + " || ") if rows[kid].get("note") else ""
        ) + " | ".join(frags)

    # Second pass: same operator + **same building-id** + within
    # SAME_OPERATOR_MERGE_RADIUS_M. This catches the "Princeton Digital Group
    # JH1" vs "Princeton Digital JH1 Campus" case (both have building_id
    # 'jh1'), but importantly does *not* collapse YTL Johor 1/2/3 or GDS
    # NTP1/2/3 — those have different building_ids even though they sit within
    # 100–200 m of each other.
    op_groups: dict[tuple[str, str], list[int]] = defaultdict(list)
    for i, r in enumerate(rows):
        if i in drop_ids:
            continue
        op = str(r.get("operator_norm") or r.get("operator") or "").lower().strip()
        if not op or op.startswith("unnamed facility"):
            continue
        bid = building_id(str(r.get("name") or "")) or "__none__"
        op_groups[(op, bid)].append(i)

    for (op, bid), idxs in op_groups.items():
        if len(idxs) < 2:
            continue
        # Skip the "__none__" bucket: if neither row has a building id we have
        # no positive signal that they're the same facility (could be two
        # unrelated buildings of the same operator sharing a coarse address).
        # The earlier name_root pass handles the "both have the same name
        # modulo trivia" case.
        if bid == "__none__":
            continue
        visited = [False] * len(idxs)
        for i_off in range(len(idxs)):
            if visited[i_off]:
                continue
            stack = [i_off]
            current: list[int] = []
            while stack:
                k = stack.pop()
                if visited[k]:
                    continue
                visited[k] = True
                current.append(idxs[k])
                a_lat = _float(rows[idxs[k]].get("lat"))
                a_lon = _float(rows[idxs[k]].get("lon"))
                if a_lat is None or a_lon is None:
                    continue
                for m_off in range(len(idxs)):
                    if visited[m_off]:
                        continue
                    b_lat = _float(rows[idxs[m_off]].get("lat"))
                    b_lon = _float(rows[idxs[m_off]].get("lon"))
                    if b_lat is None or b_lon is None:
                        continue
                    if haversine_m(a_lat, a_lon, b_lat, b_lon) <= SAME_OPERATOR_MERGE_RADIUS_M:
                        stack.append(m_off)
            if len(current) < 2:
                continue

            def completeness(idx: int) -> int:
                return sum(
                    1 for v in rows[idx].values()
                    if str(v).strip() not in ("", "NA", "nan", "None")
                )
            keeper = max(current, key=completeness)
            losers = [i for i in current if i != keeper]
            frags = []
            for l in losers:
                drop_ids.add(l)
                loser_note = str(rows[l].get("note") or "")
                osm_frags = [p for p in re.split(r"\s*\|\s*|\s*\|\|\s*", loser_note)
                             if p.startswith("osm_way=") or p.startswith("osm_ways=")]
                frag = (
                    f"merged_from={rows[l].get('name')!r}@"
                    f"({rows[l].get('lat')},{rows[l].get('lon')}) "
                    f"src={rows[l].get('source_category') or ''} "
                    f"[same-op coord-tight]"
                )
                if osm_frags:
                    frag += " [carries " + "; ".join(osm_frags) + "]"
                frags.append(frag)
                try:
                    rows[keeper]["n_sources"] = int(float(rows[keeper].get("n_sources") or 1)) + 1
                except (TypeError, ValueError):
                    rows[keeper]["n_sources"] = 2
            if frags:
                rows[keeper]["note"] = (
                    (str(rows[keeper].get("note") or "") + " || ") if rows[keeper].get("note") else ""
                ) + " | ".join(frags)
            stats["merges"] += 1
            stats["rows_in_merges"] += len(current)

    surviving = [r for i, r in enumerate(rows) if i not in drop_ids]
    dropped = [r for i, r in enumerate(rows) if i in drop_ids]
    return surviving, dropped, stats


# ---------------------------------------------------------------------------
# Fix 3: flag cloud-region pins
# ---------------------------------------------------------------------------

# Public disclosures (AWS, Microsoft, Google, Alibaba, Oracle docs — *not*
# from the master). Used only in the explanatory note; we do not expand the
# region into AZ placeholder rows because that would create multiple pins
# with identical coordinates, which is worse than a single labelled pin.
REGION_AZ_COUNTS: dict[str, int] = {
    "aws": 3,       # ap-southeast-5 — 3 AZs per AWS Malaysia launch release
    "azure": 3,     # Malaysia West — 3 AZs per MS Build 2024 disclosure
    "gcp": 3,       # asia-southeast3 — 3 zones per GCP docs
    "alibaba": 3,   # ap-southeast-3 — 3 zones per Alibaba Cloud docs
    "oracle": 1,    # MY region — 1 public AD disclosed
}


def flag_cloud_regions(rows: list[dict]) -> dict:
    stats = {"flagged": 0}
    for r in rows:
        if str(r.get("facility_type") or "").strip() == "cloud_region":
            r["physical_facility"] = False
            key = str(r.get("operator_norm") or r.get("operator") or "").lower().strip()
            az_n = REGION_AZ_COUNTS.get(key, "?")
            existing = str(r.get("note") or "")
            frag = (
                f"logical_region; {az_n} AZ(s) publicly disclosed; "
                f"physical AZ-site discovery deferred (needs trade-press enrichment)"
            )
            r["note"] = (existing + " | " if existing else "") + frag
            stats["flagged"] += 1
        else:
            # Keep the column consistent across the whole DataFrame
            r.setdefault("physical_facility", True)
    return stats


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    configure_logging(HERE / "logs" / "postprocess.log")
    log = logging.getLogger(__name__)

    in_path = HERE.parent / "outputs" / "malaysia_datacenters_v5.csv"
    df = pd.read_csv(in_path)
    # Normalize NaN to empty string for safe string ops
    rows = df.fillna("").to_dict(orient="records")
    log.info("Loaded %d v5 rows", len(rows))

    # Fix 3 first so flagged cloud-region rows aren't targets for Fix 1/2
    # attribution logic (they have trivially shared coords at KL centroid).
    region_stats = flag_cloud_regions(rows)
    log.info("Fix 3: flagged %d cloud-region rows", region_stats["flagged"])

    rows_after_1, _dropped_osm, osm_stats = attribute_or_cluster_osm(rows)
    log.info(
        "Fix 1: %d unnamed OSM rows → attributed=%d, clustered-into=%d aggregate rows",
        osm_stats["total_unnamed"], osm_stats["attributed"], osm_stats["aggregate_rows"],
    )

    rows_after_2, _dropped_dup, dedup_stats = intra_dedup(rows_after_1)
    log.info(
        "Fix 2: %d near-duplicate merges collapsing %d rows",
        dedup_stats["merges"], dedup_stats["rows_in_merges"],
    )

    out_path = HERE / "outputs" / "malaysia_datacenters_v5_1.csv"
    out_df = pd.DataFrame(rows_after_2)
    # Preserve the original column order then append new columns
    original_cols = list(df.columns)
    extra_cols = [c for c in out_df.columns if c not in original_cols]
    out_df = out_df.reindex(columns=original_cols + extra_cols)
    out_df.to_csv(out_path, index=False)
    log.info(
        "Wrote %s: %d rows (was %d in v5)",
        out_path, len(out_df), len(df),
    )

    # Report
    report = [
        "# v5 → v5.1 post-process report",
        "",
        f"Generated 2026-04-17 by `postprocess_v5.py`. Input: {in_path.name} "
        f"({len(df)} rows). Output: {out_path.name} ({len(out_df)} rows).",
        "",
        "## Fix 1 — OSM unnamed-way attribution / clustering",
        "",
        f"* Unnamed OSM rows in v5: **{osm_stats['total_unnamed']}**",
        f"* Attributed to a named v5 facility within {ATTRIBUTION_RADIUS_M:.0f} m: "
        f"**{osm_stats['attributed']}** (OSM way IDs appended to that row's note)",
        f"* Clustered into aggregate rows (no named neighbour within radius): "
        f"**{osm_stats['clustered']}** OSM ways → **{osm_stats['aggregate_rows']}** aggregate rows",
        "",
        "## Fix 2 — intra-corpus near-duplicate merge",
        "",
        f"* `(operator_norm, name_root)` merges within {DEDUP_RADIUS_M:.0f} m: **{dedup_stats['merges']}**",
        f"* Rows collapsed by those merges: **{dedup_stats['rows_in_merges']}** "
        f"(net −{dedup_stats['rows_in_merges'] - dedup_stats['merges']} rows)",
        "",
        "## Fix 3 — cloud-region pin flagging",
        "",
        f"* Rows with `facility_type='cloud_region'`: **{region_stats['flagged']}**",
        "* Each received `physical_facility=FALSE` plus a note documenting the "
        "publicly-disclosed AZ count. Physical AZ-site discovery is deferred; "
        "no placeholder expansion was performed because duplicating the region "
        "centroid N times would fabricate geographic signal without adding "
        "any.",
        "",
        "## Net effect",
        "",
        f"* v5 rows: {len(df)}",
        f"* v5.1 rows: {len(out_df)} (Δ {len(out_df) - len(df):+d})",
        f"* Physical-facility rows in v5.1: "
        f"{int(out_df['physical_facility'].astype(str).str.lower().isin(['true', '1']).sum())}",
    ]
    (HERE / "outputs" / "postprocess_report.md").write_text("\n".join(report) + "\n")
    log.info("Wrote %s", HERE / "outputs" / "postprocess_report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
