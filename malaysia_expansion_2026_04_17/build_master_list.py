"""
build_master_list.py — produce ONE combined master CSV for manual triage.

Concatenates:
    v5.1 cleaned base         (66 rows, post-processed v5)
  + v6 high-confidence tier   (4 rows)
  + v6 medium-confidence tier (0 rows)
  + v6 needs-review tier      (31 rows)

and writes `outputs/malaysia_datacenters_v6_master.csv`. Every row gets:

    v6_layer          v5.1 | v6_high | v6_medium | v6_review
    confidence        float 0–1, computed consistently across all rows
    promotion_note    human-readable explanation of the confidence level
                      and what's blocking auto-promotion (if anything)
    promotion_action  EMPTY — reserved for the reviewer to fill in
                      ("merge", "reject", "needs_verification", etc.)

Design notes
------------
* v5.1 rows are re-scored with the same sibling-aware logic used for the
  candidate tiers so the confidence column is directly comparable across
  layers. Prior v5 confidence scoring (malaysia_datacenters_v5_scored.csv)
  was merged-mode only; this pass uses sibling mode with the full 101-row
  pool as the sibling set.
* `promotion_note` is generated, not free-form — the reviewer overrides via
  the promotion_action column.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from confidence_score import (  # noqa: E402
    geocode_precision_score,
    source_agreement_score,
    source_count_score,
)
from scrape_common import haversine_m, normalize_name  # noqa: E402

V51_PATH = HERE / "outputs" / "malaysia_datacenters_v5_1.csv"
HIGH_PATH = HERE / "outputs" / "v6_high_confidence.csv"
MEDIUM_PATH = HERE / "outputs" / "v6_medium_confidence.csv"
REVIEW_PATH = HERE / "outputs" / "v6_needs_review.csv"
OUT_PATH = HERE / "outputs" / "malaysia_datacenters_v6_master.csv"


def _float(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    import math
    return None if math.isnan(f) else f


def build_sibling_index(rows: list[dict], radius_m: float = 300.0) -> list[list[int]]:
    """Return idx → list of sibling indices (coord ≤ radius OR name-normalized match)."""
    out: list[list[int]] = [[] for _ in range(len(rows))]
    coords = []
    names = []
    for r in rows:
        coords.append((_float(r.get("lat")), _float(r.get("lon"))))
        n = r.get("name_normalized") or normalize_name(str(r.get("name") or ""))
        names.append(n)
    for i in range(len(rows)):
        lat_i, lon_i = coords[i]
        for j in range(i + 1, len(rows)):
            lat_j, lon_j = coords[j]
            match = False
            if None not in (lat_i, lon_i, lat_j, lon_j):
                if haversine_m(lat_i, lon_i, lat_j, lon_j) <= radius_m:
                    match = True
            if not match and names[i] and names[j] and names[i] == names[j]:
                match = True
            if match:
                out[i].append(j)
                out[j].append(i)
    return out


def compute_confidence(row: dict, sibling_rows: list[dict]) -> float:
    """Sibling-aware confidence in [0,1], same formula as Task 1."""
    n_sources_effective = max(
        int(float(row.get("n_sources") or 1) or 1),
        1 + len(sibling_rows),
    )
    s_count = source_count_score(n_sources_effective)
    agree = source_agreement_score(row, siblings=sibling_rows) if sibling_rows \
            else source_agreement_score(row)
    s_geo = geocode_precision_score(row.get("coord_confidence"))
    return round(0.4 * s_count + 0.3 * agree + 0.3 * s_geo, 4)


def promotion_note_for(row: dict, layer: str, sibling_count: int) -> str:
    n_sources = int(float(row.get("n_sources") or 1) or 1)
    phys = str(row.get("physical_facility", True)).lower() in ("true", "1")
    src = str(row.get("source_category") or row.get("sources") or "unknown")

    if layer == "v5.1":
        if not phys:
            return (
                "Logical cloud-region pin, not a physical facility. "
                "Not eligible for land-transformation analysis; keep for reference."
            )
        if n_sources >= 2 or sibling_count >= 1:
            return (
                f"Established v5 row with {n_sources} merged source(s) "
                f"and {sibling_count} sibling(s). Safe."
            )
        return (
            f"Established v5 row, single-source ({src}). "
            "Worth a spot-check against satellite imagery before downstream use."
        )

    if layer == "v6_high":
        return (
            f"MIDA-sourced facility announcement with multi-source agreement "
            f"({sibling_count} sibling(s) in corpus). Ready to merge."
        )
    if layer == "v6_medium":
        return (
            f"Mid-confidence candidate, {n_sources} source(s). "
            "Cross-check against satellite imagery and local press."
        )
    if layer == "v6_review":
        if "PeeringDB" in src:
            return (
                "PeeringDB single-source discovery. High intrinsic quality "
                "(operator self-registered with coords) but needs a second "
                "source to auto-promote. Check against local press or OSM."
            )
        if "Energy Commission" in src:
            return (
                "Energy Commission licensee matching a DC-suggestive keyword. "
                "May be a genuine licensed DC or a false positive "
                "(shopping mall / hospital / office tower matching 'centre'). "
                "Confirm via operator lookup."
            )
        if "MIDA" in src:
            return (
                "MIDA release describing an exploratory / unbuilt project. "
                "Keep tracking; promote when a ground-breaking or operational "
                "announcement follows."
            )
        if "Gamuda" in src or "IR" in src:
            return (
                "Operator IR release referencing a DC-adjacent JV or stake, "
                "not a facility per se. Probably not promotable unless the "
                "JV produces a specific build."
            )
        return f"Single-source candidate from {src}. Manual review required."

    return "Unclassified."


def load_layer(path: Path, layer: str) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    df["v6_layer"] = layer
    return df


def main() -> int:
    v51 = load_layer(V51_PATH, "v5.1")
    high = load_layer(HIGH_PATH, "v6_high")
    medium = load_layer(MEDIUM_PATH, "v6_medium")
    review = load_layer(REVIEW_PATH, "v6_review")

    frames = [d for d in (v51, high, medium, review) if not d.empty]
    all_cols = sorted({c for d in frames for c in d.columns})
    frames = [d.reindex(columns=all_cols) for d in frames]
    combined = pd.concat(frames, ignore_index=True)

    # Default physical_facility where missing
    if "physical_facility" in combined.columns:
        combined["physical_facility"] = combined["physical_facility"].fillna(True)
    else:
        combined["physical_facility"] = True

    # Sibling-aware confidence across the whole 101-row pool
    records = combined.to_dict(orient="records")
    siblings = build_sibling_index(records)
    confidences = []
    promotion_notes = []
    for i, row in enumerate(records):
        sib_rows = [records[j] for j in siblings[i]]
        conf = compute_confidence(row, sib_rows)
        confidences.append(conf)
        promotion_notes.append(
            promotion_note_for(row, row.get("v6_layer", ""), len(sib_rows))
        )
    combined["confidence"] = confidences
    combined["promotion_note"] = promotion_notes
    combined["promotion_action"] = ""  # reviewer fills this in

    # Column order: identity first, layer/confidence/decision columns at the
    # start-right for scannability, then everything else.
    lead_cols = [
        "name", "operator", "operator_norm", "lat", "lon",
        "v6_layer", "confidence", "promotion_action", "promotion_note",
        "source_category", "n_sources", "physical_facility",
        "address", "source", "note",
    ]
    ordered = [c for c in lead_cols if c in combined.columns]
    tail = [c for c in combined.columns if c not in ordered]
    combined = combined.reindex(columns=ordered + tail)

    combined.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH.name}: {len(combined)} rows")
    print()
    print("By layer:")
    print(combined["v6_layer"].value_counts().to_string())
    print()
    print("Confidence distribution:")
    print(combined.groupby("v6_layer")["confidence"].describe().round(3).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
