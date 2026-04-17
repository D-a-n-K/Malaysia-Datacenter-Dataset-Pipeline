"""
confidence_score.py — per-row confidence for the Malaysia data-center corpus.

Formal scoring model approved 2026-04-17 for the country-pilot extension of the
Data-Centers-GIS-Land-Transformation project.

        confidence = 0.4 * source_count
                   + 0.3 * source_agreement
                   + 0.3 * geocode_precision

Each component is a float in [0, 1]. The output is likewise in [0, 1].

Weights
-------
**Source count (0.4)** — single largest weight because cross-source corroboration
is the most direct signal that a facility is real and not a marketing
placeholder. `min(n_sources / 3, 1.0)` saturates at three independent sources:
beyond three we hit diminishing returns and the remaining weight should come
from agreement and geocode quality.

**Source agreement (0.3)** — if 2+ sources agree on coordinates within 300 m
(≈ the footprint of a single hyperscale campus), they are independently
describing the same physical site. Name-only agreement is weaker (operators
reuse facility names across cities) so it gets half weight. Single-source rows
have nothing to agree with, so they score 0.

**Geocode precision (0.3)** — downstream land-transformation analysis crops
Landsat/Sentinel tiles around each coordinate. A 300 m geocoding error
translates directly into a misattributed pre-development land cover, so coordinate
provenance carries real weight even though it is only one facet of trust.

Mapping for `coord_confidence` values observed in v5:

    manual_correction             → 1.0   (human-verified against imagery)
    geocoded                      → 0.8   (single-address geocode, street-level)
    geocoded_with_campus_offset   → 0.6   (geocode plus heuristic campus offset)
    source_native                 → 0.5   (source supplied coords, provenance unclear)
    anything else / missing       → 0.3

CLI
---
    python confidence_score.py INPUT.csv OUTPUT.csv

Reads a v5-schema CSV, adds a `confidence` column, and writes OUTPUT.csv.

Tests
-----
    python confidence_score.py --test

Runs three synthetic rows exercising high / mid / low confidence.
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pandas as pd


COORD_PRECISION_MAP: dict[str, float] = {
    "manual_correction": 1.0,
    "geocoded": 0.8,
    "geocoded_with_campus_offset": 0.6,
    "source_native": 0.5,
}
_DEFAULT_COORD_PRECISION = 0.3

_EARTH_RADIUS_M = 6_371_000.0
AGREEMENT_RADIUS_M = 300.0


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def _as_float(value) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def source_count_score(n_sources) -> float:
    n = _as_float(n_sources) or 0.0
    return max(0.0, min(n / 3.0, 1.0))


def source_agreement_score(
    row: Mapping,
    siblings: Sequence[Mapping] | None = None,
) -> float:
    """
    Score cross-source agreement for a single facility.

    Two modes:

    1. **Candidate mode.** When `siblings` (other candidate rows describing the
       same facility, e.g. during Task 7 merge) are supplied, compute real
       agreement: 1.0 if any sibling sits within AGREEMENT_RADIUS_M of `row`,
       else 0.5 if a normalized name matches, else 0.0.

    2. **Merged mode.** When no siblings are supplied, infer from a row that
       has already been merged across sources. `n_sources >= 2` with a
       precise `coord_confidence` (manual_correction / geocoded) implies the
       merge survived a coord check → 1.0. `n_sources >= 2` with a looser
       coord confidence implies only name-level agreement → 0.5. Single-source
       rows → 0.0.
    """
    n = _as_float(row.get("n_sources")) or 0.0

    if siblings:
        lat = _as_float(row.get("lat"))
        lon = _as_float(row.get("lon"))
        name = str(row.get("name_normalized") or row.get("name") or "").strip().lower()
        coord_match = False
        name_match = False
        for sib in siblings:
            slat = _as_float(sib.get("lat"))
            slon = _as_float(sib.get("lon"))
            if lat is not None and lon is not None and slat is not None and slon is not None:
                if _haversine_m(lat, lon, slat, slon) <= AGREEMENT_RADIUS_M:
                    coord_match = True
                    break
            sname = str(sib.get("name_normalized") or sib.get("name") or "").strip().lower()
            if name and sname and name == sname:
                name_match = True
        if coord_match:
            return 1.0
        if name_match:
            return 0.5
        return 0.0

    if n < 2:
        return 0.0
    coord_conf = str(row.get("coord_confidence") or "").strip()
    if coord_conf in {"manual_correction", "geocoded"}:
        return 1.0
    return 0.5


def geocode_precision_score(coord_confidence) -> float:
    key = str(coord_confidence or "").strip()
    return COORD_PRECISION_MAP.get(key, _DEFAULT_COORD_PRECISION)


def score_row(row: Mapping, siblings: Sequence[Mapping] | None = None) -> float:
    s_count = source_count_score(row.get("n_sources"))
    s_agree = source_agreement_score(row, siblings=siblings)
    s_geo = geocode_precision_score(row.get("coord_confidence"))
    return round(0.4 * s_count + 0.3 * s_agree + 0.3 * s_geo, 4)


def score_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["confidence"] = [score_row(row) for row in out.to_dict(orient="records")]
    return out


def _run_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Score Malaysia DC rows.")
    parser.add_argument("input", nargs="?", help="Input CSV (v5 schema).")
    parser.add_argument("output", nargs="?", help="Output CSV path.")
    parser.add_argument("--test", action="store_true", help="Run unit tests and exit.")
    args = parser.parse_args(argv)

    if args.test:
        return _run_tests()

    if not args.input or not args.output:
        parser.error("INPUT and OUTPUT required unless --test is passed.")

    in_path, out_path = Path(args.input), Path(args.output)
    df = pd.read_csv(in_path)
    scored = score_dataframe(df)
    scored.to_csv(out_path, index=False)
    print(
        f"Scored {len(scored)} rows → {out_path}. "
        f"mean={scored['confidence'].mean():.3f} "
        f"min={scored['confidence'].min():.3f} max={scored['confidence'].max():.3f}"
    )
    return 0


def _run_tests() -> int:
    high = {
        "n_sources": 3,
        "coord_confidence": "manual_correction",
        "lat": 3.14,
        "lon": 101.69,
        "name_normalized": "foo",
    }
    mid = {
        "n_sources": 2,
        "coord_confidence": "geocoded_with_campus_offset",
        "lat": 3.14,
        "lon": 101.69,
        "name_normalized": "bar",
    }
    low = {
        "n_sources": 1,
        "coord_confidence": "source_native",
        "lat": 3.14,
        "lon": 101.69,
        "name_normalized": "baz",
    }
    hs = score_row(high)
    ms = score_row(mid)
    ls = score_row(low)
    # high: 0.4*1 + 0.3*1 + 0.3*1 = 1.00
    # mid : 0.4*(2/3) + 0.3*0.5 + 0.3*0.6 = 0.2667 + 0.15 + 0.18 = 0.5967
    # low : 0.4*(1/3) + 0.3*0   + 0.3*0.5 = 0.1333 + 0       + 0.15 = 0.2833
    assert hs == 1.0, f"high={hs}"
    assert abs(ms - 0.5967) < 1e-3, f"mid={ms}"
    assert abs(ls - 0.2833) < 1e-3, f"low={ls}"

    # sibling mode: coord match within 300m → agreement 1.0 regardless of merged inference.
    near = {"lat": 3.1402, "lon": 101.6902, "name_normalized": "other"}
    assert source_agreement_score(low, siblings=[near]) == 1.0

    # sibling mode: name-only match → 0.5
    far_same_name = {"lat": 4.0, "lon": 102.0, "name_normalized": "baz"}
    assert source_agreement_score(low, siblings=[far_same_name]) == 0.5

    print("OK: 3 synthetic rows + 2 sibling-mode assertions passed.")
    return 0


if __name__ == "__main__":
    sys.exit(_run_cli(sys.argv[1:]))
