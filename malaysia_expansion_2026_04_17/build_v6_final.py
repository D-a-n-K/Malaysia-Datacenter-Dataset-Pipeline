"""
build_v6_final.py — produce the combined v6 CSV.

    malaysia_datacenters_v5_1.csv     (post-processed v5 base)
  + v6_high_confidence.csv            (4 MIDA-sourced candidates that
                                       cleared confidence ≥ 0.7 and ≥2
                                       sibling sources)
  = malaysia_datacenters_v6.csv       (final combined corpus)

Medium-confidence (0 rows) and needs-review (31 rows) tiers are deliberately
left out of the combined corpus — they live alongside as separate files for
manual triage before any future promotion.

Schema alignment
----------------
v5.1 has all v5 columns plus `physical_facility`. v6 candidates have the v5
columns plus merge/confidence metadata. We keep v5.1's schema as canonical
and append candidate-only columns at the end so nothing is lost.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
V51_PATH = HERE / "outputs" / "malaysia_datacenters_v5_1.csv"
HIGH_PATH = HERE / "outputs" / "v6_high_confidence.csv"
OUT_PATH = HERE / "outputs" / "malaysia_datacenters_v6.csv"


def main() -> int:
    v51 = pd.read_csv(V51_PATH)
    high = pd.read_csv(HIGH_PATH)

    # Default physical_facility = True on candidates (none were cloud-region)
    if "physical_facility" not in high.columns:
        high["physical_facility"] = True

    # Tag provenance so downstream consumers can tell v5.1 rows from v6 adds
    v51["v6_layer"] = "v5.1"
    high["v6_layer"] = "v6_candidate_high"

    columns = list(v51.columns) + [c for c in high.columns if c not in v51.columns]
    combined = pd.concat(
        [v51.reindex(columns=columns), high.reindex(columns=columns)],
        ignore_index=True,
    )

    combined.to_csv(OUT_PATH, index=False)
    print(
        f"Wrote {OUT_PATH.name}: {len(combined)} rows "
        f"(v5.1={len(v51)}, v6 high-confidence adds={len(high)})"
    )
    print(
        f"Physical-facility rows: "
        f"{int(combined['physical_facility'].astype(str).str.lower().isin(['true', '1']).sum())}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
