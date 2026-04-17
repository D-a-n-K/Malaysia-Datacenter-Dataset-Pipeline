"""
build_dedup_fixes.py — produce v6_dedup_fixes.csv addressing the five known
duplicate cases in v6_master.

Each row describes one proposed fix with a recommended action
(merge / update_coords / review) and evidence for the judgment call.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
V6_PATH = HERE.parent / "outputs" / "malaysia_datacenters_v6_master.csv"
OUT = HERE / "outputs" / "v6_dedup_fixes.csv"


def find_row_idx(v6: pd.DataFrame, name_substring: str, layer: str | None = None,
                 lat_near: float | None = None) -> list[int]:
    mask = v6["name"].fillna("").str.contains(name_substring, case=False, regex=False)
    if layer is not None:
        mask &= v6["v6_layer"] == layer
    if lat_near is not None:
        mask &= v6["lat"].astype(float).between(lat_near - 0.01, lat_near + 0.01)
    return v6[mask].index.tolist()


def main() -> int:
    v6 = pd.read_csv(V6_PATH)

    fixes: list[dict] = []

    # ─── Case 1: Keppel DC Johor 1 ──────────────────────────────────────
    keepers = find_row_idx(v6, "Keppel DC Johor 1 (Kulai)", layer="v5.1")
    losers = find_row_idx(v6, "Keppel Data Centres | Keppel DC Johor 1", layer="v6_review")
    if keepers and losers:
        fixes.append({
            "action": "merge",
            "keep_row_idx": keepers[0],
            "keep_row_name": v6.loc[keepers[0], "name"],
            "drop_row_idx": losers[0],
            "drop_row_name": v6.loc[losers[0], "name"],
            "rationale": (
                "v5.1 row has valid coords from Manual Curation (1.6686, 103.5224); "
                "v6_review row is the PeeringDB record with null coords. Same facility — "
                "merge PeeringDB provenance (pdb_id, clli, status) into the v5.1 row's note "
                "and drop the review row."
            ),
            "evidence_urls": (
                "https://www.peeringdb.com/org/" + str(v6.loc[losers[0], "source"]).replace(
                    "https://www.peeringdb.com/fac/", "")
                if str(v6.loc[losers[0], "source"]).startswith("https://www.peeringdb.com/fac/")
                else str(v6.loc[losers[0], "source"])
            ),
        })

    # ─── Case 2: Princeton Digital JH1 — which coord is correct? ───────
    # v5.1 has (1.692301, 103.414519) — Sedenak area
    # v6_review has (1.664183, 103.529826) — ~15km east, Kempas area
    # Princeton Digital's JH1 is publicly sited at the Sedenak Taman Teknologi
    # Sedenak per datacenterHawk + Princeton's own announcement. The v6_review
    # coords appear mis-entered.
    keepers = find_row_idx(v6, "Princeton Digital Group JH1 (Johor)", layer="v5.1")
    losers = find_row_idx(v6, "JH1", layer="v6_review")
    # Scope the v6_review match to the operator to avoid capturing Equinix JH1
    losers = [i for i in losers if "Princeton" in str(v6.loc[i, "operator"])]
    if keepers and losers:
        fixes.append({
            "action": "merge",
            "keep_row_idx": keepers[0],
            "keep_row_name": v6.loc[keepers[0], "name"],
            "drop_row_idx": losers[0],
            "drop_row_name": v6.loc[losers[0], "name"],
            "rationale": (
                "Same facility recorded twice: v5.1 row at (1.6923, 103.4145) matches "
                "Princeton Digital's published Sedenak location; v6_review PeeringDB record "
                "at (1.6642, 103.5298) is ~15km east and appears geocoding-drifted. "
                "Keep v5.1 coords, merge PeeringDB id into note, drop review row."
            ),
            "evidence_urls": (
                "https://datacenterhawk.com/marketplace/providers/princeton-digital-group"
                " | " + str(v6.loc[losers[0], "source"])
            ),
        })

    # ─── Case 3: YTL Johor DC 1/2/3 — artifactual 0.001° stagger ───────
    ytl_rows = sorted(find_row_idx(v6, "YTL Johor Data Center", layer="v5.1"))
    if len(ytl_rows) >= 3:
        fixes.append({
            "action": "review_campus",
            "keep_row_idx": "|".join(str(i) for i in ytl_rows),
            "keep_row_name": " || ".join(str(v6.loc[i, "name"]) for i in ytl_rows),
            "drop_row_idx": "",
            "drop_row_name": "",
            "rationale": (
                "Three rows at (1.6206, 103.5216), (1.6216, 103.5216), (1.6226, 103.5216) — "
                "a perfect 0.001°-lat stagger on identical lon. That pattern is not how a "
                "real campus lays out. YTL Green DC Park publishes a single campus in Kulai "
                "on 111ha; if these are distinct buildings they need real per-building coords "
                "from satellite imagery. RECOMMENDATION: keep as separate rows but flag all "
                "three coord_confidence='unknown' until each building is individually verified, "
                "or collapse to one campus row keyed by 'YTL Green Data Center Park (Kulai)'. "
                "Deferring to human decision."
            ),
            "evidence_urls": (
                "https://www.ytl.com/press-releases/ytl-green-data-center-park-launches-in-johor"
                "-the-first-integrated-data-center-park-powered-by-renewable-solar-energy-in-"
                "malaysia-2/"
            ),
        })

    # ─── Case 4: AIMS Cyberjaya duplicates ─────────────────────────────
    # v5.1: "AIMS Cyberjaya Block 2" at (2.938, 101.657)
    # v6_review: "AIMS CJ1 Cyberjaya" (no coords)
    # v6_review: "AIMS Cyberjaya" (no coords, generic)
    cj1 = find_row_idx(v6, "AIMS CJ1", layer="v6_review")
    generic = [i for i in find_row_idx(v6, "AIMS Cyberjaya", layer="v6_review") if i not in cj1]
    block2 = find_row_idx(v6, "AIMS Cyberjaya Block 2", layer="v5.1")
    if cj1 and block2:
        fixes.append({
            "action": "review",
            "keep_row_idx": block2[0],
            "keep_row_name": v6.loc[block2[0], "name"],
            "drop_row_idx": cj1[0],
            "drop_row_name": v6.loc[cj1[0], "name"],
            "rationale": (
                "AIMS has multiple Cyberjaya buildings (CJ1, Block 2, possibly more). "
                "PeeringDB labels one 'CJ1 Cyberjaya'; v5.1 has 'Block 2'. These may or "
                "may not be the same building — AIMS uses both conventions. Without an "
                "authoritative AIMS facility list, flag for human review rather than auto-merge."
            ),
            "evidence_urls": str(v6.loc[cj1[0], "source"]),
        })
    if generic and cj1:
        fixes.append({
            "action": "merge",
            "keep_row_idx": cj1[0],
            "keep_row_name": v6.loc[cj1[0], "name"],
            "drop_row_idx": generic[0],
            "drop_row_name": v6.loc[generic[0], "name"],
            "rationale": (
                "Generic 'AIMS Cyberjaya' with no coords is almost certainly the same "
                "facility as the PeeringDB 'AIMS CJ1 Cyberjaya' record (also from PeeringDB). "
                "Drop generic row; keep CJ1 as the named version."
            ),
            "evidence_urls": (
                str(v6.loc[cj1[0], "source"]) + " | " + str(v6.loc[generic[0], "source"])
            ),
        })

    # ─── Case 5: NTT Cyberjaya CBJ vs CBJ6 ────────────────────────────
    cbj = find_row_idx(v6, "NTT Cyberjaya Data Center (CBJ)", layer="v6_review")
    cbj6 = find_row_idx(v6, "NTT Cyberjaya CBJ6", layer="v5.1")
    if cbj and cbj6:
        fixes.append({
            "action": "review",
            "keep_row_idx": cbj6[0],
            "keep_row_name": v6.loc[cbj6[0], "name"],
            "drop_row_idx": cbj[0],
            "drop_row_name": v6.loc[cbj[0], "name"],
            "rationale": (
                "NTT operates multiple Cyberjaya buildings (CBJ1, CBJ2, ..., CBJ6). "
                "The PeeringDB record labels just 'CBJ' which could mean the first building "
                "(CBJ1) or the campus name. v5.1's CBJ6 is a specific later building. "
                "These are likely DIFFERENT facilities — NTT's CBJ1 vs CBJ6 — not duplicates. "
                "Do not auto-merge; human should verify by fetching NTT's facility page."
            ),
            "evidence_urls": (
                str(v6.loc[cbj[0], "source"])
            ),
        })

    OUT.parent.mkdir(exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["action", "keep_row_idx", "keep_row_name",
                        "drop_row_idx", "drop_row_name",
                        "rationale", "evidence_urls"],
        )
        w.writeheader()
        for fix in fixes:
            w.writerow(fix)
    print(f"Wrote {OUT} ({len(fixes)} fixes)")
    for fix in fixes:
        print(f"  [{fix['action']}] {fix['keep_row_name'][:40]} ← {fix['drop_row_name'][:40]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
