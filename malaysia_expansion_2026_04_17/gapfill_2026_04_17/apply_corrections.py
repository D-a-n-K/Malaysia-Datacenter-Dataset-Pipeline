"""
apply_corrections.py — rewrite v6_gapfill_candidates.csv and v6_dedup_fixes.csv
to address the six issues enumerated in the 2026-04-17 correction task:

  1. datacentermap.com TOS violation — strip sole-source rows, downgrade
     multi-source rows, delete cache.
  2. coord_confidence mislabels — relabel, blank state-centroid coords.
  3. Row 23 YTL Green DC Park — change add_new → skip/update_existing.
  4. Rows 5 (Maxis i-City) and 16 (HDC) — co-tenant review flagging.
  5. TM Global ↔ VADS pair cross-references.
  6. Dedup-fixes file — NTT distance, merge verification.

Produces `v6_gapfill_corrections.md` summarising every change.
"""

from __future__ import annotations

import csv
import math
import re
import shutil
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
CAND_PATH = HERE / "outputs" / "v6_gapfill_candidates.csv"
FIX_PATH = HERE / "outputs" / "v6_dedup_fixes.csv"
REPORT_PATH = HERE / "outputs" / "v6_gapfill_corrections.md"
CACHE_DIR = Path("/tmp/gapfill_cache")
V6_MASTER = HERE.parent / "outputs" / "malaysia_datacenters_v6_master.csv"

SOLE_SOURCE_ROWS = [27, 28, 30, 40, 41]          # Issue 1a
MULTI_SOURCE_ROWS = [13, 14, 15, 16, 26, 29, 32, 35, 36]  # Issue 1b

TM_VADS_PAIRS = [
    (21, 3, "IPDC"),   # TM Global IPDC ↔ VADS IPDC
    (20, 4, "KVDC"),   # TM Global KVDC ↔ VADS KVDC
    (18, 26, "BFDC"),  # TM Global BFDC ↔ VADS Brickfields
]


def haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(a_lat), math.radians(b_lat)
    dphi = math.radians(b_lat - a_lat)
    dlam = math.radians(b_lon - a_lon)
    x = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(x))


def append_log(cell: str, msg: str) -> str:
    return (cell + " || " + msg) if cell else msg


def strip_dcm_urls(sources: str) -> tuple[str, int]:
    urls = [u.strip() for u in (sources or "").split("|") if u.strip()]
    remaining = [u for u in urls if "datacentermap.com" not in u]
    removed = len(urls) - len(remaining)
    return " | ".join(remaining), removed


def main() -> int:
    df = pd.read_csv(CAND_PATH)
    if "correction_log" not in df.columns:
        df["correction_log"] = ""
    log_lines: list[str] = []

    # ─── Issue 1a: sole-source rejections ──────────────────────────────
    rejected_rows = []
    for idx in SOLE_SOURCE_ROWS:
        row = df.loc[idx]
        reject_prefix = (
            "REJECTED: sole source was datacentermap.com which is TOS-blocked. "
            "Facility may still exist — re-investigation needed via other sources "
            "(SSM, MCMC, operator-direct, trade press). "
        )
        old_action = row["promotion_action"]
        df.at[idx, "source"] = ""
        df.at[idx, "n_sources"] = 0
        df.at[idx, "address"] = ""
        df.at[idx, "lat"] = ""
        df.at[idx, "lon"] = ""
        df.at[idx, "coord_confidence"] = "unknown"
        df.at[idx, "promotion_action"] = "reject:sole_source_tos_blocked"
        df.at[idx, "note"] = reject_prefix + str(row["note"])
        df.at[idx, "correction_log"] = append_log(
            str(df.at[idx, "correction_log"]),
            f"§1a rejected: sole datacentermap.com source removed; "
            f"coords+address blanked; n_sources→0; action {old_action}→reject.",
        )
        rejected_rows.append((idx, row["name"]))

    # ─── Issue 1b: multi-source downgrades ─────────────────────────────
    downgraded_rows = []
    for idx in MULTI_SOURCE_ROWS:
        row = df.loc[idx]
        new_source, removed = strip_dcm_urls(str(row["source"]))
        try:
            new_n = max(0, int(float(row["n_sources"])) - removed)
        except (TypeError, ValueError):
            new_n = 0
        df.at[idx, "source"] = new_source
        df.at[idx, "n_sources"] = new_n
        log_msg = f"§1b stripped {removed} dcm url(s); n_sources→{new_n}"
        if new_n == 0:
            # Treat per §1a
            df.at[idx, "address"] = ""
            df.at[idx, "lat"] = ""
            df.at[idx, "lon"] = ""
            df.at[idx, "coord_confidence"] = "unknown"
            df.at[idx, "promotion_action"] = "reject:sole_source_tos_blocked"
            df.at[idx, "note"] = (
                "REJECTED: after stripping TOS-blocked datacentermap.com citation "
                "no independent source remained. Re-investigation needed. "
                + str(row["note"])
            )
            log_msg += "; row promoted to §1a rejection (zero sources left)."
            rejected_rows.append((idx, row["name"]))
        elif new_n == 1:
            warning = (
                " WARNING: downgraded to single-source after removing TOS-blocked "
                "citation; address/coords derive from the remaining source only — verify."
            )
            df.at[idx, "note"] = str(row["note"]) + warning
            log_msg += "; appended single-source warning."
        df.at[idx, "correction_log"] = append_log(
            str(df.at[idx, "correction_log"]), log_msg
        )
        downgraded_rows.append((idx, row["name"], removed, new_n))

    # ─── Issue 2a: row 22 TM Global SJDC Penang ────────────────────────
    idx = 22
    old_conf = df.at[idx, "coord_confidence"]
    df.at[idx, "coord_confidence"] = "approximate_locality"
    df.at[idx, "lat"] = ""
    df.at[idx, "lon"] = ""
    df.at[idx, "promotion_action"] = "hold:needs_penang_address"
    df.at[idx, "note"] = (
        str(df.at[idx, "note"]) + " | Penang state-centroid coord removed — "
        "too coarse for land-transformation use. Need street-level Penang "
        "address for SJDC (likely Bayan Lepas FIZ per TM Global public site)."
    )
    df.at[idx, "correction_log"] = append_log(
        str(df.at[idx, "correction_log"]),
        f"§2a coord_confidence {old_conf}→approximate_locality; "
        "coords blanked (state-centroid false-precision); action→hold."
    )

    # ─── Issue 2b: audit all other rows ────────────────────────────────
    relabels: list[tuple[int, str, str, str]] = []
    for idx_i, row in df.iterrows():
        if idx_i in SOLE_SOURCE_ROWS or idx_i == 22:
            continue
        note = str(row.get("note") or "")
        conf = str(row.get("coord_confidence") or "")
        # Extract Nominatim match if present
        m = re.search(r"match='([^']+)'", note)
        match_str = m.group(1) if m else ""
        ml = match_str.lower()
        new_conf = conf
        reason = ""
        # Heuristics: state or country-only match → blank + approximate_locality
        state_only = match_str and ml.count(",") <= 1 and any(
            kw in ml for kw in ["negeri", "selangor", "perak", "pulau pinang",
                                  "johor", "kedah", "sabah", "sarawak", "terengganu",
                                  "kelantan", "melaka", "malaysia"]
        ) and not any(kw in ml for kw in [
            "jalan", "persiaran", "lebuh", "lorong", "lintang", "lebuhraya"
        ])
        # Full street address with number
        has_street_number = bool(re.search(r"\b\d{1,4}[ ,]", match_str))
        has_street_name = bool(re.search(
            r"\b(jalan|persiaran|lebuh|lorong|lintang|lebuhraya|jln)\b", ml
        ))
        imp_match = re.search(r"importance=([\d.]+)", note)
        try:
            imp = float(imp_match.group(1)) if imp_match else 0.0
        except ValueError:
            imp = 0.0

        if state_only and conf in ("exact", "building"):
            new_conf = "approximate_locality"
            reason = "state-level match reclassified to approximate_locality"
        elif conf == "exact" and not has_street_number:
            new_conf = "building"
            reason = "exact→building (no street number in match)"
        elif conf == "building" and not has_street_name and not has_street_number:
            new_conf = "approximate_locality"
            reason = "building→approximate_locality (no street token in match)"

        if new_conf != conf:
            df.at[idx_i, "coord_confidence"] = new_conf
            if state_only:
                df.at[idx_i, "lat"] = ""
                df.at[idx_i, "lon"] = ""
                reason += "; coords blanked (state-centroid false-precision)"
            df.at[idx_i, "correction_log"] = append_log(
                str(df.at[idx_i, "correction_log"]),
                f"§2b relabel {conf}→{new_conf}: {reason}"
            )
            relabels.append((idx_i, row["name"], conf, new_conf))

    # ─── Issue 3: row 23 YTL Green DC Park ─────────────────────────────
    idx = 23
    # Check if row 23 is indeed YTL Green DC Park
    if "YTL Green" in str(df.at[idx, "name"]):
        # Look up the v5.1 reference row index to cite
        v6 = pd.read_csv(V6_MASTER)
        v5_ytl = v6[v6["name"].fillna("").str.contains(
            "YTL Green Data Center Park", case=False)]
        matched_idx = int(v5_ytl.index[0]) if len(v5_ytl) else -1
        matched_name = str(v5_ytl.iloc[0]["name"]) if len(v5_ytl) else "unknown"
        old_action = df.at[idx, "promotion_action"]
        df.at[idx, "promotion_action"] = (
            f"update_existing:{matched_idx}" if matched_idx >= 0
            else "skip:duplicate_of_v5.1_YTL_Green_DC_Park"
        )
        df.at[idx, "note"] = (
            str(df.at[idx, "note"])
            + f" | UPDATE target: v6_master row {matched_idx} "
            + f"({matched_name}). This candidate's sources (YTL operator + sustainability "
            + "reports) enrich the existing v5.1 row's provenance; no new facility."
        )
        df.at[idx, "correction_log"] = append_log(
            str(df.at[idx, "correction_log"]),
            f"§3 action {old_action}→update_existing:{matched_idx} "
            f"(resolves self-contradiction with note)"
        )

    # ─── Issue 4a: rows 5 (Maxis) and 16 (HDC) co-tenancy ─────────────
    maxis_idx, hdc_idx = 5, 16
    df.at[maxis_idx, "promotion_action"] = "review:cotenant_of_HDC High Performance Data Centre (Shah Alam)"
    df.at[maxis_idx, "note"] = str(df.at[maxis_idx, "note"]) + (
        " | Co-tenant building with HDC at i-City Block M "
        f"(gapfill row {hdc_idx}). Dual-operator convention per VADS/TM Global "
        "pattern — keep as separate rows if convention applies, or merge per "
        "human decision."
    )
    df.at[maxis_idx, "correction_log"] = append_log(
        str(df.at[maxis_idx, "correction_log"]),
        f"§4a added cotenant-review flag linking to row {hdc_idx} (HDC)"
    )
    df.at[hdc_idx, "note"] = str(df.at[hdc_idx, "note"]) + (
        f" | Co-tenant building with Maxis i-City DC at i-City Block M "
        f"(gapfill row {maxis_idx}). Different operators at same building — "
        "keep both rows if the project's dual-brand convention applies."
    )
    df.at[hdc_idx, "correction_log"] = append_log(
        str(df.at[hdc_idx, "correction_log"]),
        f"§4a symmetric cotenancy note linking to row {maxis_idx} (Maxis)"
    )

    # ─── Issue 4b: rewrite existing review rows to open-question framing ─
    review_rows_to_clean = [36, 37, 38, 41]  # Exabytes×3, AWS Bukit Jalil
    for idx_i in review_rows_to_clean:
        if idx_i >= len(df):
            continue
        note = str(df.at[idx_i, "note"])
        action = str(df.at[idx_i, "promotion_action"])
        match = re.match(r"review:possible_dup_(.+)", action)
        other_name = match.group(1) if match else "(unknown)"
        clean_note = (
            f"CO-TENANCY QUESTION: this candidate's coords fall within 150 m "
            f"of v6 row '{other_name}'. Different operators → not auto-duplicate, "
            "but may be the same physical building. Reviewer to decide: "
            "(a) same building co-tenant — keep both rows (carrier-hotel convention), "
            "(b) same facility with rebrand — merge, or "
            "(c) genuinely different buildings — keep both and refine coords. "
            "| "
        ) + note
        df.at[idx_i, "note"] = clean_note
        df.at[idx_i, "correction_log"] = append_log(
            str(df.at[idx_i, "correction_log"]),
            "§4b rewrote review note to open-question framing"
        )

    # ─── Issue 5: TM Global ↔ VADS pair cross-references ──────────────
    # Skip pairs where one side was already dropped in §1b (e.g. VADS Brickfields row 26
    # became zero-source and got rejected).
    for tm_idx, vads_idx, tag in TM_VADS_PAIRS:
        tm_row = df.loc[tm_idx]
        vads_row = df.loc[vads_idx]
        tm_rejected = str(tm_row["promotion_action"]).startswith("reject:")
        vads_rejected = str(vads_row["promotion_action"]).startswith("reject:")
        if tm_rejected and vads_rejected:
            continue
        df.at[tm_idx, "note"] = str(df.at[tm_idx, "note"]) + (
            f" | Co-branded with VADS {tag} at same building "
            f"(gapfill row {vads_idx})."
        )
        df.at[vads_idx, "note"] = str(df.at[vads_idx, "note"]) + (
            f" | Co-branded with TM Global {tag} at same building "
            f"(gapfill row {tm_idx})."
        )
        df.at[tm_idx, "correction_log"] = append_log(
            str(df.at[tm_idx, "correction_log"]),
            f"§5 dual-brand link: ↔ row {vads_idx} (VADS {tag})"
        )
        df.at[vads_idx, "correction_log"] = append_log(
            str(df.at[vads_idx, "correction_log"]),
            f"§5 dual-brand link: ↔ row {tm_idx} (TM Global {tag})"
        )

    # Write candidates
    df.to_csv(CAND_PATH, index=False)

    # ─── Issue 6: dedup fixes ─────────────────────────────────────────
    fix_df = pd.read_csv(FIX_PATH)
    if "correction_log" not in fix_df.columns:
        fix_df["correction_log"] = ""
    dedup_changes: list[str] = []

    # §6b — NTT CBJ vs CBJ6: compute distance
    ntt_mask = fix_df["keep_row_name"].fillna("").str.contains("NTT Cyberjaya CBJ6")
    if ntt_mask.any():
        v6 = pd.read_csv(V6_MASTER)
        cbj6_row = v6[v6["name"] == "NTT Cyberjaya CBJ6"]
        cbj_row = v6[v6["name"] == "NTT Cyberjaya Data Center (CBJ)"]
        if len(cbj6_row) and len(cbj_row):
            # NTT CBJ has no coords in v6_review — distance undefined
            try:
                lat1 = float(cbj6_row.iloc[0]["lat"])
                lon1 = float(cbj6_row.iloc[0]["lon"])
                lat2 = float(cbj_row.iloc[0]["lat"])
                lon2 = float(cbj_row.iloc[0]["lon"])
                if not (math.isnan(lat2) or math.isnan(lon2)):
                    d = haversine_m(lat1, lon1, lat2, lon2)
                    msg = f"Distance CBJ6 ↔ CBJ = {d:.0f}m."
                    if d < 300:
                        new_action = "merge_to_named_building_with_operator_verification"
                    else:
                        new_action = "review"
                    msg += f" action={new_action}"
                else:
                    msg = (
                        "Distance undefined — v6_review 'NTT Cyberjaya Data Center (CBJ)' "
                        "has null coords (PeeringDB row carried no lat/lon). Cannot compute "
                        "haversine; `review` stands."
                    )
                    new_action = "review"
                fix_df.loc[ntt_mask, "action"] = new_action
                fix_df.loc[ntt_mask, "rationale"] = (
                    str(fix_df.loc[ntt_mask, "rationale"].iloc[0]) + " | " + msg
                )
                fix_df.loc[ntt_mask, "correction_log"] = (
                    "§6b added haversine computation; " + msg
                )
                dedup_changes.append(f"NTT CBJ/CBJ6: {msg}")
            except (ValueError, TypeError):
                pass

    # §6d — merge rows [0], [1], [4] verification check
    merge_idxs = fix_df.index[fix_df["action"] == "merge"].tolist()
    for mi in merge_idxs:
        evidence = str(fix_df.at[mi, "evidence_urls"])
        # Have we cached any URL from this evidence? The PeeringDB facility
        # URL would be the independent reference we'd need.
        # For the sprint, we only have PeeringDB URLs in the cache and the v6
        # evidence link. Without cross-source operator pages, the merge is
        # "pending verification" per the user's §6d rule.
        old_action = fix_df.at[mi, "action"]
        fix_df.at[mi, "action"] = "merge_pending_verification"
        fix_df.at[mi, "rationale"] = (
            str(fix_df.at[mi, "rationale"])
            + " | PENDING: no independent operator-site coordinate available in "
              "the gapfill cache to corroborate the 'keep' row's lat/lon. "
              "Downgraded from merge to merge_pending_verification per §6d. "
              "Operator-direct fetch (Keppel/Princeton/AIMS websites) would clear this."
        )
        fix_df.at[mi, "correction_log"] = append_log(
            str(fix_df.at[mi, "correction_log"]),
            f"§6d {old_action}→merge_pending_verification (no independent coord corroboration in cache)"
        )
        dedup_changes.append(
            f"Row {mi} '{fix_df.at[mi, 'keep_row_name']}': {old_action}→merge_pending_verification"
        )

    fix_df.to_csv(FIX_PATH, index=False)

    # ─── Issue 1c: cache deletion ──────────────────────────────────────
    # We cannot tell which cache files came from datacentermap.com by name alone
    # (SHA1 of URL), so scan each cached file for the datacentermap.com string.
    deleted = 0
    if CACHE_DIR.exists():
        for f in CACHE_DIR.glob("*.cache"):
            try:
                sample = f.read_text(encoding="utf-8", errors="replace")[:4000]
            except Exception:
                continue
            if "datacentermap.com" in sample or "datacentermap" in f.name:
                f.unlink()
                deleted += 1

    # ─── Report ────────────────────────────────────────────────────────
    actions = df["promotion_action"].astype(str).value_counts()

    lines = ["# v6 gapfill corrections report", "",
             "Applied by `apply_corrections.py` on 2026-04-17 in response to the",
             "six-issue review of `v6_gapfill_candidates.csv` and `v6_dedup_fixes.csv`.",
             "",
             "## Issue 1 — datacentermap.com TOS violation (the big one)",
             "",
             "The parent project's `project_datacentermap_tos.md` memory and the",
             "original gapfill brief both forbid datacentermap.com as a source. My",
             "original pass cited it on 14 rows anyway — apologies. Corrections:",
             "",
             "### §1a — sole-source rejections (fully blanked)",
             "",
             "| Row | Facility name |",
             "|---|---|",
             *[f"| {idx} | {name} |" for idx, name in rejected_rows[: len(SOLE_SOURCE_ROWS)]],
             "",
             "Each row had `source`, `lat`, `lon`, `address` blanked; `n_sources=0`; ",
             "`coord_confidence=unknown`; `promotion_action=reject:sole_source_tos_blocked`. ",
             "The `name` and `operator` fields are kept so a follow-up investigator ",
             "knows what to re-source.",
             "",
             "### §1b — multi-source downgrades",
             "",
             "| Row | Facility | dcm removed | new n_sources | outcome |",
             "|---|---|---|---|---|",
             *[f"| {idx} | {name} | {rem} | {new_n} | "
               f"{'single-source warning' if new_n == 1 else ('downgraded to reject (§1a)' if new_n == 0 else 'multi-source intact')} |"
               for idx, name, rem, new_n in downgraded_rows],
             "",
             "### §1c — cache cleanup",
             "",
             f"Scanned `/tmp/gapfill_cache/` for files containing `datacentermap.com` ",
             f"in their content or filename. Deleted **{deleted}** file(s).",
             "",
             "## Issue 2 — coord_confidence relabels",
             "",
             "### §2a — row 22 (TM Global SJDC Penang)",
             "",
             "Was `exact` with a state-centroid geocode; note admitted rough-locality. ",
             "Corrected to `approximate_locality` with blanked coords (state centroid is ",
             "worse than no coord — suggests false precision). Action changed to ",
             "`hold:needs_penang_address`.",
             "",
             "### §2b — audit of other rows",
             "",
             f"Relabeled {len(relabels)} row(s):",
             "",
             "| Row | Facility | Old | New |",
             "|---|---|---|---|",
             *[f"| {idx} | {name[:45]} | {old} | {new} |"
               for idx, name, old, new in relabels],
             "",
             "## Issue 3 — row 23 YTL Green Data Center Park",
             "",
             "Self-contradicting (note said 'already in v5.1' while action was ",
             "`add_new`). Changed to `update_existing:<row_idx>` pointing at the ",
             "matching v6_master row. The candidate's sources enrich the existing ",
             "v5.1 row's provenance rather than add a new facility.",
             "",
             "## Issue 4 — co-tenancy flagging",
             "",
             "### §4a — rows 5 (Maxis i-City) and 16 (HDC)",
             "",
             "Both sit in the same i-City Block M building. Row 5 previously had no ",
             "co-tenancy note; now flagged `review:cotenant_of_HDC...` and its note ",
             "mirrors row 16's. Row 16's note updated symmetrically to reference row 5.",
             "",
             "### §4b — existing review rows 36/37/38/41",
             "",
             "Rewrote the note on each to explicit open-question form: 'reviewer to ",
             "decide: (a) same building co-tenant, (b) rebrand-merge, or (c) distinct ",
             "buildings.' No preemptive conclusion.",
             "",
             "## Issue 5 — TM Global ↔ VADS pair cross-references",
             "",
             "| TM Global row | VADS row | Tag | Status |",
             "|---|---|---|---|",
             *[
                 f"| {tm} | {vads} | {tag} | "
                 f"{'linked' if not (str(df.at[tm, 'promotion_action']).startswith('reject') and str(df.at[vads, 'promotion_action']).startswith('reject')) else 'both rejected in §1b, link skipped'} |"
                 for tm, vads, tag in TM_VADS_PAIRS
             ],
             "",
             "## Issue 6 — v6_dedup_fixes.csv",
             "",
             *[f"* {c}" for c in dedup_changes],
             "",
             "## Final promotion_action distribution",
             "",
             "| promotion_action | count |",
             "|---|---|",
             *[f"| `{a}` | {n} |" for a, n in actions.items()],
             "",
             "## Success criteria check",
             "",
             f"* datacentermap.com URL still in any `source` field: "
             f"{'NO' if not df['source'].fillna('').str.contains('datacentermap.com', regex=False).any() else 'YES (BUG)'}",
             f"* datacentermap.com cache files remaining: "
             f"{deleted_remaining_check(CACHE_DIR)}",
             f"* Row 23 promotion_action: `{df.at[23, 'promotion_action']}`",
             f"* Row 5 references row 16 in note: "
             f"{'yes' if 'row 16' in str(df.at[5, 'note']).lower() or 'gapfill row 16' in str(df.at[5, 'note']) else 'NO'}",
             f"* Row 16 references row 5 in note: "
             f"{'yes' if 'row 5' in str(df.at[16, 'note']).lower() or 'gapfill row 5' in str(df.at[16, 'note']) else 'NO'}",
             "",
             ]
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")
    print(f"Candidates now: {len(df)} rows")
    print("Final actions:")
    for a, n in actions.items():
        print(f"  {a}: {n}")
    return 0


def deleted_remaining_check(cache_dir: Path) -> str:
    if not cache_dir.exists():
        return "cache dir absent"
    remaining = 0
    for f in cache_dir.glob("*.cache"):
        try:
            txt = f.read_text(encoding="utf-8", errors="replace")[:2000]
        except Exception:
            continue
        if "datacentermap.com" in txt:
            remaining += 1
    return f"{remaining}"


if __name__ == "__main__":
    sys.exit(main())
