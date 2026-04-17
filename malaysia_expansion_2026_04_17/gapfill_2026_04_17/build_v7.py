"""
build_v7.py — apply reviewed gapfill + dedup decisions to v6 master, producing v7.

Option 1 per 2026-04-17 direction: Step 4 (rejected_resolution) is SKIPPED;
v6_rejected_resolution.csv has not been produced yet. The 5
`reject:sole_source_tos_blocked` gapfill rows are carried forward as known gaps
for v8.

Inputs (read-only):
  outputs/malaysia_datacenters_v6_master.csv
  gapfill_2026_04_17/outputs/v6_gapfill_candidates.csv
  gapfill_2026_04_17/outputs/v6_dedup_fixes.csv

Outputs (written into gapfill_2026_04_17/outputs/):
  malaysia_datacenters_v7_master.csv
  v6_to_v7_change_log.csv
  v7_build_report.md
"""

from __future__ import annotations

import csv
import math
import re
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
V6_MASTER = PROJECT / "outputs" / "malaysia_datacenters_v6_master.csv"
GAP_CSV   = HERE / "outputs" / "v6_gapfill_candidates.csv"
DEDUP_CSV = HERE / "outputs" / "v6_dedup_fixes.csv"
OUT_DIR   = HERE / "outputs"

V7_MASTER    = OUT_DIR / "malaysia_datacenters_v7_master.csv"
V7_CHANGELOG = OUT_DIR / "v6_to_v7_change_log.csv"
V7_REPORT    = OUT_DIR / "v7_build_report.md"

V7_EXTRA_COLS = ["v7_status", "v7_pending_review", "v7_change_log"]

# ---------------------------------------------------------------------------
# loaders
# ---------------------------------------------------------------------------

def _read(path: Path) -> tuple[list[str], list[dict]]:
    with path.open() as f:
        reader = csv.DictReader(f)
        cols = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]
    return cols, rows


def _normalize(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def _fuzzy_overlap(a: str, b: str) -> bool:
    """Loose token overlap check: any token ≥3 chars shared (after stop removal)."""
    stop = {"data", "center", "centre", "facility", "dc", "campus", "sdn",
            "bhd", "berhad", "malaysia", "the", "of", "and"}
    ta = {t for t in _normalize(a).split() if len(t) >= 3 and t not in stop}
    tb = {t for t in _normalize(b).split() if len(t) >= 3 and t not in stop}
    return bool(ta & tb)


_EARTH_R = 6_371_000.0
def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return 2 * _EARTH_R * math.asin(math.sqrt(a))


def _float_or_none(x):
    if x is None or x == "":
        return None
    try:
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    master_cols, v6_rows = _read(V6_MASTER)
    gap_cols,   gap_rows = _read(GAP_CSV)
    dedup_cols, dedup    = _read(DEDUP_CSV)

    v7_cols = list(master_cols) + V7_EXTRA_COLS
    assert len(v7_cols) == 41, f"Expected 41 cols, got {len(v7_cols)}"

    # ---- pre-flight ------------------------------------------------------
    gap_pa = Counter(r.get("promotion_action", "") for r in gap_rows)
    add_new        = sum(1 for r in gap_rows if r.get("promotion_action") == "add_new")
    gap_review     = sum(1 for r in gap_rows if r.get("promotion_action","").startswith("review:"))
    gap_hold       = sum(1 for r in gap_rows if r.get("promotion_action","").startswith("hold:"))
    merge_confirmed= sum(1 for r in dedup    if r.get("action") == "merge_confirmed")
    merge_reversed = sum(1 for r in dedup    if r.get("action") == "merge_reversed")
    estimate = (len(v6_rows) + add_new + gap_review + gap_hold
                - merge_confirmed - merge_reversed)

    print(f"v6 master rows: {len(v6_rows)}")
    print(f"gapfill rows  : {len(gap_rows)}")
    print(f"  add_new        : {add_new}")
    print(f"  review:*       : {gap_review}")
    print(f"  hold:*         : {gap_hold}")
    print(f"  update_existing: {sum(1 for r in gap_rows if r.get('promotion_action','').startswith('update_existing:'))}")
    print(f"  skip           : {sum(1 for r in gap_rows if r.get('promotion_action','').startswith('skip:'))}")
    print(f"  reject         : {sum(1 for r in gap_rows if r.get('promotion_action','').startswith('reject:'))}")
    print(f"dedup rows    : {len(dedup)}")
    print(f"  merge_confirmed  : {merge_confirmed}")
    print(f"  merge_reversed   : {merge_reversed}")
    print(f"Pre-flight v7 estimate = 101 + {add_new} + {gap_review} + {gap_hold} - {merge_confirmed} - {merge_reversed} = {estimate}")
    print()

    change_log: list[dict] = []

    def log(change_type, source_file, source_row_idx, v6_row_idx, v7_row_idx, description):
        change_log.append({
            "change_type":    change_type,
            "source_file":    source_file,
            "source_row_idx": "" if source_row_idx is None else str(source_row_idx),
            "v6_row_idx":     "" if v6_row_idx is None else str(v6_row_idx),
            "v7_row_idx":     "" if v7_row_idx is None else str(v7_row_idx),
            "description":    description,
        })

    # Working table: start as a copy of v6 rows, each carrying original v6 idx + v7 fields
    rows: list[dict] = []
    for i, r in enumerate(v6_rows):
        nr = dict(r)
        nr["_v6_idx"] = i
        nr["v7_status"] = "inherited"
        nr["v7_pending_review"] = ""
        nr["v7_change_log"] = "inherited from v6 master unchanged"
        rows.append(nr)

    # -------------------------------------------------------------------
    # STEP 2 — apply dedup fixes
    # -------------------------------------------------------------------
    # Track which v6 indices are flagged/updated for later cross-referencing
    def row_by_v6idx(idx: int) -> dict | None:
        for r in rows:
            if r["_v6_idx"] == idx:
                return r
        return None

    deletions: set[int] = set()

    for dedup_idx, d in enumerate(dedup):
        action = d["action"]
        keep_idx_raw = d["keep_row_idx"]
        drop_idx_raw = d["drop_row_idx"]
        # Support multi-idx review_campus ("53|54|55")
        keep_idxs = [int(x) for x in str(keep_idx_raw).split("|") if x.strip()]
        drop_idxs = []
        if drop_idx_raw and str(drop_idx_raw).lower() not in ("", "nan"):
            try:
                drop_idxs = [int(float(x)) for x in str(drop_idx_raw).split("|") if x.strip()]
            except ValueError:
                drop_idxs = []

        rationale = d.get("rationale", "")
        evidence  = d.get("evidence_urls", "")
        # One-line summary for v7_pending_review
        summary = rationale.split(" | ")[0][:300]

        if action == "merge_confirmed":
            keep = row_by_v6idx(keep_idxs[0])
            drop = row_by_v6idx(drop_idxs[0]) if drop_idxs else None
            if keep is None or drop is None:
                log("error", "dedup_fixes", dedup_idx, None, None,
                    f"merge_confirmed: keep v6={keep_idxs} drop v6={drop_idxs} — missing row")
                continue
            # Append drop row's fields to keep row using ' | merged from v6_row_<n>: <fields>'
            prov_bits = []
            for f in ("source", "note"):
                dv = (drop.get(f) or "").strip()
                if dv:
                    prov_bits.append(f"{f}={dv}")
            prov_str = "; ".join(prov_bits) if prov_bits else "(no additional fields)"
            # append to keep's source and note
            for f in ("source", "note"):
                dv = (drop.get(f) or "").strip()
                if dv:
                    kv = (keep.get(f) or "").strip()
                    merged = f"{kv} | merged from v6_row_{drop['_v6_idx']}: {dv}" if kv else f"merged from v6_row_{drop['_v6_idx']}: {dv}"
                    keep[f] = merged
            keep["v7_status"] = "updated"
            keep["v7_change_log"] = f"dedup_merge_confirmed: absorbed v6_row_{drop['_v6_idx']} ({drop.get('name','')})"
            deletions.add(drop["_v6_idx"])
            log("field_append", "dedup_fixes", dedup_idx, keep["_v6_idx"], None,
                f"merge_confirmed: folded v6_row_{drop['_v6_idx']} source/note into v6_row_{keep['_v6_idx']}")
            log("deletion", "dedup_fixes", dedup_idx, drop["_v6_idx"], None,
                f"merge_confirmed: dropped v6_row_{drop['_v6_idx']} ({drop.get('name','')})")

        elif action == "merge_reversed":
            # symmetric: delete keep, retain drop
            keep = row_by_v6idx(keep_idxs[0])
            drop = row_by_v6idx(drop_idxs[0]) if drop_idxs else None
            if keep is None or drop is None:
                log("error", "dedup_fixes", dedup_idx, None, None,
                    f"merge_reversed: missing row(s) keep={keep_idxs} drop={drop_idxs}")
                continue
            for f in ("source", "note"):
                kv = (keep.get(f) or "").strip()
                if kv:
                    dv = (drop.get(f) or "").strip()
                    merged = f"{dv} | merged from v6_row_{keep['_v6_idx']}: {kv}" if dv else f"merged from v6_row_{keep['_v6_idx']}: {kv}"
                    drop[f] = merged
            drop["v7_status"] = "updated"
            drop["v7_change_log"] = f"dedup_merge_reversed: absorbed v6_row_{keep['_v6_idx']} ({keep.get('name','')})"
            deletions.add(keep["_v6_idx"])
            log("field_append", "dedup_fixes", dedup_idx, drop["_v6_idx"], None,
                f"merge_reversed: folded v6_row_{keep['_v6_idx']} into v6_row_{drop['_v6_idx']}")
            log("deletion", "dedup_fixes", dedup_idx, keep["_v6_idx"], None,
                f"merge_reversed: dropped v6_row_{keep['_v6_idx']}")

        elif action == "not_duplicate_keep_both":
            keep = row_by_v6idx(keep_idxs[0])
            drop = row_by_v6idx(drop_idxs[0]) if drop_idxs else None
            if keep is None or drop is None:
                log("error", "dedup_fixes", dedup_idx, None, None,
                    f"not_duplicate_keep_both: missing row(s) keep={keep_idxs} drop={drop_idxs}")
                continue
            note = f"not_duplicate_per_dedup_{dedup_idx}: {summary}"
            for r in (keep, drop):
                r["v7_status"] = "pending_review"
                prev = r.get("v7_pending_review") or ""
                r["v7_pending_review"] = (prev + (" || " if prev else "") + note)
                r["v7_change_log"] = f"dedup_not_duplicate_keep_both (entry {dedup_idx}): paired v6_row_{keep['_v6_idx']} ↔ v6_row_{drop['_v6_idx']}"
            log("flag_added", "dedup_fixes", dedup_idx, keep["_v6_idx"], None,
                f"not_duplicate_keep_both: flagged v6_row_{keep['_v6_idx']}")
            log("flag_added", "dedup_fixes", dedup_idx, drop["_v6_idx"], None,
                f"not_duplicate_keep_both: flagged v6_row_{drop['_v6_idx']}")

        elif action in ("review", "review_campus", "review_unresolved"):
            note_prefix = f"{action}_per_dedup_{dedup_idx}"
            all_idxs = keep_idxs + drop_idxs
            for vi in all_idxs:
                r = row_by_v6idx(vi)
                if r is None:
                    log("error", "dedup_fixes", dedup_idx, None, None,
                        f"{action}: missing row v6={vi}")
                    continue
                r["v7_status"] = "pending_review"
                prev = r.get("v7_pending_review") or ""
                msg = f"{note_prefix}: {summary}"
                r["v7_pending_review"] = (prev + (" || " if prev else "") + msg)
                r["v7_change_log"] = f"dedup_{action} (entry {dedup_idx}): rows {all_idxs}"
                log("flag_added", "dedup_fixes", dedup_idx, vi, None,
                    f"{action}: flagged v6_row_{vi}")

        else:
            log("error", "dedup_fixes", dedup_idx, None, None,
                f"unknown dedup action {action!r}")

    # -------------------------------------------------------------------
    # Special-case flags per task spec
    # -------------------------------------------------------------------
    aims_cross = ("see also dedup entry 3 (review): v6_row_74 vs v6_row_9; "
                  "and dedup entry 4: v6_row_91 may describe same Cyber 3 building as v6_row_9 — "
                  "three-way review needed before v8.")
    for vi in (9, 74, 91):
        r = row_by_v6idx(vi)
        if r is not None:
            prev = r.get("v7_pending_review") or ""
            r["v7_pending_review"] = prev + (" || " if prev else "") + aims_cross
            r["v7_status"] = "pending_review"
            r["v7_change_log"] = (r.get("v7_change_log") or "") + " | special_case:AIMS_three_way"
            log("flag_added", "special_case", None, vi, None,
                "AIMS three-way cross-reference flagged")

    keppel_note = ("merge_confirmed but coord (from Dgtl Infra XLSX per v6 note) not "
                   "operator-verified; Keppel does not publish street-level coord for this "
                   "BTS facility. Flag for coord corroboration in v8.")
    r44 = row_by_v6idx(44)
    if r44 is not None:
        prev = r44.get("v7_pending_review") or ""
        r44["v7_pending_review"] = prev + (" || " if prev else "") + keppel_note
        # Keppel kept 'updated' from merge_confirmed; promote to pending_review for the coord flag
        r44["v7_status"] = "pending_review"
        r44["v7_change_log"] = (r44.get("v7_change_log") or "") + " | special_case:Keppel_coord_caveat"
        log("flag_added", "special_case", None, 44, None,
            "Keppel coord-caveat flagged (operator does not publish street-level coord)")

    ntt_note = ("recommended for v8: rename to 'NTT Cyberjaya CBJ1' and coord-fill from "
                "published campus address 43000 Jalan APEC, Cyberjaya 63000 "
                "(per dedup resolution report).")
    r73 = row_by_v6idx(73)
    if r73 is not None:
        prev = r73.get("v7_pending_review") or ""
        r73["v7_pending_review"] = prev + (" || " if prev else "") + ntt_note
        r73["v7_status"] = "pending_review"
        r73["v7_change_log"] = (r73.get("v7_change_log") or "") + " | special_case:NTT_rename_recommendation"
        log("flag_added", "special_case", None, 73, None,
            "NTT CBJ rename + coord-fill recommendation flagged")

    # -------------------------------------------------------------------
    # Apply deletions
    # -------------------------------------------------------------------
    rows = [r for r in rows if r["_v6_idx"] not in deletions]

    # -------------------------------------------------------------------
    # STEP 3 — apply gapfill candidates
    # -------------------------------------------------------------------
    # Build a lookup of candidate note folding (correction_log + promotion_note → note)
    def _fold_candidate(cand: dict) -> dict:
        """Return a v7-row-shaped dict from a gapfill candidate."""
        out = {c: "" for c in master_cols}
        for c in master_cols:
            if c in cand and cand[c] is not None:
                out[c] = cand[c]
        # Fold correction_log + promotion_note into note
        cl = (cand.get("correction_log") or "").strip()
        pn = (cand.get("promotion_note") or "").strip()
        existing_note = (out.get("note") or "").strip()
        parts = []
        if existing_note:
            parts.append(existing_note)
        if cl:
            parts.append(f"[correction_log]: {cl}")
        if pn:
            parts.append(f"[promotion_note]: {pn}")
        out["note"] = " | ".join(parts)
        # Drop candidate-only columns / promotion_* from v7 row value-wise
        out["promotion_action"] = ""
        out["promotion_note"]   = ""
        return out

    for ci, cand in enumerate(gap_rows):
        pa = cand.get("promotion_action", "")

        if pa == "add_new":
            nr = _fold_candidate(cand)
            nr["_v6_idx"] = None
            nr["v7_status"] = "promoted"
            nr["v7_pending_review"] = ""
            nr["v7_change_log"] = f"gapfill_add_new: candidate row {ci} ({cand.get('name','')})"
            rows.append(nr)
            log("row_append", "gapfill_candidates", ci, None, None,
                f"add_new: {cand.get('name','')}")

        elif pa.startswith("review:"):
            nr = _fold_candidate(cand)
            nr["_v6_idx"] = None
            nr["v7_status"] = "pending_review"
            nr["v7_pending_review"] = pa  # e.g. "review:cotenant_of_HDC High Performance Data Centre (Shah Alam)"
            nr["v7_change_log"] = f"gapfill_review: candidate row {ci} ({cand.get('name','')})"
            rows.append(nr)
            log("row_append", "gapfill_candidates", ci, None, None,
                f"review appended: {cand.get('name','')} — {pa}")

        elif pa.startswith("hold:"):
            nr = _fold_candidate(cand)
            nr["_v6_idx"] = None
            nr["v7_status"] = "pending_review"
            nr["v7_pending_review"] = pa
            nr["v7_change_log"] = f"gapfill_hold: candidate row {ci} ({cand.get('name','')})"
            rows.append(nr)
            log("row_append", "gapfill_candidates", ci, None, None,
                f"hold appended: {cand.get('name','')} — {pa}")

        elif pa.startswith("update_existing:"):
            target_v6 = int(pa.split(":", 1)[1])
            tr = row_by_v6idx(target_v6)
            if tr is None:
                log("error", "gapfill_candidates", ci, target_v6, None,
                    f"update_existing:{target_v6}: target row deleted or missing")
                continue
            # Stale-index guard
            if not (_fuzzy_overlap(tr.get("name",""), cand.get("name",""))
                    or _fuzzy_overlap(tr.get("operator",""), cand.get("operator",""))):
                log("error", "gapfill_candidates", ci, target_v6, None,
                    f"stale_index: candidate row {ci} targets v6_row_{target_v6} but "
                    f"names/operators do not overlap "
                    f"(target='{tr.get('name','')}' / '{tr.get('operator','')}' vs "
                    f"candidate='{cand.get('name','')}' / '{cand.get('operator','')}'). "
                    f"No update applied.")
                continue
            # Fold correction_log + promotion_note into the candidate's effective note
            folded = _fold_candidate(cand)
            field_changes = []
            for c in master_cols:
                cv = (folded.get(c) or "").strip()
                if not cv:
                    continue
                tv = (tr.get(c) or "").strip()
                if not tv:
                    tr[c] = cv
                    field_changes.append(f"filled:{c}")
                elif tv == cv:
                    continue  # identical, no-op
                else:
                    tr[c] = f"{tv} | {cv}"
                    field_changes.append(f"appended:{c}")
            tr["v7_status"] = "updated"
            tr["v7_change_log"] = (f"gapfill_update_existing: candidate row {ci} "
                                   f"merged into v6_row_{target_v6}; " + ", ".join(field_changes))
            log("row_update", "gapfill_candidates", ci, target_v6, None,
                f"update_existing:{target_v6}: {', '.join(field_changes)}")

        elif pa.startswith("skip:"):
            log("skip", "gapfill_candidates", ci, None, None,
                f"skip: {cand.get('name','')} — {pa}")

        elif pa.startswith("reject:"):
            log("skip", "gapfill_candidates", ci, None, None,
                f"reject (carried to v8 gap): {cand.get('name','')} — {pa}")

        else:
            log("error", "gapfill_candidates", ci, None, None,
                f"unknown promotion_action {pa!r}: {cand.get('name','')}")

    # -------------------------------------------------------------------
    # STEP 5 — final dedup sweep (surface only)
    # -------------------------------------------------------------------
    # Assign provisional v7 indices for sweep reporting
    for i, r in enumerate(rows):
        r["_v7_idx"] = i

    sweep_findings: list[tuple[int, int, float, bool]] = []  # (a, b, dist, same_op)
    for i in range(len(rows)):
        ai = rows[i]
        la = _float_or_none(ai.get("lat"))
        lo = _float_or_none(ai.get("lon"))
        if la is None or lo is None:
            continue
        for j in range(i + 1, len(rows)):
            bj = rows[j]
            lb = _float_or_none(bj.get("lat"))
            lob = _float_or_none(bj.get("lon"))
            if lb is None or lob is None:
                continue
            d = _haversine_m(la, lo, lb, lob)
            if d >= 150.0:
                continue
            same_op = (_normalize(ai.get("operator","")) != "" and
                       _normalize(ai.get("operator","")) == _normalize(bj.get("operator","")))
            sweep_findings.append((i, j, d, same_op))

    for (i, j, d, same_op) in sweep_findings:
        tag = "sweep_potential_dup" if same_op else "sweep_cotenant_pair"
        for (a, b) in ((i, j), (j, i)):
            r = rows[a]
            other = rows[b]
            msg = f"{tag}:paired_with_v7_row_{other['_v7_idx']} (haversine={d:.0f}m)"
            prev = r.get("v7_pending_review") or ""
            if prev and tag in prev:
                continue  # already flagged for this type
            r["v7_pending_review"] = (prev + (" || " if prev else "") + msg)
            if r["v7_status"] not in ("pending_review", "unresolved"):
                r["v7_status"] = "pending_review"
        log("flag_added", "final_sweep", None, None, i,
            f"{tag}: v7_row_{i} ↔ v7_row_{j} @ {d:.0f}m")

    # -------------------------------------------------------------------
    # datacentermap.com holdover scan
    # -------------------------------------------------------------------
    dcm_holdovers = []
    for i, r in enumerate(rows):
        for f in ("source", "note", "_source_file"):
            v = (r.get(f) or "")
            if "datacentermap" in v.lower() or "dcm" in v.lower().split():
                dcm_holdovers.append((i, r.get("_v6_idx"), r.get("name",""), f, v[:160]))
                break

    # -------------------------------------------------------------------
    # write v7 master
    # -------------------------------------------------------------------
    with V7_MASTER.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=v7_cols)
        w.writeheader()
        for r in rows:
            out = {c: r.get(c, "") for c in v7_cols}
            w.writerow(out)

    # change log
    with V7_CHANGELOG.open("w", newline="") as f:
        fn = ["change_type","source_file","source_row_idx","v6_row_idx","v7_row_idx","description"]
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        for entry in change_log:
            w.writerow(entry)

    actual = len(rows)
    print(f"v7 row count (actual): {actual}")
    print(f"v7 row count (estimate): {estimate}")
    print(f"match: {actual == estimate}")

    # -------------------------------------------------------------------
    # build report
    # -------------------------------------------------------------------
    gap_review_rows = [r for r in gap_rows if r.get("promotion_action","").startswith("review:")]
    gap_hold_rows   = [r for r in gap_rows if r.get("promotion_action","").startswith("hold:")]
    gap_skip_rows   = [r for r in gap_rows if r.get("promotion_action","").startswith("skip:")]
    gap_reject_rows = [r for r in gap_rows if r.get("promotion_action","").startswith("reject:")]
    gap_addnew_rows = [r for r in gap_rows if r.get("promotion_action","") == "add_new"]
    gap_update_rows = [r for r in gap_rows if r.get("promotion_action","").startswith("update_existing:")]

    status_counts = Counter(r["v7_status"] for r in rows)
    pending_rows = [r for r in rows if r["v7_status"] in ("pending_review", "unresolved")]

    lines: list[str] = []
    lines.append("# v7 Master Build Report")
    lines.append("")
    lines.append("**Date:** 2026-04-17")
    lines.append("**Mode:** Option 1 (Step 4 `rejected_resolution` skipped — file does not yet exist)")
    lines.append("")
    lines.append("## Known gaps carried forward to v8")
    lines.append("")
    lines.append("5 rows rejected in `v6_gapfill_candidates` with "
                 "`promotion_action = reject:sole_source_tos_blocked` were NOT included in v7. "
                 "Their resolution (via SSM, MCMC, planning portals, and operator sources) is a "
                 "separate pass not yet run. These are known-unknowns: the facilities likely "
                 "exist but lack legitimate independent sourcing in the current pipeline.")
    lines.append("")
    for r in gap_reject_rows:
        lines.append(f"- **{r.get('name','(unnamed)')}** — operator: "
                     f"`{r.get('operator','')}` — locality: `{r.get('address','') or r.get('_source_file','')}`")
    lines.append("")
    lines.append("## Pre-flight vs actual row count")
    lines.append("")
    lines.append(f"```")
    lines.append(f"v7_estimate =  101  v6 master")
    lines.append(f"            +  {add_new}  gap add_new")
    lines.append(f"            +  {gap_review}  gap review:*")
    lines.append(f"            +  {gap_hold}  gap hold:*")
    lines.append(f"            -  {merge_confirmed}  dedup merge_confirmed")
    lines.append(f"            -  {merge_reversed}  dedup merge_reversed")
    lines.append(f"            = {estimate}")
    lines.append(f"")
    lines.append(f"v7_actual   = {actual}")
    lines.append(f"```")
    lines.append("")
    if actual == estimate:
        lines.append("✅ Match.")
    else:
        lines.append(f"❌ Mismatch of {actual - estimate}. See `v6_to_v7_change_log.csv` to reconcile.")
    lines.append("")
    lines.append("**Note on user-supplied stub formula:** the task message gave "
                 "`count(gapfill.review:*) = 3` as the stub value. The actual file has "
                 f"{gap_review} rows whose `promotion_action` starts with `review:` — "
                 "`review:cotenant_of_HDC…`, `review:possible_dup_AIMS Bangunan AIMS / KL2`, "
                 "`review:possible_dup_Open DC CJ1`, `review:possible_dup_Open DC PE1`. "
                 "I used the file's actual count; if only 3 were expected, one of these should be "
                 "reclassified before v8.")
    lines.append("")
    lines.append("## Row-count summary")
    lines.append("")
    lines.append(f"- v6 master: **{len(v6_rows)}** rows")
    lines.append(f"- v7 master: **{actual}** rows")
    lines.append(f"- Net delta: **+{actual - len(v6_rows)}**")
    lines.append("")
    lines.append("### v7_status distribution")
    for k, v in sorted(status_counts.items()):
        lines.append(f"- `{k}`: {v}")
    lines.append("")
    lines.append("## Per-category outcomes")
    lines.append("")
    lines.append("### Gapfill")
    lines.append(f"- `add_new` promoted: **{len(gap_addnew_rows)}** rows appended to v7")
    lines.append(f"- `review:*` flagged: **{len(gap_review_rows)}** rows appended as `pending_review`")
    lines.append(f"- `hold:*` flagged: **{len(gap_hold_rows)}** rows appended as `pending_review`")
    lines.append(f"- `update_existing:*` applied: **{len(gap_update_rows)}** rows merged into base")
    lines.append(f"- `skip:*` logged: **{len(gap_skip_rows)}** (no v7 row produced)")
    lines.append(f"- `reject:*` carried to v8 gap: **{len(gap_reject_rows)}** (no v7 row produced)")
    lines.append("")
    lines.append("### Dedup")
    d_counts = Counter(r["action"] for r in dedup)
    for k, v in sorted(d_counts.items()):
        lines.append(f"- `{k}`: {v}")
    lines.append("")
    lines.append("### Rejected resolution (Step 4)")
    lines.append("- **Skipped** — `v6_rejected_resolution.csv` does not yet exist. See "
                 "\"Known gaps carried forward to v8\" above for the 5 facilities awaiting resolution.")
    lines.append("")
    lines.append("## Final dedup sweep")
    lines.append("")
    potdup = [s for s in sweep_findings if s[3]]
    cotenant = [s for s in sweep_findings if not s[3]]
    lines.append(f"- Potential duplicates surfaced (same operator, <150m): **{len(potdup)}**")
    lines.append(f"- Cotenant pairs surfaced (different operator, <150m): **{len(cotenant)}**")
    if potdup:
        lines.append("")
        lines.append("### Potential duplicates")
        for (i, j, d, _) in potdup:
            lines.append(f"- v7_row_{i} `{rows[i].get('name','')}` ↔ v7_row_{j} "
                         f"`{rows[j].get('name','')}` @ {d:.0f}m")
    if cotenant:
        lines.append("")
        lines.append("### Cotenant pairs")
        for (i, j, d, _) in cotenant:
            lines.append(f"- v7_row_{i} `{rows[i].get('name','')}` ({rows[i].get('operator','')}) ↔ "
                         f"v7_row_{j} `{rows[j].get('name','')}` ({rows[j].get('operator','')}) @ {d:.0f}m")
    lines.append("")
    lines.append("## Special-case flags")
    lines.append("")
    def _find_v7(v6_idx):
        for i, r in enumerate(rows):
            if r.get("_v6_idx") == v6_idx:
                return i
        return None
    lines.append("### AIMS three-way (v6 rows 9, 74, 91)")
    for vi in (9, 74, 91):
        v7i = _find_v7(vi)
        r = row_by_v6idx(vi)
        if r is None:
            lines.append(f"- v6_row_{vi}: MISSING from v7")
        else:
            lines.append(f"- v6_row_{vi} → v7_row_{v7i}: **{r.get('name','')}** ({r.get('operator','')}) "
                         f"— `v7_pending_review`: {r.get('v7_pending_review','')[:250]}")
    lines.append("")
    lines.append("### Keppel coord caveat (v6 row 44)")
    v7i = _find_v7(44)
    r = row_by_v6idx(44)
    if r:
        lines.append(f"- v6_row_44 → v7_row_{v7i}: **{r.get('name','')}** — `v7_pending_review`: "
                     f"{r.get('v7_pending_review','')[:250]}")
    lines.append("")
    lines.append("### NTT CBJ rename + coord-fill recommendation (v6 row 73)")
    v7i = _find_v7(73)
    r = row_by_v6idx(73)
    if r:
        lines.append(f"- v6_row_73 → v7_row_{v7i}: **{r.get('name','')}** — `v7_pending_review`: "
                     f"{r.get('v7_pending_review','')[:250]}")
    lines.append("")
    lines.append("## Errors")
    lines.append("")
    errors = [e for e in change_log if e["change_type"] == "error"]
    if errors:
        for e in errors:
            lines.append(f"- {e['description']}")
    else:
        lines.append("- None")
    lines.append("")
    lines.append("## Pending-review queue")
    lines.append("")
    lines.append(f"Total rows with `v7_status` ∈ {{`pending_review`, `unresolved`}}: **{len(pending_rows)}**")
    lines.append("")
    # Group by leading reason token
    def _reason(r):
        v = r.get("v7_pending_review","") or ""
        if not v:
            return "(no reason)"
        head = v.split(":", 1)[0]
        return head[:60]
    groups: dict[str, list[tuple[int,dict]]] = {}
    for i, r in enumerate(rows):
        if r["v7_status"] not in ("pending_review", "unresolved"):
            continue
        groups.setdefault(_reason(r), []).append((i, r))
    for reason, items in sorted(groups.items()):
        lines.append(f"### `{reason}` — {len(items)} row(s)")
        for (i, r) in items:
            lines.append(f"- v7_row_{i} | v6_row_{r.get('_v6_idx','(new)')} | "
                         f"**{r.get('name','')}** ({r.get('operator','')})")
            lines.append(f"  - `v7_pending_review`: {r.get('v7_pending_review','')[:300]}")
        lines.append("")
    lines.append("## datacentermap.com holdover check")
    lines.append("")
    if not dcm_holdovers:
        lines.append("- None detected in `source`, `note`, or `_source_file` fields.")
    else:
        lines.append(f"**{len(dcm_holdovers)}** v7 row(s) still reference datacentermap / dcm "
                     "(inherited from pre-TOS-enforcement v6). These are NOT silently stripped — "
                     "flag for remediation in v8:")
        lines.append("")
        for (i, v6i, name, field, snippet) in dcm_holdovers:
            lines.append(f"- v7_row_{i} (v6_row_{v6i}): **{name}** — `{field}` contains: "
                         f"`{snippet.replace('`','')[:200]}`")
    lines.append("")
    lines.append("## Recommendations for v8")
    lines.append("")
    lines.append("1. **Run the rejected-resolution pass** on the 5 `reject:sole_source_tos_blocked` "
                 "rows (VADS ×3, AWS KUL ×2) using SSM / MCMC / planning-portal / operator-IR sources.")
    lines.append("2. **AIMS three-way consolidation** (v6 rows 9, 74, 91) — decide whether the "
                 "Cyber 3 building is one physical facility (currently represented twice: row 9 "
                 "and row 91) or two.")
    lines.append("3. **Rename NTT CBJ → CBJ1** and coord-fill from `43000 Jalan APEC, Cyberjaya 63000`.")
    lines.append("4. **Keppel DC Johor 1 coord verification** — pull Keppel DC REIT property "
                 "schedules (Bursa/SGX) or reverse-geocode (1.6686, 103.5224) against OSM to "
                 "confirm the park name.")
    lines.append("5. **Resolve the dedup-sweep findings** — review each `sweep_potential_dup` and "
                 "`sweep_cotenant_pair` pair flagged in this pass.")
    if dcm_holdovers:
        lines.append("6. **Remediate the datacentermap holdover rows** listed above — replace "
                     "`source` with operator-direct or PeeringDB/OSM URLs.")
    lines.append("")

    V7_REPORT.write_text("\n".join(lines))

    print(f"\nWrote: {V7_MASTER}")
    print(f"Wrote: {V7_CHANGELOG}  ({len(change_log)} entries)")
    print(f"Wrote: {V7_REPORT}")


if __name__ == "__main__":
    main()
