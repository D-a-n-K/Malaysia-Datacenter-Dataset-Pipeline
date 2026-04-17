"""
build_v7_1.py — apply 6 targeted fixes to v7 master, producing v7.1.

Fixes:
  1. AWS double-pin resolution (delete v7[68], absorb into v7[0])
  2. DayOne/GDS naming collision (v7[21] vs v7[33]) — cautious, default-keep-both
  3. NTT CBJ rename + coord-fill (v7[73])
  4. Shah Alam / Google coord collision flag (v7[69])
  5. Port Dickson pair flag (v7[127], v7[128])
  6. Racks Central cluster documentation (v7[104..106], plus YTL 53/54/55 and DayOne NTP 33/34/35)
"""
from __future__ import annotations
import csv
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "outputs"
V7_IN      = OUT / "malaysia_datacenters_v7_master.csv"
V7_1_OUT   = OUT / "malaysia_datacenters_v7_1_master.csv"
CHANGELOG  = OUT / "v7_to_v7_1_change_log.csv"
REPORT     = OUT / "v7_to_v7_1_fix_report.md"

CACHE_DIR  = Path("/tmp/gapfill_cache")

USER_AGENT = ("ITEC724-MalaysiaDCPilot-v7_1/1.0 "
              "(American University research; contact daniel@american.edu)")
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def load_v7():
    with V7_IN.open() as f:
        reader = csv.DictReader(f)
        cols = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]
    return cols, rows


def append_field(row: dict, field: str, add: str, sep: str = " | ") -> None:
    if not add:
        return
    cur = (row.get(field) or "").strip()
    row[field] = f"{cur}{sep}{add}" if cur else add


def append_change_log(row: dict, msg: str) -> None:
    cur = (row.get("v7_change_log") or "").strip()
    row["v7_change_log"] = f"{cur} | {msg}" if cur else msg


def geocode_address(q: str) -> dict | None:
    """Nominatim query with cache (matches gapfill_common.py's key scheme)."""
    params = {"q": f"{q}, Malaysia", "format": "json", "limit": "1",
              "addressdetails": "1", "countrycodes": "my"}
    url = NOMINATIM_URL + "?" + urllib.parse.urlencode(params)
    # Match gapfill_common.py cache key: hash of "geo:" + url
    import hashlib
    h = hashlib.sha1(("geo:" + url).encode("utf-8")).hexdigest()[:16]
    cpath = CACHE_DIR / f"{h}.cache"
    if cpath.exists():
        text = cpath.read_text(encoding="utf-8")
        if text.startswith("__HTTPERR__"):
            return None
        print(f"  [cache hit] {q}")
    else:
        print(f"  [live geocode] {q}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=20) as resp:
                text = resp.read().decode("utf-8")
        except Exception as exc:
            print(f"  geocode failed: {exc}")
            return None
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cpath.write_text(text, encoding="utf-8")
        time.sleep(1.1)
    try:
        data = json.loads(text)
    except Exception:
        return None
    if not data:
        return None
    hit = data[0]
    try:
        return {
            "lat": float(hit["lat"]),
            "lon": float(hit["lon"]),
            "match": hit.get("display_name", ""),
            "query": q,
            "importance": hit.get("importance"),
        }
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# change log accumulator
# ---------------------------------------------------------------------------

_change_log: list[dict] = []
def log(fix_num: int, change_type: str, v7_idx, v7_1_idx, description: str, outcome: str) -> None:
    _change_log.append({
        "fix_number": str(fix_num),
        "change_type": change_type,
        "v7_row_idx": "" if v7_idx is None else str(v7_idx),
        "v7_1_row_idx": "" if v7_1_idx is None else str(v7_1_idx),
        "description": description,
        "outcome": outcome,
    })


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    cols, rows = load_v7()
    for i, r in enumerate(rows):
        r["_v7_idx"] = i
    assert len(rows) == 134, f"expected 134 v7 rows, got {len(rows)}"

    # -------- Fix 1 — AWS double-pin ----------------------------------------
    r0  = rows[0]
    r68 = rows[68]
    # Absorb 68's source & note into 0
    if (r68.get("source") or "").strip():
        append_field(r0, "source", r68["source"].strip())
    if (r68.get("note") or "").strip():
        append_field(r0, "note", f"[v6_row_68 provenance merged v7.1]: {r68['note'].strip()}")
    if (r68.get("address") or "").strip() and not (r0.get("address") or "").strip():
        r0["address"] = r68["address"].strip()
    r0["v7_status"] = "updated"
    append_change_log(r0, "v7.1 fix 1: absorbed v7_row_68 (duplicate AWS logical-region pin); v7_row_68 deleted.")
    # Clear sweep flag pointing to the now-deleted row 68 from row 0
    pr = (r0.get("v7_pending_review") or "")
    pr_parts = [p for p in pr.split(" || ") if "paired_with_v7_row_68" not in p]
    r0["v7_pending_review"] = " || ".join(pr_parts)
    log(1, "field_append", 0, None,
        "Absorbed source/note from v7_row_68 into v7_row_0 and cleared its sweep pointer", "applied")
    # Mark row 68 for deletion
    r68["_delete"] = True
    log(1, "deletion", 68, None, "Deleted duplicate AWS logical-region pin v7_row_68", "applied")

    # Also clean other rows that had sweep pointers to 68 (row 2 had 'paired_with_v7_row_68')
    # Per guardrail #5, we only touch rows in scope. But row 2 carries a now-stale pointer.
    # Task says "Do not touch pending-review rows outside the 6 fixes" — keep that stale pointer
    # alone so the user can see it. Add a sibling note? No — leave untouched; the user's triage
    # will see the deleted row reference and resolve.

    # -------- Fix 2 — DayOne/GDS --------------------------------------------
    r21 = rows[21]
    r33 = rows[33]
    # Decision rule: r21 name "DayOne (Johor)" is generic (does not name NTP1). The note is empty.
    # r33 is NTP1-specific. Per task: "Do NOT auto-delete if this is ambiguous" —
    # default to keep-both; rename r21 as campus pin; flag for user decision.
    old_name_21 = r21["name"]
    r21["name"] = "DayOne Nusajaya Tech Park Campus"
    append_field(r21, "note",
                 f"v7.1 fix 2: renamed from '{old_name_21}' to campus-level label; "
                 f"coincident coord with v7_row_33 (GDS Nusajaya NTP1) may indicate same "
                 f"physical building — user to decide.")
    append_field(r21, "v7_pending_review",
                 "review:campus_vs_building_decision — coincident coord with v7_row_33 (NTP1); "
                 "r21 had generic 'DayOne (Johor)' name, r33 is NTP1-specific. Determine whether "
                 "to collapse or keep separate as campus-vs-building.",
                 sep=" || ")
    append_change_log(r21, "v7.1 fix 2: renamed to campus label; flagged review:campus_vs_building_decision (kept both per building-granularity default).")
    log(2, "rename", 21, None,
        f"Renamed v7_row_21 from '{old_name_21}' to '{r21['name']}' (kept both; flagged for user)",
        "flagged_for_user")
    # Mirror a one-line cross-reference on r33 so the user sees the pairing
    append_field(r33, "v7_pending_review",
                 "review:campus_vs_building_decision — paired with v7_row_21 (DayOne campus pin)",
                 sep=" || ")
    append_change_log(r33, "v7.1 fix 2: cross-ref flag added for campus-vs-building decision with v7_row_21.")
    log(2, "flag_added", 33, None, "Cross-ref flag on NTP1 row pointing to campus pin (v7_row_21)",
        "flagged_for_user")

    # -------- Fix 3 — NTT CBJ rename + coord-fill ---------------------------
    r73 = rows[73]
    old_name_73 = r73["name"]
    old_op_73   = r73["operator"]
    r73["name"] = "NTT Cyberjaya CBJ1"
    # Match row 11's operator format
    r73["operator"] = rows[11]["operator"]  # "NTT DATA"
    r73["operator_norm"] = rows[11]["operator_norm"]
    # Coord-fill from campus address
    geo_q = "43000 Jalan APEC, Cyberjaya 63000, Selangor"
    hit = geocode_address(geo_q)
    geo_note = ""
    if hit is not None:
        r73["lat"] = f"{hit['lat']:.7f}"
        r73["lon"] = f"{hit['lon']:.7f}"
        r73["coord_confidence"] = "building"
        geo_note = (f"v7.1 fix 3 geocode: query='{geo_q}' match='{hit['match'][:120]}' "
                    f"importance={hit.get('importance')}")
        log(3, "coord_added", 73, None,
            f"Coord-filled from campus address via Nominatim: ({r73['lat']}, {r73['lon']})", "applied")
    else:
        r73["coord_confidence"] = "unknown"
        geo_note = f"v7.1 fix 3 geocode FAILED for query='{geo_q}' — coord left blank, coord_confidence=unknown"
        log(3, "coord_added", 73, None,
            "Geocode failed; coords left blank, coord_confidence=unknown", "skipped_missing_data")
    # Address field
    addr = "43000 Jalan APEC, Cyberjaya 63000, Selangor"
    if addr not in (r73.get("address") or ""):
        append_field(r73, "address", addr)
    # Source: append NTT newsroom as corroboration
    ntt_url = ("https://services.global.ntt/en-US/newsroom/"
               "ntt-strengthens-commitment-to-malaysia-with-the-launch-of-cyberjaya-6-data-center")
    if ntt_url not in (r73.get("source") or ""):
        append_field(r73, "source", ntt_url)
    # Note
    append_field(r73, "note",
                 f"Renamed from '{old_name_73}' to 'NTT Cyberjaya CBJ1' per dedup resolution; "
                 f"CBJ is historical PeeringDB label for the original building, distinct from CBJ6 "
                 f"(row 11). {geo_note}")
    # Clear NTT-specific portion of v7_pending_review, keep anything else
    pr = (r73.get("v7_pending_review") or "")
    parts = pr.split(" || ")
    keep = []
    for p in parts:
        # drop the NTT dedup + special-case-rename messages; keep sweep-related or unrelated flags
        if ("not_duplicate_per_dedup_5" in p
            or "recommended for v8: rename to 'NTT Cyberjaya CBJ1'" in p):
            continue
        keep.append(p)
    r73["v7_pending_review"] = " || ".join(keep)
    r73["v7_status"] = "updated"
    append_change_log(r73, "v7.1 fix 3: renamed to CBJ1, coord-filled from campus address per dedup entry 5 recommendation.")
    log(3, "rename",   73, None,
        f"Renamed from '{old_name_73}' to '{r73['name']}'", "applied")
    log(3, "row_update", 73, None,
        f"Operator normalized from '{old_op_73}' to '{r73['operator']}'; address/source/note updated", "applied")
    log(3, "flag_removed", 73, None,
        "Cleared NTT-specific pending_review flags (not_duplicate_per_dedup_5 + rename recommendation)", "applied")

    # -------- Fix 4 — Google/i-City collision flag (row 69) -----------------
    r69 = rows[69]
    flag4 = ("v7.1 fix 4 flag: coord at (3.073, 101.519) collides with Maxis i-City (row 103) and "
             "HDC (row 114) at 30m — likely a geocoder artifact since Google's Malaysia facility "
             "is reported at Elmina/Sungai Buloh, not i-City Shah Alam. The sweep_cotenant_pair "
             "flags pointing at rows 103 and 114 from this row may be false positives. User to "
             "verify and re-source coord during manual triage.")
    append_field(r69, "v7_pending_review", flag4, sep=" || ")
    append_change_log(r69, "v7.1 fix 4: flagged coord as likely geocoder artifact colliding with i-City cluster; no data changed.")
    log(4, "flag_added", 69, None,
        "Flagged Google Selangor coord as likely Nominatim city-centroid artifact colliding with i-City cluster",
        "flagged_for_user")

    # -------- Fix 5 — Port Dickson pair -------------------------------------
    flag5 = ("v7.1 fix 5: Google Port Dickson and Gamuda Springhill share coords and may be: "
             "(a) same facility operator-vs-developer, (b) separate buildings in Springhill "
             "development, (c) phased campus. Needs user decision on row model. Do not auto-merge.")
    for idx in (127, 128):
        r = rows[idx]
        append_field(r, "v7_pending_review", flag5, sep=" || ")
        append_change_log(r, "v7.1 fix 5: flagged as operator/developer ambiguity; pending user decision.")
        log(5, "flag_added", idx, None,
            "Flagged Google/Gamuda Port Dickson coord-sharing ambiguity", "flagged_for_user")

    # -------- Fix 6 — Shared-geocode documentation -------------------------
    racks_map = {104: "RCJM1", 105: "RCJM2A", 106: "RCJM2B"}
    ytl_map   = {53: "YTL Johor DC 1", 54: "YTL Johor DC 2", 55: "YTL Johor DC 3"}
    ntp_map   = {33: "NTP1", 34: "NTP2", 35: "NTP3"}
    fix6_note = ("v7.1 fix 6: building-level row preserved per project convention; coord "
                 "inherited from shared geocode. Individual building coords require satellite "
                 "imagery review (deferred to satellite pass).")
    for cluster_label, cluster in [("racks_central", racks_map),
                                   ("ytl_johor", ytl_map),
                                   ("dayone_ntp", ntp_map)]:
        for idx, fac in cluster.items():
            r = rows[idx]
            append_field(r, "note", fix6_note)
            append_field(r, "v7_pending_review", f"pending_satellite_coord_refinement:{fac}",
                         sep=" || ")
            append_change_log(r, "v7.1 fix 6: documented shared-geocode status; coord refinement deferred.")
            log(6, "field_append", idx, None,
                f"Documented shared-geocode status for {cluster_label} member {fac}",
                "applied")

    # -------- Apply deletions ----------------------------------------------
    rows_out = [r for r in rows if not r.get("_delete")]

    # Reindex: compute new v7.1 positions and backfill into change_log
    v7_to_v71: dict[int, int] = {}
    for new_idx, r in enumerate(rows_out):
        v7_to_v71[r["_v7_idx"]] = new_idx
    for entry in _change_log:
        if entry["v7_row_idx"]:
            v7_idx = int(entry["v7_row_idx"])
            new_idx = v7_to_v71.get(v7_idx)
            if new_idx is not None:
                entry["v7_1_row_idx"] = str(new_idx)

    # -------- Write v7.1 master -----
    v7_cols = [c for c in cols]
    assert len(v7_cols) == 41, f"expected 41 cols, got {len(v7_cols)}"
    with V7_1_OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=v7_cols)
        w.writeheader()
        for r in rows_out:
            out = {c: r.get(c, "") for c in v7_cols}
            w.writerow(out)

    # -------- Write change log -----
    with CHANGELOG.open("w", newline="") as f:
        fn = ["fix_number", "change_type", "v7_row_idx", "v7_1_row_idx", "description", "outcome"]
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        for e in _change_log:
            w.writerow(e)

    # -------- Write fix report -----
    actual = len(rows_out)
    out_of_range = not (132 <= actual <= 133)
    from collections import Counter
    outcomes = Counter(e["outcome"] for e in _change_log)
    per_fix = {}
    for e in _change_log:
        per_fix.setdefault(e["fix_number"], []).append(e)

    lines = []
    lines.append("# v7 → v7.1 fix-pass report")
    lines.append("")
    lines.append("**Date:** 2026-04-17")
    lines.append("")
    lines.append(f"- v7 rows: 134")
    lines.append(f"- v7.1 rows: **{actual}** (expected 132–133)")
    lines.append(f"- Deletions: {sum(1 for e in _change_log if e['change_type']=='deletion')}")
    if out_of_range:
        lines.append(f"- ⚠️ Row count outside expected range. Deletions = "
                     f"{sum(1 for e in _change_log if e['change_type']=='deletion')}; "
                     f"expected 1 (Fix 1) or 2 (Fix 1 + Fix 2 merge-branch). Current: "
                     f"{134 - actual}. Since Fix 2 took the keep-both branch, 1 deletion is correct "
                     f"and the row count should be 133.")
    else:
        lines.append("- ✅ Row count within expected range.")
    lines.append("")
    lines.append("## Per-fix outcomes")
    lines.append("")
    outcome_by_fix = {
        "1": ("AWS double-pin", "applied"),
        "2": ("DayOne/GDS naming collision", "flagged_for_user (keep-both per building-granularity default)"),
        "3": ("NTT CBJ rename + coord-fill",
              "applied" if any(e["change_type"]=="coord_added" and e["outcome"]=="applied" for e in _change_log if e["fix_number"]=="3")
              else "partial (rename applied, geocode failed)"),
        "4": ("Google/i-City collision flag", "flagged_for_user"),
        "5": ("Port Dickson pair flag", "flagged_for_user"),
        "6": ("Racks Central / YTL / NTP documentation", "applied (doc-only)"),
    }
    for k, (desc, status) in outcome_by_fix.items():
        count = len(per_fix.get(k, []))
        lines.append(f"- **Fix {k} — {desc}**: {status} ({count} change-log entries)")
    lines.append("")
    lines.append("## Fix-2 decision record")
    lines.append("")
    lines.append("v7_row_21 (`DayOne (Johor)`, empty source/note, specific Gelang Patah street address) "
                 "and v7_row_33 (`GDS Nusajaya NTP1 (Johor)`, NTP1-specific name, GDS press-release "
                 "provenance) sit at identical coords. v7_row_21's name/note do not explicitly name "
                 "NTP1, but neither do they name NTP2 or NTP3. The task's decision rules make this "
                 "an ambiguous case. Per the user's building-level-granularity preference and the "
                 "explicit guardrail `\"Do NOT auto-delete if this is ambiguous\"`, both rows were "
                 "kept: v7_row_21 was renamed to `DayOne Nusajaya Tech Park Campus` to signal the "
                 "campus-level role, and both rows carry `review:campus_vs_building_decision` "
                 "flags for user triage.")
    lines.append("")
    lines.append("## Fix-3 geocode outcome")
    lines.append("")
    geo_entries = [e for e in _change_log if e["fix_number"]=="3" and e["change_type"]=="coord_added"]
    for e in geo_entries:
        lines.append(f"- {e['description']} — outcome: `{e['outcome']}`")
    lines.append("")
    lines.append("## Rows whose `v7_pending_review` was cleared")
    lines.append("")
    if any(e["change_type"]=="flag_removed" for e in _change_log):
        for e in _change_log:
            if e["change_type"] == "flag_removed":
                lines.append(f"- v7_row_{e['v7_row_idx']} → v7.1_row_{e['v7_1_row_idx']}: {e['description']}")
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Rows whose `v7_pending_review` was appended")
    lines.append("")
    appended = [e for e in _change_log if e["change_type"]=="flag_added"]
    for e in appended:
        lines.append(f"- v7_row_{e['v7_row_idx']} → v7.1_row_{e['v7_1_row_idx']} (fix {e['fix_number']}): {e['description']}")
    lines.append("")
    lines.append("## Sanity: rows modified outside the 6 fixes")
    lines.append("")
    touched = {int(e["v7_row_idx"]) for e in _change_log if e["v7_row_idx"]}
    expected = {0, 21, 33, 68, 69, 73, 127, 128, 104, 105, 106, 53, 54, 55, 33, 34, 35}
    unexpected = touched - expected
    if unexpected:
        lines.append(f"⚠️ Unexpected touches: {sorted(unexpected)}")
    else:
        lines.append(f"✅ All {len(touched)} touched rows are within scope: {sorted(touched)}")
    lines.append("")
    lines.append("## Outcome tally")
    lines.append("")
    for k,v in outcomes.most_common():
        lines.append(f"- `{k}`: {v}")
    lines.append("")
    REPORT.write_text("\n".join(lines))

    # -------- console summary --------
    print()
    print(f"v7.1 rows: {actual}")
    print(f"Expected range [132, 133]: {'OK' if 132 <= actual <= 133 else 'OUT OF RANGE'}")
    print(f"Change log entries: {len(_change_log)}")
    print(f"Wrote: {V7_1_OUT}")
    print(f"Wrote: {CHANGELOG}")
    print(f"Wrote: {REPORT}")


if __name__ == "__main__":
    main()
