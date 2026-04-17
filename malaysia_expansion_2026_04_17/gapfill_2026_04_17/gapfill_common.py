"""
gapfill_common.py — shared utilities for the 2026-04-17 gapfill sprint.

- Loads malaysia_datacenters_v6_master.csv as the dedup baseline.
- Exposes fetch_cached() against /tmp/gapfill_cache/.
- Provides a Nominatim-backed geocoder with 1s delay, descriptive UA, and a
  small Malaysia bias to reduce false matches.
- Provides dedup_check() returning (status, matched_name, distance_m) so
  each candidate row gets a promotion_action stamped automatically.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
V6_MASTER = ROOT.parent / "outputs" / "malaysia_datacenters_v6_master.csv"
CACHE_DIR = Path("/tmp/gapfill_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "ITEC724-MalaysiaDCPilot-Gapfill/1.0 "
    "(American University research; contact daniel@american.edu)"
)
REQUEST_DELAY_S = 1.0
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

V6_SCHEMA = [
    # from malaysia_datacenters_v6_master.csv in its current column order
    "name", "operator", "operator_norm", "lat", "lon",
    "v6_layer", "confidence", "promotion_action", "promotion_note",
    "source_category", "n_sources", "physical_facility",
    "address", "source", "note",
    # trailing v5/v6 columns
    "ADMIN", "ISO_A3", "CONTINENT", "SUBREGION",
    "name_normalized", "has_operator", "in_both_sources",
    "sources", "osm_id", "wikidata_id", "provider", "region_code",
    "facility_type", "status", "year", "coord_confidence",
    "cluster_id", "osm_type", "merge_status", "merge_matched_name",
    "merge_distance_m", "_source_file", "operator_tier",
]


_EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r"[^a-z0-9]+", " ", n)
    return " ".join(n.split())


def _cache_path(url: str) -> Path:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{h}.cache"


def fetch_cached(url: str, extra_headers: dict | None = None,
                 delay: float = REQUEST_DELAY_S) -> str:
    p = _cache_path(url)
    if p.exists():
        return p.read_text(encoding="utf-8", errors="replace")
    headers = {"User-Agent": USER_AGENT}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    logging.info("GET %s", url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        # Persist the failure marker so we don't retry repeatedly this session
        p.write_text(f"__HTTPERR__ {exc.code}", encoding="utf-8")
        raise
    text = raw.decode("utf-8", errors="replace")
    p.write_text(text, encoding="utf-8")
    if delay:
        time.sleep(delay)
    return text


def geocode_address(q: str, country: str = "Malaysia") -> dict | None:
    """Return {lat, lon, match, query} or None."""
    query = f"{q}, {country}"
    params = {"q": query, "format": "json", "limit": "1", "addressdetails": "1",
              "countrycodes": "my"}
    url = NOMINATIM_URL + "?" + urllib.parse.urlencode(params)
    p = _cache_path("geo:" + url)
    if p.exists():
        text = p.read_text(encoding="utf-8")
        if text.startswith("__HTTPERR__"):
            return None
    else:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=20) as resp:
                text = resp.read().decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            logging.warning("Geocode fail %s: %s", q, exc)
            return None
        p.write_text(text, encoding="utf-8")
        time.sleep(1.1)  # Nominatim 1 req/sec policy
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
            "query": query,
            "importance": hit.get("importance"),
        }
    except (KeyError, TypeError, ValueError):
        return None


@dataclass
class DedupHit:
    status: str            # "new" | "skip:duplicate_of_<n>" | "review:possible_dup_<n>" | "update_existing:<row_idx>"
    matched_name: str = ""
    matched_idx: int = -1
    distance_m: float | None = None
    reason: str = ""


def load_v6() -> pd.DataFrame:
    df = pd.read_csv(V6_MASTER)
    df["_norm_name"] = df["name"].fillna("").astype(str).apply(normalize_name)
    df["_norm_addr"] = df["address"].fillna("").astype(str).apply(normalize_name)
    return df


def _name_token_overlap(a: str, b: str) -> int:
    ta = set(a.split())
    tb = set(b.split())
    ta -= {"data", "center", "centre", "facility", "dc", "campus",
           "sdn", "bhd", "berhad", "malaysia", "the"}
    tb -= {"data", "center", "centre", "facility", "dc", "campus",
           "sdn", "bhd", "berhad", "malaysia", "the"}
    return len(ta & tb)


def dedup_check(
    v6: pd.DataFrame,
    candidate_name: str,
    candidate_operator: str,
    candidate_lat: float | None,
    candidate_lon: float | None,
    candidate_address: str = "",
    candidate_city: str = "",
) -> DedupHit:
    cand_norm = normalize_name(candidate_name)
    cand_op = normalize_name(candidate_operator)
    cand_addr = normalize_name(candidate_address)

    # Rule 3: same normalized street address
    if cand_addr and len(cand_addr) > 10:
        for idx, r in v6.iterrows():
            if r["_norm_addr"] and r["_norm_addr"] == cand_addr:
                return DedupHit(
                    status=f"skip:duplicate_of_{str(r['name'])[:40]}",
                    matched_name=str(r["name"]),
                    matched_idx=int(idx),
                    reason="same normalized street address",
                )

    # Rule 1: same-operator proximity dedup. Different operators at the same
    # building are NOT duplicates (e.g. Exabytes at Menara AIMS ≠ AIMS; AWS at
    # TPM ≠ JARING; multi-tenant carrier hotels are normal). Only skip when the
    # operator matches — otherwise flag for review at most.
    if candidate_lat is not None and candidate_lon is not None:
        for idx, r in v6.iterrows():
            try:
                v_lat = float(r["lat"])
                v_lon = float(r["lon"])
            except (TypeError, ValueError):
                continue
            import math
            if math.isnan(v_lat) or math.isnan(v_lon):
                continue
            d = haversine_m(candidate_lat, candidate_lon, v_lat, v_lon)
            v_op_norm = normalize_name(str(r["operator"]))
            same_operator = cand_op and v_op_norm == cand_op
            if d < 150.0 and same_operator:
                return DedupHit(
                    status=f"skip:duplicate_of_{str(r['name'])[:40]}",
                    matched_name=str(r["name"]),
                    matched_idx=int(idx),
                    distance_m=round(d, 1),
                    reason=f"same operator within {d:.0f}m — duplicate",
                )
            if d < 300.0 and same_operator:
                return DedupHit(
                    status=f"review:possible_dup_{str(r['name'])[:40]}",
                    matched_name=str(r["name"]),
                    matched_idx=int(idx),
                    distance_m=round(d, 1),
                    reason=f"same operator @ {d:.0f}m — possibly same campus",
                )
            # Different-operator proximity: flag in the note for human awareness
            # (co-tenant building), but don't skip.
            if d < 150.0 and _name_token_overlap(cand_norm, str(r["_norm_name"])) >= 2:
                return DedupHit(
                    status=f"review:possible_dup_{str(r['name'])[:40]}",
                    matched_name=str(r["name"]),
                    matched_idx=int(idx),
                    distance_m=round(d, 1),
                    reason=f"{d:.0f}m + 2+ shared name tokens — verify same facility",
                )

    # Rule 2: exact operator match + same city + no coords either side
    if cand_op and candidate_city:
        city_norm = normalize_name(candidate_city)
        for idx, r in v6.iterrows():
            if normalize_name(str(r["operator"])) != cand_op:
                continue
            import math
            has_coords = False
            try:
                if not math.isnan(float(r["lat"])) and not math.isnan(float(r["lon"])):
                    has_coords = True
            except (TypeError, ValueError):
                pass
            if has_coords and (candidate_lat is not None):
                continue
            addr_norm = str(r["_norm_addr"])
            if city_norm in addr_norm:
                return DedupHit(
                    status=f"review:possible_dup_{str(r['name'])[:40]}",
                    matched_name=str(r["name"]),
                    matched_idx=int(idx),
                    reason="same operator + city, uncoordinated either side",
                )

    return DedupHit(status="add_new")


def blank_candidate_row() -> dict:
    r = {c: "" for c in V6_SCHEMA}
    r["ADMIN"] = "Malaysia"
    r["ISO_A3"] = "MYS"
    r["CONTINENT"] = "Asia"
    r["SUBREGION"] = "South-Eastern Asia"
    r["v6_layer"] = "v6_gapfill"
    r["n_sources"] = 1
    r["physical_facility"] = True
    r["has_operator"] = True
    r["in_both_sources"] = False
    return r
