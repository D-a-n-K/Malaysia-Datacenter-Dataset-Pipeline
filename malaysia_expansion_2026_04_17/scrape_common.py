"""
scrape_common.py — shared utilities for the MIDA / MDEC / DNB / Bursa / ST scrapers.

Design choices
--------------
* stdlib only (urllib, re, html.parser). No requests / bs4 / spaCy so we do
  not perturb the shared r-reticulate venv.
* All network fetches go through `fetch_cached` which caches bodies under
  `malaysia_expansion_2026_04_17/cache/` keyed by URL hash. Re-runs are free;
  delete the cache dir to force refresh.
* 1-second delay between live requests per scrape-politeness convention,
  0 delay on cache hits.
* Descriptive user-agent identifies the research project and provides an
  email contact per MIDA/Bursa norms.
"""

from __future__ import annotations

import dataclasses
import hashlib
import html
import logging
import math
import re
import time
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

USER_AGENT = (
    "ITEC724-MalaysiaDCPilot/1.0 "
    "(American University research; contact: daniel@american.edu)"
)
REQUEST_DELAY_S = 1.0
_EARTH_RADIUS_M = 6_371_000.0

# Used by every scraper to narrow releases to DC-relevant text.
DC_KEYWORDS = [
    "data center",
    "data centre",
    "hyperscale",
    "cloud region",
    "colocation",
    "co-location",
    "colo ",
    "digital infrastructure",
    "megawatt",
    "MW data",
    "availability zone",
    "edge facility",
]

# Malaysian DC-cluster locations. Used to infer coarse location when an exact
# address isn't in the release. Coordinates are city-centroids; rows relying
# on these get coord_confidence='geocoded_with_campus_offset' in the emitter.
MY_LOCATION_CENTROIDS: dict[str, tuple[float, float]] = {
    "Cyberjaya": (2.9188, 101.6521),
    "Putrajaya": (2.9264, 101.6964),
    "Kuala Lumpur": (3.1390, 101.6869),
    "Petaling Jaya": (3.1073, 101.6067),
    "Shah Alam": (3.0733, 101.5185),
    "Subang Jaya": (3.0438, 101.5810),
    "Sepang": (2.6895, 101.6993),
    "Bangi": (2.9083, 101.7825),
    "Klang": (3.0446, 101.4450),
    "Kulai": (1.6607, 103.6060),
    "Nusajaya": (1.4316, 103.6200),
    "Iskandar Puteri": (1.4316, 103.6200),
    "Iskandar": (1.4927, 103.7414),
    "Johor Bahru": (1.4927, 103.7414),
    "Johor": (1.4854, 103.7618),
    "Kempas": (1.5538, 103.6988),
    "Senai": (1.5960, 103.6630),
    "Sedenak": (1.6378, 103.4811),
    "Pasir Gudang": (1.4737, 103.8918),
    "Melaka": (2.1896, 102.2501),
    "Penang": (5.4164, 100.3327),
    "Bayan Lepas": (5.2946, 100.2670),
    "Batu Kawan": (5.2213, 100.4523),
    "Nilai": (2.8116, 101.7970),
    "Port Dickson": (2.5233, 101.7951),
    "Seremban": (2.7297, 101.9381),
    "Selangor": (3.0738, 101.5183),
    "Negeri Sembilan": (2.7297, 101.9381),
    "Sarawak": (1.5533, 110.3592),
    "Kuching": (1.5533, 110.3592),
    "Bintulu": (3.1667, 113.0333),
}

log = logging.getLogger("malaysia_expansion")


def _cache_path(url: str) -> Path:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{h}.cache"


def fetch_cached(url: str, delay: float = REQUEST_DELAY_S) -> str:
    """GET `url`, honoring a local file cache. Returns decoded text."""
    p = _cache_path(url)
    if p.exists():
        return p.read_text(encoding="utf-8", errors="replace")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    log.info("GET %s", url)
    with urllib.request.urlopen(req, timeout=45) as resp:
        raw = resp.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="replace")
    p.write_text(text, encoding="utf-8")
    if delay:
        time.sleep(delay)
    return text


class _TextExtractor(HTMLParser):
    """Strip tags to plain text, keeping paragraph/newline structure."""

    _BLOCK_TAGS = {
        "p", "br", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6",
        "article", "section", "header", "footer", "blockquote",
    }
    _DROP_TAGS = {"script", "style", "noscript", "nav", "aside", "form"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._drop_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._DROP_TAGS:
            self._drop_depth += 1
        if tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._DROP_TAGS and self._drop_depth:
            self._drop_depth -= 1
        if tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        if self._drop_depth:
            return
        self.parts.append(data)

    def text(self) -> str:
        raw = "".join(self.parts)
        # collapse whitespace but preserve paragraph breaks
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def html_to_text(source: str) -> str:
    extractor = _TextExtractor()
    extractor.feed(html.unescape(source))
    return extractor.text()


def extract_between(source: str, start: str, end: str) -> str | None:
    i = source.find(start)
    if i == -1:
        return None
    j = source.find(end, i + len(start))
    if j == -1:
        return None
    return source[i + len(start):j]


# ---------------------------------------------------------------------------
# Information extraction primitives
# ---------------------------------------------------------------------------

_CAPEX_RE = re.compile(
    r"(?:RM|MYR|US\$|USD|\$)\s?"
    r"(?P<amount>\d{1,4}(?:[.,]\d{1,3})?)\s?"
    r"(?P<unit>billion|bn|million|mil|m|b)\b",
    re.IGNORECASE,
)

_MW_RE = re.compile(
    r"(?P<amount>\d{1,4}(?:[.,]\d{1,3})?)\s?"
    r"(?:MW|megawatt)s?\b",
    re.IGNORECASE,
)

_YEAR_RE = re.compile(r"\b(20[12]\d)\b")

_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})\s+"
    r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)


def find_capex(text: str) -> list[str]:
    return [m.group(0).strip() for m in _CAPEX_RE.finditer(text)]


def find_megawatts(text: str) -> list[str]:
    return [m.group(0).strip() for m in _MW_RE.finditer(text)]


def find_years(text: str) -> list[str]:
    return sorted(set(_YEAR_RE.findall(text)))


def find_dates(text: str) -> list[str]:
    return sorted({m.group(0) for m in _DATE_RE.finditer(text)})


def find_operators(text: str, operators: list[str]) -> list[str]:
    """
    Match operators as whole words. Short all-caps brands (≤5 chars, e.g.
    AIMS, AWS, GCP, TM, VADS) are matched case-sensitively so that English
    verbs like "aims"/"opens"/"edge" don't get spuriously classified as
    operator brands.
    """
    hits: list[str] = []
    for op in operators:
        if not op:
            continue
        pattern = r"\b" + re.escape(op) + r"\b"
        flags = 0 if (len(op) <= 5 and op.isupper()) else re.IGNORECASE
        if re.search(pattern, text, flags=flags):
            hits.append(op)
    return hits


def find_locations(text: str) -> list[tuple[str, float, float]]:
    hits = []
    for name, (lat, lon) in MY_LOCATION_CENTROIDS.items():
        if re.search(rf"\b{re.escape(name)}\b", text, flags=re.IGNORECASE):
            hits.append((name, lat, lon))
    return hits


def load_operators(path: Path) -> list[str]:
    return [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]


# ---------------------------------------------------------------------------
# Deduplication against v5
# ---------------------------------------------------------------------------

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r"[^a-z0-9]+", " ", n)
    return " ".join(n.split())


@dataclasses.dataclass
class DedupHit:
    status: str       # "duplicate_exact_name" | "duplicate_proximity" | "new"
    matched_name: str | None = None
    matched_lat: float | None = None
    matched_lon: float | None = None
    distance_m: float | None = None


def dedupe_against_v5(
    candidate_name: str,
    candidate_lat: float | None,
    candidate_lon: float | None,
    v5_rows: list[dict],
    radius_m: float = 300.0,
) -> DedupHit:
    cand_norm = normalize_name(candidate_name)
    if cand_norm:
        for v in v5_rows:
            if normalize_name(str(v.get("name", ""))) == cand_norm:
                return DedupHit(
                    status="duplicate_exact_name",
                    matched_name=v.get("name"),
                    matched_lat=v.get("lat"),
                    matched_lon=v.get("lon"),
                )
    if candidate_lat is not None and candidate_lon is not None:
        for v in v5_rows:
            vlat = v.get("lat")
            vlon = v.get("lon")
            try:
                vlat_f = float(vlat)
                vlon_f = float(vlon)
            except (TypeError, ValueError):
                continue
            d = haversine_m(candidate_lat, candidate_lon, vlat_f, vlon_f)
            if d <= radius_m:
                return DedupHit(
                    status="duplicate_proximity",
                    matched_name=v.get("name"),
                    matched_lat=vlat_f,
                    matched_lon=vlon_f,
                    distance_m=round(d, 1),
                )
    return DedupHit(status="new")


# ---------------------------------------------------------------------------
# v5 schema helpers
# ---------------------------------------------------------------------------

V5_COLUMNS = [
    "name", "operator", "lat", "lon", "sources", "osm_id", "wikidata_id",
    "provider", "region_code", "facility_type", "n_sources", "ADMIN",
    "ISO_A3", "CONTINENT", "SUBREGION", "name_normalized", "has_operator",
    "in_both_sources", "source_category", "operator_norm", "status", "year",
    "coord_confidence", "cluster_id", "address", "source", "note", "osm_type",
]


def blank_v5_row() -> dict:
    r = {c: "" for c in V5_COLUMNS}
    r["ADMIN"] = "Malaysia"
    r["ISO_A3"] = "MYS"
    r["CONTINENT"] = "Asia"
    r["SUBREGION"] = "South-Eastern Asia"
    r["n_sources"] = 1
    r["has_operator"] = True
    r["in_both_sources"] = False
    return r
