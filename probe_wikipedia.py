"""Diagnostic probe for the Wikipedia pipeline's low-yield outcome."""
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
import wikipedia_scrape as mod

UA = "ITEC724-DatacenterPilot/1.0 (probe; daniel@american.edu)"
cache_dir = Path(__file__).parent / "cache"
client = mod.CachedClient(UA, cache_dir)

# Replay seed → wikitext
print("=== Replaying seed resolution + wikitext fetch ===")
seeds = mod.SEED_TITLES
props = mod.mw_pageprops(client, seeds)
ok = [t for t, v in props.items() if v.get("exists")]
wikitexts = mod.mw_wikitext(client, ok)
print(f"Seed titles that exist: {len(ok)}")
print(f"Wikitexts fetched: {len(wikitexts)}")

# Step 3: link expansion with the same regex
dc_pat = re.compile(r"data\s*cent(?:er|re)|hyperscale|colocation", re.IGNORECASE)
candidate_links = set()
for title, text in wikitexts.items():
    for link in mod.extract_wikilinks(text):
        if dc_pat.search(link):
            candidate_links.add(link)

print(f"\n=== Candidate linked articles ({len(candidate_links)}) ===")
link_props = mod.mw_pageprops(client, sorted(candidate_links))
link_ok = [t for t, v in link_props.items() if v.get("exists")]
print(f"Exist on Wikipedia: {len(link_ok)}  |  Redlinks: {len(candidate_links) - len(link_ok)}")
link_texts = mod.mw_wikitext(client, link_ok)

# Dump each candidate with pass/fail + which tokens matched
print(f"\n{'title':<60} | malaysia? | tokens matched")
print("-" * 100)
for t in sorted(link_ok):
    text = link_texts.get(t, "")
    lower = text[:50_000].lower()
    matched = [tok for tok in mod.MY_PLACE_TOKENS if tok in lower]
    status = "PASS" if matched else "FAIL"
    print(f"{t[:58]:<60} | {status:<9} | {', '.join(matched[:5])}")

# Also dump the redlinks (titles with DC keyword but no article)
redlinks = sorted(set(candidate_links) - set(link_ok))
if redlinks:
    print(f"\n=== Redlinks (DC-keyword titles with no Wikipedia article, {len(redlinks)}) ===")
    for t in redlinks:
        print(f"  - {t}")

# Test a LOOSER SPARQL variant — drop the subclass traversal, just instance-of
print("\n\n=== Testing looser SPARQL variants against live Wikidata ===")
variants = {
    "v1 original (P31/P279* data center + P17 Malaysia)": f"""
SELECT ?item ?itemLabel WHERE {{
  ?item wdt:P31/wdt:P279* wd:{mod.DATA_CENTER_QID} .
  ?item wdt:P17 wd:Q833 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",
    "v2 direct instance-of only (no subclass traversal)": f"""
SELECT ?item ?itemLabel WHERE {{
  ?item wdt:P31 wd:{mod.DATA_CENTER_QID} .
  ?item wdt:P17 wd:Q833 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
""",
    "v3 country-only, label contains 'data center' or 'data centre'": """
SELECT ?item ?itemLabel WHERE {
  ?item wdt:P17 wd:Q833 .
  ?item rdfs:label ?lbl .
  FILTER(LANG(?lbl) = "en")
  FILTER(CONTAINS(LCASE(?lbl), "data center") || CONTAINS(LCASE(?lbl), "data centre"))
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT 50
""",
    "v4 global count of Wikidata 'data center' instances": f"""
SELECT (COUNT(DISTINCT ?item) AS ?n) WHERE {{
  ?item wdt:P31/wdt:P279* wd:{mod.DATA_CENTER_QID} .
}}
""",
}
for name, q in variants.items():
    r = requests.get(
        mod.WIKIDATA_SPARQL,
        params={"query": q, "format": "json"},
        headers={"User-Agent": UA, "Accept": "application/sparql-results+json"},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"[{name}] HTTP {r.status_code}")
        continue
    bindings = r.json()["results"]["bindings"]
    print(f"\n[{name}] → {len(bindings)} results")
    for b in bindings[:10]:
        if "itemLabel" in b:
            print(f"  - {b['item']['value'].rsplit('/', 1)[-1]}  {b['itemLabel']['value']}")
        elif "n" in b:
            print(f"  count = {b['n']['value']}")
