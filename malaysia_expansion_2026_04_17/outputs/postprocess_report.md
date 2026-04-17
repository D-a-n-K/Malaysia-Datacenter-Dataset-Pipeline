# v5 → v5.1 post-process report

Generated 2026-04-17 by `postprocess_v5.py`. Input: malaysia_datacenters_v5.csv (80 rows). Output: malaysia_datacenters_v5_1.csv (66 rows).

## Fix 1 — OSM unnamed-way attribution / clustering

* Unnamed OSM rows in v5: **15**
* Attributed to a named v5 facility within 500 m: **9** (OSM way IDs appended to that row's note)
* Clustered into aggregate rows (no named neighbour within radius): **6** OSM ways → **2** aggregate rows

## Fix 2 — intra-corpus near-duplicate merge

* `(operator_norm, name_root)` merges within 1500 m: **1**
* Rows collapsed by those merges: **2** (net −1 rows)

## Fix 3 — cloud-region pin flagging

* Rows with `facility_type='cloud_region'`: **5**
* Each received `physical_facility=FALSE` plus a note documenting the publicly-disclosed AZ count. Physical AZ-site discovery is deferred; no placeholder expansion was performed because duplicating the region centroid N times would fabricate geographic signal without adding any.

## Net effect

* v5 rows: 80
* v5.1 rows: 66 (Δ -14)
* Physical-facility rows in v5.1: 61
