# GH Archive dataset for Datomic benchmarking

Public data: https://data.gharchive.org/YYYY-MM-DD-H.json.gz  (~180k events/hour, ~83 MB gz)
No auth, no rate limits, CC-licensed, hourly files back to 2011.

## Why this dataset
Unlike the mbrainz backup (restored pre-indexed, read-only), GH Archive is
**loaded through the transactor**, so it exercises the real write + index path:
 - `:db.unique/identity` upserts -- 722k events collapse onto 116k users /
   166k repos / 15k orgs, so most tx resolve existing entities via AVET
 - nested maps auto-upsert (event -> actor -> org) in one tx
 - `:db.type/instant` + `:db/index` for time-range queries
 - `:db/fulltext` on issue/PR titles for Lucene
 - real skew: `github-actions[bot]` is the hottest actor, giving genuine join fan-out

## Fetch
```bash
mkdir -p ~/dtm/gh && cd ~/dtm/gh
for h in 15 16 17 18; do curl -sS -O "https://data.gharchive.org/2024-01-01-$h.json.gz" & done; wait
```

## Run
```bash
cd ~/dtm/bench
clojure -J-Xmx12g -J-server -M -m bench.gharchive ~/dtm/gh/2024-01-01-1{5,6,7,8}.json.gz
```
Requires `org.clojure/data.json` in deps.edn. Creates a fresh timestamped db each run.

## Results (4 hours = 722,746 events -> 5,164,140 datoms)
Load:  65.5 s -> 11,040 events/s, 78,886 datoms/s (upsert-heavy)
Index: 3.94 s

Queries at 722k events (p50 ms): point lookup 0.020 / login AVET 0.140 /
3-join 9.4 / rule collab 161 / group-by count 401 / sum+group-by 1530 /
time window 17.9(*) / fulltext 8.9 / raw AEVT scan 24.5 / count via query 146

(*) measured at 2-hour scale

## Other datasets evaluated
- NYC TLC trips (parquet, 50MB/mo) -- flat rows, no joins; columnar workload, poor datalog fit
- OpenAlex works -- excellent graph (authors/institutions/citations) but API-paginated
- SEC EDGAR full-index -- good for time-series filings, weaker entity graph
- StackExchange dumps (archive.org) -- good Q/A/user graph, but 302-redirect + large XML
GH Archive won on: no auth, natural entity graph, real upsert pressure, time dimension.
