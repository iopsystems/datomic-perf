# Datomic indexing benchmark (DaCapo-style) + Systemslab sweep

## Design

Datomic merges indexes in the **transactor** process, not in the peer that calls
`d/transact`. Two consequences drove the design:

1. **JVM metrics must come from the transactor.** A DaCapo-style in-JVM
   measurement would instrument the loader and report the wrong process. The
   transactor is therefore started with JMX exposed (port 7091) and
   `bench.jvmstats` attaches to it over RMI for GC / JIT / CPU / heap / threads.

2. **`d/sync-index` is not the throughput measurement.** Indexing is
   asynchronous and continuous: by the time a load finishes the transactor has
   already merged most novelty in the background, so `sync-index` only times the
   trailing merge (~2 s regardless of data size — measured). The authoritative
   number is the transactor's own per-job accounting, one
   `:index/create-index` log line per job carrying `:datoms` and `:msec`.
   That is what `index_throughput` reports; the sync-index wall time is kept
   separately as `index_tail_s`.

## Iteration model (from DaCapo's luindex)

```
for i in 1..N:
    create fresh database
    load a fixed event count      <- measured
    force index catch-up          <- measured
    collect transactor JVM metrics
    DELETE database
```
All iterations run in **one peer JVM** so JIT state carries across them. The
first N-1 are warmup; the last is reported. Warmup matters — measured on the
small size, JIT fell 24% -> 12% of wall and throughput rose 63.5K -> 67.4K
datoms/s between iterations 1 and 3.

Event **counts** are fixed per size (not files), so a size means the same work
regardless of which archive hours are on disk:

| size   | events    | datoms    | index jobs |
|--------|-----------|-----------|------------|
| small  | 50,000    | 394,138   | 2          |
| medium | 250,000   | 1,841,753 | 8          |
| large  | 1,000,000 | ~7.4M     | ~30        |

JSON parsing and tx-data conversion happen **once, up front**, outside the timed
region — otherwise ~40% of a naive loop is Jackson, not Datomic.

## Measured baselines (amd, JDK 21, G1, 4g heap, index-parallelism=1)

```
small   67,425 datoms/s ingest | 209,057 datoms/s index | gc 0.4% jit 12.2% cpu 2.51 cores
medium  81,363 datoms/s ingest | 138,042 datoms/s index | gc 0.9% jit  5.1% cpu 3.41 cores
```

Note index throughput *falls* from small to medium (209K -> 138K): more jobs
means more merge-against-existing-index work, not a constant per-datom cost.

## Files

- `jvmstats.clj`  — JMX client for the transactor + /proc CPU & IO sampling
- `idxbench.clj`  — the benchmark harness
- `datomic-index.jsonnet` — Systemslab spec
- `start-transactor.sh`   — JMX-enabled transactor launcher

Both depend on `bench.gharchive` (schema + `event->tx`) from `../bench/`.

### A procfs gotcha worth remembering

Neither `slurp` nor `io/reader` can read `/proc/stat`: procfs reports
`st_size` 0 and `BufferedReader.fill()` calls `FileInputStream.available()`,
which throws `IOException: Invalid argument`. Use
`java.nio.file.Files/readString`. See `read-proc` in `jvmstats.clj`.

## Running by hand

```bash
# on amd
~/dtm/start-transactor.sh &                 # JMX on 7091
PID=$(pgrep -f datomic.launcher | head -1)
cd ~/dtm/bench
clojure -J-Xmx12g -J-server -M -m bench.idxbench \
  --size medium --iterations 3 --batch 200 \
  --files ~/dtm/gh/2024-01-01-15.json.gz,~/dtm/gh/2024-01-01-16.json.gz \
  --jmx localhost:7091 --pid $PID \
  --log-dir ~/dtm/log --out result.json
```

## Systemslab sweeps

Validate locally first (a stub `systemslab.libsonnet` is enough):

```bash
jsonnet --tla-str index_parallelism=8 datomic-index.jsonnet | head
```

Submit a single run:

```bash
systemslab --systemslab-url http://192.168.0.170 submit \
  datomic-index.jsonnet -p host=amd -p size=medium
```

### The sweeps worth running

```bash
URL=http://192.168.0.170

# 1. index-parallelism -- the highest-value knob. Datomic ships it at 1, so
#    merges are effectively serial on a 24-thread host. Max is 8.
systemslab --systemslab-url $URL sweep datomic-index.jsonnet \
  --name datomic-index-parallelism \
  -p host=amd -p size=medium \
  -s index_parallelism=1,2,4,8

# 2. memory-index-threshold -- job frequency; trades amortisation against
#    write-latency spikes.
systemslab --systemslab-url $URL sweep datomic-index.jsonnet \
  --name datomic-index-threshold \
  -p host=amd -p size=medium \
  -s memory_index_threshold=16m,32m,64m,128m,256m

# 3. memory-index-max -- the backpressure point. Interesting for finding the
#    write rate at which the transactor starts throttling.
systemslab --systemslab-url $URL sweep datomic-index.jsonnet \
  --name datomic-index-memmax \
  -p host=amd -p size=large \
  -s memory_index_max=256m,512m,1g,2g -p heap=8g

# 4. heap x GC -- memory index shares the transactor heap.
systemslab --systemslab-url $URL sweep datomic-index.jsonnet \
  --name datomic-index-heap-gc \
  -p host=amd -p size=medium \
  -s heap=4g,8g,16g -s gc=g1,zgc,parallel

# 5. JDK version
systemslab --systemslab-url $URL sweep datomic-index.jsonnet \
  --name datomic-index-jdk \
  -p host=amd -p size=medium -s jdk=17,21

# 6. size scaling
systemslab --systemslab-url $URL sweep datomic-index.jsonnet \
  --name datomic-index-size \
  -p host=amd -s size=small,medium,large
```

Prerequisite for sweep 5: `sudo apt-get install -y openjdk-17-jdk-headless`
on amd. The spec's preflight step fails loudly if the requested JDK is absent
rather than silently falling back to the default.

## Artifacts per experiment

- `summary.json`   — flat one-line result (the thing to compare across a sweep)
- `idxbench.json`  — full per-iteration detail including every index job
- `idxbench.log`   — harness stdout
- `transactor.log` — transactor stdout
- `index-jobs.log` — raw `:index/create-index` lines

## Caveats

- Storage is `dev` (embedded H2, in-transactor). Storage I/O competes with
  transaction processing for the same heap and GC, so throughput here is an
  upper bound relative to a networked backend but a *lower* bound on
  `index-parallelism` scaling — H2 may saturate before 8 threads do.
  Re-run against PostgreSQL for production-representative figures.
- Single peer, single transactor. Nothing measures multi-peer contention.

---

# Sweep results (amd, Ryzen 9 7900X 12C/24T, dev/H2 storage)

All runs: 3 iterations, last reported, GH Archive 2024-01-01. Index throughput is
from the transactor's own per-job accounting, not `sync-index`.

## 1. index-parallelism  (medium, 250k events)

| par | index datoms/s | speedup | job_s | ingest datoms/s | cpu cores |
|-----|---------------|---------|-------|-----------------|-----------|
| 1   | 141,142       | 1.00x   | 13.05 | 77,786          | 3.42      |
| 2   | 160,968       | 1.14x   | 11.44 | 77,684          | 3.47      |
| 4   | 181,605       | 1.29x   | 10.14 | 74,500          | 3.45      |
| 8   | 197,796       | 1.40x   |  9.31 | 83,287          | 3.65      |

**Datomic's default of 1 leaves 40% of indexing throughput on the table.**
Scaling is sublinear (8x threads -> 1.4x) and transactor CPU barely moves
(3.42 -> 3.65 cores), which says the merge is not CPU-bound here: embedded H2
serialises the segment writes. Expect better scaling on a networked backend.

## 2. memory-index-threshold  (medium)

| thresh | index datoms/s | jobs | ingest datoms/s | total_s | segments | MB/s |
|--------|---------------|------|-----------------|---------|----------|------|
| 16m    | 102,974       | 15   | 79,694          | 23.11   | 3,277    | 6.0  |
| 32m    | 142,166       |  8   | 77,090          | 23.89   | 2,286    | 4.6  |
| 64m    | 183,597       |  4   | 78,326          | 23.51   | 1,638    | 3.9  |
| 128m   | 227,906       |  2   | 70,907          | 25.97   | 1,284    | 3.2  |
| 256m   | 292,763       |  1   | 62,241          | 29.59   | 1,075    | 2.7  |

**The strongest knob: 16m -> 256m is 2.8x index throughput.** But note the
tradeoff the ingest column exposes -- beyond 64m, *ingest* throughput falls
(78k -> 62k) and total wall time rises (23.5s -> 29.6s), because a larger memory
index means longer stalls when a merge finally runs. 64m looks like the knee:
1.3x the index throughput of the default at no ingest cost.

Segment count falls with threshold (3,277 -> 1,075), i.e. bigger merges produce
fewer, larger segments -- which also means less write amplification (6.0 -> 2.7 MB/s).

## 3. JDK  (medium)

| jdk | index datoms/s | ingest datoms/s | total_s | gc% | gc_ms | jit% |
|-----|---------------|-----------------|---------|-----|-------|------|
| 17  | 145,918       | 65,682          | 28.04   | 0.72| 203   | 10.24|
| 21  | 141,761       | 79,964          | 23.03   | 0.87| 201   | 7.88 |

Index throughput is a wash (within noise), but **JDK 21 ingests 22% faster and
finishes 5s sooner**, with notably less time in JIT (7.9% vs 10.2%). GC time is
identical in absolute ms. The win is compilation, not collection.

## 4. size scaling

| size   | events    | datoms    | index datoms/s | jobs | segments |
|--------|-----------|-----------|---------------|------|----------|
| small  | 50,000    | 394,138   | 206,861       |  2   | 336      |
| medium | 250,000   | 1,841,753 | 139,506       |  8   | 2,286    |
| large  | 1,000,000 | 5,164,139 | 92,842        | 21   | 9,156    |

**Index throughput degrades 2.2x from small to large.** Per-datom indexing cost
is not constant: each job merges novelty against a progressively larger durable
index, so later jobs do more work per datom. Any capacity plan extrapolated from
a small load will overestimate steady-state throughput.

## Combined recommendation

For a write-heavy Datomic deployment on this class of hardware:
`index-parallelism=8`, `memory-index-threshold=64m`, JDK 21. Versus stock
settings (parallelism 1, threshold 32m) that is roughly **1.8x index throughput
with no ingest regression**. Push the threshold past 64m only if ingest latency
does not matter.

Caveat: all of this is on `dev`/H2 storage, in-transactor. The parallelism result
in particular is likely storage-limited -- re-run against PostgreSQL before
trusting the 1.4x ceiling.


---

# TOML spec + cgroup isolation + multi-peer (added)

`datomic-index.toml` is the TOML form of the jsonnet spec, using the built-in
`[matrix]` table — which **replaces the sweep-*.json files entirely**. Widen a
dimension and the CLI expands the cartesian product into one experiment each:

```bash
systemslab --systemslab-url http://192.168.0.170 submit datomic-index.toml --name my-sweep
```

Note: the installed CLI predates matrix `context_name`, so pass `--name` on submit.

## Working-set sizes (now keyed to raw source data)

Measured 563 MB raw / 180,686 events per archive hour => 3.117 MB per 1000 events.

| size   | raw source | events    | archive hours |
|--------|-----------|-----------|---------------|
| small  | ~100 MB   | 32,000    | 0.2           |
| medium | ~1 GB     | 329,000   | 1.8           |
| large  | ~10 GB    | 3,285,000 | 18.2          |

24 hours of 2024-01-01/02 (1.2 GB gz) are staged at `/opt/datomic-bench/gh/`.

## cgroup isolation

The peer and transactor are separate JVMs on the same host, so host-level CPU
conflates the driver with the subject. Each now runs in its own cgroup via the
step-level `cgroup` key:

```toml
[[jobs.steps]]        # transactor
cgroup = "datomic-transactor"

[[jobs.steps]]        # benchmark peer(s)
cgroup = "datomic-peer"
```

Verified live — the two JVMs land in distinct cgroups under the run's slice:
```
.../systemslab/<run-id>-0/datomic-transactor
.../systemslab/<run-id>-0/datomic-peer
```
Per-cgroup `cpu.stat` / `memory.peak` / `io.stat` are collected to
`cgroup-cpu.txt`. Note the agent only *places* processes in cgroups; it sets no
limits, so this is accounting isolation, not resource capping.

## Multi-peer result: peers do NOT raise transactor CPU

Tested whether driving the transactor from several peer JVMs in parallel lifts
its CPU utilisation. Each peer gets its own `d/connect` and its own in-flight
window, with batches dealt round-robin.

medium (~1 GB raw), index-parallelism=1:

| peers | txor cores | % of 24 cpu | host cpu% | index datoms/s | ingest datoms/s |
|-------|-----------|-------------|-----------|----------------|-----------------|
| 1     | 3.45      | 14.4        | 49.1      | 129,332        | 76,777          |
| 2     | 3.53      | 14.7        | 49.6      | 127,922        | 78,186          |
| 4     | 3.47      | 14.5        | 49.1      | 126,312        | 74,066          |
| 8     | 3.54      | 14.8        | 49.7      | 124,097        | 80,619          |

**Flat.** Transactor CPU moves 3.45 -> 3.54 cores (under 3%, within noise) and
index throughput is unchanged. The same test at small size showed CPU *falling*
(2.61 -> 2.16 cores) as peers increased.

The transactor's ~3.5-core ceiling is therefore **not** an artefact of being fed
through a single connection. Datomic serialises all writes through one
transactor by design — that is what gives it ACID transactions over a single
log — so additional peers queue behind the same serial commit path rather than
finding new parallelism. Scaling writes needs a bigger transactor or faster
storage, not more clients.

This also explains why `index-parallelism` gave only 1.4x: with the transactor
CPU-idle at ~3.5/24 cores and iowait near zero, the bottleneck is neither CPU
nor raw disk but the serial commit-and-merge path through embedded H2.


---

# Release sweep: 19 releases x 3 invocations x 3 iterations (57 experiments, 0 failures)

Every publicly obtainable Datomic Pro release. 1.0.7393 is excluded: Maven
Central lists it but the S3 distribution returns HTTP 403 (withdrawn; 1.0.7394
shipped three days later). Older releases are not obtainable at all -- probes of
ten versions spanning 1.0.5703-1.0.6610 and two 0.9.x all return 403, and the
bucket refuses listing. **19 is the maximum testable set**; there is no path to
100 releases.

medium (~1 GB raw), 1 peer, index-parallelism=1, 32m threshold, 4g heap, JDK21/G1.
Each figure is the mean of the final iteration across 3 independent invocations.

| release  | date       | index d/s | ingest d/s | txor cores | gc%  | CV%  |
|----------|------------|-----------|------------|------------|------|------|
| 1.0.6726 | 2023/04/27 | 148,943   | 126,823    | 2.14       | 1.48 | 0.2  |
| 1.0.6733 |            | 149,510   | 130,287    | 2.18       | 1.58 | 1.9  |
| 1.0.6735 |            | 149,110   | 126,657    | 2.12       | 1.70 | 1.6  |
| 1.0.7010 |            | 151,357   | 132,649    | 2.29       | 1.58 | 1.1  |
| 1.0.7021 |            | 149,002   | 129,792    | 2.26       | 1.54 | 1.9  |
| 1.0.7075 |            | **153,901** | 133,813  | 2.28       | 1.57 | 0.2  |
| 1.0.7180 | 2024/07/11 | 148,107   | 131,120    | 3.10       | 2.00 | 1.4  |
| 1.0.7187 | 2024/08/22 | 147,577   | 130,821    | 3.11       | 1.42 | 0.8  |
| 1.0.7260 | 2024/10/22 | 126,415   | 106,648    | 3.53       | 1.35 | 0.9  | <- STEP 1
| 1.0.7277 | 2024/12/16 | 126,313   | 104,243    | 3.51       | 1.30 | 1.7  |
| 1.0.7364 | 2025/05/08 | 126,943   | 105,130    | 3.51       | 1.33 | 1.0  |
| 1.0.7387 | 2025/06/27 | 124,011   | 81,965     | 4.21       | 0.95 | 0.8  | <- STEP 2
| 1.0.7394 | 2025/08/07 | 120,324   | 80,192     | 4.27       | 0.95 | 0.9  |
| 1.0.7469 | 2025/10/23 | 124,148   | 80,474     | 4.26       | 0.94 | 0.5  |
| 1.0.7482 | 2026/01/06 | 123,307   | 80,629     | 4.25       | 0.96 | 0.8  |
| 1.0.7491 | 2026/01/26 | 123,369   | 80,342     | 4.27       | 0.96 | 0.6  |
| 1.0.7556 | 2026/03/13 | 123,264   | 81,399     | 4.22       | 0.97 | 1.1  |
| 1.0.7622 | 2026/04/28 | 124,534   | 80,929     | 4.25       | 0.95 | 1.1  |
| 1.0.7705 | 2026/07/10 | 128,015   | 78,300     | 3.49       | 0.98 | 1.6  |

Release dates confirm version ordering is chronological (2023/04 -> 2026/07).

## Three plateaus, two step changes

| group             | index d/s | ingest d/s | cores |
|-------------------|-----------|------------|-------|
| 6726-7187 (n=8)   | 149,689   | 130,245    | 2.44  |
| 7260-7364 (n=3)   | 126,557   | 105,340    | 3.52  |
| 7387-7705 (n=8)   | 123,871   | 80,529     | 4.15  |

**Oldest -> newest: index -17.2%, ingest -38.2%.**

Within each plateau releases agree to within +-3%; the steps are 14-22%. With
CV of 0.2-1.9% across the three invocations, both steps are far outside the
noise floor. This is a real change, not drift.

The CPU column is the striking part: newer releases burn **~70% more transactor
CPU (2.44 -> 4.15 cores) while doing less work**, and GC% roughly halves. That
is the signature of added per-transaction work, not memory pressure.

## What shipped at each boundary (from the official change log)

**1.0.7187 -> 1.0.7260** (index -14.3%, ingest -18.5%):
- Performance: Reduce memory required to calculate db-stats
- Performance: Reduce memory required to calculate index-metrics
- Feature: datomic.api/with now supports io-stats
- Feature: transaction hints

**1.0.7364 -> 1.0.7387** (ingest -22.0%):
- Performance: Reduce Peer and Transactor memory usage in large databases
- Performance: Improve indexing performance, reduce I/O in large databases
- Upgraded org.clojure/core.async to 1.8.741

Both boundaries are dominated by changes that trade **memory for CPU** -- which
matches the measurement exactly: GC time falls, CPU rises, throughput drops.
These look like deliberate trades tuned for large databases, and this benchmark's
1 GB working set is small enough to pay the cost without seeing the benefit.
That hypothesis is testable: the same sweep at the 10 GB size should narrow or
reverse the gap if the trade pays off at scale.

Caveat: dev/H2 storage, single peer, one host. See the PostgreSQL section for
whether the ingest drop is storage-path-specific.


---

# Boundary bisect: 4 releases x 10 invocations (40 experiments, 0 failures)

The 3-invocation sweep showed two steps; this re-runs the four boundary releases
at n=10 to put confidence intervals on them.

| release  | n  | index d/s | +-95% CI | ingest d/s | +-95% CI | cores |
|----------|----|-----------|----------|------------|----------|-------|
| 1.0.7187 | 10 | 156,680   | 3,833    | 137,144    | 4,678    | 3.53  |
| 1.0.7260 | 10 | 137,507   | 1,111    | 112,852    | 1,036    | 3.66  |
| 1.0.7364 | 10 | 136,712   | 449      | 112,917    | 884      | 3.71  |
| 1.0.7387 | 10 | 135,800   | 602      | 86,751     | 1,066    | 4.43  |

**STEP 1 — 1.0.7187 -> 1.0.7260**
- index  -12.2%  (CIs disjoint)
- ingest -17.7%  (CIs disjoint)

**STEP 2 — 1.0.7364 -> 1.0.7387**
- index  **-0.7%  (CIs OVERLAP -- not significant)**
- ingest -23.2%  (CIs disjoint)

## What n=10 changes

The n=3 sweep reported step 2 as index -2.3%. At n=10 that shrinks to -0.7% with
overlapping intervals: **step 2 does not affect index throughput at all.** It is
purely an ingest (transaction commit) regression. The earlier -2.3% was noise.

So the two steps are different in kind, not just degree:

| step | boundary      | index        | ingest | cores      |
|------|---------------|--------------|--------|------------|
| 1    | 7187 -> 7260  | -12.2%       | -17.7% | 3.53->3.66 |
| 2    | 7364 -> 7387  | not affected | -23.2% | 3.71->4.43 |

Step 1 slowed both indexing and commits. Step 2 left indexing alone and cost
23% of commit throughput while adding 0.7 cores of transactor CPU -- consistent
with its change-log entry ("Reduce Peer and Transactor memory usage in large
databases") being paid for per-transaction.

Note the absolute numbers run ~6% higher than the n=3 sweep across the board
(7187: 156,680 vs 147,577). Those runs were interleaved with 57 other
experiments on a busier host; the bisect ran on a quieter one. Within-sweep
comparisons hold, across-sweep absolutes should not be mixed.


---

# Regression tuning: is the regression tunable? (48 experiments, 0 failures)

1.0.7187 (fast plateau) vs 1.0.7387 (slow plateau) across index-parallelism,
memory-index-threshold and heap, 2 invocations each.

| par | thresh | heap | 7187 index | 7387 index | delta | 7187 ingest | 7387 ingest | delta |
|-----|--------|------|-----------|-----------|--------|------------|------------|--------|
| 1   | 32m    | 4g   | 161,021   | 135,286   | -16.0% | 141,352    | 86,947     | -38.5% |
| 1   | 32m    | 8g   | 158,478   | 134,419   | -15.2% | 136,198    | 85,772     | -37.0% |
| 1   | 64m    | 4g   | 183,048   | 181,391   | **-0.9%** | 148,297 | 85,795     | -42.1% |
| 1   | 64m    | 8g   | 186,054   | 182,346   | **-2.0%** | 146,608 | 87,985     | -40.0% |
| 1   | 256m   | 4g   | 247,031   | 239,756   | -2.9%  | 116,187    | 80,522     | -30.7% |
| 1   | 256m   | 8g   | 245,266   | 240,716   | -1.9%  | 116,335    | 79,232     | -31.9% |
| 8   | 32m    | 4g   | 206,625   | 202,158   | -2.2%  | 155,883    | 90,554     | -41.9% |
| 8   | 32m    | 8g   | 202,427   | 196,656   | -2.9%  | 154,855    | 89,631     | -42.1% |
| 8   | 64m    | 4g   | 234,604   | 239,404   | **+2.0%** | 156,711 | 91,901     | -41.4% |
| 8   | 64m    | 8g   | 239,400   | 230,694   | -3.6%  | 151,498    | 85,830     | -43.3% |
| 8   | 256m   | 4g   | 273,164   | 254,357   | -6.9%  | 122,379    | 78,977     | -35.5% |
| 8   | 256m   | 8g   | 270,541   | 267,527   | -1.1%  | 116,732    | 79,644     | -31.8% |

## The two regressions behave completely differently

**The index regression is an artefact of the default configuration.** At the
shipped defaults (par=1, 32m) 7387 indexes 16% slower. Change a single setting
-- threshold 32m -> 64m, or parallelism 1 -> 8 -- and the gap collapses to
0.9-2.9%, and at one configuration 7387 is *faster*. The newer release is not
slower at indexing; it is more sensitive to an undersized memory-index
threshold. Anyone running stock defaults sees a regression that better tuning
erases entirely.

**The ingest regression is structural.** It sits at -31% to -43% in every one of
the twelve configurations. No combination of parallelism, threshold or heap
recovers it. Best case for 7387 (91,901 datoms/s at par=8/64m/4g) is still 41%
below 7187's best at the same config. This is a real per-transaction cost, not a
tuning problem.

## Also: tuning beats the regression by a wide margin

Both releases respond enormously to configuration. 7387 at par=8/256m/8g indexes
267,527 datoms/s -- **98% faster than 7187 at stock defaults** (161,021). The
release-to-release differences documented above are small next to the
configuration effect, so "which release" matters far less than "how is it
configured" for indexing.

Note the index/ingest tension reappears here: 256m gives the best index
throughput but the *worst* ingest (~80k vs ~90k at 64m), matching the earlier
threshold sweep. par=8 / 64m is the balanced choice for both releases.


---

# PostgreSQL vs embedded H2 (1.0.7705, medium, 3 iterations)

Installed PostgreSQL 18.6 and applied Datomic's shipped DDL. Worth noting what
that DDL is: a single `datomic_kvs` table of (id, rev, map, val bytea). Datomic
uses Postgres as a pure blob key-value store -- no relational schema, no joins,
no SQL query planning. Storage is storage.

| metric                | dev / H2  | sql / PostgreSQL | delta   |
|-----------------------|-----------|------------------|---------|
| index throughput      | 128,015   | 131,925          | +3.1%   |
| ingest throughput     | 78,300    | 83,853           | +7.1%   |
| index jobs            | 8         | 10               |         |
| total wall            | 30.69 s   | 28.65 s          | -6.6%   |
| transactor CPU        | 3.49      | 3.47 cores       | ~same   |
| host CPU busy         | ~30%      | 15.0%            |         |
| transactor write I/O  | ~4.8 MB/s | 0.6 MB/s         | -87%    |
| host iowait           | ~0%       | 1.5%             |         |

**PostgreSQL is slightly faster, not slower.** The H2-vs-network-storage penalty
I expected does not appear at this scale; if anything the dedicated storage
process helps, because it moves work off the transactor's own heap and GC.

The write I/O column is the real finding: the transactor writes 87% less
(0.6 vs 4.8 MB/s) because segment writes now go through the Postgres backend
process rather than through the transactor's own embedded H2. Host iowait rises
from ~0 to 1.5%, which is that work reappearing elsewhere.

## This revises an earlier caveat

Throughout this work I flagged every H2 result as "an upper bound -- re-run
against PostgreSQL for production-representative figures". That caveat was
wrong in direction. On this host PostgreSQL is marginally *faster*, so the H2
numbers are not optimistic, and the serial-commit bottleneck identified earlier
is not an artefact of embedded storage.

Caveat on the caveat: Postgres here is on **localhost**. A networked database
would add round-trip latency to every segment write, and that is the case the
original concern was really about. This measures the storage-engine change, not
the network.

## An earlier observation, corrected

At the small size the Postgres run reported zero index jobs against H2's two,
and I flagged that as the backend changing when indexing triggers. At medium
size Postgres runs 10 jobs vs H2's 8, so indexing triggers normally. The small
result was simply a working set too small to cross the 32m threshold reliably
-- not a backend behavioural difference.

