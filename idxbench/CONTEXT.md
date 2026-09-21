# Datomic Pro indexing performance across 19 releases

## What this tests

This context measures how Datomic Pro's **indexing and ingest throughput changed
across every publicly obtainable release**, from 1.0.6726 (2023/04) to 1.0.7705
(2026/07). Each release indexes an identical 1 GB slice of the GH Archive public
GitHub event stream (329,000 events → ~2.4M datoms) on the same host, so any
difference between runs is attributable to the Datomic version rather than the
workload or the machine.

The headline result is a **real, reproducible regression**: index throughput
falls 17% and ingest throughput falls 38% from the oldest release to the newest,
while transactor CPU rises 2.45x and IPC drops from 1.94 to 1.40. The change is
not gradual — it arrives as two discrete steps at identifiable releases, with
flat plateaus between them.

**Scope and caveats.** Storage is `dev`/H2 (embedded, in-transactor); results may
not transfer to `sql`, `ddb` or `cass` backends. Single transactor, single peer,
one host. The 1 GB working set is small relative to what the implicated Datomic
changes appear to target — see the interpretation note below.

## Release performance

19 releases × 3 independent invocations × 3 iterations each (57 experiments,
0 failures). Each figure is the mean of the **final** iteration across the three
invocations; the first two iterations of every invocation are JVM warmup and are
discarded.

| release  | date       | index d/s | ingest d/s | txor CPU | sys % | IPC  | group |
|----------|------------|-----------|------------|----------|-------|------|-------|
| 1.0.6726 | 2023/04/27 | 148,943   | 126,823    | 149.5 s  | 2.5%  | 2.01 | A |
| 1.0.6733 |            | 149,510   | 130,287    | 148.9 s  | 2.6%  | 2.00 | A |
| 1.0.6735 |            | 149,110   | 126,657    | 149.7 s  | 2.6%  | 2.01 | A |
| 1.0.7010 |            | 151,357   | 132,649    | 154.1 s  | 2.8%  | 1.98 | A |
| 1.0.7021 |            | 149,002   | 129,792    | 154.1 s  | 2.7%  | 1.98 | A |
| 1.0.7075 |            | **153,901** | 133,813  | 152.3 s  | 2.8%  | 1.99 | A |
| 1.0.7180 | 2024/07/11 | 148,107   | 131,120    | 200.0 s  | 5.9%  | 1.77 | A |
| 1.0.7187 | 2024/08/22 | 147,577   | 130,821    | 201.0 s  | 5.7%  | 1.77 | A |
| 1.0.7260 | 2024/10/22 | 126,415   | 106,648    | 267.6 s  | 15.0% | 1.63 | B |
| 1.0.7277 | 2024/12/16 | 126,313   | 104,243    | 272.7 s  | 15.2% | 1.61 | B |
| 1.0.7364 | 2025/05/08 | 126,943   | 105,130    | 269.4 s  | 15.2% | 1.62 | B |
| 1.0.7387 | 2025/06/27 | 124,011   | 81,965     | 402.6 s  | 33.3% | 1.39 | C |
| 1.0.7394 | 2025/08/07 | 120,324   | 80,192     | 410.9 s  | 33.5% | 1.38 | C |
| 1.0.7469 | 2025/10/23 | 124,148   | 80,474     | 407.8 s  | 33.7% | 1.39 | C |
| 1.0.7482 | 2026/01/06 | 123,307   | 80,629     | 411.1 s  | 34.0% | 1.38 | C |
| 1.0.7491 | 2026/01/26 | 123,369   | 80,342     | 414.3 s  | 34.0% | 1.38 | C |
| 1.0.7556 | 2026/03/13 | 123,264   | 81,399     | 406.4 s  | 33.9% | 1.39 | C |
| 1.0.7622 | 2026/04/28 | 124,534   | 80,929     | 408.6 s  | 34.0% | 1.38 | C |
| 1.0.7705 | 2026/07/10 | 128,015   | 78,300     | 352.1 s  | 26.6% | 1.50 | C |

*(1.0.7393 is absent: Maven Central lists it but the S3 distribution returns 403
— a withdrawn release, with 1.0.7394 shipping three days later.)*

### Three plateaus, two step changes

| group | releases | index d/s | ingest d/s | txor CPU | IPC |
|-------|----------|-----------|------------|----------|-----|
| **A** ≤ 7187 | 8 | 149,689 | 130,245 | 163.7 s | 1.94 |
| **B** 7260–7364 | 3 | 126,557 | 105,340 | 269.9 s | 1.62 |
| **C** ≥ 7387 | 8 | 123,871 | 80,529 | 401.7 s | 1.40 |

Within each plateau releases agree to within ±3%; the steps are 12–23%. A
follow-up bisect at n=10 confirmed both boundaries with **disjoint 95%
confidence intervals**:

- **Step 1 — introduced in 1.0.7260**: index −12.2%, ingest −17.7%
- **Step 2 — introduced in 1.0.7387**: index **unaffected** (−0.7%, CIs overlap),
  ingest −23.2%

The two steps are different in kind. Step 1 slowed both indexing and commits;
step 2 is purely a commit-path regression.

### What the CPU counters show

System CPU share rises **~10x** (2.5% → 34%) while IPC falls 28% and total
instructions retired stays roughly flat. A syscall trace of a live transactor
attributes **93.4% of syscall time to `futex`** — JVM lock contention across 183
threads — with file I/O negligible. The newer releases are not doing more work;
they are stalling more while doing the same work.

`1.0.7705` partially recovers (IPC 1.39 → 1.50, system share 34% → 26.6%),
consistent with its change-log entry *"Reduce CPU and memory required to
calculate index metrics."*

### Interpretation

The change logs at both boundaries are dominated by **memory-for-CPU trades
aimed at large databases**:

- **7187 → 7260**: *"Reduce memory required to calculate db-stats"*,
  *"Reduce memory required to calculate index-metrics"*, transaction hints
- **7364 → 7387**: *"Reduce Peer and Transactor memory usage in large
  databases"*, *"Improve indexing performance, reduce I/O in large databases"*

That matches the measurement exactly — GC time roughly halves while CPU rises.
**This 1 GB working set may be paying the cost without seeing the benefit.** The
same sweep at a 10 GB working set would test that directly, and has not been run.

A second important qualification: **most of the index regression is a defaults
artefact.** At stock settings (index-parallelism=1, threshold=32m) 1.0.7387
indexes 16% slower than 1.0.7187, but at parallelism=8/threshold=64m the newer
release is marginally *faster*. The **ingest** regression is structural — it
persists at −31% to −43% across all twelve tuning configurations tested.

## How the indexing tests are run

Each experiment is one job on the `amd` host running these steps:

1. **Preflight** — verify the requested release is staged and the JDK is
   present; kill any surviving JVM (a stale one would hold the H2 write lock);
   wipe storage so no earlier variant's segments affect this run.
2. **Start the transactor** under test, with JMX exposed on 7091 and a
   per-variant properties file (`protocol=dev`, `memory-index-threshold`,
   `index-parallelism`, …). It runs in its own cgroup, `datomic-transactor`.
3. **Run the benchmark** in a second JVM in cgroup `datomic-peer`, with a
   `deps.edn` pinning `com.datomic/peer` to the same release — a peer/transactor
   version mismatch either refuses to connect or silently exercises different
   code.
4. **Collect and upload** results, logs and per-cgroup CPU accounting.

### The measurement loop (DaCapo `luindex` model)

```
for i in 1..3:
    create a fresh database
    load 329,000 pre-converted events   <- measured
    request index + wait for catch-up   <- measured
    sample transactor JVM metrics over JMX
    DELETE the database
report iteration 3; discard 1 and 2 as warmup
```

All three iterations share one peer JVM so JIT state carries across them — JIT
time fell from 24% to 12% of wall between iterations 1 and 3, which is what the
warmup exists to absorb. JSON parsing and tx-data conversion happen **once, up
front, outside the timed region**; otherwise ~40% of the measurement would be
Jackson rather than Datomic. Transactions are pipelined with a bounded in-flight
window (50 transactions of 200 events each).

### Two measurement decisions worth knowing

**JVM metrics come from the transactor, not the benchmark process.** Datomic
performs index merges in the transactor, so instrumenting the benchmark JVM
would measure the wrong process. The harness attaches to the transactor's PID
over JMX for GC, JIT, CPU and heap.

**Index throughput is read from the transactor's own job accounting, not from
`d/sync-index`.** Indexing is asynchronous and continuous: by the time a load
finishes, the transactor has already merged most novelty in the background — of
66 index jobs observed in development, 60 fired spontaneously with no peer
involvement. `d/sync-index` therefore times only the trailing merge (a flat ~2 s
regardless of data size) and is reported separately as `index_tail_s`. The
throughput figure sums `:datoms` and `:msec` across every `:index/create-index`
event the transactor logged during the run.

## Where to find the results

Every experiment carries these artifacts:

| artifact | contents |
|----------|----------|
| **`summary.json`** | **the flat, comparable result — start here** |
| `idxbench.json` | full per-iteration detail, including every index job |
| `idxbench.log` | benchmark stdout |
| `transactor.log` | transactor stdout |
| `index-jobs.log` | raw `:index/create-index` lines (the throughput source) |
| `cgroup-cpu.txt` | per-cgroup `cpu.stat` for transactor and peer |
| `metrics.parquet` | Rezolus system metrics incl. cycles/instructions |
| `experiment.json` | the full spec as submitted |

Experiment names encode the whole configuration:

```
datomic-index-1.0.7705-r2-medium-peers1-p1-t32m-m512m-4g-jdk21-g1
              └release┘ └r=invocation 2      └parallelism, threshold,
                        └size                 memory-index-max, heap, jdk, gc
```

### Fetching them

```bash
URL=https://nubank.systemslab.iop.systems
CTX=<this context id>

# every summary.json in the context
systemslab --systemslab-url $URL artifact download-all \
  -o out -j 8 --context $CTX --name 'summary.json'
```

Key fields in `summary.json`:

| field | meaning |
|-------|---------|
| `index_throughput_datoms_per_sec` | datoms merged ÷ **index-job busy time** |
| `ingest_datoms_per_sec` | datoms ÷ **total wall clock** |
| `index_jobs`, `index_segments` | index work performed |
| `index_tail_s` | `d/sync-index` wall time (not throughput) |
| `gc_pct`, `jit_pct` | transactor JVM, as % of wall |
| `txor_cpu_cores` | transactor CPU, in cores |

The two throughput metrics are **not comparable to each other**: index
throughput divides by busy time only, ingest divides by wall clock including
idle. Index is always the larger number. Keeping both is what revealed that
step 2 moves one and not the other.

### Averaging across invocations

Each release has three invocations (`-r1`, `-r2`, `-r3`). Average the
final-iteration value across them, and report the spread — the coefficient of
variation across invocations was 0.2–1.9%, which is what establishes the 12–23%
steps as real.

```bash
python3 analyze-releases.py out     # from the idxbench project
```

This prints per-release means with a CV column, plus a relative view against the
oldest release, and writes `release-averages.json`.

### Rezolus metrics

`metrics.parquet` is recorded automatically per experiment and carries CPU
cycles, instructions, and per-cgroup counters:

```bash
rezolus mcp describe-metrics <file>
rezolus mcp query <file> \
  'sum(rate(cgroup_cpu_instructions{name="/system.slice/systemslab-agent.service"}[30s]))
   / sum(rate(cgroup_cpu_cycles{name="/system.slice/systemslab-agent.service"}[30s]))'
```

Two caveats on these. The recording spans the **whole experiment** — transactor
startup plus all three iterations — so it is not restricted to the measured final
iteration, which occupies only the last ~24% of the window. And Rezolus samples
systemd-managed cgroups, so the finest available scope is
`systemslab-agent.service`, which contains **both** JVMs; the per-JVM split is
available only as CPU *time* in `cgroup-cpu.txt`, not as cycles or instructions.
Since the peer accounts for ~29% of CPU and is flat across all 19 releases, the
transactor-only IPC decline is necessarily steeper than the 28% reported here.
