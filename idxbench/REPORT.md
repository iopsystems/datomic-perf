# Datomic Indexing Performance: What We Did and What We Found

Host `amd` — AMD Ryzen 9 7900X (12C/24T), 61 GB RAM, Ubuntu 24.04, JDK 21.
All figures from Datomic Pro on `dev`/H2 storage unless stated otherwise.

---

## 1. What we built

**A benchmark harness** (`idxbench.clj`) modelled on DaCapo's `luindex`: N
iterations in one peer JVM, each creating a fresh database, loading a fixed
event count, forcing the index, then dropping the database. The first N-1
iterations are warmup; the last is reported. Warmup matters — JIT fell from
24% to 12% of wall between iterations 1 and 3.

**A workload** from GH Archive — 24 hours of GitHub's public event firehose
(1.2 GB gzipped, ~14 GB raw). Chosen over the mbrainz sample because mbrainz
arrives as a pre-built backup you restore, so it cannot exercise the write path
at all. GH Archive is loaded *through the transactor* and its natural entity
graph (event → actor → repo → org) exercises `:db.unique/identity` upserts:
722k events collapse onto 116k users / 166k repos / 15k orgs.

Working sets are sized by **raw source bytes**, measured at 3.117 MB per 1000
events: small ~100 MB (32k events), medium ~1 GB (329k), large ~10 GB (3.285M).

**A Systemslab spec** (`datomic-index.toml`) with matrix expansion, sweeping
release, size, peers, index-parallelism, memory-index-threshold,
memory-index-max, heap, JDK, GC and batch size. Peer and transactor run in
separate cgroups (`datomic-peer` / `datomic-transactor`) so their CPU, memory
and IO are accounted independently — verified on all 145 runs.

**Total: 169 Systemslab experiments.**

---

## 2. The single most important thing we learned

**Datomic indexes in the transactor, on its own schedule, and the peer can
neither drive nor observe it.**

This is not a footnote; it invalidates the obvious way to build this benchmark.
Of 66 index jobs observed in early runs, **60 fired spontaneously** when
accumulated novelty crossed `memory-index-threshold`. Only 6 came from explicit
`d/request-index` calls — and even those are advisory hints, not commands.

Three consequences shaped everything:

1. **JVM metrics must come from the transactor.** A DaCapo-style in-process
   measurement would instrument the loader and report the wrong process. We
   attach over JMX to the transactor PID instead.

2. **`d/sync-index` is not a throughput measurement.** By the time a load
   finishes, background indexing has already done ~95% of the work, so
   sync-index times only the trailing merge — a flat ~2 s regardless of data
   size. Real throughput comes from the transactor's own per-job accounting.

3. **More peers do not help.** Driving the transactor from 1, 2, 4 and 8 peer
   JVMs moved its CPU from 3.45 to 3.54 cores — under 3%. Datomic serialises
   all writes through one transactor by design; extra peers queue behind the
   same commit path.

### Two throughput numbers, deliberately different

| metric | numerator | denominator | typical |
|---|---|---|---|
| **index datoms/s** | datoms merged | *busy time only* (summed job durations) | ~130k |
| **ingest datoms/s** | datoms in db | *wall clock* (includes idle) | ~80k |

Index is always higher — same data, smaller denominator. In one medium run, 10
jobs took 18.2 s of busy time inside a 28.7 s window, so the indexer was idle
37% of the time. Keeping both metrics is what let us find a regression that
moved one and not the other.

---

## 3. Configuration dominates everything else

| knob | effect on index throughput |
|---|---|
| **memory-index-threshold** 16m → 256m | **2.84x** (103k → 293k datoms/s) |
| **index-parallelism** 1 → 8 | **1.40x** (141k → 198k) |
| batch size 200 → 1000 | ~1.17x on ingest (78k → 91k) |
| peers 1 → 8 | **none** (flat) |
| JDK 17 → 21 | index unchanged; ingest +22% |
| heap 4g → 8g | negligible |

**Datomic ships `index-parallelism=1`**, so index jobs are effectively serial.
On a 24-thread host that leaves most of the machine idle during merges.

**The threshold is the dominant lever, but it trades against ingest.** Past 64m,
index throughput keeps climbing while *ingest* falls (78k → 62k at 256m) and
total wall time rises, because a larger memory index means longer stalls when a
merge runs. **64m is the knee** — 1.3x the default's index throughput at no
ingest cost.

Combined best (par=8, 256m): **273,164 datoms/s — roughly double stock
defaults.**

### Throughput degrades with database size

| size | source | index datoms/s | jobs |
|---|---|---|---|
| small | ~100 MB | 206,861 | 2 |
| medium | ~1 GB | 139,506 | 8 |
| large | (old sizing) | 92,842 | 21 |

Per-datom indexing cost is **not constant**: each job merges novelty against a
progressively larger durable index, so later jobs do more work per datom. Any
capacity plan extrapolated from a small load will overestimate steady state.
The `main-segs` field in the transactor log makes this visible — 1 for the first
job's `:eavt` merge, 45 by the next.

---

## 4. A real cross-release regression

We tested **19 releases** (every publicly obtainable one — 1.0.7393 is withdrawn
and returns 403; older releases are gone from S3 entirely), each run 3 times at
3 iterations. Then re-ran the four boundary releases at n=10.

| group | releases | index d/s | ingest d/s | txor cores |
|---|---|---|---|---|
| A: 6726–7187 | 8 (2023/04–2024/08) | 149,689 | 130,245 | 2.44 |
| B: 7260–7364 | 3 (2024/10–2025/05) | 126,557 | 105,340 | 3.52 |
| C: 7387–7705 | 8 (2025/06–2026/07) | 123,871 | 80,529 | 4.15 |

**Oldest → newest: index −17.2%, ingest −38.2%**, while transactor CPU rose 70%
and GC time roughly halved. Within each plateau releases agree to ±3%; the steps
are 12–23%. At n=10 both steps have disjoint 95% confidence intervals.

### The two steps are different in kind

| step | boundary | index | ingest |
|---|---|---|---|
| 1 | 7187 → 7260 | −12.2% | −17.7% |
| 2 | 7364 → 7387 | **not affected** (−0.7%, CIs overlap) | −23.2% |

Step 2 is purely a commit-path regression. At n=3 we measured it as −2.3% index
and would have called it a small index regression; n=10 showed that was noise.

### What shipped at those boundaries

Both are **memory-for-CPU trades aimed at large databases**:

- **7187 → 7260**: "Reduce memory required to calculate db-stats",
  "Reduce memory required to calculate index-metrics", transaction hints
- **7364 → 7387**: "Reduce Peer and Transactor memory usage in large databases",
  "Improve indexing performance, reduce I/O in large databases"

This matches the measurement exactly — GC halves, CPU rises, throughput drops.
**Our 1 GB working set may be paying the cost without seeing the benefit.**
Testable: the same sweep at 10 GB should narrow or reverse the gap.

### Most of the index regression is a defaults artefact

Running 7187 vs 7387 across the tuning knobs:

| config | index delta |
|---|---|
| par=1, 32m (**stock defaults**) | **−16.0%** |
| par=1, 64m | −0.9% |
| par=8, 64m | **+2.0%** (newer is faster) |
| par=8, 256m | −1.1% |

Change one setting and the index gap collapses. The newer release is not slower
at indexing — it is more sensitive to an undersized threshold.

**The ingest regression is structural**: −31% to −43% in all twelve
configurations tested. No combination of parallelism, threshold or heap recovers
it.

---

## 5. Things that turned out to be wrong

Stating these plainly, because several were load-bearing assumptions:

**"Releases will differ by only a few percent."** Predicted before the sweep.
The actual spread was 17–38% with clean step changes.

**"Bigger batches are better."** The synthetic sweep said 5,000 entities/tx was
fastest. On real GH Archive data, **batch=5,000 fails** with
`:db.error/datoms-conflict` — the same actor/repo recurs within an hour, so a
large batch asserts two values for one `[entity attribute]` in a single
transaction. Batch size is bounded by the **upsert collision rate of the data**,
not by memory. 1,000 works; 5,000 does not.

**"The transactor's CPU ceiling is an artefact of one connection."** Tested with
1–8 peers; flat. It is architectural.

---

## 6. Known limitations of this work

- **`dev`/H2 storage only.** Every result here is embedded H2. A PostgreSQL
  backend was stood up and shown to work, but only as single ad-hoc runs
  outside Systemslab, with no repeats — not enough to report. Whether these
  findings transfer to `sql`, `ddb` or `cass` storage is untested.
- **Single transactor, single host.** No HA, no storage contention.
- **The 10 GB working set has never run.** `preload!` materialises all tx-data
  in the peer heap (measured 0.0157 MB/event), so 3.285M events needs ~50 GB —
  more than the peer can hold alongside the transactor on a 61 GB box. Fixing
  this needs a streaming loader with a parse-ahead thread.
- **The in-flight window uses a full drain barrier** (wait for all 50, then
  refill) rather than a sliding window. This leaves the transactor briefly idle
  at each barrier, so **reported ingest figures are conservative**. Constant
  across all runs, so comparisons hold.
- **Index throughput came from log scraping** for all 145 experiments —
  regexing `:index/create-index` lines, which depends on an unstable log
  format. Datomic's metrics callback is the supported source; it is now wired
  in (49 transactor metrics including `IndexDatoms`, `IndexWrites`,
  `TransactionMsec`) but the headline numbers above predate it.
- **Absolute numbers are not comparable across sweeps.** The release sweep ran
  interleaved with 57 experiments; the bisect ran on a quieter host and reads
  ~6% higher. Within-sweep comparisons are sound.

---

## 7. Practical recommendations

**For anyone running Datomic Pro with write-heavy workloads:**

1. **Change `index-parallelism` from 1 to 8.** It is the shipped default and
   costs 40% of index throughput on a multi-core host.
2. **Raise `memory-index-threshold` from 32m to 64m.** 1.3x index throughput at
   no ingest cost. Go higher only if ingest latency does not matter.
3. **Batch transactions at ~1,000 entities**, but validate against your data —
   upsert collisions, not memory, set the ceiling.
4. **Do not add peers to scale writes.** They queue behind one transactor.
5. **Use JDK 21 over 17.** Index throughput is a wash but ingest is 22% faster.
6. **Test your own release upgrade.** Releases after 1.0.7187 carry a real
   commit-path regression on small-to-medium databases that no tuning recovers.

**The headline:** on this hardware, tuning moves index throughput by ~2x while
release choice moves it by ~17%. Configuration matters far more than version —
but the ingest regression is version-bound and untunable, so both deserve
attention.

**Where the ceiling actually is:** the transactor never exceeded ~4.3 of 24
cores with iowait near zero, in any configuration tested. It is bound by the
serial commit-and-merge path, not by CPU, disk, or client concurrency. See
section 8 for what its CPU time is actually spent on — it is not what we
first assumed.

---

## 8. Where the transactor's CPU actually goes

Per-cgroup accounting across all 145 runs (peer and transactor in sibling
cgroups):

| | user | system | total | system share |
|---|---|---|---|---|
| **Transactor** | 216.6 s | 65.3 s | 281.9 s | **18.7%** |
| **Peer** | 79.1 s | 3.8 s | 82.9 s | **4.6%** |

The transactor burns 3.4x the peer's CPU with a 4x higher system share. The
obvious reading is that this is the I/O path — durable log writes and segment
writes. **That reading is wrong.**

Tracing a live transactor for 6 seconds:

| syscall | % of syscall time | calls |
|---|---|---|
| **futex** | **93.4%** | 447,468 |
| restart_syscall | 3.6% | 32 |
| epoll_wait | 1.8% | 115 |
| read | 1.15% | 56 |
| write | 0.01% | 368 |
| pwrite64 | ~0% | 12 |

**Actual file I/O is negligible.** The system time is `futex` — the kernel
primitive behind JVM lock contention and thread parking. The transactor ran
**183 threads**; every block and wake-up between them is a futex syscall
charged to system time.

H2's disk writes do not appear here because MVStore uses memory-mapped I/O:
page writes are memory stores that the kernel flushes asynchronously, which is
why `/proc/<pid>/io` shows real block-layer traffic (~4.8 MB/s) while `write()`
syscalls are almost absent.

This gives a concrete mechanism for three results that were otherwise only
described:

- **The ~4.3-core ceiling.** Threads spend their time handing work off and
  waiting, not computing. Adding cores cannot help a workload bound by lock
  handoffs on a serial path.
- **8 peers changed nothing** (3.45 -> 3.54 cores). More producers add more
  futex traffic against the same serial commit path.
- **System share rises with `index-parallelism`** (18.2% at par=1 -> 21.0% at
  par=8). More concurrent index workers means more lock handoffs — not more
  disk writes.

Caveats: the trace was taken while a sweep was running, so it reflects one
configuration; `strace` itself perturbs futex counts. The 93% dominance is far
too large to be an artefact, but the absolute call count should not be quoted
as precise. The per-config system-share figures are also drawn from an
unbalanced sample (n=113 at 32m vs 16 elsewhere), since most experiments held
those knobs constant — directional, not a clean factorial.
