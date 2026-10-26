---
name: datomic-indexing-bench
description: Run the Datomic indexing benchmark (indexing.toml) on a Systemslab host and read its results — result.json fields, phase timestamps, index-job logs, transactor JVM deltas, and slicing Rezolus host metrics by phase. Use when asked to submit, chain (restore → index → backup), compare, or interpret runs of this benchmark.
---

# Datomic indexing benchmark

The harness is `bench.bigindex` ([`bigindex.clj`](bigindex.clj)). A run has up
to three phases, in this order: **restore** → **index** (with an optional
**final-index**) → **backup**. For the design, read [`README.md`](README.md);
for the schema, read [`SCHEMA.md`](SCHEMA.md). This file covers how to run the
benchmark and how to read what it produces.

## Before submitting

- **Server and host.** The Systemslab server is `http://192.168.0.170`. Pass
  `--systemslab-url` on every `systemslab` call. Choose the host with
  `-p HOST=<tag>`; list the tags with
  `systemslab graphql '{ host { name tags state } }'`.
- **Staging.** The host must be staged under `/mnt/datomic/datomic-bench` by
  [`setup-host.sh`](setup-host.sh), one step per call (`prereqs`, `releases`,
  `data`, `harness`, `deps`, `verify`). After changing any `.clj` file, run
  the `harness` step again, or copy the file to
  `/mnt/datomic/datomic-bench/bench/src/bench/` owned by `systemslab-agent`.
  Specs run the **staged** copy, not your working tree.
- **Preflight on small hosts.** The defaults assume the 1B-datom EC2 run: a
  mounted `/mnt/datomic` and 150–200 GiB free. On a host like `amd`, add
  `-p REQUIRE_MOUNT=false -p MIN_FREE_GIB=20`.
- **Only one run per host at a time.** Preflight kills every `java` process,
  and all runs share port 4334, JMX port 7091 and the `scratch/` directory.

## Parameters (`indexing.toml`)

| Parameter | Default | Meaning |
|---|---|---|
| `RESTORE_FROM` | `""` | Backup directory to restore and resume from; empty or `none` = fresh database |
| `BACKUP_NAME` | `indexing` | Backup to `${R}/backup/<name>`; empty or `none` = no backup |
| `TARGET` | `1000000` | Datoms to **add** in this run; 0 skips the index phase |
| `FINAL_INDEX` | `true` | Force and time one more index job after the load |
| `FILES` | `*.json.gz` | Glob over `${R}/gh` |
| `ORDER` | `desc` | `desc` = newest archive hour first, `asc` = oldest first |
| `BATCH` / `QUEUE` / `INFLIGHT` | `1` / `100000` / `2000` | Events per tx / producer queue depth (txs) / txs submitted before awaiting |
| `SAMPLE_MS` | `1000` | Datom-count sampling period, which is also the stop check |
| `MEM_THRESHOLD`, `MEM_MAX`, `OBJECT_CACHE_MAX` | `1g`, `8g`, `8g` | Transactor memory index and cache. `OBJECT_CACHE_MAX + MEM_MAX` must be ≤ 75% of `TXOR_HEAP` or the transactor won't start |
| `WRITE_CONCURRENCY`, `INDEX_PARALLELISM`, `INDEX_CPU_PARALLELISM` | `100`, `8`, `8` | Transactor concurrency. `index-cpu-parallelism` is ignored by Datomic 1.0.7705 |
| `TXOR_HEAP`, `PEER_HEAP` | `24g`, `8g` | JVM heaps; both use `-Xms = -Xmx`, G1, `MaxGCPauseMillis=50` |
| `GC` | `g1` | Collector for both the transactor and the peer: `g1` (`-XX:+UseG1GC -XX:MaxGCPauseMillis=50`) or `zgc` (generational, `-XX:+UseZGC -XX:+ZGenerational`). Heaps are unchanged |
| `TXOR_JVM_EXTRA` | empty | Extra transactor JVM options, e.g. `-XX:+ExitOnOutOfMemoryError` |
| `THREAD_MS` | `5000` | Per-thread transactor CPU sampling period for `threads.jsonl`; `0` turns it off |
| `TX_LOG`, `STORAGE_LOG` | `false`, `false` | Per-transaction transactor log events (`datomic.transaction` `:tx/process`, `datomic.kv-cluster`), about 1.3 KB per tx together. Set `TX_LOG=true` for a short diagnostic run: it logs each transaction's apply time, index reads and apply breakdown. The generated config is uploaded as `logback.xml` |
| `DB`, `RELEASE`, `R` | `gharchive`, `1.0.7705`, `/mnt/datomic/datomic-bench` | Database name, Datomic release, staging root |
| `REQUIRE_MOUNT`, `MIN_FREE_GIB`, `MIN_FILES` | `true`, `200`, `24` | Preflight guards |

To reproduce the earlier 1B-datom run's load shape, use
`-p BATCH=1000 -p QUEUE=2000 -p INFLIGHT=500 -p ORDER=asc -p SAMPLE_MS=15000 -p TARGET=1000000000 -p FINAL_INDEX=false`.

## Common recipes (`indexing.toml`)

```bash
U=--systemslab-url=http://192.168.0.170
SMALL="-p HOST=amd -p REQUIRE_MOUNT=false -p MIN_FREE_GIB=20"

# index only (fresh database, no backup)
systemslab $U submit indexing.toml $SMALL -p BACKUP_NAME=

# fresh database → 1M datoms → backup/idx-1m
systemslab $U submit indexing.toml $SMALL -p BACKUP_NAME=idx-1m

# restore idx-1m → +1M datoms → backup/idx-2m (chains on the cursor)
systemslab $U submit indexing.toml $SMALL \
  -p RESTORE_FROM=/mnt/datomic/datomic-bench/backup/idx-1m -p BACKUP_NAME=idx-2m
```

- **Phase switches.** An empty value or `none` turns off `RESTORE_FROM` or
  `BACKUP_NAME`.
- **Run a chain sequentially.** Wait for each run's backup before submitting
  the restore that reads it.
- **Resume rules.** A restore resumes from the backup's `bench-cursor.json`.
  It fails on purpose if `ORDER` or the file list (`FILES` glob over `gh/`)
  differs from what the backup was built with. Do not add or remove archive
  files between chained runs.

## Steady-state TPH (`tph.toml`)

Restore a backup → restart transactor and peer → warm up → measure for a fixed
time. See README "Steady-state transaction rate".

| Parameter | Default | Meaning |
|---|---|---|
| `RESTORE_FROM` | `.../backup/idx-10m` | Backup written by `indexing.toml` (needs `bench-cursor.json`) |
| `WARMUP_MIN`, `MEASURE_MIN` | `5`, `30` | Minutes of loading before and during the measured window |
| `SAMPLE_MS` | `5000` | Progress sampling only; the stop is time-based |
| Others | as `indexing.toml` | `BATCH`, `INFLIGHT`, `QUEUE`, transactor settings, heaps, guards. No `TARGET`, `BACKUP_NAME` or `FINAL_INDEX` |

```bash
# write-concurrency sweep; submit sequentially (one run per host at a time)
for wc in 4 50 100; do
  systemslab $U submit tph.toml $SMALL -p WRITE_CONCURRENCY=$wc
done
```

- **Headline numbers** are top-level in `result.json` and are the *measure*
  window: `tx-per-sec`, `tx-per-hour`, `ingest-datoms-per-sec`,
  `commit-latency`, `index-jobs`/`index-throughput`, `jvm`. The warmup's
  numbers are under `warmup`; the restore's are in `restore-result.json`.
- **Phases** in `phases.jsonl`: `restore`, `restart`, `warmup`, `measure`.
  Use the `measure` start/end to cut Rezolus.
- **Compare like with like.** Runs are comparable only from the same
  backup, `FILES` and `ORDER` (then they load the same events).
- **Commit latency is mostly queueing** behind ~600–750 outstanding
  transactions (`transact-async` blocks beyond that, so raising `INFLIGHT`
  past ~750 changes nothing). Compare it across runs, not as the cost of one
  transaction.

Wait for a run to finish:

```bash
E=<experiment-id>
until s=$(systemslab $U graphql "{ experimentById(id: \"$E\") { state } }" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["experimentById"]["state"])'); \
      [[ $s =~ ^(success|failure|cancelled)$ ]]; do sleep 20; done; echo $s
```

## Getting results

```bash
systemslab $U artifact download-all -o runs/$E -j 8 $E
# files land in runs/$E/<experiment name>/bench/
```

All runs of one spec share an experiment name, so give **each experiment its
own empty output directory**. Otherwise download-all's folders collide.

| Artifact | Read it for |
|---|---|
| `result.json` | All numbers. Start here |
| `phases.jsonl` | Phase start/end markers (also in `result.json`) |
| `progress.jsonl` | Growth curve during index: `{t, elapsed-s, datoms, added, events, txs, cycles, datoms-per-sec}`, one line every `SAMPLE_MS` |
| `index-jobs.log` | One transactor line per index job: `:msec`, `:datoms`, `:written`, `:as-of-t` |
| `bigindex.log` | Harness console output, including `PHASE ...` lines and `tx failed:` errors |
| `metrics-transactor.jsonl` | Datomic metrics callback, about once a minute (`TransactionMsec`, `CreateEntireIndexMsec`, `MemoryIndexMB`, ...) |
| `threads.jsonl` | Transactor CPU per thread, one line every `THREAD_MS`: `{t, interval-ms, live, threads: [{id, nid, name, state, cpu-ms, user-ms}]}`, busiest first, idle threads left out. `id` is the Java thread id that Datomic logs as `:tid`, `nid` the Linux thread id. The apply thread is `processor-<db>`. `jcmd-ms` marks the samples where `jcmd Thread.print` ran to map new threads, which pauses the JVM briefly |
| `logback.xml` | Transactor logging config the run used (see `TX_LOG`, `STORAGE_LOG`) |
| `cgroup-cpu.txt` | `cpu.stat`, `memory.peak`, `io.stat` for the `datomic-transactor` and `datomic-peer` cgroups |
| `storage.txt` | Database and backup sizes on disk |
| `logs.json` | Systemslab job log; every line has a UTC `ts`. Check here for preflight `FATAL:` messages |
| `metrics.parquet` | Rezolus host metrics, 1 s resolution |

## Reading `result.json`

Top level (whole run, plus the **index** phase):

| Field | Meaning |
|---|---|
| `datoms` | Final datom count (`db-stats`) |
| `base-datoms` | Datoms at the start of index (restored count, or 0) |
| `target` | Requested datoms to add |
| `added` | Datoms actually added. Exceeds `target` by up to one sample period plus the in-flight txs |
| `source-events` | GH events consumed by submitted transactions |
| `events` | Tx maps submitted; equals `source-events` today (one map per event) |
| `txs`, `failed-txs` | Transactions submitted and failed. A non-zero `failed-txs` means data was dropped; check `bigindex.log` |
| `load-ms` | Wall time of the index phase |
| `ingest-datoms-per-sec` | `added / load-ms`, end to end |
| `index-jobs`, `index-datoms`, `index-job-ms` | Index jobs that ran **during load**, from `index-jobs.log` |
| `index-throughput` | `index-datoms / index-job-ms`, in datoms/s, inside index jobs. Null if no job ran |
| `cycles` | Full replay passes over the files. Above 0 means synthetic offset data was loaded |
| `order`, `skip-events`, `cursor-events` | Event-stream position: started at `skip-events`, ended at `cursor-events` |
| `batch`, `inflight`, `hints` | Load configuration |
| `jvm` | Transactor deltas over the index phase (see below) |
| `phases[]` | `{phase, start-ms, end-ms, start, end, duration-ms}` |

Nested objects (`null` when the phase did not run):

| Object | Fields |
|---|---|
| `restore` | `restore-ms` (the `restore-db` command), `exit`, `bytes`, `mib-per-sec`, `datoms`, `datoms-per-sec`, `connect-ms` / `first-stats-ms` (cold peer), `from`, `tail`, `jvm` |
| `final-index` | `ms` (`request-index` + `sync-index`), `index-jobs`, `index-datoms`, `index-job-ms`, `jvm` |
| `backup` | `backup-ms`, `exit`, `bytes`, `tail`, `jvm` |

`jvm` deltas measure the **transactor** (JMX + `/proc`), not the peer:

| Field | Meaning |
|---|---|
| `wall-ms` | Length of the measured interval |
| `proc-cpu-cores` | Average transactor CPU in cores |
| `proc-cpu-pct-host` | The same, as a share of the whole host |
| `gc-count`, `gc-ms`, `gc-pct` | Summed over every collector bean. Under ZGC this includes concurrent cycle time, so do not compare it across collectors |
| `gc-pause-ms`, `gc-pause-pct` | Stop-the-world pause time only (G1 Young/Old Generation, ZGC Minor/Major Pauses). Use this to compare G1 with ZGC |
| `gc-collectors` | Per collector bean: `{name {:count :ms}}` over the interval |
| `jit-ms`, `jit-pct` | Compile time summed over compiler threads. Can be large on short phases; it is not "% of one core" |
| `io-read-bytes`, `io-write-bytes`, `io-write-MBps` | Transactor block-layer I/O (page-cache hits excluded) |
| `host-cpu-pct`, `host-iowait-pct` | Whole host, from `/proc/stat` |
| `heap-used-after`, `heap-max`, `threads-after` | Transactor heap and threads at the end of the interval |

Quick summary:

```python
import json, glob, sys
d = json.load(open(glob.glob(f"{sys.argv[1]}/**/result.json", recursive=True)[0]))
for p in d.get("phases", []):
    print(f"{p['phase']:<11} {p['duration-ms']/1000:7.1f}s  {p['start']} -> {p['end']}")
if d.get("load-ms"):
    print(f"added {d['added']:,} datoms in {d['load-ms']/1000:.1f}s; "
          f"ingest {d['ingest-datoms-per-sec']:,.0f}/s, index {d.get('index-throughput') or 0:,.0f}/s "
          f"({d['index-jobs']} jobs), failed txs {d['failed-txs']}, cycles {d['cycles']}")
print(f"final datoms {d['datoms']:,}")
```

## Slicing Rezolus host metrics by phase

`phases[].start-ms` and `end-ms` are UTC epoch milliseconds, on the same clock
as Rezolus. `systemslab promql --start/--end` has returned no results for these
experiments. Fetch the whole series and filter by time on your side instead:

```python
import json, subprocess
def series(exp, q):
    out = subprocess.run(["systemslab", "--systemslab-url", "http://192.168.0.170", "promql",
                          "--experiment", exp, "--output", "json", q],
                         capture_output=True, text=True).stdout
    return [(float(t), float(v)) for t, v in json.loads(out)["result"][0]["values"]]

cpu = series(E, "sum(rate(cpu_usage[5s]))")          # ns/s; divide by 1e9 for cores
for p in d["phases"]:
    v = [x for t, x in cpu if p["start-ms"] / 1000 <= t <= p["end-ms"] / 1000]
    print(p["phase"], f"{sum(v) / len(v) / 1e9:.2f} cores" if v else "no samples")
```

Rezolus metrics cover the whole host. Use the `jvm` deltas or
`cgroup-cpu.txt` to attribute work to the transactor or the peer. For which
metrics exist, see the `rezolus-query` skill (`describe_metrics`).

## Interpretation pitfalls

- **Ingest vs index.** `ingest-datoms-per-sec` is end to end.
  `index-throughput` is the cost per datom inside background index jobs. They
  answer different questions, so don't compare them as if they were the same
  measure.
- **No index jobs on short loads.** With `MEM_THRESHOLD=1g`, a 1M load
  never reaches the threshold: `index-jobs` is 0 and `index-throughput` is
  null, and all indexing happens in `final-index` (1 job over ~1M datoms on
  amd). Read `final-index` for indexing cost at this scale; background-job
  throughput needs a much larger load.
- **`final-index.ms` vs phase duration.** The `final-index` phase duration is
  about 5 s longer than its `ms`. The harness waits that long for the job's
  log line, so the tail of that phase is idle.
- **Overshoot.** Compare runs by `added` and rates, not by `target`.
- **Batch size.** Results at `BATCH=1` (the default) and `BATCH=1000` are not
  comparable as ingest numbers. Per-transaction overhead dominates at
  `BATCH=1`.
- **Replays.** `cycles > 0` means part of the load was offset replays, not
  new real events.
- **Resume overlap.** A restored run that shows far fewer datoms per event
  than about 7 is probably upserting events the backup already holds. Check
  `skip-events` and the `resume:` line in `bigindex.log`.
- **Name-sorted runs.** Runs from before archive-hour sorting loaded hours in
  name order (0, 1, 10, 11, …, 2, 20, …). Their file order differs from newer
  runs.
- **Backups without a cursor.** Backups written before `bench-cursor.json`
  existed cannot be restored by `indexing.toml`; preflight fails with "has no
  bench-cursor.json".
- **`/mnt/datomic` on EC2** is instance store and is wiped on stop/start. If
  preflight fails with "not mounted" or "no backup", the host needs staging
  again.
