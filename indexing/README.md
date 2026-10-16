# Datomic indexing benchmark

This benchmark measures how fast a Datomic Pro transactor ingests and indexes
[GH Archive](https://www.gharchive.org/) events. It has three optional phases: `restore` that uses the `restore-db` to load a backup index; `index` that keeps index a fixed amount of datoms; `backup` that backs up the index to local files. This benchmark uses one transactor and a seperate peer JVM. It collects datomic, JVM, and host-level performance metrics.

| File | Role |
|---|---|
| [`bigindex.clj`](bigindex.clj) | The harness (`bench.bigindex`): restore, streaming load, final index, backup, and all measurement |
| [`gharchive.clj`](gharchive.clj) | Schema and the GH event → tx-data mapping |
| [`jvmstats.clj`](jvmstats.clj) | JMX and `/proc` snapshots of the transactor |
| [`metrics.clj`](metrics.clj) | Datomic metrics callback for the transactor and peer |
| [`indexing.toml`](indexing.toml) | The Systemslab spec: newest-first, one event per transaction, every phase optional |
| [`setup-host.sh`](setup-host.sh) | Stage a host: JDK, Clojure, Datomic release, GH Archive, harness |
| [`SCHEMA.md`](SCHEMA.md) | Schema details and their indexing cost |


## Process layout

Each run starts a transactor and a separate peer JVM, each in its own cgroup.
Their CPU, memory and I/O are therefore accounted separately (`cgroup-cpu.txt`).

```
 cgroup datomic-transactor             cgroup datomic-peer
┌─────────────────────────┐          ┌──────────────────────────────────┐
│ bin/transactor (dev/H2) │◀─────────│ bench.bigindex                   │
│  - applies transactions │ tx-async │  - producer thread (parse, map)  │
│  - runs index jobs      │          │  - consumer (transact-async)     │
│  - JMX on :7091         │◀── JMX ──│  - sampler (db-stats)            │
│  - metrics callback     │          │  - runs bin/datomic restore/backup│
└─────────────────────────┘          └──────────────────────────────────┘
          data-dir, log-dir, backups: /mnt/datomic/datomic-bench/...
```

Indexing happens in the transactor, not in the peer that calls `d/transact`.
That is why every JVM measurement targets the transactor process (see
[Measurement](#measurement)).

## Three optional phases

A run executes up to three phases in order. Each one is switched on by a spec
parameter:

| Phase | Enabled by | What is timed |
|---|---|---|
| **restore** | `RESTORE_FROM` non-empty | `bin/datomic restore-db` into fresh storage, then a cold `d/connect` and the first `d/db-stats` |
| **index** | `TARGET` > 0 | Streaming load until the database has grown by `TARGET` datoms. `FINAL_INDEX=true` then forces one more index job and times it separately (`final-index`) |
| **backup** | `BACKUP_NAME` non-empty | `bin/datomic backup-db` into a new directory (a full backup, not incremental) |

Without a restore, the harness creates the database and transacts the schema.
After a restore, the schema is already in the database and is not
re-transacted.

### Restore

Dev storage is served by the transactor, so the transactor must be running
before `restore-db` can write to it. After the restore, the harness times the
first connect and the first `db-stats`. Both are cold: the peer has nothing
cached and has to read the index roots from storage.

`TARGET` is the number of **new** datoms to add on top of whatever the
database starts with. The load stops once the datom count reaches
`base + TARGET`, where `base` is the restored datom count, or 0 for a fresh
database. For example, a restore of 1,028,835 datoms with `TARGET=1000000`
loads until at least 2,028,835 datoms. Every added datom is indexed, either by
a job during the load or by the forced final index. In that run, the three
index jobs covered exactly the 1,035,471 datoms added. Restored datoms are
already in the durable index and are not indexed again.

### Resuming the event stream

A restored database already holds some events. Replaying those events would
upsert onto existing entities and add almost no datoms, so a restored run has
to continue past them.

Every successful backup writes `bench-cursor.json` into the backup directory.
It records the file order, the file list and how many events the stream had
consumed. When `indexing.toml` restores a backup, it passes `--resume true`:
the harness reads the cursor and skips that many events before transacting.
Skipped lines are gunzipped but not parsed. The harness refuses to resume if
the order or the file list differs from the backup's, because the event count
would then point somewhere else. `restore-db` ignores the extra file.

A backup without a cursor, written before this mechanism existed, cannot be
restored by `indexing.toml`. At the harness level you can still restore one
without `--resume` and pass `--files` the backup never saw.

### Final index

A short load on top of a large database may never reach
`memory-index-threshold`, so no index job runs during it. Without a forced
job, the run would measure ingest only, not the cost of merging new datoms
into the large durable index. `final-index` calls `d/request-index` followed by
`d/sync-index` and times both. It then waits 5 s so the job's log line lands
before it is parsed.

### Backup

`backup-db` writes to `${R}/backup/${BACKUP_NAME}`. The harness records the
latency, the size on disk and the exit code. If the restore or the backup
exits non-zero, the experiment fails.

## Loading GH Archive: producer and consumer

Hourly GH Archive files are about 75–150 MB of gzipped JSON each. Earlier
harnesses (`bench.idxbench`) parsed everything into the peer heap before
timing started. That costs about 0.0157 MB per event, or roughly 2 TB at
1B datoms. `bigindex` streams instead, so peer memory is bounded by the queue
depth rather than by the dataset:

```
 files (sorted by archive hour, ORDER asc|desc)
   │
   ▼
┌──────────── producer thread ─────────────┐     ┌── consumer (main thread) ──┐
│ gunzip → line → JSON → event->tx         │     │ poll queue                 │
│ group BATCH events into one tx           │     │ d/transact-async           │
│ dedupe-tx (one full map per identity)    │────▶│ ... up to INFLIGHT futures │
│ offset ids on replay passes              │QUEUE│ await all, repeat          │
│ blocks when the queue is full            │     └────────────────────────────┘
└──────────────────────────────────────────┘                  ▲
                                         sampler: every SAMPLE_MS
                                         d/db-stats → progress.jsonl,
                                         sets stop when datoms ≥ goal
```

- **Producer.** One thread reads the files in order. It parses each line and
  maps it with `event->tx`, groups `BATCH` events into one transaction, and
  puts that transaction on an `ArrayBlockingQueue` of depth `QUEUE`. When the
  queue is full, the producer blocks, so parsing never outruns the transactor
  by more than `QUEUE` transactions. Because parsing runs on this thread, it
  stays off the consumer's critical path.
- **Consumer.** The consumer takes transactions off the queue and submits each
  with `d/transact-async`. Once `INFLIGHT` futures are outstanding, it awaits
  all of them and then continues, so this is a group barrier rather than a
  sliding window. A failed transaction is logged (the first 10) and counted,
  and the load continues.
- **Sampler.** Every `SAMPLE_MS`, the sampler reads `d/db-stats`, appends a
  line to `progress.jsonl` with the wall-clock time, datoms, events and rate,
  and sets the stop flag once the datom count reaches the goal. The stop check
  only happens on samples, so a run overshoots by up to one sample period of
  ingest plus the transactions in flight. With `SAMPLE_MS=1000` and
  `INFLIGHT=2000`, the 1M-target runs on amd added 1.02–1.03M datoms.
- **Ordering.** Files are sorted by archive hour, not by name. GH Archive hours
  are not zero-padded (`2024-01-01-9.json.gz`), so name order would put hour
  10 before hour 9. `ORDER=desc` loads newest first.
- **Replay.** If the files run out before the target is reached, the producer
  replays them. Each replay pass shifts every user, repo and org id by
  `pass × 10⁹` and suffixes the event id. Replayed events therefore create new
  entities, with the same datom shape and upsert pattern as the original
  data, instead of upserting onto the first pass.

`indexing.toml` defaults to one end of this design. The earlier 1B-datom run
used the other end, and both can be set through parameters:

| | `indexing.toml` default | Earlier 1B-datom run |
|---|---|---|
| `BATCH` (events per tx) | **1** | 1000 |
| `QUEUE` (txs) | **100000** | 2000 |
| `INFLIGHT` (txs) | **2000** | 500 |
| `ORDER` | **desc** (newest first) | asc |
| `SAMPLE_MS` | **1000** | 15000 |

With `BATCH=1`, every GH event is its own transaction. Throughput then comes
from concurrency (a deep queue and many transactions in flight), not from
batching, which stresses per-transaction overhead in the transactor. On amd,
this ingested about 34k datoms/s, against about 85k datoms/s with 1000 events
per transaction.

## Schema

The full schema, with the mapping and each design choice's indexing cost, is in
[`SCHEMA.md`](SCHEMA.md). In short:

```
event ──:event/actor──▶ user
  │ ──:event/repo───▶ repo ──:repo/org──▶ org
  └──:event/org─────────────────────────▶ org
```

- **The natural entity graph, not flat rows.** 4 entity types and 17
  cardinality-one attributes.
- **Identity attributes.** `:user/id`, `:repo/id`, `:org/id` and `:event/id`
  are `:db.unique/identity`. `event->tx` emits one nested map per event, and
  the transactor resolves the nested maps by upsert. Every event therefore
  costs up to four AVET lookups. Users, repos and orgs recur heavily, so they
  add datoms only the first time they appear. The 1B run averaged 6.64 datoms
  per event.
- **Secondary indexes.** 5 more attributes have `:db/index` (logins, repo name,
  event type, event time), so 9 attributes are in AVET. The 4 ref attributes
  also feed VAET. `:event/title` is `:db/fulltext`, about 2% of datoms.
- **Conflicting values within a transaction.** GitHub reuses one numeric user
  id across login spellings, which makes a batched transaction assert two
  values for one cardinality-one attribute (`:db.error/datoms-conflict`).
  `dedupe-tx` keeps the first full map per identity in a transaction and
  reduces later mentions to the id alone. `BATCH=1` cannot hit this.
- **Missing ids.** Events without an id are dropped. Actor, repo or org maps
  without an id are left off the event, since a nil identity would fail the
  whole transaction.

## Running

Stage the host once, then submit `indexing.toml`. Everything lives under
`/mnt/datomic/datomic-bench` and is owned by the Systemslab agent user.

```bash
# on the host
for step in prereqs releases harness deps; do ./setup-host.sh $step; done
START=2024-01-01 ./setup-host.sh data 24        # 24 hourly files from START
./setup-host.sh verify

# from a workstation
systemslab submit indexing.toml -p HOST=amd -p BACKUP_NAME=idx-1m
systemslab submit indexing.toml -p HOST=amd \
  -p RESTORE_FROM=/mnt/datomic/datomic-bench/backup/idx-1m -p BACKUP_NAME=idx-2m
```

The preflight checks are sized for the 1B-datom EC2 run. On a host where
`/mnt/datomic` is a plain directory or the disk is smaller, add
`-p REQUIRE_MOUNT=false -p MIN_FREE_GIB=20`.

## Measurement

### Indexing throughput

Ingest and indexing are separate measurements:

- **Ingest** = datoms added ÷ load wall time. This is end-to-end, so it
  includes parsing, transacting and any back-pressure from indexing.
- **Index throughput** = Σ datoms ÷ Σ msec over the index jobs that ran during
  the phase. The transactor logs one line per index job:

  ```
  ... datomic.index - {:event :index/create-index, :msec 2660.0, :datoms 488622, ...}
  ```

  The harness counts the jobs in the transactor log directory before and
  after the phase and parses the new ones. This is the transactor's own
  accounting of time spent building indexes. Index jobs run in the background
  alongside ingest, so this rate is a cost per indexed datom, not a share of
  wall time. `d/sync-index` alone would only time the trailing merge.
  `final-index` reports its own jobs, datoms and in-job time the same way.

These lines are collected in `index-jobs.log`. A run's `memory-index-threshold`,
`memory-index-max`, `index-parallelism` and the other transactor settings are
in the spec (`p.properties`).

### JVM and host metrics of the transactor

Around each phase, `jvmstats` takes a snapshot of the **transactor** before
and after, and reports the difference (the `jvm` field of each phase in
`result.json`):

| Source | Metrics |
|---|---|
| JMX (`localhost:7091`) | GC count and time (all collectors), JIT compile time, process CPU time → `proc-cpu-cores`, heap used/max, thread count |
| `/proc/<pid>/io` | Bytes the transactor read and wrote at the block layer (page-cache hits excluded) → `io-write-MBps` |
| `/proc/stat` | Host CPU busy and iowait % |

`jit-pct` is cumulative compile time summed across compiler threads, divided
by wall time. On a short phase it can be large (about 80% for a 35 s load) and
does not mean 80% of one core.

Datomic's own metrics also flow through `metrics-callback`. Datomic calls
`bench.metrics/transactor`, and `-Ddatomic.metricsCallback=bench.metrics/peer`
for the peer, about once a minute. Each payload is appended to
`metrics-transactor.jsonl` / `metrics-peer.jsonl`, and includes
`IndexDatoms`, `CreateEntireIndexMsec`, `TransactionMsec`, `MemoryIndexMB`,
`IndexWriteMsec` and others. These are the supported counterparts of the
log-parsed numbers, but their once-a-minute resolution is too coarse for runs
shorter than a few minutes.

### Phase timestamps for Rezolus

Systemslab records host metrics with Rezolus (`metrics.parquet`, one sample
per second). To cut those metrics by phase, the harness brackets every phase
with UTC wall-clock markers. They are written to three places:

- `bigindex.log`: `PHASE index start 2026-10-16T12:35:47.207Z (1792154147207)`
- `phases.jsonl`: one line per marker, appended as it happens, so a crashed
  run still records the start of the phase it died in
- `result.json` → `phases`: `start-ms`, `end-ms`, ISO times and duration

`progress.jsonl` adds a wall-clock sample every `SAMPLE_MS` inside the index
phase. To slice Rezolus by phase, fetch the series and filter by
`start-ms`/`end-ms`:

```bash
systemslab promql --experiment <ID> --output json 'sum(rate(cpu_usage[5s]))'
```

## Outputs

| Artifact | Contents |
|---|---|
| `result.json` | Every phase's numbers, `jvm` deltas, `phases`, event-stream cursor |
| `progress.jsonl` | Growth curve during the index phase |
| `phases.jsonl` | Phase start/end markers (`indexing.toml`) |
| `bigindex.log` | Harness console output |
| `index-jobs.log` | The transactor's `:index/create-index` lines |
| `metrics-transactor.jsonl` | Datomic metrics callback samples |
| `cgroup-cpu.txt` | `cpu.stat`, `memory.peak`, `io.stat` per cgroup |
| `storage.txt` | Database and backup sizes on disk |
| `transactor.out` | Transactor stdout |
| `metrics.parquet` | Rezolus host metrics (recorded by Systemslab) |
