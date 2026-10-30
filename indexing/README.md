# Datomic indexing benchmark

This benchmark measures Datomic indexing performance with the public
[GH Archive](https://www.gharchive.org/) records. Each run uses one transactor and a separate peer JVM, and collects Datomic, JVM and host-level metrics.

There are two Systemslab specs, and they share one harness:

- **`tph.toml` measures.** It restores a backup, warms up, then indexing for a
  fixed time and reports peer-side transactions per second, commit latency and the
  transactor's indexing throughput. It has a a list of parameters for comparing releases, GC and transactor settings.
- **`indexing.toml` builds backups.** It loads a fixed number of datoms, from
  scratch or on top of a restored backup, and backs the result up. That is how
  the `idx-1b-1.0.7705`, `idx-2b-1.0.7705` and `idx-4b-1.0.7705` databases that `tph.toml` restores are
  made. See [Building backups](#building-backups-indexingtoml).

| File | Role |
|---|---|
| [`tph.toml`](tph.toml) | Measurement spec: restore a backup, restart both JVMs, warm up, then measure for a fixed time |
| [`indexing.toml`](indexing.toml) | Backup-building spec: optional restore, load `TARGET` datoms, optional backup |
| [`bigindex.clj`](bigindex.clj) | The harness (`bench.bigindex`): restore, streaming load, final index, backup, and all measurement |
| [`gharchive.clj`](gharchive.clj) | Schema and the GH event → tx-data mapping |
| [`jvmstats.clj`](jvmstats.clj) | JMX and `/proc` snapshots of the transactor |
| [`metrics.clj`](metrics.clj) | Datomic metrics callback for the transactor and peer |
| [`setup-host.sh`](setup-host.sh) | Stage a host: JDK, Clojure, Datomic release, GH Archive, harness |
| [`SCHEMA.md`](SCHEMA.md) | Schema details and their indexing cost |


## Process layout

Each run starts a transactor and a separate peer JVM, each in its own cgroup.
Their CPU, memory and I/O are therefore accounted separately.

```
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

## Measuring indexing throughput: `tph.toml`

`tph.toml` measures the indexing performance of Datomic using the public data source GH Archive. It has three phases: restore that restores the index from a index backup, warmup that sends transactions to transactor for 5 minutes to warmup both Peer and Transactor JVM, then the the 30 minute measure phase. After the restore phase, we restart both Peer and Transactor JVM. In the end of warmup phase, the peer sends out a sync-index and request-index request to finish indexing all inflight records before starting the measure phase.

| Step | What happens |
|---|---|
| **restore** | Peer JVM #1 runs `bin/datomic restore-db` of `RESTORE_FROM` (default `backup/idx-1b-1.0.7705`) into fresh dev storage, then exits |
| **restart** | The transactor is stopped and started again with the same `p.properties` and heap |
| **warmup** | Peer JVM #2 resumes the event stream from the backup's cursor and loads for `WARMUP_MIN` (5) minutes: JIT, caches and the memory index fill up. With `WARMUP_SYNC_INDEX=true` (default) it then forces an index job and waits (`d/request-index` + `d/sync-index`) until every warmup transaction is in the durable index, so measure starts with an empty memory index |
| **measure** | The same JVM loads for `MEASURE_MIN` (30) more minutes; this window is the result |

- **Why restart.** Without it, the measured transactor would be the one that
  served the restore, with whatever that left in its heap and caches.
  Restarting both JVMs makes the measured system one that started on an
  existing database and loaded its index from storage.
- **One stream across warmup and measure.** Both phases read from one producer,
  so measure starts at exactly the event warmup stopped at. Nothing is
  skipped or replayed between them.
- **Same events every run.** Every run restores the same backup and resumes at
  the same cursor, so runs that differ only in a setting (e.g.
  `WRITE_CONCURRENCY`) load the same events.
- **Nothing is backed up.** The grown database is deleted at the end.



### Key Parameters

| Parameter | Default | What it changes |
|---|---|---|
| `RELEASE` | `1.0.7705` | Datomic release under `releases/datomic-pro-<RELEASE>` |
| `RESTORE_FROM` | `backup/idx-1b-1.0.7705` | Starting database. On EC2: `idx-1b-1.0.7705`, `idx-2b-1.0.7705`, `idx-4b-1.0.7705` (all hosts) and `idx-1b-1.0.7622` (datomic-perf only); on amd: `idx-10m`. Backup names end in the release that built them |
| `INDEX_PARALLELISM` | 8 | Transactor `index-parallelism`. 1.0.7705 refuses values above 8 at startup (`:db.error/invalid-config-value`); 1.0.7745 accepts more |
| `INDEX_CPU_PARALLELISM` | 8 | Transactor `index-cpu-parallelism`. 1.0.7705 accepts it but ignores it |
| `GC`, `TXOR_GC`, `PEER_GC` | `g1` | `g1` or `zgc` (generational) for both JVMs, or for one of them |
| `TXOR_HEAP`, `PEER_HEAP` | 24g, 8g | Heaps. `OBJECT_CACHE_MAX` + `MEM_MAX` must be at most 75% of `TXOR_HEAP` |
| `WARMUP_MIN`, `MEASURE_MIN` | 5, 30 | Phase lengths |
| `WARMUP_SYNC_INDEX` | `true` | Force an index job at the end of warmup, so measure starts fully indexed |
| `GC_LOG` | `true` | Transactor GC log (`gc.log`), stamped in UTC. ZGC pauses only show up here, not in JMX |
| `THREAD_MS` | 5000 | Transactor per-thread CPU sampling period for `threads.jsonl`; 0 turns it off |



## Building backups: `indexing.toml`

`indexing.toml` builds the databases that `tph.toml` restores. It also has three optional phases: restore that restores the index from an index backup; index that indexes a fixed number of datoms; the backup phase that generates a new index backup.

### Three optional phases

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

## Benchmark Methodology

### Loading GH Archive: producer and consumer

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

| | `tph.toml` default | `indexing.toml` |
|---|---|---|
| `BATCH` (events per tx) | 1 | 1 |
| `QUEUE` (txs) | 100000 | 100000 |
| `INFLIGHT` (txs) | 2000 | 2000 |
| `ORDER` | desc | desc |
| `SAMPLE_MS` | 5000 | 1000 |


### Schema

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
  also feed VAET. No attribute is `:db/fulltext` (`:event/title` was until
  it was dropped; see [SCHEMA.md](SCHEMA.md)).
- **Conflicting values within a transaction.** See
  [Transaction conflicts](#transaction-conflicts) below.
- **Missing ids.** Events without an id are dropped. Actor, repo or org maps
  without an id are left off the event, since a nil identity would fail the
  whole transaction.

### Datomic Indexing throughput

Transaction throughput is measured at the peer. The benchmark replays GitHub Archive events, one event per transaction, through d/transact-async. A producer fills a queue of up to 100,000 transactions, and the consumer keeps up to 2,000 in flight before it waits for the whole group to commit. Transaction throughput is the number of committed transactions (issued minus failed) divided by the length of the phase. All in-flight transactions are awaited before the phase clock stops. Commit latency is recorded per transaction from submit to completion. Indexing throughput is measured at the transactor. Each index job writes an :index/create-index line to the transactor log with the number of datoms it indexed (:datoms) and how long it took (:msec). Indexing throughput is the total :datoms divided by the total :msec over the jobs that finish within the phase. It is therefore a rate over time spent indexing, not over the whole 30 minutes, and the summary reports the job count and total time in jobs next to it.

### JVM Metrics
We use JMX to take a snapshot in the start and the end of each phase to collect GC, JIT, CPU, and heap usage. Every 5 second, we also use jmx to collect the per-thread CPU usage. We also collect the GC log.
### Rezolus Metrics

### Phase timestamps for Rezolus

Systemslab records host metrics with Rezolus (`metrics.rez`, one sample
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

### Outputs

| Artifact | Contents |
|---|---|
| `result.json` | Every phase's numbers, `jvm` deltas, `phases`, event-stream cursor |
| `progress.jsonl` | Growth curve during the load (`tph.toml`: warmup and measure) |
| `phases.jsonl` | Phase start/end markers (`tph.toml` also marks restore and restart) |
| `bigindex.log` | Harness console output (`tph.toml`: the restore JVM's output first) |
| `restore-result.json` | Restore time and datom count (`tph.toml`) |
| `index-jobs.log` | The transactor's `:index/create-index` lines |
| `metrics-transactor.jsonl` | Datomic metrics callback samples |
| `threads.jsonl` | Transactor per-thread CPU every `THREAD_MS` |
| `process-monitor.log` | The transactor log's `process-monitor` lines (periodic transactor status) |
| `cgroup-cpu.txt` | `cpu.stat`, `memory.peak`, `io.stat` per cgroup |
| `storage.txt` | Database and backup sizes on disk |
| `transactor.out`, `logback.xml` | Transactor stdout and the logging config it ran with |
| `gc.log` | Transactor GC log (`tph.toml`, `GC_LOG=true`) |
| `metrics.rez` | Rezolus host metrics (recorded by Systemslab) |

### Running
  A typical experiment has two runs. First, `indexing.toml` builds the database that later measurements start from. With no `RESTORE_FROM`, it creates an empty `gharchive` database and transacts the schema. It then streams GitHub Archive
  events into it, newest hour first, until the database has grown by `TARGET` datoms. Next it forces one final index job, so the backup holds no unindexed data, and writes a full backup to `backup/<BACKUP_NAME>/` with `bench-cursor.json`
  beside it. The cursor records how far into the event stream the run got. Second, `tph.toml` measures on top of that backup. It restores `RESTORE_FROM` into fresh storage and restarts the transactor so the measurement starts with a cold JVM.
  It resumes the event stream at the backup's cursor, so no event is replayed, and runs a 5-minute warmup that ends with a forced index job and `d/sync-index`. It then measures for 30 minutes with the same load settings (`BATCH=1`,
  `QUEUE=100000`, `INFLIGHT=2000`). Both specs collect the transactor log, GC log, JVM and thread samples, and the Rezolus recording as experiment artifacts. Then they delete the live database and keep the backups.

  ```bash
  # build a 1B-datom backup (once per host, or copy it from another host)
  systemslab submit indexing.toml -p HOST=datomic.bench \
    -p TARGET=1000000000 -p BACKUP_NAME=idx-1b-1.0.7705

  # measure transaction and indexing throughput on it (idx-1b-1.0.7705 is the default RESTORE_FROM)
  systemslab submit tph.toml -p HOST=datomic.bench -p RELEASE=1.0.7745
  ```
### Data layout on the host

Everything the benchmark reads or writes lives under one root, `R` in
`indexing.toml` and `ROOT` in `setup-host.sh` (default
`/mnt/datomic/datomic-bench`). The tree is owned by `systemslab-agent`,
because the agent runs as its own user and cannot read a normal user's home
directory.

```
/mnt/datomic/datomic-bench/
├── releases/datomic-pro-1.0.7705/     Datomic distribution             (setup: releases)
│   ├── bin/transactor, bin/datomic      transactor launcher; restore-db / backup-db
│   └── resources/bench/metrics.clj      metrics callback on the transactor classpath (setup: harness)
├── gh/                                GH Archive input                  (setup: data)
│   └── YYYY-MM-DD-H.json.gz             one file per hour, hour not zero-padded
├── bench/src/bench/*.clj              staged harness source             (setup: harness)
├── scratch/indexing/                  working directory of the current or last run
│   ├── data/                            dev (H2) storage: the live database
│   ├── log/                             transactor log, including :index/create-index lines
│   ├── bench/                           copy of bench/src + generated deps.edn the peer runs from
│   ├── p.properties                     transactor config written by the spec
│   ├── transactor.out, txor.pid, txor.cgroup
│   ├── bigindex.log, result.json, progress.jsonl, phases.jsonl
│   └── metrics-transactor.jsonl, metrics-peer.jsonl
└── backup/<BACKUP_NAME>/              one directory per backup, kept across runs
    ├── owner, roots/<t>, values/        written by backup-db (roots/ is named by basis t)
    └── bench-cursor.json                event-stream position, written by the harness
```

### Inputs: `releases/`, `gh/`, `bench/`

`setup-host.sh` stages these, and runs only read them. Two things to keep in
mind:

- **Re-stage after harness changes.** The peer runs the copy in
  `bench/src/bench/`, not your working tree. After editing a `.clj` file, run
  `./setup-host.sh harness` again.
- **Don't change `gh/` during a restore chain.** `FILES` (default
  `*.json.gz`) is a glob over `gh/`. A backup's cursor records the exact file
  list, so adding or removing archive files makes the next resume fail.

### Run state: `scratch/indexing/`

- **Every run starts clean.** Preflight deletes `scratch/indexing/`, so this
  directory only ever holds one run: the one in progress, or the last one.
- **What survives after a run.** The collect step copies the outputs into the
  experiment's artifacts. The cleanup step then deletes `data/`, the database
  itself, but leaves everything else for debugging until the next run starts.
- **Ports.** All runs use the same transactor port (4334) and JMX port (7091),
  which is one more reason only one run can use a host at a time.
- **Transactor log size.** `log/` is the transactor's full log. It grows to
  about 160 MB for a 1M-datom run, so expect it to be large at billion-datom
  scale.
- **`scratch/tph/`.** `tph.toml` uses its own directory with the same layout,
  plus `start-txor.sh` (used for both transactor starts), `restore.log` and
  `restore-result.json` from the restore's peer JVM, and one `phases-*.jsonl`
  per step. Its preflight deletes it and its cleanup deletes `data/`.

### Results that persist: `backup/`

- **One directory per backup.** Each backup gets
  `backup/<BACKUP_NAME>/`, written by `bin/datomic backup-db`.
- **Overwriting.** A run deletes and rewrites its own `BACKUP_NAME` directory.
  It refuses to run if that directory is the one it is restoring from.
- **Nothing is cleaned up.** Backups are kept across runs and nothing deletes
  them, so remove old ones by hand.
- **Moving a backup.** `bench-cursor.json` sits next to Datomic's files, so
  copying the whole directory carries the resume position with it.
- **No cursor, no resume.** Older backups without `bench-cursor.json` can't be
  restored by `indexing.toml`.





