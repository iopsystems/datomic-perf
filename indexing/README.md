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
| [`tph.toml`](tph.toml) | Steady-state spec: restore a backup, restart both JVMs, warm up, then measure transactions per hour for a fixed time |
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
a job during the load or by the forced final index. In that run (with the
earlier 64m memory-index threshold), the three index jobs covered exactly the
1,035,471 datoms added. Restored datoms are
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

A short load may never reach `memory-index-threshold`, so no index job runs
during it. With the default `MEM_THRESHOLD=1g`, a 1M-datom load is one of
these: every datom is indexed by the final job. Without a forced
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
  also feed VAET. No attribute is `:db/fulltext` (`:event/title` was until
  it was dropped; see [SCHEMA.md](SCHEMA.md)).
- **Conflicting values within a transaction.** See
  [Transaction conflicts](#transaction-conflicts) below.
- **Missing ids.** Events without an id are dropped. Actor, repo or org maps
  without an id are left off the event, since a nil identity would fail the
  whole transaction.

### Transaction conflicts

**What causes them.** GH Archive records each user, repo and org with its
numeric id *and* the name it had when the event happened. When an account or
repo is renamed, or only its capitalization changes, the same id shows up
under two names:

```
user 19433964    oVinayak-MP / vinayakmp007          (2024-01-01, hour 15)
repo 732652557   GangaTricks/Quiz  15:00:28  →  GangaTricks/quiz  15:25:56
```

In 2024-01-01 hour 15 (180k events), 9 user ids and 79 repo ids appear with
two names. Repo renames are by far the most common case.

**Why Datomic rejects it.** `:user/id`, `:repo/id` and `:org/id` are
`:db.unique/identity`, so within one transaction every map carrying the same id
resolves to the **same entity**. `:user/login`, `:repo/name` and `:org/login`
are cardinality-one, so asserting two different values for one entity in one
transaction is a contradiction. Datomic rejects the **whole** transaction, and
every event in it is lost:

```
:db.error/datoms-conflict Two datoms in the same transaction conflict
{:d1 [17592186045420 :repo/name "GangaTricks/Quiz" 13194139534313 true],
 :d2 [17592186045420 :repo/name "GangaTricks/quiz" 13194139534313 true]}
```

The same rename split across two transactions is fine. The second transaction
retracts the old value and asserts the new one, and the history keeps both. The
same name repeated in one transaction is also fine, because redundant datoms
collapse.

| Same id, two names | Result |
|---|---|
| in one transaction | `:db.error/datoms-conflict`, the whole tx fails |
| in two transactions | OK: retract old name, assert new name |
| in one transaction after `dedupe-tx` | OK: the first name wins for that tx |

**Why batch size matters.** A conflict needs both names inside one
transaction, and an hour holds about 180k–300k events. So the larger the
batch, the longer the time window it covers, and the more likely it is to
contain both sides of a rename. Batching the 48 staged hours (9.6M events)
without `dedupe-tx`:

| Events per tx | Approx. time per tx | Failed txs | Events lost |
|---|---|---|---|
| 1 | — | 0 (impossible) | 0 |
| 100 | 1–2 s | 3 / 96,211 | 300 (0.003%) |
| 1,000 | 10–20 s | 5 / 9,622 | 5,000 (0.05%) |
| 2,000 | 20–40 s | 18 / 4,811 | 36,000 (0.37%) |
| 5,000 | 1–2 min | 120 / 1,925 | 600,000 (6.2%) |
| 10,000 | 2–3 min | 325 / 963 | 3,250,000 (33.8%) |

Before `dedupe-tx` existed, this capped how large a batch could be. Even at
1,000 events per transaction, a long run loses a few whole transactions.

**The fix: `dedupe-tx`.** For each identity, `dedupe-tx` keeps the first full
map in a transaction and reduces later mentions to the id alone:

```clojure
{:event/actor {:user/id 1 :user/login "kevinr99089"}}  ; kept in full
{:event/actor {:user/id 1}}                            ; id only, still upserts
```

Every event and every reference survives. The later name is dropped only for
that transaction, and the next transaction that mentions the id updates it.
With `BATCH=1`, there is only one event per transaction, so conflicts cannot
happen and `dedupe-tx` has nothing to do.

**Reproducing it.** Transact two maps with the same `:user/id` and different
`:user/login` in one transaction against a `datomic:mem://` database with the
schema from `gharchive.clj`. Or batch a real hour with the same rule the table
above uses: a batch fails if any id appears in it with two different names.

## Steady-state transaction rate: `tph.toml`

`indexing.toml` answers "how long does it take to add N datoms". `tph.toml`
answers "how many transactions per hour does a warm system sustain on top of
an existing database", and how fast the transactor indexes meanwhile.

| Step | What happens |
|---|---|
| **restore** | Peer JVM #1 runs `bin/datomic restore-db` of `RESTORE_FROM` (default `backup/idx-10m`) into fresh dev storage, then exits |
| **restart** | The transactor is stopped and started again with the same `p.properties` and heap |
| **warmup** | Peer JVM #2 resumes the event stream from the backup's cursor and loads for `WARMUP_MIN` (5) minutes: JIT, caches and the memory index fill up |
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

What the measure phase reports (`result.json`, top level; warmup's own numbers
are under `warmup`):

| Field | Meaning |
|---|---|
| `tx-per-sec`, `tx-per-hour` | Transactions that committed, divided by the window (peer side). With `BATCH=1` a transaction is one GH event |
| `ingest-datoms-per-sec` | Datoms added in the window ÷ window |
| `commit-latency` | p50/p90/p99/p99.9/max ms from `transact-async` to the future completing, on 1%-wide buckets |
| `index-jobs`, `index-datoms`, `index-throughput` | Transactor index jobs whose log line landed in the window, and Σdatoms ÷ Σmsec over them |
| `jvm` | Transactor JVM and `/proc` deltas over the window |

Commit latency includes the time a transaction waits behind the other
outstanding ones, so it mostly reflects queueing, not the cost of one
transaction. About 600–750 transactions are outstanding at once whatever
`INFLIGHT` is (see the `INFLIGHT` sweep below).

An index job is counted in the window its log line lands in, so a job that
started in warmup and finished in measure counts fully in measure.

**Event budget.** At ~7k tx/s a 35-minute run uses ~15M events. The 96 hours
staged on amd (2024-01-01, 2024-01-29, 2024-01-30, 2024-02-01) hold 21.1M
events, of which `idx-10m` used the first 1.2M, so a run stays on real events
up to about 9.5k tx/s. Past that the producer replays the files with offset
ids (`cycles` > 0 in `result.json`).

**Disk per run.** At ~60k datoms/s the database grows by roughly 120M datoms
in 35 minutes. The transactor log stays small: by default the specs set the
two per-transaction loggers (`datomic.transaction`, `datomic.kv-cluster`) to
WARN. With `TX_LOG=true` and `STORAGE_LOG=true` the log grows by about 1.3 KB
per transaction, tens of GB in `scratch/tph/log` per run, and the next run's
preflight deletes it.

```bash
# one run, defaults (restore idx-10m, 5 + 30 min)
systemslab submit tph.toml -p HOST=amd -p REQUIRE_MOUNT=false -p MIN_FREE_GIB=20
# write-concurrency sweep
for wc in 4 50 100; do
  systemslab submit tph.toml -p HOST=amd -p REQUIRE_MOUNT=false -p MIN_FREE_GIB=20 \
    -p WRITE_CONCURRENCY=$wc
done
```

`RESTORE_FROM` must be a backup written by `indexing.toml` (it needs
`bench-cursor.json`), and `FILES`/`ORDER` must match the ones it was built
with.

### Results on amd: `WRITE_CONCURRENCY` sweep

`idx-10m` restored, 5 + 30 min, `BATCH=1`, `INFLIGHT=2000`, other settings
at their defaults. Measure window only; one run per setting.

| `WRITE_CONCURRENCY` | tx/s | tx/h | datoms/s | index throughput | commit p50 / p99 | transactor CPU |
|---|---|---|---|---|---|---|
| 4 | 7,061 | 25.4M | 55,941 | 151,613 datoms/s (13 jobs) | 87 / 164 ms | 6.3 cores |
| 50 | 6,883 | 24.8M | 54,557 | 156,795 datoms/s (13 jobs) | 89 / 169 ms | 6.2 cores |
| 100 | 6,836 | 24.6M | 54,194 | 154,358 datoms/s (13 jobs) | 90 / 170 ms | 6.2 cores |

Write concurrency makes no measurable difference: the spread is about 3%, within
what one run per setting can resolve. The transactor's own metrics
(`metrics-transactor.jsonl`) show why. `TransactionApplyMsec` averages
0.13 ms per transaction, and summed over a second it reaches 0.93 s in every
run: the transactor applies transactions one at a time, and that step is
93% busy, capping the rate near 1 ÷ 0.13 ms ≈ 7.5k tx/s. Per-thread CPU
during a measure window agrees: the transactor's `processor-gharchive`
thread runs at ~95% of one core, while no peer thread exceeds ~35%, so
neither the producer nor the consumer is the limit. Storage writes
are only ~2.6 MB/s, so a limit on concurrent writes has nothing to throttle.

### Results on amd: `INFLIGHT` sweep

Same setup, `WRITE_CONCURRENCY=100`. The 2000 row is a second run of the
default, which gives the run-to-run spread (6,783 vs 6,836 tx/s, under 1%).

| `INFLIGHT` | tx/s | tx/h | datoms/s | index throughput | commit p50 / p99 |
|---|---|---|---|---|---|
| 2,000 | 6,783 | 24.4M | 53,771 | 158,471 datoms/s (13 jobs) | 90 / 170 ms |
| 20,000 | 6,899 | 24.8M | 54,677 | 152,222 datoms/s (12 jobs) | 109 / 181 ms |
| 200,000 | 6,889 | 24.8M | 54,607 | 148,863 datoms/s (12 jobs) | 110 / 188 ms |

More transactions in flight don't raise the rate: 2,000 already keeps the
transactor's transaction thread saturated. Commit latency also barely grows
(90 → 110 ms), although with 200,000 truly outstanding it would be ~29 s. By
Little's law only about 600–750 transactions are outstanding (p50 × tx/s) at
any setting. The latency clock starts before `transact-async` is called, so
the explanation is that `transact-async` itself blocks the consumer once
roughly that many are outstanding, and each call waits only ~0.15 ms for the
next slot to free up. The consumer thread used ~7% CPU, consistent with it
spending most of its time blocked. So `INFLIGHT` above ~750 only delays the
group barrier; it adds no work in flight. The cap is inferred from these
measurements, not from Datomic documentation.

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

## Data layout on the host

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

Approximate sizes on amd for comparison: a 1M-datom backup is about 28 MB, a
2M-datom one about 55 MB, and the 10M-datom `idx-10m` 248 MB (all
`BATCH=1`). The 96 hourly archive files take 9.4 GB.

Adding the 2024-01-29 and 2024-01-30 files changed the file list, so backups
whose cursor recorded the older 48-file list (`idx-1m`, `idx-2m`) can no
longer be resumed.

On EC2, `/mnt/datomic` is instance-store NVMe and is wiped by an instance
stop/start, together with every backup on it. Stage the host again with
`setup-host.sh` after one.

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
