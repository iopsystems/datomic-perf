# Indexing benchmark schema

The indexing benchmark loads [GH Archive](https://www.gharchive.org/) events
through the transactor. This document describes the Datomic schema those events
are loaded into, how each raw event maps onto it, and which parts of the schema
drive indexing cost.

- Schema: [`gharchive.clj:19-45`](gharchive.clj) (`bench.gharchive/schema`)
- Event mapping: [`gharchive.clj:52-91`](gharchive.clj) (`event->tx`)
- Per-transaction dedupe and replay offsets: [`bigindex.clj:35-89`](bigindex.clj)

## Entities and attributes

The schema has 4 entity types and 17 attributes. Every attribute is
cardinality-one.

```
event ──:event/actor──▶ user
  │ ──:event/repo───▶ repo ──:repo/org──▶ org
  └──:event/org─────────────────────────▶ org
```

| Attribute | Type | Index / uniqueness | Present |
|---|---|---|---|
| `:user/id` | long | `:db.unique/identity` | always |
| `:user/login` | string | `:db/index` | always |
| `:org/id` | long | `:db.unique/identity` | when the event has an org |
| `:org/login` | string | `:db/index` | when the event has an org |
| `:repo/id` | long | `:db.unique/identity` | always |
| `:repo/name` | string | `:db/index` | always |
| `:repo/org` | ref → org | | when the event has an org |
| `:event/id` | string | `:db.unique/identity` | always |
| `:event/type` | keyword | `:db/index` | always |
| `:event/at` | instant | `:db/index` | always |
| `:event/actor` | ref → user | | always |
| `:event/repo` | ref → repo | | always |
| `:event/org` | ref → org | | when the event has an org |
| `:event/action` | keyword | | payload has `action` (opened, closed, …) |
| `:event/commits` | long | | `PushEvent` with `payload.size` |
| `:event/number` | long | | event has an issue or pull request |
| `:event/title` | string | `:db/fulltext` | event has an issue or pull request |

## Mapping a raw event

`event->tx` turns one GH Archive JSON event into a single nested map. The
actor, repo and org are nested maps keyed by their unique ids:

```clojure
{:event/id    "35000001"
 :event/type  :PullRequestEvent
 :event/at    #inst "2024-01-01T15:00:00Z"
 :event/actor {:user/id 583231 :user/login "octocat"}
 :event/repo  {:repo/id 1296269 :repo/name "octocat/Hello-World"
               :repo/org {:org/id 9919 :org/login "github"}}
 :event/org   {:org/id 9919 :org/login "github"}
 :event/action :opened
 :event/number 42
 :event/title  "Fix typo in README"}
```

The peer never looks up entity ids. The transactor resolves every nested map
when it applies the transaction.

## Design choices and their indexing cost

### Upserts on four identity attributes

`:user/id`, `:repo/id`, `:org/id` and `:event/id` are `:db.unique/identity`.
When a map in a transaction carries one of them, the transactor looks up that
value in AVET:

- If an entity already has that value, the map resolves to that entity and its
  other attributes are added to it. Values that are already true are dropped
  as redundant and write no datom.
- Otherwise a new entity is created.

So each event costs up to four identity lookups in the transactor:

| Lookup | Usual result | Why |
|---|---|---|
| `:event/id` | not found → create | every event is new, but uniqueness still has to be checked |
| `:user/id` | usually found | active users recur |
| `:repo/id` | often found | popular repos recur |
| `:org/id` | found when present | few orgs, and they recur |

The data is upsert-heavy: in a 4-hour sample, 722k events collapsed onto 116k
users, 166k repos and 15k orgs (see
[`../idxbench/GHARCHIVE.md`](../idxbench/GHARCHIVE.md)). This is why a user or
repo only costs datoms the first time it appears, and why the 1B run averaged
**6.64 datoms per event**.

The lookups read the in-memory index and the stored index, so they touch more
data as the database grows. That is a plausible contributor to the 14%
throughput decline over the 1B run, but the lookup share of transactor time has
not been measured.

### Conflicting values within one transaction

Within one transaction, every map with the same identity value refers to the
same entity. Because `:user/login` is cardinality-one, two events in one batch
that give the same user different logins make the transaction invalid. GH
Archive has this case: GitHub reuses one numeric id across login spellings
within an hour.

```clojure
;; both events in one batch
{:event/actor {:user/id 17592186449658 :user/login "kevinr99089"}}
{:event/actor {:user/id 17592186449658 :user/login "Kevinr99089"}}
;; => :db.error/datoms-conflict, the whole transaction is rejected
```

Larger batches hit this more often, so it used to cap the batch size.
`dedupe-tx` ([`bigindex.clj:67-89`](bigindex.clj)) keeps the first full map
for each identity in a transaction and reduces later mentions to the id alone:

```clojure
{:event/actor {:user/id 17592186449658 :user/login "kevinr99089"}}  ; kept in full
{:event/actor {:user/id 17592186449658}}                            ; id only, still upserts
```

Every reference is kept. The same conflict *across* transactions is not an
error: Datomic retracts the old login and asserts the new one (2 datoms).

### Secondary indexes

- **AVET:** 9 attributes are in AVET, the 4 unique ids plus the 5
  `:db/index` attributes (`:user/login`, `:org/login`, `:repo/name`,
  `:event/type`, `:event/at`). These support login and name lookups,
  per-type filtering and time-range queries.
- **VAET:** the 4 ref attributes (`:event/actor`, `:event/repo`,
  `:event/org`, `:repo/org`) are also indexed in reverse, which supports
  "all events by this user/repo" queries.
- **Fulltext:** `:event/title` is also written to Datomic's Lucene fulltext
  index, which is eventually consistent.

### How much fulltext is in the workload

Fulltext is a small part of the data. From 14 hourly files (2024-01-01/02,
2.53M events):

| Metric | Value |
|---|---|
| Events with a title | 14.8% |
| Title length | mean 42 B, median 40 B, p99 107 B, max 1,003 B |
| Title bytes averaged over all events | 6.3 B |
| Titles' share of string data (event id + login + repo name + title) | 11.8% |
| Event types carrying titles | PullRequest 47%, IssueComment 27%, PullRequestReview 11%, Issues 10%, PullRequestReviewComment 5% |

Scaled to the 1B run, that is about 22M titles, roughly **2.2% of datoms** and
under 1 GB of text, against a 125 GB database. These are estimates from a
holiday sample, which may under-represent pull request activity.

The benchmark therefore mostly measures ordinary datom indexing and upserts.
Each title does cost more than a plain datom, so fulltext's share of indexing
time could exceed its 2.2% share of datoms. The transactor log only reports
total time per index job, so that split has not been measured. An A/B run with
`:db/fulltext` removed from `:event/title` would measure it directly.

## Replaying data past the end of the archive

When `bigindex` runs out of archive files before reaching its datom target, it
replays them with `offset-event` ([`bigindex.clj:35-50`](bigindex.clj)). Each
replay pass adds `cycle × 1,000,000,000` to every user, repo and org id and
appends `-c<cycle>` to the event id. Without the offset, replayed events would
upsert onto the entities from the first pass and the database would stop
growing. With it, the datom shape and the within-pass upsert pattern stay the
same, but every replay creates new entities.

The 1B run exhausted January 2024 at about 134M events, so its last ~17M
events (~11%) are offset replays. The 2B run uses
January–March 2024 so that it does not need to replay.
