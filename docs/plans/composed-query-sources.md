# Composed query sources and recent/archive visibility

Status: implementation in progress, 2026-10-08. The implemented contracts and
limits below are distinct from the remaining long-term architecture.

This extends the [lake ingestion and publication plan](lake-ingestion-and-publication.md)
and complements the [query pipeline proposal](pipelined-query-api.md). Source
composition selects the visible input relation; pipeline stages operate on query
results. They should share planning machinery without becoming interchangeable
concepts.


## Implemented contracts

The global `/query` endpoint accepts `source.union` and `source.overlay` expressions
using literal table names. Each leaf goes through catalog authorization and the
normal native/object query dispatcher. Results include `_table` provenance; union
preserves equal IDs from different tables. Incarnation/object-generation changes
fail with a catalog conflict. Overlay currently rejects identities with row filters,
because hidden-key precedence needs a separately reviewed policy contract.

Score ordering requires explicit `source_ranking: "rrf"`. Equal-weight RRF uses
`1 / (60 + source_rank)`; scores are not shared-corpus BM25. A disjoint RRF union
can serve the first 4096 global positions over arbitrarily large exact leaf totals:
4096 candidates per input suffice because ranks decrease strictly within an input.
Field ordering requires compatible sortable mappings/doc values in each leaf
index. A field-specific text index does not consume unrelated sort columns.
Field ordering and keyed overlays currently require complete matching inputs of
at most 4096 rows per table. These requests fail closed above that budget. Up to
16 union inputs and a 64 MiB composition arena are supported. Aggregation, hierarchy,
graph queries, joins, analyses and stateful execution are not composed yet.

`next_source_cursor` feeds `source_cursor` on the next request. The cursor fingerprints
the source/query/ranking, observed candidate window, totals, table incarnations and
lake publication metadata. Changes to that observed cut return a conflict. This
is an invalidation contract, not retention of old cross-table snapshots or isolation
from changes outside the observed window. RRF union pagination stops at position
4096; adaptive streaming and retained serving descriptors remain future work.

Overlay keys are flat integer, string or boolean fields; numeric/timestamp key
normalization is not enabled yet. The changes input must retain one unique latest
row per key, including deleted rows with a boolean `deleted: true` (or the explicitly
configured `tombstone_field`). Indexed anti-lookups omit the user's search and filter
so a nonmatching edit still suppresses a matching base row. Tombstone rows never
appear in results. Physical removal from the changes table cannot express deletion
of an older base row. No saved source/view DDL is implemented yet.

Direct HTTP SQL SELECT requests accept `lake_visibility: "accepted"`; the default
remains `"committed"`. A statement pins one committed source and one immutable WAL
suffix per writable lake table, reusing it across aliases and joins. Typed upserts
and deletes resolve before SQL filtering, aggregation, ordering and limits. Integer,
string and boolean stable keys are supported. Accepted visibility is not enabled
for connection/session/prepared or transactional modes, or timestamp/numeric keys.
Existing SQL scan and memory budgets still apply.

Writable-lake search requests accept `lake_read` with `visibility: "accepted"` or
`"published"`, an optional `through` receipt (`table_id`, `object_generation`,
`wal_lsn`) and `wait_ms` from 0 to 60000. Change acceptance includes those receipt
fields. Published reads explicitly select the archive publication and skip pending
changes. A receipt requires its coverage and rejects a different incarnation.
Vector/hybrid accepted reads with pending changes wake publication and wait within
the request deadline; they return readiness errors when coverage is unavailable.
This improves the previous unsupported-query rejection, but **does not implement
recent vector segments or background embedding enrichment**. Those remain required
for immediate vector visibility independently of archive publication.

Maintenance is opt-in through the Iceberg string property `antfly.maintenance.policy`,
containing a JSON policy. The existing supervised publication sweep executes bounded
compact, WAL GC and optional managed-catalog vacuum stages. A conditional object-store
ledger stores the exact operation request before work, leases, completion, retries and
backoff, so restart resumes the same operation. `POST /tables/{table}/lake/maintenance`
with `{"action":"status"}` reports the policy and durable progress. Scheduling does
not expand the existing compaction limits. Automatic vacuum requires explicit exclusive
ownership and remains disabled for REST catalogs with external metadata writers.

Example policy (serialized as the Iceberg property value):

```json
{"enabled":true,"interval_ms":3600000,"compact":true,"wal_gc":true,"vacuum":false,"max_rows":16384,"max_bytes":33554432,"max_deleted":128,"retain_ms":604800000,"keep_latest":2}
```

The Hacker News HTTP fixtures qualify a historical/current table pair, union ordering,
keyed precedence, accepted SQL after restart, receipt readiness and durable scheduling.
The ingestion CLI supports separate workers with immutable `--created-before` /
`--created-after` creation-time cohorts and independent state/table/warehouse roots.
Edits and deletes use the retained original creation time; missing timestamps fail
until reconciliation. Cutoff migration remains explicit. This is not a deployed
rolling-year service or full-archive latency qualification. Vendor subscriptions are
still attached/provisioned by operators; automatic vendor resource provisioning is a
separate remaining phase.

## Existing foundations and remaining work

SQL already applies joins, CTEs, set operations, windows, sorting and limits to
native and lake relations. This provides relational composition, including
`UNION ALL` and explicit precedence rules. It does not establish shared full-text
statistics across tables or automatic visibility of accepted lake WAL changes.
SQL full-text execution across a composed native/lake source needs separate
qualification before promising that capability.

The global JSON multi-query endpoint executes table requests and returns separate
responses. Existing `merge_config` concerns fusion of search indexes within a
query, not the definition of a multi-table input relation.

Native writable Iceberg tables currently provide durable acceptance, automatic
Parquet/catalog/index publication, and a bounded accepted-WAL text overlay over
a published archive baseline. SQL defaults to committed snapshots, with opt-in accepted visibility for direct
SELECT requests. Pending vector queries wait for matching publication. Maintenance
is bounded and can be explicitly invoked or scheduled through an opt-in policy. Additional vendor subscriptions are not automatically
provisioned. These are implementation boundaries, not the final product contract.

## JSON source expressions

Preserve existing single-table requests. Add a source expression to the global
query API for one composed result set. A union concatenates compatible input
relations; it does not implicitly deduplicate record keys:

```json
{
  "source": {
    "union": [
      {"table": "hackernews_current"},
      {"table": "hackernews_history"}
    ]
  },
  "full_text_search": {"match": "distributed databases", "field": "body"},
  "source_ranking": "rrf",
  "order_by": [{"field": "_score", "desc": true}],
  "limit": 20
}
```

For overlapping records, expose a distinct keyed overlay operator:

```json
{
  "source": {
    "overlay": {
      "base": {"table": "hackernews_history"},
      "changes": {"table": "hackernews_current"},
      "key": ["id"]
    }
  },
  "source_ranking": "rrf",
  "full_text_search": {"match": "distributed databases", "field": "body"},
  "limit": 20
}
```

The changes relation wins for a matching key. Its contract must define unique
latest versions, tombstone representation, null/key validation and provenance.
An ordinary current table that physically forgets deletes is insufficient to
hide historical versions. Resolve replacements and tombstones before filters,
counts, aggregation, ranking, sorting and pagination: a newer nonmatching row
must still suppress an older matching row.

Provider offsets are comparable only within their declared source epoch. Do not
infer precedence by comparing unrelated CDC offsets. Multiple writers require
an explicit conflict policy. A saved catalog view may encapsulate a source
expression so clients can query a stable logical `hackernews` name; its DDL and
authorization behavior remain to be specified.

## Binding, planning and execution

Bind every underlying table through the existing catalog resolver and engine
dispatch. Authorize each source, apply its row policies, validate compatible
column types and searchable fields, and retain table incarnation and object
generation fences. Reject unsupported combinations rather than silently changing
semantics. Overlay anti-lookups must not expose records hidden by authorization;
the row-policy/precedence contract requires explicit review.

Lower source expressions into shared relational operators, reusing SQL execution
where suitable and native search executors for indexed leaves. Apply exact
predicates through text/predicate indexes when eligible; use bounded residual
execution otherwise. Explain/profile should expose chosen scans/indexes, source
cuts, overlay coverage, and fallback or readiness decisions.

Pushdown is valid only when it preserves visibility. Independently filtering or
taking top-K from an overlapping source before resolving keys can produce wrong
results. Candidate expansion must account for shadowed rows and residual filters;
local top-K limits alone do not prove a correct composed top-K.

## Ranking, result identity and cursors

Offer explicit ranking contracts:

- Shared corpus scoring for compatible text indexes, with defined combined
  statistics and the engine's declared tombstone statistics semantics.
- Rank fusion or reranking of independently scored candidates when shared corpus
  scoring is unavailable. Declare candidate budgets and approximation explicitly;
  this is a different ranking contract from global BM25.

Index compatibility includes analyzer, field projection and scoring settings.
Reuse existing fusion machinery where appropriate, but keep source composition
separate from `merge_config`. Do not silently compare per-table BM25 scores as
though they were computed from one corpus.

Preserve source-table provenance and physical hydration identities; equal `_id`
values in independent union inputs must not collide. Keyed overlays additionally
expose stable logical identity. Apply a deterministic tie-breaker for ordering.
Cursors bind the source expression, policies, table incarnations, pinned source
snapshots, index publications, accepted-change cuts and ranking configuration.
Retain these cuts for a declared cursor lifetime or explicitly expire the cursor
when they are unavailable. The current lake text overlay invalidates cursors when
its archive/WAL cut changes; retained cursor generations are future work.

## Shared visibility for SQL and search

Introduce a reusable visible-row provider below SQL and JSON execution:

```text
pinned committed snapshot + accepted changes through a watermark
                            |
                resolve keyed replacements/deletes
                            |
            filter / aggregate / search / sort / paginate
```

Represent the accepted suffix as typed rows with stable keys and versioned
tombstones so SQL joins and aggregates can consume the same cut as search.
Avoid a full archive copy. Retain bounded memory, cancellation, deadlines and
restart reconstruction. The bounded direct-SELECT implementation now provides accepted-WAL visibility;
transactional/session modes still keep their committed-snapshot contract.

Specify freshness independently of source composition. Proposed modes should
support reading a published generation or requiring coverage through a write
receipt with a bounded wait. Report a readiness error if the requested coverage
cannot be met; do not silently fall back to older data. Final field names and
defaults must integrate with existing `sync_level` and snapshot contracts.

Expose durable acceptance, text/vector/enrichment searchable coverage,
lake-committed coverage and indexed serving coverage separately. Across independent
tables these are per-source cuts, not a common atomic transaction. Stronger
cross-table cutover requires an explicit coordinated serving descriptor.

## Vector and enrichment visibility

Durable row acceptance does not imply an embedding exists. Background enrichment
builds recent vector segments against pinned row versions and model/index recipes.
Publish vector coverage only after artifacts are durable and queryable. Replacement
and delete masks must apply to archive and recent vectors before candidate selection.

Define separate guarantees for text-only and hybrid/vector reads. A strict hybrid
query requires all participating indexes to cover the same requested row cut,
waits within its deadline, or returns a readiness error. Serving an older published
cut must be explicit. Failed enrichment exposes retry/error status and never
fabricates a vector. Qualification must include model changes, restart, deletes,
stale task completion and promotion from recent segments into archive publications.

## Automated maintenance

Use the existing durable `lake/maintenance` operations as the execution layer for
a supervised scheduler. Table policies should define target file sizes, small-file
and delete thresholds, snapshot retention, WAL retention and resource budgets.
Workers acquire fenced ownership, persist operation IDs and progress, resume
after restart, and back off under foreground load. A completed compaction wakes
searchable publication; WAL retirement waits for coverage and retained readers.

Keep dry-run planning and explicit operator overrides. Enforce the existing bounded
job limits or extend them through separately qualified streaming algorithms;
automatic scheduling alone does not remove current compaction size limits.
Expose job backlog, last success, failures and retained bytes.

Snapshot/file GC protects active readers, retained cursors, publication intents,
recovery proofs and external reader agreements. Delete only objects whose ownership
and unreachability are proven. Standard REST catalog commits do not automatically
coordinate new refs with vacuum. Require an authority-supported retirement protocol
or quiesced external metadata/ref writers; retain the current destructive-maintenance
restriction until that coordination exists. Object-store lifecycle rules cannot
substitute for reader-aware retention.

## Managed source setup

Extend the existing connection/source control plane with two explicit setup modes:
attach to existing vendor resources, or provision and reconcile Antfly-owned
resources for supported adapters. Record ownership, required permissions, setup
progress and teardown policy. Queries never provision subscriptions.

Cover S3/GCS notifications and database-specific CDC through separate adapters.
Reuse existing PostgreSQL `replication_sources` ownership and exported-snapshot
cutover instead of creating another coordinator. Normalize database transactions
into the common durable ingress, preserve retry identity, and acknowledge the
provider only after durable acceptance. Expose checkpoint, lag, reconciliation,
retention risk and reseed status. Backfill/stream cutover guarantees are explicit.

Object notifications are hints for authoritative snapshot reconciliation, not
row transactions or commit authority. Periodic reconciliation repairs missed or
duplicate notifications. HN API polling similarly needs reconciliation and cannot
promise gap-free vendor CDC. Credentials remain named connections/secret references.

## Hacker News rollout and qualification

The example currently targets one `hackernews` table. Its accepted-WAL overlay is
a bounded publication backlog, not a rolling year-long current tier. It does not
configure or qualify a deployed current/history pair.

Start the two-table example with an explicit date boundary and nonoverlapping
inputs: current-only by default, history-only on selection, and composed union
for all-time search. Define migration across the boundary so records are neither
lost nor duplicated; atomic all-time cutover needs a coordinated descriptor.
Date partitions alone do not handle edits/deletes of old HN records. Route these
to the historical writer or retain keyed changes/tombstones and use overlay
composition. Define that policy before claiming complete moderation visibility.

Implementation order:

1. Add disjoint-source DSL union, schema/auth binding, exact ordering/counts/cursors,
   a declared ranking contract, and HN current/history qualification.
2. Add keyed overlay composition with tombstone/precedence semantics and saved
   source definitions; prove visibility before filtering and candidate selection.
3. Extend typed SQL visibility to accepted changes, with receipt-bound coverage.
4. Add vector/enrichment coverage and coherent strict hybrid visibility.
5. Schedule bounded maintenance with reader-safe retention and catalog coordination.
6. Add managed provisioning and reconciliation for additional source adapters.

Reuse existing operators rather than introducing a second union executor. Tests
must cover overlap, newer nonmatching rows, tombstones, schema mismatch, source
authorization, incarnation changes, cancellation, restart, cursor retention,
global ordering and declared ranking behavior. Then qualify cold/restart latency,
selective indexed filters, backlog bounds and resource usage against archive-scale
HN data on remote storage. Small protocol fixtures do not establish full archive
performance or a production deployment.
