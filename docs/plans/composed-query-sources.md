# Composed query sources and recent/archive visibility

Status: native implementation and qualification, 2026-10-09. The implemented contracts and
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
`1 / (60 + visible_source_rank)`; scores are not shared-corpus BM25. The executor
streams each source in 128-hit pages and merges at most 16 source heads. Keyed
visibility resolves before assigning logical ranks. Indexed, unfiltered batched
anti-lookups suppress replaced base rows even if the replacement does not match.
Field ordering requires compatible sortable mappings/doc values in every leaf.
Pages allow 1–4096 hits; there is no 4096-position archive horizon. A 64 MiB memory
budget and request cancellation/deadlines apply. Union totals are exact. Ordinary
large overlay totals use `relation: "gte"`; an explicit `count: true` request
streams the entire visible relation for an exact count within its deadline.
Aggregation, hierarchy, graph queries, joins, analyses and stateful execution are
not composed yet.

`next_source_cursor` feeds `source_cursor` with the same source/query/ranking.
Its server-written, content-addressed descriptor stores only per-leaf positions,
retained snapshot capabilities and totals. Each leaf retains its original archive
publication, Iceberg metadata, recent segment references and bounded WAL final
images for the configured retention period. The composed cursor expires with the earliest leaf cut;
paging does not extend that lifetime. Publication and process restart preserve
that cut. Native durable reader leases and source snapshot pins protect archive
artifacts; copied WAL images allow independent WAL retirement. Access, row-policy,
schema/index recipe and source/table incarnation are rechecked on every page.
Drop/recreate or policy changes return a conflict. Mutable native-table leaves
retain an immutable physical generation with a server-written `native2:`
capability. Ordered native table queries return the same capability in
`remote_snapshot`; clients echo it with the hit's `_sort` tuple as `search_after`
or `search_before`. A bare cursor tuple cannot select a new live generation.
These are independent per-table cuts, not an atomic cross-table transaction.

Native owners drain accepted derivations to one sequence, then seal primary and
text/dense/sparse/graph projection manifests together. Immutable LSM runs and
source-vector blocks/sealed extents are hardlinked; only committed generated and
active source-vector WAL prefixes are copied, with a shared 16 MiB budget.
Both `primary_lsm` and `vector_store` source embedding ownership participate.
No result set or corpus-sized document copy is retained. Filtering, preflight,
term statistics and algebraic partials open that same readonly generation, so
ranking and totals do not drift between pages. The descriptor fences incarnation,
schema/index recipes and artifact-store identity; each physical manifest also
fences table/shard/range identity. A missing owner generation, incompatible storage
backend, active vector migration or incomplete projection fails explicitly;
resume never falls back to live data.

Capabilities expire after the configured `lake_indexes.query_cursors.retention_ms`
(default five minutes; bounded to one hour), including across daemon restart;
pages do not renew them. Composed cursors expire with their earliest leaf.
Operators also bound native cut count and retained bytes; shared immutable inode
extents count once. Per-cut shared file leases protect active readers from expiry
collection; subsequent captures reclaim expired cuts and interrupted staging
under a separate parent guard.

Native owners borrow a durable query-generation repository from their runtime.
Standalone uses its configured artifact provider or its local artifact directory;
distributed owners use the configured artifact connection. The common provider
supports filesystem, S3 and GCS. Capture seals the complete document, index and
vector generation, transfers authenticated 4 MiB chunks, and conditionally commits
one immutable manifest before returning a usable cursor. Table/shard/range identity,
recipe, expiry and artifact location remain authoritative; credential rotation does
not change native cursor identity. A replacement owner opens a missing local
generation through an authenticated remote manifest. Primary, index and vector
payload reads fetch the chunks they need through read-only storage leases; a
32-entry cache bounds retained chunk bytes to 128 MiB per open reader. Recovery
does not reconstruct the complete generation on local disk or recapture current
rows. Repository adapters without the remote-read capability retain the complete
verified staging-transfer fallback.
Missing or corrupt authoritative data fails with a catalog conflict.

Immutable SST/vector extents reuse expiring chunk inventories across captures;
new files and the bounded committed WAL suffix require transfer. Remote manifests
and chunks have bounded expiry collection; file hints are reclaimed by epoch.
A durable local commit marker avoids repeating provider checks for the same
sealed generation. It is cache metadata and is excluded from snapshot inventories.
Small remote owner registry records let the supervised collector discover expired
generations after local owner loss or table deletion. Each pass handles one owner
with a bounded deletion budget; registry records remain as discovery witnesses.
This adapter still requires filesystem-managed LSM checkpoints and compatible
native projection codecs. A retained request can carry its authenticated original
namespace and open that generation on a replacement range in the same table
incarnation. Each original namespace gets a separate cache root, preserving its
physical document identities. Creation cannot capture a different owner's live
namespace. Version-2 public capabilities retain the complete original ordered range
cover. Capture rejects a changed cover; continuation selects one current,
catalog-fenced carrier and opens each original immutable range exactly once.
Original logical group IDs remain distinct for distributed result merging, even
when a merged or replacement owner serves all origins. Search and work preflight
use the retained cover; this does not grant cross-table virtual catalog access.
Version-1 capabilities without that cover must restart their query. Storage-level
two-origin pagination and routing split/merge/incarnation checks are qualified;
public live-cluster topology-change qualification remains outstanding.
Query capture briefly closes write admission while derivations converge and
manifests are sealed; deadlines, cancellation and WAL/capacity admission bound
this work. The retained files never hold an apply lock across cursor pages.
The shared maintenance scheduler warms coherent private generations in bounded
8 MiB upload turns with a ten-second I/O deadline. Capture has a two-second
admission deadline. Chunk progress survives restart; foreground captures reuse
immutable extent proofs. Failed turns back off, and close cancels and joins the
worker. One private local generation is retained while uploading, then released;
warming is disabled when configured capacity allows only one cut. The repository
commits no readable manifest until every file is uploaded and revalidated.
First capture can still require a complete generation when hints are cold.
Remote-native checkpoint references and measured cold-publication latency remain
necessary before claiming that portability is latency-neutral.

Overlay keys are flat integer, string or boolean fields; numeric/timestamp key
normalization is not enabled yet. The changes input must retain one unique latest
row per key, including deleted rows with a boolean `deleted: true` (or the explicitly
configured `tombstone_field`). Indexed anti-lookups omit the user's search and filter
so a nonmatching edit still suppresses a matching base row. Tombstone rows never
appear in results. Physical removal from the changes table cannot express deletion
of an older base row. Saved source definitions are immutable catalog resources: `POST /sources/{name}`
with `{"source": <literal expression>}`, `GET /sources` or `/sources/{name}`, and
`DELETE /sources/{name}`. Query with `"source":{"saved":"hackernews"}`. Native
Antfly catalog persistence assigns a stable source ID; no SQLite is involved.
Reads authorize the saved name and every leaf. Definitions permit literal union
or overlay leaves only, preventing recursion. Drop/recreate changes the source ID
and invalidates previous cursors; definitions do not freeze table contents.

Direct HTTP SQL SELECT requests accept `lake_visibility: "accepted"`; the default
remains `"committed"`. A statement pins one committed source and one immutable WAL
suffix per writable lake table, reusing it across aliases and joins. Typed upserts
and deletes resolve before SQL filtering, aggregation, ordering and limits. Integer,
string and boolean stable keys are supported. Accepted visibility is not enabled
for connection/session/prepared or transactional modes, or timestamp/numeric keys.
Existing SQL scan and memory budgets still apply.

The intended extension to accepted SQL visibility uses a transaction-pinned
read contract. A transaction binds each writable lake table's incarnation,
committed snapshot and accepted WAL watermark on its first read of that table.
Subsequent statements, including prepared execution, reuse those cuts and add
the transaction's own final write images. Changes accepted by other writers
after that watermark remain invisible, even if background publication advances
the committed snapshot. Repeated aliases share the same cut. A prepared plan
binds table identity; execution binds visibility through the active transaction,
rather than freezing rows when the statement is prepared.

These cuts belong to the durable session authority, survive request boundaries,
and retain their source files until transaction completion or lease expiry.
Commit, rollback and session teardown release the cuts; savepoint rollback
changes the transaction's write overlay without recapturing its base read cut.
Recovery must restore the exact pinned snapshot and watermark or fail the
transaction explicitly. It must never substitute the latest published source.
Outside a transaction, each accepted statement obtains its own cut. This is the
agreed implementation contract; the transactional extension is still pending.

Writable-lake search requests accept `lake_read` with `visibility: "accepted"` or
`"published"`, an optional `through` receipt (`table_id`, `object_generation`,
`wal_lsn`) and `wait_ms` from 0 to 60000. Change acceptance includes those receipt
fields. Published reads explicitly select the archive publication and skip pending
changes. A receipt requires its coverage and rejects a different incarnation.
Vector/hybrid accepted reads use native recent HBC/sparse segments over the same
pinned WAL final images as text. Superseded archive vectors are masked before ANN
or sparse candidate selection, then compatible archive/recent scores are merged.
All declared vector recipes must finish for the requested cut; incomplete or failed
enrichment returns readiness rather than stale hits. The background worker runs
recent enrichment before WAL-to-Parquet draining, so blocked Parquet publication
does not block completed recent vectors. Materialized dense/sparse vectors and
managed embedding providers share the archive builder's row-to-vector semantics.
Templates render against the complete typed row through native template helpers,
including secret-aware remote content and binary media. URLs are resolved before
memoization, so cached vectors describe captured bytes. Native text/media chunking
creates independently indexed vectors rather than pooling a document's chunks.
Input/output bytes, media parts and unit counts are bounded and share the request
cancellation/deadline controls. Sparse recipes support text chunks and reject
media explicitly; dense media support depends on the configured provider.

Chunk membership and original source units are durable records in the same
native vector checkpoint. They retain offsets, time/frame coordinates and source
fingerprints. Private vector identities extend the parent row identity with an
ordinal; public IDs use Antfly's canonical artifact key encoding and index-specific
`<index>_chunks` / `<index>_sources` names, preventing collisions between recipes.
Parent, member and unit result shaping uses the native hierarchy pipeline and its
bounded grouping/candidate expansion. Overlay visibility is evaluated on each
chunk's parent before vector selection, so an edit/delete suppresses every old
archive chunk. Incremental publication retires all replaced chunk/source records
and rebuilds the changed input's complete membership. Archive and accepted-WAL
builders use the same producer, binary-safe memo identity and durable payloads.

Durable, ETag-fenced jobs bind table incarnation, archive generation, WAL cut and
index recipe. Completed per-input embeddings are memoized for seven days and
reused after restart and during archive promotion. Leases fence stale completion; failures persist retry/backoff and errors.
Graceful cancellation conditionally releases unfinished claims with a separate
one-second cleanup budget, including ambiguous successful claim writes recovered
by random ownership token; hard crashes use the 120-second lease expiry. `{"action":"enrichment_status"}` on
`/tables/{table}/lake/maintenance` reports completed/failed recent coverage. Jobs
and segment artifacts retain for 24 hours; bounded background conditional cleanup
collects expired state. Index work still obeys the bounded WAL, memory and request
budgets; large jobs make vendor progress through durable memoized inputs. A live
embedding vendor and a compatible published baseline are required for managed
embedding indexes. Acceptance is not a promise that an embedding already exists.

Maintenance is opt-in through the Iceberg string property `antfly.maintenance.policy`,
containing a JSON policy. The existing supervised publication sweep executes bounded
compact, WAL GC and optional managed-catalog vacuum stages. A conditional object-store
ledger stores the exact operation request before work, leases, completion, retries and
backoff, so restart resumes the same operation. `POST /tables/{table}/lake/maintenance`
with `{"action":"status"}` reports the policy and durable progress. Scheduling does
keeps each compaction turn within its row/byte limits while a durable file,
row-group and row cursor resumes a larger selection across restarts. Output pages
are immutable; final publication retains the original parent requirement. Confirmed
parent conflicts terminate the job; unknown outcomes replay the exact intent.
Antfly-owned vacuum requires explicit exclusive ownership and publishes an
irreversible file-retirement root through the same catalog HEAD CAS used by
writers. Future commits reject retired references; earlier staged writers conflict.
The authority independently verifies that selected files are absent from every
current snapshot. A content-addressed persistent radix index avoids rewriting a
flat archive-sized retirement list. Reader admission remains fenced by durable
snapshot pins. Destructive REST vacuum cannot use an exclusive-ownership assertion
to bypass catalog coordination. Optional Nessie/Polaris controller integrations
journal and delegate provider-owned maintenance, with explicit capability checks
for writer fencing, external readers and the shared Antfly pin registry. The provider-side gateway/controller implements both integrations with conditional
object-store authority, real catalog operations and local real-provider qualification.
Production deployment must enforce private vendor credentials and external reader
leases; archive-scale qualification remains. See [external maintenance](../design/external-lake-maintenance.md).

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
a published archive baseline, with coherent recent vector/enrichment segments.
SQL defaults to committed snapshots, with opt-in accepted visibility for direct
SELECT requests. Pending vectors wait for recent enrichment or explicit archive
coverage. Maintenance is bounded and can be explicitly invoked or scheduled
through an opt-in policy. Additional vendor subscriptions are not automatically
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
authorization behavior follow the native immutable source catalog contract above.

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
when they are unavailable. Retained lake cuts preserve archive/WAL pagination across publication and restart
within their configured lifetime (five minutes by default, at most one hour);
policy, recipe and incarnation changes invalidate them.

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
durable compaction progress removes the former total-input row/byte limit, while
metadata manifests, decoded Parquet pages, output count and progress documents
remain bounded. Oversized metadata manifests still require a streaming selector.
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

The example supports independent historical/current workers and HTTP qualification
of a two-table union or overlay, including a saved logical source. Its accepted-WAL
suffix is a bounded publication backlog, not a rolling year-long current tier. A
public deployed current/history service still needs archive-scale latency qualification.

Start the two-table example with an explicit date boundary and nonoverlapping
inputs: current-only by default, history-only on selection, and composed union
for all-time search. Define migration across the boundary so records are neither
lost nor duplicated; atomic all-time cutover needs a coordinated descriptor.
Date partitions alone do not handle edits/deletes of old HN records. Route these
to the historical writer or retain keyed changes/tombstones and use overlay
composition. Define that policy before claiming complete moderation visibility.

The first five phases below are implemented within the contracts above; full-archive
qualification and the additional adapters in phase six remain:

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
