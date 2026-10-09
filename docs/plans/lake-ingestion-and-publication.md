# Lake ingestion, change capture, and publication

Status: architecture and implementation contracts, captured 2026-10-08. The
native transaction ingestion section describes the implementation in PR #1025;
recent text overlays and bounded compaction/GC are implemented. JSON source
composition, opt-in accepted SQL visibility, receipt-aware publication waits,
automated maintenance, and remaining extensions are described in
[Composed query sources and recent/archive visibility](composed-query-sources.md).
Existing behavior remains documented in
[LAKES.md](../../zig/LAKES.md), [REMOTE_TABLE_SERVING.md](../../zig/REMOTE_TABLE_SERVING.md),
[CDC.md](../../zig/CDC.md), and [SERVERLESS.md](../../zig/SERVERLESS.md).

## Problem and intended behavior

Antfly already reads remote Parquet/Iceberg snapshots and maintains native
indexes over external rows. Its serverless path also has durable WAL ingest and
publication of Antfly-owned artifacts; native row-fragment publication is a
separate existing foundation. Native writable Iceberg now adds catalog commits,
bounded recent text visibility, JSON union/keyed composition, opt-in accepted
SQL visibility and scheduled bounded compaction/GC. Recent dense/sparse vector
segments, durable enrichment jobs, saved source definitions and retained composed
cursor cuts are implemented for writable lakes. Additional managed connectors,
multimodal/chunk enrichment and mutable native-table cursor retention remain
extensions.

The long-term goal is one table/query contract for externally owned lakes and
Antfly-owned data, with a shared durable ingestion path for application writes,
database CDC, and custom pipelines. Object notifications should accelerate lake
discovery without becoming the authority for table commits. S3, GCS, and future
object stores should use the same format, indexing, and execution machinery.

Hacker News is a motivating integration, not an engine-specific source type.
An HN adapter owns API polling, normalization, moderation interpretation, and
story ancestry. With native transaction ingestion, Antfly owns durable mutations,
progress, archive publication, indexing, and recovery. The Python adapter retains
its source normalization state in Antfly Lite; its optional PyIceberg file writer
is an interim producer mode, not the required product topology.

## Related table-object-storage implementation

A source review of `.worktrees/table-object-storage` on 2026-10-08 covered
`feat/table-object-storage` at `c7988f0abc`, its initial implementation commit
`c07c75362a`, and the uncommitted request-lifetime/routing changes present during
review. These are branch observations, not claims of merged or released behavior;
the review did not execute its runtime tests. The branch's living contract is
`docs/design/table-object-storage.md`.

This work provides the hosting foundation for the plan:

- `storage.engine: local | object` is a table choice, independent of deployment,
  document/relational schema, and external/owned base source. Native API processes
  can host local and object tables together; the serverless command remains a
  deployment preset rather than the only way to use durable objects.
- Object tables allocate no data ranges or data Raft replicas. Native metadata
  still owns table existence, definitions, and incarnation. Metadata consensus
  does not establish linearizable object-data visibility by itself.
- `api/object_table_runtime.zig` hosts the existing object WAL, progress, catalog
  projection, manifest, build, and query stack for owned document tables. Its
  private catalog projection is not another public DDL authority. External lake
  attachments continue through native catalog-fenced lake publication.
- The stack receives an authorized `objectstore.Client` through
  `BootstrapConfig.native_location`, so its execution is independent of S3/GCS
  URI-specific bootstrap. The physical destination uses `storage.artifacts`;
  metadata pins a credential-independent locator digest and an incarnation
  generation. Credential rotation does not imply relocation, and recreated
  tables cannot inherit a dropped incarnation's object root.
- Existing WAL/HEAD coordination and publication fences are reused. Request
  deadlines span native binding and object dispatch; once WAL acceptance occurs,
  timeout handling preserves the durable outcome rather than reporting a
  pre-commit rejection. This is the required boundary for future source receipts.

The reviewed capability boundaries matter for implementation planning:

| Capability | Reviewed branch boundary | Extension needed here |
| --- | --- | --- |
| Owned object writes | Document batch, lookup, and search over Antfly publications | Optional Parquet/Iceberg writer and authoritative catalog commits |
| Owned relational tables | Rejected; external relational lake reads remain supported | Typed write/constraint capability before admission |
| CDC | Object creation rejects nonempty `replication_sources` | Engine-aware canonical apply, source receipts, and checkpoint integration |
| Read visibility | Owned lookup requires explicit stale published-HEAD reads; indexed sync can wait for publication | Durable WAL visibility/overlay protocol for stronger reads |
| Definition evolution | Owned object schema/index definitions and engine are immutable | Fenced versioned migration before online schema/index changes |
| Destination selection | Shared node-configured destination is pinned per table | Optional named per-table destinations with resolved durable bindings |
| Worker lifetime | Bounded hosted runtimes; dropped entries retained until restart | Safe runtime draining/eviction and owned-root retention/collection |

Implement connectors and format writers around this table-engine dispatch and
authority model, rather than constructing a second object-backed ingestion
service. Antfly-native object publications remain valid without Iceberg. Selecting
`engine: object` alone must not promise Parquet output, Iceberg write authority,
CDC support, or stronger read consistency than the admitted capability provides.

## Independent storage and source layers

| Layer | Contract | Initial and future implementations |
| --- | --- | --- |
| Object storage | Bounded range reads, streaming writes, object identity, conditional operations, listing, cancellation | Existing S3/GCS/file adapters; future providers |
| File codec | Typed column batches, field identities, compression, statistics, bounded encoding/decoding | Parquet first; additional codecs independently |
| Table format | Schema/partition evolution, snapshot inventory, delete semantics, snapshot differences | Iceberg; future formats; Antfly-native fragments remain independent |
| Catalog | Resolve an authoritative table head and conditionally commit metadata changes | Iceberg REST/provider adapters; explicitly Antfly-managed catalogs |
| Source connector | Backfill, change discovery/decoding, resumable source positions | Existing Postgres CDC; additional databases, lake notifications, warehouse APIs |
| Antfly engine | Durable admission, typed execution, indexes, serving publication, maintenance | Shared across providers and source types |

Build on `zig/lib/objectstore`, external-source inventories, `RowSource` /
`ColumnBatch`, native fragments, and existing publication/catalog fences.
Choosing `gs://` instead of `s3://` must not choose different table semantics.
Table formats and catalogs must not be embedded into provider-specific I/O.

Adapters declare capabilities rather than pretending all sources have the same
guarantees: conditional replacement/create, stable versions, streaming upload,
consistent backfill, ordered CDC, snapshot differences, predicate/projection
pushdown, and writable catalog commits. Unsupported guarantees fail explicitly.
An S3-compatible endpoint needs qualification for the operations actually used.

A warehouse with an open Iceberg table can reuse catalog and file adapters. A
warehouse exposing only query/read APIs needs a batch row-source or ingestion
adapter; its table is not assumed to be accessible as Parquet objects. Direct
BigQuery reads and BigQuery exports are distinct integration paths. Read,
ingestion, and write capabilities may differ for the same provider.

## Table ownership and write modes

### Catalog authority decision

Implement both an Antfly-managed catalog and an external Iceberg REST catalog
behind one capability-driven contract. Use the managed catalog for HN and other
Antfly-owned archives; allow customers to retain the existing authoritative
catalog for shared lakes. Neither choice changes the object-store provider or
requires indexes to exist only on local disk. Data files, index artifacts,
publication manifests, and recovery records are durable remote objects; local
disk and memory contain disposable serving caches.

| Concern | Antfly-managed authority | External Iceberg REST authority |
| --- | --- | --- |
| Setup | Antfly owns the table's durable catalog head in its configured object location | A configured catalog connection identifies the existing namespace and table |
| Commit | Antfly validates expected state and changes the head conditionally after writing immutable metadata | Antfly sends requirements and updates; the service validates and writes committed metadata |
| Other writers | Writers must participate in Antfly's catalog protocol | Other lake engines use the same catalog authority |
| Operations | Antfly owns fencing, commit validation, recovery, retention, and compatibility | Adds service availability/authentication dependencies and capability negotiation |
| Serverless | Catalog head and recovery state survive worker loss in object storage | External service owns the durable commit authority; Antfly recovery state remains remote |
| Interoperability | Iceberg files are interoperable; a standard REST server is a separate future exposure of the managed authority | Standard REST client interoperability, subject to discovered server capabilities |

For REST, a client uploads data/delete files and submits commit requirements and
metadata updates. It does not independently replace the metadata head. Conflicts
require refresh and replanning; timeouts require outcome resolution before replay.
Credential vending, remote signing, idempotent retries, and multi-table commits
are negotiated capabilities, not assumptions. See the
[Iceberg REST protocol](https://iceberg.apache.org/docs/latest/rest-protocol/).

For managed authority, an object-store CAS is the atomic publication primitive,
not a substitute for Iceberg validation. Validate table identity, requirements,
schema/spec/sort references, snapshot and sequence evolution, and immutable
metadata before replacing the head. Preserve unknown commit outcomes and prevent
stale writers from publishing. The existing private object-table catalog is an
Antfly WAL/publication projection, not an Iceberg catalog. See the
[Iceberg specification](https://iceberg.apache.org/spec/).

The shared contract resolves and pins metadata, conditionally commits changes,
resolves uncertain outcomes, declares capabilities, and identifies ownership and
credential scope. Object storage, format, catalog authority, and deployment remain
independent choices. Both catalog adapters must feed the existing lake inventory
and index publication machinery rather than establishing a second serving path.

Lake commit and index publication remain separate milestones. Expose durably
accepted, lake-committed, and searchable watermarks. Recover a crash between lake
commit and index publication, and retire recent mutations only after a complete
matching publication. A persistent Lite ingestion worker is valid; an ephemeral
worker's local state cannot be the sole authority for serverless durability.

1. **Externally managed attachment.** The external catalog owns table commits.
   Antfly follows committed snapshots, stores its own derived indexes, and
   queries base files in place. No complete row import is required. Default row
   writes are rejected, as in the current read-only lake contract.
2. **Antfly-managed table.** Antfly accepts canonical mutations, owns durable
   progress and publication, and writes through the selected storage/format /
   catalog adapters. Native fragments remain a valid storage choice; Iceberg
   interoperability is optional rather than the core storage protocol.
3. **Explicit overlay or delegated writer.** An external table may opt into a
   durable Antfly overlay or authorize Antfly to commit through its catalog.
   Ownership, primary keys, external writer conflicts, and reconciliation must
   be defined before enabling either mode.

These refine the future modes in LAKES.md; they do not add working schema enums
or imply that arbitrary external attachments are writable. Preserve one
authoritative base and rebuildable indexes/caches. Avoid a permanent full copy
of the historical dataset solely to drive publication.

## Three ingestion entry points

Expose three ways to feed the same engine:

- **Application mutations:** existing table batch semantics, extended only as
  needed for source identity, idempotency, and explicit visibility receipts.
- **Managed source connectors:** configure a connection and table mapping;
  Antfly owns source discovery, backfill, checkpoints, retry, and status.
- **Authenticated commit hook:** custom pipelines report a committed snapshot
  or a committed file manifest. Antfly validates authority and schedules
  reconciliation. A hook is not permission to trust arbitrary supplied URIs.

Row changes and lake changes are different inputs. Database CDC supplies keyed
inserts/updates/deletes and source transaction positions. S3/GCS notifications
identify objects that changed. Iceberg catalog discovery identifies a committed
table snapshot. All can share job ownership, retry, observability, and durable
progress without translating every object event into a row mutation.

For plain Parquet datasets, an explicit manifest/commit hook should identify a
complete set of immutable objects and their versions. A prefix listing remains
a convenience discovery mode, not a guarantee of an atomic multi-file update.
For Iceberg, use the authoritative catalog commit, not the arrival of individual
data files. Events can be duplicated, delayed, or reordered; periodically
reconcile the authoritative source to repair missed notifications.

The following is an illustrative source-resource shape, not a current endpoint
or generated OpenAPI schema:

```json
{
  "type": "iceberg",
  "catalog": {
    "type": "rest",
    "connection": "warehouse_catalog"
  },
  "table": "analytics.hackernews",
  "changes": {
    "mode": "notifications",
    "connection": "archive_events",
    "reconcile_interval": "5m"
  }
}
```

A database source additionally needs source table/key mapping, a resumable CDC
position, and a declared backfill/cutover guarantee. Reuse and evolve existing
`replication_sources` and status metadata rather than creating a second
independent Postgres coordinator. The final source-resource API and migration
from current configuration remain open decisions.

Connections carry credential references, endpoint and resource scope. Prefer
workload identities where available. Hooks require authentication, table-level
authorization, size/rate bounds, and validation against allowed source locations.
Do not put raw secrets into source definitions or checkpoints. Provisioning
notification subscriptions, CDC slots/publications, and IAM needs explicit
setup behavior and visible ownership; it is not an implicit query side effect.

## Durable apply and source checkpoints

Source adapters normalize row changes into bounded batches containing source
identity/epoch, source positions, stable record keys, mutation operation, and
transaction boundaries where available. Source positions are provider-specific;
there is no assumed globally comparable offset across connectors.

The control-plane owner orchestrates source jobs and owns durable progress.
Writes go through the canonical data-plane path, including schema validation,
transforms, indexes, and enrichments. Preserve CDC.md's metadata/data ownership
split. Workers need leases and fencing so a former owner cannot advance progress
or publish after reassignment; leases alone do not establish commit authority.

Use at-least-once delivery with idempotent, version-aware apply. Duplicate events
must not create duplicate records, and stale source changes must not overwrite
newer values. Primary-key changes, partial update records, tombstones, and source
transactions need explicit normalization. Multiple sources writing the same key
require a configured conflict policy rather than comparing unrelated offsets.

Advance a checkpoint only after Antfly durably owns all changes through that
position. Where mutation apply and progress span separate stores, use a durable
batch identity and replay protocol; do not assume a cross-store atomic commit.
Re-read ambiguous outcomes before retrying. Never checkpoint past a failed or
unresolved batch. Acknowledge event delivery only after durable acceptance of
its reconciliation work, not merely after placing work in a process-local queue.

Backfill must establish a source boundary before streaming catch-up. Reuse
Postgres's exported-snapshot/slot cutover where available. Other sources must
declare exact or non-exact cutover, retention requirements, and reseed behavior.
Periodic polling without a durable change log cannot promise gap-free CDC.
Expose that limitation and the reconciliation policy in source status.

## Native archive publication

For an Antfly-managed table:

1. Admit mutations into the deployment's canonical durable write path and assign
   an Antfly coverage watermark. Return a durable acceptance receipt.
2. Maintain recent searchable changes and tombstones, according to requested
   indexing/enrichment visibility. This is pending archive work, not a required
   complete duplicate of the historical base.
3. A bounded background writer consumes a pinned mutation range, writes new
   immutable data/delete files, and persists recoverable publication intent.
4. Commit through the table's authoritative catalog with validated expected
   state. Resolve conflicts and ambiguous replies by inspecting committed state.
5. Publish matching Antfly indexes and a serving descriptor binding the source
   snapshot, schema, index definitions, and mutation coverage.
6. Retire covered recent changes only after recovery and retained readers no
   longer require them. Collect unreferenced uploads through reader-safe GC.

Iceberg writes must obey schema field IDs, partition specs, sequence/delete
semantics, and the catalog's optimistic commit protocol. A provider's conditional
object PUT is a useful primitive, not a replacement for catalog requirements.
The current supported directory `metadata/version-hint.text` convention is not
the universal commit protocol for catalog-managed Iceberg tables.

Data commits and index publication are separate operations; there is no assumed
distributed transaction between an external catalog and Antfly metadata. Persist
enough intent to recover every boundary, including data committed but indexes
not published. Upload completion alone never makes data or an index query-ready.

File-level updates/deletes and bounded compaction should replace the initial
HN worker's affected-month rewrites. Compaction is itself a snapshot commit.
Retention must protect pinned table snapshots, serving generations, and in-flight
work. Antfly must not delete externally owned data; managed-table collection
must respect catalog retention and readers in other engines as well as Antfly.

## Unified recent/archive query semantics

The [composed-source proposal](composed-query-sources.md) defines a proposed JSON
DSL for disjoint unions and keyed overlays, including ranking, authorization,
cursors, SQL visibility, and a two-table HN rollout. Existing SQL relational
composition and JSON multi-query responses are foundations, not proof of a shared
ranked corpus or accepted-change visibility across independent tables.

A statement pins an archive snapshot, a compatible index publication, and a
recent-change watermark. Newer keyed changes replace historical versions;
tombstones suppress historical rows. Apply that visibility rule before filters,
aggregates, counts, ranking, sorting, LIMIT, and pagination. Querying two tiers
and concatenating their results is not correct.

Logical mutation identity is a stable record key plus version. Physical lake row
references remain snapshot/file/ordinal-bound for hydration. Compaction changes
physical locations without changing logical record identity. A source without
stable keys cannot silently receive keyed upsert semantics.

Text ranking across tiers needs a defined corpus/scoring contract and sufficient
candidate evaluation; independent top-K lists are not automatically a correct
global top-K. Cursor tokens must bind both archive and overlay coverage, and
expire explicitly if their retained generation is unavailable.

Preserve the existing freshness contract: consume only proved index coverage;
scan uncovered data through a correct bounded fallback, wait/reject when the
requested visibility cannot be met, or serve an older snapshot only when the
request explicitly permits it. Never silently mix a new source with old indexes.

Expose durable acceptance, searchable coverage, lake-committed coverage, and
indexed serving coverage separately. Integrate these receipts/watermarks with
existing `sync_level` behavior; do not redefine current acknowledgements or
equate lake commitment with synchronous indexing.

## Storage durability, deployment, and cold starts

Durable data files, index artifacts, publication metadata, pending work, and
accepted mutations must survive worker replacement. Stateful deployments can
use their canonical replicated storage; serverless deployments use the durable
WAL/catalog/artifact substrate. Local SSD/RAM hold bounded caches, staging, and
scratch. A cache miss or discarded cache must not lose acknowledged writes.

Antfly Lite is suitable for embedded deployments and persistent worker state,
with stable snapshots for backup. A Lite file on ephemeral disk plus occasional
backups is not sufficient durability for serverless write acknowledgements.
Do not require the HN example's separate persistent Lite worker for every hosted
source connector.

Restart should load a small committed serving descriptor and lazily fetch
version-bound metadata/index pages, reusing authenticated proofs where valid.
It must not require listing the whole bucket, scanning all historical records,
or rebuilding indexes before the first query. Bound metadata loading, remote
fanout, working memory, and cancellation independently of archive size.

Stateful, standalone, embedded, and serverless modes should reuse source/codec /
query/publication contracts. Scheduling and durability ownership vary by
deployment; a shared engine does not imply identical transaction guarantees.

## Status and setup experience

The setup flow should configure a connection, validate capabilities and access,
select the source/table/key mapping, declare ownership and backfill policy, and
start the job. Notification delivery with periodic reconciliation and polling-only
discovery should both be supported where appropriate. Do not require users to
operate a custom writer merely to attach an existing external table.

Expose source phase, snapshot/cutover guarantee, accepted and applied source
positions, searchable/lake/index coverage, backfill progress, pending work,
ingestion/index/publication lag, last reconciliation, retry/error class, and
reseed guidance. Explain should show the pinned source and serving generations,
overlay coverage, selected indexes, and any fallback. Bound queues and disk use;
surface backpressure rather than allowing unlimited lag or retained changes.

## Implementation stages and validation

1. **External attachment and change discovery.** Add capability-driven catalog /
   notification adapters and the authenticated commit hook around existing lake
   reconciliation. Validate duplicate/reordered/missed events, incomplete file
   uploads, catalog advancement, source authorization, and provider versions.
2. **Shared source jobs.** Extend existing CDC ownership/checkpoint/status seams
   for additional sources. Validate snapshot-to-stream cutover, transaction
   replay, source retention loss, failover fencing, bounded backpressure, and
   crash after durable apply but before checkpoint acknowledgement.
3. **Native managed archive writer.** Implement bounded Parquet output and proper
   catalog commits alongside native-fragment publication, reusing table-level
   object hosting as it lands. Validate concurrent
   writers, schema/partition evolution, updates/deletes, ambiguous commit replies,
   and crashes at every upload/commit/index-publication boundary on S3 and GCS.
4. **Recent/archive merge and visibility.** Validate against a single logical
   row oracle, including update suppression, deletions, exact totals, ranked
   results, sort/cursor pagination, cancellation, and restart at each watermark.
5. **Maintenance and operational qualification.** Add bounded compaction and
   reader/catalog-aware retention. Measure cold/warm/restart latency, backfill
   throughput, catch-up time, remote requests/bytes, peak memory, and concurrent
   ingestion/query behavior at archive scale. Qualify future providers through
   the same contract suites rather than URI parsing alone.

Stages can overlap, but a connector setup API must not advertise a write or
cutover guarantee before its underlying protocol is qualified.

## Open decisions

- Source-resource API versus extensions to existing table replication sources;
  connection reuse, status routes, and compatibility migration.
- External-writer row conflict policies and optional REST exposure of the managed
  catalog; both managed and external REST catalog adapters are required.
- Recent-tier realization per deployment, transaction scope, text corpus scoring,
  and bounded exact query behavior when archive indexes lag.
- Plain-Parquet manifest protocol, key/schema requirements, and hook receipts.
- Notification transport adapters, subscription provisioning/ownership, warehouse
  capabilities, and missing-log/reseed policies.
- Compaction policy, external-reader retention evidence, publication cadence,
  and operational limits. No fixed performance guarantees are established here.

### Native transaction ingestion and searchable publication

The native row ingress is `POST /db/v1/tables/{tableName}/lake/changes`.
It is available for a current-snapshot Iceberg binding with `iceberg_writer`
and either managed or REST catalog authority. It requires table admin permission
and independent Antfly artifact storage with `storage.primary`; lake read
credentials alone cannot authorize ingestion or publication writes.

A transaction supplies `batch_id`, `source`, `epoch`, `checkpoint`, optional
`expected_checkpoint`, `key_fields`, and an ordered `changes` array. Each change
is an `upsert` with a complete row image or a `delete` containing only its key
fields. Provider offsets are opaque strings: the predecessor must match the
previously admitted transaction, and Antfly never orders unrelated offsets.
Only one source epoch and key definition owns a table in this implementation;
changing source ownership needs an explicit reseed/migration. Multiple source
conflict policies remain a separate extension.

This is the common push boundary for database CDC adapters and application
producers, rather than a new PostgreSQL polling coordinator. Existing managed
PostgreSQL slot/publication and exported-snapshot orchestration stays in
`replication_sources`. An adapter supplies complete transactions and must only
acknowledge a provider transaction after durable acceptance. It must preserve
its stable batch identity after a timeout. Partial images, key changes, and
provider-specific snapshot/stream cutover must be normalized by the adapter;
a key change is a delete of the old key plus an upsert of the new key in the
same transaction. An object-created notification is not a row transaction and
continues to require authoritative snapshot reconciliation.

Native admission validates images against the pinned Iceberg schema and appends
the transaction to Antfly-owned object storage. The WAL uses immutable segment
headers and separate content-addressed row payloads,
immutable request intents, a conditional tail, and durable acceptance receipts.
Recovery walks only small headers and loads one bounded transaction payload.
A tail response lost after successful CAS is resolved from the segment chain.
A retained receipt makes an old request retry constant-size work. A bounded
pending window provides backpressure while publication is unavailable; normal
append work does not rewrite an archive-length log. The WAL namespace includes
native table identity, object generation, and the configured catalog authority,
so a recreated table or changed binding cannot inherit another writer's queue.
WAL segments and receipts are retained; automated reader-safe WAL retention is
not enabled by this path.

One transaction is drained per publication-worker pass. The native writer
preserves Parquet field IDs and nullability and writes immutable Parquet data,
Iceberg equality deletes, data/delete manifests, and a manifest list. Repeated
keys in a transaction keep their final mutation. Deletes and replacement data
share a new sequence number: equality deletes suppress earlier versions and
leave their replacement rows visible. Unpartitioned manifests allow global key
deletes across existing partition specs without rewriting historical files.
Existing manifest references are retained in the new snapshot. The writer
supports flat primitive scalar schemas; unsupported nested and decimal schemas
fail admission explicitly instead of dropping fields or encoding JSON as text.

An exact catalog commit request is saved before calling the authority. Recovery
replays that request or resolves its identity; it cannot rebase an ambiguous
outcome. Catalog commitment advances the source checkpoint and WAL coverage in
one metadata transaction. Neither uploaded files nor WAL acceptance advance
these committed watermarks. A conditional conflict can be retried against a new
pinned metadata version only after the prior outcome is proven uncommitted.

Catalog commits and accepted transactions wake the existing supervised index
publication worker. Its periodic authoritative sweep recovers missed wakeups,
external commits, and restarts, including writable tables without requested
indexes. Each draining pass also publishes the resulting snapshot's matching
text and predicate indexes, so a continuous producer cannot indefinitely starve
search visibility. A published generation remains fenced by the source snapshot,
schema, index recipes, credential identity, artifact store, and native table
incarnation. Upload completion alone never makes indexes ready.

The acceptance response contains `state: accepted`, `wal_lsn`, and
`searchable: false`: acceptance does not assert that an archive index already
exists or that every search mode can serve the transaction. The catalog properties
`antfly.wal.coverage` and `antfly.cdc.checkpoint` expose committed progress.

### Immediate text search

With a published baseline text/predicate index, a query pins the archive generation
and accepted WAL tail. It resolves the suffix's stable keys against the archive
predicate index, masks superseded rows with query-private tombstones, and builds
native text segments for the last upsert per key. Archive readers and encoded
bytes stay shared. One composed native corpus supplies BM25 and global top-K;
this is not a merge of independently scored result lists. Immutable segment term
statistics follow the ordinary native tombstone scoring contract. Filters, sorting,
hydration and snippets use the same visibility cut. Deletes do not create hits.

The suffix is bounded to 64 transactions, 32 MiB and 65,536 distinct changes.
A query fails closed when coverage is unavailable or the bound is exceeded.
The archive must be an ancestor of the current head through native WAL or
compaction transitions: an unpublished external writer commit requires archive
publication. Cursors retain both the archive publication and accepted tail for 60 seconds in
object-store descriptors. Publication and restart preserve the original cut;
recipe, policy and incarnation changes invalidate it.
The overlay is reconstructed from durable WAL after restart. Direct SQL SELECT
requests can opt into `lake_visibility: "accepted"`; session/transaction modes
retain committed visibility. `lake_read` selects accepted or published search
visibility and optional receipt coverage with a bounded readiness wait. Pending
vector queries wake background enrichment and wait for coherent native recent
HBC/sparse segments, independently of Parquet publication. Durable per-input
embedding completions resume after restart and are reused during archive promotion.
Archive/recent vector masks apply before candidate selection. See the
[implemented contracts](composed-query-sources.md#implemented-contracts).

### Compaction and garbage collection

`POST /tables/{tableName}/lake/maintenance` accepts `action` (`compact`, `vacuum`,
`wal_gc`, `status`, `enrichment_status`), a stable `operation_id` for mutating/planning jobs, and defaults
to `dry_run: true`. Jobs can be invoked explicitly or scheduled through the
Iceberg `antfly.maintenance.policy` property. Durable CAS progress resumes after
restart; `status` reports that progress without an operation ID. Scheduling is
opt-in and does not provision vendor resources. Publication is woken after a
successful compaction commit.

```json
{"action":"compact","operation_id":"hn-compact-2026-10-08","dry_run":false}
```

Compaction scans delete-aware live rows from whole selected manifests in one
pinned parent snapshot and writes standard native Parquet/manifests. Per-pass
limits are 16,384 physical rows, 32 MiB input and 32 manifests; oversized external
manifests/files are skipped. Oldest sequences are selected first. Untouched files
retain their sequence numbers. Equality-delete manifests are removed only when
all remaining data sequences prove those deletes obsolete; position deletes
remain unless the full data inventory is rewritten. Exact durable catalog intents
are replayed after interruptions, without rebasing ambiguous commits.

Vacuum first expires unprotected Iceberg snapshots through the catalog CAS,
then marks retained snapshot file graphs and sweeps only unreachable objects
with native ownership proofs. Defaults retain seven days and the newest two
snapshots; named refs, current/live index publications, and durable snapshot
readers are roots. SQL scans, compaction and index construction hold renewable
snapshot pins (two-minute leases, thirty-second renewals and thirty-second
retirement grace). Hosts must keep clock skew within that grace, as with the
existing publication-reader leases. Antfly deployments sharing a writable lake
must share the durable artifact/pin authority; moving it requires a coordinated
reader drain and retention migration. Snapshot readers need write access to
that shared pin lane even when source data credentials are read-only. Renewal
failures or expired readers fail
closed, including during cached reads. A full reader mark pass precedes any
shared-file deletion. Native ownership markers bind URI, content digest and
ETag; deletes are conditional on that original ETag. Unmarked external files,
legacy files, catalog history and orphan uploads outside retired snapshot graphs
are retained. This is intentionally conservative migration behavior.

Destructive file vacuum requires `exclusive_ownership: true`, acknowledging an
exclusive native-owned file lifecycle and an external-reader retention contract.
For a REST authority, this agreement also requires quiescing administrative and
external metadata/ref writers for the lifetime of the vacuum job: the standard
REST requirements do not provide a full-metadata CAS that can reject creation
of a previously unknown named ref. Writers must not resurrect expired files.
Antfly cannot observe leases of arbitrary external engines. Use a catalog's own
maintenance integration when that authority owns shared-file cleanup.

```json
{"action":"vacuum","operation_id":"hn-vacuum-2026-10-08","dry_run":false,
 "exclusive_ownership":true,"max_deleted":128}
```

Vacuum jobs persist their expired graphs before commitment and resume bounded
sweeps after restart. WAL cleanup separately uses the minimum committed coverage
of all current/live search publications. It retains the boundary header and newer
suffix, publishes a monotonic GC floor, and persists a per-record delete cursor
before deleting payloads and headers. `max_deleted` bounds objects for vacuum
and transaction records for `wal_gc` (each record can remove multiple objects).
Compact acceptance receipts preserve old
batch-id idempotency proofs; legacy shared payloads are retained. Request receipts
and catalog outcome proofs are not garbage-collected in this implementation.

Vendor-specific CDC adapters beyond existing PostgreSQL, coordinated schema
evolution and notification subscription provisioning remain follow-on work.

The native formats follow the [Iceberg v2 specification](https://iceberg.apache.org/spec/)
and [Parquet format definitions](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift).
Independent Arrow and PyIceberg readers qualify the emitted artifacts and catalog
updates in the managed and REST HTTP tests.
