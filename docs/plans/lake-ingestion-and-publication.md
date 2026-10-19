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
cursor cuts are implemented for writable lakes. Archive and accepted-WAL vector
builders now share native multimodal templates and independent chunk enrichment,
including durable source-unit payloads. Mutable native tables retain readonly
physical generations for ordered and composed cursors; primary LSM and native
source-vector storage participate. The bounded capture, expiry, owner-location
and storage-provider contracts are documented in
[Composed query sources](composed-query-sources.md). Additional vendor subscription
provisioning and full-archive deployment/performance qualification remain separate
work.

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

Accepted native rows use the same canonical key identity as ingestion, including
numeric equivalence. Retained data records its encoding so legacy opaque row IDs
stay stable during replay. Final unit/chunk delivery resolves the exact declared
artifact for the winning stable-prefix run, even when several runs share an
index name.

Preserve the existing freshness contract: consume only proved index coverage;
scan uncovered data through a correct bounded fallback, wait/reject when the
requested visibility cannot be met, or serve an older snapshot only when the
request explicitly permits it. Never silently mix a new source with old indexes.

Expose durable acceptance, searchable coverage, lake-committed coverage, and
indexed serving coverage separately. Integrate these receipts/watermarks with
existing `sync_level` behavior; do not redefine current acknowledgements or
equate lake commitment with synchronous indexing.

## Storage durability, deployment, and cold starts

Native cursor capture publishes a durable discovery witness before the cursor
descriptor. The supervised maintenance owner can therefore collect expired
descriptors even when physical capture fails, the process restarts, or the table
is dropped. Descriptor creation and generation upload do not sweep retained
history inline; deletion availability and accumulated generations must not add
work to a foreground search. Local cut admission still enforces capacity and
protects active readers. Deployments must keep the background artifact collector
enabled, or run explicit maintenance when they disable automatic collection.

Lake search cursors issue a fresh small lease envelope per query, with its current
reader token and full configured retention period. Envelopes reference reusable
immutable state and at most sixteen row groups; unchanged groups survive WAL
updates without reserialization. Cached partition digests avoid walking the full
suffix on warm queries. Payload reuse requires expiry beyond the new envelope's
expiry, and the bounded retained-cut collector observes both expiry classes.
Restart loads exact provider-qualified state and row objects, using the shared
I/O scheduler with four concurrent reads and a bounded inline fallback. Combined
encoded state remains limited to 40 MiB and 65,536 changes.

The reusable state's retention horizon is the minimum across its own scope and
all referenced row groups. Extending a cursor renews aging groups and publishes
a new state; it cannot extend immutable objects by changing memo metadata.
Bounded in-process construction flights coalesce concurrent state and group
uploads. Waiters preserve their own cancellation, deadline and requested expiry;
a failed leader releases admission for another request to retry. Every fresh cut capture
still writes its own small lease envelope.

Pinned pagination shares decoded state and overlay maps through the existing
resource-accounted decoded cache. Keys include store, table incarnation and the
exact state reference, including provider generation and length. Each request
reads and validates its envelope, checks every transitive scope against its own
expiry, and independently acquires source and index reader leases. Cached data
holds no provider client or request capability. Cold restart retains the durable
four-read path; cache saturation preserves bounded query-owned leases.

Selective required metadata conjuncts can lead text serving-pack admission.
The planner consults native compound or column indexes, retains a physical
selection only up to 4,096 rows, and maps it through paged source/rank routes.
The complete predicate (including disjunctions and exclusions) remains a ranking
membership check. Recent accepted rows are evaluated separately. Broad or
unsupported predicates keep compressed membership and bounded text windows;
planning never scans Parquet merely to attempt this optimization. Global term
statistics, score order, highlight delivery and cursor semantics stay unchanged.

Archive file maps depend on publication and schema rather than the WAL head.
Recent vector planning borrows immutable prefix views and memoizes ready job
metadata only through its proved retention interval. Bounded, resource-accounted
memos hold copied identities and declarations; every query still validates source
coverage, permissions and reader authority. See the protocol-44 implementation
and qualification details in
[`NATIVE_LAKE_PERFORMANCE_FOLLOWUP.md`](../../zig/NATIVE_LAKE_PERFORMANCE_FOLLOWUP.md).

Filesystem object-store prefix listings descend only through the prefix's
complete path components. The final partial component remains a string filter,
and ordering, continuation tokens, delimiter grouping and symbolic-link behavior
retain the bucket-listing contract. Listing a cursor namespace must not traverse
unrelated archive artifacts.

Automatic Iceberg source opening treats publication navigation as optional until
it is adopted. Unavailable native stores or reader admission leave ordinary SQL
source scans usable; cancellation, allocation failure, ambiguous metadata outcomes
and explicitly required native reader errors still propagate. Retained/native
reads continue to validate their own authority. Once navigation is adopted, its
file lookups retain the independent publication reader pin.

Full scans over an authenticated immutable snapshot share a separate flat plan
keyed by the frozen metadata, requested snapshot and credential scope. The plan
owns its qualified version strings, manifest-chunk leases, file map and canonical
ordering. Queries borrow it through the bounded decoded cache and keep version
changes in private maps. A new metadata selection creates a new key; neither
snapshot labels nor TTLs establish freshness. Selective reads still defer data
manifests, and full scans build this view only on demand.

Selected Iceberg views share query-owned delete qualification under a 16 MiB
workspace. At most eight provider stats run concurrently on the shared scheduler;
only owned version strings and partition/sequence routes survive admission.
Native coverage validation and hydration use the same pinned evidence. Complete
partition tuples route candidate delete files, and sequence seeks exclude older
deletes before decoding. Incomplete scopes preserve authoritative applicability
checks. Only relevant recipes and decoded membership leases enter each prepared
view. New source admissions revalidate provider versions; publication retains its
independent fresh checks before and after construction. Workspace denial keeps
the bounded original path. None of this authority becomes a resident cache entry.

Ordered text batches share pattern leaves between membership prefiltering and
ordinary scoring. Prefix, wildcard, regex and fuzzy bitmaps are keyed by immutable
pack/segment and complete filter semantics. Four packs share the existing 8 MiB
query quota, with at most 32 pattern leaves per pack. Parallel scoring receives
private bitmap copies, checks current cancellation/deadlines and joins before
memo teardown. Cache denial or eviction recomputes authoritative membership;
BM25, Boolean boosts, deletions, totals, tie ordering and cursors retain shared
engine semantics.

Cold-query qualification must distinguish local execution, process restart,
empty Antfly serving caches, and fully remote reads. A restart with a retained
filesystem cache does not establish object-store cold latency. Compare prebuilt
BM25 indexes on the same corpus and machine before comparing remote Parquet
scans; an unranked substring scan is a separate baseline. Exclude index creation
and extension installation from query latency, preserve row-count/content
provenance, and report tokenizer, projection, highlighting, thread limits and
OS-cache differences. Do not benchmark during builds or compare translated x86
execution with native ARM execution as an engine speed comparison.

Seekable native text directory version 5 can carry a Snappy-compressed opening
bundle. Decoded navigation is capped at 1 MiB, authenticated against each original
range identity, and charged to the physical owner. Native bytes and page CRCs do
not change. Metadata pieces retain original pack ranges so denied residency can
fall back to required verified reads. Versions 1–4 remain readable; rebuilding
creates the new layout, while unchanged-file reuse can preserve an older layout.
Upgrade readers before publishing version-5 directories or cohort-bearing file
manifests; older binaries are not forward-compatible with these new records.

Per-file opening cohorts combine small directory and navigation objects while
preserving every member's original content-addressed identity. A cohort decodes
at most 4 MiB and contains at most 256 members; each member is checked against its
own SHA-256 proof before admission into the existing scoped immutable cache.
Cold physical admission warms only file groups with new segments, with a 16 MiB
decoded-work ceiling per admission. Memory denial or that ceiling leaves normal
required reads responsible for access. No credentials or query capabilities are
retained. Reader cancellation and current context validity are checked before
decoding and during admission. File manifests, incremental reuse validation and GC retain
both cohorts and their original artifacts. These are transport hints, not a new
commit authority, and do not require loading an archive-sized index locally.

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
publication. Cursors retain both the archive publication and accepted tail for the
configured cursor retention period (five minutes by default, at most one hour) in
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
limits are 16,384 physical rows and 32 MiB of output row images. Selection admits
up to 32 whole manifests independently of those per-turn limits. A durable cursor
records input file, row group and next physical row; immutable output pages and
CAS progress survive restart. Large input files can span many turns, including
turns that consume only deleted rows. Metadata manifests above 4 MiB remain
skipped, decoded pages remain bounded, and one job admits at most 65,536 output
files and a 32 MiB progress document. Final output manifests contain at most 128
files each. The pinned parent has a renewable 24-hour retirement lease. Oldest sequences are selected first. Untouched files
retain their sequence numbers. Equality-delete manifests are removed only when
all remaining data sequences prove those deletes obsolete; position deletes
remain unless the full data inventory is rewritten. Exact durable catalog intents
are replayed after interruptions, without rebasing ambiguous commits. The result
includes `complete`, `committed`, `conflicted`, `scanned_rows` and `output_files`.
Repeat the same operation ID until `complete`. A confirmed parent conflict marks
the job complete without publication; a fresh operation must read and rewrite a
fresh parent. Scheduler cycles use new operation IDs; obsolete images are never
transplanted into a newer parent.

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


## Follow-up execution priorities (October 9)

Implement native snapshot portability before public HN deployment. The repository
adapter and retained-owner restoration described in [composed query sources](composed-query-sources.md)
are under native and standalone qualification. The deployment must use durable
artifact storage whose lifecycle exceeds cursor/chunk retention, independently of
owner disks. Check restoration, cancellation, credential rotation, expiry cleanup,
admission pressure and cold publication before deploying.

The next source adapters are PostgreSQL CDC and S3/GCS notifications. Support both
attach and managed provisioning with persisted ownership and reconciliation;
notifications are wake-up hints, and authoritative catalog reconciliation repairs
loss or duplication. PostgreSQL transaction positions become checkpoints only
after durable Antfly acceptance. Provisioning must be idempotent and teardown
must never remove resources that Antfly only attached to.

Full-archive qualification is authorized in `antfly-dev-01` with a $150 incremental
spend ceiling. The October 9 public dataset metadata reports 49,999,000 raw rows
and 19,772,584,432 logical bytes. `examples/hackernews/export-full.sql` has a
21,474,836,480-byte billing limit; its dry run estimates 19,372,806,880 bytes.
The pinned October 9 export completed with 47,717,307 live story/comment rows,
334 Parquet objects and 21,195,813,507 compressed bytes. This establishes source
availability; full-archive index capacity and query latency still need qualification.
Use fresh export/artifact prefixes, bounded pod lifetimes and cleanup, regional
compute beside GCS, and record exact binary/source revisions. Qualify cold and
warm filtered/ranked queries, cancellation, restart and owner relocation,
archive overlays, pagination and concurrent pressure. A successful smaller sample
is not evidence of full-archive capacity. Query/export preprocessing costs, pod
resource time, GCS operations and temporary storage belong in the run ledger.

After engine qualification, finish rolling-year routing and migration, public HN
UI/deployment, scalable resumable maintenance, and the remaining composed query
classes and SQL modes. The immutable HN cohorts currently documented are not a
rolling-year production service. Aggregation/hierarchy/graph/analysis/stateful
composition and broader SQL acceptance must retain the same visibility and cursor
fences; do not replace exact archive semantics with a bounded candidate sample.


## Managed adapter reconciliation and qualification

The operator package `py/packages/lake-maintenance` now provides persisted source
definitions, conditional authority revisions, an unresolved operation journal,
provider ownership witnesses, progress/status, teardown and a bounded supervisor.
PostgreSQL native configuration uses table-incarnation and source-definition CAS;
its exported snapshot-to-stream cutover remains owned by the distributed executor.
S3 uses owned SQS queues plus an enforced notification-configuration writer policy.
GCS uses owned Pub/Sub topics/subscriptions and attributed bucket notifications.
Provider events are wakeups: native catalog/source reconciliation and durable
handoff precede acknowledgement; periodic polling repairs lost events.

The journal records references rather than credentials and never uses SQLite.
Unknown provider outcomes retain the exact intent across restart. Removed resources
must satisfy their ownership witness; existing operator resources cannot be adopted.
PostgreSQL teardown removes native configuration before taking the same advisory
cutover lock and deleting inactive physical resources. Standalone has no CDC
executor and rejects PostgreSQL managed setup.

See [operator setup](../../py/packages/lake-maintenance/README.md) for the CLI,
endpoints, cloud configuration contracts and real-provider qualification scope.

### Canonical and compacted native-text generations

Canonical per-source-file manifests remain authoritative for refresh/checkpoint
recovery. A publication may additionally reference a compacted serving root. Native
runs may span complete small files; each member records its original physical
manifest and native ordinal interval. Query admission derives physical row spans
from those members, preserving delete holes and original producer keys without
rewriting external Parquet. All global collection statistics remain canonical;
segment statistics and term envelopes describe the merged runs.

A refresh compares member identity/digest in immutable file order. Unchanged units
retain their artifacts. A replaced/removed member rebuilds only its bounded unit,
and fresh grouping stops at surviving unit boundaries to prevent a deletion from
shifting every subsequent batch. Metadata and term directories update through
copy-on-write deltas. Initial merges contain at most eight one-run source files,
target 16 MiB, cap output at 32 MiB, and use a 64 MiB workspace with exact original
run fallback on allocation denial. Larger files keep their existing run layout.

Canonical roots retain their serving root; GC also traverses member manifests and
hole bitmaps. Old serving generations stay immutable for retained cursors. Reader
protocol 36/native-text metadata 11 gates member-aware publication, while historical
metadata 9/10 stays readable. Best-first traversal consumes authenticated score
envelopes with strictly losing pruning, bounded frontier/read admission, equal-score
tie preservation and explicit lower-bound count semantics. Unsupported proofs and
exact counts keep the existing exact traversal.

Priority traversal admits up to eight candidates together to amortize schema,
statistics and native-reader setup. Newly raised cutoffs can leave at most seven
extra candidates in the already selected window; losing hits are still rejected
by exact ranking. Existing corpus/read/memory admission remains enforced and an
unaffordable optional batch falls back to exact physical windows. Deferred
highlights use the admitted snapshot's canonical segment identities, since
priority arrival order can differ from its native document ordinal order.


### Native retained payloads and inventory delta planning

Search cursor replay must preserve fresh-cut admission independently of optional
cache residency. Modern retained WAL groups use versioned typed native values
with preallocated containers; legacy JSON groups have a bounded private replay
workspace when shared decoding cannot admit them. Both use process-accounted
ownership, exact artifact generations, independent reader envelopes, and existing
retained-domain collection. Cache pressure must not invalidate an issued cut.

Broad indexed metadata predicates route compressed physical selections through
source rank/select and serving spans. The planner bounds metadata work and uses
full residual evaluation; an expensive or near-full proof falls back to bounded
text windows. Matching row count is not itself an admission limit for compressed
producers.

Publication inventories persist the exact provider identities used by native
file state. Eligible refreshes share one authenticated previous inventory and
seek only changed native file records, inheriting unchanged catalog roots.
Native roots persist proof that the prior inventory was delete-free.
Delete-bearing prior/current snapshots, partial checkpoint recovery, and legacy
roots or unqualified inventories retain complete reconciliation. Broad
replacement uses a bounded sequential catalog walk. Source inventory processing and
mandatory closure auditing remain separate costs; this optimization does not
claim constant-time snapshot discovery or waive retention qualification.


### Publication-bound file navigation and incremental delete proofs

Native search resolves public/private file identities through authenticated paged
navigation owned by the selected publication. Cold queries no longer build maps for
every source file. GC and cloud closure ownership include these navigation pages;
reader leases protect older publications independently of a refresh.

Refresh planning persists the same exact object selectors and per-file delete
fingerprints used to build native row identities. Authenticated subtree reductions
skip unchanged prior file proofs, including on snapshots containing Iceberg deletes.
Changed and removed files seek their native maintenance records, while incompatible
or historical metadata retains complete reconciliation. Current snapshot discovery
and fresh source coverage remain authoritative and may still enumerate files.

Accepted-data enrichment status describes the composed table cut, with its actual
WAL position, partition progress, and artifact-retention horizon. Reusable partition
jobs have separate identities and cannot individually claim table-wide readiness.
Retained cursor replay shares decoded immutable WAL groups across neighboring cuts,
with independent admission and conservative accounting for all pinned buffers.


### Compact coverage, bounded patterns, and recent-vector scheduling

Reader protocol 47 introduced the separation between the canonical data census
digest and qualified delete-object evidence; the current reader protocol is 49.
The publication owns a version-2 source-navigation root containing that digest and, for explicitly immutable sources, the exact
freshly discovered source-plan identity. Selection checks current binding,
credentials, schema, snapshot, file count, and reader authority before using this
proof. An identical immutable plan can reuse its census without decoding or mapping
the published inventory. Mutable sources and unmatched plans still qualify current
objects and recompute the complete census. Historical publications retain their
original signature preimage and validation path.

Reached Parquet files obtain their exact provider selectors through authenticated
point seeks in the publication's file tree. The source owns an independent native
store client and retained reader handle, so closing a short SQL index cursor cannot
invalidate later projected hydration. Conflicting fresh selectors fail validation;
selector caches carry no authorization. Delete-object versions remain fresh on
every selection unless the selected publication supplies exact version proofs
under an immutable source contract and an active catalog snapshot pin.

Prefix, wildcard, regexp, and fuzzy text queries participate in bounded routing
windows and shared Boolean preparation. Segment-local compressed membership feeds
the ordinary top-k collector instead of corpus-wide document-ID and score arrays.
Unknown pattern membership never prunes a segment. Mixed clauses retain global
BM25 statistics, constant-score boosts, deletions, filtering, and stable cursor ties.
Explicit sorts and aggregations keep their existing complete-input operators.

Recent dense and sparse vector partitions use independent mutable executions in
batches of at most three. Shared I/O scheduler admission remains held through result
joining, with a 16 MiB workspace per concurrent lane. Saturated work runs inline;
a partition exceeding its lane quota is retried serially under the existing request
allocation policy. Cancelable completion points precede joining; inline
cancellation immediately unwinds sibling work. All lanes join or cancel before
owners are released, and merges retain partition order and global result shaping. This improves overlap across
recent partitions without creating an operator-specific thread pool.

Cloud generation-map vacuum progress binds its cursor and completion state to a
versioned digest of the entire GC cut: retention domain, floor, cutoff, and mark-tree
root. Changed evidence or legacy progress without that digest restarts enumeration,
even when the numerical cutoff is unchanged. Prior reader-held map roots remain
immutable and independently retained.


Selective text patterns use publication-bound vocabulary and term-routing
summaries to admit serving packs before reading postings. Bounded complete
expansions are admission proofs; their terms never replace the pattern's scoring
semantics. Broad expansion or allocation pressure retains complete admission.
Cold Iceberg manifest plans use shared singleflight construction after fresh
catalog resolution, so concurrent readers share immutable data without sharing
request authority. Missing cloud generation-map pages are derived-artifact loss
and admit rebuilding; provider authorization failures remain operational errors.


Native serving refinements intersect complete indexed-predicate pack proofs with
the global score frontier, and use regex/fuzzy automata to skip impossible
vocabulary ranges before postings admission. Serving packs also publish paged,
authenticated minimum logical positions for safe equal-score pruning under the
default comparator. These pages participate in publication reuse and reader-safe
GC; existing publications acquire them during the next serving refresh.
Retained native WAL cuts pin independently cached row-group partition indexes
and borrow their decoded row values, so unchanged groups share indexes and key
filters across cuts without reconstructing the entire overlay. Legacy cuts keep
their existing compatibility path. Exact counts, typed tie ordering and recent
row visibility preserve their conservative execution contracts.

#### Query-local broad admission and immutable manifest reuse

Broad native metadata predicates stream compressed index pages through inverse
source routes into a quota-controlled temporary pack index. The text score
frontier intersects ranges with that index without retaining all matching rows
or pack IDs in memory. Broad patterns use bounded range-local membership proofs;
unknown proofs retain candidates. Row filtering and text scoring still establish
final correctness under the same publication and reader lease.

Fresh Iceberg metadata and manifest lists govern every new snapshot plan.
Immutable manifest Avro is reusable across snapshots under a credential/backend
scoped cache key containing its path and declared length. A separate manifest
construction coordinator prevents nested snapshot-cache admission deadlocks.
Sequence inheritance, schema and delete applicability are recomputed from the
fresh snapshot metadata/list. Mutable attachments do not use URI-only manifest
reuse. Inventory assembly remains linear in its entries.


#### Lazy physical admission and independent composed execution

Necessary metadata predicates with more than 4,096 estimated matches can lead
admission through physical-range probes instead of a complete pack census.
Reverse row-index pages optionally retain authenticated tuple minima/maxima;
new publications write these reductions and old pages keep a bounded leaf-walk
fallback. A text-competitive serving pack supplies its physical source ranges.
Only a completed negative proof excludes the pack. Unknown, memory pressure,
and exhausted optional read/work budgets retain it for final row filtering.
Tiny sets keep the existing complete compressed/spillable routing proof.
Range probes own a 1 MiB page cache and a 256 MiB cumulative read allowance,
independent of point membership's accounting. Their owner is bounded to 16 MiB.

Patterns compile one query-owned matcher per distinct clause. Broad patterns
visit the lexical field/prefix range of the existing authenticated term directory
and reject child pages whose pack envelopes cannot intersect the current hash
range. They do not rebuild a vocabulary union or compile an automaton for every
range. Bounded unsuccessful walks remain unknown, never evidence of absence.
No new publication root or provider authority is introduced.

Exact BM25 score ceilings evaluate the supported integer frequency domain
(up to 256) under the scorer's actual floating-point arithmetic at authenticated
norm minima. The same proof covers scored phrase frequency using its first
term and combined phrase IDF. Large/escaped domains retain conservative bounds.
A bounded query cache reuses ceilings across range probes. Boolean bounds include
the canonical baseline before child accumulation and boost multiplication.

Immutable Iceberg data manifests also own reusable derived file chunks. Keys
bind credential/backend scope, format version and manifest-list inheritance
inputs. Fresh snapshots merge sorted chunk views, own their snapshot-qualified
version strings, and pin borrowed paths, partitions and metrics through explicit
leases. Manifest/chunk construction uses separate cache admission from snapshot
construction and bounded shared scheduling. Deletes still use the freshly
selected schema and manifest list. The flat file view and snapshot-qualified
canonical row ordering remain proportional to the inventory; this design does
not claim constant-time discovery of an arbitrarily large new snapshot.

Native composed text/dense/sparse components run under independent executions,
source version maps, membership cursors, allocators and runtime pins. At most
three lanes participate in a pass using the shared scheduler; saturation runs
on the caller, cancellation drains pending lanes, and a lane that exhausts its
16 MiB workspace retries serially under the ordinary request allocation policy.
Owned results return in declaration order. Fusion, graph dependencies,
highlights, transforms and final source delivery remain coordinator-owned.

Nested archive/recent vector partitions execute serially inside an already
scheduled composed lane, using that lane's allocator and workspace quota.
Standalone vector queries retain archive/recent overlap. This prevents scheduler
saturation from multiplying unadmitted nested caller workspaces. Regressions
exercise nested execution, cancellation, and enclosing-quota exhaustion.

Reverse-index tuple envelopes fit the existing 256-byte authenticated page
reduction. Long lower keys use conservative prefixes; long upper keys use
prefix successors. A saturated upper prefix or legacy child emits a nonempty
unknown reduction, preserving publication compatibility and retaining candidate
packs. Exact row membership remains authoritative after this lossy pruning.


Legacy compacted text-root pointers carry no version. Serving and resumable GC
authenticate the root body before choosing the supported decoder; roots with an
advertised metadata version continue to require an exact match. GC persists a
version-discovery job so resumed traversal follows the same contract.

Composed execution lanes borrow their quiescent parent's immutable version map
and keep only private overrides. The parent remains pinned until every lane
joins. Workspace exhaustion during lane setup uses the same serial fallback as
execution exhaustion; cancellation and real allocation failures still propagate.

Immutable Iceberg snapshot catalogs use refcounted, path-copying file-ID trees
that pin immutable manifest chunks. A bounded predecessor cache scopes reuse to
credentials, source, table location and manifest inheritance. A new snapshot
reuses unchanged nodes and derives only changed manifest entries. Headers, schema
and delete applicability remain authoritative. Point hydration creates a bounded
view of the selected files. Publication-backed scans enumerate canonical public
file records through bounded independent cursors; legacy navigation and explicit
continuation validation retain flat inventory planning. For those consumers,
canonical ordering is published once per snapshot with cancellation-aware waiting.
Cache admission failure preserves the ordinary statement-owned materialized plan.
Initial discovery still reads the manifest list; this does not promise constant-time discovery of a new snapshot.

Constant-score match-all and bitmap leaves expose finite nonnegative score
bounds within selective Boolean trees. Invalid or signed boosts retain
conservative bounds. Independent full-admission ranked pages remain the oracle
for score, tie and offset behavior.


Cold snapshot catalogs merge the sorted manifest chunks and bulk-build their
Cartesian tree. Each file allocates one node and hashes its priority once;
construction takes O(N log M) merge work for N files across M chunks and O(N)
tree work. Incremental snapshots retain path copying. Catalog construction and
lazy canonical ordering retry allocation after evicting cache pins, while reader
pins preserve ownership. Exhausting reclaimable entries uses the existing flat
planning fallback rather than making cache residency a prerequisite for queries.

Delete-file decoding has a separate immutable cache shared across selected data
views. Its identity binds fresh provider ETag/version, complete delete recipe,
schema, credential scope and decoding/application policy. Objects without a
strong provider identity bypass it. Selected views apply sequence/partition
rules and row bounds independently and retain leases for shared compressed
position membership and equality keys. Loading is bounded to 64 MiB, each prepared
view retains at most 256 MiB of foreign indexes, and residency includes leased
bytes. Oversized indexes use streaming subset decoding with direct path lookup.
Fresh object validation remains mandatory before cache lookup unless an adopted
publication supplies exact provider versions for a pinned immutable catalog
snapshot. That proof binds the fresh metadata plan, complete delete recipe,
coverage and credential/store/namespace scope; reached delete files still use
version-qualified reads. Unversioned, mutable and unpinned sources retain HEAD
qualification.

Cold text-frontier traversal may warm sibling term-directory pages on two
shared scheduler lanes. Each lane owns a 2 MiB workspace and 1 MiB read allowance;
a query spends at most 8 MiB on optional warming, separately from required
navigation. Scheduler saturation skips warming. Workers publish only immutable
artifact bytes; the coordinator still authenticates and reduces metadata with
private scoring/traversal state. Cancellation drains workers before request
state is released. Benchmark component durations measure each worker's actual
execution, including membership preparation, rather than result transfer;
parallel durations can overlap and are not additive wall-clock time.


Publication file navigation can now open immutable Iceberg sources before data
manifest discovery. The source still resolves the fresh metadata location,
selected snapshot and schema. An authenticated publication root must match the
credential-scoped metadata-plan digest, table/store/namespace domain, source,
snapshot and schema. The existing digest variants for current and explicitly
pinned selectors can authenticate the same selected snapshot; the wire format
is unchanged. Missing or mismatched proof falls back to ordinary discovery.
Manifest lists and delete manifests remain fresh. Sparse hydration seeks only
selected file records, with independent native reader authority and fully owned
file fields. Ordinary full scans enumerate the authenticated public file tree in
canonical digest order, holding one owned file view at a time. Independent cursors
over disjoint digest ranges preserve shared scheduling and ordered parallel
scans. Legacy navigation without totals, explicit row selections and continuation
validation retain the frozen metadata planner. Shared flat-plan cache admission
is optional; overflow plans use the statement allocator and pin bounded manifest
chunks rather than adding a 64 MiB SQL limit.

Shared position-delete indexes keep 64-bit ordinals in high-word Roaring blocks.
A selected view borrows a single immutable set; overlapping delete files merge
compressed blocks only when necessary. Decoding computes the canonical sorted
fingerprint prefix once, preserving existing publication identities without
re-expanding every selected view into a row hash map. Row bounds, sequence and
partition applicability, scanned-row and retained-byte limits still apply.

Ordered text pagination prepares analysis and global statistics once and reuses
query-local membership across physical candidate batches. Four immutable pack
bitmaps share an 8 MiB budget and LRU eviction. The memo retains no source pins,
request callbacks or mutable scorer state. Allocation pressure returns to the
ordinary candidate path; cancellation still propagates. Distributed statistics
retain their existing preparation path. Exact scoring and predicates remain
authoritative after the optional membership prefilter.

Cloud generation navigation reclaims idle credential-scope cache owners when
either a new owner or an existing owner's decoded pages need shared-budget
headroom. Only cache pins are reclaimed; active reader pins retain their owners.
With no reclaimable owner, optional navigation caching keeps its uncached or
query-local fallback rather than requiring shared cache admission.


Fallible decoded-cache constructors now complete before installing the payload
union tag. Missing manifest or allocation failures leave the prior empty payload
valid for cleanup; fully constructed resources retain normal destruction if a
later preparation step fails. Regression coverage retries a missing-manifest
load and verifies surviving shared snapshot leases. Synonym membership also
propagates allocation failure instead of silently dropping expansion matches,
allowing the optional ordered bitmap memo to fall back without false negatives.


Publication navigation binds read callbacks to its own retained native handle,
not the source opener's temporary handle. Native search shares its existing
reader lease; SQL acquires a lease only when it has none. Navigation authority
therefore survives source-open return without adding a second search lease.
The cold-restart test exercises this boundary during final source hydration.


Residual predicates resolve a bounded source view directly from the physical
selection's file identities before assigning scan-local file ordinals. This
works with both persistent catalogs and publication navigation, without expanding
selected row coordinates or discovering unrelated data manifests. Full scans
retain credential-scoped immutable parsed-manifest reuse under fresh selection.
The cold E2E includes ordered indexed-range plus unindexed-prefix filtering while
data manifests remain unavailable.


Final qualification with origin/main `e32171bfb1`: the Debug CLI built; all
485 lake integration tests passed (180 shared-engine and 305 server), all 81
focused tests passed (19 and 62), and all 47 real Parquet/Iceberg E2Es passed in
313.34 seconds. The strengthened cold restart E2E covers native text, retained
text, SQL point lookup, ordered indexed-range/unindexed-prefix residual filtering
with data manifests unavailable, and ordinary full scans after restoration.
Pinned-compiler error metadata, Zig/Python formatting and lint, whitespace, all
109 changed source license headers, and the Apache boundary passed (2,064 source
files). Live-provider retention qualification and representative archive-scale
latency/RSS benchmarks remain deferred; no archive speedup multiplier is claimed.


### Paged ordered dictionaries and immutable membership pins

Protocol 48 publishes ordered roots with independently authenticated slot, name
and public-digest dictionaries. Roots contain tree references and counts instead
of archive-wide file arrays. Serving resolves reached file names/slots through a
bounded page cache; public-ID ties prepare digests only for participating files.
Maintenance explicitly materializes slot fingerprints under its existing build
budget. Dictionary updates copy changed paths and preserve unchanged trees;
reader-safe GC and closure audits retain all dictionary pages. Version 5/6 roots
remain readable by the new reader, while new serving/checkpoint roots use 7/8.
The historical 16,384-file admission limit is replaced by the existing u32 physical
coordinate domain and actual resource admission.

Pattern membership exposes immutable, query-owned pins for scorer leaves.
Segment-local iterators avoid both full bitmap cloning and global-offset copies
on cache hits. Pinned slots cannot be evicted; each builder releases its pins
before the query memo closes. Mutable filter APIs still return private copies,
and exhausted optional cache admission retains authoritative execution.

Delete candidate routing unions each partition's sequence suffix once, using its
minimum data sequence; global and unknown-scope lists are also visited once.
Publication-bound delete version pages are available only with an immutable
source contract, an active catalog snapshot pin and exact version-capable
provider selectors. The query builds routing metadata without per-delete HEADs,
and loads version records only for applicable delete files. Publication still
checks fresh providers before and after construction. Recipe mismatch, deadline,
credential changes and lost reader/snapshot authority remain errors; absent
proofs and exhausted optional routing admission use fresh qualification.


### Shared public ordering and bounded publication scan planning

Reader protocol 49 publishes ordered serving/checkpoint roots as versions 9/10.
Versions 5/6 and 7/8 remain readable for migration and reader-safe collection.
Every ordered index borrows the publication's authenticated public-digest-to-file
name directory. Each index keeps its own stable file slots, including retired
slots; a name lookup translates shared navigation to that index's coordinates.
The live public-file count is independent of its historical slot count. Snapshot
changes replace the shared reference without rewriting unchanged slot/name trees.
Fresh slot trees are bulk-built in slot order; hashed dictionaries use bounded
512 KiB sort runs with a four-run merge limit. Incremental edits still copy only
changed paths.

A sequential prefix of at most 65 file records identifies wide groups; small
groups pay no separate cardinality seek. For large or sufficiently dense
logical-key ties, public ordering walks that shared directory in digest order and probes exact tie membership. A small LIMIT
can stop before reading every participating file. Continuation seeks start at
the boundary digest and preserve physical row coordinates, ascending/descending
IDs, backward pages and transitions between tuples. Small or sparse ties retain
local ordering and its scoped cache; wide dense groups use lazy intersection, while sparse wide groups use
bounded participant spilling as described below. Counts choose execution strategy only;
exact membership and native rows decide results. Spill-admission fallback may still
require many directory probes, so representative archive benchmarks remain
necessary.

Publication scans can freeze at most eight file descriptors into a private
statement view, with an independent iterator preserving serial fallback after
setup failure. Unordered scans then use the existing shared row-group work queue;
single-file ordered scans split row groups into contiguous canonical ranges.
The original multi-file scan path used lazy contiguous file ranges. The rolling
row-group scheduler below replaces fixed file lanes for large publication scans
while retaining deferred descriptor/footer errors and bounded file views. Startup warming
loads compact ordered roots and selected dictionary pages under the existing
page/time budget, avoiding full dictionary materialization.

Ordinary and resumable GC now use one source-navigation dependency walker,
including delete-version proof pages. An actual bounded-sweep regression pins an
old publication, replaces its proof, verifies the page survives collection, then
releases the last reader and verifies retirement. Other regressions cover shared
dictionaries with distinct slot histories, bulk write counts, bounded sparse-tie
prefix work and continuation boundaries, small-scan allocation failures, and
real cold Iceberg scans with one/two files and many Parquet row groups.


Qualification with origin/main `f9f1582346` included: the Debug CLI built;
495 integration tests (184 shared-engine + 311 server) and 90 focused tests
(22 + 68) passed without failures or leaks. All 48 real Parquet/Iceberg E2Es
passed in 636.58 seconds. The final ordered-scan refinement passed its native
allocation/row-group regression and four ordered/cold-Iceberg E2Es against the
rebuilt CLI (28.09 seconds). Compiler-derived error metadata, formatting/lint,
source licenses, whitespace and the Apache boundary (2,065 files) passed.
Live-provider retention qualification and representative archive latency/RSS
benchmarks remain deferred; these E2E timings are functional qualification.


## Dictionary-bound pagination and rolling row-group scheduling

The public tie cache binds its slot order to the authenticated ordered-file
mapping as well as the tuple tree, publication source/snapshot and authorization
scope. Two indexes with equivalent definitions can retain different stable slot
histories, so the tuple-tree identity alone cannot authorize reuse of cached
slot numbers. Regression coverage warms one mapping and queries the other in
both directions with continuation boundaries.

Ordered data-run ancestry and dictionary ancestry are independent. Within a
checkpoint sequence, the first root establishes the complete slot/name layout;
subsequent roots authenticate and edit only the reached fingerprint slots.
Fresh data runs inherit the dictionary independently of their merge forest,
and checkpoint assembly reuses that dictionary directly. Maintenance reloads
compact roots instead of materializing/sorting all file names after each file.
The first incremental merge retains the previous layout's length when newly
admitted files extend the census. Final assembly streams the row runs normally.

Public ordering chooses between snapshot-directory intersection for dense ties
and a participant-only external sort for sparse wide ties. Participant sorting
uses 128 KiB runs and the existing bounded spill manager, resolves only group
members, and orders digest/slot keys in the requested direction. Continuations
filter digest bounds before sorting and preserve exact physical row-coordinate
seeks at an equal boundary. This bounds memory and avoids unrelated archive
membership probes; it trades up-front participant work for predictable sparse
group traversal. Small pages keep the lazy prefix when it is cheaper. Continued
traversal switches to a spilled unvisited suffix once probes exceed participant
preparation cost or the reserved metadata-read budget, finishing any current
file before switching. Spill admission
failure retains lazy traversal. A native fixture exceeds 65,000 files and 4,096 participants,
forces spilling, and checks full order plus forward/backward continuations.

Large publication scans use a rolling, query-owned row-group scheduler.
Unordered lanes share a window of at most four footer plans and claim individual
row groups by compressed size; tiny files overlap and a large file can occupy
every worker. The next window is planned after current ranges have finished and
released their borrowed plans. An independent producer preserves the unopened
parent for serial spilling fallback, and reducers release active claims on early
exit. Physical row admission belongs to the statement and survives worker
ranges, rolling windows and fallback. Waiting lanes retain immutable statement
context and I/O, independently of the producer's changing file view. Ordered
SQL delivery consumes contiguous row-group ranges as a bounded batch, joins and closes the batch, then advances to the next file.
Later descriptor/footer errors cannot displace an already delivered prefix.
Scheduler saturation executes required ordered ranges on the caller rather
than discarding an already advanced batch. The existing small-inventory path
continues to support its fixed immutable work plan.

These changes do not claim an archive-scale speedup factor. Representative
cold/warm latency, publication request counts, RSS and skewed-file throughput
measurements remain qualification work; real-provider retention qualification
remains deferred.


Validation with origin/main `f9f1582346` included: the Debug CLI built;
497 lake integration tests (185 shared-engine + 312 server) and 15 focused SQL
contracts passed without failures or leaks. The final adaptive traversal passed
four focused cache/tie regressions, including the 65,553-file fixture, small-page
prefixes, bounded promotion, spill rejection, backward cursors and a following
tuple. All 50 real Parquet/Iceberg E2Es passed in 621.84 seconds. The new Parquet
case streams 20,288 rows over pgwire in 137-row delivery batches across cold
restarts and checks filtered/grouped aggregates; cold Iceberg coverage now also
includes ten independently written files. Compiler error metadata, lint,
whitespace, source licenses and the Apache boundary passed. These timings are
functional qualification rather than archive latency benchmarks.


### Rolling scan work and ordered maintenance

Publication scans retain bounded file windows and share individual row-group
claims. Sealed dynamic filters travel with every fixed or rolling worker; their
physical min/max bounds only prune, while exact membership remains authoritative
and includes fields hidden from the result projection. Idle consumers park on
an I/O condition and continue observing request cancellation and deadlines.

One optional independently owned cursor warms parsed Parquet footers for the
next window under shared I/O admission and a 16 MiB private workspace. Required
navigation advances only on demand. Warming cannot surface a later file's
metadata error before an earlier delivered prefix, and statement close joins
all warming before releasing navigation/source authority. Denied admission
uses the ordinary required read path. Ordered batches use statement-backed
caller workspace when concurrent scheduling is saturated; their batch sizing
follows that workspace. Terminal worker EOF does not consume SQL scan quota.

Ordered index updates keep dictionary ancestry and data-run ancestry separate,
and carry explicit retirement slots as well as the streaming merge's keep mask.
The initial checkpoint handles the snapshot delta; subsequent file checkpoints
authenticate and edit only that file's dictionary/retirement paths. This preserves
stable slot histories and old-reader roots without repeated archive-sized mask
walks during sparse incremental publication.


### Overlapping windows and direct metadata reduction

Unordered SQL scans keep at most two independently owned windows of four files.
An idle consumer can plan and consume later work while an earlier claim retains
its footer and borrowed columns. The producer keeps one monotonic enumeration
cursor; every window pins its own plans until its last claim closes. Planning
errors are sticky across consumers. Aggregate worker EOF is administrative and
spends no logical scan page; nonterminal empty pages remain quota-bounded.

Reducers select fan-out from useful workspace and shared CPU admission, reserve
node I/O headroom, and retain local state in explicit worker allocator budgets.
One caller lane and admitted siblings share the operator's bounded allowance.
Local spill/fallback preserves the original pinned source and exact reducers.

Publication-wide text navigation computes sibling score bounds concurrently
with independent, bounded navigation/pattern caches. Only completed reductions
and accounting cross back to the coordinator; successful bounds avoid a second
serial traversal. Prepared syntax remains immutable, and spilled selection
clipping, ranking, errors, and read-budget reconciliation retain one owner.
Worker memory/admission failures use the required serial path. Cloud retention
qualification on real providers remains deferred.


### Logical scan credits and typed final aggregation

Native aggregates charge positive rows in statement-sized logical batches,
independently of worker memory, row-group/file fragmentation and output delivery.
Empty nonterminal pulls spend a credit; terminal EOF does not. Serial and parallel
reducers share this policy and continue enforcing the statement row limit.

Integer, boolean and text MIN/MAX participate in exact parallel reduction.
Fixed-width final group states merge by column in admitted batches of up to 64,
with one packed key matrix and no scalar reducer snapshots. Exact NUMERIC and
variable-width winners merge directly after per-group memory admission. Spilling
keeps the durable partial format and bounded reusable per-partial scratch.
Small nested spill reducers size hash partition fan-out from workspace, leaving
room for merge runs within the existing shared file ceiling. Sorted skew
fallbacks place logical runs in the existing reusable anonymous extent store,
retaining one descriptor per sort.
Floating/dynamic extrema retain source-order execution until representative ties
have a defined parallel contract. Ordered producers reserve shared I/O workspace
using the same scheduling policy as aggregates, retaining bounded caller fallback.


## GC expansion upgrades and final serving refinements

Artifact marking and dependency traversal are separate. Ordinary GC planning and
cloud publication audits retain a bounded expansion bitset beside each marked
reference. A maintenance-only canonical manifest can later be expanded for an
older reader-held serving publication, including seekable directory children.
Full expansion subsumes maintenance expansion; visiting a raw reference cannot
suppress its later traversal. Resumable collection keeps its durable expansion
keys, including a separate seekable-manifest key. Failed or incomplete traversals
never authorize a sweep.

Parquet cursors load the embedded OffsetIndex for projected chunks even without a
filter. Unfiltered scans omit the ColumnIndex bounds read. For indexed data pages,
required reads and rolling prefetch use the exact header-inclusive compressed
range. The encoded header must agree with the index length, page type, and row
count before decoding. Range and input admission precede the read. Dictionary
preludes and chunks without an OffsetIndex retain bounded header discovery.
Optional indexes that exceed navigation admission use the ordinary bounded
header path; malformed admitted indexes still fail validation.
This removes a header-probe GET for cold indexed pages larger than the probe;
small pages already fit the existing probe. The page directory remains versioned,
leased, and bounded by each consumer's allocation policy.

Exact parallel aggregation partitions in-memory partial groups by their existing
semantic key hash once there are at least 4,096 partial groups. Each admitted
reducer exclusively owns its destination hash table and merges typed state through
selection vectors. A stable owner retains reducer budgets until result shaping
consumes their batches. Final batches go directly to the existing result collector,
without a serial hash-table rebuild. Source ordinals preserve implicit ordering;
global group limits apply across all reducers. Every task joins before scratch or
source release. Low-cardinality, spilled-source, and declined-admission cases use
the existing bounded serial/spilling bridge. Reducers themselves may spill when
an owner is skewed. Floating or order-sensitive aggregate states retain their
established eligibility restrictions.

Posting scorers share query-owned immutable regex programs and wildcard navigation
plans across segments and remote routing windows. Regex execution forks private
DFA state; provider I/O never holds the compilation-cache mutex. The cache admits
at most 16 programs within 2 MiB, owns syntax bytes, and declines oversized/full
entries to normal per-consumer compilation. Deadlines and cancellation remain
authoritative. Compilation and DFA allocation failures propagate instead of
turning a valid regex into an empty result; DFA columns publish states only after
all allocations succeed. Fuzzy matching still constructs its inexpensive term
wrapper with independent execution state.

Regressions exercise both manifest traversal orders, already-marked references,
one provider GET per indexed data page, dictionary preludes, inconsistent index
lengths/ordinals, concurrent program reuse, allocation-failure cleanup, hash-owned
state lifetime, overlapping groups, winning text extrema, ordinals, and the global
group limit. The real Parquet cold-restart fixture now writes embedded page indexes
and checks a 2,048-group aggregate alongside ordered result delivery. Provider
retention qualification and representative archive latency/RSS measurements remain
deferred; request-count regression evidence is not an archive-scale speedup claim.


Qualification (2026-10-16): all 501 full native lake tests (187 shared-engine and
314 server), 24 focused SQL regressions, and 31 runtime ABI tests passed without
native failures or leaks. Final reducer sealing and four-column lookahead were
rechecked with four focused native tests and six typed SQL owner tests. All 50
real Parquet/Iceberg E2Es passed together in 639.93 seconds, with production disk
safeguards enabled. The final Debug CLI, source/license boundaries, generated
error parity, formatting, and lint checks passed. Latest fetched `origin/main`
`9dd098d773` is included. The final CLI also passed the indexed Parquet
cold-restart/grouping flow again (12.08 seconds) after the last guards.


### Decoded-page hits, global spill exchange, and physical aggregate kernels

Indexed decoded-page entries now retain validated Parquet header evidence under
an immutable object-version/range/interpretation key. Readers consult that entry
before encoded I/O or speculative prefetch and reapply their own input, decoded,
structure, row-count, directory, and cancellation checks. Dictionary dependencies
remain pinned; dictionary preludes and unindexed files retain bounded header
discovery. With encoded residency disabled, warm scans decode no pages and fetch
no indexed data-page payloads, including when asynchronous lookahead is enabled.

Final exact aggregation can transfer unreduced worker spill partition files to
global hash owners. The existing native block codec and AGS checkpoints cross the
exchange unchanged; in-memory workers export a compact checkpoint once. This
removes the serial worker-finalization and partial re-encoding bridge for compatible
fresh spill streams. All files are sealed and destination capacity is reserved
before ownership moves. After the first transfer, adoption is infallible. Source
ordinal bases, semantic key ownership, wide integer cancellation, shared scheduler
admission, disk quotas, cancellation, and sorted skew fallback are preserved.
Already consumed or incompatible partition layouts retain the established bridge.

Identity SQL column expressions now retain borrowed native vectors through encoded
cohort evaluation, projection, and selection. Aggregate kernels dispatch on the
state/vector pair and consume selections, null masks, and dictionary IDs directly.
COUNT, integer SUM/AVG, floating SUM/AVG, boolean reductions, and primitive MIN/MAX avoid
per-row Datum construction. Global unfiltered aggregation uses this path without
per-row hash probes. Floating transitions preserve source update order, compensated
sums, SQL NaN ordering, and first-occurrence signed-zero ties. Only winning text
extrema are copied. Coercions and representations without a proven physical view,
exact NUMERIC, DISTINCT, pattern sets, and filtered expression cohorts preserve
their established typed/scalar evaluation and admission paths. Temporary composed
selections are scoped to the update; retained aggregate state owns its payloads.
Computed dictionary expressions retain their existing fused dictionary kernels.

Regex and fuzzy posting traversals lease exclusive mutable DFA state from the
query-owned program cache. Up to four executions per program can retain tables
between segments; each execution has at most 256 KiB within the shared 2 MiB
query cache. Traversal and provider I/O never hold the cache mutex. Saturation uses
private execution, and cache-budget exhaustion retries private execution without
changing matching results or execution statistics. Actual allocation failures
propagate. Regex and fuzzy DFA columns publish states only after all allocations
succeed. Query teardown requires all execution leases to be returned.

Regressions cover decoded-only warm reads and stricter consumers, concurrent and
sequential pattern state reuse, saturated and exhausted execution pools, mixed
memory/disk global reducers, transfer without additional writes, ordinal offsets,
wide integer cancellation, allocation-failure cleanup, physical identity expression
ownership, selection/null/dictionary kernels, zero-allocation primitive updates,
winning text ownership, NaN ordering, and global aggregation without hash probes.
Representative archive latency/RSS benchmarks and live-provider retention
qualification remain deferred.


Qualification (2026-10-16, after merging `origin/main` `b79c65538f`): all 502
full native lake tests (188 shared-engine and 314 server), 29 focused SQL tests,
and 31 runtime ABI tests passed with zero native failures or leaks. All 50 real
Parquet/Iceberg E2Es passed against the final Debug CLI in 626.48 seconds with
production disk safeguards enabled. Generated error parity, Zig formatting,
whitespace, licenses, and the Apache boundary (2,066 source files) passed. The
private error-name table includes main's new promotion-writer fencing error.


### Bounded global state, typed expression delivery, and adjacent page envelopes

Global aggregation admits one group independently of input batch length. Fixed
primitive reducers update that state in windows of at most 256 lanes; variable
MIN/MAX admission reserves the largest possible winner. Physical descriptors are
normalized once per batch and reused for admission, semantic key hashes, exact
typed key lookup, and aggregate updates. The small-budget path avoids per-row
hash probes without bypassing exact NUMERIC, DISTINCT, pattern, spill, or error
semantics.

Computed primitive expressions deliver owned typed payloads and separate null
states through the existing batch reader contract. Their instruction DAG retains
shared subexpressions and uses shared CPU scheduling for sufficiently large
cohorts; joined output copies primitive lanes without a Datum delivery matrix.
INT16/INT32 validation applies only to selected lanes. REAL inputs and each REAL
instruction retain scalar rounding, overflow, and underflow behavior. Borrowed
retained columns expose native payload/dictionary views; portable spill blocks
decode numeric wire lanes once into bounded typed buffers, preserving borrowed
text and dictionary entries. Complex values retain their exact fallback.

Aggregate FILTER expressions form lazy cohorts. Only surviving physical lanes
evaluate aggregate inputs. Sparse cohort positions gather group IDs directly
into bounded native updates; scalar fallback observes SQL NULL elsewhere.
Keys resolve once, unrelated unfiltered inputs stay native, and
filtered-out division/coercion errors remain unevaluated. Ordered and DISTINCT
consumers receive the same logical rows and null semantics as before. Compact
computed dictionary entries retain their existing encoded dictionary path.

Failed regex/fuzzy executions are removed from the query-owned pool on lease
return, including failures caused by the parent budget rather than the child.
Their tables are reclaimed before a private retry starts; the first attempt's
reader and iterator also close before retry. Later traversals can build and reuse
a healthy bounded execution when headroom becomes available. Actual allocation
failures still propagate, and cache pressure does not change matches/statistics.

Parquet prefetch merges only adjacent explicitly planned cold ranges, with zero
gap and a 2 MiB envelope target. At most four columns plan the current page plus
one adjacent indexed page each. Filtered/pruning consumers defer future payloads,
pruned current pages are not prefetched, and decoded hits skip encoded fetches.
Each exact page cache entry pins a slice of the shared immutable response; its
cache key retains source version and credential scope. Workers check the
persistent tier before coalescing only missing adjacent ranges. Cache admission
charges shared backing once until its final resident alias leaves; pinned aliases
remain resident and charged, while entry limits independently bound metadata.
No cache lock covers disk or provider I/O. Member flights let required
reads wait for an in-flight envelope, cancellation/close joins every owner, and
shared admission can fall back to smaller individual page jobs. Dictionary and
unindexed header discovery retain their bounded path. The real Parquet restart
fixture now also checks mixed computed FILTER cohorts and wide global counts.

These are bounded execution improvements, not an archive-scale latency claim.
Representative latency/RSS benchmarking and live cloud retention qualification
remain deferred.


Qualification at `512f16a3dd` after merging `origin/main` `ce578d9033` (2026-10-16): all 503
full native lake tests (189 shared-engine and 314 server), 43 focused SQL
tests, and 31 runtime ABI tests passed with zero native failures or leaks. All
50 real Parquet/Iceberg E2Es passed against the final merged Debug CLI in
632.59 seconds with production disk safeguards enabled. The expanded Parquet
restart fixture also passed separately. Merge coverage includes compact spill
arrays, native parameter ownership, and leased array projections. Generated error
parity, Zig/Python formatting, Python lint, whitespace, license headers, and the
Apache product boundary (2,073 source files) passed. The private error table
includes the combined branch/main names; public runtime ABI values remain pinned.

Native execution also carries primitive/dictionary projections across derived
relations, pushes selections to producers before coercion, and updates aggregate
FILTER cohorts using only accepted lanes. Group admission uses a bounded census
of distinct semantic keys and dictionary-tuple reuse, retaining exact equality,
source ordinals and existing spill fallback. These are internal performance and
correctness refinements with no new configuration or SQL syntax.

Qualification of the distinct-admission and typed-delivery refinements
(2026-10-16): all 505 full native lake tests (191 shared-engine and 314 server),
54 focused SQL tests and 31 runtime ABI tests passed with zero native failures or
leaks. All 50 real Parquet/Iceberg E2Es passed against the final Debug CLI in
618.60 seconds. The expanded Parquet restart case covers derived native
projections feeding grouped FILTER aggregates. Selection/coercion regression,
persistent-tier prefetch, shared-buffer pin/eviction accounting, distinct-key
admission under a small quota, and lazy/error-prefix/allocation-failure behavior
are covered. Zig/Python formatting, Python lint, whitespace, licenses and the
Apache product boundary (2,073 source files) passed. Current `origin/main`
`ce578d9033` is included. Archive-scale latency/RSS benchmarking and live-provider
retention qualification remain deferred.

### Mixed projection delivery and pressure-aware grouping

Derived streaming queries propagate output demand before physical scan planning.
Unused expressions and their cold scan fields are omitted while schema widths
and ordinals remain stable. Predicate and named-window input dependencies are
retained, including scalar-subquery cardinality in unused definitions; ordered,
aggregate, window, recursive, set and materialized domains conservatively retain
the positional inputs they need. External projection providers retain their
existing invocation contract. This is internal planning, with no new SQL syntax.

A derived output batch may combine native primitives, compact dictionaries and
exact scalar arrays/NUMERIC/JSON-null columns. Speculative encoded evaluation
owns a separate reclaimable arena: an expression error releases that workspace
before evaluating the existing scalar error prefix. Unsupported scalar columns
are evaluated once alongside eligible native outputs. The mixed 49-column
regression now succeeds under its existing 448 KiB working-memory limit.

Expression kernels gather validated primitive/dictionary payloads from mapped,
selected and physical-only readers directly into typed kernel lanes. Selection
is pushed before coercion; SQL null masks, narrow integer checks, REAL rounding,
nonfinite rules and scalar JSON-null/exact-NUMERIC fallbacks remain authoritative.
Identity expressions borrow exact column views as well, avoiding full-row scratch
when an aggregate consumes an array or NUMERIC field.

Grouping admits distinct new state, variable-width winner growth, replacement
vector/hash capacities and whole-column mixed-extrema/key promotions. Shared
statement headroom participates in admission. Existing fixed-width groups remain
batched above half quota; oversized censuses
or reservations split into smaller prefixes instead of deoptimizing the entire
input. Partitioned spill ingestion reuses semantic hashes, scatters bounded
256-row windows and writes the existing native column-block codec, preserving
per-partition input order, raw/partial markers and source ordinals. Order-sensitive
and specialized exact aggregate paths retain their existing fallback contracts.

Qualification of mixed projections, capacity-aware group admission and physical
expression gathers (2026-10-16): the full SQL gate passed 990 tests, with three
opt-in benchmarks skipped; all 505 native lake tests and the runtime I/O ABI
gate passed, with zero failures or leaks. Focused final spill validation,
allocation-failure, mixed cursor and named-window dependency checks also passed.
All 50 real Parquet/Iceberg E2Es passed against the final Debug CLI in 642.72
seconds. The expanded Parquet restart case checks mixed exact arrays/native
outputs and unused-expression pruning. Zig/Python formatting, Python lint,
whitespace, licenses and the Apache product boundary (2,073 sources) passed.
Current `origin/main` `ce578d9033` is included. Archive-scale latency/RSS
benchmarks and live-provider retention qualification remain deferred.


### Native scan identities, exact predicate masks and spill reduction

Contiguous Parquet/Iceberg batches carry a shared external identity span.
The batch count and on-demand row-reference accessor preserve snapshot, file,
row-group and physical ordinal authority without a reference allocation per
row. Explicit identities remain available for arbitrary inputs. Slicing, delete
masks, hydration, pagination, replay and index builders consume the same accessor;
retained builds clone the shared identity once and preserve the compact span.

Residual scan predicates dispatch once per native column. Dense integer/float
comparisons use SIMD lanes; selected dictionary entries use a fixed bounded memo.
SQL NULL, excluded lanes and signed timestamp boundaries remain exact. NUMERIC,
wide datetime bounds, mixed integer/float comparisons, nonfinite coercion and
complex values retain the established scalar boundary. Literal coercion stays
lazy when no nonnull selected lane needs it. Embedded pruning remains advisory.

Native spill ingestion sizes and encodes selected primitive/dictionary columns
without Datum row reconstruction, using the existing portable block format.
Compatible raw partition blocks feed typed grouped state atomically; partial AGS
checkpoints, specialized exact state and sorted skew fallback retain their
existing representations and order. No borrowed block survives its owning read.
Declined group censuses use independently bounded workspace and iterative
reclaim-before-retry windows. Singleton fallback needs no census. Ordinary SQL
row/cardinality errors share one prefix-delivery classification; cancellation,
resource and integrity errors remain immediate failures.

Qualification: the full SQL gate passed 997 tests (three opt-in benchmarks
skipped); native lake integration passed 509 tests and the runtime I/O ABI gate
passed, with zero failures or leaks. All 50 real Parquet/Iceberg E2Es passed
against the final Debug CLI in 635.73 seconds, including filtered compact identity
stability across cold restarts. Focused allocation-failure, scalar-predicate
parity, mixed spill validation and direct/wrapped allocator pressure checks passed.
Formatting, Python lint, license headers and the Apache boundary (2,074 sources)
passed. Latest fetched origin/main `ce578d9033` is included. Representative
archive-scale latency/RSS and live-provider retention qualification remain deferred.

### Compact physical views and adaptive checkpoint reduction

Native SQL physical views borrow retained packed validity with a bit offset and
represent contiguous row selections as ranges. Sparse/permuted selections retain
explicit coordinates. Computed primitive outputs and spill readers share this
trait; hashing, admission and reduction reuse prepared block columns. Owned replay
materializes validity once at its ownership boundary. Selected coercion, REAL
rounding and SQL NULL identity retain their existing validation contracts.

Physical normalization has independently bounded optional workspace before the
group census. Admission follows immutable parent allocators through the SQL
synchronization and spill wrappers, preserving shared statement headroom. Declined windows release that workspace and retry smaller inputs;
actual allocator failures still propagate. Direct native result streams now use
the same SQL row-error classification as derived projections, preserving valid
prefixes before invalid casts without retrying resource or control failures.

Compatible AGS v2 checkpoints restore borrowed primitive columns and merge COUNT,
integer SUM, boolean reductions and eligible extrema after one destination-key
census. String extrema copy only winning payloads. Every signature and complete
window is checked before retained mutation; declined suffixes keep their original
checkpoint and ordinal. NUMERIC, DISTINCT, pattern and order-sensitive states
retain their established exact/sorted paths. No durable format or ABI changes.

Oversized fixed-state hash partitions may repartition on a discriminating unused
hash bit. Binary children, at most eight passes, descriptor headroom and disabled
nested worker fan-out bound resource usage. Identical hashes and unsupported state
keep sorted skew reduction. All partition staging is sealed and released before
reduction, preserving workspace for projections and downstream blocking operators.
The real Parquet restart fixture covers high-cardinality nested aggregation and
pgwire delivery of a successful row before an invalid text cast.

Qualification of compact physical views, allocator-aware admission, checkpoint
columns and adaptive radix reduction (2026-10-16): the full SQL gate passed
1,003 tests (777 shared-engine and 226 server), with three opt-in benchmarks
skipped. All 510 native lake tests (194 shared-engine and 316 server) and the
runtime I/O ABI gate passed with zero failures or leaks. All 50 real
Parquet/Iceberg E2Es passed against the final Debug CLI using two test workers in
325.57 seconds. Coverage includes direct/wrapped shared-pressure admission,
packed-validity ownership, partial signatures/truncation, allocation failures,
repartition progress/skew, descriptor pressure and real restart/error-prefix
flows. The fixture retries only explicitly retryable, unadmitted index-creation
catalog conflicts. Formatting, Python lint, whitespace, staged license headers
and the Apache boundary (2,074 sources) passed. Latest fetched origin/main
`ce578d9033` is included. Representative archive-scale latency/RSS and
live-provider retention qualification remain deferred.


### Survivor projection and exact spill admission

Scan-level SQL predicates pass an exact residual/delete/dynamic-filter mask
into Parquet projection. Cursor-owned pages validate framing and retain decompressed wire
payloads, definition coordinates and dictionary IDs. Projection converts only
surviving primitive, timestamp and decimal values; strings borrow immutable page
bytes. Eligible dictionary projections retain their dictionary identities for
downstream expression/group kernels. Rejected positions remain masked SQL NULL
placeholders in the original physical coordinate range. Misaligned column pages and later cohorts reuse the
same validated framing. A partially projected vector never becomes a shared
full-page cache entry. Broad page-local cohorts, complete cache hits and
unsupported layouts retain their existing paths. Density uses the projection
page intersection, even when predicate pages have different boundaries. Retained
page allocators switch to stable cursor backing after workers join, before scoped
locking adapters leave scope.

Dynamic scan filters dispatch per primitive column and hash fixed 128-row stack
windows, avoiding width-by-row Datum arrays. Dictionary memo state is bounded to
one window. Exact NUMERIC, unsupported coercions and NaN domain bounds retain
scalar semantics with reusable lane scratch. This applies equally to Parquet and
Iceberg scans; complete index-driven dynamic joins keep their existing planner.

AGS v2 checkpoint columns now restore exact NUMERIC SUM/AVG coefficients without
Aggregate/OwnedRow reconstruction. Admission reserves the union of exponent
ranges for every destination group before mutation. Raw NUMERIC spill blocks use
the typed key census as well. Adaptive radix reduction includes exact states;
file, descriptor and shared-workspace headroom constrain additional depth, and
exact finalization leaves room for downstream delivery and sorting. DISTINCT and
order-sensitive reducers retain their established exact paths. Wire formats and
public tuning surfaces are unchanged.

Retry selections have their own optional admission budget, so an oversized view
can shrink before allocating from statement headroom. Required allocation
failures still propagate. Vector-overlap qualification waits for archive entry
and recognizes either lane-entry order. The real Parquet restart fixture also
checks sparse wide/decimal/nullable projections; existing real Iceberg coverage
exercises dictionary projections, schema evolution, partition pruning and deletes.

Ancestor quota diagnostics follow scheduler and spill allocator wrappers. Reusable
pattern executions inspect local admission and their own DFA allocation-failure
state, so an ancestor's historical quota failure cannot invalidate healthy forks
after headroom recovers. Failed executions are still discarded and reconstructed.
Hard overlay and transform-size diagnostics likewise inspect their local budget;
parent cache pressure remains retryable and can use private ownership. Cancellation
and provider errors retain their own identity even after quota pressure.

Qualification: the full SQL gate passed 1,009 tests (three opt-in benchmarks
skipped), all 515 native lake tests and the runtime I/O ABI gate passed with zero
failures or leaks. Coverage includes retry-view pressure through direct and locked
allocators, allocation-free dynamic filters, selected nullable/dictionary framing
and ownership failures, exact coefficient unions, checkpoint truncation, and
small-budget NUMERIC spill/sort execution, and WAL private fallback/headroom
recovery. The archive/recent overlap fixture
passed 10 consecutive focused runs. All 50 real Parquet/Iceberg E2Es passed in
312.11 seconds against the final Debug CLI; its SHA-256 stayed unchanged across
the run. Formatting, Python lint, whitespace, staged license headers and the
Apache boundary (2,076 sources) passed. Latest fetched origin/main `968f037c9a`
is included. Representative archive-scale latency/RSS and live-provider retention
qualification remain deferred.


### Advisory SQL statistics and native page/expression ownership

SQL planning reads only statistics already owned by the captured publication or
scan cursor. Parquet footer null/distinct counts persist in inventory v19, with
backward reads of supported older inventories. A bounded metadata pass exposes
key domains, observed constant-group frequencies and row-width estimates; no planner
operation opens a source footer or enumerates a lazy inventory. Costing accounts
for equality fanout, NULLs, uniqueness, skew, retained build size and spill I/O.
Unknown NDV remains advisory, including estimates formed by merging group counts.
These statistics have no source-authority or row-pruning role.

Runtime join filters follow identity lineage across total projection/filter
frames. Both access-path selection and filter installation use the same physical
column mapping. Ordering, cardinality, invocation, aggregation/window and fallible
expression boundaries stop that mapping. Indexed probes can therefore serve safe
renamed derived tables while preserving the original producer's evaluation rules.

Nullable page coordinates retain RLE runs or packed membership/rank directories;
they do not require a page-sized ordinal vector. Native decimal expression results
own checked coefficient vectors and SQL-null flags until the downstream operator
releases them. Unsupported rounding/scale/precision cases retain exact scalar
execution, and scalar delivery owns any materialized canonical numeric values.

### Published costing and exact decimal continuity

Native publication captures advisory column statistics during its existing
projected, delete-aware reads. Per-file HLL and bounded heavy-key sketches live
in immutable, per-column trees; refresh reuses unchanged leaves and replaces or
removes changed files. An older publication backfills missing statistics once.
A compact summary is published only when the column's file coverage is complete.
Planning consumes that summary without footer requests or lazy-inventory scans.
Source navigation v3 (reader protocol 50) binds these statistics to publication,
and ordinary reader-safe GC and cloud generation proofs cover their trees.
Statistics are bounded to 64 primitive indexed column paths and remain estimates,
with no pruning or result-cardinality authority.

SQL retains finite decimal coefficients and display scales through subqueries,
retained join/group state, hashing and exact reduction. A bounded shared workspace
reuses pure expression nodes and broadcasts constants; variable division display
scales remain native. Wide/special values and unsupported expressions retain
exact scalar behavior. Attachment syntax, query results and wire formats remain
unchanged. Archive-scale benchmarks and live-provider retention qualification are
separate, deferred qualification work.

### Stable public navigation and typed execution

New source navigation v4 (reader protocol 51, inventory v20) separates snapshot
identity from file navigation. `lake3:` public IDs contain full snapshot and
stable-file SHA-256 digests plus row coordinates. The file directory keys exclude
the snapshot, so refreshes path-copy membership changes while replacements and
snapshot-only changes reuse the public tree. Public lookups and continuations
validate the snapshot proof. Legacy publications and `lake1:` IDs remain readable;
private posting identities retain their existing format. Query ordering, search,
ordered indexes and retained readers use the publication's identity version.
The complete inventory proof census remains necessary and is not delta-sized.
Ordering cache provenance is explicit and independent of the payload variant.
Builders select the identity version before preparing file order; legacy flat
callers refresh order once before parallel views are created.

Signed DECIMAL statistics are validated after applying Parquet logical types.
Decimal and timestamp predicates use exact typed comparisons for row-group,
page-index and Iceberg bounds, including join-derived ranges. Unknown metadata
cannot exclude rows. Native decimal COUNT/MIN/MAX retain compact coefficients,
and pure arithmetic forests evaluate through bounded registers with only final
output vectors. Scalar execution remains the authority for unsupported precision,
scale, wide values and fallible expressions. Real-writer Parquet/Iceberg tests
compare native results with scalar SQL both before and after process restart.

Qualification includes 58 real-writer Parquet/Iceberg E2Es (including signed
DECIMAL statistics and cold restart), 1,030 SQL checks, 468/151 lake checks,
217/301 shared/server native API checks, final publication/order regressions,
runtime I/O ABI, and 13 merged-main packaging tests. Latest merged main is
`408644a861`. Archive-scale benchmarks and real-provider retention qualification
remain deferred.

### Stable identities and explicit Parquet snapshot commits

The source binding owns `row_identity_version`. New attachments persist version
2 before their first query; schema updates preserve the stored value, including
when clients omit the read-only response field. Older bindings default to version
1. Index creation, rebuild, removal, automatic fallback and restart do not select
an identity epoch. Publication/navigation must agree with the pinned source's
epoch before native access is admitted.

Raw Parquet prefixes retain fresh object discovery. A reader cannot establish
that an arbitrary prefix has not gained files by checking one cached listing or
one existing object's ETag. Writers that need constant-size snapshot discovery
can instead attach a URI ending in `.parquet-manifest.json`. This is an explicit
commit protocol: the writer publishes immutable file-list chunks first, then
atomically replaces the small head. Antfly does not modify the source.

```json
{
  "version": 1,
  "source_id": "events",
  "file_count": 1,
  "chunks": [{
    "uri": "gs://lake/events/commits/chunk-<sha256>.json",
    "sha256": "<64 lowercase hex digits>",
    "byte_len": 2048,
    "file_count": 1
  }]
}
```

Each chunk is a JSON array of file records with `file_id`, `object_uri`,
`byte_len`, `row_count`, and `etag` and/or `version_id`. These must describe
actual immutable object versions; footers and data reads still enforce their
source/type contracts. A head admits at most 4,096 chunks/1,000,000 files and
1 MiB of JSON. Each chunk admits at most 4,096 files and 8 MiB of JSON. Duplicate
chunk locations, duplicate files, invalid counts and checksum mismatches fail
closed. Chunk hashes, source interpretation and credential scope key shared
metadata caches. A changed head's complete byte digest determines the source
snapshot, so reusing a human snapshot label cannot conceal membership changes.

Every open reads the head afresh. An exact publication-bound navigation proof
can then provide selected file records without opening file-list chunks. Other
reads prepare a persistent catalog, sharing unchanged chunks and path-copying
changed file membership. Writers retain old chunks/data for active readers; an
Antfly reader lease governs Antfly artifacts, not externally owned source GC.
Filesystem fixtures use `file:///namespace/head.parquet-manifest.json`, with the
head and chunks stored in the existing filesystem object-store namespace.

### Incremental proof preparation

Immutable Parquet manifest and Iceberg catalogs retain a bounded persistent
file-proof census alongside their immutable chunk lineage. Refresh removes old
chunk members and fingerprints added/replaced members, path-copying the affected
proof-tree branches. Cached subtree count/XOR summaries answer prior publication
range checks without constructing and sorting a new proof array or prefix-XOR
array. The domain binds source/schema, artifact namespace and complete delete
recipes/object versions. Changed delete evidence invalidates reuse and requires
fresh fingerprints. Optional cache admission failure retains ordinary preparation.
Unchanged compatible records are not reserialized while checking the delta.

This reduces repeated proof hashing/sorting and temporary census storage. The
legacy builders' flat file/changed arrays, inventory artifact serialization and
canonical coverage hash still require linear work over the file inventory.
Coverage can consume already ordered catalogs without another sort/index array.
These refinements do not make every publication operation proportional to the
delta, and do not treat cached proofs as authority over a fresh source commit.

### Typed filter and decimal execution boundaries

Runtime join filters normalize values to the declared SQL type before testing
ranges or Bloom membership. In particular, Parquet DOUBLE storage exposed as
REAL rounds before both operations, including dictionary lanes. Computed probes
and identity probes share the same equality domain.

Decimal scalar binding compiles immutable execution templates once. Templates
own instruction ordinals and register topology, and retain no source columns,
parameter values or request callbacks. Each batch binds its current views and
parameters, preserving selection/null/overflow fallback semantics. Coefficient
precision limits use a constant lookup table and are bound once per decimal
input; no row loop recomputes powers of ten.

The commit protocol admits at most 64 MiB of encoded file-chunk metadata per
snapshot, in addition to the per-chunk and decoded-residency limits. This is
validated before any chunk I/O and also bounds creation-time schema inference
and private planning when shared caches are disabled. Reader protocol 52 fences
publications predating the durable table-bound public identity epoch.

Real-file qualification covers committed-head refresh, schema inference,
unchanged-chunk sharing, and cold search/SQL count with an old file chunk
unavailable. It also covers saved public IDs through first index publication,
index removal and restart, for both plain and dictionary Parquet DOUBLE values
exposed as SQL REAL. All 61 Parquet/Iceberg cases passed on the final executable.

### Publication metadata and bounded cold discovery (protocol 53)

Ordinary native publication writes an `ANLIP001` inventory descriptor containing
only the source header, reclamation domain, file count, and authenticated file-tree
root. The source-navigation root retains the same tree through the existing GC,
reader-lease, cloud-generation, and retention-certificate closure. Inventory
metadata version 1 requires reader protocol 53 and a source-navigation root;
older flat inventory artifacts remain readable. Explicit legacy row-group delete
vectors retain flat encoding, because their validation needs row-group metadata.
Full inventory reconstruction is reserved for compatibility/reconciliation paths
and preserves provider failures and cancellation while walking authenticated pages.

Persistent immutable file catalogs reduce canonical data coverage through a
Cartesian Merkle tree over file IDs. Path-copy updates recompute only changed
branches; a bounded stack computes the same digest for uncached plans. Immutable
committed Parquet with strong provider selectors can use that root directly.
Iceberg's unresolved synthetic selectors still require provider qualification;
mutable data keeps fresh checks. Delete-object evidence is always qualified
independently. Pre-53 readers retain the earlier canonical data proof algorithm.
Publication consumers borrow the catalog's existing file-rank directory, with a
bounded tree lookup fallback, rather than copying an independent hash table for
every builder. Protocol 54 below replaces the remaining mutable native producer
arrays for census-backed publication.

Committed Parquet chunk discovery and schema-footer inference use at most three
jobs on the shared I/O scheduler. Each private speculative decode admits 8 MiB;
shared chunk decoding retains its independent node-wide loading/residency limits.
Large private metadata falls back to serial decoding under the original limits.
Results merge in source order, request allocators are synchronized while jobs are
active, and every error/cancellation drains outstanding jobs before releasing
source or allocator ownership. Schema inference drains the batch before allocating
into its returned arena. Scheduler saturation keeps required reads synchronous.

### Sparse producer state and typed residuals (protocol 54)

Census-backed publication borrows immutable file identities and represents unchanged
subtrees with confirmed hash ranges. Changed-file iterators seek past those ranges;
they do not clone the archive into mutable file descriptors and flags. Producers
keep a sparse overlay for changed document counts, key widths and document-block
references. Text construction likewise records only the reached file manifests.
The complete read-only source inventory remains shared. Shared replay union
planning now uses the sparse selection described below. Legacy inputs and
incompatible checkpoints use the full reconciliation path.

Dense and sparse metadata v3 persist per-file state in authenticated page trees.
Initial publication streams the census in hash order into a bottom-up tree builder;
refresh path-copies changed entries and removes retired entries. A changed file's
old document IDs are point-read from the previous directory for native deletion.
Delta reuse requires the producer checkpoint to match the source delta's prior
snapshot; older checkpoints reconcile their own complete state. Metadata v2 flat
roots remain readable and migrate when a compatible delta is available. Protocol 54
fences readers that cannot traverse the new state directories. Direct and resumable
GC traverse directory leaves and their document chunks, with expansion roles
independent of an earlier generic page mark.

Spilled joins stage bounded candidate cohorts while probe/build blocks remain
pinned. Eligible residual expressions consume physical and decimal column views;
accepted lanes update outer-match state before the next probe advances. Unsupported
or row-error-producing expressions replay through the scalar evaluator in source
order to preserve delivered prefixes. Exact decimal column comparisons align
coefficients in i256, retaining per-lane display scales and SQL NULLs.

Floating masks normalize stored REAL values through the same checked conversion
as scalar SQL, without narrowing comparison operands. Dense lanes use SIMD;
dictionary/encoded lanes memoize referenced values. Typed FLOAT/DOUBLE file,
row-group and page bounds use the declared SQL domain. NaNs are absent from
Parquet extrema, so upper-side predicates and inequality remain conservative;
REAL underflow/overflow and ambiguous zero-crossing ranges remain residual.
Bloom probes require physically equivalent equality bytes and decline signed zero
and narrowed DOUBLE-backed REAL. OffsetIndex cache admission exhaustion declines
the optional index and retains sequential page scanning; allocator and provider
failures remain errors.

### Native replay, probe cohorts, and compact Parquet encoding follow-up

Replay planning runs only when shared replay has multiple consumers. A sparse
union uses native Roaring sparse/dense containers for needed file ordinals.
Compatible text roots and completed ordered roots borrow the publication's authenticated source delta without loading their
complete file catalogs. Source, schema and predecessor snapshot must match. Text
coverage must match the predecessor file population. Partial or older checkpoints
retain their own reconciliation path. Dense/sparse paged roots likewise add only
changed ordinals. Compatible algebraic and ordered cuts use the protocol-55
planning described below. The sparse union governs replay reuse only; an
uncaptured file retains ordinary source reads. Discovery and the source inventory
remain shared work; total publication is not claimed to be delta-only.

Spilled hash joins collect candidates across probe rows within one pinned physical
block. A per-probe match bitmap replaces the single current-row flag. Residual
acceptance updates both probe and build state; unmatched probe rows become visible
only after the block's pending residual decisions finish. Block release, partition
advance and outer completion remain cohort boundaries. Scalar residual fallback
and delivered-prefix error handling keep their existing authority. This enables
vector residuals on one-to-one joins, as well as duplicate-key and skew joins.

Parquet readers support DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY,
DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT in addition to plain/dictionary pages.
The shared physical codec retains one predecessor checkpoint per 32 delta values
and one cached decoded block. Length streams retain offsets into immutable suffix
bytes; prefix strings reconstruct only demanded values and reuse previously
reconstructed immutable prefixes. Byte-stream-split gathers selected physical
lanes without transposing the whole page. Nullable V1/V2 framing, signed wrapping,
logical decimal/timestamp conversion and declared SQL widths remain separate.
General full-column materializers fill their final typed vectors directly,
with compact navigation and bounded string reconstruction scratch. Footer eligibility recognizes the new data encodings independently of
RLE definition levels. Existing request/cache admission and cancellation apply;
no new SQL syntax, public identities or native sidecar wire version is introduced.

Checked-in independent PyArrow fixtures and their generator cover both page
versions, NULLs, signed 32/64-bit deltas, floating/integer/decimal split streams,
and both string delta encodings. Public E2Es attach generated files, run filtered
queries, and repeat after restart. Archive-scale latency/RSS measurements and
live-provider retention qualification remain separate work.

Follow-up qualification on Zig 0.17.0: 79 SQL join tests and 265 embedded lake
reader/cursor tests passed without leaks, along with the focused sparse-replay,
append/removal aggregate, and keyed-contribution regressions. A complete stripped
Debug Antfly server build passed. The new two-case public E2E was attempted locally
but stopped before server launch by storage admission (about 2.1 GiB available;
16.25 GiB required). These HTTP attachment/restart cases still need execution in
`e2e-full` or another environment with sufficient disk headroom.


### Delta publication and direct physical decoding (protocol 55)

Completed algebraic reductions persist authenticated Patricia-tree ranges and
file counts alongside their existing child/alias ownership. Compatible refreshes
match the prior source/schema/snapshot/count proof, collect only changed and
removed file keys, and apply sorted mutations bottom-up. Each affected reduction
node is merged once; untouched branches retain their artifacts and incoming
ownership counts. Recipe-local roots are bound to the same predecessor census.
Legacy reductions reconcile once to establish range evidence. Replay planning
uses the same evidence instead of allocating all file digests or probing each
file/recipe. The prior source census carries aggregate identities for removed
files, including pinned object versions. Protocol 55 fences the additional durable
contribution metadata; aggregate payload and query wire formats do not change.

Completed ordered indexes plan small updates against authenticated paged slot/name
dictionaries and shared public navigation. Only changed/removed names are resolved;
sorted slot edits path-copy dictionary pages, preserve unchanged slots, and reuse
slots retired in the current cut. Replacements remove only their reverse ranges.
A source file checkpoints all of its new covering rows and fingerprints together.
Incomplete checkpoint cuts and incompatible/new indexes use full reconciliation;
dense refreshes retain the bounded base/run merge strategy. No-change source cuts
update the public directory without walking all ordered rows. The shared source
inventory still exists; this removes additional per-index archive-sized maps,
fingerprint arrays and keep masks from the compatible small-update path.

Compact Parquet full scans now fill final integer/floating/byte vectors without
creating an intermediate plain-wire page. Delta navigation uses vector prefix
sums, aligned-width unpacking, and arithmetic constant-delta checkpoints. Numeric
predicates compare up to 32 contiguous physical lanes at once. Prefix-string
predicates and full scans borrow reusable reconstruction scratch; only demanded
stable projection values enter the immutable value cache. NULL-only pages accept
validated zero-value delta headers, and unused terminal miniblock widths are
ignored once all declared values are consumed. Nullable framing and logical
interpretation retain their existing validation and request admission boundaries.

Independent V1/V2 NULL-only fixtures cover all three delta encodings in full and
selective scans. Additional regressions cover arbitrary unused width bytes, signed
INT32/INT64 wrapping, vector masks across checkpoint boundaries, and full decoding
inside admission that cannot hold both a final vector and a plain staging buffer.
Native publication tests deny unchanged Parquet reads while exercising aggregate
append/removal/mixed/no-change edits and ordered file churn with covering payloads.
Public attachment/restart tests include NULL-only files; their local disk admission
requirement and live cloud qualification remain unchanged. Archive-scale latency
and RSS measurements are still required before claiming workload speedups.


Fault-injection qualification also found and fixed a schema-footer cleanup error:
schema elements now remain owned until the columns array has transferred out,
including failure of its final allocation. Schema-prefetch fault injection uses
the existing deterministic allocator helper; ordinary serial and parallel checks
continue using the normal allocator. The final reader/cursor suite passes 271 tests
with no leaks. Public E2E execution still requires sufficient local disk headroom.

Publication qualification passed 175 shared and 302 native external-lake tests
without failures or leaks; the final retained-root ordered churn regression also
passed independently. All four public V1/V2 mixed/NULL-only attachment and restart
cases were attempted, but storage admission stopped them before launch (about
2.0 GiB free versus 16.25 GiB required). They remain pending in `e2e-full`; no
production admission safeguard was disabled.

The final page-expansion guard rerun passed all 271 reader/cursor tests without
leaks. A complete stripped, optimized (`--release=fast`) Antfly server build also
passed on Zig 0.17.0, and the installed CLI reports its version successfully.
Repository license/boundary, source catalog, generated engine error catalog,
formatting and whitespace checks passed. Cache cleanup preserved active caches;
it did not free enough space to satisfy the public E2E storage floor.

### File-first native publication and sequential delta pages

Native publication admits a bounded cohort of aggregate, ordered, text, sparse,
and dense builders under the shared scheduler. Consumers announce their next
source file; the producer captures one delete-aware typed run and releases all
consumers requesting that file. Canonical census order aligns fresh and delta
scans without an archive-sized permutation or spool. The run remains pinned until
all current readers drain. Multiple configurations or divergent resumed plans can
make additional passes; matching requests in an active cohort share the decode.
Stored-source and enrichment projections include their full declared dependencies,
and NUMERIC source contracts preserve exact physical values.

Worker admission completes before any builder starts. Insufficient scheduler or
known allocator headroom selects ordinary serial construction. Opaque allocator
chains also retain that path. Oversized replay files use serialized ordinary
streaming. Workers own independent construction state and share synchronized
allocation; durable checkpoints serialize through one progress owner. Deadline,
cancellation and builder failures wake waiting consumers and join workers before
releasing replay or request memory. The existing publication lease and source cut
remain authoritative throughout construction and final publication.

Outside a publication cohort, native aggregate leaves run in bounded batches of
up to eight through the shared scheduler. Each job owns its source stream and exact
partial state; the coordinator publishes artifacts and changes contribution
ownership after joining. Both full reconciliation and authenticated deltas use this
boundary. Within publication, the file producer shares each current run across
families rather than spawning consumers against conflicting files.

Full DELTA_BINARY_PACKED pages now use a sequential physical cursor with a single
32-value block. It validates framing and signed wrapping while filling final
vectors, without allocating random-access checkpoints or decoding their values a
second time. Selective reads continue using checkpoint navigation. Required integer
full materialization fits a 64 KiB admission budget for a 64 KiB result. Nullable,
zero-value and terminal-miniblock framing retain the same validation rules.

Sparse ordered-index construction registers tuple ownership before predicate
initialization. Its allocation-failure regression fails the first predicate
allocation after tuple creation and requires leak-free cleanup. Publication
regressions cover real Parquet files shared by aggregate/ordered/text families,
partial scheduler admission, working-set admission, oversized-file fallback,
deadline propagation and concurrent checkpoint persistence. Public HTTP E2Es and
archive-scale latency/RSS measurements remain subject to their existing
qualification requirements.
