# Lake ingestion, change capture, and publication

Status: proposed architecture, captured 2026-10-08. This is not a shipped API or
an implementation status checklist. Existing behavior remains documented in
[LAKES.md](../../zig/LAKES.md), [REMOTE_TABLE_SERVING.md](../../zig/REMOTE_TABLE_SERVING.md),
[CDC.md](../../zig/CDC.md), and [SERVERLESS.md](../../zig/SERVERLESS.md).

## Problem and intended behavior

Antfly already reads remote Parquet/Iceberg snapshots and maintains native
indexes over external rows. Its serverless path also has durable WAL ingest and
publication of Antfly-owned artifacts; native row-fragment publication is a
separate existing foundation. These capabilities do not yet constitute a
general writable Parquet/Iceberg table backend with managed source connectors,
catalog commits, recent/archive merging, and compaction.

The long-term goal is one table/query contract for externally owned lakes and
Antfly-owned data, with a shared durable ingestion path for application writes,
database CDC, and custom pipelines. Object notifications should accelerate lake
discovery without becoming the authority for table commits. S3, GCS, and future
object stores should use the same format, indexing, and execution machinery.

Hacker News is a motivating integration, not an engine-specific source type.
An HN adapter owns API polling, normalization, moderation interpretation, and
story ancestry. Antfly should eventually own durable mutations, progress,
archive publication, indexing, and recovery. A Python worker using Antfly Lite
and PyIceberg is an interim implementation, not the required product topology.

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
- Catalog adapters and initial managed-table commit authority; external
  multi-writer support and source conflict policies.
- Recent-tier realization per deployment, transaction scope, text corpus scoring,
  and bounded exact query behavior when archive indexes lag.
- Plain-Parquet manifest protocol, key/schema requirements, and hook receipts.
- Notification transport adapters, subscription provisioning/ownership, warehouse
  capabilities, and missing-log/reseed policies.
- Compaction policy, external-reader retention evidence, publication cadence,
  and operational limits. No fixed performance guarantees are established here.
