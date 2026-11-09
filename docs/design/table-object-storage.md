# Table-level object storage

Status: initial implementation. The catalog/API engine contract is shared across
standalone and distributed deployment. Existing native tables keep their storage
and placement contracts. The serverless command remains the specialized worker
preset.

## Independent choices

A table chooses `storage.engine: local | object`. `local` is the compatibility
default and uses the hosting process's local/Lite persistence and normal shard
placement. `object` uses shared durable objects without data Raft replicas.
`schema.storage_mode` continues to describe document versus relational data;
`schema.base_source` continues to distinguish an external lake from owned data.
Deployment controls process roles, not the table's durable engine.

Examples:

```json
{
  "storage": {"engine": "object"},
  "schema": {
    "storage_mode": "relational",
    "base_source": {
      "kind": "external",
      "table_id": "events",
      "format": "parquet",
      "uri": "s3://lake/events",
      "write_policy": "read_only"
    }
  }
}
```

```json
{
  "storage": {"engine": "object"},
  "indexes": {"search": {"type": "full_text"}}
}
```

The first table reads lake data and maintains native catalog-fenced sidecars. The
second table accepts document batches into the object WAL and queries published
object generations. The object engine is immutable; changing it requires an
explicit migration into another table. Omit `num_shards` and replication sources
on object tables. Dense `vector_store` ownership belongs to native local tables.

## Implementation sequence

1. Expose lake data and durable sidecars through the native table API. Reuse the
   lake readers, builders, publication CAS, reader leases, and collection leases.
2. Admit object tables as catalog objects with zero data ranges and zero data
   replicas. Neither table creation nor inherited tablespace range defaults may
   manufacture data Raft groups for them.
3. Host the existing object WAL/manifest/query/maintenance stack inside a native
   API process for owned document tables. Reuse the existing write acknowledgments,
   synchronization levels, work leases, publication fencing, and recovery logic.
4. Keep the serverless API/query/maintenance/combined commands as deployment
   presets. Their independent scaling is useful even when storage is a table
   property. A native process may serve native and object tables together.

This implementation provides the sequence's initial data paths: lake/SQL reads
and sidecars, plus document batch, key lookup, and search execution using the
object runtime.
Writable object document definitions are initially immutable. Unsupported native
operations must fail without allocating a local shard or claiming a local commit.
Further capability parity and explicit engine migration remain separate work.

## Authority and ownership

The native catalog owns existence, logical naming, schema, indexes, and table
incarnation. In distributed mode it retains metadata consensus. Removing data
Raft does not remove metadata authority or coordinated publication.

### Native owner serving cuts

The candidate adds opt-in native owner admission for sources whose complete
write path Antfly controls. `lake_indexes.serving_cuts.enabled` starts durable
activation after all writers are upgraded. Reader sharing alone retains strict
remote checks; an activated owned source instead serves its native admitted
publication/WAL record. This candidate still requires end-to-end qualification
before release or deployment.

Eligible sources are managed Iceberg behind an enforced writer gateway and
sealed immutable Parquet manifests. An immutable-file prefix is insufficient:
its membership can still change. Shared external catalogs retain fresh catalog
coordination or an explicit bounded-staleness contract.

Activation conditionally claims the managed catalog and WAL heads with the
same durable owner nonce. Unhooked writers fail, including older binaries whose
strict parsers reject the ownership field. Claiming stages ownership; admitting
a complete first cut atomically enables native reads. Strict coordinated reads
remain available while initial capture is incomplete. Turning the configuration
off does not remove an established fence. Sealed manifests have immutable
membership, and capture retains their exact parsed head even without a published
file-navigation index.

The native owner persists source identity, incarnation, ownership epoch,
acknowledged WAL watermark and the current reusable serving-cut reference.
A compact projection is written in the same native transaction as lifecycle
state, so fresh queries do not transfer retained publication history. Native
snapshots include both projections; older checkpoints reconstruct the compact
view on the metadata owner. Absence is materialized too, keeping unowned sources
on a small point read. During rolling upgrades, an older peer rejecting this
private tag selects the existing authoritative lifecycle read. Permission and
identity failures never select that fallback. Every accepted write advances or invalidates that
record before acknowledgment.
Catalog changes invalidate it before returning success. If remote commitment
succeeds but native acknowledgment fails, the API reports `state: unknown`
without a read receipt. Exact retries repeat native acknowledgment without
appending the WAL again; resolving a committed catalog request also repairs
native invalidation before reporting success. Admission of a rebuilt
cut compares the epoch captured before observing catalog/WAL state, and cannot
regress the acknowledged watermark. An unavailable replacement cut returns a
retryable rebuilding response rather than serving an older acknowledged state.
The checks belong inside the native metadata transaction, including table
incarnation checks. Distributed mutation and warming obey leader ownership.

Activation must fence all writers, including standalone, SQL, CDC and older
processes. Conditional WAL/catalog ownership prevents an unfenced writer from
successfully appending after activation; a separate advisory lease is not enough.
Only the gateway holds vendor write credentials. Ownership transitions must
preserve valid old cursors or drain them explicitly. Unconfigured sources keep
their existing remote checks; failure to establish a fence cannot silently
enable this mode.

Publication and write acceptance construct reusable immutable state references
containing the published generation and bounded pending row images. Queries
issue authenticated cursor references to that state and a WAL position, without
uploading another retained image or cursor envelope. The implementation uses
HMAC-authenticated `lake3:` and `source3:` capabilities, while retaining legacy
`lake2:`/`source2:` reads. A compact framed payload avoids JSON-escape expansion
within the existing token bound; existing JSON-envelope capabilities remain
readable through their original expiry. Signing keys stay in private native
metadata. Cursor validation still
checks current permissions, incarnation, recipe, ownership and expiry. Reader
leases protect published generations through every issued cursor's expiry;
state images and their row groups must live at least as long. Admission extends
the publication reader through twice the cursor TTL plus a refresh margin,
atomically with accepting the image reference. Existing generation and metadata
bounds can impose explicit backpressure at high publication rates. Refresh happens
before the remaining retention horizon becomes too short for a new cursor.
Increasing the configured cursor lifetime forces refresh when the existing
horizon cannot cover it; unavailable cuts also trigger keyed reconciliation.
Required recovery coalesces by table incarnation within four process-wide lanes.
Each waiting caller keeps its own deadline/cancellation; recovery also observes
shutdown and a 60-second maximum deadline. Readiness does not swallow a required
recovery failure for an active owner or advertise a cut whose retention cannot
cover the configured cursor lifetime. Optional warming cannot establish readiness.
Raw captured WAL heads cannot substitute for retained row images when their
underlying roots can be collected independently.

The owner warms the admitted cut's bounded working set: navigation, dictionaries,
postings, row locators and replacement-key indexes. Existing process accounting,
eviction and cancellation apply; warming yields to foreground work and stops
when the admitted generation changes. Four bounded continuation plans retain
completed navigation waves and their working-set quota across timeout, with
eviction-aware rewarming after a cooldown. Plans own immutable references, never request credentials or reader
leases. Compressed opening bundles replace redundant individual metadata fetches.
Retained state, row groups and sealed heads also use the authenticated durable
byte cache, with exact provider generation and credential scope in their keys.
Cache hits still require current native authority or a valid retained capability.
Query execution merges pinned readers
directly, fetches remote blocks on misses and hydrates winning hits. These
optimizations must preserve unfiltered replacement probes and exact pagination.
External-writer freshness, vector overlays and unsupported cursor shapes keep
their established paths until separately qualified. Warm fixture latency targets
do not qualify full-archive performance or scale-to-zero startup.

### Cold opening and restart recovery

Publication reader protocol 98 adds optional `opening_bundles` to the committed
catalog record. Each bundle contains compressed, individually authenticated
opening artifacts and page ranges: source and index roots, row-identity paths,
term statistics/directories, routing, file dictionaries, and a bounded selection
of segment opening metadata. Their identities and sizes are available from the
publication itself, so serving can fetch them without first discovering a chain
of child objects. Original artifacts remain independently rooted; missing or
invalid bundle contents fall back to verified demand reads. Bundles carry no
credentials, leases, mutable HEAD, or source authority.

The producer reads at most 32 MiB of opening payloads, visits at most 64 pages
through three child levels, and opens metadata for at most 16 segments. It emits
at most four bundles, each at most 4 MiB decoded. These limits prevent archive
size or index count from turning a publication hint into a corpus download.
The cold reader coalesces concurrent preparation of the same publication and
credential scope. Independently owned provider clients fetch bundles in waves
with at most four workers, a conservative 32 MiB wave reservation, and the shared
I/O scheduler's admission. Provider cancellation polling includes the request
deadline and explicit cancellation, while fallible artifact checkpoints preserve
reader protection and precise deadline errors. Transport polling never invokes
possibly task-bound checkpoint I/O. Cancellation joins workers before releasing clients
or scratch memory. Denied optional scheduling falls back to demand reads.
Cloud generation maps, retention certificates, and collection mark the new
bundle objects alongside the original closure.

A cleanly drained disk-cache owner writes a bounded inventory checkpoint in its
private, exclusively locked directory. Restart validates its checksum, directory
identity and timestamps, consumes it before serving, and reconstructs accounting
and eviction order without opening every cached file. An interrupted owner,
changed directory, corrupt checkpoint, or smaller capacity falls back to the
existing complete scan. Cached payloads still require exact identity, key,
length, and digest verification. API startup starts optional restoration only
after the server occupies its final address, before public HTTP admission;
queries retain the RAM/source fallback while restoration runs.

The admitted native cut remains the authority for committed generation and WAL
watermark. Restart cache recovery does not relax freshness, reader expiry,
incarnation, credential rotation, or external-catalog checks. Protocol 97 cuts
remain readable without bundles; a new publication/rebuild is needed to gain
publication-level opening bundles. Metadata capability 85 fences older readers
from new lake catalog writes.

Request-owned atomic I/O profiles record immutable RAM/disk hits, provider read
count/bytes/errors, overlapping provider elapsed time, peak active loads, and
bundle-opening time. They attach to the prepared reader's lifetime, not a leaf's
scratch arena, and survive only through the request's bounded diagnostic
snapshot. Process counters continue to cover all provider traffic; these request
profiles specifically attribute immutable index/cache reads. Slow-request health
metrics expose the profile without table names, object paths, or credentials.
Disk metrics distinguish checkpoint recovery from scanned inventory entries.

This follows [Lance's upfront metadata size hints and reading strategy](https://lance.org/format/file/#reading-strategy),
[DuckLake's snapshot/file metadata catalog](https://ducklake.select/docs/stable/specification/queries),
and [DuckDB's asynchronous I/O and governed readahead](https://duckdb.org/2026/07/31/asynchronous-io).
The cache follows the same distinction as [Databricks disk caching](https://docs.databricks.com/aws/en/optimizations/disk-cache):
local restart reuse improves repeated access, while a genuinely empty cache must
still fetch remote bytes. [Lance's cache and concurrency guidance](https://lance.org/guide/performance/)
also motivates separate metadata residency and modest, byte-budgeted parallelism
on small instances. These changes preserve selective posting reads and winning-hit
hydration; they do not download full archive indexes to make a first query fast.

External lake sidecars retain one metadata authority: a builder first commits a
pending attempt through definition CAS, renews its fence, uploads immutable
artifacts, and publishes through the exact admitted CAS. Upload completion is
never publication. Query workers pin source coverage and a published generation.
Reader and collection leases protect retained generations and artifacts.

Owned object document tables have a private object catalog projection per native
incarnation. Public requests cannot invoke object-catalog DDL. That projection
holds the immutable schema/index definition required by the existing object
builder; it is not a competing table-management authority. The native API
resolves an authoritative table binding before executing its data operation.

The object runtime owns WAL append order and visible HEAD. Ownership takeover and
HEAD publication use the existing conditionally written coordination record and
fencing token. A lease checked separately from publication is insufficient: an
old worker must not publish after another worker takes over. Multiple native API
workers may host the same object table and contend through that protocol.

Local worker affinity is a scheduling/cache optimization. Durable objects and
fences determine correctness. Object tables have no data shard voters or leaders.

## Storage binding and incarnation isolation

Use the capability-scoped `storage.artifacts` destination for native object
tables. Distributed API nodes must share this destination and have authorized
`storage.primary` access. Source lake credentials do not grant sidecar or WAL
write authority. Standalone may use the existing filesystem artifact
fallback for development.

Before owned-object data execution, the API pins a digest of the physical store
locator in native metadata through CAS and verifies the committed definition.
The digest includes protocol, connection, endpoint/root, bucket, prefix and TLS;
it excludes credential material, so rotating credentials does not relocate data.
A mismatched node fails closed. Relocation requires an explicit migration.

The durable table record carries an object generation. Distributed creation uses
the native table transition fence; standalone creation uses its persisted catalog
epoch because tables with no ranges do not advance range transition fences. The
private root includes table identity and that generation.
A late request or collector for a dropped incarnation cannot touch a recreated
table's root. The old generation remains isolated and retained; automatic
whole-table deletion of owned-object roots is not introduced by this change.

Native processes lazily host bounded per-incarnation runtimes. Each runtime owns
its authorized client, query state, and maintenance worker, and stops before its
client is destroyed at API shutdown. There is an initial cap of 128 hosted
incarnations per API process. Dropped runtimes remain retained until restart;
a later retirement owner can drain and reclaim them without racing requests.

## Compatibility and admission

Legacy table records retain their existing JSON interpretation, binary encoding,
and definition fingerprints. Native storage metadata continues using extension
version 1. Object records use storage extension version 2, including engine,
incarnation generation and physical-store identity. Metadata decoder capability
30 gates object records at admission and at final command append.

Empty data topology is valid only for object records. Native create requests with
zero ranges remain invalid. Reconciliation must preserve the engine and storage
binding when comparing records. An existing native table never becomes an object
table merely because a deployment default changes.

Tablespaces currently describe placement policy. They can later provide named
object destinations and inherited creation defaults, but this implementation
does not reinterpret their location metadata as a storage override. Resolved
bindings must remain table-owned when defaults change.

## Request routing and read consistency

The narrow query definition carries the table engine from the same catalog read
that binds the query. Single-table JSON, table NDJSON, and global multi-query
requests all dispatch through that binding, authorize each line independently,
and retain logical table labels in responses. Retrieval/RAG adapters use the same
object dispatch and published data rather than bypassing it through native shards.
Query definitions carry the object incarnation generation along with table ID;
dispatch validates both against the authoritative snapshot. Drop/recreate or an
engine change during binding returns a catalog conflict, never a result from the
replacement table. Mixed local/object requests select
a data path per table. Binding preserves the distinction between a request-local
foreign alias and a catalog table: foreign primaries go directly to their source
executor without native engine discovery. Join admission checks every native
participant, including nested right sides, against definitions from that same
catalog revision before executing any side. Owned object document participants
return HTTP 400 even when another side would produce no hits. External lake
participants retain native join support.
Point writes and lookups use a targeted definition read to select the engine,
rather than cloning every table and range in an administrative snapshot. Object
execution still revalidates the authoritative incarnation and physical binding.

Owned object key lookup requires explicit `consistency=stale` (the legacy
`read_consistency=stale` spelling is also accepted). It reads the published HEAD;
a WAL-only acknowledgment does not imply that HEAD contains the write. The
normal default `read_index` and explicit `leader_lease` return HTTP 400 for owned
object tables, and invalid consistency values are rejected before execution.
Clients requiring publication before lookup can write with `sync_level=full_index`
and then perform a stale lookup. Supporting read-index-equivalent object reads
will require a WAL visibility fence or an authoritative WAL overlay; metadata
consensus alone cannot provide that data guarantee. External lake tables retain
their existing native read contracts.

## Hosted execution configuration and request lifetime

Hosted object stacks inherit the native API's configured graph execution limits
and remote content configuration. Engine selection must not restore serverless
bootstrap defaults in place of explicit host limits.

The bound query retains its original request context and absolute deadline across
catalog resolution and object dispatch. The object runtime accepts that context,
normalizes borrowed executor clocks to platform monotonic time while preserving
remaining time, and checks it during admission, cold initialization, lookup and
response delivery. Waiting for the runtime registry mutex remains cancelable.
Request-scoped deadline checkpoints are also exposed through a fallible
cancellation token to object query and artifact readers. Session acquisition
passes that token through HEAD and manifest reads into the object-store client,
before opening reader leases or accessing document and index artifacts. The
callback is borrowed only for synchronous request execution; background workers
retain their own
lifetime and do not retain inbound request callbacks. Store operations that do
not support interruption are checked at their boundaries; their individual
transport timeouts still bound an in-flight call.

Before WAL append, expiration rejects the write. After WAL acceptance, expiration
must preserve its durable outcome. Publication synchronization uses the earlier
of the request deadline and the existing 30-second synchronization ceiling and
returns the committed/pending acknowledgment when that budget expires. A final
response checkpoint must never turn an accepted batch into a pre-commit timeout.

## Initial capability boundaries

External read-only Parquet/Iceberg tables keep their existing SQL and sidecar
capabilities. Owned object tables expose document batch, published key lookup,
and search paths. Their schema/index definitions are fixed at creation; this prevents
stale workers from projecting a newer native definition backward into the private
object catalog. Native engine changes and pinned-store changes are rejected.

Cross-engine atomic transactions, owned relational object writes, native
split/merge, native backup/restore of owned object tables, and native credential
row filters are not made equivalent to local-shard operations by this setting.
They need explicit protocols before admission. Existing serverless APIs retain
those APIs' existing semantics; the preset does not imply native capability parity.

## Validation

Cover engine parsing and admission, legacy framing, object framing and command
round trips, decoder gates, zero-range topology, definition/binding immutability,
and clone/reopen preservation. Exercise native API storage-binding admission and
policy-authority failure, JSON/NDJSON/global query routing, explicit lookup
consistency after WAL-only writes, narrow native engine selection, and an owned-object batch with synchronous index
publication, lookup, search, runtime reopen, and recreation into an empty generation.
Existing lake API and catalog suites cover source reads and sidecar publication.

Direct embedded storage APIs require a hosted object runtime and reject the object
engine rather than opening a local DB with different durability semantics.

## Iceberg catalog authority

See [Native Iceberg catalog authorities](lake-catalogs.md) for managed object-store
and external REST configuration, durable commit recovery and the boundary between
Iceberg commits and searchable index publication.
