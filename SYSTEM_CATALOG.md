# System Catalog Design

Antfly's system catalog owns databases, namespaces, table identities, and
logical tablespaces. It is independent of SQL, relational rows, and the storage
engine. PostgreSQL also calls its persistent database metadata [system
catalogs](https://www.postgresql.org/docs/current/catalogs.html). SQL and other
frontends should bind through this catalog rather than maintain separate name
or placement authorities.

## Names and identity

A table target is `{database, namespace, table}`. The defaults are `default`
and `public`. String table names always mean a literal name in that default
scope: `sales.archive`, `sales/archive`, `sales archive`, and `*` retain their
meaning as table names. Strings are never split on dots. Table names retain the
existing 1–255-byte contract excluding control bytes. Database, namespace, and
tablespace names use 1–128 ASCII letters, digits, underscores, or hyphens,
starting with a letter or underscore.

HTTP exposes explicit scope in
`/db/v1/databases/{database}/namespaces/{namespace}/tables/{table}`. Each path
component is percent-encoded independently and decoded exactly once. The legacy
`/db/v1/tables/{table}` route selects `default.public`.

Global queries accept exactly one of `table` and `table_target`. Joins likewise
accept exactly one of `right_table` and `right_target`:

```json
{
  "table_target": {"database": "analytics", "table": "events"},
  "full_text_search": {"match_all": {}},
  "join": {
    "right_target": {"database": "analytics", "table": "customers"},
    "on": {"left_field": "customer_id", "right_field": "_id"}
  }
}
```

Database, namespace, and table renames preserve IDs. Native physical routing
names are immutable and are not public aliases. Legacy tables keep their
existing physical names; a literal name beginning with `table:` is valid when
it actually names an independently cataloged table. Public `table_id` values
are decimal strings so JavaScript clients preserve all 64 bits.

Display labels use the literal name in `default.public`, and
`database.namespace.table` elsewhere. These labels are presentation, not a
parseable target format; two distinct structured targets can have the same
label. Clients must retain structured targets when composing subsequent calls.

Creating a database also creates its `public` namespace. Empty databases and
namespaces can be dropped. The compatibility database and its public namespace
are protected. Rename requires authority on both the old and new names.

## Binding and execution

HTTP and A2A retrieval use the same query binding boundary. Background entity
resolution owns a lazy binding per changed extraction artifact and resolver
configuration. All mentions in that work unit reuse the same immutable candidate
table destination. Exact-key resolution reads up to 256 candidate IDs at a time
through the existing fenced document-value query path. It preserves duplicate
mentions and missing candidates. Exact keys and label prefixes are deduplicated
per work unit. Prefix search scans each distinct prefix once, enforcing the
candidate bound even with a custom source. A second bulk read hydrates distinct
one-hop curated merge destinations; missing destinations are cached for the
work unit too. Immutable candidate records are decoded once per distinct lookup
and shared while each mention is scored independently. ANN retains its
mention-specific nearest-neighbor search.
Deterministic mint-only configurations skip candidate and embedding I/O while
still retaining the destination binding. Malformed candidate responses fail the
work unit instead of silently minting
entities. Resolution artifacts persist the destination alongside the
logical `doc_ref.table`, so deferred promotion and replay cannot redirect writes
to a replacement table. Older artifacts without a destination bind every distinct
logical target in one catalog read before submitting their atomic write batch.
Curated endpoints outside the resolver's declared table retain their independent
promotion binding. Graph hydration
keeps logical endpoint names for authorization and result provenance, while its
request-owned resolver pins physical target identities and a catalog revision
through execution retries. Target row filters run against the resolved table.
A missing graph endpoint is omitted rather than failing the surviving graph.
Binding alone does not require document admission reads: unauthenticated graph
requests hydrate only when documents are requested. Authenticated requests and
row filters retain their document-admission checks.

Default index incarnations are assigned during public create normalization and
preserved through the metadata hop and local materialization. Creating another
index cannot leave the default index waiting for a different incarnation.

Public query admission authorizes logical references and applies their row
filters before routing. It binds the primary table and every nested native
join target in one linearizable catalog read transaction. Execution and retries
retain those immutable physical identities. A rename or a drop/recreate cannot
redirect an in-flight query to a replacement table. Foreign source aliases are
resolved from the query's foreign-source map and remain separate from native
catalog targets.

The internal join worker envelope carries physical routing names and separate
logical result labels. Workers consume the coordinator's binding; they do not
resolve those names again. Plain query encoders receive the logical table label
with the search request. Joined responses set it while assembling their existing
response object. Neither path reparses a completed response just to rename its
table field.

An NDJSON request shares a resolver and catalog revision. It deduplicates
repeated targets across lines while retaining per-line authorization. New
references must resolve at the same revision; a concurrent catalog mutation
returns a catalog-generation conflict instead of mixing views. This cache is
request-owned, never a process-wide name cache with a time-based expiry.

## Metadata reads and durability

Catalog records, revisions, and table topology commit through metadata Raft.
Logical and physical name indexes are local derived projections maintained in
the same transaction as authoritative mutations. Standalone persists table,
range, and logical-resource rows together with a versioned revision record in one
storage-engine transaction. Create and restore publish table topology
and the logical binding together. Reopen and snapshot installation retain the
catalog. System catalog admission requires topology protocol version 5;
existing atomic table operations retain their version-3 gate.

Data nodes read the catalog directly from a remembered metadata endpoint,
without a preceding status RPC. Each successful read returns metadata group and
incarnation evidence. The reader validates it against its pinned identity and
uses bounded endpoint failover, a shared deadline, and cancellation. Mutations
retain the existing at-most-once forwarding and ambiguous-outcome rules.

Identity-only reads return table ID and physical name. Query binding optionally
includes only the selected tables' schema, active read schema, and index
definitions, captured in the same read transaction as their identities. Routing
and sort validation reuse this request-owned projection, including across
NDJSON lines and synchronous native join execution. Internal physical-table
queries use the narrow point-read contract when a prepared selection is absent.
The coordinator otherwise attaches versioned internal routing metadata carrying
its physical table ID/name and selected text indexes. Receivers accept it only
with a matching catalog route fence; storage admission still validates that fence.
Older peers ignore the optional header and use their existing preparation path.
Public JSON cannot populate this capability. Vector workers retain their own
retrieval index while sharing the coordinator's primary text-index selection.
Administrative snapshots remain available for whole-catalog topology consumers.
Compact binary decoding validates all record framing while copying only the
requested projection, skipping unrelated descriptions and restore metadata.
Metadata mutation planning uses the same transaction-pinned reader interface as
standalone's owned indexes. Ordinary create/rename/binding changes read their
names, identities, and dependencies directly. Namespace and database deletion
scan only the affected child scopes. Reverse indexes answer physical-binding
collisions and tablespace-use checks. The indexes update atomically with primary
records and share their rebuild/version boundary. Completion verifies compact
revision/hash metadata without rereading the catalog inventory.

Named database/namespace/tablespace reads return only their selected resources
and related labels. Listings scan covering kind/parent rows sequentially, avoiding
a separate primary-record seek for every result; related tablespaces are fetched
once. Covering rows are disposable derived records, maintained atomically and
validated/rebuilt with the other catalog indexes. Standalone owns equivalent child/name/ID/reference indexes.
Response formatting uses an index instead of repeated inventory scans.
Standalone updates only affected rows and their in-memory indexes.

A writable projection rebuilds name indexes from validated authoritative records
before its first catalog point read after open or snapshot installation. Read-only
open validates the persisted projection instead. A version marker, checked inside
each read transaction, establishes that both positive and negative lookups use a
complete projection. Raft apply maintains indexes atomically; snapshot install
invalidates the in-process verification and excludes all derived rows from the
snapshot. Reopen repairs missing derived rows; malformed or duplicate primary
records fail closed. This lifecycle proof relies on transactional mutation paths
and the storage engine's integrity checks. Negative and legacy unbound lookups
therefore use point reads without allocating or scanning unrelated catalog
records. First-use rebuild/validation remains proportional to catalog size.

Bounded exact-document candidate queries select only owning shards from the
request's pinned routing snapshot. Sorted range references support binary search
per key, and each shard receives only its own keys. They retain the existing
index-independent document-value execution path and route/generation fencing;
scored, graph, and hierarchy queries keep their existing fan-out semantics.

Table listings build an identity map and select scope, prefix, and authorized
tables before per-table status collection and public schema materialization.
The administrative snapshot remains the source of topology information.

Standalone owns name, ID, child-position, physical-name, and counted tablespace
reference indexes. A mutation clones only affected records and reserves index
capacity before changing them under the metadata mutex. Undo is allocation-free;
a successful durable commit releases the old records. Readers cannot observe
partially applied deltas. Legacy adoption retains physical names in the mutation
arena so replacement cannot invalidate a pending binding.

Local standalone stores catalog rows in an LSM directory beside the legacy file
(`local-metadata.json.store`), using the existing engine's WAL, recovery, and
compaction. Local commits sync the WAL before acknowledgement; they do not
repeat that sync after a successful commit. Lite uses its existing `system/metadata` namespace. Startup prefers
the versioned row catalog; without it, startup reads the legacy JSON file or Lite
`catalog` value. The first successful mutation atomically imports the legacy
state and publishes the new head. Subsequent DDL writes changed rows and the head,
not a full catalog checkpoint. The legacy input is retained but no longer
updated; downgrading after migration requires restoring a compatible backup.
Extension mutations snapshot their own extension section; unrelated table and
logical-resource inventories are excluded. Reopen validates row keys and rebuilds
derived indexes once. A commit/sync failure with an uncertain outcome fences
catalog reads and mutations until restart instead of claiming rollback.

Routing generations own compact table/range records and immutable indexes.
Eventual cache hits retain a generation instead of cloning the catalog and
sorting ranges again. Point routing and fence rechecks use the same capability.
Authoritative captures still cross a read barrier; indexes can be reused only
when the observed incarnation/revision matches exactly. A cache TTL never proves
absence. Active sessions retain their generation through cache invalidation and
publication; their storage identity and topology fences remain unchanged.
Standalone captures records under its metadata mutex and builds indexes outside
that lock, publishing the cache only if its revision is still current.

## Grants and row filters

Permissions use either legacy `resource` strings or a structured `table_target`,
never both. A legacy `resource: "*"` remains a global wildcard. A structured
scope without `table` means every table in that namespace; a structured scope
with `table: "*"` means exactly the table named `*`.

```json
{
  "resource_type": "table",
  "table_target": {"database": "analytics", "namespace": "serving"},
  "type": "read"
}
```

Internal policy keys use a reserved NUL prefix and length-framed components,
which cannot collide with a valid literal table name. Public string inputs
reject that prefix. API responses project keys back to structured targets.
API-key permissions intersect the credential's scope with the owner's current
permissions, including namespace scopes. Request-local physical aliases retain
their logical authorization target for live revocation checks.

Legacy row-filter maps remain literal. API keys can also supply
`scoped_row_filters`, each containing `table_target` and `filter`. Row-filter
management routes accept explicit `database` and `namespace` query parameters;
`all_tables=true` selects a namespace-wide filter. Exact table filters take
precedence over namespace filters, then the global filter. Literal `*` and a
wildcard scope remain distinct through storage, lookup, and removal.

## Restore jobs

Restore jobs persist immutable destination identities. Admission intent is a
bounded, URL-safe encoding of the structured target, allowing names with
slashes, spaces, dots, or the full table-name length. Replica bootstrap carries that destination namespace so even the first staged
import uses the new identity. The binding is published
atomically with restored topology. Repeated idempotent admission retains the
same destination identity.

Job listing and authorization share one request-owned catalog snapshot and a
physical-to-logical map. Renamed legacy tables are included in this projection.
Each new request refreshes the view; authorization uses canonical logical keys,
and response labels never expose those keys. Existing durable job execution,
leadership fencing, cancellation, and retention remain authoritative.

Cluster backup entries retain the immutable source storage name and a separate
structured destination target. Backup listings render those targets even after
the source catalog entries are deleted. Restore validates the source manifest,
recreates the logical binding, and refuses to redirect an overwrite to a later
reuse of the same name. Scoped destinations require their database and namespace
to exist; table restore inherits the destination's current placement defaults.
Native restores reassign identity only within an integrity-validated staged
generation before publication. Ordinary opens retain exact identity checks.
Repair and artifact-reprocessing job responses also project logical labels while
retaining physical identities in their durable state.

## Tablespaces and placement

Tablespaces are declarative placement policies. Effective precedence is table,
namespace, database, then native defaults. Create bodies can explicitly select
`tablespace_name`; explicit `num_shards` overrides inherited `min_ranges`.

Parent binding changes affect defaults for future table creation. Explicitly
changing an existing table's binding atomically changes its native placement
policy, and the normal reconciler performs the placement work. Clearing a table
binding reapplies inherited policy or native defaults. Standalone retains one
local replica; Lite retains its single-range constraint.

Tablespaces with references cannot be dropped. Renaming a tablespace preserves
bindings. `location_json` remains opaque metadata: it does not migrate files or
select a storage engine. A future physical-location feature needs its own
versioned storage and migration contract.

## Coherent listing and portable state

Table listing selects logical names and physical records in one metadata read
transaction, or under the standalone metadata mutex. Namespace and prefix
selection happens before loading full table definitions. Legacy default-scope
fallback checks binding absence in that same observation; a concurrent drop
cannot expose a private physical table as an unbound public table. Metadata
uses the table-to-range index and returns runtime reports for selected groups.
HTTP and MCP share this projection and authorize logical names before collecting
runtime status or materializing public schemas.

The API server retains immutable schema projections by a length-framed SHA-256
of the write and read schema bytes. The cache holds at most 256 entries and
64 MiB of owned arenas. Only finished projections are retained; compiler, parser,
and aggregation scratch is released after compilation. Leases
keep evicted generations alive until response serialization finishes. Index
incarnations, permissions, dynamic field observations, storage counters, and
replication status remain request data. Schema changes select a new entry;
renaming a table or using the same schema elsewhere can reuse an entry.

Portable HA topology version 4 captures logical catalog state, physical topology,
and extension records together. It preserves database/namespace names, immutable
table bindings, tablespaces, revision, and the next logical ID. Materialization
validates references and rebuilds derived indexes at the destination. Version 3
seeds without logical state remain readable; new exports require the explicit
coherent-export capability and cannot silently fall back to a physical snapshot.

Standalone delta application retains rollback capacity only until the change
finishes. Commit and undo then reclaim empty parent buckets, so repeatedly
creating and dropping tenants does not retain per-tenant child arrays.

## Clients and validation

Generated Go, Python, TypeScript, and Zig clients expose the scoped routes and
structured query/grant types. TypeScript callers can use `client.api` directly.
The CLI accepts `--database` and `--namespace` on table, index, query, lookup,
load, insert, delete, backup, and restore commands, with `ANTFLY_DATABASE` and
`ANTFLY_NAMESPACE` defaults. MCP table operations accept separate `database`,
`namespace`, and literal `tableName` arguments.

`zig/e2e/antfly/test_system_catalog.py` covers literal names, scoped joins,
namespace isolation, placement, rename/restart, index lifecycle, MCP, and
idempotent restore with maximum-length targets. Catalog cases in `test_auth.py`
cover scoped grants and row filters on standalone and split metadata/data
processes, NDJSON authorization, and literal-star permissions.

Focused Zig targets are `antfly-system-catalog-test`, `antfly-system-catalog-api-test`, and
`antfly-system-catalog-standalone-test`. Their regressions cover atomic publication,
stale revisions, reopen and snapshot installation, corruption checks, compact
allocation budgets, batched join binding, direct-read identity evidence, and
request-local authorization projections. Client checks use `cmd-test` and
`antfly-client-test`. The derived visibility deadline-clock regression remains
in the storage enrichment lane and protects the Lite timeout fix.

These contracts and optimizations do not depend on the optional M1–M3 refactors.


## Benchmark workloads

[System catalog benchmarks](zig/pkg/antfly/benchmarks/SYSTEM_CATALOG.md) document
the ReleaseFast catalog-scale target and disposable live-server workloads.
They cover tenant provisioning, scoped reads and joins, NDJSON reuse, concurrent
lookups, table listing and rename, tenant offboarding, and multi-node ingestion
through entity promotion and graph hydration. Measurements include workload and
binary provenance; timing thresholds are not part of correctness tests.
