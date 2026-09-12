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
catalog. System catalog admission requires topology protocol version 7;
existing atomic table operations retain their version-3 gate.

Catalog failures use the shared JSON `error` field plus a machine-readable `code`.
Resource mutations whose committed projection cannot be rendered return typed HTTP
202 with `status: "committed_visibility_pending"`; clients observe the resource
with GET rather than replaying the mutation. The OpenAPI contract and generated
clients preserve this outcome.

Scoped table creation returns HTTP 201 with `TableStatus`; default-scope creation
retains HTTP 200. Both scopes use the shared `CommittedMutationOutcome` for
accepted table mutations, including visibility, supersession and repair outcomes.
Scoped drop retains HTTP 204 on completion. SDKs preserve the actual status and
typed result; Rust's shared mutation decoder accepts both completed create codes.
The same immutable catalog binding routes document and relational storage. Packed
rows require no additional catalog identity or migration: table/database rename
changes logical bindings while preserving their physical row destination.

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
scored, graph, and hierarchy queries keep their existing fan-out semantics. Graph metric reads and reranking also use the general routing path. Metric maintenance actions resolve the same immutable destination through literal or scoped routes, require table admin permission, and continue to address that identity after rename. The shared HTTP router captures both fields in the colon-delimited metric/action segment before handlers decode names.

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


## Bounded inventory and schema definitions

Public table lists retain the array response and complete-list behavior when
`limit` is absent. Clients can request 1–1,000 catalog rows and follow
`X-Antfly-Next-Cursor` with `cursor`, using the same database, namespace and
prefix. A continuation without an explicit limit defaults to 100. Pages use
bytewise logical-name order. Authorization is checked on every page; an empty
page can still carry a continuation. Default CORS configuration exposes the
continuation header; custom `exposed_headers` must include it for browser clients.
Cursors contain an opaque table identity,
not the private name of a filtered row. They confer no authorization.

Each page captures definitions, ranges, placements, store headers and selected
group reports in one metadata transaction (or the standalone metadata lock).
Logical catalog changes invalidate a continuation with HTTP 409. An order-independent SHA-256
membership fingerprint also detects unbound legacy table creation, deletion or
rename and is rebuilt from primary identities after restore. Runtime counters
can change between pages; pagination is not a retained historical snapshot.

Metadata uses ordered logical-child and legacy-identity indexes to seek directly
to the requested prefix/keyset boundary. It loads full definitions only after
merging and truncating those candidate streams. Compact store headers exclude
both group-summary and detailed-runtime arrays. Selected definitions use sorted
batch reads. Runtime selection seeks each selected group's actual reporters,
avoiding a probe for every store/group combination. Logical result order is
preserved independently of storage key order.

Store reports have a normalized local primary representation: one compact
header and stable per-group slots in bounded 64-group pages. Runtime payloads,
runtime clocks, group facts and group clocks occupy independent pages. A compact
sorted directory lists live pages; each 64-byte membership entry stores the
group, slot, runtime digest and group/runtime observation counts. Group fact and
clock changes leave runtime payloads and their membership digest untouched.
Reporter indexes map selected groups
to actual stores and slots; fixed page directories locate only the selected
component record without decoding adjacent reports. Component entries have a
local codec version and contain group facts or runtime observations directly;
they do not repeat store headers. Grouping partitions contiguous report buffers
while preserving duplicate order. Hydration decodes into caller-owned memory
with separate scratch storage, without a second deep copy. Deleted slots are reused, so
ordinary group churn does not renumber or rewrite unrelated pages. Structural
SHA-256 runtime digests exclude observation clocks and include the reporter incarnation.
Cached reports update only changed headers; fresh observations update clock pages
without re-encoding unchanged runtime payloads. Sparse changes rebuild only affected
pages, copying the unchanged encoded members. Full status changes still require work
proportional to the incoming report. Duplicate observations remain distinct so
reconciliation can reject ambiguous evidence. Whole-store consumers deduplicate
and batch-read pages and reconstruct the normal owned StoreRecord. Metadata uses
the existing immutable LSM block/index cache with a 64 MiB retention budget per
apply store (`block_cache_bytes`, zero disables). Active transaction leases can
temporarily exceed retention; the cache is released after the backend closes.
With caching disabled, nearby report keys reuse a bounded cursor instead of
reloading the same block for each row.

These primary rows commit together. Legacy full records migrate atomically;
rebuilding derived indexes reconstructs reporter references without removing
normalized primary pages. Raft
snapshot export reconstructs the existing full-record wire format from one read
transaction. Snapshot installation removes the replaced group's local report
rows together with its old headers; other metadata groups remain intact. The
local format is versioned separately from the logical snapshot wire
format. Directly opening a normalized data directory with an older binary is not
a supported downgrade path; use the compatible logical snapshot format.

Compatibility is required for formats shipped on `main`, including full store
records, the standalone catalog input, and applied-batch watermarks. Intermediate
catalog layouts introduced only during this unmerged PR are not upgrade inputs.
There are no migrations between those development layouts; recreate disposable
development data when changing between them.
The logical catalog JSON reader also serves current HA seed import, including
the new catalog resources. That active restore contract remains supported; it
is not a migration between development layouts.

Cached store heartbeats may reference committed runtime observations by exact
reporter incarnation and status generation. A separate internal heartbeat endpoint
and a full-report response capability header negotiate support; all metadata voters
must pass the catalog protocol readiness fence before proposing the new command.
Missing support or a mismatched base triggers a full report. Apply checks the fence
again in its write transaction and preserves existing observation clocks. Current
per-group Raft facts still travel with every heartbeat. Changed observations use
full reports. The reference must preserve group IDs and duplicate multiplicity;
inventory changes require a full report. Admission reads only group facts and
the header. Apply retains runtime pages without decoding, hashing or rewriting
them. Reference work remains proportional to that store's groups, independent
of the size of its unchanged runtime/index payloads.

Current reporters negotiate the sparse report endpoint
`POST /internal/v1/nodes/{store}/status/update`, gated by topology protocol 8.
The owner retains acknowledged per-group leaves and sends complete replacements
only for changed groups, with explicit removals for retired groups. Duplicate
observations keep their order within each group. A cursor identifies the reporter
incarnation, monotonically increasing request sequence and SHA-256 of the exact
HTTP request. A delta names its exact acknowledged base; both admission and apply
check that fence. Responses acknowledge committed state, so an exact retry after a
lost response is idempotent. Stale bases cause a full-inventory repair, never a
best-effort patch. Unsupported peers use the existing full/reference endpoints.
An empty delta with an unchanged header returns the existing applied cursor,
including for telemetry-only requests; acknowledging a transport sequence alone
does not require a Raft entry.

The acknowledged baseline retains the last transmitted observation clocks for
unchanged groups. Local clock coalescing therefore cannot keep delaying the
periodic freshness update. Preparing a report owns only changed leaves and reserves
commit capacity before network I/O; errors leave the prior leaves intact.
Admission reads selected report components through covering group references.
Raft command 56 carries the durable update and request digest; volatile embedding
activity travels separately in the HTTP envelope and is checked against committed
identities before entering the activity cache. It does not enter the Raft command.
Apply preserves untouched component pages. It still scans compact membership
records to maintain the page directory; sparse apply is not independent of the
store's group count. Cursor and component updates commit in the same transaction.
Ordinary store replacement invalidates the cursor. Reopening preserves it;
installing a logical snapshot discards it and requires a full report.

Committed projection notifications coalesce changed store IDs and separate group
and runtime change flags in a bounded, allocation-free queue. Refcounted immutable
store snapshots own separate group and runtime leaves. Header updates share both;
reference heartbeats replace group facts and share runtime observations. Retained
admission leases survive publication, deletion and snapshot replacement. A store-ID
index selects reporting stores under the runtime lock. After releasing the lock,
admission borrows their pinned records and compares each observation once.
Only accepted replacements allocate owned report payloads. Repeated reports for
one store see the preceding accepted candidate and produce one final proposal;
stale generations cannot overwrite that candidate. Capability
counts update when a store is replaced or removed; unchanged runtime capabilities
are retained with the runtime leaf. Protocol admission therefore retains global
requirements without scanning every store's indexes. Repair identity comparisons
use temporary hash indexes over borrowed reports, preserving first-match and
causal fencing semantics in linear expected time. The admission plan reuses its
prior repair index when preserving committed facts across protocol gating.
Allocation failure releases candidates without modifying pinned observations.

Other projection collections refresh only when their own kind changes. Overflow,
snapshot replacement and failed refresh force a full rebuild. Local reconciliation
retains the captured immutable catalog generation and store leaves. Transition
readiness retains store leaves as well. Public owned snapshots clone their large
payloads after releasing publication locks. Smaller workflow progress collections
still use owned copies. Data-owner report collection builds table-ID and group-ID
indexes over the same captured inventory; group lookup no longer repeatedly scans
all tables and ranges or resolves a newer catalog generation.

Raft transport batches ready heartbeat and heartbeat-response messages only when
destination, source identity, protocol, address and endpoint metadata match. Frames
cap at 256 groups, 1,024 messages and 1 MiB (a single group's existing size contract
still applies). No timer or deduplication changes consensus evidence. The codec
transport owns retries, including asynchronous HTTP failures, and resolves each
group's current route. Route changes invalidate unsent HTTP bundles. In-flight
requests finish their admitted attempt; failures return to the transport. Codec
retry retention is bounded by 4,096 frames and 8 MiB. The HTTP driver independently
budgets queued, in-flight and failed-completion bytes before copying, with a default
of four maximum-size requests globally and one per peer, each including 64 KiB of
routing overhead (128.25 MiB globally and 32.0625 MiB per peer). Frame caps also
include in-flight and failed completions. Failed completion ownership transfers
back to the codec; its budget releases on delivery or transfer. Retained byte/frame
gauges expose HTTP pressure. Raft retransmits work dropped on budget/attempt
exhaustion. Retry draining stably compacts survivors in one linear pass. The HTTP
sender keeps per-peer FIFO queues and an intrusive ready-peer list. One request
per peer is in flight; completion returns that peer to the end of the ready list.
Workers wait on the ready predicate with a condition variable. Route invalidation
scans only that peer and requires no allocation to remove its queued frames.
Append, vote and snapshot scheduling retain their existing path.

Metadata apply commits a versioned 26-byte checkpoint in the same transaction
as projected records. It contains the applied index, input kind (committed entries
or snapshot), and input byte count for diagnostics. It is an apply watermark,
not a state hash or quorum proof. Raft owns replay entries; the metadata store
neither duplicates nor retains the full last batch. `latestCheckpoint` returns a
value under the apply mutex. Legacy index-plus-batch rows remain readable and
convert on the next successful apply or snapshot installation. Unsupported
checkpoint versions and malformed records fail closed. Snapshot preparation
checks the checkpoint index inside its pinned read transaction; logical snapshot
wire projections are unchanged. Placement drain admission reads only the store
header's node identity and drain flag; termination-debt checks still read reports.

Standalone maintains ordered namespace/name and table/range indexes in the
same durable transaction as catalog mutations. It rebuilds those derived rows
once after startup, seeks only the requested page, and copies selected records
into an owned arena under the mutex. Serialization runs after releasing that
mutex. Selected range prefixes are visited in storage order, reusing one cursor
and bounding unrelated skips before the next seek. Logical table result order
is unchanged. Rollback and ambiguous-durability fencing cover index changes too. The
owned standalone catalog retains immutable index blocks in an 8 MiB cache;
borrowed stores, including Lite, retain their owner's cache policy.

Single-table reads resolve logical identity and capture status together behind
one Raft read barrier; HTTP and MCP use the same operation. Create acknowledgements
use its physical-identity form. All share the immutable schema cache. Labels are applied before encoding. Fresh
runtime evidence is merged on reads; acknowledgements do not wait for runtime
coverage. Detail captures include selected replication-source checkpoints, errors,
and action hints. Artifact enrichment summaries are typed before final encoding;
producer configuration is removed before constructing public enrichment values,
so detail responses need no full-response JSON parse/redaction/encode cycle. Cache admission weighs recent frequency against retained bytes, so
one-pass inventories cannot replace equally useful residents. Concurrent misses
for the same definition share one compilation. Retention remains limited to
256 entries and 64 MiB, excluding active leases and compilation scratch. Access
frequencies decay to allow the working set to change. Inventories wider than the
cache still pay for nonresident schemas and response serialization; bounded pages
control individual response work, not total inventory cost.

The Rust SDK generator adapts operations with heterogeneous JSON success bodies
to private typed unions. A completed resource and `committed_visibility_pending`
remain distinct variants, and `ResponseValue` retains the HTTP status. This is a
Progenitor input adapter; the public per-status OpenAPI contract is unchanged.

Resource mutation results carry a typed projection captured during admission:
revision, resource ID, and its database/tablespace labels. Both standalone and
Raft paths serialize that projection before committing/proposing, then return it
only after commit or exact receipt verification. HTTP renders that result without
a second name lookup or read barrier. Concurrent rename, drop, and name reuse
therefore cannot substitute another identity in an admitted mutation response.
An unknown receipt still returns the existing ambiguous outcome contract.
