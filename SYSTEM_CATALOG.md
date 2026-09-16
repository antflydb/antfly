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

Transaction admission binds all read-set and mutation targets in one catalog read.
The request retains logical labels separately from physical table identities.
Row-filter reads and distributed participants use the physical bindings; responses
and permission checks retain logical names. Staged requests and savepoints persist
server-authored bindings in their private session records. Public transaction JSON
cannot supply those bindings. Native standalone persists sessions in a reserved
storage-engine namespace, just as Lite does inside its file, so restart followed
by rename or name reuse cannot redirect a staged write.

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

Internal catalog reads fence metadata identity before and after the authoritative
read through a dedicated group/incarnation projection. Identity verification
never constructs a diagnostic status snapshot or enumerates tables, ranges,
stores, or indexes. Missing identity capability requires an upgrade; an identity
change fails the request with an availability error.

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

Whole-instance hot standby table creation logs the physical table, owning ranges,
and logical catalog delta in one WAL record. Local publication and standby apply
use the same row journal, so a public name cannot be acknowledged without its
binding surviving failover. Duplicate-create responses require the standby's
catalog frontier acknowledgement. Startup accepts the table-create records
already shipped on main; new records include the catalog revision fence and
binding. Other catalog mutations remain subject to the hot standby mutation
policy.

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
An empty delta with an unchanged header returns the existing applied cursor;
acknowledging a transport sequence alone does not require a Raft entry.

Once every protected metadata member demonstrates protocol 9 support, command 57
persists the activated version with the cluster incarnation and membership
fingerprint. Elections and reopen reuse that proof; a membership change requires
fresh validation. Protocol 8 members still support sparse reports, but cannot
admit the new activation command until all members advertise its decoder.
The activation row is included in Raft snapshots. Proposal
admission rechecks the observed term and membership under the catalog gate before
committing activation. Upgrade-required errors retain their typed runtime ABI and
HTTP 426 contract rather than becoming an untransportable runtime failure.

Report failover pins its endpoint order for the entire attempt. An authoritative
base-mismatch response returns directly to the publisher so it can send a full
inventory; later follower responses cannot replace that recovery signal. This is
required for replica retirement and draining as well as ordinary lost baselines.

The acknowledged baseline retains the last transmitted observation clocks for
unchanged groups. Local clock coalescing therefore cannot keep delaying the
periodic freshness update. Preparing a report owns only changed leaves and reserves
commit capacity before network I/O; errors leave the prior leaves intact.
Admission reads selected report components through covering group references.
Raft command 56 carries the durable update, request digest and the admitted
header/cursor precondition. Apply compares both before writing, including for full
repairs, so concurrent registration, header changes or report publication cannot
silently overwrite the observed state. Reporters use bounded per-store admission
lanes. A shared catalog gate protects admission/proposal ordering; it is released
before waiting for Raft apply. Different stores and unrelated table DDL can make
progress concurrently. Membership and protocol changes retain exclusive ordering.

Volatile embedding activity uses a separate bounded outbox and background delivery
job. The durable publisher commits its acknowledged baseline and enqueues counters
without waiting for their delivery. One collection may be in flight and one is
pending. New pending collections coalesce by exact group/index/coverage identity,
retaining unsent observations for quiet indexes. An in-flight collection completes
so frequent updates cannot starve its tail. Owner shutdown drains the delivery
worker before freeing its payloads. Failed delivery marks activity dirty for a
later fresh collection and cannot fail a durable heartbeat.

Each outbox generation retains at most 16,384 samples and 4 MiB of sample structs
and names, plus bounded arena/container overhead. Larger inventories rotate through
successive windows. HTTP batches contain at most 512 samples; names and kinds are
bounded to 1,024 bytes each. `telemetry_only` requests cannot contain durable group
changes or removals. They use locally committed owner/index identities and sample
fences, without a read-index barrier, catalog admission gate, or durable cursor
comparison. Their response echoes the delivery base; it does not acknowledge a
new durable report. The active leader pins immutable group leaves, validates index
identities and coverage generations, and updates its volatile cache. Activity never
enters Raft. Full inventory upload JSON still has the normal HTTP body limit;
bounded telemetry and paged downloads do not change that contract.

Sparse apply locates affected pages through group references. It reads/rebuilds
only those membership and component pages. A free-space bitmap per page supports
insertions and reuse after removal; allocation metadata changes only with the
inventory. The small live-page directory changes only when a page becomes empty
or live. Full repairs still walk the inventory. Cursor and component updates
commit together. Cursors are logical replicated snapshot state, preserved through
snapshot installation and normalization as well as reopen. Ordinary store
replacement invalidates the cursor. Snapshot-plus-log-replay must produce the same
state as uninterrupted application, without requiring a new full report.

Committed notifications retain affected group IDs through commit and coalesce up
to 32 IDs per store before falling back to a full refresh. Refcounted immutable
report payloads are owned per group; a sparse cache publication loads changed
components and retains untouched leaves. Header updates share both components;
reference heartbeats replace group facts and share runtime observations. A
persistent radix tree copies at most 17 small nodes for a changed 64-bit group ID;
counts and capability summaries update along that path. Sparse publication neither
rebuilds a group map nor sorts or flattens the whole inventory. Consumers requesting
a flat StoreRecord array materialize it once per component under its own mutex.
That O(G) read cost is measured separately from publication. Retained admission
leases survive publication, deletion and snapshot replacement. A store-ID
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


## Bounded metadata control reads

Data-node control reads retain table/range identity, peer headers, placement and
transition state, and group facts referenced by ranges, placement intents, splits,
or merges. They do not fetch peer index diagnostics. The control projection pins
the same immutable catalog and store components used by reconciliation. Public
administrative consumers use a separate diagnostic cache, so compact control reads
cannot hide remote index status.

`POST /internal/v1/snapshots/read` transfers an immutable encoded view in pages of
at most 512 KiB. A token names one capture; the client checks token and total size
on every page and publishes only the fully decoded view after authority checks.
Concurrent control snapshot cache misses share one refresh per data node and
recheck the catalog generation after waiting. Each waiter retains its own deadline
and cancellation; authoritative snapshots keep their separate publication fences.
Transfer capture admission retries at most eight times under the original caller
budget and bounded Retry-After delay. A known transfer token is released using an
independent one-second cleanup budget even when its caller expires or cancels.

Control and diagnostic transfers use independent admission lanes. A completed
client releases its token; abandoned tokens expire after 30 seconds. Control views
have a 16 MiB ceiling and 128 MiB lane budget; diagnostic views have a 64 MiB ceiling
and 96 MiB lane budget. The aggregate encoding budget is 224 MiB; the control
lane supports up to eight concurrent maximum-size reservations, accommodating
background reader fan-in independently of diagnostics. Before collecting a view,
admission reserves its maximum size and one of the lane's bounded slots.
Diagnostic callers cannot use control slots or its byte budget. Reservations are conservative:
a second diagnostic capture can receive 503 even when its eventual encoded size
might fit. The encoder sizes its output before allocation; oversized views return
413. Failed and canceled captures release their reservations.

The store report cache and the placement/progress cache publish under separate
locks. Report projection, materialization, and diagnostic cloning do not hold the
Raft runtime mutex. Control reads fetch store headers and only the authoritative
group facts through the covering index, without loading runtime index payloads.
The commit listener records changed group IDs in a shared, allocation-free journal
of 8,192 entries. The old 32-groups-per-store cliff is removed. Journal overflow
invalidates only the affected store; allocation and deduplication run in the reader.

Clients still assemble the complete bounded view, and full diagnostics remain
inventory-sized. A changed or expired transfer restarts as a whole; pages from
different captures are never combined.

### Resumable full store inventories

Protocol 10 adds `POST /internal/v1/nodes/:node_id/status/baseline`. Prepare creates
an invisible generation identified by reporter incarnation and sequence. Ordinary
chunks contain complete replacements for at most 64 groups and 1 MiB of canonical
report JSON. Up to eight consecutive chunks share one ReadIndex barrier and one
Raft entry, capped at 1.5 MiB of report data and 2 MiB for the HTTP/replicated envelope.
The generation remains limited to 4,096 logical chunks and 512 MiB of report data.
Smaller reports retain the ordinary sparse endpoint.

A group larger than one ordinary chunk uses 512 KiB canonical byte fragments,
base64 encoded on the wire. The full-group digest, group identity, offsets, and
fragment digest chain fence assembly. Each fragment persists independently and
survives snapshot installation. Only the last fragment materializes the complete
group, once for admission and once for atomic apply; no growing prefix is repeatedly
parsed or rewritten. Group materialization is bounded by the generation byte limit
and admitted one at a time
per metadata service. It is proportional to that group's size, not constant-time.
Admission stays inside the storage owner, including compiled storage, returning only
the required runtime-status protocol version. The final fragment command contains
only that fragment; it does not retransmit the assembled group.

The control owner performs registration and Raft placement reconciliation, then
transfers an owned metadata snapshot and a local ownership-generation fence to an
independently reserved reporting worker. The worker collects group/index facts,
observes capacity, prepares the update, and publishes it. A changed ownership
generation rejects the observation before publication. The idle/capturing/ready
handoff permits one collection or pinned generation at a time; concurrent dirty
notifications survive collection for a subsequent report. Placement annotation
indexes local intents once rather than scanning every intent for every group.
Control rounds skip collection while work is pending, leaving maintenance
scheduling available during slow collection and uploads. A baseline worker quantum has a shared
two-second transport deadline across discovery, requests, and retries, plus a
32-request ceiling. Cached heartbeats use the same reserved worker and a shared
two-second deadline; the control thread only fills the bounded work slot. Ordinary
report requests retain their per-request deadlines and observe worker cancellation.
Schema-progress batches share a two-second transport budget per collection; the
next acknowledged snapshot resumes the remaining records. Shutdown cancels
transport and joins the worker before stopping its Raft/storage providers or
releasing its owner. Ordinary payloads remain borrowed from the pinned generation; exceptional
large groups retain immutable frame bytes. Transport failures resume from replicated
progress, including after leader changes or lost responses. A rejected header/cursor
fence discards the generation and schedules fresh collection. Unsupported requests
retain the plan and retry after a bounded pause. Permanent size or validation
rejections release the pinned plan and prepared inventory, clear the diff cursor,
and back off for 30 seconds before a fresh collection. The control owner sees a
retryable `StoreReportInventoryRejected` signal; the worker logs the original
cause. A subsequently reduced inventory can recover without restarting the node.

Retained-runtime heartbeats compare only structural group rows. Each acknowledged
runtime leaf owns an immutable arena shared by reference between the acknowledged
and prepared structural rows. Abandonment releases the prepared reference; commit
releases the old structural row. Runtime-only groups remain retained. A heartbeat
that adds or removes structural groups fails its base fence and requires fresh
collection. Ordinary full observations still compare and replace runtime leaves.

Schema migration readiness builds table, range, and local-runtime indexes once per
observation, then evaluates every hosted range. Missing or non-authoritative
runtime observations still withhold readiness. Ready records are compared against
the captured replicated schema-progress records: equal versions generate no
request, and missing acknowledgements after failed delivery or restore are resent.
`POST /internal/v1/schema-progress/batch` accepts 1–64 distinct table records for
one node, with a 16 KiB body limit. Protocol 10 command 59 applies the batch in one
storage transaction and the endpoint acknowledges only after apply. Exact record
replay skips writes and projection notifications. The existing single-record
command remains decodable; new reporters use the bounded batch endpoint.


HTTP admission computes each chunk's canonical digest once and reuses it for replay
checks, the binary command, and the application receipt. Command 58 carries admitted
facts and the binary store codec. Batches apply in one storage transaction. Chunks
write normalized generation-addressed pages without invalidating the visible report
cache. Activation checks the complete digest chain, byte count, chunk count, and
original header/cursor fences, then swaps the active root, compact header, and
acknowledged cursor in one transaction. No partial group or inventory is visible.

A store retains at most active, pending, and retired generations. Collection reclaims
at most one retired fragment and one page (at most 64 covering references) per
subsequent report or prepare step; activation stays constant-size. Completed-group temporary fragments
are removed when that group is installed. Logical snapshots retain active inventory
and pending upload state/pages/fragments, exclude retired data, and normalize the
active generation on install. Derived-index reconstruction also handles a generation
whose only durable content is an incomplete first group.


### Compiled storage ownership

Native HA seed capture prepares the physical owner before the exclusive mutation
freeze, then captures through that same leased owner. The frozen phase never
opens another owner or resolves catalog metadata. Inline and compiled execution
share the storage snapshot implementation and its durable artifact copy. Retryable
capture contention has a stable ABI identity so the admin route can return 503.
HA identities, LSNs, and counters retain their declared unsigned OpenAPI widths
through generation, request validation, owner responses, and CLI rendering.
Valid 64-bit physical identities must never be narrowed to signed integers.


Catalog operations cross the storage boundary as complete requests: admission,
qualified resolution, scoped listing, export, and targeted report reads each
retain one storage transaction in the owner. Control code receives owned values;
it never reaches through the boundary to a backend cursor. The apply progress
contract carries a fixed-size checkpoint, without retaining replay bytes in the
client. Sparse projection notifications preserve changed-group IDs and separate
group/runtime invalidation flags through the synchronous callback boundary.
Query execution options carry the catalog's public response label through both
compiled archives. The label is borrowed until serialization completes and never
selects a storage owner or grants authorization; responses own their encoded
names. A shared options adapter keeps the two query entry points consistent.

Portable HA seed catalog validation lives with the storage-free seed topology
contract, so both distributed capture and physical materialization enforce the
same identity rules. Restore publication resolves the current destination group
from the catalog; the manifest's source group selects the backup artifact.

The compiled boundary retains all status strings before response arenas are
released. Bounded status vocabularies are re-interned, including enrichment,
projection, repair and schema lifecycle states. Graph traversal retains physical
edge provenance only when paths are requested, sharing ancestry while queued.
Native restore checks catalog compatibility after primary transfer and validates
physical projection checkpoints after their artifacts are installed, before the
restored generation is published.


### Report scheduling and migration readiness

A data node with Raft refreshes cached live Raft facts at the existing reporting
cadence. Raft presence alone does not force runtime inventory collection. Dirty
observations and expired local group snapshots still select a full collection;
leadership, membership, and ownership changes still invalidate the caches. Cached
heartbeats carry the local ownership generation and check it before publication.
A rejected base is a `StoreReportRepairRequired` control outcome: the worker clears
the cursor, marks inventory dirty, and backs off, while the control loop remains
available to schedule a fresh report. Unknown-store recovery still re-registers.

The heartbeat cache retains the publisher's acknowledged immutable runtime leaves.
A small ordered snapshot preserves duplicate runtime rows without copying index
arrays. Leaf references are atomic because cache invalidation and publisher
replacement can release them on different owners. Cache replacement swaps ownership
under its mutex and destroys the old snapshot after unlocking. The fallback for
peers without sparse reporting retains the existing independently owned report.

Migration finalization builds one readiness index for the reconciliation snapshot:
range-to-table ownership, distinct hosting nodes, and exact table/node/schema-version
acknowledgements. It deduplicates hosting nodes across shards and keeps tables with
no hosts or missing acknowledgements unready. Only migrating tables need schema
version parsing. The same index serves every table in the round, removing repeated
placement/range scans from the metadata leader's cutover path.

Runtime observations address the exact local physical group through the storage
boundary. A remote sibling shard cannot suppress a local observation, and a busy
owner yields an unavailable observation so the existing cached/synthetic fallback
can serve the control loop. Allocation failures remain errors. Live Raft apply
indexes refresh with heartbeats so drain and relocation fences observe catch-up
without requiring an index-inventory rebuild.

An in-progress schema migration admits local full-text reconstruction on every
hosting replica. A node-local fair queue targets the exact schema-version index,
rotates pending shards, and retains completed proofs for the table, schema/index
contract, physical root, and local ownership generation. Completed proofs expire
after 60 seconds and are rechecked on the next maintenance scan while the migration
remains active. Discovery scans routes once; completed prefixes no longer reopen
their physical owners. A shard rotates only when its turn starts, so a pass that
exhausts its budget preserves the priority of unstarted selections. Pending migration
work wakes at 100 ms intervals and admits up to 16 groups within a 100 ms pass budget.
Both the schema and ordinary repair queues use the same 25 ms reconstruction
quantum, so ordinary repair cannot bypass migration fairness. The cooperative
deadline is checked after a bounded
page (at most 256 documents and 256 KiB source data, except a single large document).
These are scheduling budgets, not hard wall-clock bounds on storage I/O.

Background structural admission takes an exclusive lease only when idle, without
installing writer preference behind foreground readers. A configured repair reuses
a shared lease only when its table, schema, indexes, storage policy, root and target
match the retained configuration proof. Structural follow-up invalidates that proof.
The scheduler translates its remaining quantum into the catalog's clock before
descriptor lookup (awake and native monotonic epochs differ on Darwin), then checks cancellation
and yield before admission. Shared generation ownership permits foreground reads
and Raft apply while preventing concurrent reconfiguration. The compiled boundary
carries borrowed cancellation, yield, activation and resource policy for the
synchronous call. Cold-owner retention remains an explicit, independent choice.
Standalone supplies the same bounded point projection: it retains the indexed
routing generation, verifies its revision under the metadata mutex, and copies
only the requested table's complete definition and ranges. Budgeted repair never
falls back to a full administrative snapshot.

Full-text candidates stage every segment produced by a bounded page. One candidate
metadata transaction publishes all active segment markers and the source cursor;
files become durable before that transaction. The separate repair intent may lag
this cursor after a crash. Reopening resumes after the candidate's committed page,
without scanning and deleting IDs from the accumulated prefix. An aborted page
publishes neither segments nor its cursor. Catch-up still replays writes from the
pinned build floor before bounded activation. Normal bulk text
loading keeps its larger throughput-oriented batches. Exact repair selection uses the
resident name index, leaves the general repair cursor unchanged, honors paused and
future-dated intents, and excludes unrelated repair debt from the target's readiness.
Ordinary repairs retain their existing leader admission. Busy runtime observations
retain their typed errors across the compiled boundary; the control owner preserves
refresh debt before using cached or synthetic facts.

Replicated foreground reads retain one request identity through ReadIndex
retransmission. Missing quorum responses retry with 100 ms to 1 s backoff under
the original five-second maximum; retries stop after the first valid quorum proof.
The read still waits for that replica's applied index. Duplicate responses cannot
move the apply target or revoke completion, and retired/cancelled waiters do not
retransmit. Deadline exhaustion uses the compiled callback's canonical timeout
classification. The production three-node simulation drops the first forwarded
request and checks recovery without an application retry.

Catalog routing timeout, unavailability and projection-refresh errors retain exact
identities across compiled write callbacks. The public handler can therefore keep
pre-proposal admission failures distinct from an unknown Raft or transaction commit
outcome. These stable ABI details do not authorize replay of ambiguous writes.


### Foreground write validation

Scoped tables expose schema replacement (`PUT`) and JSON Merge Patch (`PATCH`)
at their namespace-qualified `/schema` path. Both delegate to the same schema
mutation authority, ETag/version checks, and migration flow as literal table
routes. Generated Zig, Go, TypeScript, Python, and Rust clients expose these operations.
The Rust generator uses the shared status-directed mutation decoder for both
scoped schema methods, preserving completed and committed-pending outcomes.

Write admission reads an immutable table-specific projection containing only the
schema and applicable extension document/row data shapes. The metadata owner uses
its physical-name index and table-to-extension-member index in one read transaction;
large index definitions, unrelated tables, placements and diagnostic inventory are
not materialized. Standalone maintains a membership-generation index for the same
constraints. Extension add, move, replacement and removal invalidate membership.

Remote nodes cache at most 128 projections of 256 KiB each. Table-keyed admission
coalesces cache misses with cancellation and deadlines. A shared revision probe is
a point read of the durable catalog revision, including extension membership and
schema changes, excluding heartbeat traffic. Local mutations invalidate immediately;
remote changes follow the existing one-second metadata cache freshness window.
Snapshots crossing local mutation invalidation are discarded. Large projections
remain valid but bypass cache retention. Validation never falls back to diagnostic
snapshots on production sources or drops extension constraints on overload.

Forwarded batch ingress establishes one absolute local deadline before decoding.
Validation, catalog lookup, writer admission and further forwarding consume it.
Clock translation preserves the remaining budget instead of restarting it after
validation. Cancellation or expiry before proposal carries the not-proposed outcome;
post-proposal uncertainty and committed-but-pending visibility retain their existing
explicit outcomes.

### Read peer routing

Remote document reads select endpoints from an immutable index derived from the
accepted control snapshot. It contains healthy node API URLs, readable placement
membership, and merged leader hints, keyed by node and group. The shared control-read
generation also owns the compact join-planning view described below. Neither view
retains table schemas or diagnostic inventory. Serving/draining relocation
rules remain identical to the peer-aware placement rules.

Control snapshot publication replaces the index under the same incarnation and
mutation fences; retained readers finish against their original view. Invalidation
removes the current view immediately. Cold refreshes share admission under the
request’s remaining deadline and cancellation token, translating between the
request and metadata clocks without extending the budget. They reuse the control
snapshot refresh path and return a
retained peer view directly; they do not clone a full control snapshot merely to
discard it. Replaced snapshots are freed after releasing the publication lock.
Peer freshness starts at publication, so a slow capture does not arrive already
expired. They do not request independent diagnostic snapshot transfers.
Single-group routing and each parallel or sequential search fanout phase retain
one view for leader, placement, and endpoint selection. Each fanout resolves all
its destinations before dispatching shard work. Missing-document fallback uses
readable peer IDs from the router, avoiding another administrative inventory capture.

Exact-group queries issued by join coordinators use the same hosted routing
adapter before entering local read admission. Remote typed-result probes defer
to the fenced HTTP query path; local destinations bind their selected route and
execute through the resident storage owner's routed callbacks. Worker admission
must never attempt to open a foreign shard on the coordinator.

A local serving leader requires no endpoint discovery. A local Raft member's
leader observation takes precedence over metadata hints; a non-member's retained
Raft hint does not. Routing hints never replace the destination's topology fence,
placement checks, or requested read-consistency barrier.


### Join planning and deadline ownership

Public joins pin an authoritative routing session before reading either side.
Index lookup, broadcast, and shuffle select groups from that session, and the
read adapter carries its fences to local admission and remote workers. A split
after selection must fail the old fence; a split before selection is included
even when advisory observations still describe the donor's former range. A
finalizer that fans out further compares its new session against the topology
admitted by its coordinator and rejects a mismatch.

Joins also acquire one immutable planning generation lazily for cost estimates.
Background jobs do not retain request-scoped pointers; resumed work acquires its
own routing session and planning observation. The planning generation owns
only names, table identities, range boundaries/identities, group IDs, aggregated
row/byte estimates, and document-identity readiness. Name lookup is indexed and
authoritative point-key routing searches sorted table-local ranges. Missing shard reports leave
statistics explicitly unknown rather than treating missing data as zero.

Data nodes publish this generation with the peer-routing index when accepting a
control observation. Warm acquisition retains it under the cache mutex without
copying catalog/schema data. Cold acquisition shares the existing control refresh,
including its deadline, cancellation, singleflight, incarnation, and invalidation
fences; it never requests the paginated diagnostic snapshot. The cold control
transfer still includes the existing control-plane inventory. Projection build
cost is paid at publication, not by every planning phase or table lookup.

Embedded metadata sources publish retained routing and planning generations with
their immutable catalog projection. Warm acquisition does not copy or reindex
the catalog. Old request leases survive replacement; authoritative acquisitions
still cross the metadata read barrier. The local runtime-statistics adapter
supplies estimates. Query planning has no production fallback to administrative
snapshots. Statistics guide costs only; destination workers validate topology
and document identity.

Lookup joins partition left rows by owner once before preparing requests. Shard
reads run in batches of at most eight on the server's I/O executor, with isolated
result arenas and deterministic merging in catalog order. All launched work is
drained before returning an error or releasing the routing lease. Request
construction stays serial because it shares the request's resolver. Graph phases
remain serial to honor their request-wide retained-state budgets.

An admission deadline always travels with its clock authority. Narrowing a fence
first converts the incoming remaining budget into the fence's clock, then takes
the earlier deadline. Lookup, coordinator, and worker paths preserve this pair;
clock translation never refreshes an expired deadline or extends a tighter fence.
Finalizers retain both the caller's cancellation and the admitted worker's
cancellation for their entire fanout. Durable job stores retain runtime services
and clocks, but clear borrowed request deadlines, cancellation, labels, resolvers,
and catalog leases. Wire-visible shard query durations use the executor clock so
local and hosted responses have the same timing contract.
Forwarded system-catalog requests likewise keep the listener's executor clock
through admission and preserve any earlier ingress deadline. The concrete
metadata-service adapter translates the remaining budget to its native CPU clock;
transport-neutral adapters retain the original clock capability.


Offline vector-storage migration shares standalone's durable catalog row keys and
exclusive operator lock. It resolves public names through catalog bindings,
updates the selected table and catalog epoch in one WAL-backed transaction, and
preserves namespace resources, extensions, and listing indexes. Server startup
rejects offline admission markers for both row-store and imported checkpoints.
`storage migrate --action status --catalog ...` inspects the stopped catalog;
online migration uses the same authenticated table-identity resolution as other
public operations. Migration publication participates in the standalone mutation
journal, including rollback and restart recovery.
