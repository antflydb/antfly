# Graph metric execution and resource ownership

Graph metrics use shared numerical semantics with backend-specific persistence.
The production boundary is admitted, generation-fenced work—not a synchronous
full-graph calculation hidden inside a query or maintenance tick.

### Ordinary serverless graph construction admission

WAL/document graph construction shares the lake sidecar allocation limiter:
10 million input rows, 512 MiB of document bodies, 20 million retained node/edge
identities, 512 MiB encoded output, and 1 GiB live construction allocations by
default. Parsing, interning, sorting, routing and output allocations are included;
upstream materialized document buffers are not. Real allocator exhaustion stays
distinct from configured-limit rejection. The builder returns owned bytes only
after complete encoding; no partial graph or replacement HEAD is published.

Explicit build routes report resource rejection with HTTP 422. Background
publication retains the prior HEAD, counts/logs the affected namespace and
continues other namespaces. Process-local retry scheduling backs off from
10 seconds to at most one minute; explicit build requests bypass scheduling.
Eligibility is checked before status prediction. Each rejected live namespace
retains its deadline: there is no fixed-cardinality FIFO eviction cliff.
Successful catalog passes remove deleted namespaces; retained map capacity is
proportional to the high-water namespace count, not retry history.
This is retry throttling, not durable failure/progress state. A restart can
retry sooner. Metric computation budget rejection remains its
separate durable per-metric sidecar contract.

### Publication prediction and graph read isolation

Prediction uses one operation-local allocator for returned WAL, manifest,
document and projection buffers, capped at 1 GiB live allocations. It also
admits source rows/bytes, observes maintenance cancellation, and reports the
same configured-budget error as construction. Store owners and their allocators
are never modified. Actual backing-allocator OOM remains a distinct failure.
Prediction no longer clones/encodes a document segment only to discard it.
Document materialization transfers owned output buffers rather than copying
the entire completed view again.

Graph impact is computed once per materialization and shared by aliases and
the publication path. Byte-identical updates skip JSON parsing; explicit
local-table targets canonicalize identically to implicit local targets.
Graph neighbors, traversal and shortest-path HTTP responses fetch only the WAL
tip for freshness, not build status. An inadmissible pending publication cannot
prevent those reads from serving their pinned graph.

Prediction reads the pinned document-facts root and only WAL-touched bodies.
Persisted aggregate counters supply scheduling coverage without a corpus scan.
Publication independently derives an exact-source-fenced touched-document plan;
status requests do not leave mutable plans cached across HEAD changes.
Only changes to facts semantics or affected flat text/vector projections take
the admitted rebuild path. Graph aliases, metric configuration and unrelated
index settings do not invalidate the facts fingerprint. Metadata-only publication
reuses the facts tree and unchanged root, without fetching document bodies.
Graph-only updates do not hydrate unrelated bodies.

Catalog enrichment completion reads exact pending counters from a pinned facts
root. When the requested enrichment semantics differ, it streams authoritative
bodies under a read budget to calculate the new counts; an unreadable source is
an error, never an indication that enrichment is complete. A semantic policy
change schedules a facts publication even while the new stage is incomplete;
workers must not interpret an old index under a new pipeline version.

The current facts root (`AFDFACT3`, metadata version 3, 512 bytes) owns the point
index and four per-stage pending indexes ordered by `(WAL LSN, document ID)`.
Each pending entry carries the same source-fenced fact as the point index, with
its LSN authenticated against the ordering key. Publication updates these trees
and their exact counters atomically; a changed pending document removes its old
ordering key and enters at its new source LSN. Identical stage indexes can share
immutable pages; retention traverses all five roots as a shared graph. Workers
read only pending entries, not completed prefixes, flat compaction bases or
latest-only mutation segments.

Worker progress is a single CAS-protected, versioned per-stage record
(`AFESCAN3`). It owns both an exclusive ordering-key cursor and the inclusive
upper key captured once per cycle by a tree-height-bounded rank lookup. New WAL
arrivals sort beyond this boundary, regardless of their document IDs, and
cannot extend the active cycle. Existing pending documents updated during a
cycle move to the next cycle; failed unchanged documents remain available on
wrap. Both keys survive unrelated HEAD changes and reset when pipeline or facts
policy semantics change. Numeric offsets are diagnostic, not the resume
authority. This design needs no historical snapshot owner or additional GC
retention. Source read pins and publication/WAL fences protect every emitted
full-body upsert.
Serverless remains latest-format-only: older facts and cursor encodings are
rejected rather than interpreted as the new queue ordering.
Idempotency keys include the source HEAD, stage, pipeline version and document
key digest, so retrying or wrapping the cursor cannot alias another document.

The 64 MiB source allowance is a soft batch target, not a document-size limit.
One pending document may exceed it within the shared 512 MiB input/output and
1 GiB working-set limits. Configured allocation denial is distinct from actual
allocator exhaustion. Under `skip_document`, recoverable failures advance the
durable cursor and increment failure counters; the source remains pending for
a later cycle. Increasing capacity can therefore recover it without deleting
or rewriting the source. `fail_stage` remains an explicit stop-on-error policy.

Publication thresholds are coalescing targets, not indefinite visibility gates.
The background publisher gives below-target WAL batches a one-second maximum
coalescing delay by default, then makes them eligible even if enrichment is
waiting for publication. New arrivals and metadata-only HEAD changes do not
extend the deadline. Expiry never overrides admission budgets or lease fences.
The deadline is process-local scheduling state; a restart can begin a new delay.

All newly written publication artifacts, including flat search/document
segments, use namespace- and attempt-scoped identities. Existing immutable
references retain their identities when reused. GC fences old attempts before
sweeping their inventory, rechecks candidate authority before collecting its
references, and conditionally removes only a retired manifest identity. Object
stores use the ETag from that exact manifest read; filesystem creation and
candidate deletion share a cross-process mutation lock. A delayed collector
cannot delete a newer candidate that reused the same numeric version. Stores
without conditional removal fail closed. Unscoped synthetic/old candidate
references are conservatively retained; current production writers do not
create such uploads.

External inventory discovery receives a request-local scoped artifact
capability only after catalog publication acquires the namespace lease and pins
the source HEAD. Remote discovery and policy-refresh reads checkpoint that
authority. An unchanged inventory can reuse an authenticated reference from the
pinned current manifest; restoring identical bytes from an obsolete publication
creates a new attempt-scoped identity. A delayed collector of that obsolete
inventory therefore cannot remove the newly published one. Shared resolver and
artifact-store instances are never mutated to install request authority.
External inventory publication does not manufacture managed document facts or
implicitly request local text/graph indexes. Explicit sidecar configurations
remain visible: missing graph metrics report pending, but do not cause an
inventory-only publisher to create endless identical HEADs. Sidecar readiness
and inventory metadata publication are separate responsibilities.

For an unchanged external source descriptor, metadata publication reconciles
existing sidecars against the requested index definitions. Compatible physical
artifacts, search descriptors and document counts survive; graph metrics also
require the retained parent graph checksum and computation fingerprint to
match. Changed dependencies are removed individually, while source replacement
invalidates the prior sidecars. This reconciliation has no artifact-store or
row-source capability: metadata refresh cannot silently hydrate remote bodies.

External lake planning and catalog search targets both use explicit index
declarations. Empty objects (including whitespace variants) and graph-only
configurations do not synthesize a text index; deleting the last explicitly
configured default-named index removes its sidecar and serving descriptor.
Managed namespaces retain their existing implicit defaults. Defaults already
persisted as declarations during table creation remain ordinary explicit indexes.

Ready metric reuse depends on its computation and source identity. A budget
rejection additionally depends on the complete ordered admission plan. Metadata
reconciliation uses the same admission-plan predicate as materialization;
removing or changing a sibling, changing aliases or sources, or changing the
materializer policy invalidates the old rejection. An unchanged plan retains
its rejection without creating a retry loop. Dropped rejections become pending
and are eligible for the normal sidecar materialization workflow.

### Incremental serverless graph roots

WAL publication, compaction and lake sidecars publish immutable graph page roots.
Adjacency queries and metric preparation read the same manifest-pinned roots.
Production graph readers accept only the current format; packed graph codecs
remain explicit numerical/test oracles, not a legacy serving fallback.

The implementation is in `serverless/graph_segment/page_tree.zig`,
`page_keys.zig`, `page_graph.zig`, `page_store.zig`, `page_reader.zig` and
`page_topology.zig`:

- Content-addressed ordered pages target 32 KiB. An indivisible large identity
  can use a larger page, bounded at 1 MiB; encoded keys are capped at 256 KiB.
  Branches always pack at least two entries, including long-key cases.
  Sorted batched edits replace touched paths, merge underfull siblings and
  collapse single-child roots. There are no tombstones or unbounded overlays.
  Initial sorted construction retains only one pending page per height, owns
  borrowed source records before advancing, and writes each final page once.
  It performs no reads or intermediate root rewrites. Unsorted document sources
  use `page_bootstrap.zig`: admitted 4 MiB sorted runs and eight-way external
  merges, with at most 32 merge levels. Scratch pages belong to the publication
  attempt and are recoverable by GC. Memory scales with the run allowance,
  largest admitted document and merge fan-in, not the full graph dictionary.
- Node-first outgoing/incoming keys and type-first local topology keys use
  stable string identities. Canonical duplicate occurrence ordinals preserve
  multiplicity without depending on document-array ordering. Explicit document
  membership and implicit local endpoints have separate lifetimes.
- `page_graph.Plan` owns normalized replacements and fences publication against
  the exact prior root. It reads replaced source adjacency and touched node
  membership, not the complete namespace. Its caller must still supply coalesced,
  normalized document replacements under an admitted operation allocator.
  Ordered old/new edge merging emits only actual differences: unchanged hub
  edges do not create, sort or validate replacement mutations.
- Root metadata authenticates page digests, height, record count and size.
  Every page and the 128-byte root include a namespace reclamation domain;
  identical graphs in different namespaces cannot share reclaimable objects.
  Versions and index aliases within one namespace still reuse unchanged pages.
  Streaming range cursors have height-bounded residency. An operation-local
  512 KiB cache shares immutable routing pages without mutating store owners.
  Subtree cardinalities support selected-type admission before edge scans.
- Metric topology preparation reads selected type ranges and constructs dense
  computation-local ordinals. Tests compare node ordering, physical edge
  multiplicity and canonical per-type checksums with the packed implementation.
  All-type discovery advances ordered prefixes without rescanning prior kinds;
  requested kinds are sorted/deduplicated once. Edge-count admission stops at
  the first over-budget kind, before endpoint scanning or further discovery.
  The shared preparation context recognizes page roots and separates admission
  groups by filter. Single, batch and prepared artifact entry points support
  roots; tests compare all five metric kinds and filtered/unfiltered scores
  against the packed numerical oracle, including duplicates and qualified edges.
- Public adjacency readers dispatch to stable-key page ranges, intern only
  query-visited identities and lazily encode qualified-table metadata. Exact
  edge probes seek the source/type/target prefix. Ordinal filters are sorted and
  deduplicated before traversal; allocation-failure tests cover reader ownership.
- Retention follows page reachability for retained publications and above-HEAD
  candidates. Reclamation deletes children before their parent and graph root,
  preserving recovery inventories after interruption. Shared retained pages are
  never reclaimed. Unit and object-store retention tests cover replay and CAS
  publication/candidate boundaries.
- Query sessions over page roots acquire durable, shared per-version read
  deadlines in filesystem/object progress storage. A 64-slot local cache avoids
  per-query remote pin traffic. Deadlines last ten minutes and are reused only
  with at least five minutes remaining; a query cannot extend its captured
  authority. Readers check both wall time and suspend-inclusive local time,
  propagating expiry through transport and traversal as `DeadlineExceeded`.
  GC commits its retirement floor before observing pins, retains live pinned
  versions even below that floor, and allows 30 seconds of inter-host clock skew.
  Acquirers publish/observe a pin before their final floor check; independent
  owners cannot reopen retired history. Clock failure retains content and denies
  new read authority. Expired pins, including failed-acquisition orphans without
  manifests, are swept with bounded listing pages. This assumes clocks remain
  within the documented skew allowance, as with distributed publication leases.
  Filesystem pin/floor acknowledgements sync file contents and parent entries.

The current page wire is v3 and manifest wire is v25. Each page reference binds
its producing attempt, allowing unchanged pages to survive later publications.
Scoped artifact identities include namespace, fencing token and random attempt
nonce. A short GC work-lease barrier fences old attempts before inventory;
uploads abandoned before a candidate manifest are discoverable, while later
attempts cannot be swept. Candidate manifests carry their own publication token,
independent of the age of reused roots. Builders pin their source manifest and
recheck publication authority after candidate durability and before HEAD CAS.

Document facts use a separately typed root, leaves and `AFDBODY1` body envelopes;
arbitrary JSON bytes cannot masquerade as internal tree objects. Query filters
and hit hydration point-read facts/bodies. Enumerating document IDs does not
hydrate the corpus. Read caches and temporary body ownership are charged to
query admission. Composite graph keys, including escaped node/type/table IDs,
must fit 256 KiB; oversized identities produce a specific admission failure.

### Stateful split ownership and bounded retirement

Ownership preparation, backend commit, snapshot retirement and fsync run outside
the reader visibility mutex. A separate writer gate serializes range transitions.
Before commit, outgoing and reverse snapshots capture the old scope and epoch;
readers arriving during commit fork those pinned snapshots. A short locked
adoption replaces the visible scope and retires the handoff atomically. Forks
and their cursors can outlive the handoff and retain their original visibility.
Each logical adjacency call acquires outgoing and incoming snapshots together,
including calls spanning multiple relationship types. Repeated forks retain a
flat immutable owner anchor, not a recursively growing parent chain.
LSM forks share immutable snapshot metadata but own read hints, held blocks and
scratch; memory forks retain the immutable state; optional LMDB forks serialize
native transaction calls and require `MDB_NOTLS`. Unsupported backends fail
before any ownership commit. Metric admission reports a pending transition
throughout the commit window. Sync failure does not undo committed visibility;
idempotent retries sync without holding the reader mutex.
Commit-window tests open readers after the physical commit but before scope
adoption, retain them through cleanup, and verify independent cursor lifetimes.
Injected commit failures cover both fence creation and final retirement: neither
adopts an uncommitted scope nor retains a leaked handoff or write gate.

Logical ownership is separate from physical graph cleanup. Before committing a
narrower primary range and its Raft receipt, each private graph store durably
prepares a source-range retirement task. That transaction advances dependency
epochs and fences numerical leases, while preserving jobs and score namespaces
for their normal bounded reclamation. A prepared task does **not** hide edges
or permit deletion until the authoritative primary range excludes its interval.
Range adoption activates the fence without allocation or I/O. On reopen the
catalog reconciles the same durable task against the persisted primary range
before publishing the index. A failed primary commit therefore leaves the old
graph visible, and retry can complete the transition safely.

Outgoing and reverse reads use the same source-ownership predicate. Their
snapshot-owned scopes survive outer transaction closure and physical cleanup
until the last cursor closes. Cursors seek across excluded source intervals
rather than scanning each excluded edge, including within incoming target/type
runs. Pre-transition snapshots retain their old membership. Metrics cannot
acquire leases or publish across the ownership transition; background numerical
work resumes after physical accounting converges. Explicit synchronous metric
refresh and graph repair are drain boundaries.

The existing `backend_runtime` maintenance scheduler retires at most one graph
page per turn, round-robin across indexes, even when no metrics are configured.
Each page admits at most 1,024 identities / 4 MiB (one oversized identity is
indivisible). Its cross-store intent is durable before forward deletion; reverse
accounting, intent removal and the full-range resume cursor commit together.
Reopen can replay an interrupted page without double-counting, and later pages
seek after the committed cursor instead of revisiting prior tombstones. The
range task and visibility fence are removed only after all pages finish. One
range task is admitted per graph index; retries of that range are idempotent,
and a different transition receives `GraphMaintenanceInProgress` until the
existing task retires. Replicated split apply normalizes this admission result
to `RaftApplyWriterUnavailable`, retaining the committed entry for retry while
other groups and background cleanup progress; its receipt does not advance.
The same precommit admission applies to range expansion, including receiver
merge checkpoints and direct range updates. An excluded interval cannot become
owned again until its old retirement task finishes. This prevents both deletion
of newly accepted writes and reopen-dependent visibility. Checkpoints that leave
the range unchanged remain admissible; an overlapping expansion returns the
same retryable Raft admission result without committing its receipt or range.
Copying into a previously split receiving index drains
its prior cleanup before installing replacement edges.

Operational status and replay snapshots always read maintained physical counters
in constant time, without edge scans or a distinct-node set. Public graph status
sets `counts_pending` while retirement is pending on any observed shard: these
counts are upper bounds, not exact logical membership. The flag survives status
caches, metadata transport/persistence and durable local snapshots, and clears
after accounting converges. Provisional graph counts do not inflate the table's
document count. Explicit diagnostic graph statistics can still request an exact
scoped edge scan. Adjacency, paged, streaming and presence reads install their
physical prefix upper bound before seeking, so the ownership wrapper cannot
walk unrelated incoming targets looking for a visible edge.
Mutation and repair counters are transaction-local. Their persisted values
commit with reverse topology accounting before a short ownership-lock-protected
publication updates the status snapshot. Status reads cannot see provisional
counts from an aborted transaction; this counter publication acquires the status
lock only after commit I/O. Immutable in-memory LSM runs share one reference-counted owner for state,
routing metadata and bloom filters; readers and compaction snapshots pin that
generation until their last user closes, rather than cloning all its entries.
This is not a range-aggregate index or a single-store transactional redesign.
The two private stores and their durability barriers remain intact. Those
larger storage changes require separate write-amplification and recovery
benchmarks rather than weakening the current durability contract.

### Adaptive serverless adjacency type directories

The current graph wire adds sparse type-run offsets for high-degree rows.
Outgoing and incoming directions are admitted independently: at least 1,024
edges, and no more than `min(global_types, edges / 16)` distinct types. An
indexed row stores a 16-byte descriptor and eight bytes per reserved run;
small rows retain their existing eight-byte routing entry. High-entropy rows
fall back to edge binary search. The bounded reservation avoids resizing or
copying the full immutable payload and adds no graph-wide encoder side array.

Typed routing entries address authenticated metadata inside the same block
checksum tree as adjacency. Streaming, eager adjacency and exact probes share
the directory; only selected edges consume the physical-edge work allowance.
Metadata reads still consume cancellation, byte and allocation budgets.
Directories are resolved lazily, without materializing all type runs. A cold
wildcard query on an indexed hub can require one extra block read; the benchmark
records that tradeoff alongside reduced I/O for interior-type queries.
Serverless is unreleased: readers accept only the current graph/manifest wire
and materializer epoch, without a legacy layout branch.

### Durable cross-job stateful topology

Membership blocks, ordinal dictionaries, exact out-degree totals, and packed
adjacency have an independent durable owner. Its SHA-256 identity binds the
topology format epoch, canonical edge-filter set, and complete checksummed
generation partition plan. Metric names, damping, tolerance, and iteration
limits do not enter that identity. Numeric vectors (including the PageRank
degree-vector accelerator), folds, seeds, page attempts, and publication remain
job-local.

The first complete forward reduction seals the forward topology; HITS seals
both orientations after its first reverse reduction. Only complete phase
barriers can publish an owner. An adopting job validates the sealed owner and
writes its binding and lifetime pin in one transaction. It skips physical edge
discovery and adjacency production, reads the shared canonical membership for
its own seed/initialization, and uses shared packed tiles for every iteration.
HITS topology can serve PageRank or eigenvector; a forward-only owner cannot
satisfy HITS. Published scores keep their existing format; intermediate jobs
from execution schemas before v20 restart.

Cold scheduled builds first enqueue an index-scoped preparation task keyed by
generation, filter and required orientation. Concurrent PageRank/eigenvector
requests share it; queued HITS requirements select a bidirectional task. The
task has its own control namespace, page leases and recovery checkpoints, and
one independently admitted execution slot per index. At most 16 distinct cold
tasks are admitted per index; compatible consumers join an existing task without
using another slot. Admission precedes the numerical active-build cap, so ready
topology can be prepared while numerical slots are occupied. Each bounded
checkpoint rotates to the next task. The rotation cursor is an in-memory fairness
hint, while task incarnations and leases provide durable recovery. It builds membership and
packed adjacency without rank vectors or score publication. Waiting metrics
retain durable requests, but hold no numerical lease or admission slot.
Only after the owner seals does the coordinator admit numerical jobs. Explicit
low-level planned execution retains an independent-producer path for isolated
maintenance and parity benchmarks; the first sealed owner wins its directory.

Task failures preserve their root cause on dependent metrics. Retirement is
durably marked before bounded control-key deletion, so a crash cannot resurrect
a partially deleted task. A durable monotonic incarnation gives each retry a
separate control namespace. Admission, failure delivery and retirement validate
that incarnation, including across reopened handles. Failure delivery is
idempotent per task/canonical lifecycle owner so a delayed reporter cannot consume
a new manual retry request. Paired HITS failures use the authority owner even when
the hub alias reports first; both lanes receive the same root cause atomically.
Generation changes and loss of all eligible consumers
retire preparation; independent numerical/publication lifetimes are unchanged.
Inline numerical drains propagate coordinator terminal failures as failed status,
preserving the durable root cause instead of replacing it with an idle-page error.
Intermediate partition-plan v9 uses 4,096-unit scheduling ranges (capped at 256
partitions), with byte/work-bounded checkpoints within each range. Canonical
256-entry membership/vector chunks remain separate from scheduling page size.
Tests can inject smaller ranges to exercise takeover and partition boundaries.
The sealed plan is a 76-byte counts/identity/checksum record. Census checkpoints
write separately addressed, generation-checked boundary slots; completion hashes
the boundary set outside the writer and publishes the small header in the same
checkpoint CAS. Only initial manifest planning materializes those slots. Lease
checks, topology ownership and subsequent iteration planning read the header;
later numerical and summary pages reuse their iteration-zero range templates.
Slots are bounded to 256 per direction and reused on generation changes. Filter
removal reclaims both controls and slots through the bounded filter-plan GC.

Reclamation is index-scoped, including indexes with zero configured metrics.
Each transaction examines at most 64 pins and deletes at most 512 topology
records. Current configured filters retain reusable owners; active job pins
protect older generations. Removed filters, removed metrics, failed producers,
obsolete format epochs, and unreferenced concurrent owners become reclaimable.
A durable deleting tombstone atomically unpublishes the owner and fences late
writes/adoption; deletion resumes after crashes by removing the next key page.
Superseded packing attempts have a separate bounded retirement queue so a
retained owner does not retain abandoned tiles indefinitely.
Census position is an in-memory fairness hint: retained-owner and end-of-catalog
scans write no durable cursor or WAL record. Idle inspections have a per-sweep
budget without reporting eligible worker work. Actual reclamation consumes the
normal worker-page budget; durable tombstones/deleted keys provide recovery.

Topology task execution uses an explicit borrowed view, never a copy of the
live graph index. Synchronization, cache and cursor state is initialized afresh;
read snapshots and ownership admission delegate to the pinned live owner.
Counters come from the existing task transaction without another store scan or
snapshot. This prevents copying a concurrently held mutex into an execution
view that no thread can unlock, and avoids retaining stale ownership fences.

## Non-serverless

- The local maintenance command uses its caller's `std.Io` for an exclusive
  kernel file lock and bounded, cancelable contention waits. It requires no
  LMDB engine, private executor, persisted PID record or lock-record fsync.
- Generation-transition contention retains a stable retryable error across
  runtime archives and internal HTTP. Public queries retry a fresh snapshot
  within the existing cancellation/deadline budget; persistent contention is
  reported as temporary read unavailability instead of an opaque internal failure.
- Derived visibility waits carry their cancellation, absolute deadline and clock
  together through manual and Io-backed executors. A borrowed backend clock's
  timestamp is never reinterpreted in the native process clock domain.
- Global incidence counts use the same original/final mutation set as topology
  invalidation. Duplicate operations and delete/reinsert replacements do not
  perform intermediate counter writes. Endpoint deltas borrow input IDs, encode
  each distinct changed node once, and bulk-read sorted counts in 256-key pages.
  Both ends of a self-loop contribute; all borrowed values are decoded before
  batch mutation. This also applies to graphs without configured metrics.
- Connectivity epochs advance only when a batch changes the final edge identity
  set. Identical upserts, attribute-only updates, missing deletes, and delete/
  reinsert replacements do not restart unweighted metric jobs. Each selected
  relationship type has a durable epoch; a filtered metric depends on their
  maximum, not unrelated writes. Status generations describe that dependency.
  All-edge metrics still depend on the global connectivity epoch. Old stores
  acquire a conservative migration floor without a writer-side full scan.
- Type-addressable, empty-value covering postings retain reverse-key ordering
  within each type. Filtered discovery seeks only selected type ranges; weights
  and metadata need not be decoded. Existing stores backfill in bounded,
  checkpointed steps (record and key-memory limits) while connectivity
  mutations maintain postings transactionally; attribute updates do not rewrite
  these postings. The v2 covering index also keeps an incidence reference count
  per (type, endpoint). Insert/delete and idempotent backfill update these counts
  with edge postings in the same transaction; self-loops count twice. This adds
  storage and mutation work, shared across all filters, instead of rebuilding a
  source-wide endpoint set for each cold metric.
  A filter-epoch partition snapshot freezes scheduling boundaries so unrelated
  writes cannot invalidate in-flight discovery or shared topology adoption.
  Removed-filter snapshots are reclaimed in bounded 64-record maintenance
  sweeps, including indexes with no remaining metrics.
- Metric queries share a storage-independent read plan with serverless: load
  filters, restrict stable source-row ordinals, load ordering columns, select
  top-K, then load display-only columns. Reusable columns follow the selection
  and public nodes move only once. Qualified nodes never probe local score keys.
  A stateful read session validates every dependency policy up front and holds
  one transaction across all stages, including empty selections. Publication or
  cleanup between stages cannot mix generations or turn scores into misses.
- Query scratch, score columns, owned status metadata and replacement output
  allocations reserve bytes from the request's shared graph budget before
  allocation. Scratch frees release reservations; escaping output retains its
  request charge without retaining a pointer to a stack-owned budget allocator.
  Budget denial reports `GraphWorkBudgetExceeded`, not allocator exhaustion.
  Sorted score reads stop at 4,096 keys or 1 MiB of encoded keys; one oversized
  key may progress only if its allocation fits the caller's budget.
- Automatic and planned maintenance use resumable coordinator/worker pages.
  Standalone HITS authority/hub definitions are eligible independently; compatible
  pairs share a lifecycle as an optimization. Admission caps leave work queued
  and `runUntilIdle` returns `RunUntilIdleDidNotConverge`, rather than selecting
  unlimited local computation. Explicit legacy/oracle helpers remain opt-in.
- Planned idle maintenance uses the same catalog lifetime protection and
  transactional generation/attempt fences as background workers. It does not
  hold the DB apply lock while draining graph computation.
- A cold partition census visits at most 4,096 records per coordinator planning
  step. It skips the metric metadata namespace by range seek, checkpoints its
  cursor/counts/boundaries, and resumes after reopen. The checkpoint and completed
  plan are shared by metrics on one graph generation. Compare-and-swap checkpoint
  publication prevents competing coordinators from regressing progress. A graph
  mutation invalidates the obsolete census; it cannot publish mixed-generation
  boundaries. Memory is bounded by the maximum 256 partitions, not graph size.
  Filtered plans do not depend on that global census: they merge only selected
  edge and endpoint posting ranges, count exact selected cardinalities, then
  choose balanced boundaries. Endpoint streams deduplicate nodes shared by
  selected types using a fanout-bounded heap. Their checkpoint and CAS bind the
  filter epoch, so unrelated graph churn cannot reset cold planning. Each step
  also stops at 1 MiB of visited suffix bytes (one oversized record may progress).
- All-edge scans range-seek past metadata. Filtered scans charge only selected
  postings against their checkpoint limit and persist a type-qualified resume
  key, validated against the filter and scheduling range. Intermediate progress
  counts visited edges; completion seals the entire scheduling range. An
  unbounded final partition cannot walk all metric state.
  Ordinal topology extraction retains one reusable full resume-key buffer, not
  one per visited edge. A conservative 1 MiB input-scratch admission limit also
  bounds decoded endpoints and pending ordinal lookups, allowing one oversized
  edge to make progress. Long type names therefore cannot multiply a 4,096-record
  page into hundreds of MiB of retained cursor copies.
- Initialization writes canonical membership once in checksummed 256-row blocks,
  alongside ordinal assignments. Completed initialization leaves seal exact row
  counts. Vector initialization, iteration, convergence and publication read these addressed blocks,
  not up to 256 producer partials per node on every checkpoint. Readers bind
  block ordinals and node ranges to the leaf, validate resume-node identity, and
  reject missing/truncated/misplaced blocks. Replayed writes accept only identical
  rows even when checkpoint boundaries change.
- Reducers join sealed canonical nodes with the ordinal dictionary using
  ordered cursors, then carry ordinals through numeric reads and writes. The
  canonical membership check is essential: a missing dictionary row is an error,
  not permission to omit a node. PageRank stores immutable out-degrees in exact
  `u64` chunks, avoiding per-node string-key lookups on every iteration.
- After the initialization-summary barrier, every initializer consumes the sealed
  membership and carries its validated slots into all rank/factor/HITS lane
  writes. It does not rediscover producers or resolve the same node dictionary
  separately for each output lane. Numerical seeds still use the global summary.
- The initialization phase barrier seals a bounded metric-specific active-node
  plan. Empty node partitions in iteration zero are completed without worker
  claims; later iterations omit their data pages and scalar leaves. Original
  leaf IDs and membership blocks remain unchanged because vector slots encode
  those identities. Plan totals must match the sealed initialization root, and
  missing active leaves fail closed. Reopen and retries reuse the same plan.
- Adjacency producer phases exist only in iteration zero. Subsequent PageRank
  and eigenvector iterations start at reduction; HITS moves from authority
  reduction directly to hub reduction. Publication still verifies the sealed
  iteration-zero producer barriers. No metadata-only producer pages, claims,
  or completion transactions are scheduled for later iterations, and progress
  fractions use only the phases that actually run.
- Numerical folds, normalization and convergence enumerate dense ordinal slots
  from a checksummed active-node plan and sealed membership-leaf counts. Their
  durable completed-unit count is the resume cursor: they do not decode node IDs
  or join the node dictionary on each iteration. Range boundaries are reloaded
  in the current transaction. Initialization and publication still validate
  membership and the dictionary; missing required vector values fail closed.
- Numerical folds validate and borrow each immutable 256-edge tile from the read
  transaction. One checkpoint-local scratch buffer gathers vector values and
  maps chunk-local target slots directly to compensated accumulators. Warm vector
  gathers allocate no per-tile arrays; cold gathers reuse arena capacity. Chunk
  changes clear target mappings, and framing, receipt counts, ordinal validity,
  generation/attempt fences and accumulation order remain enforced. Transaction
  scratch is bounded by checkpoint limits, not total graph size.
- Sealed source-vector chunks may be reused across checkpoints. Each index has
  a lazy 4,096-entry LRU, but all indexes share a 64 MiB admission pool by default,
  charging entries and hash buckets. Hosts may inject a different shared pool
  through `GraphIndexOptions.sealed_vector_budget`; it must outlive its indexes.
  A full pool causes local recycling or storage-read fallback, never build
  failure. Metric retirement releases cached chunks and empty bucket storage;
  admission tickets prevent already-running checkpoints from repopulating
  retired data. Worker handles observe retirement independently of coordinators.
- Final numeric-score publication admits at most 4,096 nodes or 1 MiB of node
  IDs per checkpoint (one oversized ID is allowed to guarantee progress).
  This is independent of scheduling range size. Prior scores for both
  HITS lanes are bulk-read before either lane stages mutations. Primary scores,
  ordered staging keys, and the attempt-fenced page cursor commit atomically.
  The coordinator checkpoints the bounded top-K prefix before pointer publication.
- Execution schema 19 fences older intermediate jobs. Published score epochs
  retain their existing read contract; an execution-format change does not hide
  previously published results.

## Serverless

- Normal and lake ingestion publish stable-key, copy-on-write adjacency and
  type-topology pages. Initial construction uses bounded external sorted runs;
  incremental WAL updates merge only touched documents and changed edges.
  Document facts provide authoritative point lookup and maintained scheduling
  counters, independently of flat search-projection rebuilds.
- Queries share authenticated graph pages through a bounded single-flight cache.
  Keys bind namespace, artifact identity and trusted digest; canceled or failed
  producers cannot publish partial data. Cache hits consume no origin-read bytes.
  Root, routing, selected-row and decoded identity allocations remain admitted.
  Document bodies larger than the shared fill limit bypass the cache, retain
  checksum verification, and consume the caller's read and allocation budgets.
- Equivalent lake graph aliases build one projection per source binding and
  clone only owned declarations. Initial and changed-source alias-only groups
  stream once without corpus replay. Distinct projections may share an admitted
  replay; equivalent graph projections within that group still build once.
- Compaction uses the same graph-metric reuse/publication path as WAL builds,
  preserving configured metrics and their source provenance. Facts-backed
  compaction borrows the authoritative document view and sortable entry payloads
  instead of copying the corpus twice. Unchanged compacted heads are no-ops.
- Public traversal, path and MATCH consumers use request-local edge streams on
  native graph indexes and immutable serverless roots. Native reads pin outgoing
  and incoming snapshots in one visibility epoch across all requested types.
  Batches hold at most 64 edges and stop when the consumer has its answer.
  Exact relationship probes seek stable source/type/target prefixes.
- Packed ordinal dictionaries and their v9 trailer/directory/block codec remain
  explicit test and numerical-reference oracles. Production graph queries and
  metric preparation do not fall back to packed whole-artifact decoding.
- Stateful tree ingestion validates final identities once per `(source, type)`;
  deletes precede writes, including reinsertion of a deleted identity. Reverse
  rebuild and outgoing split-copy scans borrow cursor entries from a stable
  snapshot and commit batches bounded by both record count and 4 MiB of encoded
  key/value bytes (or one larger indivisible record).
- Split pruning persists a bounded identity intent before removing forward
  edges, then atomically retires that intent with reverse edges, counters,
  typed postings, and metric dependency invalidation. Forward data is synced
  before intent retirement, including relaxed-durability splits. Opening an
  index resumes unfinished intents before publishing it to readers.
- Exclusive counter repair uses durable cleanup/recount cursors committed with
  each page, not a graph-wide endpoint map. Pages retain at most 1,024 identities
  and 4 MiB of identity bytes (one oversized identity is indivisible). A new
  dependency epoch marks published scores stale; operator pause/disable settings
  survive repair. Reopening finishes an interrupted repair before serving reads.
- Filesystem object GET pins one file handle for metadata and body reads.
  Concurrent atomic HEAD replacement cannot turn an unconditional read into a
  failed precondition; explicit ETag conditions still bind that pinned object.
- One bounded source-control object is retained while all pending filters on
  that source drain, independent of request/alias ordering. Filters prepare
  separately, smallest selected edge count first; one oversized filter cannot
  make a small filter retain its topology. Equivalent filters still share
  preparation, and compatible metric requirements share their projection.
  Page roots admit selected types using subtree cardinalities, then stream their
  local endpoints and compute canonical type digests. Returned topology carries
  those digests, avoiding a second graph-wide hashing pass and per-node digest
  array. Packed full-source preparation is an explicit reference-oracle API.
  Compatible projection requirements share preparation when the combined work
  and memory fit. Otherwise, cheaper exact requirements are admitted first.
  Admission bounds active nodes by `min(source_nodes, 2 * selected_edges)` and
  uses the same sparse/dense census work model as construction. It still charges
  source-wide maps on the dense path. Identical computation aliases count once,
  as do compatible HITS pairs. This admission pass needs no edge scan or scratch
  allocation; exact construction admission and the live-allocation limiter
  remain authoritative. Serverless additionally reserves local-ID adapters,
  selection permutations and replacement-node buffers before allocation.
  Materializer epoch 26 binds selected-work admission. Indexed preparation is
  not rejected by unrelated source cardinality or whole-object size. Selected
  nodes/edges, actual fetched bytes (including authentication), work and live
  memory remain bounded. The 256 MiB whole-payload decode cap still applies to
  packed reference-oracle calls. Cached projections are re-admitted against each caller's
  selected cardinality limits.
- Preparation has two admission phases. The projection census is charged before
  allocations or edge scans; exact projection construction is charged after the
  census and before CSR allocation. Reserved census work remains charged when
  construction is rejected. Exhausted publications cannot repeatedly construct
  unaffordable projections. A live-allocation limiter also covers scratch buffers
  and failure paths before a post-census size estimate is available.
- If selected edges are at most 1/64 of the source node count, projection sorts
  and deduplicates their endpoints and uses binary ordinal lookup while replaying
  the original edge order. Scratch and census work then depend on the selection,
  not the source dictionary. Dense projections retain linear-time source-wide
  maps/counts. Both paths preserve canonical node order and numerical summation
  order; degree projections do not retain neighbors.
- Output has two admission phases too: a framing/row lower bound rejects
  impossible output before kernels or warm-start reads; a prepared encoding plan
  then reserves exact payload bytes before allocation. Compatible HITS lanes
  reserve both outputs atomically before either upload, while encoding one at a
  time. Allocation, cancellation and integrity failures refund reservations.
- Reuse uses a single authenticated, provider-pinned range read. A table-wide
  `max_total_reuse_read_bytes` allowance (512 MiB by default) covers requested
  headers and cold full-content authentication. This allowance is separate from
  optional warm-start reads and numerical work. Native stores charge full-content
  bytes only on a verification miss; a cached object pin needs no redundant HEAD.
  SHA-256 provider metadata can authenticate a cold object without downloading it.
  Custom stores use conservative full-object admission. Exhausting reuse admission
  skips that optimization and leaves materialization subject to its own budgets.
- Immutable metric payloads carry a SHA-256 identity of selected unweighted local
  topology, distinct from the current publication's full graph checksum. Hashing
  canonical endpoint identities (not ordinals) makes weights, unrelated types,
  qualified edges and isolated documents irrelevant to this identity. A matching
  authenticated metric header, configuration and materializer policy allow reuse
  without projection, kernels or score encoding. The manifest rebinds it to the
  current graph checksum/generations while preserving the real computation time.
  Readers validate both semantic binding and current source integrity. Original
  source strings in the immutable payload need not equal the new publication's
  strings; authenticated control lengths come from the artifact manifest.
  Changed page graphs authenticate their root and selected topology ranges.
  Semantic digests are calculated from those ranges; unrelated adjacency and
  relationship types need not be fetched. Unchanged roots can reuse their
  already-bound computation identity.
  Optional hashing has a separate 1 GiB byte-work allowance and live-allocation
  admission including any retained projection. Per-type digest scratch is freed
  before numerical work. If admission is exhausted, a zero identity disables
  cross-source reuse and retains exact-source validation; cold work keeps its
  independent budget.
- Optional PageRank warm starts authenticate control, root, directory, selected
  routing pages and primary score windows. Sparse selections skip unrelated
  blocks; consecutive selected blocks share windows up to 1 MiB, narrowed to
  available memory headroom (one larger block is allowed if it fits). Only one
  page/window is retained alongside the seed and
  bounded metadata. Ranked score payloads are not read. A live allocator bounds
  preparation memory, and requested bytes plus any cold provider verification
  are charged to the seed budget before I/O. Budget/integrity failures discard
  the partial seed and fall back to cold computation; cancellation and genuine
  allocation failures propagate.
- Cold providers without comparable SHA-256 metadata still require a bounded
  full-content hash. Identity caches are process-local; no untrusted durable
  “verified” flag bypasses authentication. Persisting verification evidence would
  require a defined trust and provider-generation contract, not just caching a
  boolean in a manifest.
- Manifest v19 and metric segment v10 are current-version-only. Missing graph
  provenance starts at the current publication generation; there is no inference
  from pre-release sidecars and no obsolete wire decoder or migration path.

## Query and operator views

Score, top-K, and column snapshots read compact publication/freshness metadata
and scores under one stable transaction. Queries do not fetch operator event or
failure histories, aggregate worker progress, or enumerate page details. Detailed
administrative status remains available through the existing operator paths.
Freshness requirements are checked before score reads, including reranking.

Serverless authenticated disk hits promote into the same bounded canonical block
cache used by network fills. Promotion is optional and never waits on a pending
fill or pinned-capacity pressure. Point-score consumers borrow ref-counted leases
on warm blocks instead of copying payloads; leases keep entries alive during
decoding. Authentication is unchanged, and a cache failure remains a miss rather
than authority over the immutable source.

Decoded point-routing pages, roots, and directories share the configured
`max_graph_metric_routing_bytes` allowance (16 MiB by default), without a separate
64-entry residency ceiling. Intrusive hash buckets provide keyed lookup and
separate unpinned LRUs prioritize page eviction over metadata eviction. Pinned
entries stay charged; a saturated cache safely bypasses admission. Eviction does
not scan pinned entries. The bounded 64-slot in-flight ownership table is still
independent of residency and retains its cancellation/single-flight contract.
Decoded page misses reserve this same ownership table before fetching or
decoding. A bounded group publishes and finishes every owned fill before
waiting on other producers or table capacity, preventing multi-page deadlocks.
Waiters share the decoded lease and are charged retained memory, not a duplicate
decode. Cancellation and failed producers release fill registrations.

Point queries admit output descriptors/cells before allocating them, then admit
one `u32` candidate permutation shared by every physical metric column. IDs are
validated once and a common prefix is skipped. Admitted transient `u64` prefix
keys accelerate sorting, with full-string comparison on ties; they are freed
before any metric plan is prepared. Only the shared `u32` permutation survives.
Original row indexes preserve duplicate IDs and public result order. Routing
uses binary boundaries in that order, so dense block/page spans do not rescan
every row per column. Per-column ownership contains only unresolved block spans,
not another row map; authenticated cache hits are consumed during preparation.
Span, range, selected-page and decoded-routing capacities are charged before
allocation, including possible owned-slice replacement peaks. These reservations
share the request memory limit, but point-read scratch and routing leases release
their conservative charge when the read ends and all children have joined.
Control buffers, routing transport buffers, and decoded routing leases have
explicit live reservations that retire at their actual ownership boundaries.
Cold decode reservations transfer into leases without a release/reacquire gap.
Preparation fanout falls back to one column when a conservative two-column
memory envelope does not fit; exact reservations, not that envelope, decide
request eligibility.
Request-scoped output columns transfer move-only reservations into the staged
HTTP query cache; replacing or discarding a column releases its prior charge.
Rebasing reserves the replacement before allocation and commits ownership only
after successful scatter, preserving old data and admission on failure.
Public output APIs detach the reservation because their results may outlive the
session; those escaping results retain a conservative request charge. Network
requests/bytes, decoded blocks and work remain cumulative and cannot be refunded
by dropping a stage. The complete transport plan is admitted before score I/O.

Live transport buffers also reserve this shared memory budget, including
authenticated cache-fill/lease bytes, the contiguous output and block descriptors.
The reservation survives until decoding releases the range. Column execution
reserves its joined group before launching workers and reduces column/range
fanout when only serial execution fits. The eight-range/32 MiB transport cap is
an additional ceiling, not a substitute for request-wide memory admission.

Within an authenticated score block, sparse candidates use binary lookup while
dense sorted candidates merge once through the block. Original row ordinals
scatter results without reordering callers or losing duplicate/missing IDs.

Top-K reserves descriptor storage before allocation or ranked-block reads, then
charges each decoded node ID before allocating it. Both per-result and shared
request limits apply. The HTTP response transfers those node allocations, rather
than temporarily retaining a second copy; its replacement descriptor array is
also admitted before allocation. Transfer failure leaves the original owner
intact. Retained-byte accounting remains conservative and request-cumulative.

## Validation and measurements

Serverless retention uses an exact, admitted mark set rather than unbounded
heap growth. `Pruner.limits.max_working_set_bytes` defaults to 256 MiB and covers
manifest inventories and decoding, retained/pinned version sets, owned artifact
identities, and graph/document-facts page traversal buffers. Store-owned fixed
transport and coordination state is outside that operation allocator. A denied
allocation reports `GarbageCollectionBudgetExceeded`; callers can raise the
limit for larger retained namespaces. Marking must finish before sweeping, so a
mark denial never makes a live publication partially collectible. Later cleanup
interruptions remain replayable through immutable manifests and the physical
namespace/attempt inventory. This is a bounded exact-mark contract, not an
external-memory collector or a claim of constant-memory GC for arbitrary graphs.

Regression coverage includes planning reopen/mutation fencing, metadata range
skipping, missing ordinal rows, exact integer degrees, standalone/incompatible
HITS definitions, admission caps, pre-allocation rejection, cold/warm artifact
authentication budgets, and point-only query metadata. See
[the preparation benchmark report](../bench/graph/METRIC_PREPARATION.md) and
[end-to-end publication qualification](GRAPH_METRICS_PUBLICATION_BENCHMARK.md)
for measured scope, fixtures, and limitations. Kernel or mock-storage microbenchmarks are not claims
about whole-query, whole-build, or cloud-network latency.

Focused validation targets are `graph-metric-core-test`,
`antfly-graph-page-test`, `antfly-document-facts-test`, `lake-test` and
`antfly-serverless-test`. The graph-page target supports `ReleaseSafe` for
checked page/key invariants; allocation-failure tests also validate ownership.
