# Graph metric execution and resource ownership

Graph metrics use shared numerical semantics with backend-specific persistence.
The production boundary is admitted, generation-fenced work—not a synchronous
full-graph calculation hidden inside a query or maintenance tick.

### Stateful split ownership and bounded retirement

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
Copying into a previously split receiving index drains
its prior cleanup before installing replacement edges.

Physical counters remain cheap and durable during retirement. A request for
exact logical graph statistics temporarily uses a scoped edge scan and node
set until cleanup completes; ordinary adjacency queries do not pay that cost.
This is not a range-aggregate index or a single-store transactional redesign.
The two private stores and their durability barriers remain intact. Those
larger storage changes require separate write-amplification and recovery
benchmarks rather than weakening the current durability contract.

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

## Non-serverless

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

- Normal and lake ingestion share one ordinal graph builder. Distinct node IDs,
  relationship types, and target tables are interned once; retained edges carry
  numeric ordinals. Encoding counts adjacency sizes and scatters directly into
  the final wire allocation, then sorts each node/direction in place. It does not
  retain separate forward and reverse edge arrays. Scratch is 24 bytes per
  dictionary node instead of up to 40 bytes per edge; sparse graphs can have a
  different tradeoff than dense graphs. Canonical ordering, qualified endpoints
  and isolated local nodes are preserved.
  Both JSON adapters propagate allocator exhaustion and unwind partial edge
  ownership; allocation failure cannot silently produce an empty graph.
- Graph wire v8 and manifest v23 bind a 112-byte topology trailer to the
  manifest. Its SHA-256-authenticated directory contains canonical per-type
  semantic digests, dictionary page offsets, bounded first-key fence prefixes,
  and SHA-256 checksums for 64 KiB
  data blocks. A small authenticated root binds independently addressable
  64 KiB directory leaves; a fixed-width type-offset array supports lazy type
  lookup. Cold query readers fetch only the trailer, root, directory leaves and blocks covering
  selected ranges. They verify every fetched block before decoding; actual
  aligned/overfetched origin bytes count against the shared read allowance.
  Crossing 1 MiB of directory metadata no longer removes the accelerator.
  Preparation may read the complete directory under its existing peak budget;
  queries retain up to four directory leaves and eight data blocks. An explicitly unavailable directory retains the
  current-wire full-preparation path, not a legacy decoder. Low-level callers
  without a manifest control binding must authenticate the complete artifact
  before trusting its directory.
- Query sessions share authenticated root, directory, and data blocks through
  the same bounded single-flight cache as metric reads. Keys bind artifact
  identity, extent, and trusted digest; canceled or failed producers cannot
  publish partial data. Cache hits consume no origin-read bytes.
- Dedicated packed graph traversal and shortest-path queries retain numeric
  node/type IDs during BFS and decode only returned paths. Table-qualified
  targets cannot alias local identities. Ordered neighbor queries stop after
  the requested prefix; all inspected records still consume the work budget.
- Public traversal, path, and MATCH consumers use request-local edge streams
  on both native graph indexes and packed serverless artifacts. Native streams
  use the caller-pinned generation; packed streams bind immutable artifacts.
  Batches hold at most 64 edges, preserve vectorized admission/filtering, and
  stop when the consumer has its answer. Adapters without a streaming source
  retain the existing bounded materialized fallback.
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
- An authenticated eight-byte-per-dictionary-node routing array addresses
  adjacency rows. Public MATCH, traversal, path, and dedicated graph-query
  readers resolve dictionary pages through the small fence directory, then read
  only the requested row/type intervals. Exact relationship probes binary-search
  canonical `(type, neighbor)` ordinals and preserve the minimum-weight match.
  Fence prefixes are capped at 64 bytes per page; long common prefixes widen a
  bounded binary search rather than making the control object unbounded.
  Query allocators admit directory, cache, page, and decoded-row memory before
  allocation. Traversal parent identities borrow only visited retained rows.
  All reads share one byte allowance and authenticate every fetched block.
  Adjacent metric type runs share one authenticated boundary block, preventing
  thousands of small runs from repeatedly downloading the same 64 KiB range.
  Point/traversal readers retain up to eight blocks (512 KiB) per source so
  dictionary, routing, and row reads do not evict each other on every hop.
- Filesystem object GET pins one file handle for metadata and body reads.
  Concurrent atomic HEAD replacement cannot turn an unconditional read into a
  failed precondition; explicit ETag conditions still bind that pinned object.
- One bounded source-control object is retained while all pending filters on
  that source drain, independent of request/alias ordering. Filters prepare
  separately, smallest selected edge count first; one oversized filter cannot
  make a small filter retain its topology. Equivalent filters still share
  preparation, and compatible metric requirements share their projection.
  An unavailable directory falls back to one shared full-source preparation.
  Returned topology carries verified type digests, avoiding a second graph-wide
  hashing pass and per-node digest array. Fallback preparation computes the same
  digests under the separate identity-work allowance.
  Compatible projection requirements share preparation when the combined work
  and memory fit. Otherwise, cheaper exact requirements are admitted first.
  Admission bounds active nodes by `min(source_nodes, 2 * selected_edges)` and
  uses the same sparse/dense census work model as construction. It still charges
  source-wide maps on the dense path. Identical computation aliases count once,
  as do compatible HITS pairs. This admission pass needs no edge scan or scratch
  allocation; exact construction admission and the live-allocation limiter
  remain authoritative. Serverless additionally reserves local-ID adapters,
  selection permutations and replacement-node buffers before allocation.
  Materializer epoch 24 binds the current addressed graph layout and preparation admission.
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
  Changed indexed graphs authenticate their directory and only the selected
  topology blocks. Reuse can read semantic digests directly from that verified
  directory; full-content authentication is unnecessary when the manifest
  already binds the control root.
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

Regression coverage includes planning reopen/mutation fencing, metadata range
skipping, missing ordinal rows, exact integer degrees, standalone/incompatible
HITS definitions, admission caps, pre-allocation rejection, cold/warm artifact
authentication budgets, and point-only query metadata. See
[the benchmark report](../bench/graph/METRIC_PREPARATION.md) for measured scope,
fixtures, and limitations. Kernel or mock-storage microbenchmarks are not claims
about whole-query, whole-build, or cloud-network latency.
