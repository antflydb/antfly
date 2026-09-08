# Graph metric execution and resource ownership

Graph metrics use shared numerical semantics with backend-specific persistence.
The production boundary is admitted, generation-fenced work—not a synchronous
full-graph calculation hidden inside a query or maintenance tick.

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
from execution schemas before v15 restart.

Concurrent cold producers have distinct owner and staging namespaces. They may
duplicate initial preparation, but never share partial attempts or wait on
another numerical job. The first sealed owner wins each reuse directory entry;
losing owners are reclaimed after their own jobs release their pins. This avoids
introducing producer dependencies into worker-pool admission or recovery.

Reclamation is index-scoped, including indexes with zero configured metrics.
Each transaction examines at most 64 pins and deletes at most 512 topology
records. Current configured filters retain reusable owners; active job pins
protect older generations. Removed filters, removed metrics, failed producers,
obsolete format epochs, and unreferenced concurrent owners become reclaimable.
A durable deleting tombstone atomically unpublishes the owner and fences late
writes/adoption; deletion resumes after crashes by removing the next key page.
Superseded packing attempts have a separate bounded retirement queue so a
retained owner does not retain abandoned tiles indefinitely.
Cursor-only census advances do not count as eligible worker work: periodic
wakeups continue scanning without keeping idle worker pools busy. Actual
reclamation consumes the normal worker-page budget.

## Non-serverless

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
- Worker edge scans also range-seek past metadata and charge every physical
  non-metadata record against their checkpoint limit, including excluded records.
  Progress counts continue to describe matching graph edges, independently of
  physical work. An unbounded final partition cannot walk all metric state.
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
  This is independent of 64-node planning granularity. Prior scores for both
  HITS lanes are bulk-read before either lane stages mutations. Primary scores,
  ordered staging keys, and the attempt-fenced page cursor commit atomically.
  The coordinator checkpoints the bounded top-K prefix before pointer publication.
- Execution schema 15 fences older intermediate jobs. Published score epochs
  retain their existing read contract; an execution-format change does not hide
  previously published results.

## Serverless

- Verified packed graph ordinals are prepared once per immutable source.
  Compatible projection requirements share preparation when the combined work
  and memory fit. Otherwise, cheaper exact requirements are admitted first.
  Admission bounds active nodes by `min(source_nodes, 2 * selected_edges)` and
  uses the same sparse/dense census work model as construction. It still charges
  source-wide maps on the dense path. Identical computation aliases count once,
  as do compatible HITS pairs. This admission pass needs no edge scan or scratch
  allocation; exact construction admission and the live-allocation limiter
  remain authoritative. Serverless additionally reserves local-ID adapters,
  selection permutations and replacement-node buffers before allocation.
  Materializer epoch 18 invalidates cached rejections made with source-wide
  group estimates or duplicate-alias work charges.
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
- Materializer epoch 17 captures sparse projection and seed-window admission. Serverless remains
  current-version-only; no obsolete wire decoder or migration path is introduced.

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
use the same shared, conservative, request-cumulative budget as score storage.
The complete transport plan is still admitted before score network I/O.

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
