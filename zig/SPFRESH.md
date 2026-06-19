# SPFresh-Style HBC Refactor Plan

## Goal

Evaluate whether Antfly should move toward an SPFresh-style mutable AKNN
index without prematurely replacing the current HBC implementation.

The current conclusion is:

- Do not build a separate SPFresh index yet.
- Refactor the current HBC so its implicit pieces become explicit:
  - a centroid/routing directory
  - a posting store
  - a vector-to-posting assignment map
- Keep the existing HBC as the first centroid directory implementation.
- Replace HBC's packed mutable posting/centroid persistence incrementally,
  starting with explicit posting base/delta records that can be tested against
  the existing HBC format.
- The concrete storage target is to stop making packed HBC leaf records the
  authoritative posting store. Posting membership should live in explicit
  posting base/delta records, posting centroids should live in explicit
  centroid-directory records, and HBC should initially remain only the routing
  structure over those posting IDs.
- Use the refactor to test SPFresh-style maintenance policies: lazy centroid
  refresh, local split/merge, and targeted boundary reassignment after
  split/merge if we later choose to enforce a nearest-partition invariant.

The important distinction is not RaBitQ versus some other quantizer. The
important distinction is whether routing and posting maintenance are cleanly
separated enough that posting updates can stay local.

## Implementation Status

Current status:

- Phase 1 is implemented.
- `zig/lib/vectorindex/src/posting.zig` defines the initial `PostingId`,
  `PostingView`, `PostingStore`, `AssignmentMap`, and `CentroidDirectory`
  names.
- `PostingFormat` now defines the first explicit SPFresh-style posting storage
  contract: immutable posting base records, append-only delta tails, tombstone
  and replacement records, and deterministic overlay materialization.
- Existing vector-to-leaf assignment storage now flows through
  `AssignmentMap`, while preserving the current key format.
- Existing leaf/member scoring setup now flows through `PostingStore`.
- Existing online leaf member append/remove paths now flow through
  `PostingStore` helpers.
- Existing leaf centroid recompute paths now flow through
  `PostingStore.recomputeCentroid`.
- Existing leaf RaBitQ refresh vector materialization now flows through
  `PostingStore.loadTransformedVectorsForQuantizedRefresh`.
- Existing leaf RaBitQ payload cache/write mechanics now flow through
  `PostingStore.refreshQuantizedPayload`; internal-node quantized payloads
  remain owned by HBC.
- Search now profiles internal-routing quantized misses separately from leaf
  posting-payload misses. If an internal routing payload is missing or stale in
  a read transaction, search can rebuild a read-only in-memory payload from
  cached child centroids and cache it, avoiding repeated warm-query storage
  reads while leaving durable repair to maintenance.
- Search profiles posting overlay cost as separate base decode and delta replay
  counters. Read/write benches and query profile metadata now expose
  `profile_posting_base_decode_*` and `profile_posting_delta_replay_*`, so
  comparisons can distinguish base materialization from tail replay.
- Search profiles scratch allocator pressure and retained workspace bytes.
  Read/write benches and query profile metadata now expose
  `profile_search_scratch_allocations`,
  `profile_search_scratch_allocation_bytes`, and
  `profile_search_scratch_retained_bytes`, so slab compaction work can be
  driven by allocator evidence.
- Delta folds profile peak retained fold scratch bytes through
  `posting_delta_fold_peak_scratch_bytes`, so maintenance tuning can compare
  fold memory pressure against written base bytes and deleted tail bytes.
  Retained fold scratch is also cleared and trimmed against
  `max_retained_posting_fold_scratch_bytes` before it is cached, so a one-off
  large fold no longer leaves oversized encoded-base, member, delta, or
  overlay buffers resident.
- The comparison summarizer derives posting-family LSM cost through
  `posting_lsm_keys_per_mutation` and `posting_lsm_bytes_per_mutation`, so
  backend overhead can be separated from logical base/delta format cost.
- The comparison summarizer also reports query materialization cost through
  `post_query_materialization_ns_per_posting`,
  `post_query_materialization_ns_per_member`,
  `query_materialization_ns_per_posting`, and
  `query_materialization_ns_per_member`, so overlay replay CPU can be compared
  independently of cache and backend effects.
- The bounded posting-member overlay cache is now keyed by posting id, base
  generation, posting mutation version, and the applied delta-tail high-water
  sequence. Hot canonical base/delta queries can reuse materialized members
  without serving entries across base folds, logical posting mutations, or
  newer tail records. Clean-base queries use the known zero high-water mark
  without scanning the delta tail.
- HBC maintenance paths that only need base generation/member count now use
  `PostingStore.loadBaseHeader`, while paths that need structural validation
  use `PostingStore.loadBaseStats`; both avoid decoding an owned member array
  and keep specialized base-decode modes in live code. Non-adaptive delta-tail
  replay now also delays member-scratch growth until a live insert/replace
  record survives the base-generation filter, so stale folded tail values do
  not grow query/materialization scratch. Sorted medium-tail replay grows its
  temporary delta-record scratch geometrically up to the compact-replay cap
  instead of reallocating one record at a time. Cached sorted replay now also
  converts its buffered compact records into the overlay plan when that cap is
  exceeded and continues from the current record, avoiding a full replay
  restart on large cached tails. Sorted replay and compact base materialization
  size temporary output buffers from deduped live operations rather than raw
  tail length, so tombstone-heavy tails do not inflate scratch allocation.
  Generic delta-tail scans now also append decoded records only after the
  base-generation filter accepts them, avoiding decoded-record scratch growth
  for stale folded LSM tail values. LSM delta-tail stats now use the ordered
  posting-delta key sequence to skip per-record generation checks when the whole
  value is newer than the base generation while still validating every encoded
  record. The same key-boundary fast path is now used by LSM delta replay for
  fold/materialization and latest-op scans, so all-live tail values do not pay a
  generation test on every decoded record. Committed segment catalog replay uses
  the same min-sequence shortcut for all-live delta values.
  Unsorted materialization and fold fallback paths likewise reserve member
  output capacity from surviving insert/replace records rather than total delta
  records. Large unsorted materialization tails now build a latest-op map and
  replay only final live operations, preserving existing output order while
  avoiding repeated linear remove/shift work. Fold scratch delta-record and
  compact-delta id/op arrays also grow geometrically, which avoids one realloc
  per buffered delta in append-heavy scans.
- Sorted canonical bases now expose `PostingFormat.baseContainsSortedMember`
  and a strict validating variant, giving delete/update and validation paths a
  streaming membership primitive that can stop before materializing the full
  member list when the target is absent or found early. The primitive also
  checks each encoded block minimum before decoding member deltas, which gives
  large negative lookups a cheap early exit until richer block skip metadata
  exists. A stats-returning probe helper reports blocks seen, blocks skipped by
  max hint, blocks decoded, and members decoded, so tests and future profiles
  can verify partial-decode behavior directly. Segment-backed write profiles
  now record those probe counters from base/delta membership checks, and the
  write comparison summary carries them through with
  `posting_base_member_probe_skip_rate` so block-level pruning is visible in
  benchmark output. Canonical base/delta single-member checks now use that
  streaming probe, so after-block negative membership tests can skip encoded
  blocks without decoding their member payloads, and they reuse the already
  decoded base header instead of parsing it twice.
- `SearchScratch` already groups fixed query arrays into `query_storage`; cold
  scratch retention now also releases query, distance, member-id, vector-batch,
  and rerank-flag slabs when `max_retained_search_scratch_bytes` demands it,
  then regrows them through the existing `ensure*Capacity` paths. Query-side
  posting delta-tail prefetch cache entries now grow their parallel record
  arrays geometrically instead of reallocating all three arrays for each
  appended cached record. Query and fold overlay-append scratch uses the same
  geometric growth shape for appended-id/live arrays, reducing allocator churn
  on hash-backed overlay replay. Search scratch member-id, vector-batch, and
  posting-delta-record buffers also grow geometrically across reused query
  scratch handles, and the grouped query/distance slabs now grow geometrically
  instead of resizing exactly to each transient candidate count. Small
  transformed-vector matrix loads now use stack-backed lookup, vector-view, and
  batch-vector scratch before falling back to heap allocation.
- Delete-heavy posting mutations now switch `PostingStore.removeMembers` from
  repeated linear membership checks to a temporary removal set for larger
  postings/delete lists, preserving member order while avoiding quadratic CPU
  in churn-heavy maintenance paths.
- Delta-tail scans now carry encoded key/value byte debt alongside record and
  tombstone counts plus the max applied delta sequence. Fold policy has an
  internal default-off value-byte threshold wired through maintenance
  configuration and status. This keeps today's behavior stable while giving
  future cost-aware maintenance and overlay-cache high-water checks direct
  signals for tail replay and backend overhead.
- The first dedicated posting segment container now exists in
  `posting_segment.zig`. It stores existing v1 posting-base, posting-delta, and
  centroid-directory values in one immutable posting-local indexed blob with
  footer validation, segment-level checksum validation, index checksum
  validation, per-value checksum validation, segment metadata, ordered delta
  iteration, and a borrowed catalog that can pick the newest point record while
  merging deltas across segment blobs. The catalog now has a
  typed snapshot facade that decodes those segment values through the existing
  logical posting codecs, so future runtime read paths can depend on
  posting-base, delta-tail, and centroid-directory structs instead of segment
  byte layout. Segment readers also expose deterministic logical-entry
  iteration plus indexed point-value locations for future compaction,
  migration, backup verification, and range-read plumbing, and the segment
  module can compact segment blobs by retaining the newest base and
  centroid point records while rewriting only delta records newer than the
  selected base generation. Compaction reports input/output bytes, retained
  records, and dropped superseded/stale/duplicate records for future resource
  accounting and policy tuning. A manifest replacement helper can encode the
  deterministic commit step for replacing compacted-away segment ids with new
  segment entries while advancing `next_segment_id`. Snapshot delta reads can
  also filter by base generation and use segment delta-range metadata to skip
  stale tail files before decode. A compact
  durable manifest codec with trailer checksum
  records segment ids, paths, and posting/delta range metadata for reopen, and
  an owned segment-store opener can rebuild a snapshot from manifest bytes plus
  caller-provided segment reads while validating each segment's actual metadata
  against the manifest. Directory helpers can write segment and manifest files
  through temporary files plus rename, then reopen and validate the store from a
  manifest path. Segment writers can also commit directly to a directory by
  reading the current manifest, assigning `next_segment_id`, writing the segment
  file, and publishing the updated manifest with an atomic rename. Segment
  directory compaction can now write a compacted replacement segment first and
  atomically swap the manifest to remove compacted-away segment ids, either for
  the whole manifest or for an explicit selected segment-id set. That gives
  future maintenance a bounded compaction primitive: manifest-only summary and
  planning helpers can report segment/byte/entry/posting/delta ranges and choose
  oldest segment ids under segment-count and input-byte caps before any segment
  file is opened, then selected compaction can preserve unselected manifest
  segments while replacing only the scheduled compaction inputs. The selected
  path rejects duplicate selected segment ids before segment IO and reads and
  validates only the selected segment files, not every segment referenced by the
  manifest. Old segment files are left as
  unreferenced orphans. A manifest-aware
  directory GC pass can then scan the `postings/` directory and delete only
  canonical `.afps` segment files that are absent from the current manifest,
  while ignoring manifest files, temp files, and non-segment entries. A separate
  recovery cleanup pass removes only known temp artifacts from interrupted
  atomic writes (`*.afps.tmp` segment files and `manifest.afpm.tmp`) while
  leaving unknown temp names alone. The segment layer also has a manifest-only
  lazy directory store: it can reopen by decoding the manifest without reading
  every segment file, then read and validate only manifest entries whose posting
  range can contain the requested posting. Segment
  writers now also have typed helpers for appending posting-base,
  posting-delta batch, and centroid-directory records, and eager/lazy snapshots
  can materialize the authoritative member view directly from segment-backed
  base+delta values. A directory verification pass now reads the current
  manifest, validates every referenced segment checksum and metadata entry, and
  returns manifest, segment-byte, entry, base, centroid, delta-value, and
  delta-record counts for backup verification and resource accounting. A
  directory copy/restore helper validates and writes referenced segment files
  into a destination directory before publishing the manifest last, so restored
  stores do not become visible until all referenced segment bytes are present
  and valid. Segment writers can also produce a manifest-ready built segment
  with validated metadata and a stable segment path. Segment manifests now
  persist trusted index offsets and checksums, and lazy segment snapshots use
  verified range reads for base, centroid, and delta-tail records instead of
  loading the full segment file for matching postings. Lazy member
  materialization now scans each candidate segment index once, reusing those
  verified index bytes to find the latest base and relevant delta values before
  reading value ranges. Segment-backed delta-tail stats now stream matching
  encoded delta values directly, so HBC status and fold prechecks can count
  tail records, tombstones, max sequence, and value-byte debt without
  allocating the decoded delta array. Segment-backed delta record loads,
  member materialization, and compaction now also stream `DeltaTailIterator`
  records directly into their output/scratch buffers instead of allocating a
  decoded array per delta value. Segment and pending-batch delta readers now
  also grow decoded-record scratch only after a record survives base-generation
  filtering, so stale folded tail values do not inflate retained segment replay
  scratch. A bounded segment directory batch writer can accumulate
  posting-local records under entry/byte caps and flush them through
  the existing atomic segment+manifest commit path, including coalescing
  individual and multi-record posting delta appends into one encoded delta-tail
  value per posting, which gives the future runtime backend an explicit
  micro-batch append primitive. The runtime batcher also coalesces pending
  posting-base and centroid-directory point records so a publish window keeps
  only the latest point value while the immutable segment writer continues to
  reject duplicate exact keys. Pending segment delta micro-batches now count
  their exact encoded value bytes while still buffered as records, so
  `max_pending_value_bytes` bounds delta batches as well as point values and
  pending-tail stats expose the same encoded-byte total to fold policy.
  Segment folds can now materialize and re-encode directly through retained
  fold scratch, avoiding separate owned
  materialized-member and encoded-base allocations before publishing the folded
  base; when canonical base ordering is active, they use a sorted compact
  delta merge instead of linear remove/apply replay. A runtime-facing directory
  store adapter now owns the lazy
  manifest snapshot plus the bounded batch writer, making flushed
  base/delta/centroid appends visible through refreshed lazy snapshots and
  refreshing again after bounded directory maintenance. A
  directory maintenance step now
  composes temp cleanup, manifest-only compaction planning, selected segment
  compaction, and orphan collection into one bounded stats-returning call for
  future background maintenance wiring. The generic posting store now has a
  physical posting-backend axis and segment-backend hooks for base,
  delta-tail, centroid-directory, query materialization, stats, and fold
  operations, so segment mode fails closed instead of falling through to LSM
  keys when a concrete index has not implemented the hooks. The HBC adapter can
  now open an owned posting segment runtime store, route base/delta/centroid
  posting hooks through it behind `posting_backend = segments`, materialize and
  fold from the segment manifest, and preserve those posting artifacts across
  reopen without writing posting-base records into the LSM namespace. Segment
  base-header reads and fold decisions in a write transaction first consult
  pending runtime batch point records and pending delta-tail stats, giving
  segment-backed bulk-build parent updates and same-transaction folds the same
  read-your-writes behavior that the LSM-backed path gets from pending LSM
  transaction keys. Segment committed-plus-pending delta materialization now
  merges both sources under the global delta sequence order before replay, so
  write-transaction reads do not depend on whether a delta was already flushed
  to a segment file or is still buffered in the runtime batch. Single-member
  delta-op lookups use the same committed-plus-pending sequence comparison, so
  membership checks do not prefer buffered operations over newer committed
  operations, and the segment runtime exposes sequence-bearing latest-record
  helpers so that path no longer has to allocate whole delta tails just to
  compare one vector's latest op. Segment committed-plus-pending scratch replay
  also now sorts the two delta sources inside retained replay scratch and
  applies them in-place instead of allocating a separate materialized member
  slice; canonical base/delta query replay uses merge-style sorted delta
  application against sorted base members, preserving the sorted-base contract
  across pending-overlay reads without the generic remove scan and final member
  sort. Segment folds with a committed encoded base now stream sorted
  committed-plus-pending deltas directly into base encoding from the existing
  base bytes, avoiding a full decoded base member slice before writing the new
  base. Committed and pending segment delta tails are appended directly into
  retained query/fold/materialization scratch while collecting fold-policy
  stats where needed, so replay no longer decodes the same delta values twice
  or allocates separate owned delta slices on those paths. Pending in-memory
  bases still use retained member scratch because they have no encoded base
  value yet.
  Segment
  file writes are now staged in the runtime batch and flushed at HBC commit
  boundaries, where the resulting segment manifest bytes are stored in the same
  LSM meta transaction as the namespace changes. Reopen prefers that committed
  manifest marker, so uncommitted directory manifest state is physical garbage,
  not logical visibility. Segment directory maintenance can now run from that
  committed manifest snapshot, compact selected segment files, collect
  temp/orphan files, publish the replacement manifest through the same LSM meta
  transaction, and report compact segment counters through dense posting
  maintenance stats and Prometheus. HBC can also export and import a
  self-contained segment-backed posting bundle through a standalone directory:
  the bundle contains a versioned snapshot of the HBC private namespaces plus
  the segment artifacts referenced by the committed manifest marker. The
  private namespace sidecar includes a CRC32 trailer and is written through a
  temporary file and renamed into place, matching the segment manifest
  publication shape. Import validates the private namespace snapshot checksum
  and its committed manifest marker before mutating HBC namespaces, validates
  each referenced segment, writes the physical manifest last, then publishes
  the restored HBC metadata. Imports
  whose segment manifest does not match the bundled committed marker fail
  without rolling the live index back to the private snapshot. Dense
  maintenance now runs
  segment compaction under the existing dense posting maintenance working-set
  reservation, passing that reservation through as a cap on selected compaction
  input bytes. HBC segment recovery can reload from the committed manifest
  marker, rewrite that manifest into the segment directory, skip compaction, and
  delete ignored temp/orphan physical segment files. The dense index config now
  separates `backend`, `format`, and `version`; `backend = segments,
  format = base_delta, version = 1` is an opt-in DB-facing mode while the
  default remains `backend = lsm, format = packed_hbc, version = 1`. The
  OpenAPI source and generated Zig config docs now describe that opt-in segment
  backend instead of calling it reserved. Generic DB
  snapshots now include the self-contained bundle for segment-backed dense
  indexes and restore it before runtime repair begins, while non-segment dense
  indexes keep the existing logical-store rebuild behavior for generated/stored
  embedding artifacts. This
  segment container/catalog/snapshot/manifest/build/open stack is the
  file-format substrate for the segment-backed base/delta mode.
- Leaf postings now carry persisted maintenance state: mutation version,
  centroid refresh version, payload refresh version, and dirty flags. The state
  is stored as a backward-compatible node side record. Record-backed flat and
  two-level centroid directories read freshness from that small posting-state
  record when available, so payload-only maintenance can skip rewriting the full
  centroid-directory point value.
- A bounded posting maintenance pass now exists. It scans leaf postings,
  repairs dirty centroids/payloads, persists clean posting state, refreshes HBC
  ancestor centroids when needed, and reports repair counters.
- A disabled-by-default `lazy_posting_maintenance` mode now lets foreground
  leaf writes persist dirty posting state while deferring leaf centroid,
  payload, and ancestor refresh work to posting maintenance.
- Dirty posting backlog visibility now exists via `PostingBacklogStats` and a
  `std.Io.Writer` debug renderer, so we can inspect how much deferred work is
  accumulating, including dirty-posting count, oldest dirty mutation version,
  max dirty age, delta-tail postings, max delta records, max tombstone records,
  delta-tail key/value bytes, delta-to-base ratio, centroid lag, and payload
  lag.
- A disabled-by-default bounded automatic repair hook now runs before write
  commit when `auto_posting_maintenance_max_postings` is non-zero. Optional
  dirty-count, dirty-age, delta-tail, tombstone-tail, delta-ratio,
  centroid-lag, and payload-lag gates let us amortize lazy posting repair
  without turning every foreground write into synchronous full repair.
- Automatic repair also exposes `auto_posting_maintenance_max_delta_tail_postings`.
  Fold thresholds can stay high to avoid eager folding, while maintenance
  force-folds only enough postings to keep total delta-tail debt under a
  configured cap.
- Dense-index config parsing and DB/API runtime status now expose the lazy
  posting knobs, posting backlog/maintenance counters, and the configured
  automatic-maintenance policy bounds next to the observed debt.
- DB/API posting status now includes delta-tail record/byte debt,
  tombstone/ratio debt, overfull and at-capacity posting debt,
  boundary-reassignment capacity skips, swap moves, delta-fold
  attempts/skips/records, and the dirty-age,
  delta-tail, centroid/payload lag, layout-change, and overfull-reassignment
  policy caps. These are the production counters and bounds needed to enforce
  the "bounded maintenance, no hidden overfull debt" optimization gate outside
  the write bench.
- Overfull reassignment is no longer only an unlimited opt-in. Maintenance
  options and dense config now support explicit max-overfull-posting and
  max-over-capacity-member limits; direct moves that would exceed those limits
  are skipped or converted to capacity-neutral swaps when possible.
- Existing insert routing now flows through `CentroidDirectory.findPosting`,
  which still delegates to current HBC leaf routing.
- Bounded local posting layout maintenance now exists: oversized postings can
  split, underfull postings can merge with nearby siblings, and sibling
  boundary reassignment can move vectors that are a better local fit elsewhere.
- DB idle maintenance now drains dirty dense posting work outside the foreground
  write hook.
- An opt-in lazy-versus-eager posting maintenance benchmark exists. Current
  local samples show lazy centroid deferral is working, but centroid deferral
  alone is not the dominant write-latency cost in those runs.

## Current HBC Shape

The current HBC already has most of the primitives that an SPFresh-like design
would need:

- internal nodes route through child centroids
- leaves own member IDs
- leaf payloads use RaBitQ to score member vectors approximately
- results are exact-reranked from stored raw vectors
- a vector-to-leaf map already exists

The current persisted key families are effectively:

- `hbc_nodes`: node headers, centroids, children, leaf members, and split ranges
- `hbc_quant`: quantized payloads for node search
- `hbc_vecs`: vector-to-leaf assignments plus standalone-HBC raw vectors and
  vector metadata
- `hbc_meta`: index metadata

Normal DB-backed dense indexes already load raw vectors from the primary
doc/artifact store. For those indexes the high-churn HBC state is structural:
posting membership, centroid/payload refresh, splits, ancestor refresh, and
assignment-map writes.

In the current implementation, one `Node` abstraction does both jobs:

- internal node: `centroid + children`
- leaf node: `centroid + members`

Search walks a tree:

```text
root
  -> score child centroids
  -> expand promising internal nodes
  -> reach promising leaves
  -> score leaf member RaBitQ payloads
  -> exact rerank
```

Quantized payloads also depend on node role:

- root payloads may be non-quantized for bootstrap/special-case behavior
- internal-node payloads quantize child centroids relative to the node centroid
- leaf payloads quantize member vectors relative to the leaf centroid

This means HBC already resembles:

```text
hierarchical centroid index -> leaf postings -> RaBitQ member scoring
```

## What SPFresh Would Change

An SPFresh-style layout would make the leaf/posting layer first-class and put a
separate searchable directory over posting centroids:

```text
centroid directory -> posting IDs -> RaBitQ posting payloads -> rerank
```

The components would be:

1. `CentroidDirectory`
   - one entry per live posting
   - stores `posting_id -> centroid`, count, version, and maybe radius/drift
   - returns top `nprobe` posting IDs for a query
   - can initially be implemented by HBC
   - can later be replaced by exact scan, HNSW, graph routing, or another ANN
     structure over centroids

2. `PostingStore`
   - owns posting membership
   - stores posting centroid
   - stores RaBitQ payload for member vectors
   - tracks tombstones, dirtiness, and version
   - supports local rebuild, split, merge, and compaction

3. `AssignmentMap`
   - maps `vector_id -> posting_id`
   - supports delete/update routing without scanning postings
   - replaces the current vector-to-leaf role at the abstraction boundary

The query path would become:

```text
query
  -> CentroidDirectory.search(query, nprobe)
  -> PostingStore.load(posting_ids)
  -> RaBitQ estimate all members in selected postings
  -> keep candidate_limit
  -> exact rerank from raw vectors
```

## Key Reasoning

### HBC as the centroid directory is not by itself a new index

If we use HBC as the centroid directory and synchronously update it whenever a
posting centroid moves, the result is not meaningfully different from what we
already have.

It would look like:

```text
HBC over centroids -> selected posting IDs -> RaBitQ score posting members
```

Current HBC already looks like:

```text
HBC internal centroids -> selected leaves -> RaBitQ score leaf members
```

That would mostly be an extra layer of indirection.

### The meaningful difference is the maintenance model

The SPFresh-style advantage appears only if postings become mutable units whose
foreground writes stay local:

```text
insert/update/delete
  -> mutate one posting
  -> update assignment map
  -> maybe mark centroid dirty
  -> maybe enqueue split/merge/reassignment
```

Instead of synchronously maintaining every routing consequence:

```text
insert/update/delete
  -> mutate leaf
  -> update leaf centroid
  -> update ancestor centroids
  -> update internal quantized payloads
  -> maybe split leaves/internal nodes
```

If every write deletes and reinserts a posting centroid in a centroid-HBC, then
the design may be worse than current HBC. The value comes from lazy and batched
directory maintenance plus local background repair.

### Current lazy posting maintenance is only an intermediate step

The existing lazy mode can defer centroid, quantized payload, and ancestor
refresh work, but current posting membership is still embedded in the packed HBC
leaf node. That means every non-noop insert still rewrites the packed leaf
member list, even when centroid repair is lazy.

The next storage target is:

```text
posting_base(posting_id, generation)
  -> compact sorted/stable member IDs

centroid_directory(posting_id, generation)
  -> centroid and posting metadata

posting_delta(posting_id, sequence)
  -> insert(vector_id)
  -> tombstone(vector_id)
  -> replace(vector_id)

assignment_map(vector_id)
  -> posting_id
  -> vector/version reference
```

Queries materialize a posting view by reading the immutable base and overlaying
the delta tail. Maintenance folds a bounded tail into a new base generation,
refreshes centroid/payload state, then publishes the new generation.

### The leaf RaBitQ payload can remain conceptually similar

The RaBitQ posting list does not need to be replaced to test the SPFresh
hypothesis. Current leaf payloads already quantize member vectors relative to a
leaf centroid. That maps naturally to:

```text
posting centroid + member vectors -> RaBitQ posting payload
```

The refactor should preserve this machinery where possible.

### The centroid index should be separate from posting payloads

Even if HBC is used as the first centroid directory, the abstraction should
separate:

- how we select postings
- how postings store and score members
- how vector IDs are assigned to postings

This is the seam that lets us change maintenance policy later without rewriting
search and quantization together.

## Expected Performance Implications

### Search/read path

Potential wins:

- direct probing of top posting centroids can avoid some hierarchical routing
  mistakes
- `nprobe` gives a simple recall/latency control
- posting payload IO can be cleaner: centroid directory first, selected
  postings second, exact rerank last

Potential losses:

- a flat centroid directory scan will not scale
- a second HBC layer over centroids may add overhead if maintained eagerly
- more postings selected by `nprobe` may increase RaBitQ scoring work

For mostly bulk-built, read-heavy workloads, current HBC may remain competitive
or better.

### Write/update path

Potential wins:

- foreground writes can touch one posting instead of a tree path
- with base/delta storage, foreground writes can append a small delta instead
  of rewriting the full posting member list
- posting centroid updates can be batched or made approximate
- splits/merges can run as local background work
- write amplification should drop for continuous insert/update/delete workloads

Potential losses:

- background maintenance becomes necessary
- query-time overlay adds CPU and possibly extra reads while deltas are pending
- stale centroids can reduce recall until repaired
- local reassignment logic is more complex than pure tree maintenance
- correctness around versions, tombstones, and concurrent search becomes more
  explicit

The strongest reason to pursue this is high mutable-ingest pressure, not static
search performance alone.

## Key Decisions

1. Refactor before replacing.

   Build seams inside current HBC before adding a separate SPFresh index. This
   avoids duplicating tree/search/quantization behavior before we have measured
   evidence that a new index is needed.

2. Keep HBC as the first `CentroidDirectory`.

   This preserves current behavior while decoupling the API. Later centroid
   directory implementations can be swapped in behind the same interface.

3. Treat current leaves as initial postings.

   Leaf IDs can remain posting IDs during the first refactor. This keeps the
   vector-to-leaf map useful as an initial vector-to-posting map.

4. Keep RaBitQ posting payloads.

   The leaf RaBitQ payload is already the right conceptual primitive for a
   posting list. The first pass should move ownership, not reinvent quantized
   scoring.

5. Do not eagerly update a centroid HBC for every vector write.

   Eager centroid-directory maintenance would erase the main SPFresh-style
   advantage and may increase write amplification.

6. Measure the maintenance policy, not just the index shape.

   The question to answer is whether lazy posting maintenance improves
   write-heavy workloads without unacceptable recall/latency regressions.

7. Split implementation files by HBC core versus SPFresh-style maintenance.

   The code should not fan out into many small modules yet. The useful split is
   one base HBC implementation file plus one SPFresh-style maintenance extension
   file:

   ```text
   go/pkg/antfly/lib/vectorindex/go/pkg/antfly/src/
     hbc_index.zig       # base HBC tree/index mechanics and public facade
     spfresh_index.zig   # SPFresh-style posting maintenance layer
     posting.zig         # shared posting data/state helpers
   ```

   `hbc_index.zig` should keep the base index mechanics:

   - node, vector, metadata, and vector-to-posting load/save helpers
   - search and rerank integration
   - insert/update/delete and batch write paths
   - fundamental HBC tree split/merge primitives
   - internal-node quantized payload helpers
   - compatibility wrappers for the current public API

   `spfresh_index.zig` should own the SPFresh-style policy layer:

   - `postingBacklogStatsTxn`
   - `repairDirtyPostingsTxn`
   - `repairDirtyPostingsTxnWithOptions`
   - `runAutoPostingMaintenanceTxn`
   - local maintenance helpers for posting split/merge decisions
   - sibling boundary reassignment
   - lazy posting centroid/payload refresh policy, where it can move cleanly

   `posting.zig` remains neutral shared infrastructure, not a separate index:

   - `PostingId`
   - `PostingView`
   - `PostingState`
   - `PostingStore`
   - `AssignmentMap`
   - posting maintenance option/result structs

   `spfresh_index.zig` should be an extension over the existing HBC index type,
   not a second object model. Its functions should continue using the current
   generic style:

   ```zig
   pub fn repairDirtyPostingsTxnWithOptions(
       self: anytype,
       txn: anytype,
       options: posting.PostingMaintenanceOptions,
   ) !posting.PostingMaintenanceResult {
       ...
   }
   ```

   During the split, `hbc_index.zig` should re-export wrappers so adapter and
   DB call sites do not churn:

   ```zig
   const spfresh_index = @import("spfresh_index.zig");

   pub fn repairDirtyPostingsTxnWithOptions(
       self: anytype,
       txn: anytype,
       options: posting.PostingMaintenanceOptions,
   ) !posting.PostingMaintenanceResult {
       return spfresh_index.repairDirtyPostingsTxnWithOptions(self, txn, options);
   }
   ```

   This gives us clear naming without claiming there is a fully independent
   SPFresh index implementation. If we later introduce a distinct index type,
   it can reuse `posting.zig` and selected maintenance code behind a cleaner
   interface.

8. Introduce the file format before switching the write path.

   The first base/delta implementation lives in `PostingFormat` and is tested
   without changing HBC behavior. This gives us a stable byte contract and
   overlay semantics before we route production HBC writes through the new
   storage. The migration should preserve the existing packed-node HBC as the
   benchmark baseline.

## Long-Term Roadmap

### Storage axes

The current base/delta work is a new posting record and value format, not a
new physical file backend. Posting membership is represented by explicit
posting base records, append-only posting delta records, and centroid-directory
records, but those records are still persisted through the LSM-backed index
namespace:

```text
PB<posting_id>              -> posting base value
PD<posting_id><sequence>    -> posting delta tail value
CD<posting_id>              -> centroid-directory value
```

That distinction should stay explicit in configuration and docs. The durable
storage substrate and the logical posting format are separate axes:

```text
backend = lsm | segments
format  = packed_hbc | base_delta
version = 1
```

Today this PR is:

```text
backend = lsm
format  = base_delta
version = 1
```

The old default remains effectively:

```text
backend = lsm
format  = packed_hbc
version = 1
```

A future dedicated vector posting file or segment store should be introduced as
`backend = segments`, not by overloading the base/delta format name. If the
physical backend changes while the logical base/delta encoding stays the same,
that is a backend change. If the posting bytes, overlay rules, sequence rules,
or centroid-directory value contract change incompatibly, that is a format
version change.

This avoids misleading names such as `segments_base_delta` while the
implementation still writes LSM records. It also lets us compare high-level
implementations cleanly:

- `lsm + packed_hbc + v1`: current production baseline
- `lsm + base_delta + v1`: current experimental canonical posting store
- `segments + base_delta + v1`: future dedicated posting-file backend, if the
  LSM substrate becomes the bottleneck
- `segments + base_delta + v2`: future dedicated backend plus an incompatible
  posting-value format revision

### Memory and CPU priorities

1. Micro-batch delta writes per posting.

   The current LSM-backed base/delta path can still pay one LSM key per small
   posting update when updates are not already grouped. Ingest paths should
   keep accumulating posting-local operations until a durability, size, or
   visibility boundary requires a flush, then write one ordered delta value.

   Expected win: fewer LSM keys, fewer cursor records, lower write
   amplification, and cheaper delta scans.

   Constraint: the buffering boundary must be explicit. Operations cannot sit
   in an invisible process-local buffer unless the WAL/transaction semantics
   make crash recovery and read visibility obvious.

2. Cache hot materialized overlays by exact replay boundary.

   Repeated queries often touch the same large posting. A bounded cache keyed
   by `(posting_id, base_generation, mutation_version, max_delta_sequence)` now
   stores the materialized member view for exact clean-base and replayed-tail
   states. Longer term, the same boundary could cache a prepared overlay
   summary instead of only final member ids.

   Expected win: avoid repeated base decode and delta replay for hot postings.

   Constraint: cache entries need strict byte accounting, generation-based
   invalidation, and pressure integration with existing resource-manager
   budgets. This should not become an unbounded second posting store.

3. Continue compacting query scratch into slabs.

   `SearchScratch` now groups the fixed query result arrays that grow together
   into slabs: positions, vector ids, metadata, rerank flags, lookups, key
   views, values, and vector views share `query_storage`, while distances and
   error bounds share `distance_storage`. The remaining separate hot buffers
   are for distinct lifetime/shape classes such as vector batches, member ids,
   posting overlay summaries, and posting caches. Small transformed-vector
   matrix loads, external batch metadata scratch, and small metadata batch
   lookups now use stack-backed scratch before falling back to heap allocation.
   Bulk-split external-vector matrix cache misses also keep the common
   missing-id/matrix-position lists on stack scratch before falling back to
   heap allocation. Centroid recompute and quantized
   refresh also use stack-backed paired vector scratch for common dimensions,
   and overfull leaf splitting uses the same shape while materializing split
   inputs, falling back to one combined heap allocation instead of two.
   Routing scratch now slabs child ids, distances, error bounds, and
   competitive candidate storage behind one backing allocation while preserving
   retained-scratch byte accounting.
   RaBitQ estimate scratch now slabs query-diff and four query-code buffers
   behind one backing allocation, so retained query/routing scratch no longer
   pins five small allocations per estimator.
   Local HBC cache clock metadata now slabs key/ref arrays behind one backing
   allocation per cache kind, reducing retained allocation count without
   changing eviction behavior.
   Flat/two-level centroid-directory blocks now slab posting metadata arrays
   and f32 centroid/radius/measure arrays into two backing allocations per
   block, and directory construction fills those slabs directly from sorted
   entries instead of first copying through block-sized scratch arrays. The
   record-backed build path also pre-sizes entry and block lists from exact
   record/posting counts, avoiding geometric growth churn on rebuild. Coarse
   block quantization now reads block centroids through a source view instead
   of copying them into a temporary dense centroid array.
   Packed-node fallback directory rebuilds also pre-size the pending node
   traversal queue from the published node count, avoiding geometric growth
   churn while scanning the HBC tree.
   Directory block construction also reuses one zero-centroid quantization
   vector across all blocks in a rebuild instead of allocating/clearing it per
   block.
   Leaf bounds-radius recompute now streams member vectors through two-vector
   scratch instead of allocating a full leaf matrix during centroid-directory
   repair.
   `SearchScratch` also slabs the fixed transformed-query, centroid, and
   vector work buffers, and single-vector inserts use stack-backed transform
   scratch when possible. Existing-vector single updates reuse stack-backed
   compare/previous-vector scratch instead of allocating separate buffers, and
   batch coalesced existing updates reuse one compare/transform scratch pair
   across posting groups. Posting-delta tail cache entries slab their
   sequence/id/op arrays and discard oversized opportunistic prefetch entries
   instead of retaining large decoded tails in query scratch; prefetch also
   checks value record counts before record decode so oversized tails are
   skipped without building throwaway decoded arrays, and once a prefetched
   posting is over the cache cap the rest of that posting's tail values are
   skipped without header decode. Query/fold overlay append IDs plus live flags
   share one backing allocation. Fold compact delta IDs plus ops are likewise
   slabbed.

   Expected win: lower allocator CPU, fewer fragmented allocations, and better
   cache behavior.

   Constraint: only group arrays with compatible lifetime and growth behavior.
   Avoid making ownership obscure for slices that can be retained separately.

4. Add specialized base decode modes.

   Not every path needs a fully materialized member slice. Base header,
   count/stat, streaming member iteration, delta-stat, and full materialization
   paths are separate. Canonical base/delta membership checks now use the sorted
   base stream plus a focused latest-delta-op scan for one vector, so fallback
   delete scans can avoid materializing whole postings when looking for a single
   member. LSM delta-tail stats also use an all-record decoder when the
   posting-delta key sequence proves the whole value is newer than the base
   generation, avoiding a per-record generation branch without weakening value
   validation. LSM replay paths use the same all-live key boundary for fold and
   materialization scans. Mixed-generation delta-tail replay now grows buffered
   delta-record scratch geometrically per live record instead of reserving the
   rest of the encoded tail after the first live record, keeping retained
   scratch proportional to records that can affect the current base.

   Expected win: fold decisions, backlog stats, and membership checks can avoid
   decoding or allocating full member arrays.

   Constraint: every specialized path must share the same validation rules as
   the full decoder. Corrupt or unsupported values should fail consistently.

5. Add per-block skip metadata and runtime block-size evidence.

   Sorted canonical base members make it possible to store block offsets and
   last-member ids. Base blocks now store a max-member hint alongside the
   block min plus the encoded payload byte length, and validation checks that
   decoded members stay inside that range and consume exactly the advertised
   payload bytes. Sorted membership checks can use the max hint and payload
   length to skip whole blocks for after-block negative lookups without
   decoding each member. Query materialization and fold application could
   later use block offsets to jump to relevant blocks instead of scanning from
   the start. Before changing the public format again, keep using the internal
   bench-only block-size knob for 16/32/64-member blocks and report
   `posting_base_value_bytes_per_member` beside
   `posting_base_decode_ns_per_member`. The comparison runner now has an
   opt-in `ENABLE_POSTING_BASE_BLOCK_SIZE_SWEEP=1` mode that adds HBC
   base/delta read/write arms for the configured
   `POSTING_BASE_MEMBER_BLOCK_SIZE_SWEEP` values without changing the default
   optimized gate. Canonical sorted fold auto-sizing now computes the 16/32/64
   encoded-size candidates in one merged base/delta stream for overlay and
   compact-delta paths instead of replaying the same folded posting three
   times.

   Expected win: faster large-posting negative checks, partial decode, and
   sorted merge application.

   Constraint: block metadata costs bytes. Benchmarks should track base
   bytes/member and base decode ns/member across clustered, sequential, and
   random vector-id distributions before changing defaults.

6. Prefer merge-style overlay application where sortedness is guaranteed.

   Canonical base members are sorted. If append and tombstone summaries are
   also sorted and deduplicated, materialization and fold output can be a
   linear merge instead of hash-map-heavy replay.

   Expected win: lower memory, better cache behavior, and predictable CPU for
   large postings.

   Constraint: shadow mode may still need compatibility checks against packed
   member order while packed HBC remains the source of truth. Merge-style paths
   should be enabled only when the canonical ordering contract is active.

7. Compact overlay plan storage.

   The overlay plan still uses hash maps plus append arrays for large
   churn-heavy tails, but canonical query replay now uses a sorted temporary
   delta-record vector for medium tails before falling back to the hash-backed
   overlay plan. That keeps small and medium sorted tails on a linear merge
   path without allocating removal/append hash maps.

   Expected win: lower peak fold/query scratch and better cache locality.

   Constraint: use measured thresholds. Large churn-heavy tails may still need
   hash-backed deduplication.

8. Stream fold output directly into base encoding.

   Fold paths should avoid holding both a large materialized member slice and a
   second worst-case encoded-base buffer when the final base can be emitted from
   a prepared scratch summary. The target shape is an exact-size prepass or a
   growable writer that caps retained memory after encoding.

   Expected win: lower peak fold memory and less allocator pressure for large
   postings.

   Constraint: the encoder must preserve the v1 validation contract and stable
   ordering. Reservation and rollback behavior must stay compatible with the
   transaction/storage layer.

9. Evaluate frame-of-reference base blocks.

   Varint deltas are simple and good enough for v1, but sorted vector IDs may
   compress better with block-level frame-of-reference plus bit-packed deltas.

   Expected win: fewer bytes/member and potentially faster block decode for
   clustered ids.

   Constraint: this is a format-version candidate, not a small cleanup. It
   needs side-by-side bench columns before becoming public.

10. Add membership hints for large postings.

   The first hint is now in the base block itself: every block stores min and
   max member ids plus payload length, so sorted membership checks can skip
   whole encoded blocks for target ids that fall after a block. For
   delete/update and some validation paths, compact per-base bloom filters or
   richer block hints could further avoid full decode on negative membership
   tests.

   Expected win: faster tombstone and reassignment checks on large postings.

   Constraint: hints add bytes and maintenance cost. They should be optional or
   thresholded by posting size.

11. Make fold policy cost-aware and coalesce fold scans.

   Current fold policy is mostly threshold based. Longer term, folds should
   consider observed query replay cost, tail bytes, tombstone density, and
   maintenance resource pressure. Fold decision scans and replay/application
   scans should also converge toward one streaming pass that buffers only a
   bounded operation summary before deciding whether to fold. LSM tail deletion
   now stores fixed-size posting-delta keys inline while collecting keys to
   delete, avoiding one heap allocation per tail key during folds.

   Expected win: avoid folding cold tiny tails while keeping hot query
   overlays cheap, and avoid duplicate cursor/decode work when folding.

   Constraint: avoid unstable feedback loops. Use slow-moving counters and
   keep hard caps for tail debt. The one-pass fold path should fall back to
   the simpler scanner when the bounded summary overflows.

12. Move to a segment backend only if LSM costs dominate.

    A dedicated posting segment backend could pack posting bases and delta
    runs into vector-index-specific files with posting-local indexes, block
    offsets, compact append batches, and a partial-read integrity scheme.
    Segment v1 blobs now carry whole-segment, index, and per-value checksums,
    and manifests persist the index offset/checksum so lazy point lookups can
    verify the index bytes and fetched value bytes without reading the whole
    segment. HBC now has internal adapter hooks that can write and read posting
    bases, deltas, and centroid-directory records through an owned segment
    runtime store. Segment batch commits now publish the committed manifest via
    the same LSM meta transaction as HBC namespace commits, including bulk
    publish windows, procedural external-vector bulk builds, and posting
    maintenance writes. Runtime segment flushes use the loaded committed
    manifest snapshot as authority instead of trusting any stale physical
    manifest file left by an interrupted or uncommitted write. Dense posting maintenance
    can now run segment directory maintenance from the committed manifest,
    compact selected segment files, collect ignored temp/orphan files, publish
    the replacement manifest transactionally, and report segment run,
    compaction, delete, and manifest-size counters. HBC also has a
    committed-manifest export/import primitive that copies and verifies only
    referenced segment files, publishes manifests last, writes the HBC
    private-store sidecar with a CRC32 trailer through temp+rename, and
    validates the bundled private-store checksum and manifest marker before
    mutating HBC namespaces. Bad bundles therefore fail without rolling the live
    index back to the bundled HBC snapshot. Segment
    maintenance compaction now runs under the dense posting maintenance
    working-set reservation and caps selected compaction input bytes from that
    reservation, so physical segment compaction cannot bypass resource pressure.
    HBC segment recovery now uses the committed manifest as authority, rewrites
    that manifest into the segment directory, skips compaction, and deletes
    ignored temp/orphan physical segment files. DB restore runtime repair now
    calls that recovery hook for segment-backed dense indexes, so restored HBC
    runtimes normalize physical segment artifacts against the committed
    manifest before completing repair. Public dense config also accepts the
    opt-in `backend = segments, format = base_delta, version = 1` combination
    while leaving the default on `lsm + packed_hbc + v1`. DB and IndexManager
    now expose aggregate dense posting segment snapshot export/import hooks
    that copy every segment-backed dense index into a deterministic
    `dense-posting-segments/<index>` bundle. Each bundle carries a versioned
    HBC private-namespace snapshot plus the referenced segment files, so DB
    snapshots and restores can preserve segment-backed dense indexes through
    the public snapshot API instead of relying on external orchestration to
    stitch HBC metadata and segment files together. Segment query, lazy snapshot
    materialization, and explicit base/delta materialization now decode segment
    bases into retained scratch, append committed and pending delta tails
    directly into that scratch, replay globally sequence-sorted delta records,
    and use compact sorted merge for small canonical tails instead of allocating
    owned intermediate base and delta slices before producing the final member
    view. Segment folds also include pending runtime-batch bases and deltas in
    their threshold decision and materialization before publishing the folded
    base, so a fold in the same HBC transaction as a delta append cannot drop
    that in-flight tail. Segment materialization likewise sorts committed and
    pending delta sources into one replay stream when a write transaction has
    buffered deltas, and segment single-member lookup compares the same global
    sequence space through latest-record helpers instead of loading full tails
    for the one-vector case. Segment latest-member scans also coalesce matching
    delta values into one contiguous range read per segment before checking the
    target vector. Segment batch base prefetch now coalesces adjacent posting
    base point values from the same segment into bounded range reads, so
    flat/two-level directory probes do not turn a selected posting batch into
    one file read per base value. The same newest-first batch scan keeps a
    compact unresolved-position list, so older segments only check postings
    still missing from the batch, and its per-segment point-read candidates are
    stack-backed for common probe windows. That preserves the format's sequence
    contract across the file/runtime boundary. Query scratch replay applies that combined stream
    directly into retained member scratch, using sorted merge for canonical bases
    and avoiding a second owned materialized member buffer on pending-overlay
    reads while preserving canonical sorted output; segment materialization now
    also reuses no-resort sorted replay helpers after its own delta ordering
    pass instead of sorting the same record buffer twice. When a sorted
    committed manifest has a newer base segment, lazy replay also starts delta
    scanning at that base segment instead of visiting older manifest entries
    only to skip them. Fold replay for committed
    segment bases can now stream the same sorted delta summary straight from the
    encoded base bytes into the replacement base encoder, so large committed
    bases do not need a decoded member slice during fold. Query replay, lazy
    snapshot replay, explicit materialization, and fold replay all append segment
    delta records directly into retained scratch; folds collect tail stats in
    that same pass. Pending and committed segment delta entries now use their min
    sequence to select all-record stats/replay/latest-op paths when the whole
    value is newer than the base generation. This removes the duplicate
    stats-then-replay scans, delta-location lists, and owned delta slices that
    existed before. Segment delta-tail stats also read a posting's contiguous
    delta-value range once per segment instead of issuing one range read per
    delta value while still verifying each value checksum. Mixed-generation
    segment delta filtering now grows list/scratch output by live records rather
    than reserving the rest of the encoded tail after the first live record.
    Fold scratch release now resets transient replay state and
    trims retained buffers under the configured byte budget before caching the
    scratch for reuse, so exact-size fold encoders do not still pin a previous
    worst-case posting's memory. The HBC
    read/write benches now expose
    `posting_backend` in result JSON, and the comparison runner has an opt-in
    `ENABLE_POSTING_SEGMENT_BACKEND_COMPARISON=1` mode that adds segment-backed
    base/delta read and write arms without changing the default optimized
    gate. The segment directory batch writer also accounts exact encoded bytes
    for pending delta micro-batches before flush, so value-byte bounds apply
    to buffered delta records rather than only already-encoded point values.
    The comparison summarizer now also carries segment-relevant physical
    IO columns, including manifest writes, renames, deletes, read-call
    breakdowns, and bytes-per-read/write-call ratios, so segment-vs-LSM
    comparisons can separate logical posting cost from backend file behavior.
    That may reduce LSM key overhead and improve sequential IO.

    Expected win: lower LSM fanout, fewer small keys, better posting-local read
    locality, and format-specific compaction.

    Constraint: this is a storage-backend project, not a posting-format tweak.
    It needs its own crash/recovery, compaction, checksumming, resource
    accounting, backup/restore, and migration story. Do it only after metrics
    show the LSM substrate is the limiting cost for `lsm + base_delta + v1`.

### Benchmark and observability work

The next optimization decisions need counters that separate logical format
cost from backend cost. This branch now emits the base byte/decode, delta
byte/replay, posting-family LSM cost, query materialization cost, fold scratch,
and search scratch allocation/retention counters. The full set to keep
tracking before making a segment-backend decision is:

- base bytes/member
- base decode ns/member
- delta bytes/record
- delta replay ns/record
- LSM keys per posting mutation
- LSM bytes per posting mutation
- fold peak scratch bytes
- fold output bytes per folded record
- overlay cache hit/miss/admission/eviction counts
- query materialization ns/posting and ns/member
- search scratch allocation count and retained bytes

The write comparison summary now reports both the foreground delta density
(`fg_delta_value_bytes_per_record`) and the total posting delta encoding density
(`posting_delta_value_bytes_per_record`) so segment-vs-LSM rows can separate
foreground grouping behavior from the full posting-format byte cost. Write and
read summaries also carry `posting_base_member_block_size`, so the opt-in
16/32/64 block-size sweep can be interpreted directly beside base bytes/member
and decode ns/member instead of relying on row labels alone. Probe summaries
also derive skipped blocks, decoded blocks, and decoded members per call, which
makes block-pruning efficiency comparable across different sample counts. Fold
summaries likewise normalize retained peak scratch as bytes per folded record
and as ratios to folded base bytes and deleted tail bytes, so fold-memory
pressure can be compared across workloads.

Benchmarks should report these by workload shape:

- bulk build, read-heavy
- append-only ingest
- hot overwrite
- random overwrite
- semantic drift overwrite
- delete/tombstone-heavy churn
- mixed insert/delete/update
- clustered vector ids versus random vector ids

The roadmap should remain evidence driven. `lsm + base_delta + v1` is the
right next comparison point against packed HBC. A dedicated segment backend is
only justified if those measurements show LSM key/value overhead, cursor work,
or LSM compaction is the dominant remaining bottleneck.

## Refactor Plan

### Phase 1: Name the boundaries

Objective:
- introduce interfaces/types without changing behavior

Plan:
- define a `PostingId` alias that initially maps to current leaf node IDs
- define a `PostingStore` wrapper over current leaf members, centroid, and
  quantized payload access
- define a `CentroidDirectory` wrapper over current HBC routing behavior
- define an `AssignmentMap` wrapper over current vector-to-leaf keys
- keep existing search and write tests passing

Acceptance:
- no material behavior change
- no new index format required
- current HBC search results remain equivalent within existing tolerances

### Phase 2: Move leaf operations behind `PostingStore`

Objective:
- make leaf member and RaBitQ operations posting-owned

Plan:
- route leaf member reads/writes through `PostingStore`
- move leaf quantized rebuild/update logic behind posting operations
- expose posting-level operations:
  - `loadPosting(posting_id)`
  - `appendMember(posting_id, vector_id, vector)`
  - `removeMember(posting_id, vector_id)`
  - `rebuildPosting(posting_id)`
  - `splitPosting(posting_id)`
- keep internal-node quantized payloads in HBC for now

Acceptance:
- leaf mutation logic is no longer spread across generic node operations
- posting rebuild can be called independently from tree maintenance
- existing HBC writes still behave the same by default

### Phase 3: Move routing behind `CentroidDirectory`

Objective:
- make "find postings for query" separate from "score posting members"

Plan:
- introduce a search path that asks `CentroidDirectory` for posting IDs
- initially implement it using current HBC traversal
- preserve current beam/search-width behavior as the default
- keep exact rerank unchanged

Acceptance:
- search can be described as:

  ```text
  directory.search(query) -> posting IDs
  posting_store.score(posting IDs) -> candidates
  rerank(candidates) -> final results
  ```

- current HBC remains the default directory implementation

### Phase 4: Add posting dirtiness and lazy centroid refresh

Objective:
- create the first real SPFresh-style behavior behind the refactored boundary

Plan:
- track posting count, tombstone count, centroid version, and dirty score
- allow small inserts/deletes to update posting state without immediately
  refreshing the centroid directory
- enqueue dirty postings for background rebuild/refresh
- support a foreground fallback for excessively dirty postings

Acceptance:
- write path can avoid synchronous ancestor/directory refresh for configured
  workloads
- search can tolerate stale posting centroids via versioned posting loads
- metrics expose dirty postings and refresh lag

### Phase 5: Local split/merge and boundary reassignment

Objective:
- test the real SPFresh maintenance hypothesis

Plan:
- split oversized postings locally
- merge underfull or tombstone-heavy postings with nearby postings
- after split/merge, refresh the centroid directory entries
- if we enforce SPFresh-style NPA, scan only nearby postings for boundary
  vectors that should be reassigned after split/merge
- keep boundary reassignment bounded and background-driven

Acceptance:
- foreground write amplification drops on continuous update workloads
- recall remains within an agreed tolerance
- assignment-map/list mismatches remain treated as consistency bugs, not normal
  maintenance debt
- background maintenance debt is bounded

### Phase 6: Posting base/delta storage

Objective:
- make posting membership a first-class storage unit instead of packed leaf
  node payload

Plan:
- define immutable posting base records with posting id, generation, and base
  member IDs
- define independent centroid-directory records with posting id, generation,
  centroid, and posting metadata
- define append-only delta/tail records for insert, tombstone, and replacement
  operations
- materialize query/maintenance views by overlaying base plus ordered deltas
- fold bounded delta tails into new base generations in maintenance
- group same-posting batch mutations into one delta key/value when the caller
  already has an ordered posting-local mutation set
- keep the current packed HBC leaf format available as the baseline
- treat the explicit posting base, posting delta, and centroid-directory bytes
  as a new file format with strict magic/version decoding and no legacy
  posting-list variants. If we ever need to move data between posting-list
  formats, that should be an explicit migration tool or rewrite pass, not extra
  branches in the hot decode path.

Acceptance:
- format encode/decode and overlay tests pass
- write benchmarks can count packed-leaf rewrites versus delta appends
- query benchmarks can compare base-only and base-plus-tail overlay cost
- maintenance benchmarks can measure fold cost, tail debt, and post-fold query
  behavior

Current slice:
- `PostingFormat.encodeBase` / `decodeBase`
- `PostingFormat.encodeDeltaTail` / `decodeDeltaTail`
- posting base and delta records now use compact exact-size v1 byte layouts:
  base records store only magic, version, posting id, generation, member count,
  and member ids; delta records store only magic, version, record count, and
  packed `(sequence, op, vector_id)` entries. The posting-list decoders reject
  unsupported versions, corrupt lengths, and unsupported delta ops instead of
  carrying legacy/future variant branches in the hot path.
- grouped delta tails with at least three records now use a compact v2 value
  layout: magic, version, record count, base sequence, and packed
  `(u32 sequence_offset, op, vector_id)` entries. Single-record and two-record
  tails keep the v1 layout because v2 would be larger or equal. The decoder
  still accepts v1 tails, rejects compact sequence overflow, and treats other
  versions as unsupported, so existing base-delta tail values remain readable
  while grouped overwrite/append batches use fewer value bytes.
- `PostingFormat.materializeMembers` and
  `materializeMembersAfterGeneration`
- `PostingStore.appendDeltaRecords` writes multiple ordered posting-local delta
  records under one tail key, so grouped insert/delete batches can reduce LSM
  key count without changing query overlay semantics
- batched existing-vector reroutes now normalize source tombstones and grouped
  target inserts to the posting's final mutation generation with ordered
  per-record offsets before appending the tail value. That avoids duplicate or
  generation-gapped sequences inside one grouped tail and lets the compact v2
  value layout apply to multi-reroute source/target batches.
- delta-tail folds now report deleted tail key/value bytes, replacement base
  key/value bytes, and folded record/member counts, so benchmark rows can
  verify both sides of the maintenance write-amplification tradeoff:
  posting-local delta keys should collapse into one compact base generation,
  but the replacement base bytes per folded record must stay bounded
- `CentroidDirectoryFormat.encode` / `decode` records posting-id, generation,
  mutation version, payload version, dirty flags, parent, level, member count,
  bounds radius, and centroid independently of the posting base blob. These
  records are the query-time source of posting metadata for flat/two-level
  base-delta directories.
- `AssignmentFormat.encode` / `decode` records posting-id, assignment version,
  and flags in a compact fixed-size value. The vector id and current vector
  reference are implied by the `hbc.encodeAssignmentKey` key because dense
  vectors already live in the main DB store. Assignment compatibility is
  separate from posting-list storage; the new posting base/delta and
  centroid-directory decoders accept only the current v1 bytes, with unsupported
  assignment/centroid flags rejected at decode time.
- `hbc.encodePostingBaseKey` and `hbc.encodePostingDeltaKey` provide stable
  namespaced keys for base records and ordered delta records. Delta-tail cursor
  scans now accept only the exact current `PD<posting-id><sequence>` key shape,
  so future posting-list variants cannot be silently interpreted as v1 tail
  records in query or maintenance paths.
- `hbc.encodeCentroidDirectoryKey` provides a separate stable key family for
  posting-id -> centroid-directory records
- `hbc.encodeAssignmentKey` provides a separate stable key family for versioned
  assignment-map records while preserving the legacy `l:` vector -> leaf key
- `PostingStore.saveBase`, `loadBase`, `appendDelta`, `loadDeltaTail`,
  `materializeBaseDeltaMembers`, and `foldDeltaTailIntoBase` persist, scan,
  overlay, and fold the new records through existing transactional namespace
  helpers
- `PostingStore.saveCentroidDirectoryRecord` /
  `loadCentroidDirectoryRecord` persist the separate centroid-directory record;
  `loadCentroidDirectoryRecords` scans the `CD` key family directly when a
  cursor-capable transaction is available
- `AssignmentMap.put` and `delete` keep the legacy vector -> leaf mapping for
  update/delete routing. `shadow_base_delta` still dual-writes the versioned
  assignment record, while canonical `base_delta` derives `getRecord` from the
  legacy vec-leaf key instead of writing a duplicate current assignment-map key
  for every append.
- `HBCConfig.posting_storage_mode = .shadow_base_delta` opt-in dual-writes
  posting base records on leaf saves and posting delta records at semantic
  insert/replace/tombstone mutation points while keeping packed HBC reads as
  the source of truth
- `HBCConfig.posting_storage_mode = .base_delta` is the first experimental
  canonical posting-store mode. Incremental leaf writes keep the packed HBC node
  record for routing metadata, but omit packed leaf member IDs and use
  posting-base plus posting-delta materialization as the authoritative member
  view.
- generic HBC loads and DB adapter pointer/debug paths now overlay
  `posting_base + posting_delta` into leaf member views in `base_delta`, so
  cached nodes, validation, upper-tree pinning, and routing fallback paths do
  not depend on packed leaf member IDs
- structural leaf split saves publish a fresh full posting-base snapshot in
  `base_delta`, and split/merge outputs advance persisted posting state before
  saving. This prevents stale pre-split append deltas from being replayed over a
  new partitioned posting base.
- node deletion now also removes explicit posting-base, posting-delta, and
  centroid-directory records when the transaction supports delta-tail scans, so
  a record-backed centroid directory does not probe dead postings after splits,
  merges, or deletes and old posting tails do not linger as query/repair debt
- default bulk-build leaf saves now use the known-vector, base/delta-aware leaf
  save path. Parent updates in `base_delta` refresh centroid-directory parent
  metadata and invalidate stale root/non-root quantized payload shape lazily;
  internal-node quantized rebuilds load raw child node centroids instead of
  forcing a posting overlay, because child membership is not needed for that
  operation.
- the same shadow mode also dual-writes centroid-directory records on leaf saves
  and after bounded centroid repair
- HBC query leaf scoring now asks `PostingStore.copyQueryMemberIds` for member
  IDs. In `shadow_base_delta` mode it attempts to materialize base+delta,
  records overlay counters in `SearchProfile`, and falls back to packed HBC if
  the base is missing, older than the packed posting state, or does not exactly
  match packed member order. The exact-match guard is required while quantized
  payloads are still built in packed-member order.
- In canonical `base_delta` flat/two-level directory search, the selected
  centroid-directory probe now carries posting parent, level, and persisted
  posting state, so query materialization goes straight through the posting
  store and does not read packed leaf nodes as a metadata fallback.
- Flat/two-level centroid-directory selection now consumes the resolved search
  epsilon when scoring quantized posting-centroid candidates. Sorted lower
  bounds stop exact centroid scoring once the best exact posting centroid is
  outside the effort window, while inner-product search remains unpruned to
  match the HBC dynamic-pruning guard.
- delta sequence high bits are treated as the posting mutation generation; query
  overlay and maintenance fold ignore records at or below the base generation so
  full base snapshots are authoritative over older shadow deltas.
- standalone dirty-posting repair uses a cursor-capable write transaction so
  shadow and canonical `base_delta` tails can fold into a new base after
  writes. Its default fold policy is thresholded
  (`min_delta_records_to_fold=64`, `min_tombstone_records_to_fold=16`, ratio
  trigger disabled) so repair does not recreate foreground write amplification
  by folding every tiny tail. Explicit eager fold remains available by setting
  the thresholds to `1/0/0`, and ratio-based folding remains available through
  explicit options and DB idle-maintenance policy. Automatic repair invoked
  inside a namespace batch still skips tail folding because batches do not
  expose cursors
- automatic posting maintenance is now policy-shaped instead of only
  `max_postings`: `HBCConfig` exposes fold thresholds, layout-change budgets,
  a max-delta-tail debt cap, dirty-count/age trigger gates,
  delta-tail/tombstone/ratio trigger gates, centroid/payload lag trigger gates,
  overfull/full-posting capacity gates, capacity-safe boundary reassignment
  budgets, opt-in full-posting splits for creating layout slack, and an
  explicit overfull opt-in. Zero trigger gates preserve the old "run whenever
  max_postings is non-zero" behavior. For
  canonical `base_delta`, auto repair skips batch transaction contexts that
  cannot scan delta tails; the explicit background repair API remains the
  cursor-capable path for draining that debt.
- DB idle posting maintenance now treats dirty postings, actionable delta-tail
  fold debt, and layout debt as separate reasons to run. Clean postings with
  delta tails can be folded without requiring a synthetic dirty flag, and
  successful fold calls count as idle maintenance progress. The DB scheduler
  also accepts per-run index-count and elapsed-time budgets
  (`ANTFLY_DENSE_POSTING_IDLE_MAX_INDEXES_PER_RUN`,
  `ANTFLY_DENSE_POSTING_IDLE_MAX_ELAPSED_NS`) plus overfull/full-posting
  trigger thresholds (`ANTFLY_DENSE_POSTING_IDLE_MIN_OVERFULL_POSTINGS_TO_RUN`,
  `ANTFLY_DENSE_POSTING_IDLE_MIN_POSTINGS_AT_CAPACITY_TO_RUN`) with defaults
  that preserve the current "first layout-debt posting is actionable" behavior.
  Base-delta tail folding is also thresholded and capped through
  `ANTFLY_DENSE_POSTING_IDLE_FOLD_DELTA_TAILS`,
  `ANTFLY_DENSE_POSTING_IDLE_MIN_DELTA_RECORDS_TO_FOLD`,
  `ANTFLY_DENSE_POSTING_IDLE_MIN_TOMBSTONE_RECORDS_TO_FOLD`,
  `ANTFLY_DENSE_POSTING_IDLE_MIN_DELTA_TO_BASE_RATIO_BPS`, and
  `ANTFLY_DENSE_POSTING_IDLE_MAX_DELTA_TAIL_POSTINGS`; the default ratio
  trigger is disabled so tiny clean tails are not folded solely because their
  posting is small, while the explicit tail cap can still force bounded debt
  drainage.
  This lets production-shaped background maintenance be bounded independently
  from LSM compaction and avoids attempting clean at-capacity postings until
  policy says the accumulated layout debt is worth a repair pass. Profiled
  dense searches also feed a query-pressure guardrail:
  `ANTFLY_DENSE_POSTING_IDLE_MAX_PROFILED_SEARCH_NS=0` disables it, while a
  non-zero value makes idle posting maintenance skip when a recent profiled
  dense query exceeded that latency budget. The recency window defaults to one
  second and is controlled by
  `ANTFLY_DENSE_POSTING_IDLE_PROFILED_SEARCH_MAX_AGE_NS`, so one cold or slow
  profiled query cannot suppress idle repair indefinitely. The profiled
  idle-maintenance path reports scanned indexes, repaired indexes, elapsed
  time, and whether the pass stopped on index-count, elapsed-time, resource, or
  query-guardrail budget.
  `DBStats.dense_posting_maintenance` preserves the cumulative and last-pass
  scheduler result plus observed profiled dense-search latency through runtime
  snapshots and provisioned runtime-status Prometheus metrics, so bounded
  maintenance debt is observable instead of inferred from backlog deltas alone.
  When a bounded pass reports remaining debt via `limit_reached`, `runUntilIdle`
  and the submitted background maintenance job now repeat dense posting
  maintenance while progress continues, stopping only when debt is drained or a
  scheduler budget/resource/query guardrail fires.
- Dense posting maintenance is now part of the shared runtime abstraction
  surface instead of a bespoke side loop. The DB exposes a `BackendRuntime`
  maintenance-job submission path for the same idle repair pass, and the pass
  reserves `ResourceManager` slice
  `dense.posting_maintenance_working_set` before mutating postings. Resource
  pressure can therefore defer/reject posting repair independently from LSM
  compaction, dense replay, search scratch, and text merge buffers, and
  `DBStats.dense_posting_maintenance`, runtime-status snapshots, and
  provisioned Prometheus metrics report resource-budget stops separately from
  index-count and elapsed-time stops. The focused DB tests cover both the
  inline/manual and threaded `BackendRuntime` durable-job lanes, including the
  combined threaded-lane plus `ResourceManager` rejection path.
- Metadata-optional external vector loading now applies to both batch loaders
  and single-vector scratch fallbacks. That matters for DB-backed dense indexes:
  posting repair, centroid recompute, and RaBitQ refresh can load vectors by
  vector ID through the external loader without requiring duplicate vector
  metadata in the dense index's private keyspace. Metadata-addressed loaders
  keep the old strict behavior by leaving `external_vector_metadata_required`
  enabled.
- Large external-vector evidence should use the DB-backed vector-store shape,
  not the standalone-HBC "own every vector payload" shape. `hbc-write-bench`
  now has `--dataset-mode procedural` for
  `online_batches_dense_external_vectors_empty` and
  `online_batches_dense_external_vectors_per_batch_session_empty`: it generates
  deterministic vectors per batch, installs the normal external-vector loader,
  forces index-local vector payload storage off for those VDBB-style rows, and
  computes post-write recall by regenerating exact ground truth instead of
  materializing a `vectors * dims` array. This is the practical path for
  1M x 1536 VectorDBBench-shaped runs while preserving the production contract
  that vectors live in the main DB store and the dense index stores posting
  membership plus assignment metadata. `--post-write-recall-mode self_hit`
  adds a scalable diagnostic mode for large procedural runs: it still executes
  real ANN read-after-write queries and records QPS, latency, and profile
  counters, but validates only whether the queried vector id is returned. Exact
  recall remains the default and remains required for the optimized gate. The
  comparison runner exposes opt-in `ENABLE_VDBB_PROCEDURAL=1` rows for packed
  HBC, canonical base/delta HBC, canonical base/delta flat RaBitQ, and
  canonical base/delta two-level RaBitQ, plus
  `ENABLE_VDBB_1M_PROCEDURAL=1` rows for `write_vdbb_1m_procedural_*` labels
  that default to `VDBB_1M_VECTORS=1000000`, exact post-write recall, 100
  post-write query vectors, and k=10. That gives the optimized gate a 1000-item
  exact-recall denominator for each required 1M VDBB row, but the synthetic
  procedural exact oracle is still a brute-force generated-vector scan. The
  cosine path avoids materializing and normalizing a candidate vector for every
  row by computing exact generated-vector cosine distance in one pass, which
  cuts a large constant factor but does not change the asymptotic scan cost.
  Materialized and generated post-write exact truth is precomputed before timed
  query loops, so post-write QPS and p95 rows measure ANN search/rerank work
  instead of brute-force oracle work. Generated exact truth is also cached per
  repeated post-write query row so warm-round query timings are not polluted by
  recomputing the exact oracle.
  The comparison runner now passes a shared `--post-write-truth-cache-path` for
  comparable procedural write rows: online VDBB rows share one file per
  vector/query shape, and procedural mutation rows share one file per
  distribution. The cache header includes seed, vector/query shape, metric,
  dataset mode, live-row count, and a hash of active rows plus override vectors,
  so hot/random/semantic/append/mixed workloads cannot silently reuse each
  other's truth. Write summaries preserve `post_write_exact_truth_cache_hit`
  and `post_write_exact_truth_build_ns`, plus the corresponding `pre_repair_*`
  fields for mutation rows. This still leaves the first exact 1M synthetic
  oracle expensive, but it prevents paying that brute-force cost again for each
  packed/base-delta comparison row.
  Procedural mode now also supports warm random-overwrite,
  semantic-drift-overwrite, append-streaming, and mixed insert/delete/update
  rows. Those rows build the warm index from generated vectors, materialize
  only changed/appended vectors, and install an override-aware external-vector
  loader so post-write repair, rerank, exact recall, and self-hit diagnostics
  see the current vector values without allocating the full `vectors * dims`
  matrix. The comparison runner exposes those rows through
  `ENABLE_VDBB_PROCEDURAL_MUTATIONS=1` and
  `ENABLE_VDBB_1M_PROCEDURAL_MUTATIONS=1`; the same slow exact-oracle guard
  applies at 1M scale. The optimized-gate summarizer treats these procedural
  mutation rows as DB-backed vector evidence and, for hot/random/semantic/
  append/mixed distributions, selects the largest available row so 1M
  procedural mutation evidence can satisfy the distribution checks without
  requiring a materialized 1M x 1536 vector matrix.
  The focused bench unit step verifies that one-pass generated cosine distance
  matches the materialized-vector exact distance:

  ```text
  zig build hbc-write-bench-test
  ```

  The runner fails fast when that scan would exceed
  `VDBB_1M_EXACT_RECALL_MAX_OPS` (default `20_000_000_000` generated-vector
  dimensions) unless the selected 1M row's exact-truth cache already exists or
  `ALLOW_SLOW_VDBB_1M_EXACT_RECALL=1` is set. This keeps accidental brute-force
  1M oracle work out of normal runs while still allowing cached exact-recall
  rows to be appended to a resumable result file. Use
  `VDBB_1M_POST_WRITE_RECALL_MODE=self_hit` for 1M performance diagnostics, or
  use real VectorDBBench query/ground-truth artifacts for the optimized 1M
  recall proof instead of the synthetic brute-force oracle. Even the 1M
  `self_hit` rows still build the full 1M-vector packed/base-delta index, so
  they are long-running benchmark jobs rather than smoke validation rows.
  `VDBB_1M_POST_WRITE_RECALL_MODE=self_hit` remains an explicit diagnostic
  override only and cannot satisfy the optimized gate. The procedural VDBB rows
  use posting-count-derived two-level directory block sizing instead of
  inheriting the small default comparison geometry.
  Flat/two-level online ingest now publishes deferred node keys at each safe
  batch boundary inside a bulk-ingest session so foreground routing can see the
  staged tree while the public search snapshot remains gated by the session
  finish path.
- The write bench can now build generated exact post-write truth caches without
  building or querying an index by passing `--post-write-truth-cache-only` with
  `--dataset-mode procedural`, exact recall mode, a single supported workload,
  and `--post-write-truth-cache-path`. The comparison runner exposes the same
  path for selected 1M rows:

  ```text
  env ENABLE_VDBB_1M_PROCEDURAL=1 \
    PREBUILD_VDBB_1M_EXACT_TRUTH_CACHES=1 \
    RUN_LABELS=write_vdbb_1m_procedural_base_delta_two_level_rabitq_bounded_maintenance \
    RESULT_DIR=zig/bench/results/spfresh-hbc-comparison-1m \
    zig/scripts/run_spfresh_hbc_comparison.sh
  ```

  This builds the selected 1M exact-truth cache files and exits without
  appending benchmark rows. Later exact-recall runs with the same result
  directory load those files and can pass the slow-oracle guard without setting
  `ALLOW_SLOW_VDBB_1M_EXACT_RECALL=1`. Generated cosine exact-truth scans also
  precompute the query norm once per query and candidate norms once per cache
  build before scanning candidates, avoiding repeated `dims`-wide norm
  accumulation while preserving the same distance formula and top-k tie-breaker.
- DB idle maintenance defaults to bounded full-posting split scheduling while
  keeping overfull reassignment disabled. That creates layout slack for
  capacity-safe reassignment under background budgets instead of buying recall
  with unbounded overfull posting debt.
- Bounded posting repair results now report remaining dirty, delta-tail,
  overfull, at-capacity, and max-over-capacity debt after the pass, and set
  `limit_reached` when the configured maintenance budget did not drain the debt
  it is responsible for. Write-bench rows emit matching
  `posting_repair_*_remaining_*` fields, and the optimized gate requires the
  production proof log to include a bounded split-budget reporting test.
  `IndexManager.DensePostingMaintenanceResult` and
  `DBStats.dense_posting_maintenance` now preserve those remaining-debt fields,
  including last-pass dirty/delta-tail/overfull/at-capacity debt and
  limit-reached index counts, so production status can tell the difference
  between "bounded pass made progress" and "bounded pass finished all debt".
- Non-overfull maintenance policies now publish explicit zero overfull debt
  limits by default. The write bench, HBC runtime config, and parsed dense-index
  config all report `max_overfull_reassignment_postings=0` and
  `max_over_capacity_reassignment_members=0` when overfull reassignment is
  disabled; the comparison runner keeps separate finite defaults only for the
  explicit overfull contrast row. The optimized gate now requires required
  non-overfull repair and automatic-maintenance rows to publish `0/0` limits.
  Existing 1M artifacts collected before this change still show
  `unbounded/unbounded` until those rows are rerun.
- A bounded repair pass no longer lets an exhausted dirty-refresh budget prevent
  later layout work in the same scan when layout-change budget remains. This
  keeps capacity debt draining independently from ordinary centroid/payload
  refresh budget.
- multi-write `base_delta` batches that may update existing vector IDs use a
  cursor-capable write transaction instead of the namespace batch path, because
  leaf loads need to materialize posting base+delta state
- overwrite-heavy write-bench workloads now enable same-leaf overwrite
  coalescing by default. When an existing vector still belongs to its current
  posting, the write path refreshes vector/metadata state and dirties the
  posting without appending a replacement posting-delta key. Pass
  `--no-coalesce-overwrite-leaf-writes` to measure the uncoalesced baseline.
- `overwrite_same_leaf_vectors_warm` is the clean same-posting refresh
  workload: it keeps vector values stable and changes metadata, so benchmark
  rows can separate same-leaf overwrite cost from semantic-drift replacement
  cost.
- canonical lazy `base_delta` replacements for existing vector IDs are
  posting-local: they update the current vector/assignment, append a replacement
  delta to the current posting, persist dirty posting state, and defer
  centroid/payload/relocation repair
- mixed write batches now coalesce existing-vector replacement deltas by current
  posting before routing remaining absent inserts through the grouped insert
  path. A focused tiny mixed smoke improved foreground delta density from
  27 records / 22 physical appends to 32 records / 14 physical appends, clearing
  the 2.0 grouped-density gate at that shape.
- writes-only overwrite batches now use the same cursor-transaction coalescing
  pass as mixed batches. For canonical `base_delta`, an existing vector that
  stays in its current posting updates vector/metadata state and dirty posting
  maintenance state without appending a replacement delta record, because the
  posting membership did not change. Delta records are reserved for real
  membership edits such as tombstone/insert moves. A focused hot-overwrite auto
  maintenance smoke dropped the row from one physical delta key per logical
  overwrite to 102 logical delta records over 65 physical appends while keeping
  same-posting replacement records out of the tail.
- moved overwrite target inserts are now grouped by target posting before
  appending delta tails. On the same focused hot-overwrite auto-maintenance
  smoke, per-source grouping dropped physical foreground delta appends from
  65 to 41 for the same 102 logical delta records, and batch-wide target
  grouping dropped them again to 27, raising foreground delta density to
  3.78 records per physical append.
- lazy existing-vector reroutes now refuse full target postings when
  `base_delta` is the canonical posting store. If the nearest posting is at
  capacity and overfull reassignment is disabled, the overwrite stays in the
  source posting, updates vector/metadata state, dirties posting maintenance
  state, and leaves relocation to bounded background repair. The apply path no
  longer pre-counts a reroute before the delegated insert path decides whether
  a physical move happened. A focused hot-overwrite auto-maintenance smoke with
  `AUTO_SPLIT_FULL_MAX_LAYOUT_CHANGES=2` kept remaining overfull and
  over-capacity debt at `0 / 0` while reducing storage write bytes to about
  `1.40x` packed HBC for that tiny shape.
- `hbc-write-bench --posting-storage shadow_base_delta --workload <name>`
  exposes posting base/delta, centroid-directory, and assignment-map write
  counters alongside existing packed-HBC, host-storage, LSM, latency, and
  `vectors_per_second` counters. `posting_delta_append_calls` versus
  `posting_delta_records` now shows whether grouped posting mutations are
  sharing physical delta keys, and `posting_delta_fold_deleted_tail_keys` /
  `posting_delta_fold_deleted_tail_value_bytes` show whether repair reclaimed
  those physical tail records after folding.
- warm append/overwrite/mixed rows also emit `foreground_*` profile counters
  captured immediately after the foreground mutation phase and `repair_*`
  counters for the optional explicit repair pass. Use `foreground_*` fields for
  write-amplification comparisons; aggregate profile fields still describe the
  final post-repair row.
- `hbc-write-bench --workload overwrite_hot_vectors_warm` builds a warm index
  and repeatedly overwrites a configurable hot set of existing vector IDs using
  `--overwrite-hot-keys` and `--overwrite-rounds`, so overwrite-specific
  structural work, posting deltas, fold work, host IO, and LSM bytes are
  visible
- `hbc-write-bench` now exposes the automatic posting-maintenance policy knobs
  (`--auto-posting-maintenance-*`) and emits them on each JSON row, so
  pre-commit/background-style repair can be measured separately from explicit
  `--repair-postings-after-write` passes. It also emits backlog counters after
  the write phase, so "skipped repair" rows show the remaining dirty posting
  count, max dirty age, delta-tail count, max tombstone tail, max delta ratio,
  centroid lag, and payload lag.
- `hbc-write-bench --post-write-queries <n>` now runs a same-index query phase
  after foreground writes and optional post-write posting repair. It reports
  post-write QPS, read IO, search workspace bytes, scored-vector counts, and
  posting-overlay counters on the same JSON row as the write result. With
  `--post-write-query-rounds <n>`, it also reports warm-round counters that
  exclude the first query round, so base/delta cold overlay misses can be
  separated from steady cached-query behavior.
- the same post-write query phase now computes exact recall@k against the
  current post-mutation vector snapshot, including overwritten vector values.
  The JSON rows include full and warm `post_write*_recall_at_k` plus raw
  hit/total counters so write-path savings can be read alongside after-write
  quality.
- `hbc-write-bench --centroid-directory <mode>` now mirrors the read benchmark's
  centroid-directory controls, including `flat_rabitq`, `two_level_rabitq`,
  `--flat-centroid-block-size`, `--flat-centroid-probe-count`, and
  `--flat-centroid-block-probe-count`. This lets an overwrite run compare
  foreground write cost and post-write query behavior for HBC routing versus
  the separate centroid-directory paths.
- in `shadow_base_delta` and `base_delta`, `flat_rabitq` now builds from the
  explicit centroid-directory record key range first and falls back to leaf-node
  traversal only when no records exist, keeping packed-HBC data compatible
  while making the separate centroid directory an actual query input
- write-bench storage counters now separate full-file reads, range reads,
  trailer reads, file-size calls, and read bytes. This matters for base/delta
  overlay reads, which commonly show up as range reads rather than full-file
  loads.
- write-bench scenarios install a dataset-backed external vector loader, so
  `skip_vector_store` workloads can still split postings, refresh payloads, and
  run post-write repair without falling back to missing raw-vector keys
- `hbc-read-bench --posting-storage shadow_base_delta` exposes read-side overlay
  counters (`profile_posting_overlay_*`) alongside existing query profile,
  storage-read, latency, and `queries_per_second` counters
- the posting-member overlay cache is now bounded by
  `max_posting_overlay_cache_bytes` on `HBCConfig` and by
  `--max-posting-overlay-cache-bytes` in the HBC write/read benches and recall
  harness; zero disables the cache instead of making it unbounded. A separate
  `max_posting_overlay_cache_entry_bytes` /
  `--max-posting-overlay-cache-entry-bytes` admission cap prevents one large
  posting from consuming the full resident set. When the entry cap is zero, the
  runtime derives a per-posting cap of one quarter of the total byte budget.
  Cache hits refresh recency, and byte-pressure eviction removes the
  least-recently-used posting member view first. Profile rows report
  overlay-cache evictions, admission skips, and resident member bytes so
  warm-query wins cannot hide unbounded scratch memory. The optimized gate
  now checks resident overlay member bytes against the configured cache byte
  cap for both write-side post-write queries and read rows, and rejects
  admission skips in required optimized rows by default so a too-small
  per-posting entry cap cannot pass as a "bounded" cache while skipping every
  useful posting view.
- the optimized gate now distinguishes DB-backed procedural vector rows from
  arbitrary `skip_vector_store` experiments. Procedural VDBB rows with
  dense-external-vector workloads count as the production "vectors live in the
  DB store" shape, but they still must run at gate scale and use exact
  post-write recall. Other `skip_vector_store` rows remain excluded unless
  `--gate-allow-skip-vector-store` is passed for an intentional external-vector
  layout experiment. Same-leaf metadata-only rows also gate on zero warm
  post-write range reads so a warm-query win cannot hide repeated base/delta or
  quantized-payload storage misses.
- the optimized gate now requires the `write_base_delta_hbc_auto_split_full`
  automatic-maintenance row. That row must run at the target scale, keep vectors
  in the DB store by default, publish bounded per-pass posting, layout-change,
  delta-tail, and boundary-reassignment budgets, use thresholded delta/tombstone
  folding instead of eager folds, keep overfull reassignment disabled, and show
  that bounded repair work actually ran during the write phase while satisfying
  the same warm internal-routing and write-amplification limits as the other
  optimized overwrite rows. Scheduled automatic split work is counted against
  the auto layout budget; accidental non-maintenance foreground splits remain
  disallowed.
- the comparison runner's automatic-maintenance row now uses deferred fold
  thresholds by default (`AUTO_MIN_DELTA_RECORDS_TO_FOLD` inherits the repair
  threshold, tombstone folds default high, ratio folding defaults off) and caps
  layout changes to `AUTO_SPLIT_FULL_MAX_POSTINGS` unless the run overrides it.
  This keeps the row closer to a production scheduler: bounded repair progress,
  no eager tail folding, and no one-layout-change-per-hot-key burst by default.
- canonical `base_delta` queries now skip the posting-delta cursor scan when
  the loaded posting base generation is already at or beyond the posting view's
  mutation version. Repaired/folded postings can therefore copy the current
  base member list directly into query scratch and cache it without paying a
  range scan for an empty or obsolete tail. Query profiles report this as
  `posting_overlay_delta_scan_skips`.
- `hbc-read-bench --repair-postings-after-build` runs bounded posting repair
  after the build phase and before optional reopen, and emits the repair cost
  on every read result row. This lets read-side benchmarks compare dirty
  post-build state against the post-maintenance state.
- `recall-harness` now accepts the same experimental storage/directory flags:
  `--posting-storage`, `--centroid-directory`, `--flat-centroid-block-size`,
  `--flat-centroid-probe-count`, and `--repair-postings-after-build`. This lets
  the recall suite validate `base_delta` and flat-directory query behavior
  against the existing HBC recall baselines instead of only comparing
  performance counters.
- storage regression coverage now verifies `base_delta` close/reopen
  invariants across separate posting-base, posting-delta, assignment-map, and
  centroid-directory key families. It covers both dirty pending tails and
  folded clean generations, including current-member materialization and search
  after reopen.
- `PostingStore` unit coverage now models recovery by cloning only the committed
  posting-family bytes into a fresh store, then verifies folded base generation,
  materialized members, centroid-directory generation/member count, assignment
  records, removed assignments, and drained delta tails all agree.
- `PostingStore` unit coverage also directly verifies posting deletion cleanup:
  deleting a posting removes its posting-base record, centroid-directory record,
  and delta tail while preserving unrelated posting tails.
- close/reopen coverage also verifies clean postings with remaining delta-tail
  debt still report repair debt and can fold that tail after reopen without
  requiring a synthetic dirty flag.
- modeled LSM crash/reopen coverage now verifies committed and synced
  `base_delta` posting-family state after storage crash. The test covers both a
  pending delta-tail generation and a folded clean generation, including member
  materialization and search after recovery.
- modeled LSM crash/reopen coverage now also verifies a committed multi-posting
  `batchApply` mutation. After recovery, updated vectors keep assignment-map
  records aligned with materialized posting members, deleted vectors have no
  assignment records, affected centroid-directory records exist and mark stale
  member counts as dirty, and global stored-structure validation passes.
- modeled write-fault coverage now verifies a failed `base_delta` fold
  transaction does not leave partial folded state behind. After an injected
  write failure and modeled crash, recovery still materializes the original
  pending delta tail and search returns the overwritten vector correctly.
- modeled write-fault coverage now also verifies failed foreground `base_delta`
  overwrite and delete transactions recover the previous committed posting
  membership, assignment map, and vector/search state after modeled crash.
- `validateStoredStructure` now checks canonical `base_delta` posting-family
  invariants globally: every reachable materialized posting member must have a
  current assignment shadow record and legacy vector-to-leaf mapping pointing
  back to that posting, duplicate live members are rejected, live materialized
  members must equal `active_count`, and clean centroid-directory member counts
  must match the materialized posting size. Wiring this validator into modeled
  crash/reopen tests exposed and fixed a real layout-repair recovery bug:
  public `repairDirtyPostingsWithOptions` could commit root/layout changes
  without flushing updated index metadata, leaving recovered HBC traversal on
  the old root while assignment/posting records pointed at the split postings.
- quantized `base_delta` reopen coverage now verifies that missing durable
  internal routing payloads self-heal from child centroids after modeled crash
  and that corrupt durable leaf payload bytes self-heal from vector data after
  modeled crash: the first search rebuilds an in-memory payload, and the next
  warm search reports zero misses for the repaired payload level.
- both HBC write and read benches now emit HBC cache byte counters
  (`hbc_cache_total_bytes`, node, quantized, vector, metadata), giving the
  packed-HBC versus base/delta comparison a stable memory/cache footprint signal
- dense index JSON config accepts `"posting_storage_mode": "shadow_base_delta"`
  and `"posting_storage_mode": "base_delta"` for the same opt-in paths
- unit coverage in `lib-vectorindex-test`

Initial smoke comparison:

```text
zig build hbc-write-bench -- \
  --samples 1 --vectors 128 --dims 16 --batch-size 32 \
  --leaf-size 16 --branching-factor 16 --storage memory \
  --workload online_batches_coalesced_empty \
  --lazy-posting-maintenance --repair-postings-after-write \
  --posting-storage packed_hbc

packed_hbc:
  ns_per_vector=13609.38
  ns_nodes_put_calls=107
  ns_nodes_value_bytes=10390
  posting_base_put_calls=0
  centroid_directory_put_calls=0
  assignment_map_put_calls=0
  posting_delta_append_calls=0
  posting_delta_fold_calls=0
  posting_repair_after_write_ns=466000
  lsm_total_run_bytes=36225

shadow_base_delta:
  ns_per_vector=20179.69
  ns_nodes_put_calls=173
  ns_nodes_append_calls=128
  ns_nodes_value_bytes=25254
  ns_nodes_delete_calls=8
  posting_base_put_calls=33
  posting_base_value_bytes=6560
  centroid_directory_put_calls=33
  centroid_directory_value_bytes=3696
  assignment_map_put_calls=174
  assignment_map_value_bytes=6960
  posting_delta_append_calls=128
  posting_delta_value_bytes=4608
  posting_delta_fold_calls=4
  posting_delta_fold_records=0
  posting_repair_after_write_ns=981000
  lsm_total_run_bytes=52556
```

Host-storage smoke with the same arguments except `--storage host`:

```text
packed_hbc:
  ns_per_vector=13578.13
  storage_write_file=45
  storage_write_bytes=90331
  storage_read_file=5
  storage_read_bytes=964607
  lsm_total_run_bytes=36225
  latest_lsm_keys=429

shadow_base_delta:
  ns_per_vector=15914.06
  storage_write_file=45
  storage_write_bytes=143457
  storage_read_file=5
  storage_read_bytes=1628217
  posting_base_put_calls=33
  centroid_directory_put_calls=33
  assignment_map_put_calls=174
  posting_delta_append_calls=128
  lsm_total_run_bytes=52556
  latest_lsm_keys=640
```

This is intentionally a shadow-write experiment, not the final storage win: it
adds base/delta, centroid-directory, and assignment-map bytes on top of packed
HBC and, when repair folds tails, also adds delete traffic for old delta
records. The next step is an opt-in read/write path that can skip packed
leaf-member rewrites for non-structural posting mutations, build quantized
payloads from posting-store materialization, and route from the separate
centroid-directory records.

First canonical posting-store smoke:

```text
zig build hbc-write-bench -- \
  --samples 1 --vectors 128 --dims 16 --batch-size 32 \
  --leaf-size 16 --branching-factor 16 --storage memory \
  --workload online_batches_coalesced_empty \
  --lazy-posting-maintenance --repair-postings-after-write \
  --posting-storage base_delta

base_delta:
  ns_per_vector=15210.94
  vectors_per_second=65742.17
  ns_nodes_put_calls=180
  ns_nodes_append_calls=128
  ns_nodes_value_bytes=20290
  posting_base_put_calls=22
  posting_base_value_bytes=4480
  centroid_directory_put_calls=29
  centroid_directory_value_bytes=3248
  assignment_map_put_calls=174
  posting_delta_append_calls=128
  posting_delta_fold_calls=0
  lsm_total_run_bytes=51340
```

Host-storage smoke with the same arguments except `--storage host`:

```text
base_delta:
  ns_per_vector=15101.56
  vectors_per_second=66218.31
  storage_write_file=45
  storage_write_bytes=135110
  storage_read_file=5
  storage_read_bytes=1337279
  ns_nodes_value_bytes=17478
  posting_base_put_calls=12
  lsm_total_run_bytes=50844
  latest_lsm_keys=640
```

This confirms the intended direction: `base_delta` reduces packed node bytes
relative to `shadow_base_delta` by making posting membership authoritative
outside the packed HBC leaf record. It is not yet a full replacement of HBC
storage. The packed node record still carries leaf routing metadata, internal
nodes still carry child centroids, split ranges still live in the HBC node
namespace, and quantized payload refresh still follows the HBC save path.

The split-state fix intentionally increases `posting_base_put_calls` from the
first smoke's 12 to 22 in this workload: structural split outputs need to
overwrite full posting bases so old append deltas do not leak across the new
posting partition. That is the right tradeoff for correctness; ordinary
incremental appends still use delta records.

Current `base_delta` caveats:

- the default Hilbert-seeded bulk-build path now runs in `base_delta`; during
  parent assignment it updates the separate centroid-directory parent metadata
  and invalidates stale root/non-root quantized payload shape instead of forcing
  an in-batch posting overlay scan
- structural split/build saves still publish full posting bases, so large
  topology changes intentionally trade extra base writes for correctness
- focused online-coalesced query behavior now matches packed-HBC scored-vector
  counts in memory and host/reopen smokes, but broader recall comparisons still
  need to be run before treating `base_delta` as generally equivalent

Initial read-through smoke:

```text
zig build hbc-read-bench -- \
  --samples 1 --vectors 128 --dims 16 --queries 4 --k 4 \
  --batch-size 32 --leaf-size 16 --branching-factor 16 \
  --storage memory --build online_coalesced --no-reopen \
  --posting-storage packed_hbc

packed_hbc warm_query_no_metadata:
  ns_per_query=21000
  profile_posting_overlay_calls=0
  profile_posting_overlay_fallbacks=0
  approx_vectors_scored=342

shadow_base_delta warm_query_no_metadata:
  ns_per_query=139250
  profile_posting_overlay_ns=252000
  profile_posting_overlay_calls=22
  profile_posting_overlay_base_members=234
  profile_posting_overlay_delta_records=0
  profile_posting_overlay_materialized_members=234
  profile_posting_overlay_fallbacks=12
  approx_vectors_scored=342
```

This confirms the query path can read through `PostingStore` and account for
overlay cost, but it also shows the current compatibility cost clearly. Because
packed HBC is still written and used to validate quantized payload order,
base/delta reads either materialize a base-only view or fall back; the real
performance experiment still requires a mode that stops rewriting packed leaf
members and owns quantized payload refresh from the posting store.

Focused `base_delta` read-through smoke after split/base snapshot fixes:

```text
zig build hbc-read-bench -- \
  --samples 1 --vectors 128 --dims 16 --queries 4 --k 4 \
  --batch-size 32 --leaf-size 16 --branching-factor 16 \
  --storage memory --build online_coalesced --no-reopen \
  --posting-storage base_delta

base_delta warm_query_no_metadata:
  ns_per_query=124250
  queries_per_second=8048.29
  profile_posting_overlay_ns=417000
  profile_posting_overlay_calls=34
  profile_posting_overlay_base_members=332
  profile_posting_overlay_delta_records=10
  profile_posting_overlay_materialized_members=342
  profile_posting_overlay_fallbacks=0
  approx_vectors_scored=342
  exact_vectors_scored=101
```

Host/reopen online-coalesced smoke:

```text
zig build hbc-read-bench -- \
  --samples 1 --vectors 128 --dims 16 --queries 4 --k 4 \
  --batch-size 32 --leaf-size 16 --branching-factor 16 \
  --storage host --build online_coalesced \
  --posting-storage base_delta

base_delta warm_query_no_metadata:
  ns_per_query=31000
  queries_per_second=32258.06
  storage_read_range=1
  storage_read_bytes=7251
  hbc_cache_total_bytes=9792
  profile_posting_overlay_ns=0
  profile_posting_overlay_calls=34
  profile_posting_overlay_base_members=342
  profile_posting_overlay_delta_records=0
  profile_posting_overlay_materialized_members=342
  profile_posting_overlay_fallbacks=0
  profile_posting_overlay_cache_hits=34
  profile_posting_overlay_cache_misses=0
  approx_vectors_scored=342
  exact_vectors_scored=101
```

The matching packed-HBC host/reopen run scored the same 342 approximate and 101
exact vectors for warm no-metadata. Before the scratch-level posting-member
cache, `base_delta` warm no-metadata read 296,288 bytes in 35 ranges because
base/delta posting materialization was per-posting and cursor-based. With the
versioned scratch cache, repeated warm queries hit cached materialized postings,
bringing the same workload down to 7,251 bytes in 1 range. The tradeoff is
search-workspace memory for cached posting member lists. That cache is now
bounded by `max_posting_overlay_cache_bytes`; tiny-cache overwrite smoke with a
128-byte cap reported 103 post-write overlay-cache evictions, 0 admission skips,
and 128 resident member bytes, making the memory/IO tradeoff explicit instead of
silently unbounded.

Host write smoke for the same online-coalesced workload:

```text
base_delta:
  ns_per_vector=18914.06
  vectors_per_second=52870.71
  storage_write_file=45
  storage_write_bytes=137692
  storage_read_file=5
  storage_read_bytes=1305776
  hbc_cache_total_bytes=26812
  ns_nodes_put_calls=180
  ns_nodes_value_bytes=20290
  posting_base_put_calls=22
  posting_delta_append_calls=128
  lsm_total_run_bytes=51340
  latest_lsm_keys=716
```

Recursive bulk-build smoke:

```text
zig build hbc-write-bench -- \
  --samples 1 --vectors 128 --dims 16 --batch-size 32 \
  --leaf-size 16 --branching-factor 16 --storage memory \
  --posting-storage base_delta --bulk-build-recursive \
  --workload bulk_build_empty

base_delta recursive bulk:
  ns_per_vector=11351.56
  vectors_per_second=88093.60
  posting_base_put_calls=12
  posting_delta_append_calls=0
  lsm_total_run_bytes=35291
```

Recursive bulk host/reopen read also works with zero overlay fallbacks:

```text
base_delta recursive bulk warm_query_no_metadata:
  ns_per_query=75500
  queries_per_second=13245.03
  profile_posting_overlay_calls=21
  profile_posting_overlay_base_members=239
  profile_posting_overlay_delta_records=0
  profile_posting_overlay_materialized_members=239
  profile_posting_overlay_fallbacks=0
```

Default Hilbert-seeded bulk-build smoke:

```text
zig build hbc-write-bench -- \
  --samples 1 --vectors 128 --dims 16 --batch-size 32 \
  --leaf-size 16 --branching-factor 16 --storage memory \
  --posting-storage base_delta --lazy-posting-maintenance \
  --repair-postings-after-write --workload bulk_build_empty

base_delta default bulk:
  ns_per_vector=7179.69
  vectors_per_second=139281.83
  posting_repair_after_write_ns_per_vector=8531.25
  ns_nodes_put_calls=82
  ns_nodes_value_bytes=7759
  posting_base_put_calls=8
  posting_delta_append_calls=0
  centroid_directory_put_calls=16
  assignment_map_put_calls=128
  lsm_total_run_bytes=30796

packed_hbc default bulk, same shape:
  ns_per_vector=10375.00
  vectors_per_second=96385.54
  ns_nodes_put_calls=34
  ns_nodes_value_bytes=4087
  posting_base_put_calls=0
  centroid_directory_put_calls=0
  assignment_map_put_calls=0
  lsm_total_run_bytes=24704
```

This is the expected storage-shape tradeoff: `base_delta` removes authoritative
leaf members from packed node values, but pays extra key count for posting
bases, centroid-directory records, and explicit assignment records. On a small
bulk build with no deltas, packed HBC is still leaner. The win for `base_delta`
is not bulk-build byte count; it is making later overwrite/insert/delete
mutations posting-local instead of repeatedly rewriting packed leaf member
payloads.

Default Hilbert host/reopen read comparison:

```text
base_delta warm_query_no_metadata:
  ns_per_query=81750.00
  queries_per_second=12232.42
  storage_read_range=32
  storage_read_bytes=345792
  search_workspace_bytes=4656
  profile_posting_overlay_calls=26
  profile_posting_overlay_cache_hits=26
  profile_posting_overlay_cache_misses=0
  profile_posting_overlay_fallbacks=0
  approx_vectors_scored=0
  exact_vectors_scored=416

packed_hbc warm_query_no_metadata:
  ns_per_query=21750.00
  queries_per_second=45977.01
  storage_read_range=0
  storage_read_bytes=0
  search_workspace_bytes=6336
  profile_posting_overlay_calls=0
  approx_vectors_scored=416
  exact_vectors_scored=110
```

The read tradeoff is also explicit: default bulk `base_delta` currently marks
leaf payloads dirty after root/non-root payload shape changes, so the query path
uses exact vector scoring until bounded repair refreshes the quantized payloads.
That avoids impossible in-batch overlay cursor work and keeps correctness, but
it is slower than packed HBC's immediately fresh quantized payloads in this
bulk-only smoke.

Post-repair default Hilbert host/reopen read:

```text
zig build hbc-read-bench -- \
  --samples 1 --vectors 128 --dims 16 --queries 4 --k 4 \
  --batch-size 32 --leaf-size 16 --branching-factor 16 \
  --storage host --build bulk_build --posting-storage base_delta \
  --repair-postings-after-build

base_delta warm_query_no_metadata after repair:
  ns_per_query=32250.00
  queries_per_second=31007.75
  posting_repair_after_build_ns_per_vector=10007.81
  storage_read_range=0
  storage_read_bytes=0
  search_workspace_bytes=7616
  profile_posting_overlay_calls=26
  profile_posting_overlay_cache_hits=26
  profile_posting_overlay_cache_misses=0
  profile_posting_overlay_fallbacks=0
  approx_vectors_scored=416
  exact_vectors_scored=110
```

After bounded repair, default bulk `base_delta` is back on the approximate
payload path and matches packed HBC's scored-vector shape for this smoke. The
remaining cost is explicit: repair takes about 10 us/vector in this tiny run,
and the search workspace is larger because the scratch cache holds materialized
posting member lists.

Hot overwrite workload:

```text
zig build hbc-write-bench -- \
  --samples 1 --vectors 128 --dims 16 --batch-size 16 \
  --leaf-size 16 --branching-factor 16 --storage host \
  --workload overwrite_hot_vectors_warm \
  --overwrite-hot-keys 16 --overwrite-rounds 4

packed_hbc:
  ns_per_vector=46484.38
  storage_write_file=36
  storage_write_bytes=85404
  storage_read_file=4
  storage_read_bytes=3676564
  split_leaf_calls=6
  save_node_calls=154
  ns_nodes_put_calls=432
  ns_nodes_value_bytes=36246
  ns_quant_put_calls=154
  range_put_calls=154
  lsm_total_run_bytes=57331
  latest_lsm_keys=444

base_delta + lazy repair:
  ns_per_vector=27140.63
  posting_repair_after_write_ns_per_vector=30687.50
  storage_write_file=45
  storage_write_bytes=67508
  storage_read_file=5
  storage_read_bytes=3333627
  split_leaf_calls=0
  save_node_calls=6
  ns_nodes_put_calls=104
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=7986
  range_put_calls=6
  posting_base_put_calls=6
  posting_delta_append_calls=64
  posting_delta_fold_calls=6
  posting_delta_fold_records=64
  centroid_directory_put_calls=6
  assignment_map_put_calls=64
  lsm_total_run_bytes=49951
  latest_lsm_keys=628
```

This is the first overwrite smoke where `base_delta` shows the intended
foreground shape. Existing-ID replacements stay in their current posting:
foreground routing is skipped, no leaves split, packed node/range writes drop
sharply, and the write path appends one replacement delta plus one assignment
record per overwrite. Bounded repair then folds the 64 replacement records into
new posting bases and refreshes quantized payloads. The remaining tradeoff is
explicit: base-delta foreground writes are faster and smaller than packed HBC in
this overwrite case, but post-write repair is still about 31 us/vector and must
be scheduled/bounded as real background maintenance.

Combined overwrite + post-write query smoke:

```text
zig build hbc-write-bench -- \
  --samples 1 --vectors 128 --dims 16 --batch-size 32 \
  --leaf-size 16 --branching-factor 8 --storage host \
  --workload overwrite_hot_vectors_warm \
  --overwrite-hot-keys 32 --overwrite-rounds 2 \
  --post-write-queries 8 --post-write-query-rounds 2 \
  --post-write-k 5

packed_hbc:
  ns_per_vector=46953.13
  split_leaf_calls=6
  save_node_calls=172
  ns_nodes_put_calls=490
  ns_nodes_value_bytes=41778
  storage_read_range=302
  storage_read_bytes=3727774
  post_write_query_ns_per_query=14562.50
  post_write_queries_per_second=68669.53
  post_write_storage_read_range=0
  post_write_storage_read_bytes=0
  post_write_profile_posting_overlay_calls=0
  post_write_warm_query_ns_per_query=10750.00
  post_write_warm_storage_read_range=0
  post_write_warm_storage_read_bytes=0
  post_write_warm_profile_posting_overlay_calls=0
  post_write_search_workspace_bytes=7816

base_delta + lazy repair:
  ns_per_vector=38437.50
  posting_repair_after_write_ns_per_vector=30906.25
  split_leaf_calls=0
  save_node_calls=8
  ns_nodes_put_calls=112
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=9024
  posting_delta_append_calls=64
  posting_delta_fold_calls=8
  posting_delta_fold_records=64
  storage_read_range=584
  storage_read_bytes=4369478
  post_write_query_ns_per_query=32250.00
  post_write_queries_per_second=31007.75
  post_write_storage_read_range=10
  post_write_storage_read_bytes=46599
  post_write_profile_posting_overlay_calls=104
  post_write_profile_posting_overlay_cache_hits=96
  post_write_profile_posting_overlay_cache_misses=8
  post_write_profile_posting_overlay_delta_records=0
  post_write_warm_query_ns_per_query=9375.00
  post_write_warm_queries_per_second=106666.67
  post_write_warm_storage_read_range=0
  post_write_warm_storage_read_bytes=0
  post_write_warm_profile_posting_overlay_calls=52
  post_write_warm_profile_posting_overlay_cache_hits=52
  post_write_warm_profile_posting_overlay_cache_misses=0
  post_write_search_workspace_bytes=9096
```

This adds the missing after-write read-side signal. In this small repaired
overwrite run, `base_delta` still removes foreground splits and most packed node
rewrites. The first post-write query round pays base-posting range reads on
cold posting-cache misses. The warm round has zero posting-base reads, all
posting-overlay cache hits, and lower query latency than packed HBC in this
tiny run. Repair folded the delta records first, so the remaining cold overlay
cost is base materialization rather than tail replay. That points at two
separate follow-ups: decide whether scratch-local member caching is enough for
production concurrency, and consider a read-optimized packed posting base layout
for repaired postings.

Same overwrite setup, `base_delta + lazy repair`, comparing post-write query
directory modes:

```text
hbc directory:
  ns_per_vector=38796.88
  posting_repair_after_write_ns_per_vector=29140.63
  post_write_query_ns_per_query=32250.00
  post_write_queries_per_second=31007.75
  post_write_profile_child_expand_ns=2000
  post_write_storage_read_range=10
  post_write_storage_read_bytes=46599
  post_write_profile_posting_overlay_cache_misses=8
  post_write_warm_query_ns_per_query=9625.00
  post_write_warm_queries_per_second=103896.10
  post_write_warm_storage_read_range=0
  post_write_warm_profile_posting_overlay_cache_hits=52

flat_rabitq directory:
  command adds:
    --centroid-directory flat_rabitq --flat-centroid-probe-count 8
  ns_per_vector=37312.50
  posting_repair_after_write_ns_per_vector=30390.63
  post_write_query_ns_per_query=49625.00
  post_write_queries_per_second=20151.13
  post_write_profile_child_expand_ns=51000
  post_write_storage_read_range=11
  post_write_storage_read_bytes=57405
  post_write_profile_posting_overlay_cache_misses=8
  post_write_warm_query_ns_per_query=9625.00
  post_write_warm_queries_per_second=103896.10
  post_write_warm_storage_read_range=0
  post_write_warm_profile_posting_overlay_cache_hits=64
```

The flat directory path is now benchmarkable in the same write/read row. In
`base_delta`, it now builds from the separate centroid-directory record key
range instead of published leaf nodes. The first flat query still pays directory
build/read cost, but the read footprint drops compared with probing node IDs
because it scans the `CD` key family directly. The warm path uses zero storage
reads once the flat directory and posting-member overlay cache are hot. The
remaining caveat for `flat_rabitq` is algorithmic scale: it still scans/probes
all posting centroids, so it is a useful experimental directory source, not the
final scalable centroid index.

`two_level_rabitq` is the first scalable increment over that flat source. It
groups posting centroids into blocks, builds coarse block centroids, and probes
a selected block subset before scoring posting centroids. Blocks are no longer
formed in posting-id order: directory construction sorts posting centroids by
the dimension with the largest observed range before chunking, so coarse block
centroids represent spatial neighborhoods instead of arbitrary storage order.
The current query path uses RaBitQ at both directory layers. Coarse block
centroids are ranked with the block-level RaBitQ payload plus a
radius/error-bound margin, so large directories can avoid touching every
posting block exactly. Once blocks are selected, posting centroids inside those
blocks are first ranked with each block's quantized posting-centroid payload,
then only a bounded candidate subset per block is exact-scored before final
probe insertion. Exact posting-centroid scoring remains the full fallback when
quantization is disabled.

The runtime config supports adaptive block probing when
`flat_centroid_block_probe_count=0`: after ranking coarse blocks by lower
bound, it includes additional blocks whose lower bound overlaps the selected
boundary's upper bound, capped at `2x` the default probe count. Small
directories are full-scanned by default (`<=32` coarse blocks), because a
16-block/4096-vector smoke showed that pruning before the directory is large
enough loses recall while proving little about 1M behavior. The comparison
runner now leaves two-level block probing adaptive by default. Posting probe
count follows `SEARCH_WIDTH`, block size is derived from the estimated posting
count with a 128-posting maximum, and the internal default block probe count is
based on the block count and the minimum blocks needed to cover the requested
posting probes. This keeps small smokes from degenerating to one centroid
block, keeps VDBB-style 1M runs near 128 postings per block, and avoids a fixed
block-probe cap that can under-select after foreground split-full repair. In
the current VDBB-like 1M shape (`leaf_size=168`), the effective policy sees
about `47` coarse blocks and probes `7` before adaptive expansion.

A focused 4096-vector directory smoke with `SEARCH_WIDTH=32` demonstrates the
current tradeoff. Centroid ranking at 32 posting probes lifts repaired warm
no-metadata recall from `0.1375` to `0.5875` for flat and to `0.375` for
two-level while two-level still evaluates only `1024 / 4096` posting centroids.
With the wider capped block default (`8` blocks/query in that shape),
two-level recall rises to `0.45`, evaluates `2048 / 4096` posting centroids,
selects `128 / 256` blocks, and keeps workspace within `0.954x` of flat. That
is scalability and recall progress, not yet a finished directory: the focused
smoke still needs to scale to the 1M read-after-write gate and the two-level
row still trades recall for directory pruning at the default probe settings.

After moving parent/level/posting-state metadata into the strict
centroid-directory record and into flat/two-level probes, maintenance must also
republish the centroid-directory record when it only refreshes the posting
payload. Otherwise the directory can keep a stale `payload_dirty` flag and
queries will exact-score every selected posting member even though a fresh
RaBitQ payload exists. With payload refresh now updating the directory record,
the same focused 4096-vector run uses the posting RaBitQ path again:
warm no-metadata packed HBC is `12678` QPS at `50us` p95 with `0.275` recall;
repaired `base_delta + flat_rabitq` is `2732` QPS at `358us` p95 with `0.5875`
recall; and repaired `base_delta + two_level_rabitq` is `2758` QPS at `353us`
p95 with `0.45` recall. Both canonical base-delta rows have zero node-cache and
quantized-cache misses in the warm profile, approximate-score `8192` posting
members, and exact-rerank about `633-637` candidates instead of exact-scoring
all selected members. This is a correctness and read-path efficiency fix, but
it exposed another avoidable cost: the flat/two-level search branch was still
pinning the HBC upper-tree cache during generic query setup, even though it
does not traverse that tree when directory probing succeeds.

After moving upper-tree pinning onto the HBC traversal/fallback path and
profiling it separately, the same focused 4096-vector warm no-metadata row
changes materially. Packed HBC is `12030` QPS at `53us` p95 with `0.275`
recall and spends `384us` cumulative in upper-tree pinning. Repaired
`base_delta + flat_rabitq` is `16789` QPS at `35us` p95 with `0.5875` recall,
and repaired `base_delta + two_level_rabitq` is `17391` QPS at `29us` p95 with
`0.45` recall. The canonical base-delta directory rows report
`profile_upper_tree_pin_ns=0`, zero storage reads in warm no-metadata, and
single-digit-microsecond setup. This clears the focused small-read performance
smoke, but not the optimized claim: the 1M read-after-write gate still has to
show comparable recall/QPS and materially better overwrite write amplification,
and two-level still needs recall/probe tuning at larger posting counts.

The search profile now reports:

```text
centroid_directory_blocks_scanned
centroid_directory_blocks_selected
centroid_directory_block_probe_limit
centroid_directory_block_probe_count
centroid_directory_posting_centroids_scored
centroid_directory_posting_centroid_estimates
profile_upper_tree_pin_ns
profile_runtime_txn_ns
profile_scratch_acquire_ns
```

Those counters are emitted by the HBC read/write benches so flat versus
two-level runs can show whether the directory is actually pruning work. Local
smoke coverage verifies that `two_level_rabitq` can preserve the nearest hit
while selecting one centroid block, but recall/performance still need to be
measured against packed HBC at larger posting counts.
The two-level query path also reuses distance-only scratch for coarse block
selection, so scoring block centroids does not grow vector-fetch buffers by the
number of centroid blocks or inflate the query-result slab. Selected block IDs
live in a stack/usize scratch buffer instead of `SearchScratch.positions`, and
adaptive two-level probes size that buffer to the bounded selected-block
candidate window instead of the full directory block count. `SearchScratch`
exposes distance-only growth for coarse block scoring. Directory blocks also
retain the coarse centroid's metric measure, so non-quantized block scoring can
use the same precomputed candidate-measure path as posting-centroid scoring
instead of recomputing block centroid norms/measures on every query. Adaptive
two-level selections carry the selected block lower bounds into posting scoring,
so once the current exact posting heap or epsilon window proves later sorted
blocks cannot contribute, the query stops before per-block posting estimates
and exact centroid scoring. Per-block quantized posting candidate collection
also checks the current final posting heap bound before inserting into the
temporary candidate heap, so clearly rejected candidates no longer participate
in candidate sorting. Bounded flat posting and block probe collectors also
build their internal heaps lazily only when an overflow insert or rejection
check requires it, so exactly-filled candidate buffers can go straight to the
final sort. Adaptive
two-level queries also delay the
posting-count-sized
distance/error-bound scratch reservation until the selected block set is known
to full-scan the directory and use the global posting quantized payload; pruned
block probes stay at block/per-block scratch size. The optimized gate checks the
resulting
`search_workspace_bytes` ratio against the flat directory row. Write/read bench
rows now also emit the observed effective block-probe limit and selected block
count, so adaptive two-level rows can prove how many coarse blocks they actually
probed instead of relying only on the configured
`flat_centroid_block_probe_count` value.
Automatic flat/two-level posting-probe effort is now at least the rerank
candidate budget, while an explicit `flat_centroid_probe_count` remains an
upper bound. That keeps low `search_width` requests from probing fewer posting
centroids than the query is prepared to retain/rerank.

Repeatable comparison runner:

```text
zig/scripts/run_spfresh_hbc_comparison.sh
```

The runner writes labeled JSON rows to:

```text
zig/bench/results/spfresh-hbc-comparison/spfresh-hbc-comparison.jsonl
```

It defaults to `STORAGE=host` for the evidence run, but accepts
`STORAGE=memory` for cheap matrix-shape smoke checks that should not be used as
optimization evidence.

Summarize those wide JSON rows with:

```text
python3 zig/scripts/summarize_spfresh_hbc_comparison.py \
  zig/bench/results/spfresh-hbc-comparison/spfresh-hbc-comparison.jsonl
```

Use `--format markdown` for a pasteable table and `--kind write` or
`--kind read` when only one side of the matrix matters. The summary groups
multiple samples by `comparison_label + workload` and reports the gate fields:
post-write recall, QPS, baseline ratios versus the packed-HBC row for the same
workload, foreground node bytes/saves/splits, posting-delta foreground records,
explicit repair cost, overfull repair limits, backlog debt, overlay delta scans,
centroid-directory blocks selected plus posting centroid scores/estimates,
upper-tree pin time, LSM run bytes, and read locality counters.

The default matrix runs:

- overwrite-hot write workload with post-write query rounds:
  - `packed_hbc + hbc + no coalesced overwrite fast path`
  - `base_delta + hbc + lazy repair + eager fold`
  - `base_delta + hbc + lazy repair + deferred fold`
  - `base_delta + hbc + lazy repair + capacity-safe reassignment/swap`
  - `base_delta + hbc + auto maintenance + split full postings`
  - `base_delta + hbc + lazy repair + bounded overfull reassignment`
  - `base_delta + flat_rabitq + lazy repair`
  - `base_delta + two_level_rabitq + lazy repair + capacity-safe reassignment/swap`
- overwrite-random write workload with post-write query rounds:
  - `packed_hbc + hbc + no coalesced overwrite fast path`
  - `base_delta + hbc + lazy repair + capacity-safe reassignment/swap`
  - `base_delta + two_level_rabitq + lazy repair + capacity-safe reassignment/swap`
- overwrite-same-leaf write workload with post-write query rounds:
  - `packed_hbc + hbc + no coalesced overwrite fast path`
  - `base_delta + hbc + lazy repair`
  - `base_delta + hbc + lazy repair + no coalesced overwrite fast path`
- overwrite-semantic-drift, append-streaming, and mixed insert/delete/update
  write workloads:
  - `packed_hbc + hbc + no coalesced overwrite fast path`
  - `base_delta + hbc + lazy repair`
  - `base_delta + two_level_rabitq + lazy repair`
  - mixed rows use the same default finite capacity-reassignment budget as
    overwrite rows; set `MIXED_REPAIR_DIRTY_REASSIGNMENTS=0` only for an
    explicit no-reassignment contrast run
  - packed-HBC write rows use `--no-coalesce-overwrite-leaf-writes` as the
    stable existing-index baseline; coalesced overwrite routing depends on the
    explicit assignment-map/base-delta state that the packed baseline does not
    maintain after bulk build
- VectorDBBench-shaped warm batch-apply write workload with post-write query
  rounds:
  - `packed_hbc + hbc + dense external-vector batch apply`
  - `base_delta + hbc + dense external-vector batch apply + capacity-safe repair`
  - the runner exposes this as `write_vdbb_*` rows with independent
    `VDBB_*` sizing knobs; the intended 1M validation shape is 1536D vectors,
    500-row batches, 168-member leaves/branching, and warm post-write recall/QPS
  - the optimized base-delta row repairs dirty postings before closing the
    bulk-ingest session, so repair and fold output can stay in the same ingest
    window instead of creating a separate maintenance flush/run
- repaired bulk-build read workload:
  - `packed_hbc + hbc`
  - `base_delta + hbc`
  - `base_delta + flat_rabitq`
  - `base_delta + two_level_rabitq`

For write-amplification decisions, compare the `foreground_*` fields first.
The aggregate profile fields include optional explicit repair work, while
`repair_*` fields isolate that maintenance phase.
The overfull reassignment row is bounded by default with
`REPAIR_DIRTY_REASSIGNMENT_MAX_OVERFULL_POSTINGS=4` and
`REPAIR_DIRTY_REASSIGNMENT_MAX_OVER_CAPACITY_MEMBERS=1`; the summary table
prints those as `repair_overfull_limit` and `repair_over_capacity_limit`.
The capacity-safe row remains the default comparison for optimized behavior,
and now pairs boundary reassignment with bounded full-posting split scheduling
via `repair_split_full_postings` and `repair_max_layout_changes`. The summary
also prints `repair_allow_overfull` and `auto_allow_overfull`, and the optimized
gate requires both to remain false for required optimized rows. The
bounded-overfull row measures whether limited debt buys enough recall to justify
additional split repair; it is a contrast row, not the optimized default.
Production dense-index config enforces the same rule for automatic maintenance:
if `auto_posting_maintenance_allow_overfull_reassignment=true`, both
`auto_posting_maintenance_max_overfull_reassignment_postings` and
`auto_posting_maintenance_max_over_capacity_reassignment_members` must be
explicit finite limits. The unbounded opt-in form is rejected as
`InvalidIndexConfig`.

Large comparison shape for the 1M gate:

```text
cd /path/to/antfly
env VECTORS=1000000 DIMS=768 BATCH_SIZE=512 LEAF_SIZE=512 \
  BRANCHING_FACTOR=16 SEARCH_WIDTH=32 \
  OVERWRITE_HOT_KEYS=10000 OVERWRITE_ROUNDS=2 \
  MAX_POSTING_OVERLAY_CACHE_BYTES=8388608 \
  MAX_POSTING_OVERLAY_CACHE_ENTRY_BYTES=0 \
  POST_WRITE_QUERIES=200 POST_WRITE_QUERY_ROUNDS=2 POST_WRITE_K=10 \
  READ_QUERIES=200 READ_K=10 \
  RESULT_DIR=zig/bench/results/spfresh-hbc-comparison-1m \
  zig/scripts/run_spfresh_hbc_comparison.sh
```

When `VECTORS>=1000000`, the runner defaults repaired read-bench rows to
`READ_QUERIES=100`, `READ_K=10`, and `READ_DATASET_MODE=procedural` unless
those variables are set explicitly, so the read side also produces a 1000-item
recall denominator by default without allocating a materialized
`vectors * dims` matrix. The procedural read-bench path uses the same
external-vector loader contract as the procedural write rows and preserves
`dataset_mode` / `skip_vector_store` in the summarized read output. Exact read
truth is precomputed once per read-bench invocation and reported as
`exact_truth_build_ns`, so read QPS/latency rows measure search and result
materialization rather than brute-force truth generation. Exact read recall
still scans generated candidates, so this removes the 1M memory wall and
prevents oracle work from polluting timings, but it does not by itself make the
synthetic exact oracle cheap.

The comparison runner passes a shared `READ_EXACT_TRUTH_CACHE_PATH` to all
read-bench rows by default. The first comparable read row builds and writes the
truth IDs, later read rows load the same file, and summaries preserve
`exact_truth_cache_hit` next to `exact_truth_build_ns`. With a cache file,
`exact_truth_build_ns` is the load-or-build elapsed time for that row, not query
latency.

The runner also supports resumable targeted rows, which is the practical way
to collect the 1M gate because the packed-HBC hot overwrite baseline alone can
take tens of minutes:

```text
env ... RESULT_DIR=zig/bench/results/spfresh-hbc-comparison-1m \
  RUN_LABELS=write_base_delta_hbc_reassign_capacity \
  APPEND_RESULTS=1 \
  zig/scripts/run_spfresh_hbc_comparison.sh
```

`RUN_LABELS` accepts a space- or comma-separated label list. `APPEND_RESULTS=1`
keeps existing rows in `spfresh-hbc-comparison.jsonl`, so packed baselines can
be captured once and optimized rows can be appended without truncating the
file. The optimized repair defaults are finite but scale with overwrite
pressure: boundary reassignment and layout-change budgets default to
`OVERWRITE_HOT_KEYS`, and `REPAIR_MAX_DELTA_TAIL_POSTINGS` defaults to the gate
cap (`1024`). This prevents a high-ingest 1M run from passing foreground write
amplification by leaving unbounded posting-layout or delta-tail debt behind.
The comparison runner passes the same cap into the automatic repair contrast
row through `--auto-posting-maintenance-max-delta-tail-postings`, so the
background-style row is measured with the same bounded-debt policy.

That run is the current evidence gate for calling the approach optimized:
base-delta should preserve comparable post-write recall/QPS versus packed HBC
while materially lowering foreground structural write amplification under the
same-leaf, hot, random, semantic-drift, append-streaming, mixed, and
two-level-directory hot-overwrite workloads, plus the VectorDBBench-shaped
1536D dense external-vector warm batch-apply workload. The `two_level_rabitq` write row
must use the same bounded repair-side split/reassignment and delta-tail debt
policy as the HBC-directory capacity row, so the scalable directory is tested
under read-after-write pressure rather than only on repaired bulk reads. The
procedural VDBB bounded-maintenance rows pass finite
`repair_dirty_reassignments`, keep overfull reassignment disabled, enable
full-posting split scheduling, and cap layout changes, so they model the
capacity-safe posting-local policy instead of a repair pass that only refreshes
centroids and folds delta tails. The `two_level_rabitq` write and read rows
should also show materially fewer
posting centroid evaluations than `flat_rabitq` at the same recall target, and
coarse block centroid scores/estimates should stay small relative to flat
posting-centroid evaluations; otherwise the directory change is only moving
bytes around, not solving the large-posting-count search shape.
The summary table is the first pass/fail readout. Write rows print both
`operation_vectors` and `active_vectors`: `operation_vectors` is the number of
mutations in the workload sample, while `active_vectors` is the post-write index
population reported by `active_count_after`. The optimized gate uses
`active_vectors` for write-row scale, so a 20k-overwrite workload over a 1M
corpus is judged as a 1M-index run rather than as a 20k-vector toy run. Write
rows also print automatic-maintenance run counts and per-pass observed maxima
(`auto_max_*_observed`) so bounded background policy checks can distinguish
total work accumulated across a workload from the maximum work allowed in any
single maintenance pass. Write and read `recall_delta` should stay within the
accepted tolerance,
`qps_vs_packed` should not regress materially (write rows use warm post-write
QPS when `post_write_query_rounds > 1`; write recall, overlay-cache, and
centroid-directory counters also prefer warm post-write rounds when present),
non-bulk optimized write rows also report pre-repair recall/QPS/p95 so bounded
maintenance lag cannot hide a large temporary recall or latency regression,
warm post-write and read-bench `p95_vs_packed` should stay bounded,
`fg_node_bytes_vs_packed` and foreground split/save counts should be materially
lower on overwrite-heavy, append-heavy, mixed, and VDBB-shaped rows,
`storage_write_bytes_vs_packed` and
`lsm_run_bytes_vs_packed` should stay bounded so the storage layer is not hiding
the write amplification, `storage_write_files_vs_packed` and
`lsm_runs_vs_packed` should also stay bounded so smaller bytes do not hide
excess physical file/run churn, and backlog/overlay-debt columns should stay
bounded rather than growing with the run.
Overlay-cache admission skips should remain zero in required optimized rows
unless a run explicitly relaxes `--gate-max-overlay-cache-admission-skips`;
otherwise a small entry cap can hide cache ineffectiveness behind a bounded
resident-byte counter.
The write-side overlay-cache hit-rate gate now requires true warm evidence:
`post_write_query_rounds >= 2`, at least one warm post-write query, and warm
overlay-cache observations. Cold one-round totals remain useful diagnostics but
cannot satisfy the optimized cache-hit proof.
`fg_delta_records_per_append` should stay at least 1.0 when foreground delta
records are emitted, and the append-streaming row should stay at least 2.0 to
prove posting-local grouping is reducing physical delta keys when write
locality is expected. Mixed insert/delete/update remains a required workload,
but it intentionally spreads operations across postings; same-posting mixed
grouping is covered by focused regression tests rather than by requiring the
whole mixed workload to have append-streaming locality.
When grouped foreground appends average at least three records, the gate also
checks `fg_delta_value_bytes_vs_legacy` against the equivalent v1 value layout
and requires the compact value bytes to stay at or below 95% of legacy. That
keeps the file-layout proof tied to actual physical bytes instead of only
record/key density.
Rows that fold delta records should also show nonzero `repair_fold_tail_keys`,
`repair_fold_tail_value_bytes`, and `repair_fold_written_base_value_bytes`;
otherwise the run has not proven that posting-local mutations use compact
physical delta keys, that maintenance reclaims those keys after folding, and
that the replacement base write is visible. The optimized gate additionally
bounds `repair_fold_base_bytes_per_record` and `repair_fold_base_tail_ratio` so
deferred maintenance cannot pass by folding tiny tails into large base rewrites.
Use the machine-checkable gate before calling the result optimized:

```text
python3 zig/scripts/summarize_spfresh_hbc_comparison.py \
  zig/bench/results/spfresh-hbc-comparison-1m/spfresh-hbc-comparison.jsonl \
  --optimized-gate check \
  --gate-production-proof-file zig/bench/results/spfresh-production-test.log
```

The comparison runner exposes a curated optimized-gate evidence set:

```text
RUN_OPTIMIZED_GATE_LABELS=1 PRINT_OPTIMIZED_GATE_LABELS=1 \
  zig/scripts/run_spfresh_hbc_comparison.sh
```

`RUN_OPTIMIZED_GATE_LABELS=1` defaults `VECTORS` to `1000000`, enables the
1M procedural VDBB online and mutation rows, and selects the packed baselines,
same-leaf row, bounded base-delta rows, VDBB-shaped two-level rows, and repaired
flat/two-level read rows required by the gate. It does not bypass the exact
truth guard: either prebuild the selected 1M exact-truth caches with
`PREBUILD_VDBB_1M_EXACT_TRUTH_CACHES=1`, provide existing cache files, or set
`ALLOW_SLOW_VDBB_1M_EXACT_RECALL=1` intentionally.

The same-leaf overwrite row is the metadata-only/no-vector-change control. Its
optimized behavior is different from semantic overwrite rows: it should not
schedule posting split repair or perform split/reassign/fold maintenance at
all. The gate treats this as proof that base-delta can avoid structural
maintenance when the posting membership and vector payload did not change.

The default gate requires each required packed-HBC baseline and optimized
write/read comparison row, not just the overall result file, to run at least
1M vectors. For write rows that means at least 1M active vectors after the
mutations, not at least 1M overwrite operations in the sample. It also requires
those packed baselines to report the recall, latency, and structural/storage
counters used for ratios, so resumable 1M result files cannot accidentally
compare optimized rows against missing, smaller, stale, or
external-vector-only baselines. When small VDBB smoke rows and 1M procedural
VDBB rows are present in the same result file, the gate uses the largest
available VDBB baseline/optimized rows so a small smoke row cannot shadow the
1M evidence. The VDBB optimized quality row is now the bounded-maintenance
two-level RaBitQ row when that evidence exists; the older HBC-directory
base-delta row is only a fallback and cannot satisfy the separate scalable
directory pruning proof. It applies the same largest-row rule to the write-side
flat-RaBitQ versus two-level-RaBitQ directory comparison and requires both
selected rows to meet the gate's minimum vector count before accepting the
two-level pruning proof. The selected two-level write row must be the
bounded-maintenance variant, report precomputed/loadable exact truth outside
query timing, and pass the same exact-recall, QPS, and p95 envelope against the
packed-HBC baseline, and
ratio baselines are chosen from the largest packed row for the matching
workload so small smoke baselines cannot shadow 1M procedural evidence. By
default required write rows must keep vectors
in the DB store, either directly or through procedural dense-external-vector
rows that model DB-backed vector storage; `--gate-allow-skip-vector-store` is
reserved for separate external-vector layout experiments. It also requires
post-write and
read-bench recall within 0.05 of the packed-HBC baseline, warm post-write
QPS at least 80% of packed HBC when warm rounds exist, warm post-write and
read-bench p95 query latency at most 1.25x packed HBC, zero warm internal
quantized routing misses on optimized write rows, exact post-write recall
denominators of at least 40 for ordinary write rows and 1000 for required 1M
VDBB-shaped rows, a read-bench recall denominator of at least 1000, pre-repair recall within 0.20
of packed HBC plus pre-repair QPS at least 50% and p95 at most 2.0x packed HBC
on non-bulk optimized mutation rows. VDBB-shaped bulk-ingest rows instead must
publish `repair_before_bulk_finish=true`, proving posting repair happens before
the bulk ingest window closes. VDBB bulk rows keep the storage-write-file,
storage-write-byte, LSM-run-byte, and L0-run-fanout bounds. Total active run
bytes remain capped against packed HBC, while the run-count maintenance-debt
gate uses `lsm_l0_runs_vs_packed` so a bounded compact finish may leave one
lower-level compacted run plus one deferred L0 run without being treated as
unbounded L0 backlog. The zero foreground split and foreground-node-byte
overwrite checks apply to mutation workloads, not initial bulk index
construction. Foreground node bytes at
most 50% of packed HBC on overwrite rows, storage write bytes, storage write
file count, LSM run bytes, and LSM run count at most 1.5x packed HBC on
overwrite rows, no foreground splits on those
overwrite rows including the required two-level-directory overwrite row,
repair-side split scheduling with a finite layout-change budget on required
optimized rows that can create layout debt, no split/reassign/fold repair on
the same-leaf metadata-only row, zero same-leaf warm post-write range reads, explicit
overfull reassignment disabled for both repair and automatic maintenance, a
required automatic-maintenance row with bounded per-pass postings, bounded
layout changes, bounded delta-tail debt, thresholded delta/tombstone folding,
capacity-debt-triggered split scheduling, finite boundary-reassignment budget,
observed repair work during the write phase, observed per-pass layout work no
larger than the advertised `auto_posting_maintenance_max_layout_changes`, and
the same foreground/storage write-amplification limits as other overwrite rows,
while separating scheduled maintenance splits from non-maintenance foreground
splits, no
remaining dirty/overfull/over-capacity posting debt in those required rows,
bounded full-posting slack debt, bounded remaining delta-tail debt, grouped
physical delta-key density on append-streaming rows, fold-tail cleanup evidence
and bounded replacement-base write amplification when rows fold deltas, an
enabled but bounded posting-overlay cache, warm post-write and warm read
overlay-cache hit rates of at least 0.80 when overlay lookups occur, and
pre-finish posting repair on VDBB-shaped optimized batch-apply rows, and
two-level RaBitQ directory rows
on both write-side post-write queries and repaired read-bench queries. At
smoke scale the rows must preserve recall/QPS and show coarse block RaBitQ
estimate counters with zero exact coarse-block scoring. At larger proof
shapes, the aggregated scanned-block count must cross the gate threshold
(default `256`), the directory must select fewer blocks than it scans, and
both selected posting-centroid and coarse block-centroid evaluations must be
materially lower than the flat directory row. VDBB-shaped two-level rows use
the observed per-query block-probe budget when present, falling back to the
configured block-probe count only for older artifacts, and require that budget
to be at least the final posting probe count. The gate also
checks read-bench search workspace bytes against the flat row and requires
zero upper-tree pin time on record-backed flat/two-level query paths.
`--optimized-gate report` prints the same checks without failing the process,
which is useful for smoke runs and tuning.

Production integration is part of that bar, not a follow-on cleanup. The
SPFresh-style maintenance path must run through the DB's idle maintenance
entrypoint and `BackendRuntime` durable-job lane, and each pass must reserve
the `ResourceManager` `dense.posting_maintenance_working_set` slice before
mutating postings. Focused storage tests currently cover the direct idle path,
manual and threaded runtime submission, elapsed-budget stops,
query-guardrail skips, resource-budget stops, and runtime-submitted
resource-budget stops on both durable-job lanes:

```text
zig build spfresh-production-test --summary all 2>&1 \
  | tee zig/bench/results/spfresh-production-test.log
zig build lib-storage-test -- --test-filter "db dense posting maintenance"
```

Crash/reopen and posting-family recovery invariants are kept in a smaller gate
that runs both the low-level posting-family tests and the storage-backed modeled
crash cases:

```text
zig build spfresh-recovery-test
```

Current partial 1M evidence:

- `write_packed_hbc_hbc`, `overwrite_hot_vectors_warm`, 1M active vectors,
  20k overwrites, 768 dims:
  - foreground node bytes: `295406801`
  - foreground leaf splits: `1237`
  - post-write recall@10: `0.0530`
  - post-write QPS: `6.61`
  - write throughput: `14.85` vectors/s
- `write_base_delta_hbc_reassign_capacity`, same workload, finite repair
  budgets (`repair_dirty_reassignments=10000`,
  `repair_max_layout_changes=10000`,
  `repair_max_delta_tail_postings=1024`):
  - foreground node bytes ratio: `0.0084x`
  - foreground leaf splits: `0`
  - post-write recall delta: `-0.0085`
  - post-write QPS ratio: `0.956`
  - write throughput: `530.33` vectors/s
  - remaining overfull / over-capacity debt: `0 / 0`
  - remaining delta-tail postings: `518`
- VDBB-shaped procedural diagnostic, 10k active vectors, 1536 dims,
  `skip_vector_store=true`, self-hit validation, bounded pre-query repair:
  - packed HBC: self-hit@10 `0.75`, warm QPS `1612`, p95 `650000 ns`
  - `base_delta + hbc`: self-hit@10 `0.75`, warm QPS `1664`,
    p95 `646000 ns`, repair cost `18279 ns/vector`, remaining dirty postings
    `0`, remaining delta-tail postings `73`
  - an under-probed `base_delta + two_level_rabitq` row originally used only
    the generic small-benchmark `flat_centroid_probe_count=8`, producing
    self-hit@10 `0.50`. The comparison runner now derives VDBB-specific
    posting probe counts from `VDBB_SEARCH_WIDTH`; with the corrected default
    (`flat_centroid_probe_count=168`,
    `flat_centroid_block_probe_count=15`), the same shape produced
    self-hit@10 `1.00`, warm QPS `1193`, p95 `904000 ns`, selected 60 of
    88 centroid blocks, and scored 240 posting centroids.
  - interpretation: bounded repair fixes the HBC-directory query-latency
    regression from raw lazy posting maintenance, and the two-level directory
    quality gap was partly a benchmark under-probing artifact. The corrected
    row trades more centroid/posting scoring for self-hit quality and is still
    not optimized-gate proof because `self_hit` is diagnostic only and the gate
    requires exact recall at the target scale.

The hot-overwrite 1M row has encouraging foreground, recall, QPS, and debt
signals, but the artifact is not a passing optimized-gate result: it is missing
required workloads/read rows and was captured before the current p95,
in-DB-vector, pre-repair, and fold-base-byte counters were fully enforced. The
full optimized gate is still incomplete until same-leaf, random, semantic-drift,
append-streaming, mixed, VDBB-shaped batch-apply, and read-directory rows are
collected at 1M scale with current instrumentation.

Append-streaming and mixed insert/delete/update rows are now part of the
foreground/storage write-amplification gate rather than only the quality/debt
gate. The optimized rows now enable
`defer_leaf_splits_to_posting_maintenance`, and the gate requires that knob so
append/mixed runs cannot silently fall back to foreground HBC splitting. A
focused 128-vector smoke now shows `write_append_base_delta_hbc_lazy_fold`
clearing foreground write amp (`fg_node_bytes_vs_packed=0.3305`,
`fg_splits=0`, `storage_write_bytes_vs_packed=1.4508`,
`recall_delta=-0.025`) and
`write_mixed_base_delta_hbc_reassign_capacity` clearing foreground node/split
amp (`fg_node_bytes_vs_packed=0.3315`, `fg_splits=0`), storage bytes
(`storage_write_bytes_vs_packed=1.4894`), and recall/QPS
(`recall_delta=0.200`, `qps_vs_packed=0.7826`) with
`fg_delta_records_per_append=2.2963` and no overfull debt. The mixed row uses
zero dirty boundary reassignment by default; its shape is posting-local repair
plus bounded layout repair, not recall bought by moving vectors into overfull
targets. The remaining focused failure is append-specific: under the current
layout budget, append repair leaves bounded over-capacity debt
(`backlog_overfull=4`, `backlog_max_over_capacity=4`). Raising the overfull-only
layout budget to drain that debt fixed capacity debt in a probe but regressed
storage bytes and recall, so the next optimization target is a smarter append
maintenance/split policy rather than more repair work. The write bench now
emits split-input diagnostics (`split_leaf_input_members_total`,
`split_leaf_input_overflow_members_total`, and bulk-leaf rebuild counters) so
this can be measured directly. The latest append smoke consumed 64 overflow
members across 8 maintenance splits (`split_input_members=192`,
`split_input_overflow=64`) while still leaving 4 overfull postings. That points
to imbalanced binary split output recreating capacity debt, not simply too few
split attempts. Posting-maintenance splits now request a capacity-aware binary
split output: when the split input can fit into two postings, the split result
is rebalanced by centroid-distance margin so neither child exceeds `leaf_size`.
A regression test covers the skewed case that used to produce an oversized
child. In the focused append smoke, the layout-8 policy now drains capacity debt
(`backlog_overfull=0`, `backlog_max_over_capacity=0`) with the same 8
maintenance splits. The split helper also keeps the larger capacity-balanced
child on the old posting ID when possible, so only the smaller child needs
vector-to-posting remaps. That drops append storage write bytes from `1.5015x`
to `1.4551x` packed HBC while keeping `backlog_overfull=0` and
`backlog_max_over_capacity=0`.

The VDBB-shaped warm batch-apply row now reports foreground route fanout
(`fg_route_leaf_groups`, `fg_route_items`, and
`fg_route_items_per_leaf_group`) so we can distinguish routing scatter from
physical delta fragmentation. A focused 128-vector VDBB smoke initially routed
64 inserted items into only 6 leaf groups but fell back to per-item insert work
for 26 items, producing 30 foreground delta appends and
`fg_node_bytes_vs_packed=2.2927`. For bulk ingest rows that explicitly defer
oversized leaf splits to posting maintenance, the grouped insert path now allows
temporarily larger overfull leaves before falling back. The same smoke now keeps
63 of 64 items in the grouped path, drops foreground delta appends from 30 to 6,
raises `fg_delta_records_per_append` from `2.1333` to `10.67`, and lowers
`fg_node_bytes_vs_packed` from `2.2927` to `1.3083`. A follow-up changed
deferred overfull base-delta leaf saves to publish only explicit posting state
instead of rewriting the packed leaf body, and the write bench now attributes
bulk-finish repair artifacts to repair rather than foreground. With the compact
assignment-map value, the same focused no-legacy-format VDBB smoke clears the
focused VDBB gates: `fg_node_bytes_vs_packed=0.3103`,
`storage_write_bytes_vs_packed=1.4233`, `lsm_run_bytes_vs_packed=1.3932`,
`storage_write_files_vs_packed=1.2308`, `lsm_runs_vs_packed=1.5`,
`recall_delta=0.100`, `qps_vs_packed=0.8739`, and
`p95_vs_packed=1.0769`, while draining overfull debt
(`backlog_overfull=0`, `backlog_max_over_capacity=0`) and preserving grouped
delta density (`fg_delta_records_per_append=10.67`). This is still a focused
128-vector result, not completion of the optimized claim: the full matrix still
needs same-leaf, random, semantic-drift, append-streaming, mixed, read-directory,
and 1M VDBB-scale rows before we call the SPFresh path optimized.

Fresh small matrix, default runner settings
(`samples=1 vectors=128 dims=16 batch_size=32 leaf_size=16
branching_factor=8 overwrite_hot_keys=32 overwrite_rounds=2`):

```text
overwrite_hot_vectors_warm + post-write queries:

packed_hbc + hbc:
  ns_per_vector=43203.13
  storage_write_bytes=59079
  storage_read_bytes=3738352
  save_node_calls=172
  split_leaf_calls=6
  ns_nodes_put_calls=490
  ns_nodes_append_calls=0
  ns_nodes_value_bytes=41778
  posting_delta_append_calls=0
  posting_delta_fold_records=0
  lsm_total_run_bytes=49255
  post_write_query_ns_per_query=12937.50
  post_write_queries_per_second=77294.69
  post_write_recall_at_k=0.5500
  post_write_recall_hits=44
  post_write_recall_total=80
  post_write_storage_read_bytes=0
  post_write_search_workspace_bytes=7816
  post_write_warm_recall_at_k=0.5500

base_delta + hbc + lazy repair + eager fold:
  ns_per_vector=33843.75
  posting_repair_after_write_ns_per_vector=28078.13
  storage_write_bytes=61367
  storage_read_bytes=4369478
  save_node_calls=8
  split_leaf_calls=0
  ns_nodes_put_calls=112
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=9024
  posting_maintenance_delta_fold_attempts=8
  posting_maintenance_delta_fold_skipped=0
  posting_maintenance_delta_fold_records=64
  posting_delta_append_calls=64
  posting_delta_fold_records=64
  lsm_total_run_bytes=48838
  post_write_query_ns_per_query=27062.50
  post_write_queries_per_second=36951.50
  post_write_recall_at_k=0.2500
  post_write_recall_hits=20
  post_write_recall_total=80
  post_write_storage_read_bytes=46599
  post_write_search_workspace_bytes=9096
  post_write_warm_recall_at_k=0.2500

base_delta + hbc + lazy repair + deferred fold:
  ns_per_vector=33484.38
  posting_repair_after_write_ns_per_vector=26531.25
  storage_write_bytes=53529
  storage_read_bytes=4369478
  save_node_calls=8
  split_leaf_calls=0
  ns_nodes_put_calls=104
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=7232
  posting_maintenance_delta_fold_attempts=8
  posting_maintenance_delta_fold_skipped=8
  posting_maintenance_delta_fold_records=0
  posting_delta_append_calls=64
  posting_delta_fold_records=0
  lsm_total_run_bytes=46016
  post_write_query_ns_per_query=25750.00
  post_write_queries_per_second=38834.95
  post_write_recall_at_k=0.2500
  post_write_recall_hits=20
  post_write_recall_total=80
  post_write_storage_read_bytes=91049
  post_write_profile_posting_overlay_delta_records=64
  post_write_search_workspace_bytes=9096
  post_write_warm_recall_at_k=0.2500

base_delta + hbc + lazy repair + capacity-safe reassignment/swap:
  ns_per_vector=33921.88
  posting_repair_after_write_ns_per_vector=58875.00
  storage_write_bytes=67188
  storage_read_bytes=4840488
  save_node_calls=16
  split_leaf_calls=0
  ns_nodes_put_calls=140
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=12906
  posting_maintenance_boundary_reassigned_vectors=23
  assignment_map_put_calls=110
  posting_maintenance_delta_fold_attempts=2
  posting_maintenance_delta_fold_records=8
  posting_delta_append_calls=64
  posting_delta_fold_records=8
  lsm_total_run_bytes=50620
  post_write_query_ns_per_query=18000.00
  post_write_queries_per_second=55555.56
  post_write_recall_at_k=0.6250
  post_write_recall_hits=50
  post_write_recall_total=80
  post_write_storage_read_bytes=51765
  post_write_search_workspace_bytes=9096
  post_write_warm_recall_at_k=0.6250

base_delta + hbc + lazy repair + overfull reassignment (pre-limit sample):
  ns_per_vector=30937.50
  posting_repair_after_write_ns_per_vector=32015.63
  storage_write_bytes=69992
  storage_read_bytes=4890703
  save_node_calls=10
  split_leaf_calls=0
  ns_nodes_put_calls=112
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=9655
  posting_maintenance_boundary_reassigned_vectors=54
  assignment_map_put_calls=118
  posting_maintenance_delta_fold_attempts=1
  posting_maintenance_delta_fold_records=4
  posting_delta_append_calls=64
  posting_delta_fold_records=4
  lsm_total_run_bytes=51954
  post_write_query_ns_per_query=18000.00
  post_write_queries_per_second=55555.56
  post_write_recall_at_k=0.6750
  post_write_recall_hits=54
  post_write_recall_total=80
  post_write_storage_read_bytes=56987
  post_write_search_workspace_bytes=9160
  post_write_warm_recall_at_k=0.6750

base_delta + flat_rabitq + lazy repair:
  ns_per_vector=35031.25
  posting_repair_after_write_ns_per_vector=27281.25
  storage_write_bytes=61367
  storage_read_bytes=4369478
  save_node_calls=8
  split_leaf_calls=0
  ns_nodes_put_calls=112
  ns_nodes_append_calls=64
  ns_nodes_value_bytes=9024
  posting_delta_append_calls=64
  posting_delta_fold_records=64
  lsm_total_run_bytes=48838
  post_write_query_ns_per_query=26562.50
  post_write_queries_per_second=37647.06
  post_write_recall_at_k=0.2250
  post_write_recall_hits=18
  post_write_recall_total=80
  post_write_storage_read_bytes=57405
  post_write_search_workspace_bytes=9096
  post_write_warm_recall_at_k=0.2250
```

```text
repaired bulk-build warm_query_no_metadata:

packed_hbc + hbc:
  ns_per_query=26000.00
  queries_per_second=38461.54
  storage_read_bytes=14607
  hbc_cache_total_bytes=11424
  search_workspace_bytes=7816
  approx_vectors_scored=800
  exact_vectors_scored=279

base_delta + hbc:
  ns_per_query=24500.00
  queries_per_second=40816.33
  posting_repair_after_build_ns_per_vector=9984.38
  storage_read_bytes=10806
  hbc_cache_total_bytes=11424
  search_workspace_bytes=9096
  profile_posting_overlay_cache_hits=50
  profile_posting_overlay_cache_misses=0
  approx_vectors_scored=800
  exact_vectors_scored=279

base_delta + flat_rabitq:
  ns_per_query=21000.00
  queries_per_second=47619.05
  posting_repair_after_build_ns_per_vector=8757.81
  storage_read_bytes=3899
  hbc_cache_total_bytes=11832
  search_workspace_bytes=9096
  profile_posting_overlay_cache_hits=64
  profile_posting_overlay_cache_misses=0
  approx_vectors_scored=1024
  exact_vectors_scored=295
```

This is the clearest current tradeoff:

- `base_delta` gives the overwrite behavior we want: zero foreground leaf
  splits in this run, 8 node saves instead of 172, and 9,024 packed-node value
  bytes instead of 41,778, with foreground work represented as 64 posting
  deltas and 64 folded records during repair.
- Deferred folding addresses the eager-fold write-amplification problem: the
  same 64 overwrite deltas stay in the tail, storage write bytes fall from
  61,367 to 53,529, node value bytes fall from 9,024 to 7,232, and LSM bytes
  fall from 48,838 to 46,016. The debt is explicit: post-write query reads rise
  from 46,599 to 91,049 bytes and the overlay must merge 64 delta records.
- The same threshold policy is now available to automatic maintenance. A small
  auto-maintenance overwrite smoke with
  `auto_posting_maintenance_min_dirty_postings=999` skipped pre-commit repair
  and reported the explicit remaining debt: 8 dirty postings, max dirty age 8,
  centroid lag 12, and payload lag 12. The same smoke with
  `auto_posting_maintenance_min_dirty_postings=1` repaired 8 postings, folded
  34 records, and left 4 dirty postings because the repair budget was capped.
  A tail-debt-triggered smoke with dirty-count threshold still at 999 and
  `auto_posting_maintenance_min_delta_records_to_run=1` repaired 8 postings,
  skipped all 8 fold attempts under high fold thresholds, folded 0 records, and
  reported 8 postings with live delta tails, max tail length 12, and max
  delta-to-base ratio 7500 bps. This avoids recreating foreground write
  amplification through eager pre-commit folding while still letting
  centroid/payload repair make bounded progress.
- Capacity-safe dirty-posting reassignment now swaps members between sibling
  postings instead of relying on overfull targets. The apply path re-checks
  target capacity and source minimum occupancy before every non-swap move, so a
  stale plan cannot overfill a posting when overfull reassignment is disabled.
  Unit coverage now verifies both sides of that default: a useful
  capacity-neutral swap is applied, while a better direct move into a full
  target is skipped when no useful swap exists.
  It moved 23 vectors and raised post-write recall from 0.25 to 0.625 without
  creating overfull posting debt. The earlier unbounded overfull shortcut
  reached 0.675 by moving 54 vectors, but current comparison runs bound that
  row with explicit max-overfull-posting and max-over-capacity-member limits.
  It remains a contrast case, not the default target.
- Capacity-pressure maintenance can now run on layout debt, not only dirty
  posting state. Opt-in `split_full_postings` lets automatic/idle repair split
  full-but-not-overfull postings when the postings-at-capacity gate fires,
  creating slack for later non-overfull reassignment instead of requiring
  overfull moves. Bench rows expose this through
  `auto_posting_maintenance_split_full_postings`,
  `posting_maintenance_split_postings`, per-pass observed maxima such as
  `auto_posting_maintenance_observed_max_layout_changes`, and the capacity
  backlog counters. A focused 128-vector auto-maintenance smoke accumulated 9
  maintenance splits across 2 automatic repair passes while keeping the maximum
  observed layout work per pass at the configured cap (`8/8`). That is the
  bounded-background shape we want the gate to test; aggregate split count still
  matters for write amplification, but it is not the same invariant as the
  per-pass scheduler budget.
- Random overwrite coverage now uses the same post-write exact recall path.
  In the small default matrix, packed HBC had 11 foreground leaf splits and
  0.475 recall; `base_delta + hbc + capacity-safe reassignment/swap` had zero
  foreground splits, moved 22 vectors, and reached 0.60 recall. This is still
  small-scale evidence, but it avoids tuning only for hot-key churn.
- Mixed insert/delete/update coverage now exercises canonical `base_delta`
  through a single batch-apply workload. The smoke run updates 32 vectors,
  appends 16, deletes 16, emits 64 posting-delta records, skips all 7 repair
  fold attempts under the deferred-fold threshold, and reaches 0.80 post-write
  recall@5 on 8 read-after-write queries. This required batch cursor support
  through the vector-index storage wrappers so base/delta materialization can
  scan tails inside write batches, plus explicit tombstone deltas on grouped
  delete paths.
- Append-heavy streaming coverage now builds a warm index, appends a 64-vector
  tail, and queries the expanded post-write population. In the small
  `base_delta + hbc + lazy repair` smoke, the append stream emitted 64 posting
  deltas, repaired 4 dirty postings, skipped all 3 fold attempts under the
  deferred-fold threshold, left `active_count_after=192`, and reached 0.40
  post-write recall@5 on 8 read-after-write queries. This makes the benchmark
  matrix cover append-heavy streaming separately from empty-index ingest.
- `base_delta + hbc` repaired reads are close to packed HBC for this small
  warm no-metadata workload, while using more search workspace for the posting
  overlay cache.
- `base_delta + flat_rabitq` proves the separate centroid-directory path is a
  real query input and has lower warm read bytes here, but it probes/scores more
  postings and is slower. It is still an experiment, not the scalable directory
  answer.
- the new post-write recall fields expose the missing repair policy: in this
  synthetic overwrite workload, hot vector IDs are replaced by vectors copied
  from other clusters. The local `base_delta` replacement path keeps those
  vectors in their current postings, so centroid/payload repair alone leaves
  recall lower than packed HBC. The new reassignment row shows the next
  maintenance policy direction: changed vectors need bounded relocation or
  swap/reassignment work, not only delta folding and centroid refresh.

Focused recall equivalence smoke:

```text
zig build recall-harness -- \
  --dataset-dir testdata/vectorsets --suite hbc \
  --dataset random-20d-1k.gob

zig build recall-harness -- \
  --dataset-dir testdata/vectorsets --suite hbc \
  --dataset random-20d-1k.gob \
  --posting-storage base_delta --repair-postings-after-build

zig build recall-harness -- \
  --dataset-dir testdata/vectorsets --suite hbc \
  --dataset random-20d-1k.gob \
  --posting-storage base_delta --repair-postings-after-build \
  --centroid-directory flat_rabitq --flat-centroid-probe-count 8
```

All three runs matched the existing recall baselines for both fixture cases:

```text
randomize=false:
  recall(E=99.50 IP=99.50 C=98.50)
  expected(E=99.50 IP=99.50 C=98.50)
  OK

randomize=true:
  recall(E=97.50 IP=99.00 C=97.50)
  expected(E=97.50 IP=99.00 C=97.50)
  OK
```

This is not broad proof that `base_delta` is generally equivalent yet, but it
does prove the repaired base/delta posting overlay and flat centroid-directory
path can satisfy an existing HBC recall fixture, including randomized
orthogonal transforms and all three metrics.

### Phase 7: Alternative centroid directories

Objective:
- decide whether HBC is still the right centroid directory

Candidate implementations:
- current HBC over posting centroids
- exact scan for small centroid counts
- graph/HNSW-like directory over posting centroids
- flat IVF-style directory for simpler experiments

Acceptance:
- choice is based on measured read latency, recall, write amplification, and
  maintenance debt
- no posting-store rewrite is required to swap directory implementations

## Metrics To Track

Search:

- query latency p50/p95/p99
- centroid directory time
- postings loaded per query
- RaBitQ vectors scored per query
- exact rerank vectors per query
- recall at fixed `k`
- recall versus `nprobe` or search width

Writes:

- foreground write latency p50/p95/p99
- nodes/postings written per vector write
- quantized payload rebuilds per vector write
- centroid-directory updates per vector write
- split/merge/reassignment queue depth
- dirty posting count and max dirty age
- delta/tombstone tail ratio by posting
- posting delta key grouping: delta append calls versus logical delta records
- posting capacity debt: overfull posting count, postings at capacity, and max
  members over capacity
- boundary-reassignment decisions: moved vectors, capacity skips, minimum-source
  skips, and swap moves

Storage/cache:

- centroid directory cache hit rate
- posting payload cache hit rate
- raw vector cache hit rate
- posting overlay-cache evictions/skips/resident bytes under bounded caps
- bytes read per query
- bytes written per vector update

## Risks

- A refactor that only renames current HBC pieces will not improve performance.
- A centroid HBC updated eagerly per write may be worse than current HBC.
- Stale posting centroids can hurt recall if maintenance lag is too high.
- Background split/merge and any boundary reassignment need clear bounds to
  avoid unbounded maintenance debt.
- Close/reopen, committed modeled-crash, and failed-fold write-fault tests now
  cover key-family interpretation for pending and folded `base_delta` state.
  The modeled committed-crash and failed-fold recovery paths explicitly verify
  posting base, posting delta tail, centroid-directory record, and assignment
  map agreement before search, including committed multi-posting batch updates
  and compact grouped multi-record delta-tail values that must survive crash,
  decode as v2, materialize, fold, and reopen cleanly after folding
  and deletes after modeled crash. Unit coverage also verifies explicit posting
  artifact deletion removes base, centroid-directory, and delta-tail state for
  the deleted posting without crossing into other posting tails. Foreground
  overwrite/delete write-fault tests cover rollback to the previous committed
  state after modeled crash. Broader randomized fault schedules still need to
  cover publish boundaries, partial write-fault schedules across multi-posting
  batches, and arbitrary corrupt quantized payload bytes across every routing
  and posting shape.
- Introducing multiple directory implementations too early will distract from
  the main maintenance-policy experiment. The current `two_level_rabitq`
  directory is the scalable candidate, not final proof: it keeps coarse block
  selection separate from selected posting-centroid evaluation and the optimized
  gate requires it to prune against flat at the 1M-vector shape before we call
  the posting layout optimized.

## Near-Term Recommendation

Keep the LSM substrate; finish changing the vector-index maintenance model.

The current implementation has the behavior-preserving split between:

```text
CentroidDirectory
PostingStore
AssignmentMap
```

Lazy posting maintenance, dirty backlog stats, bounded repair, canonical
`base_delta` posting appends, explicit assignment maps, centroid-directory
records, exact post-write recall measurement, mixed insert/delete/update
and append-heavy read-after-write coverage, bounded overlay-cache
instrumentation, explicit capacity-debt/reassignment-skip counters,
debt-gated automatic maintenance, a two-level RaBitQ centroid-directory
candidate, and committed modeled crash/reopen plus failed-fold/foreground-
mutation fault coverage are now in place behind opt-in knobs. The remaining
work before calling this optimized is to tune those debt gates under real
ingest/query load, choose split/merge/reassignment work with non-overfull
capacity accounting, prove the two-level directory prunes enough at the
1M-vector shape, tighten the posting file layout for sequential reads, add
broader randomized fault-injection coverage around publish boundaries and
multi-posting batches, and run the comparison at 1M-vector scale against the
existing packed-HBC index.

For VDBB-shaped two-level RaBitQ rows, the comparison runner now keeps the
coarse block probe count at least as large as the final posting probe count.
This preserves a bounded final posting search while avoiding over-pruning at
the coarse directory layer; the optimized gate checks this explicitly with the
`two_level_block_probe_budget` marker.
