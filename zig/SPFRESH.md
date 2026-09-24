# SPFresh-Style HBC Indexing

## Decision

Antfly evaluated whether to move toward an SPFresh-style mutable AKNN index
instead of the current HBC implementation.

The decision: do not build a separate SPFresh index. Instead, refactor HBC so
its implicit pieces become explicit:

- a centroid/routing directory
- a posting store
- a vector-to-posting assignment map

HBC is the first `CentroidDirectory` implementation. The refactor is also the
vehicle for SPFresh-style maintenance policies — lazy centroid refresh, local
split/merge, and targeted boundary reassignment after split/merge if a
nearest-partition invariant is later enforced.

The distinction that matters is not RaBitQ versus some other quantizer. It is
whether routing and posting maintenance are cleanly separated enough that
posting updates can stay local.

Reasoning behind the decision:

1. **Refactor before replacing.** Building seams inside the current HBC
   avoided duplicating tree/search/quantization behavior before there was
   measured evidence that a new index was needed.
2. **HBC stays the first `CentroidDirectory`.** This preserves current
   behavior while decoupling the API. Later centroid directory implementations
   can be swapped in behind the same interface.
3. **Current leaves are the initial postings.** Leaf IDs remain posting IDs,
   so the existing vector-to-leaf map doubles as the initial
   vector-to-posting map.
4. **RaBitQ posting payloads are kept.** The leaf RaBitQ payload was already
   the right conceptual primitive for a posting list; the refactor moved
   ownership rather than reinventing quantized scoring.
5. **Centroid-directory maintenance is not eager.** Eagerly updating a
   centroid HBC on every vector write would erase the main SPFresh-style
   advantage and increase write amplification, so writes do not do this.
6. **The maintenance policy is what gets measured, not just the index shape.**
   The open question is whether lazy posting maintenance improves
   write-heavy workloads without unacceptable recall/latency regressions.
7. **Implementation is split by responsibility, not fanned into many small
   modules.** The code lives in one base HBC implementation file plus one
   SPFresh-style maintenance extension file, plus shared posting
   infrastructure:

   ```text
   zig/lib/vectorindex/src/
     hbc_index.zig       # base HBC tree/index mechanics and public facade
     spfresh_index.zig   # SPFresh-style posting maintenance layer
     posting.zig         # shared posting data/state helpers
   ```

   `hbc_index.zig` keeps the base index mechanics: node, vector, metadata, and
   vector-to-posting load/save helpers; search and rerank integration;
   insert/update/delete and batch write paths; the fundamental HBC tree
   split/merge primitives; internal-node quantized payload helpers; and
   compatibility wrappers for the public API.

   `spfresh_index.zig` owns the SPFresh-style policy layer:
   `postingBacklogStatsTxn`, `repairDirtyPostingsTxn`,
   `repairDirtyPostingsTxnWithOptions`, `runAutoPostingMaintenanceTxn`, local
   maintenance helpers for posting split/merge decisions, sibling boundary
   reassignment, and lazy posting centroid/payload refresh policy.

   `posting.zig` remains neutral shared infrastructure, not a separate index:
   `PostingId`, `PostingView`, `PostingState`, `PostingStore`,
   `AssignmentMap`, and posting maintenance option/result structs.

   `spfresh_index.zig` is an extension over the existing HBC index type, not a
   second object model. Its functions use the same generic style as the rest
   of HBC:

   ```zig
   pub fn repairDirtyPostingsTxnWithOptions(
       self: anytype,
       txn: anytype,
       options: posting.PostingMaintenanceOptions,
   ) !posting.PostingMaintenanceResult {
       ...
   }
   ```

   `hbc_index.zig` re-exports these as thin wrappers so adapter and DB call
   sites do not churn:

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

   This gives clear naming without claiming there is a fully independent
   SPFresh index implementation. A later, genuinely distinct index type could
   still reuse `posting.zig` and selected maintenance code behind a cleaner
   interface.

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
- `hbc_vecs`: raw vectors and vector metadata
- `hbc_meta`: index metadata and vector-to-leaf assignments

One `Node` abstraction does both jobs:

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

## The SPFresh Alternative

This is the alternative architecture that was evaluated and not adopted as a
separate index. It is described here because it is the design space HBC's
posting seams were built to test.

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

### HBC as the centroid directory is not by itself a new index

If HBC is used as the centroid directory and synchronously updated whenever a
posting centroid moves, the result is not meaningfully different from what
already exists. It would look like:

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

If every write deletes and reinserts a posting centroid in a centroid-HBC, the
design would likely be worse than current HBC. The value comes from lazy and
batched directory maintenance plus local background repair — which is what the
posting maintenance layer below implements.

### The leaf RaBitQ payload stays conceptually similar

The RaBitQ posting list does not need to be replaced to test the SPFresh
hypothesis. Leaf payloads already quantize member vectors relative to a leaf
centroid, which maps directly to:

```text
posting centroid + member vectors -> RaBitQ posting payload
```

### The centroid index stays separate from posting payloads

Even with HBC as the first centroid directory, the abstraction separates:

- how postings are selected
- how postings store and score members
- how vector IDs are assigned to postings

This is the seam that lets maintenance policy change later without rewriting
search and quantization together.

## Performance Considerations

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
- posting centroid updates can be batched or made approximate
- splits/merges can run as local background work
- write amplification should drop for continuous insert/update/delete workloads

Potential losses:

- background maintenance becomes necessary
- stale centroids can reduce recall until repaired
- local reassignment logic is more complex than pure tree maintenance
- correctness around versions, tombstones, and concurrent search becomes more
  explicit

The strongest reason to pursue this direction is high mutable-ingest pressure,
not static search performance alone.

## Architecture

### Boundaries

`posting.zig` defines the `PostingId`, `PostingView`, `PostingStore`,
`AssignmentMap`, and `CentroidDirectory` names. Existing vector-to-leaf
assignment storage flows through `AssignmentMap`, preserving the original key
format, so introducing the boundary required no new index format and no
material behavior change.

### PostingStore

Leaf member and RaBitQ operations are posting-owned. Leaf/member scoring,
online leaf member append/remove, leaf centroid recompute
(`PostingStore.recomputeCentroid`), RaBitQ refresh vector materialization
(`PostingStore.loadTransformedVectorsForQuantizedRefresh`), and quantized
payload cache/write mechanics (`PostingStore.refreshQuantizedPayload`) all flow
through `PostingStore`. Internal-node quantized payloads remain owned by HBC.

Posting-level operations exposed by this layer:

- `loadPosting(posting_id)`
- `appendMember(posting_id, vector_id, vector)`
- `removeMember(posting_id, vector_id)`
- `rebuildPosting(posting_id)`
- `splitPosting(posting_id)`

Leaf postings carry persisted maintenance state — mutation version, centroid
refresh version, payload refresh version, and dirty flags — stored as a
backward-compatible node side record.

### CentroidDirectory

Insert routing flows through `CentroidDirectory.findPosting`, which still
delegates to current HBC leaf routing. Search asks `CentroidDirectory` for
posting IDs and is implemented using current HBC traversal, preserving the
existing beam/search-width behavior as the default:

```text
directory.search(query) -> posting IDs
posting_store.score(posting IDs) -> candidates
rerank(candidates) -> final results
```

Current HBC remains the default directory implementation; the interface is
what would let it be swapped later (see [Open work](#open-work)).

### Posting Maintenance

A bounded posting maintenance pass scans leaf postings, repairs dirty
centroids/payloads, persists clean posting state, refreshes HBC ancestor
centroids when needed, and reports repair counters.

A disabled-by-default `lazy_posting_maintenance` mode lets foreground leaf
writes persist dirty posting state while deferring leaf centroid, payload, and
ancestor refresh work to the posting maintenance pass. Dirty posting backlog
visibility is available via `PostingBacklogStats` and a `std.Io.Writer` debug
renderer, so accumulated deferred work is inspectable.

A disabled-by-default bounded automatic repair hook runs before write commit
when `auto_posting_maintenance_max_postings` is non-zero, amortizing lazy
posting repair without a background thread. DB idle maintenance also drains
dirty dense posting work outside the foreground write hook. Dense-index config
parsing and DB/API runtime status expose the lazy posting knobs and posting
backlog/maintenance counters.

Bounded local posting layout maintenance handles split, merge, and boundary
reassignment: oversized postings can split, underfull postings can merge with
nearby siblings, and sibling boundary reassignment can move vectors that are a
better local fit elsewhere.

### Persistence

A first immutable posting segment container stores opaque packed posting,
quantized-checkpoint, centroid-directory, mutation, and tombstone values in a
posting-local sorted index. The v2 format checks the footer and index at
admission, validates strict key ordering and value bounds, and lazily checks
each payload on access, so opening a large segment is not O(file size). A
concurrent-safe verified reader memoizes both successful and failed per-entry
verification, avoiding a full payload CRC on every query.

A framed posting WAL codec makes whole batches query-visible with an explicit
commit record. Its CRC covers routing metadata and payload, replay ignores
incomplete or uncommitted tails, and a checksummed checkpoint records the exact
segment generation and committed WAL prefix. Keeping payloads opaque lets the
current packed HBC read path coexist with a small WAL tail instead of forcing
base/delta replay on every query. Runtime wiring for this WAL path remains
experimental.

## Metrics

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
- tombstone ratio by posting

Storage/cache:

- centroid directory cache hit rate
- posting payload cache hit rate
- raw vector cache hit rate
- bytes read per query
- bytes written per vector update

An opt-in lazy-versus-eager posting maintenance benchmark exists to evaluate
these. Current local samples show lazy centroid deferral is working, but
centroid deferral alone is not the dominant write-latency cost in those runs
(see [Open work](#open-work)).

## Risks

- A refactor that only renames current HBC pieces would not improve
  performance on its own.
- A centroid HBC updated eagerly per write could be worse than current HBC —
  this is why eager updates are avoided (see Decision, item 5).
- Stale posting centroids can hurt recall if maintenance lag is too high.
- Background split/merge and boundary reassignment need clear bounds to avoid
  unbounded maintenance debt; assignment-map/posting-list mismatches are
  treated as consistency bugs, not normal maintenance debt.
- Introducing multiple directory implementations too early would distract from
  the main maintenance-policy experiment.

## Open work

- **Alternative centroid directories.** Whether HBC should remain the centroid
  directory is unresolved. Candidates if it is revisited: current HBC over
  posting centroids, exact scan for small centroid counts, a graph/HNSW-like
  directory over posting centroids, or a flat IVF-style directory for simpler
  experiments. Any change here should be chosen from measured read latency,
  recall, write amplification, and maintenance debt, and should not require a
  posting-store rewrite to swap implementations. Replacing HBC as the centroid
  directory should stay a separate, well-scoped optimization rather than a
  full index rewrite.
- **Dominant write-latency cost.** The lazy-versus-eager posting maintenance
  benchmark shows centroid deferral working, but centroid deferral alone is
  not the dominant write-latency cost in current local samples; that cost has
  not yet been identified.
- **Posting WAL runtime wiring.** The framed posting WAL codec exists, but its
  runtime wiring into the write/read path remains experimental rather than a
  committed default.
