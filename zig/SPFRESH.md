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
- Use the refactor to test SPFresh-style maintenance policies: lazy centroid
  refresh, local split/merge, and targeted boundary reassignment after
  split/merge if we later choose to enforce a nearest-partition invariant.

The important distinction is not RaBitQ versus some other quantizer. The
important distinction is whether routing and posting maintenance are cleanly
separated enough that posting updates can stay local.

## Implementation Status

Current status:

- Phase 1 is implemented.
- `lib/vectorindex/src/posting.zig` defines the initial `PostingId`,
  `PostingView`, `PostingStore`, `AssignmentMap`, and `CentroidDirectory`
  names.
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
- Leaf postings now carry persisted maintenance state: mutation version,
  centroid refresh version, payload refresh version, and dirty flags. The state
  is stored as a backward-compatible node side record.
- A bounded posting maintenance pass now exists. It scans leaf postings,
  repairs dirty centroids/payloads, persists clean posting state, refreshes HBC
  ancestor centroids when needed, and reports repair counters.
- A disabled-by-default `lazy_posting_maintenance` mode now lets foreground
  leaf writes persist dirty posting state while deferring leaf centroid,
  payload, and ancestor refresh work to posting maintenance.
- Dirty posting backlog visibility now exists via `PostingBacklogStats` and a
  `std.Io.Writer` debug renderer, so we can inspect how much deferred work is
  accumulating.
- A disabled-by-default bounded automatic repair hook now runs before write
  commit when `auto_posting_maintenance_max_postings` is non-zero. This lets us
  amortize lazy posting repair without introducing a background thread.
- Dense-index config parsing and DB/API runtime status now expose the lazy
  posting knobs and posting backlog/maintenance counters.
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
- `hbc_vecs`: raw vectors and vector metadata
- `hbc_meta`: index metadata and vector-to-leaf assignments

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
- posting centroid updates can be batched or made approximate
- splits/merges can run as local background work
- write amplification should drop for continuous insert/update/delete workloads

Potential losses:

- background maintenance becomes necessary
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
   lib/vectorindex/src/
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

### Phase 6: Alternative centroid directories

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
- tombstone ratio by posting

Storage/cache:

- centroid directory cache hit rate
- posting payload cache hit rate
- raw vector cache hit rate
- bytes read per query
- bytes written per vector update

## Risks

- A refactor that only renames current HBC pieces will not improve performance.
- A centroid HBC updated eagerly per write may be worse than current HBC.
- Stale posting centroids can hurt recall if maintenance lag is too high.
- Background split/merge and any boundary reassignment need clear bounds to
  avoid unbounded maintenance debt.
- Introducing multiple directory implementations too early will distract from
  the main maintenance-policy experiment.

## Near-Term Recommendation

Start with the refactor, not a new index.

The first useful engineering milestone is a current-behavior-preserving split
between:

```text
CentroidDirectory
PostingStore
AssignmentMap
```

Lazy posting maintenance, dirty backlog stats, and bounded pre-commit repair are
now in place behind disabled-by-default knobs. The next useful work is measuring
that policy on write-heavy workloads and then deciding whether split/merge or
targeted boundary reassignment needs to change. Replacing HBC as the centroid
directory should remain a separate, well-scoped optimization rather than a full
index rewrite.
