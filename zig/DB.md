# DB Contract And Roadmap

This note is the DB-layer landing page for contract decisions, storage backend
boundaries, and local-shard execution roadmap work.

For the canonical enrichment architecture and artifact identity contract, see
[ENRICHMENTS.md](ENRICHMENTS.md).

## Write Contract

`DB.batch()` is document-first.

For each document write:

- parse once
- strip DB-owned special fields from the stored base document
- derive text/vector/graph directives from that parsed form
- commit the base document state
- hand derived work to index application or, next, the derived-work log

Special-field writes that do not replace the base document must preserve the
current stored document.

## Derived Artifact Contract

Derived artifacts are enrichment-owned, not index-owned.

Storage keys should use:

- `<doc>:e:<type>:<name>:...`

Examples:

- `<doc>:e:chunk:body_chunks_v1:0`
- `<doc>:e:asset:ocr_text_v1`
- `<doc>:e:embedding:body_dense_v1`

This keeps artifact type explicit and allows one chunking or asset-output
pass to feed multiple indexes without duplicating sidecar storage.
Model-produced text, OCR, transcripts, classifications, entity extraction, and
other derived payloads use the generic `asset` artifact type, with the specific
media/schema encoded by `content_type`, artifact name, and enrichment metadata.

Current first implementation:

- chunk artifacts produced by the leased enrichment runtime
- generator configs may declare `chunk_name`
- chunk artifacts are stored under `<doc>:e:chunk:<chunk_name>:<chunk_id>`
- overwrites and deletes clear stale chunk artifacts before regeneration

## Search Contract

`DB.search()` is coordinator-style orchestration over named result sets.

Current result-set sources:

- `full_text`
- `full_text_queries`
- `dense`
- `dense_queries`
- `sparse`
- `sparse_queries`
- `graph_queries`
- fusion via `merge_config`

Graph queries may reference:

- `$full_text_results`
- `$full_text_results.<name>`
- `$aknn_results.<index>`
- `$graph_results.<name>`
- `$fused_results`

`expand_strategy` is a DB-level post-graph operation over the top-level hit set.

## Transaction / HLC Contract

Local transaction semantics follow the same visible-version rule used by the
shared compat corpus:

- transaction intents remain invisible until resolution
- `commitTransaction(txn_id, commit_timestamp)` makes the committed value visible
- the visible version/timestamp of the committed key becomes `commit_timestamp`,
  not the original intent timestamp
- transaction records preserve the original begin timestamp separately from the
  visible commit version so distributed resolution can propagate commit version
  without losing coordinator-start metadata
- participant-style resolution should use the propagated commit version directly;
  a node resolving remote intents should not need to infer visibility from local
  wall-clock time or from the original begin timestamp
- optimistic predicates compare against that visible committed timestamp
- `abortTransaction` removes intents without changing the prior committed value
  or its timestamp
- local recovery uses the richer transaction record for single-node cleanup:
  stale pending records auto-abort, finalized records repair any leftover
  intents using the stored status plus visible commit version, and old finalized
  records are cleaned once no local intents remain
- transaction records may also carry participant and resolved-participant
  metadata; cleanup must be deferred while any participant remains unresolved,
  even if the local coordinator intents are already repaired
- coordinator-side recovery may retry unresolved participants through a
  transport-agnostic resolver callback; only successful acknowledgements should
  mark participants resolved
- cleanup remains gated on both conditions:
  local intents repaired and all participants resolved

This keeps ordinary batch predicates and transactional predicates on one
versioning model.

## Next Async Boundary

The async index/enrichment manager should sit between document preprocessing and
derived index mutation.

The intended flow is:

1. parse and extract once
2. commit base docs
3. append one batch-shaped derived-work record
4. let per-index workers advance watermarks from that log

The derived log should be sequence-based and idempotent. Rebuild-from-docstore
remains the fallback, not the normal replay path.

Once every managed index has advanced past a sequence, the derived log may be
truncated at the global minimum applied watermark.

## Runtime Ownership

DB background work uses a swappable execution capability instead of each
subsystem owning private threads. Native/server deployments attach a node-owned
runtime shared by DBs and stores on the node. Tests, embedded use, single-DB
usage, and WASM keep synchronous or manually pumped fallbacks.

The current model is generic runtime ownership plus typed subsystem adapters:

- the node owns capacity and lifecycle through `BackendRuntime`
- DBs borrow that runtime through `OpenOptions.backend_runtime`
- standalone DB opens may create an owned fallback runtime
- `BackendRuntime` owns the optional `std.Io.Threaded` provider, the
  owner-scoped `DurableJobLane`, and owner id allocation
- typed adapters own the meaning of their work, including derived replay, LSM
  flush/compaction, text merge, enrichment replay, TTL cleanup, and transaction
  recovery
- request-local fanout uses operation execution context and `std.Io.Group`,
  not the durable background-job lane

The important rule is that background execution is an optimization over the
inline path, not a separate durability or visibility model. Correctness cannot
depend on OS threads existing.

Runtime-backed work follows these boundaries:

- each backend owns its own flush/compaction state, queue limits, shutdown flag,
  and write-pressure policy
- the node runtime owns global worker limits so many DBs and stores do not
  multiply thread counts
- durable jobs carry an owner id so close/drain can target one DB/store/shard
  without disturbing other owners
- `BackendHandle.close()` drains or cancels submitted work before destroying the
  backend
- WASM progress is available through inline execution or explicit bounded
  maintenance/executor polling
- DBs own fallback runtimes only when no node runtime is provided

Current status:

- `BackendRuntime` is heap-owned at node/server construction sites and borrowed
  through DataServer, provisioned, hosted, metadata, and swarm DB open paths
- derived replay, full-text merge, enrichment replay, TTL cleanup, transaction
  recovery, and LSM background flush are under the shared runtime model
- `DurableJobLane` has inline and threaded implementations
- LSM has a typed adapter over the durable lane and blocks writers when bounded
  deferred immutable queues are full
- close/drain, inline/WASM-style progress, native threaded sharing, and
  owner-scoped DB runtime tests cover the lifecycle contract
- raft replica placement reconciliation remains foreground control-loop work,
  while distributed transaction recovery is already runtime-owned

## Storage Backend Boundary

The DB layer should keep most of `antfly-zig` pure Zig and portable while
isolating unavoidable OS-specific storage behavior behind a narrow backend
boundary.

The current shape is:

- higher-level runtimes, indexing, query, and most tooling no longer depend on
  `std.c`
- the remaining dense POSIX surface is concentrated in the LMDB backend
- shared backend contracts and adapters exist
- concrete backends exist for LMDB, in-memory KV, and durable prefix/LSM
- backend conformance coverage exists
- top-level DB primary-backend selection exists
- snapshot export/restore and split semantics are backend-neutral, with
  backend-specific fast paths where useful

DB-level durable-LSM coverage now proves the backend seam across:

- basic read, write, and reopen
- full-text persistence and reopen
- derived replay on reopen
- delete-index persistence
- indexed deletes and overwrites
- dense, sparse, and graph query flows
- `_embeddings` and `_edges` document special-field mutations
- split and merge cutover flows
- snapshot restore
- TTL lease-owned cleanup
- named-query fusion and graph expansion
- chunked dense-index and chunk-enrichment reopen flows

That means the remaining work is mostly confidence and product-boundary work,
not backend abstraction bring-up.

The goal is not to remove LMDB now. The goal is to keep LMDB as one backend
while making a future portable pure-Zig backend possible without leaking LMDB
assumptions upward.

### Backend Contract

Any DB storage backend needs to provide these semantics regardless of its
implementation strategy:

1. Transactions
   - read-only snapshot transactions
   - exclusive write transactions
   - nested child write transactions, or an explicit unsupported contract
   - commit and abort semantics
2. KV and range access
   - logical namespaces or partitions
   - point get, put, and delete
   - ordered iteration over key ranges
   - duplicate-key or multi-value support where callers rely on it
3. Durability
   - no-sync, data-sync-only, and fully durable commit policies
   - crash/reopen semantics documented at the backend boundary
4. Visibility and concurrency
   - when committed writes become visible to later readers
   - what long-lived reader snapshots see while writers commit
   - writer contention behavior
5. Maintenance operations
   - reopen
   - truncate or compaction equivalent
   - split/export/import hooks used by higher-level DB flows

The first backend-neutral code pieces are in:

- [pkg/antfly/src/storage/backend_types.zig](pkg/antfly/src/storage/backend_types.zig)
- [pkg/antfly/src/storage/backend_adapter.zig](pkg/antfly/src/storage/backend_adapter.zig)
- [pkg/antfly/src/storage/backend_lmdb_adapter.zig](pkg/antfly/src/storage/backend_lmdb_adapter.zig)

Those model:

- durability expectations
- read visibility
- read/write transaction modes
- logical namespaces or partitions
- cursor open requests
- ordered range-scan requests
- write-batch capability hints
- cursor start/seek/iteration semantics

### LMDB Boundary

The intentional LMDB/POSIX surface is concentrated in:

- [pkg/antfly/src/lmdb/env.zig](pkg/antfly/src/lmdb/env.zig)
- [pkg/antfly/src/lmdb/commit_support.zig](pkg/antfly/src/lmdb/commit_support.zig)
- [pkg/antfly/src/lmdb/readers.zig](pkg/antfly/src/lmdb/readers.zig)
- [pkg/antfly/src/lmdb/split_support.zig](pkg/antfly/src/lmdb/split_support.zig)
- [pkg/antfly/src/lmdb/writer_lock.zig](pkg/antfly/src/lmdb/writer_lock.zig)
- [pkg/antfly/src/storage/lmdb.zig](pkg/antfly/src/storage/lmdb.zig)

These files encode real storage semantics:

- mmap-backed page storage
- read snapshot visibility
- single-writer coordination
- durability and fsync behavior
- file growth and publication
- lock-table and reader-table behavior

Higher layers should depend on the backend contract instead of those details:

- [pkg/antfly/src/storage/docstore.zig](pkg/antfly/src/storage/docstore.zig)
- [pkg/antfly/src/storage/persistent.zig](pkg/antfly/src/storage/persistent.zig)
- [pkg/antfly/src/storage/wal.zig](pkg/antfly/src/storage/wal.zig)
- [pkg/antfly/src/storage/db/db.zig](pkg/antfly/src/storage/db/db.zig)
- [pkg/antfly/src/storage/hbc_adapter.zig](pkg/antfly/src/storage/hbc_adapter.zig)

They can use transactions and range scans, but should not depend on LMDB reader
tables, mmap assumptions, env refresh mechanics, file naming, or lock-file
details.

LMDB commit/publication stats are useful operational hooks, but they are
backend-specific extensions, not required backend-neutral semantics. The neutral
contract should cover correctness, durability policy, visibility, range access,
and split/export/import semantics.

### Backend Migration Plan

1. Freeze the boundary.
   - keep non-backend code off direct POSIX where practical
   - document transaction, scan, durability, and visibility semantics
   - identify higher-level files that still depend on backend details
2. Define shared backend types.
   - shared durability enum
   - shared backend options
   - shared namespace concept
   - shared write-batch capability
   - explicit transaction and cursor capability surface
   - backend-independent error mapping where possible
3. Move higher layers to the contract.
   - first adopters are `docstore.zig`, `persistent.zig`, and `wal.zig`
   - then reduce direct transaction threading in `hbc_adapter.zig`,
     `persistent.zig`, `docstore.zig`, and `index_manager.zig`
4. Keep proving the abstraction with multiple backends.
   - LMDB remains the mmap/single-writer backend
   - in-memory KV stays useful for tests and constrained environments
   - durable prefix/LSM is the portable backend direction

The likely Zig shape is intentionally narrow:

- a small backend module with shared option and durability enums
- a vtable-backed runtime object for environment open/close, transaction begin,
  namespace selection or binding, and sync/reopen helpers
- backend-specific transaction and cursor handles stored behind opaque pointers

This avoids forcing the whole codebase into a large generic type cascade while
still making backend behavior explicit.

Specialized engines such as the text persistent index, HBC, sparse, and graph
reverse index may continue to carry backend assumptions while the primary DB
store remains backend-selectable. Replatforming them onto the same backend
family is a follow-on decision, not a blocker for the primary store contract.

## Local Shard Backend Roadmap

The local-shard migration target is to make `ZigCoreDB` the real backend while
keeping the Go `DB` interface stable for callers above `StoreDB`.

The migration rule:

1. keep the Go `DB` interface stable
2. move the hot local data plane fully into Zig
3. keep distributed orchestration in Go
4. keep local control methods typed, not generic JSON

Treat the Go `DB` interface as three layers:

1. hot data plane
   - `Get`
   - `Scan`
   - `Batch`
   - `Search`
2. local control plane
   - `Open` and `Close`
   - range and split-state methods
   - schema/index control
   - snapshot, split, and finalize
3. higher-level local features
   - transactions
   - graph traversal
   - enrichment entrypoints

Current state:

- `Batch` already uses a typed binary C boundary
- `Search` has hot binary paths for dense kNN and simple text match
- `Get` and `Scan` have narrower Zig bridge fast paths than before
- remaining overhead is mostly Go rebuilding generic request or response
  structures around the Zig engine

### Data Plane Migration

Phase 1: finish the hot data plane.

- keep `Batch` on the typed C ABI path
- make `Search` binary-by-default internally, with fallback for rich legacy
  shapes
- keep shrinking `Get` and `Scan` result-shaping overhead
- add batch/multi-search support once single-request hot paths are stable

Acceptance:

- local dense/text search no longer pays generic JSON overhead
- `Get`, `Scan`, `Batch`, and narrowed `Search` cross the Go/Zig boundary in
  compact typed formats

Phase 2: port the local control plane.

- keep typed methods for `Open`, `SetRange`, `GetRange`, split-state CRUD,
  `AddIndex`, `DeleteIndex`, `UpdateSchema`, `Snapshot`, `Split`, and
  `FinalizeSplit`
- expose dedicated C API entrypoints instead of generic payloads
- keep Go as a thin shim over Zig C API

Acceptance:

- `ZigCoreDB` is mostly a binding layer, not a compatibility adapter

Phase 3: port remaining local features where profiling says it matters.

- transaction lifecycle and local recovery
- graph edge and traversal ops
- enrichment/local derived-batch entrypoints

The rule is to move these only when they are local-engine work, not distributed
coordinator work. Go keeps raft/distributed ownership; Zig owns local shard
execution for the bulk of `DB`.

## Hot-Path Search Wire

The hot-path search wire reduces Go/Zig boundary cost for local shard search by
replacing generic JSON request/response payloads with a narrow internal binary
codec for the hottest `coreDB.Search` shapes.

The public HTTP/store API stays JSON. Only the internal `coreDB` hot path moves
to a binary codec.

Initial shapes:

- dense kNN
- simple full-text without stored fields, aggregations, graph, explicit sort,
  or cursor

Principles:

- keep JSON fallback for richer search shapes until coverage is complete
- use a fixed header, append-only evolution, and offsets into trailing blobs
  for variable-width fields
- prefer packed IDs and packed hit metadata over per-hit allocation

### Dense Search Wire

Objective:

- remove JSON request build and generic result rebuild for dense search

Plan:

1. add a binary dense request/response codec in Zig C API
2. expose a dense wire entrypoint from
   [pkg/antfly/src/capi/db.zig](pkg/antfly/src/capi/db.zig)
3. add the matching Go-side codec in the zigdb bridge
4. route the narrowed dense path through binary wire first
5. keep the current JSON path as fallback

Acceptance:

- local dense search no longer marshals JSON on the hot path
- the Go adapter no longer rebuilds generic hit payloads before constructing
  `vectorindex.SearchResult`

### Simple Full-Text Search Wire

Objective:

- remove JSON request build and generic result rebuild for simple full-text

Plan:

1. add a binary request/response codec for `query_string`, `match`, `term`, and
   `match_phrase`
2. limit the first slice to no stored fields, aggregations, graph, explicit
   sort, or cursor
3. route the narrowed simple text path through the binary wire first
4. keep JSON fallback for richer full-text shapes

Acceptance:

- local simple full-text search no longer pays JSON/base64 overhead on the hot
  path
- small full-text searches are no longer dominated by bridge overhead

### Search Wire Evolution

The internal search wire should have:

- `magic`
- `version`
- `op`
- `flags`
- append-only evolution rules
- variable-width data in trailing blobs referenced by offset/length
- batch/multi-search support once single-search hot paths are stable

## Shard Split Roadmap

Shard splitting should be cheap enough that the system stops paying for full
logical copy plus index rebuild on every split.

The roadmap optimizes:

1. child shard creation without logical KV replay where possible
2. index handoff without full reindex where possible
3. mixed-range cleanup only where strictly necessary

The current state after recent split work:

- child docstore creation is page-level on Zig LMDB
- parent docstore reclaim is page-level on Zig LMDB
- text indexes use segment handoff and mixed-segment rewrite instead of full
  child rebuild and per-doc parent text deletion
- the next remaining split cost classes are non-text indexes, especially dense
  vector indexes

Principles:

1. Copy immutable state; do not replay documents unless forced.
2. Rewrite only mixed ranges.
3. Keep parent cleanup separate from child image construction in the first
   page-level implementation.
4. Add metadata first so split planning is cheap and deterministic.
5. Prefer subtree, block, or segment handoff over whole-index rebuild.
6. If rebuild is required, rebuild only the mixed remainder, not the full child
   index.

### Text Segment Handoff

Text index split should classify active segments using persisted key-range
metadata instead of rescanning the source docstore.

Required metadata:

- `min_doc_key`
- `max_doc_key`

Split behavior:

- `right-only` segments are copied unchanged to the child active manifest
- `left-only` segments stay in the parent unchanged
- `mixed` segments are rewritten into left and right replacement segments
- the original mixed segment is retired from both manifests

Mixed-segment rewrite can be driven from the segment blob itself using
`SegmentReader.storedDocDecompressed(...)` and
`buildTextSegmentFromDocuments(...)`, without rescanning the main docstore.

Temporary filtered mixed segments may be useful as an optimization layer: install
a mixed segment with a shard-side filter bitmap, finish the split cheaply, then
let later compaction produce clean segment ownership.

Acceptance:

- child text indexes are built mostly by manifest/segment handoff
- only mixed segments are rebuilt
- split can defer clean mixed-segment rewrite when correctness is preserved

### Page-Level LMDB Child Image

The child shard's main LMDB image should be built without logical KV replay.

Plan:

1. open a read snapshot on the source env
2. descend once to the split key
3. clone fully right-hand subtrees page-for-page into a fresh child env image
4. rebuild only the mixed branch spine and split leaf
5. emit fresh child meta and freeDB state

First version scope:

- unnamed main DB only
- correctness first
- parent cleanup stays separate

Acceptance:

- child docstore image is created from pages/subtrees, not logical key replay
- only the mixed path is rebuilt logically

Parent finalize should later avoid whole-range logical prune where metadata or
page structure can answer the same question, and should reclaim retired page
ranges and retired segment manifests cleanly.

### Dense Vector Split

Dense vector indexes should reuse existing HBC structure where possible instead
of rebuilding the child from scratch.

Use [HBC.md](HBC.md)
for HBC-specific write routing, search/rerank boundaries, vector ownership, and
bulk-build strategy. This section owns the DB-level split sequencing and
handoff requirements.

Recommendation:

- do not default to a full child rebuild from vectors
- classify existing HBC subtrees
- hand off fully right-hand subtrees unchanged
- rebuild only mixed subtrees
- use a bulk-build heuristic only inside mixed subtree rebuilds

Relevant storage already exists in:

- `hbc_nodes`
- `hbc_quant`
- `hbc_vecs`
- `hbc_meta`

Plan:

1. persist node/subtree routing metadata in
   [pkg/antfly/src/storage/hbc_adapter.zig](pkg/antfly/src/storage/hbc_adapter.zig)
   with `min_doc_key`, `max_doc_key`, and possibly `member_count`
2. classify nodes/subtrees as left-only, right-only, or mixed
3. hand off right-only subtrees by copying node records, quantized blobs, raw
   vectors, and attach metadata
4. rebuild only mixed subtrees for the child and parent
5. use bulk-build only for mixed subtree rebuilds when needed

Current status:

- node/subtree split-range metadata is implemented in `hbc_adapter.zig`
- `splitPlanningStats(...)`, `buildSplitReusePlan(...)`,
  `estimateSplitRebuildWork(...)`, and split-member collection are wired into
  [pkg/antfly/src/bench/hbc_bench.zig](pkg/antfly/src/bench/hbc_bench.zig)
- synthetic HBC workloads are a warning: `kmeans` produced almost entirely
  mixed subtrees, `hilbert` was only slightly better, and measured full-rebuild
  and mixed-rebuild costs were nearly identical because there were effectively
  no reusable right-only dense subtrees
- batched child rebuild dropped dense rebuild cost sharply even without subtree
  reuse
- a first doc-key-local leaf split heuristic slightly reduced mixed frontier
  for `kmeans`, but did not create reusable right-only subtrees; treat it as a
  secondary tuning lever rather than the main dense split strategy
- DB split destination rebuilds child dense indexes directly from HBC
  split-member plans and skips generic dense doc replay for handed-off child
  docs

Acceptance:

- child dense split mostly reuses existing HBC subtrees
- mixed rebuild cost scales with boundary-crossing structure, not full child
  size
- full child vector reinsertion is no longer the default split path

### Sparse And Graph Split

Sparse indexes should progress from forward-entry handoff to block/postings
handoff.

Sparse direction:

- treat sparse index data more like text than dense HBC
- add shardable postings/block metadata
- hand off fully right-only postings blocks
- rewrite only mixed blocks

Sparse current status:

- direct split handoff copies child-side `fwd` / `rev` coverage unchanged and
  rebuilds postings from source chunks while preserving doc-number mappings
- sparse posting chunks persist per-chunk key-range metadata
- sparse terms persist term-level key-range metadata
- fully right-only sparse chunks are copied raw into the child index
- only mixed chunks fall back to filtered rebuild
- split-time generic indexing can skip sparse docs already handed off
- `zig build sparse-test` covers Zig sparse unit behavior

Graph split should use edge ownership and direct reverse-index rebuild instead
of generic doc replay.

Graph current status:

- child graph split rebuilds reverse state directly from owned outgoing edge
  keys
- graph split no longer depends on generic doc replay for destination rebuild
- graph boundary coverage checks that reverse rebuild respects split ownership
  bounds

### Immediate DB Roadmap

1. keep backend-neutral DB behavior covered across LMDB, memory, and durable LSM
2. use `hbc_bench` split planning output on more realistic dense datasets
3. prototype dense subtree handoff for clearly right-only cases while assuming
   mixed rebuild remains important
4. replace sparse live chunk planning with persisted postings/block routing
   metadata across larger sparse datasets
5. add a smaller durable split prepare/equivalence target instead of relying on
   the heavyweight DB split bench for debugging
6. keep graph split on the direct ownership path and broaden correctness checks
