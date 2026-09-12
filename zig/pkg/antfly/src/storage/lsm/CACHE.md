# LSM Cache

The LSM backend has a shared, byte-budgeted cache: one cache per node/runtime,
shared by the local DBs, tables, and indexes opened on that node. It landed in
`lsm_backend/cache.zig` as the public type `Cache`.

The cache is not an HBC-only `vector_id -> doc_key` map. HBC result
materialization was the read profile that first motivated it, but the
underlying issue was broader: repeated LSM point reads were spending too much
time reopening or reparsing run/table structures and walking encoded table
entries. The shared cache improves HBC metadata lookups, exact rerank vector
loads, document materialization, status reads, recovery/open, and index
storage generally.

## Design Rationale

Before the shared `Cache` landed, the LSM backend had per-backend caches
(`run_state_cache`, `run_table_cache`) owned by each backend handle. They were
useful but structurally limited: keyed by path only (not byte-budgeted), not
shared across tables or DB handles, and not instrumented as a single node
resource. If there are hundreds of tables, each backend-local cache does not
compete with any other backend's cache for memory, so a node with many tables,
shards, or indexes has no way to let a hot table evict a cold one owned by a
different handle.

The shared `Cache` fixes that by being a node-scoped object threaded through
backend open options (`Options.cache`) and used by the LSM runtime, so cold
blocks from idle DBs can be evicted in favor of hot blocks from active DBs
instead of every DB getting an isolated memory budget.

## Query Isolation And Resource Budgets

Beyond the cache itself, a node needs explicit budget slices so one workload
cannot starve another: a dense-only query must not be blocked by a full-text
segment merge failure, and a large weak-sync ingest must not push unbounded
index/cache/merge debt into the first read.

`storage/resource_manager.zig` implements this as five budget slices, each
with byte accounting, metrics, and pressure-based backpressure:

- `lsm.block_table_cache`: shared LSM raw block, index/filter, run-state, and
  table-window entries. Reports pressure and actively shrinks the shared cache
  target under `shrink_cache` pressure.
- `hbc.node_metadata_cache`: HBC node bodies, split metadata, quantized
  vectors, raw vectors, and metadata lookup state. Reports bytes/pressure when
  HBC attaches a resource manager, and shrinks per-index HBC caches under
  pressure.
- `full_text.pending_segments`: pending immutable segment bytes per node and
  per full-text index. Defers scheduled background merges under pressure.
- `derived.backlog`: pending derived WAL bytes, pending batch count, oldest
  unapplied LSN age, and queue memory. Throttles producers by running derived
  work inline under pressure.
- `text_merge.buffers`: reserved memory for active full-text merge
  readers/writers. Defers background merges when the slice is pressured.

Provisioned storage derives all five slices' hard limits from the same
detected memory limit (Linux cgroup limit first, host memory as fallback),
with soft limits at 75% of each hard limit, so LSM cache, HBC cache, full-text
pending segments, derived backlog, and text merge buffers scale together
instead of mixing one adaptive cache with static side budgets.

Query planning declares the index families it needs before read preparation
so a query only waits on what it actually touches:

- Dense-only query: requires primary document visibility plus dense/HBC index
  visibility, and must not wait for full-text merge workers.
- Full-text query: requires a published full-text snapshot, but can use the
  last good snapshot while background merge is degraded.
- Hybrid query: waits only for the families present in the query plan, not all
  derived maintenance.
- Lookup/scan: should not wait on derived indexes unless stored projection or
  generated fields require them.

Full-text segment merge failures are isolated rather than allowed to poison
other query paths: segment metadata (codec version, file bounds, chunk
offsets, byte lengths, optional checksum) is validated before merge. On
`InvalidChunk`, the merge scheduler quarantines the specific failed
source/segment set by index name and source segment IDs, records the last
merge error, skips quarantined source segments during planning, and leaves the
last published full-text snapshot readable. Index status surfaces the degraded
state: last merge error, failed segment count, pending bytes, and
retry/backoff state.

For large ingest, the long-term direction is bulk publication rather than
closing DB handles more often: build HBC nodes/metadata in batches, write
sorted LSM tables directly where possible, publish immutable generations
atomically after large batches, and run full-text merges/compactions under
explicit budget after publication instead of as unbounded read-path debt. See
[WRITES.md](WRITES.md) for the write-path side of this.

## How The Cache Works

`Options.cache` is threaded through DB opens, provisioned read/write DB
caches, status fallback opens, and text/dense/graph LSM index opens.
`DataServer` owns one shared cache and sizes it from the process memory
budget (Linux cgroup memory limit first, host memory as fallback, clamped to a
practical node cache range) and mirrors that into the resource-manager
`lsm.block_table_cache` hard limit.

The cache does not treat a run table as one opaque cached value. Shared
entries are split by kind:

- `run_table_raw`: the raw table bytes
- `run_table_index`: decoded entry offsets plus the bloom filter bytes
- `run_table_block`: decoded entry-data windows for iterator/block-window paths
- `run_table_physical_block`: compressed physical block payloads used by
  prefix-compressed point reads that direct-search restart windows
- `run_state`: decoded state for state-based callers

Backend-local `CachedRunTable` values pin raw and index handles together and
expose a lightweight borrowed table view, keeping the ownership model simple
while letting index/filter metadata stay hot independently of whole-table
decode churn.

HBC uses a sorted batch point-read path (`getManySorted`) for result
materialization: it is threaded through the vectorindex and storage erased
transaction adapters, the LSM read transaction exposes it, and HBC uses it for
exact rerank vector prefetch plus final metadata population. It reuses run
hints/cursors and table cache handles across adjacent keys, capturing most of
the locality of a block-cache-backed reader/scanner without an eager full
in-memory map.

LSM point reads with a shared cache use a real block-cache path:
`repository.loadRunTableIndexAllocWithStorage` loads just the table header,
offsets, and bloom bytes without reading the full table, and
`runtime.getFromRunIndices` uses that cached index plus cached entry-data
windows to binary-search and materialize exact point hits without loading
whole run files. Cursor/scan paths still fall back to whole-table views.

## Cache Identity, Keys, And Values

Cache identity does not rely on raw path strings alone, because paths can be
reused across compaction/table replacement. The durable identity source is the
persisted monotonic `run.id`: run file names are derived from `run.id`,
`next_run_id` is stored in the manifest, and reopen continues from that stored
counter. The cache therefore keys on `path + run.id` as the durable generation
boundary. If file naming ever becomes reusable independent of `run.id`, a
generation field would need to be added before broadening cache reuse further.

Cache values are decoded/borrowed run table data, decoded run state, and
bloom/filter and entry-offset metadata, plus block-granular values: raw table
data blocks, entry-offset/index metadata, and filter bytes via the decoded
table index. Block-granular caching is limited to point reads; it is the
closer analogue of a classic LSM block cache and is safer for large tables
because a single hot lookup does not pin an entire run table forever. Cursor
and scan paths still use whole-table fallback until they grow a block-aware
iterator path.

## Ownership And Concurrency

The cache uses a byte-budgeted eviction policy, not a per-table item count:

- Every entry reports an approximate byte cost.
- The cache evicts globally across tables and indexes, so hundreds of tables
  compete inside the same node budget instead of each getting its own
  allotment.
- Borrowed values are pinned while a caller holds them; pinned values are
  skipped during eviction and freed when the last holder releases them.
- Obsolete run cleanup invalidates all entries for that run before deleting
  the file.
- Backend close releases its references but does not close the shared cache
  itself; node/runtime close owns final cache teardown.

The cache map and eviction lists use sharded locks rather than one global
mutex, since a single mutex risks becoming the hot read-path bottleneck once
LSM point reads hit the cache heavily. The access pattern is:

```zig
const handle = try cache.getOrLoad(key, loader);
defer handle.release();
const table = handle.value(BorrowedDecoded);
```

Loaders run outside the shard lock (or use a singleflight-style placeholder)
so multiple query threads do not all parse the same table on a miss.

## Instrumentation

The cache and read path expose: LSM point gets; runs probed per get;
bloom/filter rejects; cache hits/misses by kind; bytes cached and bytes
evicted; entry parse count and time; table load count and time; and
`getManySorted` batch size and hit rate. This instrumentation exists so query
time spent in `getFromSnapshotRuns`, `getFromRunIndices`, `compareEntryTo`,
and `parseEntryAt` is visible directly rather than inferred from external
sampling.

## Test Coverage

The cache has unit tests for budget enforcement and eviction order, for
pinned entries surviving eviction and being freed after release, and for
obsolete-run invalidation removing all cache entries for a run. Backend-level
tests cover shared cache entries being reused across two backend handles for
the same run, a compacted/replaced run not returning stale cached data, and
`getManySorted` equivalence against repeated `get`.

## Open work

- Benchmark before/after cache slices in VectorDBBench 50K and 1M and keep
  iterating on the hottest remaining read and write paths.
- Decide which hard-pressure cases should become client-visible retryable
  overload responses. Current write pressure drains derived backlog inline;
  full-text merge pressure defers background work and preserves the last
  readable snapshot.
- HBC cache ownership is still per-index rather than one shared node-owned
  cache. HBC currently does budget-first admission and shrinks per-index
  node/quantized/raw-vector/metadata caches when `hbc.node_metadata_cache`
  reports pressure, and exports those counters under `hbc_cache` in dense
  index runtime status. The longer-term shape is still a shared node-level HBC
  cache (or admission layer) so many indexes compete inside one budget instead
  of each index independently reacting after pressure is observed:
  - move HBC cache ownership from each index into one node-owned shared cache,
    keyed by table/index identity plus entry kind/id, enforcing one global
    byte budget before admission
  - add per-index fairness on top of the shared cache: derive a soft share
    from active index count, let hot indexes burst when global memory is
    free, and evict first from indexes over their share during pressure
  - add live RSS feedback: a low-frequency process RSS sampler that
    temporarily lowers cache soft targets when the whole process approaches
    its memory target, then restores them gradually
  - add cache admission policy for bulk ingest so raw vectors/metadata loaded
    only to complete an insert do not automatically occupy cache budget ahead
    of nodes and quantized search structures
  - expand metrics and tests: HBC bytes/evictions/admission-skips/largest
    owners by kind/index, and a many-index regression asserting aggregate HBC
    cache bytes stay under the shared budget while search still meets recall
    and latency targets
- Should embedded/test modes use the same adaptive default as `DataServer`?
  The low-level cache default is 256 MB; `DataServer` uses adaptive sizing.
  Embedded callers can still pass an explicit `Cache` or smaller cache size,
  but there is no stated policy for whether they should.
- Should status/reporting reads share the same cache as query reads? The
  working assumption is yes by default, but metrics should expose whether
  background status work is evicting hot query blocks; that visibility is not
  confirmed to exist yet.
