# LSM Cache Plan

## Summary

Zig should add a shared, byte-budgeted LSM cache with the same ownership shape as the Go implementation's Pebble cache: one cache per node/runtime, shared by the local DBs, tables, and indexes opened on that node.

This should not be an HBC-only `vector_id -> doc_key` map. The current read profile shows HBC result materialization is a strong first consumer, but the underlying issue is broader: repeated LSM point reads spend too much time reopening or reparsing run/table structures and walking encoded table entries. A shared LSM cache should improve HBC metadata lookups, exact rerank vector loads, document materialization, status reads, recovery/open, and future index storage.

## Query Isolation And Resource Budgets

The 1M VectorDBBench run exposed a second class of cache and resource-management problems: dense-vector reads, full-text maintenance, HBC insertion, LSM compaction, and derived-index catch-up currently share too many blocking boundaries. A dense-only query must not be blocked by a full-text segment merge failure, and a large weak-sync ingest must not push unbounded index/cache/merge debt into the first read.

The long-term shape should be a node-level resource manager with explicit budget slices:

- `lsm.block_table_cache`: shared LSM raw block, index/filter, run-state, and table-window entries.
- `hbc.node_metadata_cache`: HBC node bodies, split metadata, quantized vectors, raw vectors, and metadata lookup state.
- `full_text.pending_segments`: pending immutable segment bytes per node and per full-text index.
- `derived.backlog`: pending derived WAL bytes, pending batch count, oldest unapplied LSN age, and queue memory.
- `text_merge.buffers`: reserved memory for active full-text merge readers/writers.

Each slice should have byte accounting, metrics, and backpressure. Soft limits should bias eviction or scheduling; hard limits should either run work inline, delay background work, or return a retryable overload error depending on the caller and sync level.

Query planning should declare the index families it needs before read preparation:

- Dense-only query: requires primary document visibility plus dense/HBC index visibility, and must not wait for full-text merge workers.
- Full-text query: requires a published full-text snapshot, but can use the last good snapshot while background merge is degraded.
- Hybrid query: waits only for the families present in the query plan, not all derived maintenance.
- Lookup/scan: should not wait on derived indexes unless stored projection or generated fields require them.

This implies two separate mechanisms:

- `prepareForRead(kind)`: cheap read-path cache coordination scoped to the query family.
- `waitForIndexFamilies(families, sequence)`: explicit freshness gating used only when the requested consistency or sync level requires it.

Full-text segment merge failures should be isolated:

- Validate segment metadata before merge: codec version, file bounds, chunk offsets, byte lengths, and optional checksum.
- On `InvalidChunk`, mark the specific source/segment set as failed or quarantined and back off retries. The first implementation now quarantines failed scheduled merge sources by index name and source segment IDs, records the last merge error, skips quarantined source segments during planning, and leaves the last published snapshot readable.
- Keep the last published full-text snapshot readable.
- Surface degraded status through index status: last merge error, failed segment count, pending bytes, and retry/backoff state.
- Never poison dense-only query execution.

For large ingest, the correct longer-term answer is not to close DB handles more often. The ingest path should shift from online incremental rewrites to bulk publication:

- Build HBC nodes and metadata in batches, coalescing metadata writes and leaf split updates.
- Write sorted LSM tables directly for bulk index data where possible.
- Publish immutable generations atomically after large batches.
- Run full-text segment merges and LSM compactions under explicit budget after publication, not as unbounded read-path debt.

## Current State

The first implementation slice landed in `lsm_backend/cache.zig` and uses the public type name `Cache`. It provides a node-owned, byte-budgeted cache with refcounted handles, path invalidation, and per-kind hit/miss/insert/eviction/wait counters. `Options.cache` is threaded through DB opens, provisioned read/write DB caches, status fallback opens, and text/dense/graph LSM index opens. `DataServer` owns one shared cache and sizes it from the process memory budget: Linux cgroup memory limit first, host memory as fallback, clamped to a practical node cache range and mirrored into the resource-manager `lsm.block_table_cache` hard limit. Provisioned storage now derives all resource-manager slice budgets from the same detected memory limit, so LSM cache, HBC cache, full-text pending segments, derived backlog, and text merge buffers scale together instead of mixing one adaptive cache with static side budgets.

The current cache no longer treats a run table as one opaque cached value. Shared entries are split into:

- `run_table_raw`: the raw table bytes
- `run_table_index`: decoded entry offsets plus the bloom filter bytes
- `run_table_block`: decoded entry-data windows for iterator/block-window paths
- `run_table_physical_block`: compressed physical block payloads used by
  prefix-compressed point reads that direct-search restart windows
- `run_state`: decoded state for state-based callers

Backend-local `CachedRunTable` values now pin raw and index handles together and expose a lightweight borrowed table view. This keeps the ownership model simple while letting index/filter metadata stay hot independently of whole-table decode churn.

HBC now has a sorted batch point-read path for result materialization: `getManySorted` is threaded through the vectorindex and storage erased transaction adapters, the LSM read transaction exposes it, and HBC uses it for exact rerank vector prefetch plus final metadata population.

LSM point reads with a shared cache now use a first real block-cache path. `repository.loadRunTableIndexAllocWithStorage` can load just the table header, offsets, and bloom bytes without reading the full table. `runtime.getFromRunIndices` uses that cached index plus cached entry-data windows to binary-search and materialize exact point hits without loading whole run files. Cursor/scan paths still fall back to whole-table views.

## Why This Matches Go

Go creates one `pebbleutils.Cache` in each node process entry point and passes it into the store and metadata runtimes. `pebbleutils.Cache` wraps a Pebble block cache, and `Apply` installs that cache into each Pebble DB's options. That lets Pebble globally evict cold blocks from idle DBs in favor of hot blocks from active DBs, instead of giving every DB an isolated memory budget.

The Zig LSM backend currently has per-backend caches:

- `run_state_cache`
- `run_table_cache`

Those caches are useful, but they are not equivalent to Go's shared Pebble cache. They are owned by each backend handle, keyed by path, not byte-budgeted, not shared across tables or DB handles, and not instrumented as a single node resource.

The right Zig analogue is a node-scoped shared cache object that is passed through backend open options and used by the LSM runtime. Exact placement can be split:

- `storage/lsm/cache.zig` for table/cache data structures and keys.
- `storage/lsm_backend` integration for ownership, open options, invalidation, and metrics.

## Goals

- Share one cache budget across all LSM users in a node.
- Keep memory bounded when there are many tables, shards, indexes, or DB handles.
- Improve LSM point reads and sorted repeated reads without adding an HBC-specific full in-memory map.
- Preserve snapshot safety while compaction and obsolete-run cleanup are active.
- Make cache behavior observable enough to tune from profiles.
- Support both query and write paths.

## Non-Goals

- Do not eagerly load all HBC metadata or all document keys into memory by default.
- Do not make the cache persistent across process restarts.
- Do not couple cache semantics to HBC result IDs.
- Do not hide correctness issues behind cache hits. Every cached object must be keyed so stale entries are impossible or explicitly invalidated.

## Proposed Ownership Model

Add a shared cache object:

```zig
pub const Cache = struct {
    allocator: Allocator,
    shards: []Shard,
    max_bytes: usize,
    used_bytes: std.atomic.Value(usize),
    stats: Stats,
};
```

Then thread `?*Cache` into LSM backend options:

```zig
pub const Options = struct {
    // existing fields...
    cache: ?*lsm_cache.Cache = null,
};
```

The API/server runtime should allocate one shared cache per local process/node and pass it into every provisioned DB handle, cached query DB handle, cached write DB handle, and metadata/status DB handle that uses the Zig LSM backend.

If `Options.cache == null`, the backend should keep working with a small private fallback or no shared cache. Tests and host embedders should not be forced to allocate a node-level runtime object.

## Cache Keys

Do not use only raw path strings as long-term cache identity. Paths can work for a first step, but the stable target should include run identity so compaction and table replacement cannot alias stale entries.

Initial key shape:

```zig
pub const Key = union(enum) {
    run_table: RunObjectKey,
    run_state: RunObjectKey,
    table_index: TableBlockKey,
    table_block: TableBlockKey,
};

pub const RunObjectKey = struct {
    path_hash: u64,
    run_id: u64,
    generation: u64,
};

pub const TableBlockKey = struct {
    path_hash: u64,
    run_id: u64,
    generation: u64,
    offset: u64,
    kind: BlockKind,
};
```

`run_id` and `generation` should come from manifest/runtime metadata when possible. Today the durable identity source is the persisted monotonic `run.id`: run file names are derived from `run.id`, `next_run_id` is stored in the manifest, and reopen continues from that stored counter. That makes path reuse impossible in the current format, so the cache now uses `path + run.id` as the durable generation boundary. If file naming ever becomes reusable independent of `run.id`, add an explicit generation field before broadening cache reuse.

## Cache Values

Start with values that directly match the current hot path:

- Decoded or borrowed run table data equivalent to `lsm_table_file.BorrowedDecoded`.
- Decoded run state for callers that need `State`.
- Bloom/filter and entry-offset metadata for table files.

Landed block-granular values:

- Raw table data blocks.
- Entry-offset/index metadata.
- Filter bytes via the decoded table index.

This first block-granular slice is limited to point reads. It is the closer Pebble analogue and is safer for large tables because a single hot lookup does not pin an entire run table forever. Cursor and scan paths still use whole-table fallback until they grow a block-aware iterator path.

## Eviction

Use a byte-budgeted policy, not a per-table item count. LRU, segmented LRU, or clock are all acceptable; the first implementation should prefer simple and observable over clever.

Required behavior:

- Every entry reports an approximate byte cost.
- The cache evicts globally across tables and indexes.
- Borrowed values are pinned while a caller holds them.
- Pinned values are skipped during eviction and freed when the last holder releases them.
- Obsolete run cleanup invalidates all entries for that run before deleting the file.
- Backend close releases its references but does not close the shared cache itself.
- Node/runtime close owns final cache teardown.

This is the key difference from per-table caches: if there are hundreds of tables, each table does not get 256 MB. They all compete inside the same node budget.

## Concurrency

Use sharded locks for the cache map and eviction lists. A single global mutex risks becoming the next hot read-path bottleneck once LSM point reads start hitting the cache.

The public access pattern should look like:

```zig
const handle = try cache.getOrLoad(key, loader);
defer handle.release();
const table = handle.value(BorrowedDecoded);
```

Loaders must run outside the shard lock or use a singleflight-style placeholder so multiple query threads do not all parse the same table on a miss.

## Read API Work

The cache is only one part of the long-term fix. HBC metadata materialization currently performs many point reads for sorted vector IDs. Add a batched sorted point-read API to the LSM runtime:

```zig
pub fn getManySorted(
    txn: *ReadTxn,
    namespace: Namespace,
    keys: []const []const u8,
    out: []?[]const u8,
) !void
```

This should reuse run hints/cursors and table cache handles across adjacent keys. HBC can call this from metadata population and exact rerank vector loading after it sorts candidate IDs. Other callers can continue using `get`.

This avoids an eager full map while capturing most of the locality that Go gets from Pebble's block cache and reader cache.

## Instrumentation

Add counters before and during implementation:

- LSM point gets.
- Runs probed per get.
- Bloom/filter rejects.
- Cache hits/misses by kind.
- Bytes cached and bytes evicted.
- Entry parse count and time.
- Table load count and time.
- `getManySorted` batch size and hit rate.

The benchmark profile that motivated this work showed query time under HBC result population flowing through LSM `getFromSnapshotRuns`, `getFromRunIndices`, `compareEntryTo`, and `parseEntryAt`. The instrumentation should make that visible without relying only on external sampling.

## Phasing

Completed:

1. Introduce `Cache` with a byte budget, pin/release handles, invalidation, and tests for eviction.
2. Move `run_state_cache` and `run_table_cache` behind `Cache` while keeping current behavior.
3. Thread one node-owned cache from `DataServer` through provisioned read/write DB caches and LSM-backed index opens.
4. Require stable cache identity for run table/state entries using durable `path + run.id`.
5. Add `getManySorted` and use it in HBC metadata population and exact rerank vector loading.
6. Add backend read stats for point gets, sorted batch calls, sorted batch keys, mutable hits, L0 hits, level hits, run probes, and bloom negatives.
7. Split shared run-table cache values into raw-bytes and index/filter entries instead of a single cached decoded table object.
8. Shard cache locking and coalesce duplicate misses with a pending-load gate so concurrent readers do not all parse the same run on a miss.
9. Add a first real block-cache path for shared-cache point reads: load table index/filter metadata separately, cache entry-data windows by offset/length, and materialize exact point hits from cached blocks.
10. Add finer-grained LSM read instrumentation for sorted-batch hit/miss/locality, read-hint usefulness, table entry parse counts/time, index load/decode time, block load bytes/time, and shared/local/cursor block behavior.
11. Route cursor/scan block-window reads through the shared LSM block cache when a node cache is configured, while retaining per-cursor owned block bytes for stable iteration.
12. Add resource pressure classification and policy actions to `resource_manager.ResourceManager` snapshots. Each slice now reports normal/soft/hard pressure plus its configured soft/hard action.
13. Enforce the first pressure action: when `lsm.block_table_cache` reports `shrink_cache`, the shared LSM cache trims to the resource slice soft limit instead of only its configured cache size.
14. Audit remaining whole-table LSM consumers. The old `getCachedRunTable()` decoded whole-table helper is not used on active read paths; current hot point/cursor paths route through table indexes and block windows. Remaining `readFileAlloc` users in the LSM backend are manifest/legacy-table compatibility, copy/recovery utilities, tests, or the inactive helper.
15. Attach a node-level `ResourceManager` to provisioned storage and pass it through cached read/write DB handles so LSM, HBC, full-text, derived backlog, and merge-buffer accounting share one node policy surface.
16. Enforce additional pressure actions: derived backlog pressure runs derived maintenance inline until the current sequence is applied, and full-text merge pressure defers non-urgent scheduled merges instead of competing with dense query/write paths.
17. Export node resource and shared LSM cache metrics from the data health `/metrics` endpoint, and expose full-text merge quarantine/defer state through index runtime status.
18. Add Prometheus label support to the health metrics writer and export resource/cache dimensions as labeled metric families instead of encoding slice/kind in metric names.

Remaining:

1. Benchmark before/after slices in VectorDBBench 50K and 1M, and keep iterating on the hottest remaining read and write paths.
2. Decide which hard-pressure cases should become client-visible retryable overload responses. Current write pressure drains derived backlog inline; full-text merge pressure defers background work and preserves the last readable snapshot.
3. Continue hardening HBC cache enforcement under concurrent search/write workloads. HBC now shrinks per-index node, quantized, raw-vector, and metadata caches when `hbc.node_metadata_cache` reports `shrink_cache` pressure, but the longer-term shape should still be a shared node-level HBC cache or admission layer so many indexes compete inside one budget instead of each index independently reacting after pressure is observed.

HBC cache roadmap:

1. Continue budget-first admission in the current per-index HBC caches. Before retaining a cache payload, refresh the resource-manager accounting, evict local HBC entries if needed, and reserve the expected byte delta so normal cases do not allocate first and shrink later. Keep post-insert enforcement as a correctness fallback because several read paths require `cacheVector` to return stable memory.
2. Export HBC per-kind cache stats through Prometheus metrics. The adapter now tracks node, quantized, raw-vector, and metadata bytes/counters internally, and dense index runtime status includes those counters under `hbc_cache`.
3. Move HBC cache ownership from each index into one node-owned shared cache. Cache keys should include table/index identity plus entry kind/id, and the shared cache should enforce one global byte budget before admission.
4. Add per-index fairness on top of the shared cache: derive a soft share from active index count, let hot indexes burst when global memory is free, and evict first from indexes over their share during pressure.
5. Add live RSS feedback. The initial hard limits are derived from cgroup/host memory, but a low-frequency process RSS sampler should temporarily lower cache soft targets when the whole process approaches its memory target, then restore them gradually.
6. Add cache admission policy for bulk ingest. Raw vectors and metadata loaded only to complete an insert should not automatically occupy cache budget; nodes and quantized search structures should be preferred over one-off raw payloads.
7. Expand metrics and tests: expose HBC bytes, evictions, admission skips, and largest owners by kind/index; add a many-index regression that asserts aggregate HBC cache bytes stay under the shared budget while search still meets recall and latency targets.

Resource-budget progress:

- `lsm.block_table_cache`: reports pressure and actively shrinks the shared cache target.
- `hbc.node_metadata_cache`: reports bytes and pressure when HBC attaches a resource manager.
- `full_text.pending_segments`: reports bytes and defers scheduled background merges under pressure.
- `derived.backlog`: reports pending WAL bytes and throttles producers by running derived work inline under pressure.
- `text_merge.buffers`: reserves active merge working memory and defers background merges when the slice is pressured.

- Landed `storage/resource_manager.zig` with slices for `lsm.block_table_cache`, `hbc.node_metadata_cache`, `full_text.pending_segments`, `derived.backlog`, and `text_merge.buffers`.
- The resource manager is intentionally internal and typed first. Prometheus export should be an adapter over snapshots, not the primary metrics representation.
- LSM shared cache usage is observed under `lsm.block_table_cache`.
- HBC node, quantized, vector, and metadata cache usage is observed under `hbc.node_metadata_cache`.
- HBC cache accounting now actively shrinks per-index HBC caches when the `hbc.node_metadata_cache` slice is under `shrink_cache` pressure. This bounds retained HBC cache payloads better than the old per-index item-count caps, though the cache is still physically owned by each index rather than one shared HBC cache object.
- HBC cache inserts now attempt budget-first admission against the resource manager: they refresh local accounting, evict local entries if needed, reserve the expected byte delta, and roll that reservation into post-insert accounting. Post-insert shrink remains as the safety net.
- HBC now tracks per-kind cache bytes, peaks, insertions, admission skips, and evictions for node bodies, quantized sets, raw vectors, and metadata. Dense index runtime status includes the same counters under `hbc_cache`.
- Provisioned data storage derives all slice hard limits from the detected memory limit, using Linux cgroup limits before host memory. Soft limits are set at 75% of each hard limit.
- Full-text pending segment bytes are tracked from live merge stats and released when the index manager closes.
- Derived executor backlog payload bytes are observed under `derived.backlog`.
- Text merge tasks reserve `text_merge.buffers` budget before copying source segment bytes and hold that reservation until the task is destroyed.
- Pressure-policy metadata is now part of typed snapshots. Default slice actions are: shrink LSM cache on LSM cache pressure, shrink HBC caches on HBC cache pressure, defer full-text pending and merge-buffer work, throttle derived backlog pressure, and reject merge-buffer work at the hard limit.
- The LSM cache now enforces `shrink_cache` by trimming to the slice soft limit while pressure is active.
- Resource and LSM cache metrics now use Prometheus labels for slice/kind dimensions.
- Next wiring targets are benchmark-driven tuning and deciding which hard-pressure states should surface as retryable overload responses.

## Tests

- Unit test cache budget enforcement and eviction order.
- Unit test pinned entries survive eviction and are freed after release.
- Unit test obsolete-run invalidation removes all cache entries for a run.
- Backend test that shared cache entries are reused across two backend handles for the same run.
- Backend test that a compacted/replaced run cannot return stale cached data.
- `getManySorted` equivalence test against repeated `get`.
- VectorDBBench 50K before/after profile comparison for read p95/p99 and load time.

## Open Questions

- Should embedded/test modes use the same smart default? The low-level cache default is 256 MB, while `DataServer` uses adaptive sizing for the node-owned shared cache. Embedded callers can still pass an explicit `Cache` or smaller cache size.
- Does the current table-file format need a block index before block-level caching is worth doing? If yes, the first cache milestone should be whole-run/table caching with a clear migration path.
- Where should run generations live? If the manifest lacks a durable generation that survives reopen, add one before allowing broad cross-handle cache reuse.
- Should status/reporting reads use the same cache? Yes by default, but metrics should expose whether background status work is evicting hot query blocks.
