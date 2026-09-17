# LSM, HBC, and Full-Text Write Performance

This document mixes shipped write-path work with items that are explicitly
still open. The write benchmarks themselves, the WAL/immutable-memtable path,
the compaction scheduler, per-store byte-based flush thresholds, and adaptive
table-block compression are implemented and covered below in the sections that
describe them (most are marked `Status: implemented` or narrated as landed
with benchmark evidence). What remains open — per-store LSM config splits
beyond HBC, full-text writer byte budgets, a true final-state HBC bulk
publication path, DB status/Prometheus export of maintenance/debt stats, and
further memtable structure changes — is called out inline where each section
discusses it, generally as explicit task-list items or "Remaining"/"Next"
notes. Do not treat an absence of dated follow-up evidence for a task-list
item as confirmation that it shipped.

## Why add write benchmarks

Yes. We should add benchmarks for on-disk full-text segment creation, merge, and compaction, plus disk LSM write behavior with explicit timings and write-amplification metrics.

The current hot path is not just "append documents". A large load writes the primary documents, vector-id mappings, dense HBC index state, full-text segments, segment merge outputs, manifests, and LSM runs. If we only time end-to-end ingest, we cannot tell whether a change helped HBC leaf churn, LSM run churn, full-text merge pressure, or manifest/fsync frequency.

The benchmark goal is to make these costs visible before changing the write path:

- How many bytes did the store write for N logical bytes?
- How many table files, manifests, renames, and deletes happened?
- How many flushes and compactions happened, and how large were their inputs and outputs?
- How many HBC node, metadata, vector, quantized payload, and leaf range writes happened?
- How many full-text pending segments were created, merged, skipped, or failed?
- Did `bulk_ingest` behave like a real bulk build, or just like a larger online batch?

## Benchmark Targets

### Disk LSM write bench

Add `bench/storage/lsm_write_bench.zig` and a `zig build lsm-write-bench && ./zig-out/bin/lsm_write_bench --samples 3 --keys 20000 --hot-keys 1000 --overwrite-rounds 20 --value-size 128 --batch-size 1000 --storage host --mode both` step.

Workloads:

- Sorted initial load using append puts.
- Random initial load using normal puts.
- Batched overwrites against existing keys.
- Tombstone/delete batches.
- Normal write mode vs `.bulk_ingest`.
- Multiple flush thresholds and compaction policies.

Metrics:

- Total elapsed ns, ns/op, ops/sec.
- Storage writes, bytes written, renames, file deletes, tree deletes.
- LSM run count after each workload.
- L0 run count and max level after each workload.
- Total run bytes after each workload.
- Mutable entry count after each workload.
- Compactions, input runs, input bytes, output bytes.
- First-class backend counters for flushes, flush input/output bytes, table file writes/bytes, manifest writes/bytes, and timing.

Useful commands:

```sh
zig build lsm-write-bench && ./zig-out/bin/lsm_write_bench --samples 3 --keys 100000 --batch-size 5000 --storage native --mode both
zig build lsm-write-bench && ./zig-out/bin/lsm_write_bench --samples 3 --keys 100000 --batch-size 5000 --storage host --mode both
zig build lsm-write-bench && ./zig-out/bin/lsm_write_bench --samples 3 --keys 100000 --batch-size 5000 --storage native --mode bulk_ingest --flush-threshold 1024
zig build lsm-write-bench-compare && ./zig-out/bin/lsm_write_bench_compare --before /tmp/lsm-write-before.jsonl --after /tmp/lsm-write-after.jsonl
zig build text-segment-write-bench && ./zig-out/bin/text_segment_write_bench --samples 3 --docs 100000 --batch-size 5000 --merge-width 8 --storage native
zig build antfly-storage-bench && ./zig-out/bin/storage_bench hbc-write --samples 3 --vectors 100000 --dims 128 --batch-size 5000 --storage host
```

Use `--storage native` for filesystem timings and `--storage host` for lower-noise persisted table/manifest write counts through the host storage abstraction.

There is also a Pebble comparison helper at `compat/pebble_overwrite_go`. Run it with the same `--keys`, `--hot-keys`, `--overwrite-rounds`, `--value-size`, and `--batch-size` flags as `lsm-write-bench` and it reports disk totals split into SST/WAL/manifest/other bytes after load, after hot overwrites, and after a full compaction; it emits JSONL using the same high-level workload names (`load_base`, `overwrite_hotset`, `maintenance_hotset`) plus Pebble-specific L0/read-amp/WAL/obsolete-table metrics, for direct write-amplification comparison against Pebble. Use `--value-pattern keyed` for HBC-like payloads; the default repeated-byte pattern is too compressible for write-amplification comparison.

### HBC dense write bench

Add an HBC-specific write/profile benchmark after the LSM counters are in place.

Workloads:

- Empty index bulk build.
- Empty generated dense index path.
- Online insert batches of 1k, 5k, and 10k vectors.
- Updates to existing `doc_key` identities.
- Chunked writes where multiple chunk keys share a parent identity.
- Parent delete/overwrite replay.

Metrics:

- Total dense indexing ns and ns/vector.
- HBC insert time separated from vector-id mapping time.
- Node puts, metadata puts, vector puts, quantized payload puts.
- Leaf split count and split metadata bytes.
- Metadata/range rewrite count.
- Quantized payload rebuild count and bytes.
- LSM runs/bytes/compactions attributable to HBC namespaces.

The expected improvement path is:

- Thread `.bulk_ingest` from DB replay into the HBC backend batch instead of only the docstore/vector-id mapping batch.
- Use `bulkBuildWithMetadata` for empty generated dense indexes, not only explicit empty indexes.
- Add a true HBC mutation batch that transforms/routes vectors, groups by leaf, writes each touched leaf once, and defers split metadata publication until the batch end.
- Add per-namespace LSM policies for HBC metadata, nodes, vectors, and quantized payloads.

### Full-text segment and merge bench

Add a full-text on-disk benchmark that can run without the DB catalog path.

Workloads:

- Segment creation from document batches.
- Pending segment accumulation without merge.
- Scheduled merge.
- Forced merge.
- Query after merge.
- Invalid/corrupt segment handling, to verify merge failures do not poison unrelated vector queries.

Metrics:

- Segment build ns and bytes.
- Merge build ns and apply ns.
- Pending segment count and bytes.
- Merged segment count and bytes.
- Skipped and failed merge counts.
- `TextMergeStats` and `PersistentIndexStats` fields.
- LSM table bytes and manifest writes caused by full-text metadata.

Useful command shape:

```sh
zig build text-segment-write-bench && ./zig-out/bin/text_segment_write_bench --samples 3 --docs 100000 --batch-size 5000 --merge-width 8 --storage native
```

## Longer-Term Shape

The durable write path should look closer to Lucene/Tantivy/Pebble:

- Writers build immutable generations.
- Readers pin a generation and never force writer shutdown.
- Full-text writers flush bounded immutable segments, then merge in the background under a byte budget.
- The LSM has explicit cache and write budgets instead of implicit unbounded growth.
- HBC supports both online mutation and offline bulk construction.
- Large loads publish sorted table files and HBC nodes once, instead of repeatedly growing and compacting online state.

Global mutable-state memory is governed by `ResourceManager`, which admits an incoming mutable batch against a shared `lsm.in_memory_state` budget before WAL append (512 MiB soft / 768 MiB hard by default; adaptive provisioning can lower these, e.g. to a 576 MiB soft boundary under the same 768 MiB cap). The DB layer waits on the shared usage-change epoch before acquiring its apply lock, then repeats the projected admission check immediately before WAL append, so configured rejection happens before durability/apply. A backend drains local state at soft pressure but never blocks on aggregate state while its caller may hold a higher-level lock; hard pressure is a non-blocking `ResourceBudgetExceeded`. A writer behind an unlocked immutable build releases the backend mutex and waits on a completion condition instead, since that local flush can progress independently. See [LSM.md](LSM.md) for the immutable memtable lifecycle this admission contract sits on top of.

## Implementation Roadmap

Current status:

- `lsm-write-bench`, `lsm-write-bench-compare`, `text-segment-write-bench`, and `antfly-storage-bench hbc-write` exist and emit JSONL for the workloads and metrics above. Native 100k baselines are checked in under `bench/baselines/`.
- Derived dense replay threads `.bulk_ingest` into the HBC insert choice: empty indexes use the HBC bulk builder, and replay batches whose vector IDs were newly allocated skip per-vector existence probes.
- HBC leaf-write coalescing is implemented behind `BatchInsertOptions.coalesce_leaf_writes`. It reduces bytes but can be slower than the absent-id path, so production only enables it for bulk replay batches where vector IDs are known-new.
- Online DB writes are split from true bulk ingest: normal API/VectorDBBench writes use ordinary storage write mode, and provisioned API writes no longer consult auto-bulk policy or open automatic long-lived bulk sessions for normal upload. Flush, L0 pressure, and maintenance remain storage-owned. Detached LSM maintenance jobs drain a bounded batch of steps per wake, giving background flush/compaction room to catch up before hard write pressure reaches the HTTP caller.
- Obsolete run lifetime is generation-scoped instead of backend-reader-scoped: read snapshots retain the exact path-backed runs they borrow, and compaction drops each retired run's metadata, cache handles, and physical file as soon as that generation's last snapshot exits, even while readers of newer generations remain active.
- Primary document LSMs have a bounded foreground WAL soft-pressure fallback: after the soft byte/segment boundary, commit performs at most one checkpoint-producing immutable flush per enforcement interval; it does not wait for the hard boundary or drain all accumulated generations in one request, and it preserves manifest-first checkpoint durability and crash replay.
- Bulk-ingest sessions have two finish modes: the default compacts on session close, while `finishBulkIngestSessionWithOptions(.{ .compact = false })` flushes remaining mutable state, publishes the manifest once, and leaves compaction to a later maintenance window. Dense derived replay opens such a session around each index catch-up window and finishes with `.compact = false` plus the deferred-L0 guardrail.
- `Backend.ingestSortedTableEntries()` validates a sorted unique key stream, writes table files directly, publishes one manifest, and avoids mutable flush/compaction for known bulk data. Runtime `.bulk_ingest` commits direct-ingest the transaction state once a batch reaches the effective bulk threshold and no older mutable state would be shadowed; smaller bulk batches still accumulate in mutable state under the elevated threshold. See the LSM store profiles defaults below for which stores keep direct sorted ingest enabled.
- Compaction candidates are ranked by normalized `current / target` level pressure instead of absolute run/byte debt, so a proportionally overfull downstream level is promoted ahead of a numerically larger but less-overfull upstream one.
- HBC grouped mutation batching handles no-split leaf groups, writes changed leaf ranges during mutation, batch-refreshes unique ancestor range chains after split candidates settle, and defers bounded overflow leaf splits to a batch-end split-candidate phase that recursively requeues left/right leaves until they fit under `leaf_size`; very large routed groups still fall back to the online path.
- `HBCIndex.WriteProfile` and `antfly-storage-bench` expose grouped-path guardrail counters (grouped leaf groups/items, fallback items, split candidates, recursive splits, leaf range writes, ancestor range refreshes/nodes, grouped node body writes, vec-leaf mapping writes) and logical HBC namespace write counters for `nodes`, `meta`, `quant`, and `vecs` (put/append/delete calls, key bytes, value bytes), with range writes broken out separately since they live in the `nodes` namespace but rewrite differently.
- Raw vectors and metadata are pre-stored in sorted vector-id order with append puts for known-new coalesced batches, moving that work off the online-mutation put path.
- Deferred quantized rebuild tracks the HBC nodes whose bodies changed during the write transaction and refreshes only that touched-node set at finish, replacing full-tree `rebuildAllQuantized()` for adapters that provide the touched-node tracker.
- The true mutation-batch work is partly implemented, not a clean win yet. The successful pieces are sorted raw-vector/metadata pre-store, no-split leaf grouping, bounded batch-end split handling, ancestor range refresh coalescing, and touched-node quantized rebuild. A final-only vec-leaf mapping experiment and a deferred sorted leaf-range publication experiment were both rolled back: they preserved byte reductions but worsened timing/storage behavior. The grouped path splits directly from the in-memory mutated leaf and queues the resulting leaves only for recursive overflow checks, avoiding an oversized temporary leaf before splitting.
- `zig build lsm-backend-test` is a focused LSM backend unit-test bucket with no known failures or allocator leaks.
- The bench can run against host-backed in-memory persistence, native filesystem storage, or memory-only storage.
- `Backend.WriteStats` exposes flush, table-file, manifest, and compaction timing/byte counters via `snapshotWriteStats()`.

1. Add the LSM write benchmark and JSONL output.
2. Add first-class LSM write counters: flushes, flush bytes, manifest writes, manifest bytes, compaction timing, table file timing.
3. Add HBC write/profile counters around node, metadata, vector, quantized payload, and split writes.
4. Add a full-text segment/merge benchmark using existing persistent-index stats.
5. Add compare tooling for before/after JSONL runs, similar to the existing LSM backend compare step.
6. Implement the write-amplification fixes in this order:
   - Done: propagate `.bulk_ingest` into HBC runtime batches.
   - Done: use bulk build for empty generated dense indexes.
   - First production slice: coalesce HBC leaf writes for bulk replay batches with known-new vector IDs and defer quantized rebuilds to batch finish.
   - Still needed: replace the fallback-heavy grouped path with a true mutation batch that can handle splits/range writes as one batch operation.
   - Add true HBC mutation batches grouped by leaf.
   - First slice done: add sorted-run LSM ingestion for already sorted key/value streams.
   - First slice done: add explicit LSM bulk-ingest sessions so replay/API windows can defer manifests and compaction across many 5k batches.
   - First slice done: add a deferred-L0 guardrail for no-compaction bulk session finish.
   - Split LSM options by store/namespace write shape.
   - Add byte-based backpressure for derived queues, full-text pending segments, HBC caches, and LSM mutable/run pressure.

### Remaining Write-Amplification Plan

Implementation task list:

- LSM store profiles:
  - Add explicit LSM option profiles for primary docs, full-text main metadata, full-text WAL metadata, HBC dense storage, and graph reverse indexes.
  - Thread those profiles through DB/index open paths instead of relying on one implicit default.
  - Later split HBC further into node, metadata, quantized payload, raw vector, and vector-mapping stores once the backend can tune namespaces independently.
  - Production defaults: primary docs use a 32 MiB byte-window target, text main/WAL metadata and graph reverse indexes use 16 MiB, and dense HBC/vector index LSM uses 128 MiB with a 4x bulk-window multiplier (so a dense replay/bulk window can hold up to about 512 MiB of final unique mutable state before publication). Dense HBC also sets `compact_threshold_runs=8` (with higher soft/hard pressure limits than other stores) so 10-15 L0 `hbc_quant` versions become eligible for maintenance compaction without moving soft compaction back into foreground commits. Dense HBC keeps direct sorted ingest disabled for online mutation streams (see HBC writes below); generic stores keep it enabled by default via the `direct_bulk_ingest` LSM option.
- LSM write publication:
  - Keep bulk-ingest sessions for replay/catch-up windows.
  - Add policy-based online manifest batching for weak-sync/background writes.
  - Add dirty-run, dirty-byte, and max-delay guards before delayed manifest publication.
- LSM compaction:
  - Keep no-compaction bulk finish for write amplification.
  - Add maintenance compaction triggers by L0 run count, L0 bytes, and observed read amplification.
  - Default write policy: non-bulk writes still flush mutable state, persist the manifest, and enforce hard L0 pressure in the foreground, but soft L0/level cleanup is maintenance-only work unless `foreground_soft_compaction=true` is set for a test or specialized store.
- HBC writes:
  - Avoid root-style quantized payload writes for internal bulk-built nodes that will be reparented.
  - Finish the true mutation batch: route/group once, mutate leaves once, split at batch end, publish ranges once, rebuild quantized payloads once per touched subtree.
  - Direct sorted-run ingestion is correct only for true append/sorted bulk builders. It is wrong for HBC online mutation batches, which repeatedly rewrite node bodies, split ranges, vector-to-leaf mappings, and quantized payloads; bypassing mutable-state coalescing for those batches turns stale internal rewrites into immutable table bytes (disk amplification) instead of foreground compaction cost. HBC mutation transactions stay on the default mutable-state coalescing path inside outer bulk-ingest sessions; direct sorted ingest is reserved for a future true HBC bulk builder that publishes final nodes/ranges/quantized payloads once.
  - Guardrail fields on the HBC write bench (`bench/vectors/hbc_write_bench.zig`) and `antfly-storage-bench` track this: `active_hbc_quant_value_bytes`, `latest_hbc_quant_value_bytes`, and `hbc_quant_versions_per_key_bps`. A healthy bulk/dense ingest path keeps `active_hbc_quant_value_bytes` close to `latest_hbc_quant_value_bytes` after the outer ingest window finishes, and `hbc_quant_versions_per_key_bps` near `10000`.
  - Dense catch-up finish is governed by two env vars: `ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_L0_RUNS` (default `4`) bounds how far the HBC LSM collapses deferred L0 runs when the dense catch-up window closes, and `ANTFLY_DENSE_CATCH_UP_MAINTENANCE_STEPS` (default `8`) bounds dense LSM maintenance steps run after the HBC session finishes. Dense HBC LSM obsolete retention defaults to `0`: once a run is no longer referenced by an active reader it is eligible for immediate cleanup instead of lingering for minutes.
- Full-text writes:
  - Add explicit segment writer budgets for pending docs, token bytes, postings bytes, stored-doc bytes, and merge output bytes.
  - Use tiered segment merge policy by similar-sized segments, tombstone pressure, and max merge bytes.
  - Batch full-text manifest updates during replay/bulk windows.

Per-store LSM config split should come first. Add explicit store profiles for primary documents, derived log, text main/WAL metadata, HBC nodes, HBC metadata, HBC vectors, HBC vector mappings, and graph reverse indexes. The first implementation can thread per-store `lsm_backend.Options` through `IndexBackendOptions`, `PersistentIndex.open`, `HBCIndex.openWithLsmOptions`, and graph reverse store open. HBC should be the first consumer because node, metadata, vector, and mapping writes have very different sizes and rewrite behavior.

Online manifest batching should be policy-based, not a hidden durability change. Keep immediate manifest publication for full-durability writes, but add a bounded deferred policy for weak-sync/background work with `max_dirty_runs`, `max_dirty_bytes`, and `max_delay_ns`. `sync(force = true)`, close, snapshots, and export should force the manifest. The existing `manifest_dirty`, `obsolete_manifest_dirty`, and deferred storage finalization hooks are the right place to attach this.

The true HBC mutation batch should replace repeated online insertion with a staged batch:

- Transform and route all vectors once.
- Group inserts by routed leaf.
- Load each touched leaf once.
- Mutate each leaf in memory.
- Save each touched leaf once.
- Record split candidates instead of splitting per insert.
- Process splits after leaf mutation and update parent/range metadata once.
- Rebuild quantized payloads once per touched subtree or batch.
- Flush index metadata once and commit once.

Implement HBC mutation batching in phases:

- Phase A: no-split mutation batch. Batch leaves that will not overflow; fallback only overflow groups.
- Phase B: split-at-batch-end. Build the full post-mutation member set for overflow leaves, split once, save left/right/parent once, then repair ancestor ranges.
- Phase C: minimal quantized rebuilds. First slice done: track nodes whose bodies changed under deferred quantized mode and rebuild only that touched set at write finish. Next slice should make split-parent/subtree tracking explicit enough to distinguish leaf payloads from internal payloads in profile output.
- Phase D: deletes and updates. Preserve `doc_key` as vector identity for normal indexes and parent-delete semantics for chunked vectors.

The guardrail is `antfly-storage-bench`: compare default, assume-absent, current coalesced, and mutation-batch workloads before enabling broader production use.

### Hot-Path Durable-Log Batching Plan

The benchmark-only `dense-stack-bench --bulk-session` result proves that the
backend wants a larger ingest window, but the production boundary should be the
durable apply path, not arbitrary one-off API calls. Single `DB.batch(...)`
requests still need normal semantics; large ordered sources should open an
explicit ingest window around many durable records.

Target hot paths:

- Raft/apply-log consumption: drain a bounded window of committed records, apply
  primary writes in order, coalesce derived/index work, and finish the LSM
  ingest session once for the whole window.
- Replication snapshot/backfill: wrap the snapshot batch loop in a write-source
  ingest session so many backfill batches share one manifest/finalization
  window.
- Derived index catch-up: keep using `ReplayBatcher` for dense/text/sparse/graph
  coalescing and keep dense catch-up under an explicit no-compaction LSM session.
- Async derived workers: keep live worker catch-up on `derived_worker.catchUpIndex`
  so it consumes the same replay batcher as recovery.

Execution order:

1. Add write-source bulk-ingest window hooks:
   `beginBulkIngest(table)`, `finishBulkIngest(table, compact=false)`, and
   `abortBulkIngest(table)`.
2. Implement the hooks for direct bound DBs and provisioned write-cache DBs.
   Provisioned sources should begin sessions on already-open cached group DBs
   and automatically begin the same session on group DBs opened during the
   window.
3. Wrap replication snapshot/backfill loops first. This is a durable ordered
   producer and should get the same one-manifest behavior as the benchmark
   without changing public API request semantics.
4. Add a raft/apply-log drain window after backfill is proven. The drain policy
   should bound max records, bytes, elapsed time, and deferred L0 runs.
5. Keep applied-sequence persistence at the window boundary where correctness
   allows it; otherwise persist in smaller chunks but keep LSM finalization
   deferred under the outer session.
6. Add metrics for open ingest windows, rows/records per window, deferred L0
   runs, finish time, and forced compaction count.

Current execution status:

- Done: `TableWriteSource` exposes optional bulk-ingest window hooks, with
  implementations for direct bound DBs and provisioned write-cache DBs.
- Done: provisioned write caches track active table windows and automatically
  begin the same backend session for matching group DBs opened during the
  window.
- Done: replication snapshot/backfill wraps direct writes and routed target
  tables in no-compaction ingest windows, so many snapshot batches share one
  backend finalization/manifest window.
- Next: add the raft/apply-log drain window with bounded records, bytes, time,
  and deferred L0 runs, then add metrics around both backfill and apply-log
  windows.

> **Relocated:** The dated 2026-04-16 write-amplification follow-up log (nine dated sections plus the execution checklist, 971 lines) that previously lived here is preserved verbatim in [work-log/completed/lsm-writes/follow-ups-2026-04.md](../../../../../../work-log/completed/lsm-writes/follow-ups-2026-04.md). Durable decisions from it are folded into Benchmark Targets, Longer-Term Shape, and Remaining Write-Amplification Plan above.
