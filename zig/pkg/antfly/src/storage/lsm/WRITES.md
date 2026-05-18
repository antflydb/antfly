# LSM, HBC, and Full-Text Write Performance

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

Add `bench/storage/lsm_write_bench.zig` and a `zig build lsm-write-bench` step.

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
zig build lsm-write-bench -- --samples 3 --keys 100000 --batch-size 5000 --storage native --mode both
zig build lsm-write-bench -- --samples 3 --keys 100000 --batch-size 5000 --storage host --mode both
zig build lsm-write-bench -- --samples 3 --keys 100000 --batch-size 5000 --storage native --mode bulk_ingest --flush-threshold 1024
zig build lsm-write-bench-compare -- --before /tmp/lsm-write-before.jsonl --after /tmp/lsm-write-after.jsonl
zig build text-segment-write-bench -- --samples 3 --docs 100000 --batch-size 5000 --merge-width 8 --storage native
zig build hbc-write-bench -- --samples 3 --vectors 100000 --dims 128 --batch-size 5000 --storage host
```

Use `--storage native` for filesystem timings and `--storage host` for lower-noise persisted table/manifest write counts through the host storage abstraction.

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
zig build text-segment-write-bench -- --samples 3 --docs 100000 --batch-size 5000 --merge-width 8 --storage native
```

## Longer-Term Shape

The durable write path should look closer to Lucene/Tantivy/Pebble:

- Writers build immutable generations.
- Readers pin a generation and never force writer shutdown.
- Full-text writers flush bounded immutable segments, then merge in the background under a byte budget.
- The LSM has explicit cache and write budgets instead of implicit unbounded growth.
- HBC supports both online mutation and offline bulk construction.
- Large loads publish sorted table files and HBC nodes once, instead of repeatedly growing and compacting online state.

## Implementation Roadmap

Current status:

- `zig build lsm-write-bench` exists and emits JSONL for sorted load, random load, overwrite, and delete workloads.
- `zig build lsm-write-bench-compare` compares median timings and write-amplification counters by scenario/workload.
- `zig build text-segment-write-bench` exists for full-text segment build, on-disk publish, merge, and force-merge mechanics outside the DB catalog.
- `zig build hbc-write-bench` exists for empty bulk build vs default online HBC batches vs online batches with absent-id hints vs experimental coalesced leaf writes, with storage write counters and `HBCIndex.WriteProfile`.
- Native 100k baselines are checked in under `bench/baselines/` for LSM writes, HBC writes, and full-text segment writes.
- Derived dense replay now threads `.bulk_ingest` into the HBC insert choice: empty indexes use the HBC bulk builder, and replay batches whose vector IDs were newly allocated skip per-vector existence probes.
- HBC leaf-write coalescing is implemented behind `BatchInsertOptions.coalesce_leaf_writes`. It reduces bytes but can be slower than the absent-id path, so production only enables it for bulk replay batches where vector IDs are known-new.
- LSM bulk-ingest exit now keeps the elevated mutable threshold instead of flushing at every `.bulk_ingest` transaction boundary. An explicit backend bulk-ingest session can now span many API/replay batches so direct-ingested runs keep one manifest publication for the whole ingest window.
- Bulk-ingest sessions have two finish modes: the default compacts on session close for normal backend use, while `finishBulkIngestSessionWithOptions(.{ .compact = false })` flushes remaining mutable state, publishes the manifest once, and leaves compaction to a later maintenance window.
- Dense derived replay now opens an LSM bulk-ingest session around each index catch-up window. It closes with `.compact = false` and a deferred-L0 guardrail, so normal replay gets the one-manifest behavior from the benchmark while still compacting if L0 run count grows past the safety limit.
- Dense replay batching now allows larger 16k-record/embedding windows before flushing, reducing the number of direct-ingested sorted runs produced during catch-up.
- `Backend.ingestSortedTableEntries()` is the first sorted-run ingestion API. It validates a sorted unique key stream, writes table files directly, publishes one manifest, and avoids mutable flush/compaction for known bulk data.
- Runtime `.bulk_ingest` commits now direct-ingest the transaction state when the batch reaches the effective bulk threshold and there is no older mutable state that would shadow newer runs. Smaller bulk batches still accumulate in mutable state under the elevated threshold.
- `lsm-write-bench` routes sorted `.bulk_ingest` loads through sorted-run ingestion. On the 100k native smoke run, default sorted load wrote 23 table files, 24 manifests, performed 3 compactions, and issued 670 range reads; sorted bulk ingest wrote 1 table file, 1 manifest, performed 0 compactions, and issued 0 reads.
- `lsm-write-bench` wraps random `.bulk_ingest` loads in an outer no-compaction session. On the 100k native smoke run, random bulk ingest wrote 20 table files and 1 manifest, performed 0 flushes/compactions, and issued 0 reads; before the outer publish window it still wrote a manifest per 5k replay batch, and before no-compaction finish it also compacted across batches at session close.
- HBC replay now enables the existing grouped leaf-write path for bulk batches whose vector IDs are known-new and defers quantized rebuilds to the end of each HBC write batch. This is not the final mutation-batch design, but it moves the coalescing path out of benchmark-only status.
- HBC grouped mutation batching now handles no-split leaf groups with two or more inserts, writes changed leaf ranges during mutation, batch-refreshes unique ancestor range chains after split candidates settle, and defers bounded overflow leaf splits until the batch-end split-candidate phase. The split phase recursively requeues left/right leaves until they fit under `leaf_size`. Very large routed groups still fall back to the existing online path until the recursive split budget is proven under larger replay workloads.
- `HBCIndex.WriteProfile` and `hbc-write-bench` now expose grouped-path guardrail counters: grouped leaf groups, grouped items, fallback items, split candidates, recursive splits, leaf range writes, ancestor range refreshes/nodes, grouped node body writes, and vec-leaf mapping writes.
- `HBCIndex.WriteProfile` and `hbc-write-bench` also expose logical HBC namespace write counters for `nodes`, `meta`, `quant`, and `vecs`: put calls, append calls, delete calls, key bytes, and value bytes. Range writes are broken out separately because range keys live in the `nodes` namespace but have a different rewrite shape.
- A 20k native HBC smoke run with namespace counters showed the current coalesced path cuts node puts from about 92k to 26k, quantized puts from about 20.7k to 7.1k, and range puts from about 50k to 11k, but `vecs` puts stayed high at about 80k because raw vectors, metadata, and vec-leaf mappings were still interleaved with leaf mutation. The next slice now pre-stores raw vectors and metadata in sorted vector-id order with append puts for known-new coalesced batches, then leaves node/range/quantized/mapping mutation to the grouped path. On a 20k native smoke run this moved 40k raw vector/metadata writes from `vecs` puts to `vecs` appends and dropped `insert_store_vector_ns` from roughly 150-190 ms to about 2 ms; overall elapsed time was still close enough to default/assume-absent that larger samples are needed before broadening production use.
- On a 100k native HBC smoke run after sorted raw-vector/metadata pre-store, coalesced online batches measured about 57.2 us/vector versus assume-absent at about 65.5 us/vector and default at about 67.2 us/vector. Storage bytes stayed lower at about 715 MB versus about 738 MB for default/assume-absent. The main counter movement was `insert_store_vector_ns` dropping from about 2.67 s to about 11 ms while 200k raw vector/metadata writes moved to append puts.
- A follow-up final-only vec-leaf mapping experiment was rolled back. It reduced `vecs` put calls slightly, but rewrote older members in touched split leaves, increased storage bytes, and did not improve elapsed time on the 20k native smoke run.
- On 100k native HBC smoke runs after bounded recursive split batching, coalesced online batches consistently wrote fewer bytes, about 715 MB versus default/assume-absent at about 738 MB, but timing is not consistently faster. A later 100k run measured coalesced at 68.4 us/vector versus default and assume-absent at about 66.8 us/vector. A deferred sorted leaf-range publication experiment was rolled back because it worsened timing while preserving the same byte reduction.
- Deferred quantized rebuild now tracks the HBC nodes whose bodies changed during the write transaction and refreshes only that touched-node set at finish. This replaces the full-tree `rebuildAllQuantized()` behavior for adapters that provide the touched-node tracker. A 10k native smoke run with 5k batches moved coalesced deferred quantized puts to 182 versus 6,419 for non-deferred coalesced and 10,354 for default/assume-absent online batches.
- The true mutation-batch work is partly implemented, not a clean win yet. The successful pieces are sorted raw-vector/metadata pre-store, no-split leaf grouping, bounded batch-end split handling, ancestor range refresh coalescing, and touched-node quantized rebuild. The rolled-back pieces were narrower experiments: final-only vec-leaf mapping and deferred sorted leaf-range publication both preserved byte reductions but worsened timing/storage behavior. The next safe HBC slice avoids writing an oversized temporary leaf before splitting a grouped overflow leaf; instead the grouped path now splits directly from the in-memory mutated leaf and queues the resulting leaves only for recursive overflow checks.
- `zig build lsm-backend-test` is a focused LSM backend unit-test step; the current LSM backend bucket passes 56/56 tests.
- The bench can run against host-backed in-memory persistence, native filesystem storage, or memory-only storage.
- `Backend.WriteStats` now exposes flush, table-file, manifest, and compaction timing/byte counters via `snapshotWriteStats()`.

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
- LSM write publication:
  - Keep bulk-ingest sessions for replay/catch-up windows.
  - Add policy-based online manifest batching for weak-sync/background writes.
  - Add dirty-run, dirty-byte, and max-delay guards before delayed manifest publication.
- LSM compaction:
  - Keep no-compaction bulk finish for write amplification.
  - Add maintenance compaction triggers by L0 run count, L0 bytes, and observed read amplification.
- HBC writes:
  - Avoid root-style quantized payload writes for internal bulk-built nodes that will be reparented.
  - Finish the true mutation batch: route/group once, mutate leaves once, split at batch end, publish ranges once, rebuild quantized payloads once per touched subtree.
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

The guardrail is `hbc-write-bench`: compare default, assume-absent, current coalesced, and mutation-batch workloads before enabling broader production use.

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

### 2026-04-16 VectorDBBench Write Regression Follow-Up

Checkpoint: `6f0b30c ezfind checkpoint`.

Fresh 50k VectorDBBench results show reads are healthy again but writes are
still dominated by small-batch persistence and HBC mutation/finalization:

- Insert/load: about 253s/256s on the 50k run.
- Search p99: 7.8ms, recall 0.9791.
- Parsed sample window:
  - HBC wall: 43.2s total, p95 1.3s, max 2.7s.
  - HBC commit: 24.0s total, p95 1.1s, max 2.5s.
  - HBC leaf mutation: 19.5s total, p95 406ms, max 900ms.
  - HBC quantized refresh: 7.7s total, p95 124ms, max 201ms.
  - Primary batch store: 15.2s total, p95 311ms, max 3.9s.

Interpretation:

- The active dense path is bulk-new-vector aware (`all_new=true`,
  `assume_absent=true`, `coalesce=true`), but VectorDBBench still sends mostly
  100-row API batches and HBC commits each small derived/index batch.
- Current DB bulk-ingest sessions primarily cover the primary store. Dense HBC
  index stores have their own LSM backends and can still publish/flush/compact
  at each tiny batch boundary.
- The next optimization needs to turn "bulk ingest" from a per-small-batch mode
  flag into an outer ingest window across dense HBC index stores and primary
  LSM stores.

Task list:

1. Add LSM commit/finalization sub-metrics that can explain HBC `commit_ms`:
   direct sorted ingest, mutable merge, flush, compaction, manifest persist,
   obsolete cleanup, and reader-release/finalize time.
2. Surface those sub-metrics in HBC write logs so VectorDBBench runs show
   whether commit spikes are manifest/table/compaction/cache/fd work.
3. Wire bulk-ingest sessions through dense HBC index stores, not only the DB
   primary store. Begin/finish/abort should apply to each managed dense index
   backend when an outer ingest window is open.
4. Lower or adapt the API auto-bulk threshold for sustained 100-row write
   batches. A single small API request keeps normal semantics, but consecutive
   weak-sync write batches should share a bounded ingest window.
5. Coalesce dense derived replay into larger HBC batches, targeting 2k-5k items,
   so small VectorDBBench chunks do not each force HBC LSM finalization.
6. After commit/finalization time falls, revisit true HBC mutation batching:
   reduce fallback items, split churn, node writes, quantized writes, and
   vec_leaf mapping writes.

Current execution status:

- Done: HBC indexes expose their backing LSM write stats, and dense HBC write
  logs now emit `antfly_bench_hbc_lsm_write` with flush, sorted-ingest,
  compaction, manifest, and table-write deltas per HBC batch.
- Done: DB bulk-ingest sessions now open matching dense HBC index backend
  sessions, and dense replay/catch-up opens a no-compaction session on the
  target HBC index as well as the primary store. This should turn many tiny HBC
  batch commits into one deferred HBC LSM finalization window.
- Done: lower the provisioned write-cache auto-bulk trigger from 500 ops to
  100 weak-sync ops so VectorDBBench-style 100-row batches enter a sustained
  ingest window instead of finalizing each request independently.
- Done: bound the auto-bulk window by idle time and max open time. This keeps
  the change on the real hot path for sustained weak-sync ingestion, while
  avoiding indefinite deferred L0/manifest work for normal sporadic API writes.
- Next: rerun the 50k write benchmark and compare `commit_ms` against the new
  `antfly_bench_hbc_lsm_write` line to see whether remaining spikes are flush,
  manifest, compaction, table writes, or mutation work.

### 2026-04-16 Follow-Up Run: HBC Bulk Flag Without HBC Bulk Behavior

Fresh 50k VectorDBBench after `d6feb1b ezfind: wire hbc bulk ingest sessions`
completed, but insert/load regressed to about 286s while reads stayed healthy:

- Search p99/p95: 7.7ms/6.4ms.
- Recall/nDCG: 0.9793/0.9800.
- HBC wall: 187s total.
- HBC commit: 116s total.
- HBC mutate leaf: 67s total.
- HBC find leaf: 24s total.
- HBC split leaf: 29s total.
- HBC refresh quantized: 25s total.
- HBC LSM flush bytes: 20.8GB.
- HBC LSM table bytes: 39.6GB.
- HBC LSM compaction: 74.6s total.

The important signal is that HBC write records say `mode=bulk_ingest`, but the
new HBC LSM records still show `sorted_ingest_runs=0`, manifest writes on every
small HBC batch, and foreground compaction inside write commit. Example:

- `flush_bytes=112.8MB`
- `table_bytes=1.57GB`
- `table_writes=6`
- `compaction_ms=5681`

Diagnosis:

- The production path is not just missing a benchmark knob. The live dense
  derived worker path is still committing HBC's internal LSM batches like normal
  online writes.
- The previous bulk session wiring covered explicit DB sessions and
  replay/catch-up, but VectorDBBench exercises live async dense-derived
  catch-up.
- HBC's vectorindex `NamespaceStore` only exposed `beginBatch()`, so even when
  higher layers passed `.bulk_ingest`, HBC internal namespaces could not enter
  direct/sorted bulk-ingest batch mode.
- The 250ms auto-window idle bound may also be too short for 100-row batches
  whose HBC/LSM work can already take hundreds of milliseconds, but the primary
  failure is proven by per-HBC-batch manifests and compactions.

Next task list:

1. Thread `beginBatchWithOptions(.{ .mode = .bulk_ingest })` through
   vectorindex `NamespaceStore` and the erased backend namespace store.
2. Use those batch options for HBC batch insert/apply paths only after we have a
   true final-state bulk builder. Online HBC mutation batches are rewrite-heavy
   and must still use mutable-state coalescing.
3. Open dense HBC bulk sessions around live async dense-derived catch-up
   windows, not only reopen/replay paths.
4. Keep finishing live dense windows with `.compact = false` and an L0
   guardrail so compaction moves out of each small HBC write commit.
5. Rerun 50k and expect `antfly_bench_hbc_lsm_write` to show sharply fewer
   per-batch manifest writes, fewer foreground compactions, lower
   `table_bytes / flush_bytes`, and possibly non-zero sorted-ingest runs where
   batches are append/sorted enough.

Current execution status:

- Done: vectorindex `NamespaceStore` now exposes `beginBatchWithOptions`, and
  the erased storage namespace store forwards batch options to backends that
  support them.
- Reverted: HBC batch insert/apply paths no longer request direct
  `.bulk_ingest` namespace batches for online mutation. The outer LSM session
  still defers manifests/compaction, while mutation transactions coalesce
  repeated internal keys in mutable state before flush.
- Done: live dense-derived apply opens a dense HBC backend bulk session and
  finishes it with no foreground compaction plus the deferred-L0 guardrail.
- Next: rerun the 50k write benchmark. The first thing to check is whether
  `antfly_bench_hbc_lsm_write` loses per-batch `compaction_ms` and manifest
  writes; if compaction merely moves to session finish too often, widen the live
  dense-derived window across more than one apply batch.

### 2026-04-16 Follow-Up Run: Direct Sorted Ingest Is Wrong For HBC Mutations

The next 50k VectorDBBench run failed around 45.9k docs because the benchmark
root grew to roughly 50GB and the process hit `NoSpaceLeft` while writing the
metadata apply-store manifest. That size is itself the bug: 50k documents and
vectors should not need anything close to 50GB on disk.

The HBC compaction fix worked too literally:

- HBC LSM `compaction_ms` dropped to 0.
- HBC LSM `manifest_writes` stayed deferred.
- HBC LSM `sorted_ingest_runs` rose to 249 before failure.
- HBC LSM `sorted_ingest_bytes` reached about 18.4GB before failure.
- Late 100-row HBC apply batches wrote 100-190MB sorted-ingest tables.

Diagnosis:

- Direct sorted-run ingestion is correct for true append/sorted bulk builders.
- It is wrong for HBC online mutation batches because those batches repeatedly
  rewrite node bodies, split ranges, vector-to-leaf mappings, and quantized
  payloads.
- By bypassing mutable-state coalescing, every stale internal HBC rewrite became
  immutable table bytes. We moved the cost from foreground compaction into disk
  amplification.

Fix direction:

- Keep the outer HBC/LSM bulk-ingest session so manifests and compaction are
  deferred across the sustained ingest window.
- Do not open each HBC mutation transaction in direct `.bulk_ingest` batch mode.
  Let the transaction merge into the backend mutable state so repeated internal
  keys coalesce before flush.
- Reserve direct sorted-run ingestion for a future true HBC bulk builder that
  publishes final nodes/ranges/quantized payloads once, not for online mutation.
- Add a regression test asserting that HBC bulk-ingest mutation batches defer
  manifests without increasing `sorted_ingest_runs`.

Still broken after this slice unless proven otherwise:

- Total on-disk bytes may still be high outside HBC: primary docs, row-adjacent
  enrichment artifacts, full-text segments, derived-log/apply metadata, and text
  merge output all need per-store byte counters.
- `NoSpaceLeft` should propagate as a failed write/request and allow clean
  shutdown. It must not lead to a panic in `text_merge_runtime.deinit()`.

### 2026-04-16 Follow-Up: Disk Amplification Is Still Too High

The 50k VectorDBBench roots getting into the tens of GB is not explained by
"uncompressed JSON" alone. Even without block compression, 50k small documents
and one 1536D embedding each should not produce 18GB table directories or a
roughly 50GB database root.

Priority order:

1. Store embedding artifacts in a compact binary format instead of JSON float
   arrays.
   - Generated embedding artifacts are intentionally stored beside the document
     for locality, but the artifact payload should not be a text JSON array of
     decimal floats.
   - No legacy JSON artifact compatibility is required for this migration.
   - API-facing lookup/projection should continue returning the same logical
     `_embeddings` shape even when the stored payload is binary.
   - Track the storage-side artifact codec, versioning, and skip-by-source-hash
     behavior in `ENRICHMENTS.md`.
   - Status: dense and sparse embedding artifacts now use the versioned binary
     codec in the async generated-enrichment path. Whole-document and chunked
     async dense/sparse enrichment skip unchanged source hashes, and chunk
     replacement deletes stale chunk/embedding artifact rows after computing
     desired chunk keys.
2. Add byte-based mutable flush thresholds and per-store LSM configs.
   - Current flush behavior is too tied to entry counts, so tiny logical batches
     can flush very large table files once values are large or internal index
     records churn.
   - HBC nodes, HBC metadata, vector mappings, full-text metadata, graph reverse
     indexes, primary docs, derived apply metadata, and generated artifact
     stores need different flush and compaction budgets.
   - Track live bytes vs obsolete retained bytes per store. The backend retains
     obsolete files for reader safety, but benchmark roots must tell us whether
     disk use is live L0/table data, obsolete files waiting for retention, or
     leaked files.
3. Fix HBC quantized/node write staging for small derived batches.
   - Small live dense-derived batches must not rewrite hundreds of quantized
     payloads or node records immediately.
   - Stage touched node/range/quantized updates across the outer dense ingest
     window, then publish each final record once where correctness allows.
   - Keep HBC mutation transactions in mutable-state coalescing mode; direct
     sorted ingest is only appropriate once a true HBC bulk builder emits final
     sorted records.

Follow-up, after those are measured:

- LSM block compression now has adaptive per-block Snappy framing as the first
  configurable policy. Add zstd/lz4 policies later as per-store options after
  measuring CPU cost and compression ratio; this should not become a single
  global format switch.
- Compression metrics are now part of the write/maintenance snapshots:
  `table_logical_entry_bytes`, `table_physical_entry_bytes`,
  `table_compressed_blocks`, `table_raw_blocks`, and
  `table_compression_codec_mask`.
  HBC LSM benchmark logs include those deltas, and maintenance logs include
  active-run logical/physical entry bytes so disk growth can be separated from
  live table payload, obsolete files, and repeated publication.
- Compression should reduce primary/artifact/full-text table bytes, but it is
  not a substitute for fixing duplicate vector payloads, too-frequent flushes,
  obsolete-file accounting, or HBC rewrite amplification.
- Add stored-value compression for primary documents and chunk artifacts.
  Chunks can remain JSON payloads for now, but should be zstd-compressed later.
  Prefer an explicit codec/header when we need row-level version or source-hash
  metadata; zstd frame sniffing is acceptable only for opaque primary document
  blobs where the store does not need extra row metadata.

### 2026-04-16 HBC Bench Guardrail: Quantized Rewrite History

The 50k VectorDBBench root on `9e9c136` showed the vector-index directory at
about 17GB, but manifest inspection split that into two different buckets:

- About 3.3GB of active vector-index runs.
- About 14GB of obsolete run files retained for reader safety.

Within the active runs, the useful latest `hbc_quant` payload was close to the
expected raw vector size, but active L0 history held about 10 versions per
quantized node. That means the current HBC problem is not primarily private
`.vecs` vector duplication; dense DB writes already skip the raw vector store.
The problem is repeated publication of large `hbc_quant` leaf/root payloads
during online/batched ingest.

`bench/vectors/hbc_write_bench.zig` now has dense external-vector scenarios that match
the DB path more closely:

- `bulk_build_external_vectors_empty`
- `online_batches_dense_external_vectors_empty`

The bench now emits active-table namespace metrics:

- `active_hbc_quant_value_bytes`
- `latest_hbc_quant_value_bytes`
- `hbc_quant_versions_per_key_bps`
- `active_hbc_vecs_value_bytes`
- `latest_hbc_vecs_value_bytes`
- active LSM run bytes, L0 bytes, compression stats, and obsolete path count

Use this as the guardrail for the next HBC fix. A healthy bulk/dense ingest path
should keep `active_hbc_quant_value_bytes` close to
`latest_hbc_quant_value_bytes` after the outer ingest window finishes, and
`hbc_quant_versions_per_key_bps` should stay near `10000` instead of drifting to
`30000` for small benches or `100000` for the 50k run.

### 2026-04-16 Dense Catch-Up Window And Cleanup

The Go path does not rely on an empty-index HBC bulk builder for normal ingest.
It relies on a larger Pebble write batch plus writer cache, so repeated HBC node
and quantized updates coalesce before publication. The Zig async derived worker
was still opening and closing the HBC bulk session around each flushed replay
batch, even though the replay batcher could apply several batches in one
catch-up pass.

The async/manual/io derived executors now have catch-up lifecycle hooks. Dense
workers open one HBC bulk-ingest session around the whole `catchUpIndex` pass and
finish it once after all replay-batcher flushes complete. Per-batch HBC apply
calls still use nested sessions, but nested finish does not publish deferred
quantized payloads; the outer finish publishes once.

Dense catch-up finish now uses a stricter vector-index L0 cleanup policy:

- `ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_L0_RUNS`, default `4`, controls how far
  the HBC LSM collapses deferred L0 runs when the dense catch-up window closes.
- `ANTFLY_DENSE_CATCH_UP_MAINTENANCE_STEPS`, default `8`, runs bounded dense
  LSM maintenance after the HBC session finishes.
- The primary document store does not receive that stricter L0 limit; its bulk
  windows still finish with `.compact = false`.
- Dense HBC LSM obsolete retention is now `0` by default. Active readers still
  pin old runs, but once a run is no longer referenced it should be eligible for
  Pebble-like immediate cleanup instead of lingering for minutes and inflating
  `du` during benchmarks.

The next benchmark check is whether the 50k vector-index directory no longer
accumulates multi-GB obsolete history and whether active `hbc_quant` versions
stay close to latest unique payload bytes after catch-up finish.

### 2026-04-16 Hot Overwrite LSM/Pebble Guardrail

Pebble does not make overwrite bytes disappear. It keeps overwrite debt bounded
with large write batches/memtables, background L0 pressure compaction,
iterator/block-based compaction, compression, and prompt obsolete SST deletion
when no snapshots or readers pin old files. The Zig LSM needs a direct guardrail
for that behavior outside of HBC, because `hbc_quant` just makes a generic
overwrite problem obvious with large values.

`bench/storage/lsm_write_bench.zig` now includes a `hot_overwrite` scenario:

- `load_base` loads the full base keyspace.
- `overwrite_hotset` overwrites the same hot keys for
  `--overwrite-rounds`.
- `maintenance_hotset` runs bounded maintenance via
  `--hot-maintenance-steps`.

The bench now emits compression and live-run debt fields alongside the existing
flush/manifest/compaction counters:

- `lsm_table_file_logical_entry_bytes`
- `lsm_table_file_physical_entry_bytes`
- `lsm_table_file_raw_blocks`
- `lsm_table_file_compressed_blocks`
- `lsm_table_file_compression_codec_mask`
- `obsolete_paths_after`

There is also a Pebble comparison helper at
`compat/pebble_overwrite_go`. Run it with the same `--keys`,
`--hot-keys`, `--overwrite-rounds`, `--value-size`, and `--batch-size` values
as the Zig LSM bench. It reports disk totals split into SST/WAL/manifest/other
bytes after load, after hot overwrites, and after a full compaction. The helper
emits JSONL using the same high-level fields as `lsm-write-bench`:
`load_base`, `overwrite_hotset`, and `maintenance_hotset`, plus Pebble-specific
L0/read-amp/WAL/obsolete-table metrics. Use `--value-pattern keyed` for
HBC-like payloads; the default repeated-byte pattern is useful for compression
sanity checks but is too compressible for write-amplification comparison.

Useful first comparison:

```sh
zig build lsm-write-bench -- --samples 1 --keys 20000 --hot-keys 1000 \
  --overwrite-rounds 20 --value-size 65536 --value-pattern keyed \
  --batch-size 1000 --storage native --mode default \
  --workload-set hot_overwrite

(cd compat/pebble_overwrite_go && go run . --keys 20000 --hot-keys 1000 \
  --overwrite-rounds 20 --value-size 65536 --value-pattern keyed \
  --batch-size 1000)
```

Small smoke comparison:

```sh
(cd compat/pebble_overwrite_go && \
  GOCACHE=/tmp/antfly-go-build-cache go run . --samples 1 --keys 2000 \
    --hot-keys 500 --overwrite-rounds 2 --value-size 512 \
    --value-pattern keyed --batch-size 500 --compact=true)
```

On 2026-04-16 this smoke run kept Pebble at about `1.30x` live table bytes
after the overwrite phase and about `1.04x` after manual compaction. That is the
generic LSM target shape: bounded foreground overwrite debt and a maintenance
step that collapses superseded versions back near the live set.

Use this before changing generic LSM policy. The expected next fixes are
generic:

- tune byte-based mutable flush thresholds by store from real traces,
- L0 guardrails after bulk/replay windows,
- prompt obsolete-file cleanup when no reader pins old runs,
- background/maintenance compaction,
- iterator/block-based compaction to avoid whole-run materialization.

Bounded keyed overwrite comparison, `keys=5000`, `hot_keys=500`,
`overwrite_rounds=10`, `value_size=65536`, `batch_size=500`:

- Pebble:
  - Load: `sst_bytes=328064934`, close to the 327MB live value set.
  - After hot overwrite: `sst_bytes=401278646`, so the 327MB overwrite did not
    remain as 327MB of active SST debt.
  - After manual compact: `sst_bytes=328059136`, back near live bytes.
- Zig LSM default mode:
  - Load: `run_bytes_after=328336160`.
  - Hot overwrite: `lsm_table_file_bytes=1805840080`,
    `compaction_ns=3926200000`, `run_bytes_after=656670560`,
    `run_entries_after=10000`, `obsolete_paths_after=13`.
  - This is the bad path: foreground compaction rewrites about 1.8GB for a
    327MB overwrite phase and still leaves the active set with both older and
    newer versions.
- Zig LSM `bulk_ingest` mode:
  - Load: two L0 runs, `run_bytes_after=328335424`.
  - Hot overwrite: one 32MB overwrite run,
    `run_bytes_after=361169040`, `run_entries_after=5500`, no compaction.
  - This is the desired coalescing behavior: repeated hot-key overwrites within
    one bulk window collapse in mutable state before table publication.

Implication: the LSM is capable of Pebble-like overwrite coalescing when the
writer keeps repeated updates in one mutable/bulk window and uses `put`/upsert
semantics. The pathological vector-index behavior is therefore likely from HBC
or replay publishing quantized/node updates across too many independent windows,
or from direct sorted ingestion/flush boundaries that bypass mutable coalescing.
Do not use sorted-run direct ingest for overwrite-heavy namespaces unless the
stream is known to contain final unique keys.

### 2026-04-16 Default Write Policy Change

The Zig LSM default write path now defers soft compaction out of foreground
write finalization. Non-bulk writes still flush mutable state, persist the
manifest, and enforce hard L0 pressure. Soft L0/level cleanup is now maintenance
work unless `foreground_soft_compaction=true` is set for a test or specialized
store.

This makes the default policy closer to Pebble's separation:

- foreground write path: append/memtable/flush and hard pressure only,
- maintenance/background path: soft L0 and level compaction,
- explicit bulk/replay path: larger coalescing window with controlled finish.

The same bounded keyed overwrite run after this change:

- Zig LSM default mode:
  - Load: `run_bytes_after=328336160`, `compactions=0`.
  - Hot overwrite: `lsm_table_file_bytes=328336160`, `compactions=0`,
    `run_bytes_after=656672320`, `run_entries_after=10000`.
  - This removes the 1.8GB foreground-compaction rewrite from the write phase.
    It intentionally leaves L0/version debt for maintenance.
  - `maintenance_hotset` then spent `compaction_ns=4229029000` and wrote
    `lsm_table_file_bytes=1641666420`, ending at
    `run_bytes_after=591002212`.
- Zig LSM `bulk_ingest` mode remains the better coalescing shape for replay/HBC
  windows:
  - Hot overwrite: one 32MB run, `run_bytes_after=361169040`,
    `run_entries_after=5500`, no compaction.

Implication: removing foreground soft compaction fixes the write latency spike
class, but does not by itself match Pebble's background compaction quality.
The remaining generic LSM work is to make maintenance cheaper and more
continuous:

- run maintenance in the background or from the resource manager when L0 score
  crosses the soft threshold,
- compact with iterators/blocks instead of materializing whole persisted runs,
- compact overwrite-heavy L0 windows in a way that drops superseded versions
  sooner,
- publish fewer manifest updates during maintenance batches,
- keep HBC quantized/node writes in a coalescing outer session so the LSM sees
  fewer duplicate versions in the first place.

### 2026-04-16 Pebble-Like Mutable Window Follow-Up

The next step toward Pebble's `~1.30x` hot-overwrite shape is to make the
default write profiles hold WAL-backed mutable state long enough to coalesce
small API/replay commits before table publication.

Changes made:

- Production LSM profiles now use larger byte windows:
  - primary docs: `32 MiB`,
  - text main/WAL metadata: `16 MiB`,
  - graph reverse indexes: `16 MiB`,
  - dense HBC/vector index LSM: `128 MiB`.
- Dense HBC keeps direct sorted ingest disabled for online mutation streams.
  Its bulk byte multiplier is now `4`, so a dense replay/bulk window can hold
  up to about `512 MiB` of final unique mutable state before publication.
- `bench/storage/lsm_write_bench.zig` now defaults to
  `--flush-threshold-bytes 67108864` and exposes
  `--flush-threshold-bytes` explicitly. This makes the default hot-overwrite
  guardrail exercise WAL-backed byte-window coalescing instead of entry-count
  flushes.
- Added an LSM regression test where `flush_threshold=1` but
  `flush_threshold_bytes=8 MiB`. It loads 2k keys, overwrites 500 hot keys
  twice across small commits, and asserts the published runs contain
  `2000 + 500` entries, not `2000 + 1000`.
- Commit now applies transaction state into the active mutable state by moving
  owned entries instead of cloning key/value bytes through `mergeStates()`.
  This keeps sorted `State` as the immutable flush input, but makes the active
  writer path behave more like a memtable: WAL append first, then move/update
  the latest in-memory entry for each key.
- The LSM write bench now accepts `--readers N`. During
  `overwrite_hotset`, reader threads continuously open read snapshots and
  issue point gets over the hot key set. JSONL output includes `read_ops`,
  `read_misses`, `read_errors`, `read_avg_ns`, `read_p50_ns`,
  `read_p95_ns`, `read_p99_ns`, and `read_max_ns`.
- The bench also reports `writer_ns` and `finalize_ns`. This matters for
  Pebble comparison: the Pebble compat helper times batch writes separately
  from the explicit `Flush()`, while the Zig bench historically timed
  `finalizeDeferredStorageWork()` inside the workload.

Small Zig smoke after this change, matching the Pebble smoke shape:

```sh
zig build lsm-write-bench -- --samples 1 --keys 2000 --hot-keys 500 \
  --overwrite-rounds 2 --value-size 512 --value-pattern keyed \
  --batch-size 500 --storage memory --mode default \
  --workload-set hot_overwrite --hot-maintenance-steps 1 --readers 2
```

Result:

- `load_base`: `run_bytes_after=1118316`, `run_entries_after=2000`.
- `overwrite_hotset`: `run_bytes_after=1398060`,
  `run_entries_after=2500`, `l0_runs_after=2`, `writer_ns≈2.1-2.2ms`,
  `finalize_ns≈0.9ms`.
- With two concurrent readers during the overwrite phase:
  `read_ops=5491`, `read_misses=0`, `read_errors=0`, `read_p95_ns=2047`,
  `read_p99_ns=2047`.

That is about `1.37x` live bytes after hot overwrite, close to the Pebble smoke
baseline of about `1.30x`. The remaining difference is table overhead and
compaction policy, not repeated publication of every overwrite round.

For a more overwrite-heavy comparison:

```sh
zig build lsm-write-bench -- --samples 1 --keys 2000 --hot-keys 500 \
  --overwrite-rounds 20 --value-size 512 --value-pattern keyed \
  --batch-size 500 --storage memory --mode default \
  --workload-set hot_overwrite --hot-maintenance-steps 1

(cd compat/pebble_overwrite_go && \
  GOCACHE=/tmp/antfly-go-build-cache go run . --samples 1 --keys 2000 \
    --hot-keys 500 --overwrite-rounds 20 --value-size 512 \
    --value-pattern keyed --batch-size 500 --compact=true)
```

On 2026-04-16, the 20-round run showed Zig memory LSM
`overwrite_hotset writer_ns=23894000` for 10k overwrites. Pebble reported
`overwrite_hotset ns=27957334` for the same logical workload, while also doing
one automatic compaction during the overwrite phase. This is only a microbench,
but it suggests the repeated-overwrite write loop is no longer the obvious gap
once table publication is accounted separately.

### 2026-04-16 Next Work Execution Checklist

The next pass should stay focused on making Zig's LSM behavior closer to
Pebble's write shape: foreground writes publish bounded new state, while
background work incrementally collapses version debt without materializing
entire large runs.

1. Background/resource-manager maintenance scheduling for L0 soft pressure.
   - Status: data-server tick, explicit idle hooks, and initial LSM compaction
     scheduler done.
   - Trigger maintenance when `maintenanceScore()` crosses soft L0 run/byte
     pressure, and let the resource manager request bounded maintenance steps
     instead of forcing soft compaction during foreground commits.
   - `DB.runUntilIdle()` now drains primary/index LSM maintenance after derived,
     enrichment, and text maintenance are caught up. This gives tests, admin
     paths, and explicit idle drains a place to collapse deferred L0 debt
     without putting soft compaction back into normal commits.
   - The provisioned data-server loop already calls cached writable DB LSM
     maintenance each tick. The new L0 overlap score gives that loop a
     background trigger for hot-overwrite debt even when plain L0 run count has
     not crossed the generic compact threshold.
   - LSM now owns a dedicated compaction scheduler. Maintenance compactions
     acquire a scheduler grant before running. The first implementation is
     synchronous with the data-server tick, but it accounts active jobs,
     in-flight input bytes, grants, completions, capacity denials, resource
     pressure denials, and oversized single-job grants. This matches Pebble's
     separation of "work exists" from "capacity is available" without adding
     foreground soft compaction back to writes.
   - Resource manager integration is admission-style, not policy ownership:
     the LSM picker decides what compaction is useful, the LSM scheduler decides
     whether compaction capacity is available, and the resource manager can
     reject a bounded `lsm.compaction_work` reservation when memory/work
     pressure is too high. That keeps global pressure control centralized
     without making the resource manager understand LSM levels.
   - Guardrail: foreground write batches should not show large soft-compaction
     latency spikes; maintenance stats should explain the deferred work.
2. Iterator/block-based compaction.
   - Status: block-window input iterator done; persisted output streaming done.
   - Replace whole-run materialization in compaction with a block/iterator
     merge so compaction cost scales with streamed input, not cloned run state.
   - Persisted compaction now loads table indexes, reads only the active
     input block/window for each selected run, and streams winner entries into
     a table writer that keeps only offsets, bloom/hash metadata, and the
     current encoded block. This removes whole-run input materialization and
     output `State` chunk buffering for persisted compactions.
   - Added a persisted-compaction regression test that compacts three on-disk
     L0 runs through a counting storage wrapper and asserts source run files are
     read through trailer/range reads, not full-file loads. The test also
     checks the compacted output remains a persisted table run rather than an
     in-memory `State`.
   - Guardrail: hot-overwrite maintenance should not require multi-GB
     allocation/load behavior to compact a few active runs.
3. Overwrite-heavy L0 compaction.
   - Status: initial overlap-pressure policy done.
   - Add a compaction path that prioritizes L0 windows with many duplicate keys
     and drops superseded versions earlier.
   - Dense HBC now uses `compact_threshold_runs=8` while keeping higher soft
     and hard pressure limits. That makes 10-15 L0 `hbc_quant` versions
     eligible for maintenance compaction without moving soft compaction back
     into foreground commits.
   - Generic LSM maintenance now scores overlapping L0 runs separately from
     plain L0 count. When four or more L0 runs overlap the same key range, the
     compaction picker selects that overlapping span so hot overwrite/version
     debt can collapse before the generic run-count limit.
   - A 2k-key hot-overwrite smoke run now reports `overlapping_l0_runs_after=6`
     after overwrites, then one maintenance step compacts 15 L0 runs into one
     L1 run and reduces `run_bytes_after` from about 24.8 MB to about 16.5 MB.
   - Guardrail: after hot-overwrite maintenance, `run_bytes_after` should move
     toward Pebble's post-compact size instead of retaining both old and new
     versions.
4. HBC quantized/node writes inside the outer coalescing session.
   - Status: quantized deferral and staged HBC node-key publication done.
   - Current HBC mutation batches use normal mutable-state coalescing even when
     an outer bulk-ingest session is active. The remaining work is to stage
     touched quantized/node payload publication so small derived batches do not
     publish intermediate versions that survive as L0 debt.
   - Dense HBC bulk-new batches now set `defer_quantized_rebuild=true`
     regardless of microbatch item count. With an outer HBC bulk-ingest session,
     touched quantized nodes publish once at finish; without one, adapter HBC
     rebuilds only recorded touched nodes instead of falling back to a full
     tree rebuild.
   - HBC adapter now stages node header, centroid, member, child, and split-range
     writes/deletes during an outer bulk-ingest session. Reads consult staged
     node keys, and finish publishes each final node-key value once before
     quantized publication.
   - Guardrail: `active_hbc_quant_value_bytes` should stay close to
     `latest_hbc_quant_value_bytes` after a dense catch-up window.
5. Avoid sorted direct ingest for HBC namespaces unless the stream is final
   unique keys.
   - Status: initial code done.
   - LSM now has a `direct_bulk_ingest` option. Generic stores keep direct
     sorted ingest enabled by default, while the dense HBC LSM profile disables
     it. HBC mutation APIs also force mutation batches through default
     mutable-state coalescing inside outer sessions.
   - Guardrail: HBC mutation tests and write metrics should keep
     `sorted_ingest_runs=0` for online mutation/catch-up paths unless a future
     true bulk builder explicitly opts in.

6. Metrics/export.
   - Status: structured bench/log visibility improved; aggregate Prometheus
     LSM maintenance export done.
   - `lsm-write-bench`, `hbc-write-bench`, and dense HBC structured logs now
     expose overlapping L0 pressure along with existing logical/physical table
     bytes, compression blocks, run bytes, L0 counts, and HBC namespace write
     counters.
   - The data health endpoint exports cached write DB count and maximum LSM
     maintenance score so background LSM pressure is visible from `/metrics`.
     It also exports aggregate cached-write LSM totals for active run count,
     active run bytes, logical/physical table bytes, compressed/raw block
     counts, L0 runs/bytes, overlapping L0 pressure, lower-level runs/bytes,
     obsolete paths, active bulk-ingest batches, and compaction scheduler grant
     and denial counters.
   - The remaining Prometheus work is per-store/per-index families for run
     bytes, compaction input/output bytes, and HBC namespace write counters.

7. Validation.
   - Status: targeted 50k HBC guardrail run completed in memory storage.
   - The 50k x 128D HBC smoke run shows dense external vectors at about
     43 us/vector with `ns_nodes_put_calls=2424`, `ns_quant_put_calls=606`,
     `lsm_total_run_bytes=29.4 MB`, and `lsm_overlapping_l0_runs=12`.
     That confirms staged node/quantized publication is active at a larger
     HBC scale, while also showing deferred L0 overlap still needs maintenance
     after ingest.
   - The 2k-key hot-overwrite LSM guardrail still reports
     `overlapping_l0_runs_after=6` after the overwrite phase and then compacts
     15 L0 runs into one L1 run during maintenance, reducing active run bytes
     from about 24.8 MB to about 16.5 MB.

8. Scheduler follow-ups.
   - Status: remembered denied compaction candidates and bounded background
     execution added.
   - When the scheduler picks a compaction but capacity or resource accounting
     denies the grant, the backend now remembers the exact candidate run ids.
     The next maintenance round retries that candidate first, but only if the
     current run array still matches the remembered ids and ranges. If writes or
     another compaction changed the run set, the candidate is counted as stale
     and dropped before a fresh plan is selected.
   - DataServer now wakes a joined background LSM maintenance worker when cached
     write DBs report pressure. The worker runs bounded maintenance rounds and
     uses the existing write-cache mutex and scheduler grant path for conflict
     avoidance, so compaction state is not mutated concurrently with foreground
     writes.
   - Prometheus exports remembered candidate, retry, hit, stale, conflict-denial,
     and pending counters alongside the existing scheduler grant/denial metrics.
     It also exports background maintenance active/started/completed/failed
     counters from the data-server worker.

9. Write-optimized active memtable.
   - Status: first hash-indexed active memtable implemented.
   - The backend mutable table and write transaction overlays now use an
     `ActiveMemTable`: entries are kept in append/storage order with a hash
     index for point replacement, and sorted `State` materialization happens
     only when a sorted view is needed for flush, cursor snapshot, direct sorted
     ingest checks, or fallback WAL APIs.
   - WAL encoding now accepts the active table shape directly for the real LSM
     backend, so a no-WAL/default write path does not sort just because the
     backend has WAL support. Test-only backends that only expose
     `appendWalForState` still use the sorted fallback.
   - Same-size value replacements now copy into the existing allocation instead
     of allocate/free on every overwrite. This is a generic overwrite win and
     keeps hot-key batches from stressing the allocator when value sizes are
     stable.
   - Guardrail: `lsm-backend-test` includes active-memtable materialization
     coverage, and the hot-overwrite bench should keep final run entries
     coalesced to the live key count for each flush window.
   - 2026-04-16 smoke:
     `overwrite_rounds=20`, `keys=2000`, `hot_keys=500`,
     `batch_size=500`, memory/default storage reported
     `overwrite_hotset writer_ns=23316000`, `finalize_ns=954000`,
     `run_entries_after=2500`, and `run_bytes_after=1398060`.
     The result is close to the earlier move-apply path because this
     particular bench writes a sorted full hot set per transaction; the real
     benefit should show up more in random/replay batches where sorted inserts
     were doing mid-array shifts.
   - Remaining work: this is still a hash-indexed owned-entry table, not an
     arena memtable. A Pebble-like skiplist/arena would reduce per-entry
     allocation overhead further and could preserve ordered iteration without
     snapshot sorting, but it is a larger allocator/data-structure change.

10. Dense replay windowing and HBC CPU/debugging.
   - Status: targeted guardrail added and production path adjusted.
   - Why the earlier HBC benches missed the current VectorDBBench stall:
     default `hbc-write-bench` used 128D vectors, larger batches, and one outer
     HBC/LSM session. VectorDBBench sends 1536D vectors as serial 500-document
     HTTP chunks with `sync_level=write`.
   - A 50k x 1536D native HBC guardrail now includes
     `online_batches_dense_external_vectors_per_batch_session_empty`, which
     opens and finishes an HBC bulk session around each 500-vector chunk. This
     reproduces the bad shape:
     - one outer dense external-vector session: about 7.2s, about 315 MB active
       HBC LSM bytes, one quantized version per key.
     - per-500 session: about 34.9s, about 5.9 GB active HBC LSM bytes, about
       18 quantized versions per key.
   - The fix should not rely on benchmark chunk timing. Dense async replay now
     has a small group-commit window before catch-up so rapid weak-sync log
     appends can be applied in one `catchUpIndex` pass, with one outer HBC
     session and one final quantized/node publish before applied-sequence
     persistence.
   - The API write-cache auto-bulk idle/open window was widened so a serial
     client chunk that takes a few hundred milliseconds does not immediately
     close the HBC session before the next chunk arrives. This is a supporting
     policy, not the core correctness mechanism.
   - Public DB bulk-ingest begin/finish/abort now take the DB apply mutex before
     mutating dense index and store session state. Auto-window close can no
     longer race an async dense replay batch mutating the same HBC adapter.
   - Follow-up validation found the background LSM maintenance worker could
     monopolize cached-write DB locks/maintenance scoring after a create/index
     setup, causing the first VectorDBBench insert request to time out with only
     tens of MB on disk. The background path now uses an explicit outer
     scheduler: it checks a next-eligible timestamp, defers behind active
     bulk-ingest sessions, reserves `lsm_compaction_work` capacity before
     running, uses best-effort/nonblocking write-cache lock acquisition, and
     backs off after bounded work instead of immediately re-polling.
   - Remaining validation: rerun 50k VectorDBBench and confirm insert finishes,
     status endpoint remains best-effort during load, vector-index runs stay in
     the hundreds of MB rather than GBs, and samples no longer spend minutes in
     repeated HBC split/quantized publish windows.

11. Local production-shape guardrails.
   - Status: initial guardrail added to `dense-stack-bench`.
   - Full VectorDBBench should be final integration validation, not the first
     place we discover write-path regressions. The missing local coverage was
     the production envelope: 1536D vectors, 500-document chunks, weak-sync
     API/replay semantics, async derived catch-up, HBC session publication, LSM
     maintenance pressure, and status probes while ingest is active.
   - `zig build dense-ingest-guardrail` now runs `dense_stack_bench` in
     ingest-only mode with a VectorDBBench-shaped smoke:
     `--docs 5000 --dims 1536 --batch-size 500 --sync-level write`.
     It emits an `ingest_summary` JSON line with write wall time, final drain
     time, status probe latency, dense-index LSM run bytes/L0 runs/obsolete
     paths, and HBC write counters for grouped items, fallback items, recursive
     splits, quantized bytes, node bytes, vector bytes, and the main HBC timing
     buckets.
   - The guardrail has threshold flags so local/CI runs can fail on the exact
     classes of bugs VectorDBBench found:
     `--max-dense-lsm-run-bytes`, `--max-dense-l0-runs`,
     `--max-status-probe-ns`, `--max-write-ns-per-doc`, and
     `--max-hbc-quant-value-bytes`.
   - This complements the focused `hbc-write-bench` per-batch-session scenario.
     The HBC bench catches raw quantized/node publication debt; the dense ingest
     guardrail catches the DB/catalog/derived replay shape that can accidentally
     reintroduce that debt.
   - `zig build hbc-write-guardrail` runs the focused HBC version with 1536D,
     500-vector batches, and the per-batch-session scenario. `zig build
     vector-write-guardrails` runs both the HBC and dense ingest smokes.
   - Remaining local coverage to add: a `DataServer`/write-source level smoke
     that starts the real background LSM maintenance scheduler and polls the
     public status path during ingest. The direct DB guardrail measures status
     probe cost and LSM debt, but it does not yet exercise HTTP routing or the
     write-cache best-effort status adapter.

12. Plain-document extraction fast path.
   - Status: implemented for opaque JSON documents.
   - The first dense ingest guardrail run showed the local bottleneck was not
     HBC/LSM in the foreground write phase. Per-batch profile logs were
     dominated by `extract_writes_ns`, and HBC timing buckets were zero until
     final async drain. For VectorDBBench-shaped documents, the request path
     was fully materializing every `{"embedding":[1536 floats]}` JSON payload
     into `std.json.Value` even though there were no `_edges`, `_embeddings`,
     or `_summaries` fields to extract.
   - `document_mapper.extractWrite` now has an opaque JSON fast path for
     documents whose raw bytes cannot contain one of the special field names.
     It still validates JSON with the streaming scanner and stores the original
     bytes, but it avoids allocating/parsing a full JSON tree for ordinary
     documents.
   - Escaped field names stay on the full parser path because an escaped key can
     spell `_embeddings` without containing those literal bytes. Literal special
     field names anywhere in the payload also fall back to the full parser. The
     false-positive fallback is acceptable because correctness is more
     important than forcing the fast path.
   - Guardrail expectation: foreground `write_ns` for the 5k x 1536D dense
     smoke should no longer grow batch-by-batch with the size of JSON float
     arrays. If it does, the next likely layer is primary-store WAL/table bytes
     or HTTP body decoding rather than HBC.
   - Follow-up profile showed a second foreground cost under the same
     `extract_writes_ns` bucket: overwrite detection was calling
     `getStoreValue` once per written document. With the LSM backend, that meant
     opening/cloning a read snapshot for every key miss as the primary store
     grew. Batch write now opens one read transaction for the whole batch and
     probes existing document keys through that snapshot. This preserves update
     cleanup semantics while avoiding per-document LSM snapshot churn.
   - 2026-04-17 smoke on the 5k x 1536D guardrail:
     - before the batched read transaction: `write_ns=21188342000`,
       `write_ns_per_doc=4237668`, worst 500-doc batch `5474276000ns`.
     - after: `write_ns=1779491000`, `write_ns_per_doc=355898`, worst
       500-doc batch `192775000ns`.
     - Dense LSM remained bounded at about 62 MB active run bytes and two L0
       runs; status probes stayed under 15 us.
