# LSM/HBC Write-Amplification Follow-Up Log (2026-04)

> Summary of a verbatim log. Full text: [docs/design/lsm-writes/history/follow-ups-2026-04.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/lsm-writes/history/follow-ups-2026-04.md). Originally relocated verbatim from `zig/pkg/antfly/src/storage/lsm/WRITES.md` (lines 299–1269 at commit 271838a195) on 2026-09-16; the living design is [`WRITES.md`](../../../../zig/pkg/antfly/src/storage/lsm/WRITES.md).

Dated follow-ups from 2026-04-16 (with later items through a 650K-document gate) chasing VectorDBBench 50k write regressions: slow inserts dominated by small-batch HBC commit/finalization, then benchmark roots growing to ~50GB (`NoSpaceLeft`). The work moved the Zig LSM toward Pebble's write shape — foreground writes publish bounded new state, background maintenance collapses version debt incrementally — and ended with accepted LSM generation-lifetime and admission fixes.

## Decisions
- **Normal API writes stay on the online path; no API auto-bulk.** Widening the write-cache auto-bulk window made the API own storage-engine policy, regressed inserts, and still let foreground callers pay L0 pressure. Normal provisioned writes bypass auto-bulk; sustained ingest windows belong to explicit internal producers (dense async replay, raft apply, snapshot backfill, restore, rebuild/import).
- **Outer HBC/LSM bulk-ingest session, but mutable-state coalescing for HBC mutations.** The session defers manifests and compaction across the window; HBC mutation transactions must not use direct `.bulk_ingest` sorted-run batches because they repeatedly rewrite node bodies, split ranges, vector-to-leaf mappings, and quantized payloads. LSM has a `direct_bulk_ingest` option: on for generic stores, off for the dense HBC profile. Direct sorted ingest is reserved for a future true HBC bulk builder emitting final unique keys.
- **Dense catch-up opens one HBC session around the whole `catchUpIndex` pass**; nested per-batch finishes do not publish deferred quantized payloads. Finish uses `ANTFLY_DENSE_CATCH_UP_MAX_DEFERRED_L0_RUNS` (default `4`) and `ANTFLY_DENSE_CATCH_UP_MAINTENANCE_STEPS` (default `8`); dense HBC obsolete retention is `0`. Dense async replay has a small group-commit window so rapid weak-sync appends apply in one pass.
- **HBC staging**: dense bulk-new batches set `defer_quantized_rebuild=true`; node header/centroid/member/child/split-range writes are staged during an outer session and published once at finish.
- **Compact binary embedding artifacts** instead of JSON float arrays (no legacy JSON compatibility; API still returns the same `_embeddings` shape); tracked in `ENRICHMENTS.md`. Async enrichment skips unchanged source hashes.
- **Byte-based flush thresholds and per-store LSM profiles**: primary docs `32 MiB`, text main/WAL metadata `16 MiB`, graph reverse indexes `16 MiB`, dense HBC `128 MiB` (bulk multiplier `4`, ~`512 MiB`). Adaptive per-block Snappy block compression as the first per-store policy (zstd/lz4 later, not a global switch).
- **Default write policy defers soft compaction to maintenance**: foreground does append/memtable/flush and hard L0 pressure only (`foreground_soft_compaction=true` for tests/specialized stores).
- **Maintenance architecture**: LSM picker decides what is useful, a dedicated LSM compaction scheduler decides capacity, and the resource manager admits a bounded `lsm.compaction_work` reservation — admission, not policy ownership. Denied candidates are remembered and retried if the run set is unchanged. A DataServer background maintenance worker defers behind active bulk sessions, uses nonblocking write-cache lock acquisition, and backs off (it had monopolized locks and timed out the first insert).
- **Iterator/block-based persisted compaction** streams input blocks and output entries; sequential compaction indexes keep only block windows/codecs/entry counts. Overlapping-L0 pressure scoring (≥4 overlapping runs) and dense HBC `compact_threshold_runs=8`; normalized `current / target` pressure ordering across sources; wide L0 closure drains toward half the trigger in one pass.
- **`ActiveMemTable`**: append-order entries with a hash index (one-entry buckets without an `ArrayList`), same-size value replacement in place, move-apply commit; capacity-based accounting for resources and a 2x memory guard, logical bytes for flush/run sizing.
- **Global LSM state admission via `ResourceManager`** against the shared `lsm.in_memory_state` budget before WAL append (two-level: DB waits on the usage epoch before the apply lock; backend re-checks; hard pressure is non-blocking `ResourceBudgetExceeded`). Immutable queues also have local byte caps: 256 MiB primary, 512 MiB dense, 128 MiB text/sparse/graph. Adaptive provisioning caps `lsm.in_memory_state` at 768 MiB with a 576 MiB soft boundary.
- **Read/scan snapshots pin only their captured immutable generations**; probe transactions copy results into transaction-owned storage.
- **Batch write opens one read transaction** for overwrite detection instead of a snapshot per document; `document_mapper.extractWrite` has an opaque-JSON fast path when raw bytes cannot contain special field names (escaped keys stay on the full parser).
- **Durable job reaping** partitions up to 4096 completed entries per pass with O(1) swaps; job payloads are freed by a once-only destructor right after `job.run`.

## Rejected approaches
- **API auto-bulk for weak-sync upload** — see Decisions.
- **Direct sorted ingest for HBC mutations** — compaction dropped to 0 but `sorted_ingest_runs` reached 249 and ~18.4GB before `NoSpaceLeft`; late 100-row batches wrote 100-190MB tables. Moved cost from compaction into disk amplification.
- **Foreground soft compaction** — in a keyed overwrite bench (5000 keys, 500 hot, 10 rounds, 64 KiB values) it rewrote `lsm_table_file_bytes=1805840080` for a 327MB overwrite phase and still left both versions active.
- **Relying on local immutable-queue caps alone** — the guarded million gate still reached 3.402 GB physical footprint; aggregate admission was needed.
- **Waiting inside backend admission while holding the DB apply lock** — deadlocked at the soft limit.
- **File-cache advice after compaction** — deferred: LSM tables use FD-backed range reads, so there is no mapping to `madvise`.

## Key measurements
- 50k VectorDBBench at `6f0b30c`: insert ~253s/256s, search p99 7.8ms, recall 0.9791, HBC commit 24.0s total. After `d6feb1b` (bulk flag without bulk behavior): ~286s insert, HBC LSM flush 20.8GB / table 39.6GB, compaction 74.6s.
- Vector-index directory at `9e9c136`: ~17GB = ~3.3GB active + ~14GB obsolete retained; ~10 versions per quantized node in active L0.
- Pebble hot-overwrite target shape: ~`1.30x` live bytes after overwrite, ~`1.04x` after compaction (2026-04-16 smoke). Zig after byte-window mutable state: ~`1.37x`. 20-round smoke: Zig writer 23894000 ns vs Pebble 27957334 ns.
- Keyed overwrite (5000/500/10, 64 KiB): Pebble `sst_bytes` 328064934 → 401278646 → 328059136 after compact; Zig `bulk_ingest` one 32MB overwrite run, `run_bytes_after=361169040`, no compaction.
- 50k × 1536D HBC per-batch-session guardrail: one outer session ~7.2s, ~315 MB active, one quantized version per key; per-500 session ~34.9s, ~5.9 GB, ~18 versions per key.
- 5k × 1536D dense ingest smoke (2026-04-17), batched read transaction: `write_ns_per_doc` 4237668 → 355898; dense LSM ~62 MB, status probes under 15 us.
- 980,610-document runs: sequential compaction cursors cut primary-compaction peak from ~6.4 GB to ~1.70 GB; several full gates were stopped/rejected (6.075 GB, 2.647 GB, 3.402 GB, 3.004 GB footprints).
- **Final 650K gate (accepted)**: 409.261 s vs 542.442 s; peak physical footprint 3.107 → 1.486 GB, peak RSS 3.452 → 2.804 GB, peak `lsm.in_memory_state` 607.0 → 355.8 MB. Remaining RSS attributable to full-text mmap residency and allocator retention.

## Guardrails introduced
- `antfly_bench_hbc_lsm_write` log line (flush, sorted-ingest, compaction, manifest, table-write deltas per HBC batch).
- `bench/vectors/hbc_write_bench.zig` dense external-vector scenarios and `hbc_quant` active-vs-latest bytes; healthy `hbc_quant_versions_per_key_bps` stays near `10000`.
- `bench/storage/lsm_write_bench.zig` `hot_overwrite` scenario (with `--readers`, `writer_ns`/`finalize_ns`) and the Pebble helper `compat/pebble_overwrite_go` (use `--value-pattern keyed`).
- `storage_bench ingest` VectorDBBench-shaped dense guardrail with threshold flags (`--max-dense-lsm-run-bytes`, `--max-dense-l0-runs`, `--max-status-probe-ns`, `--max-write-ns-per-doc`, `--max-hbc-quant-value-bytes`); workloads in [BENCHMARKS.md](../../../../zig/BENCHMARKS.md#vector-write-regression-workloads).
- `/metrics` exports aggregate cached-write LSM totals, maintenance score, scheduler grant/denial and remembered-candidate counters, and background maintenance counters.

## Open items (as recorded)
- Per-store/per-index Prometheus families for run bytes, compaction input/output bytes, and HBC namespace write counters.
- A `DataServer`/write-source smoke exercising the real background maintenance scheduler and public status path during ingest.
- Arena/skiplist memtable instead of the hash-indexed owned-entry table.
- Stored-value compression (zstd) for primary documents and chunk artifacts.
- `NoSpaceLeft` must propagate as a failed write and allow clean shutdown, not panic in `text_merge_runtime.deinit()`.
- Rerun 50k VectorDBBench to confirm insert finishes and vector-index runs stay in the hundreds of MB.
