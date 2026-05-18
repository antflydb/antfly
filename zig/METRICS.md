# Antfly Metrics and Profiling

This document covers the current runtime metrics, benchmark-only profiling logs,
and status-count semantics. Metrics intended for operators should be Prometheus
compatible and low overhead by default. Benchmark profiling should stay opt-in.

## Data Server Metrics

`DataServer` exposes health/metrics through its health source.

Current Prometheus metrics:

- `antfly_data_server_up`: gauge, `1` when the data server process is running.
- `antfly_lsm_cache_used_bytes`: gauge for resident bytes in the shared LSM cache.
- `antfly_lsm_cache_entries`: gauge for shared LSM cache entry count.
- `antfly_lsm_cache_hits_total{kind="..."}`: counter per cache kind.
- `antfly_lsm_cache_misses_total{kind="..."}`: counter per cache kind.
- `antfly_lsm_cache_inserts_total{kind="..."}`: counter per cache kind.
- `antfly_lsm_cache_evictions_total{kind="..."}`: counter per cache kind.
- `antfly_lsm_cache_invalidations_total{kind="..."}`: counter per cache kind.
- `antfly_lsm_cache_waits_total{kind="..."}`: counter per cache kind.

Current LSM cache kinds:

- `run_state`
- `run_table_raw`
- `run_table_index`
- `run_table_block`

## Resource Budget Metrics

`DataServer` exports labeled metric families for resource-manager slices.

Current slices:

- `lsm.block_table_cache`
- `hbc.node_metadata_cache`
- `full_text.pending_segments`
- `derived.backlog`
- `text_merge.buffers`

Each slice exports:

- `antfly_resource_used_bytes{slice="..."}`: gauge for currently accounted bytes.
- `antfly_resource_peak_bytes{slice="..."}`: gauge for peak accounted bytes.
- `antfly_resource_soft_limit_bytes{slice="..."}`: gauge for configured soft limit.
- `antfly_resource_hard_limit_bytes{slice="..."}`: gauge for configured hard limit.
- `antfly_resource_soft_limit_events_total{slice="..."}`: counter for soft-limit crossings.
- `antfly_resource_hard_limit_rejections_total{slice="..."}`: counter for hard-limit crossings/rejections.
- `antfly_resource_pressure{slice="..."}`: gauge, `0 = normal`, `1 = soft`, `2 = hard`.

The shared LSM cache is sized from the node/pod memory budget and mirrored into
the `lsm.block_table_cache` hard limit, so cache metrics and resource-pressure
metrics should be interpreted together.

## Status Counts

`DB.stats().doc_count` is optimized for status/reporting. For v1 it uses the
query-visible full-text count when a full-text index is available:

- It reads `PersistentIndex.snapshot().global_doc_count`.
- With multiple full-text indexes, it uses the maximum visible full-text count.
- This avoids scanning the primary docstore during status collection.

This is intentionally an indexed/query-visible count, not a canonical primary
docstore cardinality. Tables without a usable full-text index still fall back to
the primary docstore scan. The long-term exact solution is a persisted primary
doc-count counter maintained by the write path.

## Batch Write Profiling

`DB.batchProfiled` exposes `BatchProfile` for benchmark and diagnostic callers.
`DB.batch` also logs the profile when batch metrics are enabled.

Environment toggles:

- `ANTFLY_BENCH_METRICS=1`
- `ANTFLY_BENCH_BATCH_PROFILE=1`

The `antfly_bench_batch` log line includes:

- request shape: writes, deletes, graph writes/deletes, transforms, sync level
- total wall time
- phase timings for transform resolution, effective-request merge, predicates,
  range validation, write extraction, artifact deletion, generated enrichment
  precompute, store write, split delta, derived-log construction, shadow apply,
  sync target collection, derived-log append, sync wait, and enrichment notify

These toggles are cached once per process so they do not add `getenv` overhead
to every write batch.

## Dense HBC Write Profiling

HBC write profiling is benchmark-only and logs through `std.log`.

Environment toggles:

- `ANTFLY_BENCH_METRICS=1`
- `ANTFLY_BENCH_HBC_WRITE_PROFILE=1`
- `ANTFLY_HBC_COALESCE_BULK_WRITES=0|1`

The HBC write logs include:

- `antfly_bench_hbc_write`: batch shape, bulk flags, wall time, active/vector
  counts, and node counts.
- `antfly_bench_hbc_write_counts`: insert, split, range-write, and namespace
  write counters.
- `antfly_bench_hbc_write_timing`: phase timings for vector transforms, leaf
  lookup, vector storage, quantized rebuild, node writes, split/update work,
  commit, flush metadata, and bulk-build phases.

`ANTFLY_BENCH_HBC_TREE=1` or `ANTFLY_BENCH_HBC_TREE_EVERY=N` enables periodic
tree-shape logs:

- `antfly_bench_hbc_tree`: total/internal/leaf node counts, max level, and leaf
  member percentiles.

The HBC env toggles are cached once per process.

## Dense Query Profiling

Dense query profiling can be requested explicitly through the API/profile path,
and benchmark log sampling can be enabled with:

- `ANTFLY_BENCH_QUERY_PROFILE=1`
- `ANTFLY_BENCH_QUERY_PROFILE_EVERY=N`

The sampled benchmark logs include:

- `antfly_bench_dense_query`: index name, k/limit/offset, effort, index size,
  resolved search width/epsilon, total time, HBC time, doc-key resolution,
  projected-document load, postprocess time, raw hits, and returned hits.
- `antfly_bench_dense_query_hbc`: HBC node/leaf visits, approximate/exact vector
  scoring, rerank counts, ambiguity counters, threshold/full-rerank flags,
  rerank vector-load and distance timings, and inline/fetched metadata hits.

The query env toggles are cached once per process.

## Dense Stack Benchmark Profiles

`bench/vectors/dense_stack_bench.zig` supports `--profile <path>`.

When set, each write batch emits one JSON line with:

- batch index and document range
- dataset shape and configured batch size
- wall time
- all `BatchProfile` phase timings

This file is intended for offline comparison of batch-size, HBC, LSM, and
derived replay behavior. It is not a server runtime metric endpoint.

## Guidelines

- Operator metrics should be cheap, always safe to scrape, and Prometheus
  compatible.
- Benchmark diagnostics should remain opt-in and may use logs or JSONL.
- Do not put environment-variable checks directly in hot paths without caching.
- Do not derive canonical table cardinality from secondary indexes. The current
  full-text doc-count shortcut is a v1 status optimization only.
