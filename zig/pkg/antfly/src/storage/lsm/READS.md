# LSM, HBC, and Full-Text Read Performance

## Why Add Read Benchmarks

Write amplification work needs matching query-side guardrails. A lower-write path is not a win if it leaves more L0 runs, colder table indexes, larger HBC node walks, or more rerank vector loads on the first query after ingest.

Read benchmarks should make these costs visible:

- Point-read latency, miss latency, and run probes.
- Short-scan and full-scan latency.
- Reopen/cold-read behavior after persisted runs are published.
- Cache hits, misses, waits, used bytes, and entries.
- HBC nodes visited, leaves explored, approximate vectors scored, rerank vectors loaded, and metadata load time.
- Storage read calls and bytes by workload.

## Current Benchmark Targets

### LSM Backend Reads

`zig build lsm-backend-bench` already covers the core LSM read path.

Workloads:

- Sorted load setup.
- Warm point hits.
- Warm point misses.
- Warm short scans.
- Warm full scans.
- Reopen/open-only.
- Reopen point hit.
- Reopen point miss.
- Reopen short scan.
- Mixed read/write.

Metrics:

- Elapsed ns, ops/sec, and ns/op.
- Storage read file/range/trailer/file-size calls.
- `Backend.ReadStats`: point gets, run probes, bloom negatives, mutable/L0/level hits.
- Shared cache stats for raw tables, table indexes, and table blocks.

Useful commands:

```sh
zig build lsm-backend-bench -- --samples 3 --keys 100000 --value-size 128 --storage host --cache both
zig build lsm-backend-bench -- --samples 3 --keys 100000 --value-size 128 --storage memory --cache off
zig build lsm-backend-bench-compare -- --before /tmp/lsm-read-before.jsonl --after /tmp/lsm-read-after.jsonl
```

### HBC Query Reads

`zig build hbc-read-bench` benchmarks the dense-vector query path after building an HBC index.

Build modes:

- `bulk_build`: offline HBC build path.
- `online_coalesced`: replay-like known-new online batches with coalesced leaf writes.
- `both`: compare the read shape of both index construction paths.

Workloads:

- Cold first query after reopen, without metadata loading.
- Warm top-k queries without metadata loading.
- Warm top-k queries with metadata loading.
- Warm top-k queries with a metadata prefix filter.

Metrics:

- Elapsed ns and ns/query.
- Storage read file/range/trailer/file-size calls and read bytes.
- Search profile totals: setup, root load, node cache misses, quantized cache misses, child expansion, leaf scoring, rerank, rerank vector load, and rerank metadata time.
- Search work totals: nodes visited, leaves explored, approximate vectors scored, exact vectors scored, reranked vectors, candidates, and result count.

Useful commands:

```sh
zig build hbc-read-bench -- --samples 3 --vectors 100000 --dims 128 --queries 1000 --k 10 --batch-size 5000 --leaf-size 128 --storage native --build both
zig build hbc-read-bench -- --samples 3 --vectors 100000 --dims 128 --queries 1000 --k 50 --batch-size 5000 --leaf-size 128 --storage host --build online_coalesced
```

## Read-Side Questions To Answer

- Does the lower-write coalesced HBC path preserve query latency versus bulk-built indexes?
- Does leaving many L0 runs after bulk ingest measurably hurt point reads or scan reads before maintenance compaction?
- Are first-query costs dominated by table/index reads, HBC node cache misses, quantized payload loads, rerank vector loads, or metadata loads?
- Do metadata filters turn HBC search into a metadata random-read workload?
- What cache capacity is needed for stable query latency after a 1M-vector load?

## Near-Term Work

- Capture `hbc-read-bench` and `lsm-backend-bench` baselines under `bench/baselines/`.
- Add HBC read counters for namespace-level read calls and bytes, mirroring the write counters for `nodes`, `meta`, `quant`, and `vecs`.
- Add an LSM read benchmark mode that intentionally leaves deferred bulk-ingest L0 runs uncompacted, then compares read behavior before and after maintenance compaction.
- Add full-text query/segment read benchmarks beside the full-text write benchmark: term lookup, conjunction, top-k, cold reopen, post-merge, and corrupt-segment isolation.
- Add compare tooling for HBC read JSONL once the first baselines are stable.

## Read Improvement Task List

- LSM read visibility:
  - Keep the existing point-get, run-probe, bloom-negative, table-index, table-block, and cache counters in benchmark output.
  - Add a read benchmark mode that leaves bulk-ingest L0 runs uncompacted, then compares point reads, misses, scans, and reopen reads before and after maintenance compaction.
  - Break read stats down by namespace/store once per-store LSM profiles exist.
- LSM cache policy:
  - Size table indexes, bloom/filter blocks, raw table bytes, and data blocks under explicit byte budgets.
  - Surface cache pressure through the resource manager so first-query cache warmup cannot grow without bounds.
- HBC reads:
  - Add namespace-level read counters for HBC `nodes`, `meta`, `quant`, and `vecs`.
  - Compare `bulk_build` and `online_coalesced` query shapes across cold first query, warm no-metadata, warm metadata, and metadata-filter workloads.
  - Keep quantized payload representation tied to node root/non-root state; readers should never have to repair mismatched payloads.
  - Dense DB `search_effort` now resolves to an estimated HBC leaf budget instead of the legacy node-count budget for populated indexes. This should keep mid/low effort from authorizing near-full leaf scans on 50k-style indexes where the old mapping produced `search_width=1298` for about 438 leaves. Bench query logging now includes `estimated_leaves`; the next profile should compare `search_width`, `leaves`, and `approx_vectors`.
- Full-text reads:
  - Add a full-text segment query benchmark for term lookup, conjunction, top-k, stored-doc fetch, cold reopen, post-merge, and corrupt-segment isolation.
  - Add per-segment caches for term dictionaries/FSTs, postings blocks, stored-doc chunks, typed-doc-values chunks, and deleted-doc bitsets.
  - Move toward immutable segment searcher snapshots so readers pin a generation and never force writer or merge shutdown.

## Baseline Capture

Initial read baseline files should mirror the write baseline naming:

```sh
bench/baselines/hbc-read-native-100k.jsonl
bench/baselines/hbc-read-native-100k-noquant.jsonl
bench/baselines/lsm-backend-read-host-100k.jsonl
```

The HBC baseline should use native storage because first-query and rerank vector loads need real storage counters. The LSM backend baseline should use host storage with `--cache both` so the same run records cache-off and cache-on behavior with deterministic in-memory persistence plus storage-call counters.

Quantized HBC read capture now completes at 100k vectors for both `bulk_build` and `online_coalesced`. The previous 20k+ `error.EndOfStream` failure was stale quantized-node bytes: a bulk-built internal node could be saved as root-style nonquantized while its parent was still `0`, then reparented under a higher-level root without rebuilding that node's payload as RaBit. `updateParent` now refreshes internal quantized payloads when a node crosses the root/non-root representation boundary. Keep the no-quantized baseline as a comparison point for exact vector scoring and storage read shape without quantized payload loads.
