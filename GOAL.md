# Vector Load and Query Performance Goal

## Objective

Make Antfly's vector ingest path competitive on large VDBBench-style loads while staying disciplined about memory and disk IO.

The target is not just "fast upload" or "fast query" in isolation. The durable target is:

- High serial upload throughput without unbounded foreground stalls.
- Low post-load optimize/readiness time.
- Stable query latency after load, including fresh readonly/query handles.
- Low disk write amplification and bounded on-disk run/manifest growth.
- Explicit memory budgets for any optimization that trades RAM for speed.

## Current Baseline

The main comparison case is VDBBench `Performance768D1M`, Cohere 1M, 768 dimensions, cosine, `k=10`, concurrency `[1, 5]`.

Recent observed runs:

| Label | Load | Insert | Optimize | QPS | Serial p99 | Recall | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `antfly-memory-guard-rerun` | 950.735s | 628.441s | 322.295s | 185.869 | 23.0ms | 0.9899 | Best load so far, but left heavy LSM debt. |
| `antfly-knownbest-clean-1m-44924e58` | 1278.402s | 675.368s | 603.035s | 43.033 | 80.7ms | 0.9797 | Clean rollback run; poor query shape. |
| `antfly-e24a9140-cohere-1m` | 1286.462s | 712.267s | 574.195s | 238.953 | 16.5ms | 0.9899 | Current PR shape; better query than clean rollback, not best load. |

The 50k sanity case on `e24a9140` was healthy:

| Case | Load | Insert | Optimize | QPS | Serial p99 | Recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `Performance1536D50K`, `k=100` | 31.086s | 16.431s | 14.655s | 1294.794 | 3.1ms | 0.9811 |

## What We Know

- The biggest remaining failure mode is primary table LSM run debt, not dense HBC search logic.
- The latest 1M run ended with about 1319 primary table run files and a 26G data root.
- A previous clean 1M run had an 88M primary table manifest and slow query-worker readonly opens around 6.8s.
- HBC/dense index state was comparatively small and usually looked ready or nearly drained when query shape was poor.
- Table status now exposes a richer `storage_status.lsm` snapshot for benchmark artifacts: total/L0/lower-level run shape, compactable and overlapping L0 pressure, configured L0 limits, write-stall debt, overflow debt, obsolete path count, current manifest bytes, active bulk/readers, dirty manifest flags, maintenance score, and maintenance debt hint.
- Full `/metrics` scraping can perturb runs because metrics collection may walk LSM maintenance stats. Use sparse sampling or targeted artifacts during benchmarks.
- The current VDBBench adapter defaults to `/db/v1`; Antfly v0.1 used `/api/v1`.

## Tradeoffs Already Tried

### Kept

- `ingest_compact` in `lsm-write-bench`.
  This gives a focused storage workload for random ingest plus full compaction drain before changing the end-to-end harness.

- Bulk current-scan mutable cloning with a memory cap.
  This avoids rotating mutable state into extra runs for some bulk current scans, but it must remain budgeted and visible in metrics.

- Document-data LSM level shape:
  primary, text main/WAL, sparse, and graph reverse use 128 MiB L1 target with 10x multiplier; dense HBC uses 256 MiB L1 target with 10x multiplier.
  Isolated storage smoke improved table bytes and max level substantially:
  stock 1 MiB/8x wrote about 650 MB and reached level 4; 128 MiB/10x wrote about 282 MB and reached level 1 on the same 100k ingest-compact smoke.

- Enrichment embedding batch metrics.
  These help separate API/upload stalls from embedder-side latency when load stalls happen in enriched tables.

### Reverted Or Avoided

- Broadly raising L0 run/byte limits as a production default.
  It can make upload look better by deferring pain, but it allows too much read/open debt.

- Disabling table block compression.
  This is not acceptable as a general fix because disk IO and footprint matter.

- Aggressive dense catch-up maintenance cadence.
  It improved some query shape but hurt insert throughput. Cadence alone is not the right fix.

- API-layer "optimize the index" coupling.
  Index maintenance should not leak into the API as a special-case readiness hack. The storage layer should own run publication, compaction budgeting, and drain policy.

## Working Theory

The root cause is compaction shape and run publication policy.

The old global LSM defaults were sized for tiny stores: `level_target_bytes_base = 1 MiB`, multiplier `8`. Large document stores then descend through too many levels, causing unnecessary rewrite amplification.

The current end-to-end issue is sharper: auto-bulk/dense ingest flushes primary table data and can leave too many primary L0/table runs behind. Background maintenance exists, but the load/readiness path can reach query execution with a large primary manifest and many table runs. Query workers then pay read/open costs even if dense HBC itself is ready.

## Constraints

- Memory overhead must be explicit and bounded.
  Bigger memtables or clone buffers are acceptable only as configurable profile knobs with metrics.

- Disk IO must stay low.
  We should prefer fewer rewrites, fewer manifests, and fewer transient table files over hiding work behind larger tolerances.

- Query latency includes open/readonly handle cost.
  It is not enough for steady-state vector traversal to be fast if each query worker opens through a huge primary manifest.

- Bulk-load improvements must preserve online write behavior.
  A large offline load should not make normal small writes or internal stores worse.

## Roadmap

### 1. Make The Debt Visible

- Add or expose source/primary LSM run count, L0 count, manifest size, and maintenance score in benchmark artifacts without requiring expensive full metrics scraping.
  Current status: table status exposes the LSM run/pressure shape, current manifest bytes, and score fields.
- Break down readonly DB open profiling inside `openCoreResourcesFromPrimaryStore`, especially manifest load, range/shard/schema reads, and index manager open.
- Save final primary and dense index LSM shape summaries after VDBBench load.

Success condition:
Every benchmark result can answer: how many primary runs existed at query start, how large was the primary manifest, and how long did readonly open spend on storage.

### 2. Fix Primary Run Publication Policy

- Audit auto-bulk finish options for primary table writes.
- Avoid creating hundreds or thousands of primary L0 runs during serial HTTP upload.
- Prefer storage-owned policy over API/index-specific optimize calls.
- Evaluate a bounded foreground drain at safe publication points:
  - max compaction steps,
  - max input bytes,
  - max elapsed time,
  - target L0 run count or manifest-size threshold.

Success condition:
1M load reaches query phase with bounded primary run count and no multi-second readonly open.

### 3. Tune LSM Shape With Isolated Evidence

- Use `lsm-write-bench --workload-set ingest_compact` to sweep:
  - level target base and multiplier,
  - flush threshold bytes,
  - max run file bytes,
  - compaction input byte budget.
- Keep tiny internal stores on small defaults.
- Keep document/data profiles explicit.

Success condition:
Lower write amplification and max level without raising steady-state memory by default.

### 4. Add Memory-Budgeted Speed Knobs

- Treat larger memtables as an optional deployment/profile knob, not the default first fix.
- Track peak mutable, immutable, clone, compaction scratch, cache, and HBC memory.
- Make clone and compaction budgets visible in metrics and benchmark logs.

Success condition:
Any load-speed improvement comes with a measured peak RSS and tracked in-process memory budget.

### 5. Validate End To End

Run in this order:

1. `lsm-write-bench ingest_compact` storage smoke.
2. VDBBench `Performance1536D50K`, `k=100`, concurrency `[1,5]`.
3. VDBBench `Performance768D1M`, `k=10`, concurrency `[1,5]`.
4. Only then run broader concurrency or alternate datasets.

Primary targets for the 1M Cohere case:

- Load below 950s.
- Query QPS above 275 with serial p99 below 20ms.
- Primary table run count at query start below 128 as an initial target.
- No multi-second readonly DB open.
- Disk root size materially below the current 26G result.

## Open Questions

- Should auto-bulk primary writes use a small bounded foreground compaction budget at each window close, or should the storage layer maintain a separate post-load drain before reporting data/index readiness?
- Can sorted-run ingestion be used for more of the primary VDBBench upload path, or do random doc keys prevent that without a staging/sort phase?
- Is the primary manifest large because of run count alone, or are obsolete path records and repeated manifest publications contributing materially?
- What is the right default max run file size for primary document payloads under this workload?
- Should query workers reuse/pool readonly handles during benchmark/concurrent search, or is per-worker open cost representative enough that storage must solve it?
