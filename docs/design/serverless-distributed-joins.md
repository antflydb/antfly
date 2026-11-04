# Serverless multi-node SQL joins

Status: native broadcast/shuffle execution and shared recovery contracts are
implemented as opt-in host capabilities in `zig/pkg/antfly/src/serverless/sql`.
See that module's README for integration, supported lowering and remaining
strategies. Bucket/adaptive execution and Cloudflare bindings below remain
proposals. The scale target, up to roughly a trillion rows, is not a qualified
performance claim.

## Where joins run today

Default SQL execution runs joins inside one process; the opt-in distributed
bridge now handles supported direct lake equijoins:

- `sql/relation_runtime.zig` starts with an in-memory hash join. When the
  build side exceeds three quarters of `retained_bytes` (256 MiB by default),
  it escalates to the grace hash join in `sql/partition_join.zig`. That join
  uses up to 16 partitions, repartitions on remaining hash bits, and falls back
  to typed block nested loops in `sql/skew_join.zig` for keys it cannot split.
- Spill goes to private local files with a 1 GiB default quota. WASM has no
  spill at all (`native_spill_supported`), so Cloudflare readers would fail any
  join that exceeds memory.
- Parallelism is threads only: one global scheduler with 8 CPU and 32 I/O
  workers.
- The build side is chosen by estimated bytes. Join order is a bounded
  left-deep search with at most 8 leaves.
- Per-statement caps (`scan_rows` 10M, `spill_bytes` 1 GiB) put trillion-row
  joins far out of reach.

Pieces that already exist and that a distributed design can reuse:

| Piece | Where | Reuse |
|---|---|---|
| Grace hash join with skew fallback | `sql/partition_join.zig`, `sql/skew_join.zig` | The per-partition join kernel on every reduce task |
| Dynamic filters (bloom, min/max domain, up to 4,096 exact keys) | `sql/dynamic_filter.zig`, `lake_cursor.setDynamicFilter` | Prune the probe side's files and row groups before shuffling it |
| Iceberg `bucket[N]` with the spec's Murmur3 hash | `serverless/query/lake_partition_pruning.zig` | Co-partitioned joins without a shuffle |
| Row-group scan units | `serverless/query/lake_stream.zig` (`ScanWork.Unit`) | Map-task splits |
| Versioned aggregate partials (`AGS\x02`) | `sql/aggregate_partial.zig` | Combine aggregates computed above a join across tasks |
| Fenced work leases | `serverless/build/work_lease.zig` | Task ownership and retries |
| Snapshot pins with renewable leases | `serverless/lake_snapshot_pins.zig` | Keep every task on one Iceberg snapshot while vacuum runs |
| Expiring objects | `api/lake_expiring_objects.zig` | Clean up exchange data |
| Distributed JSON joins: broadcast at 10 MiB, 8 shuffle partitions, durable shuffle leases | `api/distributed_join.zig`, `zig/JOINS.md` | Strategy thresholds and lease lifecycle, proven in Raft mode |

The native execution module now supplies serializable fragments, typed exchange,
and durable coordinator/task ownership. Join planning from partition specs
remains strategy work.

## Design principles

These follow PostHog's lessons from leaving DuckDB:

1. **Resource use is predictable.** The coordinator sizes a query from
   manifest statistics before it runs. Users never size workers. A query that
   exceeds its tenant budget fails fast with an explicit estimate, not an
   out-of-memory error halfway through.
2. **Object storage is the exchange.** Workers stay stateless. Intermediate
   data is immutable, content-addressed and garbage-collected by expiry. A lost
   worker costs a task retry, not the whole query.
3. **Pick the cheapest strategy that fits.** Most interactive lake joins are
   star schemas or bucketed tables, and neither needs a shuffle.
4. **Know when to delegate.** Multi-stage ETL and very large many-to-many
   shuffles belong to Trino or Spark over the same Iceberg tables. Antfly
   should say so in the error rather than attempt them badly.

## Strategy ladder

The coordinator evaluates strategies in this order, using manifest row and
byte counts, column statistics and partition specs.

### 1. Local

Both sides fit one worker's budget. This is today's path, unchanged.

### 2. Broadcast

Use when the build side's estimated live bytes are at most a broadcast
threshold, initially 64 MiB on native workers and 16 MiB on WASM.

- The coordinator, or one task, builds the build side once and publishes it as
  an immutable broadcast artifact: typed column blocks in the existing spill
  block format plus a serialized dynamic filter.
- Probe tasks each take a range of probe-side row-group units. When join
  semantics permit pruning this input, the task installs its dynamic filter
  before the cursor starts, so min/max domains
  prune files and row groups and bloom or exact keys filter batches. It then
  probes locally and emits result pages or aggregate partials. Install pruning
  filters only on inputs whose unmatched rows the join may discard. A left
  outer join must retain every unmatched left row; right and full outer joins
  preserve their corresponding inputs. Anti joins must retain keys absent from
  the other input. Re-evaluate these rules after choosing physical build/probe
  sides, and keep NULL-key rows whenever the join must emit them.
- This covers star schemas, the dominant analytical join. The dynamic filter
  often removes most of the fact table before any join work happens.

### 3. Co-partitioned (bucketed)

Use when both sides are Iceberg-partitioned by `bucket[N]` on the join keys,
with bucket counts where one divides the other.

- Task *i* joins bucket *i* of A with the matching buckets of B. There is no
  shuffle. File pruning per bucket already exists. The planner only needs to
  read partition specs for joins, not just for pruning.
- For Antfly-written tables, add an optional bucketed write spec on declared
  join keys. Today the native writer uses an unpartitioned spec for global key
  deletes. A bucket spec on the primary key keeps equality deletes local to
  each bucket, so both requirements can be met.

### 4. Shuffle hash join

This is the general case.

**Map stage.** Each map task scans a split of one side, applies pushed-down
filters and any semantically safe dynamic filter, and hash-partitions rows into
*P* partitions by join key. It writes **one** exchange object per task. The object holds all *P* partitions as contiguous typed block
runs and ends with a footer index of partition offsets, row counts and byte
counts.

- One object per task, rather than one per task and partition, keeps
  object-store requests at *M* PUTs and *M × P* range GETs instead of *M × P*
  objects.

**Reduce stage.** Reduce task *p* range-reads partition *p* from every map
output of both sides and runs the existing grace hash join locally. When a
partition is still too large for one task, the join recursively repartitions
through local spill, or through object storage on WASM.

**Choosing P.** P is a power of two:
`P = next_pow2(estimated_build_bytes / (0.5 × task_budget))`, clamped to a
tenant maximum.

**Skew.** Column statistics already carry top-4 frequency hashes, and map
footers report per-partition sizes. When one partition is far larger than the
median, the coordinator splits its probe side across several reduce tasks and
replicates the matching build-side rows to each of them. For joins that emit
unmatched build rows, reducers report matched row identities and a single
owner emits the globally unmatched rows after combining those reports; local
unmatched decisions on replicated inputs would produce incorrect duplicates.

### 5. Delegate

Reject the query with an explicit `SqlJoinBudgetExceeded` error. The error
carries the estimated shuffle bytes and task count, and recommends an external
engine over the same Iceberg tables. This is the documented boundary in
`zig/LAKES.md`, made visible to users.

## Execution model

- **Fragments.** Define a small, versioned fragment IR rather than serializing
  `relation_binding.Node`. It covers scan, filter, project, dynamic-filter
  install, partial aggregate, hash-partition sink, exchange source, local join
  and result sink. Tasks run fragments with the existing operators. The IR
  carries the pinned snapshot ID, the schema fingerprint and the credential
  scope, so a task cannot read anything the query was not authorized for.
- **Coordinator.** One coordinator per query. On Cloudflare that is a Durable
  Object; elsewhere it is any query worker holding a lease. It plans, pins the
  snapshot, dispatches tasks, tracks completion and assembles results.
- **Tasks are idempotent.** A task attempt writes its output under
  `query/{id}/stage/{s}/task/{t}/attempt/{a}` and commits by conditionally
  creating a small task manifest. The first committed attempt wins, so retries
  and speculative duplicates are safe. Fencing tokens from `work_lease.zig`
  stop a stale attempt from committing after its lease moves.
- **Adaptive replanning.** Map footers give actual sizes before the reduce
  stage starts. The coordinator can then:
  - switch to broadcast when the build side turned out small
  - coalesce small partitions
  - split skewed partitions
- **Aggregates above joins.** Reduce tasks emit `AGS\x02` partials. The
  coordinator merges them, or a fan-in-32 tree does for very wide shuffles.
- **Results.** Small results stream through the coordinator. Large results are
  written as result pages to object storage and exposed through the existing
  cursor and continuation contracts.
- **Consistency.** The coordinator captures a pinned base snapshot and an
  accepted WAL cut for each input, and retains both for the entire query.
  Snapshot pins keep vacuum from collecting base files; WAL retention protects
  each overlay until all tasks finish. Apply accepted inserts, replacements and
  deletes on every input before that input's predicates and limits. In
  particular, overlay the build input before broadcasting it or constructing
  dynamic filters; overlay the probe input before applying those filters.
  Bucket and shuffle tasks route mutations with their input's partitioning and
  suppress replaced base rows before exchange. Retries use the same snapshot
  and WAL cut. Admission accounts for overlay bytes and requests across all
  inputs: the existing 64 pending-record bound is per input, not a query-wide
  byte or request bound.
- **Cleanup.** Exchange objects carry an expiry and are reclaimed by the
  expiring-objects collector, even when a coordinator crashes.

## Admission and limits

- **Estimate first.** Before any task starts, the coordinator computes scan
  bytes, shuffle bytes, task count and object-store requests from metadata
  alone. Planning does not fetch Parquet footers today, so these estimates
  stay cheap.
- **Tenant budgets.** Budgets cap concurrent tasks, shuffle bytes and requests
  per query. Over budget means a fast, explicit rejection.
- **Per-task limits replace per-statement limits.** `scan_rows`,
  `retained_bytes` and `spill_bytes` apply to each task. The query-level total
  is the sum across tasks and is checked against the tenant budget.

## Cloudflare profile

- **Memory.** WASM isolates have no local spill and about 128 MiB of memory.
  Reduce tasks must fit in memory, so *P* is chosen against a smaller task
  budget. Recursive repartitioning goes through R2.
- **Dispatch.** Map and reduce tasks travel on a dedicated Queue with bounded
  concurrency. The coordinator Durable Object tracks task manifests.
- **CPU.** The current builder limit is 30 s of CPU per invocation, so tasks
  are sized by row-group units to finish well inside it. Splits are
  independent, so a long scan becomes more tasks.
- **Requests.** One exchange object per map task with footer-indexed range
  reads keeps R2 request counts linear in *M × P*.

## Fixes needed regardless of distribution

These came up while mapping the code and help single-node joins too:

1. A dynamic filter's min/max domain cannot prune `bucket[N]` partitions,
   because bucket pruning only accepts equality. An exact key set of at most
   4,096 keys can be turned into a bucket set and prune whole buckets.
2. Iceberg row-group predicates come from plan-time `file_predicates`, so
   dynamic-filter ranges may not reach row-group pruning on that path. This
   needs verification and a fix if confirmed.
3. The planner reads partition specs only for pruning. Join planning needs
   them to detect co-partitioning.
4. Sort orders are written but never read by the planner. Reading them would
   allow merge joins and ordered results on sorted tables.

## Phases

1. **Broadcast joins for lake SQL.** Fragment IR v1, broadcast artifact,
   probe-task fan-out on native workers, and fixes 1–2. This has the largest
   payoff (star schemas) and the smallest surface.
2. **Shuffle hash join.** Exchange object format, coordinator with fenced
   tasks, reduce stage reusing `partition_join`, and admission estimates. This
   runs on native and Kubernetes query workers first.
3. **Co-partitioned joins.** Bucket-aware planning (fix 3) and an optional
   bucketed write spec for Antfly-managed tables.
4. **Adaptive execution.** Runtime replanning from map footers, skew
   splitting, and partition coalescing.
5. **Cloudflare.** Memory-only reduce tasks, R2 exchange, Queue dispatch and a
   Durable Object coordinator.

## Validation

- **Correctness.** Run SQL parity suites for inner, left, right, full, semi
  and anti joins. Compare every strategy against the local path on the same
  pinned snapshot, including NULL keys and duplicate-heavy keys. Explicitly
  test unmatched preserved rows with dynamic filtering enabled, anti joins with
  absent keys, physical side swaps, and outer joins with replicated skew inputs.
- **Fault injection.** Use the simulation harnesses to kill workers mid-map
  and mid-reduce, lose a task manifest response, and expire a lease during a
  commit. Results must be identical and no exchange object may leak past
  expiry.
- **Scale.** A synthetic star schema with a 10^9 to 10^12 row fact table and
  zipfian keys. Report bytes shuffled, object-store requests, task counts,
  per-task peak memory and latency distributions against tenant budgets.
