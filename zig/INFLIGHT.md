# Inflight Batching Plan

## Goal

Reduce dense derived apply overhead by cutting the number of expensive apply and commit boundaries during replay/catch-up.

The current profile still shows these hot buckets:

- `flushMutable`
- `persistManifest`
- `Cache.invalidatePath`
- `NamespaceWriteTxn.commit`
- `state.mergeStates`

The common pattern is that dense derived apply is still finalizing too often.

## Why Not Add Another WAL

The Go repo uses `go/pkg/antfly/lib/inflight/WALBuffer` to combine:

- durability
- microbatch scheduling

That makes sense there because the per-index worker path owns both concerns.

In Zig, we already have durable replay state in the derived log:

- `pkg/antfly/src/storage/db/derived/derived_log.zig`
- `pkg/antfly/src/storage/db/derived/derived_worker.zig`

So the first inflight step here should only add microbatching. We should not add a second per-index WAL layer until measurement shows the existing derived log is insufficient.

## Placement

This should not live in `go/pkg/antfly/lib/inflight/` yet.

The batching rules are still tightly coupled to the storage/db pipelines:

- replay/catch-up batching depends on `DerivedBatch` semantics
- enrichment batching depends on generated-enrichment request semantics
- merge behavior is index-kind specific

So the first reusable shell belongs under storage/db itself:

- `pkg/antfly/src/storage/db/batcher.zig`

If sparse/full-text/enrichment/graph all converge on a truly generic queue and
flush contract later, we can promote that smaller core to `go/pkg/antfly/lib/`.

## First Narrow Implementation

Scope:

- dense-vector indexes only
- replay / catch-up path only
- existing durability unchanged

Implementation shape:

1. `derived_worker.catchUpIndex()` accumulates consecutive dense derived records for a managed dense index.
2. The worker flushes that accumulator when one of these provisional thresholds is hit:
   - source record count
   - dense embedding count
   - end of replay loop
3. Flush applies one merged `DerivedBatch` through the existing apply callback.
4. Other index kinds keep the current one-record-at-a-time behavior.

This first step is intentionally narrow. It avoids:

- a new generic inflight framework
- a second WAL
- timer-driven background batching
- cross-index coalescing

## What This Should Improve

- fewer calls into `applyDerivedBatchToIndexContext`
- fewer `bulk_ingest` batch lifetimes
- fewer LSM commit / flush / manifest reconciliation points
- lower `NamespaceWriteTxn.commit` frequency

## Expected Follow-Ups

If this first step helps:

1. Add a longer-lived DB-level dense apply scope so multiple replay flushes share one ingest lifetime.
2. Reuse the same batching shell for sparse/full-text/graph with index-specific merge behavior.
3. Add enrichment-side request coalescing on the existing worker path.
4. Only then consider a shared inflight framework or `go/pkg/antfly/lib/` promotion.

If this first step does not help enough:

1. Apply the same batching idea higher up, before replay flushes become DB apply calls.
2. Revisit in-place application of LSM overlay state on commit.

## Initial Thresholds

The first implementation should use conservative hard-coded thresholds and re-measure:

- max source records per merged dense apply
- max dense embedding writes per merged dense apply

Those can be tuned or made configurable later once the profile moves.
