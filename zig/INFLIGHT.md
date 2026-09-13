# Inflight Batching

## Goal

Reduce dense derived apply overhead by cutting the number of expensive apply and commit boundaries during replay/catch-up.

Before this work, profiling showed these hot buckets:

- `flushMutable`
- `persistManifest`
- `Cache.invalidatePath`
- `NamespaceWriteTxn.commit`
- `state.mergeStates`

The common pattern was that dense derived apply was finalizing too often.

## Single Durability Layer, No Second WAL

Zig already has durable replay state in the derived log:

- `pkg/antfly/src/storage/db/derived/derived_log.zig`
- `pkg/antfly/src/storage/db/derived/derived_worker.zig`

So this work adds only microbatching on top of that log; it does not add a second per-index WAL layer. Some Go services combine durability and microbatch scheduling in one WAL-like component, which makes sense where a single per-index worker path owns both concerns. In Zig those two concerns are already separated by the derived log, so only the batching half needs a home.

## Placement

The batching shell lives under storage/db (`pkg/antfly/src/storage/db/batcher.zig`) rather than in a shared cross-language library, because the batching rules are tightly coupled to the storage/db pipelines:

- replay/catch-up batching depends on `DerivedBatch` semantics
- enrichment batching depends on generated-enrichment request semantics
- merge behavior is index-kind specific

## Replay/Catch-up Batching

`derived_worker.catchUpIndex()` accumulates consecutive derived records for a managed index during replay and flushes the accumulator through the existing apply callback as one merged `DerivedBatch` when a per-kind threshold is hit (source record count, embedding/mutation/document count, or end of the replay loop).

`batcher.zig` implements this accumulation per index kind — dense vector, sparse vector, full-text, algebraic, and graph — each with its own accumulator and threshold, since the cost of an unbatched apply differs by kind. Durability is unchanged by this: batching only changes how many records are grouped into one apply/commit boundary, not what gets persisted or when it becomes replayable.

## Effect

Batching reduces:

- calls into `applyDerivedBatchToIndexContext`
- `bulk_ingest` batch lifetimes
- LSM commit / flush / manifest reconciliation points
- `NamespaceWriteTxn.commit` frequency

## Thresholds

Each index kind uses conservative, hard-coded thresholds (source records, embeddings, mutations, or documents accumulated per flush) rather than one shared generic value, so a more expensive kind doesn't share a limit sized for a cheaper one. These can be tuned or made configurable later as the profile moves.

## Open work

- Whether a longer-lived DB-level dense apply scope (sharing one ingest lifetime across multiple replay flushes) is needed is unresolved.
- Whether enrichment-side request coalescing should be added to the existing worker path is unresolved.
- Promoting a shared queue/flush contract out of `batcher.zig` into a common library has not happened; the batching shell still lives entirely under storage/db.
- If per-kind batching turns out insufficient, two alternatives remain unexplored: applying the same batching idea higher up (before replay flushes become DB apply calls), and revisiting in-place application of LSM overlay state on commit.
