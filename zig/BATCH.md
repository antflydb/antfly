# Batch Coalescing

## Goal

Collapse duplicate per-key work before storage, replay, and indexing fan out.

This is both a performance problem and a correctness problem for transforms.

## Current rule

For the current `BatchRequest` shape, the effective per-key order is:

1. `writes`
2. `deletes`
3. `transforms`

That matches the existing API shape, where those operations are carried in separate arrays rather than one ordered event stream.

## Why this matters

Without coalescing, the system can do multiple units of work for the same key in one batch:

- multiple store writes
- multiple replay entries worth of downstream work
- multiple full-text/dense/sparse scheduling passes
- extra overwrite/delete bookkeeping

Transforms make this more serious:

- the old path resolved each transform from committed store state
- it did not see same-batch writes/deletes for the same key
- so `write(k) + transform(k)` could resolve from stale base state

## Intra-batch semantics

For each key inside one batch or transaction-intent request:

- `write + write` => keep final write
- `write + delete` => final delete
- `delete + write` => final write
- `delete + delete` => final delete
- `write + transform` => resolve transform against pending write value
- `delete + transform` => resolve transform against null/deleted state
- `transform + transform` => compose through the prior resolved state for that key

The output of coalescing is a single final semantic operation per key:

- one final write, or
- one final delete

## Scope

Implemented first:

- intra-batch coalescing for `DB.batch`
- intra-batch coalescing for transaction intent writes

Left for later:

- broader bulk-window coalescing across multiple `DB.batch` calls in one ingest session
- richer mixed ordering if the API ever moves to a unified ordered operation stream

Current bulk-window scope:

- enabled only inside `beginBulkIngestSession() ... finishBulkIngestSessionWithOptions(...)`
- the underlying store and dense indexes stay in bulk-ingest mode across calls
- request-level writes still commit directly at their requested sync level
- the old hidden staging sync level has been removed
- any pending coalesced work from older/internal callers is flushed before the next direct batch

## Dense HBC Replay Windows

Dense replay uses bulk-style HBC batch options for throughput, but live replay must not
turn "bulk ingest" into unbounded hidden work at the outer session finish.

The production shape is:

- keep replay batches large enough to amortize routing and storage overhead
- keep grouped routing, coalesced leaf writes, and quantized routing enabled
- defer expensive HBC maintenance to the current replay or implicit bulk window
  boundary, not to each small HTTP batch
- publish after bounded window work is drained
- do not accumulate leaf splits or quantized rebuilds across the full public load

The default live dense window is 25k index items. There are two related caps that
should stay aligned:

- async dense replay collects at most 25k items per publish window
- public implicit dense bulk ingest rolls after 25k accepted ops, or after the idle
  timeout

The replay cap controls how much derived dense work a worker applies before publishing
a query-visible HBC state. The public cap controls how long a serial client upload can
keep the implicit dense bulk session open before it rolls. If those diverge, the code
can look like it has a 25k replay window while still holding public bulk state for a
larger hidden window.

The replay window is not a fixed "4k vectors" memory limit. The old
`ReplayBatcher` accumulator still has conservative per-kind count caps, but the live
change-journal replay path is `ReplayChunkBuilder`. That path must be budgeted by
work size, not by a hard-coded dense embedding count.

The long-term rule for dense replay sizing is:

- keep `max_items_per_window` as the outer coalescing limit
- estimate dense vector work as `embedding_artifact_count * estimated_vector_bytes`
- include that estimate in `derived.replay_window` while the window is collected
- derive the byte window from `resource_manager` budgets:
  - `derived.replay_window`
  - `dense.apply_working_set`
  - `dense.routing_working_set`
- allow larger batches when budgets are available
- shrink/split windows before apply when the estimated bytes would exceed the budget

The default estimate is 384 f32 dimensions, because that is the current public
benchmark shape. It can be overridden with `ANTFLY_DENSE_REPLAY_ESTIMATED_VECTOR_BYTES`.
The byte window can be overridden with `ANTFLY_DENSE_REPLAY_MAX_WINDOW_BYTES`. These
are escape hatches; production sizing should come from resource-manager budgets.

This keeps the two goals separate:

- the item cap controls how much replay work can publish as one visible HBC window
- the byte budget controls how much memory/apply/routing pressure one window is allowed
  to create

This distinction matters because the 1M public write benchmark showed two different
failure modes when HBC work was deferred to the outer bulk finish:

- deferred leaf split normalization repeatedly reloaded vectors from the docstore
- after leaf split deferral was disabled, deferred quantized rebuild became the same
  kind of finish-time vector reload tail

Both were technically batched, but at the wrong scope. They made individual replay
batches cheap while creating a large post-load stall and memory spike before the index
became query-visible.

The intended options for live `.propose` / `.write` dense replay are:

- `bulk_ingest = true`
- `assume_absent_ids = true` when replay knows the vector ids are new
- `coalesce_leaf_writes = true`
- `allow_quantized_routing = true`
- `defer_quantized_rebuild = true`
- `defer_quantized_rebuild_to_bulk_finish = false`
- `defer_leaf_splits_to_batch_finish = true`
- `defer_leaf_splits_to_bulk_finish = false`

In this contract, `defer_quantized_rebuild = true` means "coalesce touched quantized
nodes and rebuild them at the current HBC batch finalize." It does not mean "wait until
the outer bulk ingest session finishes."

`defer_leaf_splits_to_batch_finish = true` means "coalesce oversized leaves within the
current replay apply microbatch and normalize them before the microbatch commits." The
outer dense catch-up session may still coalesce publish and quantized rebuild work, but
it must not allow one leaf to absorb an entire 25k replay window before structural
maintenance runs.

The batch cap has to be enforced on the active replay cadence, not only from idle
maintenance or outer finish. Otherwise a continuous upload can keep appending to the
same leaf across a large replay window, produce one giant oversized leaf, and make
`splitLeaf` or local subtree rebuild materialize `leaf_members * dims * sizeof(f32)`
from the primary store in one publish window.

The finish path must keep per-publish split work bounded with both
`max_deferred_hbc_leaf_splits_per_publish` and
`max_deferred_hbc_leaf_split_members_per_publish`. Split count alone is not a
real work budget: for high-dimensional indexes, a window with many medium leaves
can still reload and repartition enough vectors to block foreground progress for
minutes. It may publish multiple finish windows, but no single publish should
normalize an unbounded split queue or an unbounded number of split input members.
This is the difference between useful deferred work and the old pathological
post-load stall.

Split-created leaf quantized payloads are part of that same bounded publish window.
When a split has already materialized the transformed member matrix, the leaf RaBitQ
payload must be written from that matrix. It must not enqueue a generic deferred
quantized rebuild that reloads the same member vectors from the primary store. Normal
append-only leaf mutations may still use incremental quantized append. Whole-leaf
quantized rebuild is reserved for structural rewrites such as split/repartition/local
subtree rebuild, and those rewrites should reuse the vectors already loaded for the
structural operation.

There is a second bound inside a split. A single leaf can still become far larger than
the configured leaf size, and a binary split of that leaf still has to materialize the
leaf vectors. During bulk finish, medium-large leaves should cross into the local
recursive subtree rebuild path earlier than normal point-write splitting. The default
bulk-finish rebuild threshold is therefore `2 * leaf_size` unless a caller provides a
more explicit threshold. This keeps append replay from creating one expensive
binary-split leaf that blocks the writer after the queue itself has already been
bounded.
For normal point-write splitting, the default local-rebuild threshold remains
`max(leaf_size * 4, leaf_size + 1)`, with
`ANTFLY_HBC_BULK_REBUILD_LEAF_MIN_MEMBERS` available as an absolute override.
Bulk finish uses the lower `2 * leaf_size` threshold because the work is already
known to be deferred maintenance. Above that threshold the HBC does not repeatedly
binary-split and reload the same oversized leaf. It loads the leaf's vectors once,
builds a local recursive subtree in memory, and atomically swaps that subtree into
the parent. The publish remains single-writer, but the expensive partitioning is
scoped to one bounded leaf rebuild instead of a long chain of reload/split/reload
operations.

The rejected outer-finish-only live shape was:

- `defer_leaf_splits_to_batch_finish = false`
- `defer_leaf_splits_to_bulk_finish = true`

That made individual replay batches cheap, but it allowed a single leaf to grow across
the whole replay window. At 1M scale, publish then spent minutes in one large
leaf-local vector materialization/split/rebuild. Live replay should prefer bounded
structural work over a hidden outer-finish tail.

Unbounded outer-finish deferral remains an explicit offline/full-index optimization:

- `defer_quantized_rebuild_to_bulk_finish = true`
- `defer_leaf_splits_to_bulk_finish = true`

That shape is allowed to trade a long finish phase for cheaper intermediate batches.
It is not the default for the live public replay path; live replay must keep the finish
scope tied to the current bounded window.

## Stable contract

The current long-term contract is intentionally narrow:

- public sync levels do not imply hidden cross-request staging
- explicit bulk ingest only changes backend write/finish behavior
- keep external reads on committed state only

That means:

- `get`
- `lookup`
- `search`
- general request-level predicate evaluation

do **not** see staged bulk-session state.

The only staged-state read is internal transform resolution inside the coalescer, because
`write + transform` and `transform + transform` must compose against the effective pending
per-key state for correctness.

This is the intended v1 contract. We are not turning bulk ingest into a general overlay
transaction layer.

## Why not broaden it yet

`predicates` and `timestamp_ns` are semantic boundaries, not just extra request fields.
Making them stageable would require staged version/timestamp visibility rules.

Likewise, making reads observe staged state would require overlay semantics across:

- `get`
- `lookup`
- `search`
- transform resolution
- predicate checks

That is a different feature with a much larger correctness surface.

## Design constraints

- Keep the coalescer above storage/replay/index fanout so all index kinds benefit.
- Preserve current API semantics for the separate `writes/deletes/transforms` arrays.
- Resolve transforms once per key against the effective pending state for that key.
- Avoid unnecessary copying for borrowed writes/deletes; only own transformed outputs.

## Measurement

We want a lightweight bench that isolates the coalescing win without the full replay/catch-up
graph. The important measurements are:

- requested writes
- final staged key count
- stage time
- finish/flush time
- total write time
- replay entry count / payload bytes
- bulk coalescing counters

That bench should use overwrite-heavy document/full-text input and make it easy to compare:

- no bulk session
- bulk session with coalescing
- different sync levels
