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

Live managed indexes derive the estimate from their configured dimensions. The
384-f32 default remains only for callers that do not carry dense index metadata.
Lite's fallback posting path applies a working-set factor to the vector estimate.
Replay keeps an independent ceiling in original work units, including keys and
document descriptors, so that factor does not enlarge delete-only or mixed work
windows. Resource-manager slice headroom still caps estimated memory. Adaptive
window state remains in work units when the storage estimate changes.

The estimate can be overridden with `ANTFLY_DENSE_REPLAY_ESTIMATED_VECTOR_BYTES`.
An explicit estimate replaces the storage-derived estimate and uses its original
unscaled window ceiling. The byte window can be overridden with
`ANTFLY_DENSE_REPLAY_MAX_WINDOW_BYTES`. These are escape hatches; production sizing should come from resource-manager budgets.

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

Live bulk coalescing also must not disable storage safety bounds. A public upload can
continuously replenish both the primary write window and dense replay, so it is not a
finite offline builder even though those layers use bulk transactions internally. The
normal primary and dense LSM profiles therefore keep hard L0 write pressure enabled
during active bulk windows. Soft compaction may still be deferred to protect replay and
foreground latency; hard pressure must remain observable and make bounded progress.

Persisted compaction builds output without the backend lock. Concurrent flushes prepend
new L0 runs, so publication must relocate its exact immutable input IDs before checking
the current target overlap closure. Pure positional movement is safe; a removed input or
new overlapping target is genuinely stale and must still discard the output. Without
that distinction, a continuous public load can discard every completed pressure
compaction until ingestion stops. The 1M batch-100 validation changed from 16 published
compactions across 26 pressure events and 582 final runs of hard debt to 24/24 published
compactions, zero overloads, and zero final debt.

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

## Linear Merge (Range-Based External Sync)

The linear merge endpoint gives external sources -- a foreign database, a REST
API, anything that can emit sorted records -- a stateless way to progressively
sync a table without either side keeping session state. Each request carries a
page of records plus `last_merged_id`, the cursor from the previous page (empty
on the first request). The server scans its own key range from that cursor up
to the highest ID in the page, upserts records whose content hash differs from
what is stored, and deletes any key in that scanned range that is present in
storage but absent from the incoming page; it then returns `next_cursor` (the
highest ID processed) for the client to pass back as `last_merged_id` on the
next page. Because the delete decision is scoped to exactly the range covered
by one page, retrying a page after a failure recomputes the same upserts and
the same deletions from the same range, so repeating a page is idempotent
rather than compounding.

One page's key range can span more than one shard. When that happens, the
server processes only the portion of the page up to the shard boundary rather
than silently completing a merge that only covered part of the requested
range; the client keeps paging with the returned cursor until the input is
exhausted, so a boundary crossing costs an extra round trip rather than a
correctness gap.

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

## Prepared immutable document publications (internal qualification)

Document transforms may carry a native prepared storage receipt alongside their
complete logical JSON. Positive snapshot preparation reads only bounded inline
roots and manifest metadata, and never reconstructs the old complete source.
Same-document operations share one ordered change plan; immutable descriptors
remain borrowed until one final compatibility rendering. Opaque extraction borrows
that request-owned rendering through commit. Unsupported changes reconstruct
complete source directly into request-owned buffers and use ordinary execution.
Wide batches group transforms once in key/request order, and prepared receipts
are sorted for binary lookup. Ordinary unactivated stores allocate no payload
snapshot sidecars.

Whole-document point reads transfer the already owned JSON buffer through the DB
materializer. Typed rows retain pinned-schema decoding. The owned materializer
consumes its input even on error; TTL and row-policy checks retain their original
fences and free the result on rejection. No extra full JSON allocation is needed
when the stored value already has the caller's logical representation. Native
reconstruction uses a child scope from the same pinned view to deliver whole
immutable values into final caller buffers in sorted batches of at most 64 keys.
Partial values keep range I/O, and unsupported scopes retain the reference path.
Temporary decoder memory is reclaimed independently of the returned JSON.

The receipt binds the source logical root and exact physical manifest checksum.
At apply, current source/schema/index fences remain authoritative, the receipt
must still match the complete requested logical input. A changed source root or
mutation revision rejects preparation for retry, including identical-timestamp
and delete/reinsert ABA changes. Representation-only changes and unsupported
receipts retain the ordinary full-write path. A disposable root cache is only a
hint; missing/damaged entries fall back to actual manifest metadata. Extraction,
observers, retained history and indexing still receive complete logical documents.
Receipts travel through the existing private commit participant, independently
of its observer/stamping callbacks. KV pairs keep their original two-slice layout
and ordinary fast JSON codec; no receipt field exists in the wire record.
Permissive JSON parsing cannot construct a native receipt. Replay, cloning and
paths that cannot retain the participant use the complete logical fallback.

This path remains behind an internal format qualification gate. Persisted
catalog/peer activation is required before automatic migration can ship; users
should not need a per-table opt-in. Eligible local writes now reuse root-local
HMAC validation attestations bound to the row, exact physical manifest, logical
root, validator rules and schema version. Publication is atomic with source and
revision effects; preparation grants no storage ownership or authorization.
Unknown/non-composable rules and historical ordered admission keep full validation.
DDL preserves an already upgraded physical reader fence and keeps logical schema
fingerprints independent of that local physical layout. Scoped index/producer
effects are still required before untouched derived work can be skipped safely. See the [payload and publication design](../docs/design/antfly-grep.md)
and [qualification evidence](pkg/antfly-grep/README.md). Public transform semantics
and authorization are unchanged.


Provider input capture uses a pinned authority/source snapshot after a cheap
negative authority probe. Its private verifier borrows primary/group JSON for
equality instead of allocating another source copy; relational rows retain
schema-aware decoding and an owned temporary. Hash borrowed input before
verification or later lookups can retire decoder buffers; shared source validation
hashes each row encoding before probing its alternate encoding. Existing
whole-source guards, logical fallback and wire semantics are unchanged.
`document-provider-input-test` covers ordered dense/sparse and asset capture,
source ABA, zero-allocation JSON verification and retiring-buffer adapters.
A test-only native manifest comparison candidate remains reproducible, but is
not active: its second SHA-256 pass measured about 1.9x the reference latency in
the focused macOS fixture. Composable producer proofs require their own versioned
contract before source hashing and unrelated-update invalidation can be removed.
