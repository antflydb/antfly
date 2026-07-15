# Dense Indexing Lifecycle

## Status

Core implementation complete; the single-run one-million-document qualification
passes. Repeated baseline/candidate performance runs and the full deterministic
fault matrix remain rollout gates.

Implemented in the current tree:

- ordinary dense replay uses reopenable streaming sessions rather than active
  bulk-publication state
- `IncompleteBulkPublish` is quarantined and repaired through a durable,
  restart-resumable shadow-generation state machine
- a counterless legacy artifact projection bootstraps its authoritative target
  count from a stable snapshot plus a signed concurrent-write delta; only the
  two short marker/finalization boundaries hold the apply lock, and restart
  safely repeats the scan under the existing durable repair intent. A random
  per-attempt nonce prevents a stale concurrent scanner from finalizing another
  attempt's delta. Counter routing is derived from the complete index catalog,
  including quarantined status-only configs, so writes remain accounted while
  the affected runtime index is absent. Document-only commits bypass catalog
  construction entirely
- document deletion, TTL cleanup, and remote document-child artifact batches
  commit artifact writes/deletes, the derived journal record, and dense repair
  target-counter mutations in one primary-store transaction. Duplicate keys and
  delete-then-rewrite batches are normalized to their final state before the
  counter delta is calculated. The old helper that deleted artifacts in a
  separate transaction has been removed
- dense and sparse replay distinguish a genuinely missing source artifact from
  an artifact removed by a later document deletion. A stale upsert whose source
  document is already absent advances without creating repair debt; a missing
  or corrupt artifact for a live source document still fails closed
- explicit generation rebuilds for dense, sparse, graph, and full-text indexes
  use the same durable intent and post-pointer restart reconciliation
- startup repair is ownership-fenced, resource-admitted, and driven by a
  debt-only fair queue with O(1) enqueue/removal and a 32-entry bounded
  round-robin inspection window; lost notifications are recovered by an
  independent adaptive 16-to-256-group cursor window rather than a periodic
  full-node sweep, with a measured full-rotation estimate and SLO violation
  metric when the configured group count exceeds the qualified envelope
- pending repair outcomes are re-enqueued before a lost-wakeup cursor may
  advance. Allocation failure is logged, counted, scheduler-backed off, and
  leaves the cursor on the same route instead of silently degrading exact debt
  notification into a full fallback rotation
- large dense candidate scans and pre-activation replay catch-up are
  cooperatively time-sliced by the `BackendRuntime` owner. Scan slices use
  streaming publication and checkpoint the source-store cursor; catch-up
  checkpoints the applied sequence after each bounded replay window. Both leave
  the candidate reopenable before releasing the one-per-node repair slot.
  Restart or ownership transfer resumes from the durable cursor/checkpoint; a
  crash before publication safely repeats the last idempotent unit
- retry backoff uses a separate durable consecutive-failure streak; successful
  slices reset it, so a large repair's lifetime attempt count cannot turn its
  first later transient failure into the maximum ten-minute delay
- repair execution is owned by a dedicated `BackendRuntime` maintenance owner;
  shutdown cooperatively cancels at durable boundaries and drains that owner,
  leaving the candidate resumable instead of waiting for a full rebuild
- dense repair snapshot batches are sized from vector dimensions and the
  currently available `ResourceManager` repair budget. Background planning
  stays within the soft budget and retains the hard budget as an authoritative
  race/safety fence. The legacy counter-bootstrap scan is admitted from the
  same repair slice before it publishes a marker or opens its stable snapshot
- temporarily unknown leadership retains queued debt with retry semantics;
  only an authoritative non-local ownership decision removes local queue state
- provisioned operator rebuild requests only persist/attach to the durable
  intent while holding the group lifecycle guard, then enqueue the exact group;
  the same `BackendRuntime` maintenance owner performs automatic and operator
  reconstruction. Pause/resume/cancel updates only that affected group queue,
  and ordinary writes do not wake the repair scanner
- a validated replacement is activated atomically while any prior healthy
  generation remains available
- artifact-backed dense coverage is an exact cardinality invariant at snapshot,
  replay advancement, and final fenced activation boundaries. A deficit may be
  filled in place; a surplus forces a reset or rejects shadow activation because
  replaying live artifacts cannot remove an unknown stale vector
- activation durably records and retains the previous root until the new clean
  checkpoint is published; a missing or corrupt pointer-selected generation
  fails closed and rolls back to that retained predecessor when one is valid
- root-generation rollover atomically replaces the replica repair identity and
  all pointer-selected replacement debts in one checkpoint; affected indexes
  stay fail-closed and rebuild under the new identity instead of losing an old
  activation intent during promotion or physical-root replacement
- activation adopts the already-open shadow runtime for every index family
  after the durable pointer write instead of closing and reopening a large
  index under the write fence; dense query-drain, apply-lock acquisition, final
  replay, and every reversible pre-commit step share one absolute activation
  deadline
- public repair status and errors are intentionally compact; detailed state is
  confined to operator metrics and logs; malformed durable repair state maps
  to the same compact `failed` status instead of disappearing from the response
- cache policy is node-owned by `ResourceManager` and is absent from index
  configuration
- disk growth is estimated from the selected index generation and claimed by
  `ResourceManager` in a storage-backend capacity domain; provisioned storage
  supplies a live platform filesystem probe, and claims are rechecked and may
  grow at bounded shadow publication/catch-up boundaries, including every HBC
  deferred-publication window. Before a conservative claim can deny progress,
  materialized candidate bytes are reconciled out of the future-growth claim,
  so free space and the shared reservation never charge the same bytes twice
- filesystem-observation freshness is monotonic end to end. Durable retry
  deadlines remain realtime so they survive restart, and are translated once
  into monotonic deadlines when inserted into a process-local scheduler; the
  two clock domains are never compared directly
- a forced named-index repair job requests one replacement generation across
  its bounded group traversal and then becomes observational; cancellation is
  terminal only after a bounded durable pause traversal has reached every
  affected group and signalled any active owner to yield
- forced generation completion is persisted against the stable API job
  identity before the repair intent is removed. A crash after pointer/checkpoint
  publication but before API acknowledgement therefore observes the completion
  instead of constructing a second generation
- generation completion and source-artifact debt are separate durable
  outcomes. Once a replacement pointer and clean checkpoint commit, the
  generation intent is complete even if corrupt or missing source artifacts
  remain in the bounded artifact-repair queue; those issues stay visible
  without triggering redundant full-generation rebuilds
- transient repair-job dispatch, ownership, routing, and storage failures return
  the durable job to bounded exponential backoff. Named-index cancellation
  remains queued and is resumed by the API maintenance supervisor; malformed or
  semantically invalid requests alone become terminal
- retired-generation collection is deduplicated `BackendRuntime` cleanup work.
  A rerun latch covers retirements that race an active scan, and DB shutdown
  drains the cleanup owner before destroying the index manager
- embedded/Lite databases that intentionally omit the managed durable-repair
  checkpoint treat repair-intent discovery as empty, so normal index deletion
  remains available; attempts to create managed durable repair state still
  fail closed
- replay-pin creation/removal and the in-memory pressure gate share the replay
  truncation exclusion, including concurrent repairs of different indexes
- ordinary `.write` traffic participates in derived-backlog admission: the
  node resource manager uses 200/100 pending-sequence hysteresis and also
  forces bounded progress when aggregate LSM state reaches its configured
  write-throttle pressure; if per-sequence accounting allocation fails, an
  allocation-free aggregate keeps exact byte pressure and fails closed through
  the newest affected sequence
- managed-Raft writes apply repair admission on the confirmed leader before
  proposal. Followers forward normally and committed apply always bypasses the
  gate, preserving replica determinism. The normal path performs one combined
  `ResourceManager` pressure check per distinct manager and only scans the
  exact group/table writer cache after hard replay pressure is present
- control-plane group status never cold-opens a root participating in an active
  split or merge. It reports durable transition readiness plus the most recent
  merged/store snapshot and live Raft routing; an ownership race detected after
  the cache probe falls back to the same non-opening path instead of aborting
  the complete node status refresh
- split-key discovery borrows the existing managed writer and reads primary
  keys without draining enrichment or derived-index maintenance. It finalizes
  only a pending primary-store auto-bulk boundary needed for primary visibility;
  full replay/index draining remains reserved for a DB that is actually closing
- mutable LSM snapshots have generation-specific reader references, so an old
  replay scan cannot retain unrelated later snapshots; write transactions keep
  backend-close fencing without pinning versions unless they open a cursor
- the gated 1,000,000-document streaming-ingest qualification passes exact
  count/checkpoint, bounded memory, durable reopen, and post-restart search in
  471.709 seconds under `ReleaseFast`, with an 874,287,160-byte peak pressure
  working set (1,000-document client batches)
- the final 100,000-document `ReleaseFast` regression run, after resource and
  snapshot-lifecycle hardening, completes the same ingest/catch-up/reopen/search
  contract in 69.399 seconds with a 114,001,576-byte peak pressure working set
- the post-production-hardening provisioned-path guardrail ingests 50,000
  1,536-dimensional external vectors in 21.347 seconds and drains dense replay
  in 3.748 seconds (426,949 write ns/document, 100-document client batches),
  confirming that normal writes do not enter bulk publication or repair

This document defines how ordinary dense replay, explicit bulk construction,
and interrupted HBC publication should differ. The immediate objective is to
make normal writes crash-replayable without giving up the batching and LSM
coalescing that make dense ingest fast.

The core decision is:

- normal asynchronous dense indexing uses a streaming replay session
- true bulk construction uses a bulk publication session
- only bulk publication may leave cross-batch HBC state that requires an
  incomplete-publication marker
- generation replacement is reserved for explicit import/rebuild operations,
  not used for every ordinary write burst

## Problem

Normal API writes do not automatically enter an explicit table bulk-ingest
window. The public write layer reserves those windows for rebuild and import
paths; ordinary writes use the normal DB/storage batching path.

Dense derived replay currently has a different behavior. The asynchronous dense
worker opens an HBC bulk-ingest session while catching up normal writes. A dense
session may be reused across a burst, and a replay window may contain a large
number of vector operations. Opening the HBC session durably writes:

```text
__bulk_publish_state = incomplete
```

HBC refuses to reopen an index while that marker exists. This is correct when a
session has deferred structural mutations that are unsafe until final
publication. It is unnecessarily strong for normal replay when every committed
batch can instead be made structurally reopenable.

The resulting failure mode is disproportionate:

```text
normal writes
    -> asynchronous dense replay
    -> long-lived HBC bulk session
    -> process interruption
    -> IncompleteBulkPublish
    -> index quarantined until explicit reconstruction
```

The primary documents and embedding artifacts remain durable, so the dense
index is reconstructible. The problem is the write-session contract, not loss
of source data.

## Terminology

"Bulk ingest" currently covers several independent optimizations and safety
properties. This design separates them.

### Bulk-optimized batch

One bounded apply call using write optimizations such as:

- grouped centroid routing
- coalesced leaf mutation
- batch-finish leaf splitting
- quantized routing
- reduced flush and manifest overhead
- empty-index bulk building when the batch is large enough

A bulk-optimized batch does not inherently require an index-wide incomplete
publication marker.

### Streaming replay session

A short-lived sequence of bulk-optimized, independently valid batches. The LSM
backend may coalesce work and defer flush/maintenance across the session, but
each committed HBC batch leaves a structurally valid state.

An interrupted streaming replay session is recovered by replaying from the last
durable applied sequence.

### Bulk publication session

A construction session allowed to defer HBC structural state across committed
transactions until a final publication boundary. An interrupted session may be
unsafe to reopen and therefore requires either:

- an incomplete-publication marker and artifact reconstruction, or
- isolation in a shadow generation that has not yet become active

This mode is for explicit import, rebuild, or true bulk construction.

## Current Code Shape

The relevant paths are:

- `pkg/antfly/src/api/table_writes.zig`
  - ordinary API uploads do not automatically start explicit table bulk windows
- `pkg/antfly/src/storage/db/derived/catch_up_policy.zig`
  - dense replay coalescing, session reuse, and window limits
- `pkg/antfly/src/storage/db/derived/async_runtime.zig`
  - opens and closes per-index catch-up state
- `pkg/antfly/src/storage/db/db.zig`
  - `beginDerivedCatchUpSessionAsync`
  - `finishDerivedCatchUpSessionAsync`
  - `applyDerivedBatchToIndexContextProfiled`
- `pkg/antfly/src/storage/db/catalog/index_manager.zig`
  - maps storage batch mode to HBC batch options
- `pkg/antfly/src/storage/hbc_adapter.zig`
  - owns the HBC session depth, deferred state, and
    `__bulk_publish_state`

The dense apply path already distinguishes batch-level optimizations from some
session-finish deferrals:

- leaf splits may finish at the batch boundary
- leaf splits are not deferred to the bulk-session finish by default
- quantized rebuild is not deferred to the bulk-session finish by default

That makes it plausible to retain the useful batch and LSM behavior while
removing the index-wide incomplete-publication state from ordinary replay. The
claim must still be established by crash tests and benchmarks before rollout.

## Required Contracts

### Normal write contract

For ordinary writes:

1. The primary document and derived replay record are the durability source of
   truth.
2. Dense application is asynchronous unless the requested sync level requires
   dense visibility.
3. Every committed dense replay batch leaves HBC structurally valid and
   reopenable.
4. The applied sequence never advances beyond durably reopenable HBC state.
5. A crash may cause a replay batch to be applied again.
6. Reapplication must be idempotent for inserts, overwrites, and deletes.
7. A normal replay interruption must not create `IncompleteBulkPublish`.
8. Every committed batch atomically publishes the topology, node keys,
   quantized state, and metadata required to reopen that batch.
9. A failed or aborted session may leave the index ahead of its persisted
   applied sequence, but it must never leave the index structurally invalid.

### Bulk publication contract

For a true bulk publication:

1. Cross-batch structural deferral must be explicit in the session type.
2. The active generation must never be presented as complete while deferred
   publication state remains.
3. An in-place interrupted publication is quarantined.
4. Durable primary artifacts must be sufficient to reconstruct supported index
   kinds.
5. If a previous generation must remain searchable, construction occurs in a
   shadow generation and activation is an atomic pointer swap.

### Applied-sequence ordering

The required successful finish order for streaming replay is:

```text
apply one or more structurally valid HBC batches
    -> flush/checkpoint the HBC LSM state
    -> persist the dense applied sequence
    -> permit replay-log truncation
```

If the process stops before applied-sequence persistence, restart replays work
that may already be present in HBC. That is safe only if replay application is
idempotent.

### Durability modes

"Durably reopenable" means reopenable after the durability guarantees of the
configured backend have actually been satisfied. A successful HBC batch or
session finish is not always such a boundary. In particular,
`relaxed_split_durability` may run the destination HBC LSM with durability
disabled during construction; the split path is safe only because it performs
an explicit durable `syncAll(true)` before publishing the destination.

The applied-sequence and replay-truncation rules are therefore mode-aware:

| Backend durability | Batch/session finish means | Applied-sequence rule |
| --- | --- | --- |
| Full durability | Reopen-critical data and checkpoint are stable | May publish the watermark after the finish succeeds |
| Relaxed or `none` construction | Structurally complete in the current process, but not crash durable | Must not publish a durable watermark or release replay until an explicit durable sync succeeds |
| Memory-only | No process-crash durability contract | Must not use index progress alone to authorize durable replay truncation |

LMDB `no_sync` and any future relaxed backend mode follow the same rule as
relaxed HBC construction: a logical commit is not a durable projection
checkpoint. The backend interface used by dense replay should expose whether a
finish established a durable checkpoint rather than asking callers to infer it
from configuration.

If an explicit sync fails, the index may remain structurally usable in the
current process, but its pending applied sequence is not published and its
replay-retention requirement remains. A split or candidate generation using a
relaxed construction mode must likewise remain unpublished until the final
durable sync succeeds.

## Proposed Session Model

Introduce an explicit HBC write-session kind:

```zig
pub const WriteSessionKind = enum {
    streaming_replay,
    bulk_publication,
};
```

The implementation may use one tagged session state or separate depth fields.
Mixed nested session kinds should be rejected rather than silently combined.

The behavioral matrix is:

| Behavior | Streaming replay | Bulk publication |
| --- | --- | --- |
| Bulk-optimized HBC batches | yes | yes |
| LSM session coalescing | yes | yes |
| Defer commit flush within the session | yes | yes |
| Defer routine maintenance | yes | yes |
| Stage HBC node-key mutations across committed batches | no | allowed |
| Defer leaf topology to session finish | no | allowed |
| Defer quantized state to session finish | no | allowed |
| Defer reopen-critical HBC metadata to session finish | no | allowed |
| Persist `__bulk_publish_state` | no | yes for in-place publication |
| Recovery mechanism | replay | rebuild or shadow activation |

## HBC Changes

### Split LSM batching from publication safety

`bulk_ingest_session_depth` currently influences both LSM batching and HBC
cross-batch deferred state. Split those concerns.

The target state should track at least:

```zig
write_session_depth: usize,
write_session_kind: ?WriteSessionKind,
```

Do not translate raw depth checks one-for-one. Introduce capability predicates
that state why behavior differs:

```zig
fn lsmSessionBatchingActive(self: *const HBCIndex) bool;
fn crossBatchPublicationActive(self: *const HBCIndex) bool;
fn mustPublishMetadataPerBatch(self: *const HBCIndex) bool;
fn shouldPublishSearchStatePerBatch(self: *const HBCIndex) bool;
fn shouldSuppressRoutineMaintenance(self: *const HBCIndex) bool;
```

The required classification is:

- LSM batch mode, mutable-state coalescing, deferred commit flush, and bounded
  routine-maintenance deferral may be enabled for either session kind.
- `stageNodeKeyPut` and `stageNodeKeyDelete` operate only while
  `crossBatchPublicationActive()`.
- `shouldDeferQuantizedRebuildToBulkFinish` and
  `shouldDeferLeafSplitToBulkFinish` are true only while
  `crossBatchPublicationActive()`.
- `persistBulkPublishState` and `clearBulkPublishStateTxn` are used only for an
  in-place `bulk_publication`.
- `flushMetadata` must publish reopen-critical metadata in every streaming
  batch. It may suppress that publication only when the matching topology and
  node-key changes are also isolated behind a bulk publication boundary.
- Flat-RaBitQ centroid-directory node keys must publish at streaming batch
  finish rather than being held until session finish.
- Search caches, published root/count state, workspace clearing, and
  maintenance suppression must each be classified independently. They must not
  inherit publication semantics merely because LSM coalescing is active.

Before implementation, inventory every `bulk_ingest_session_depth` read in
`hbc_adapter.zig` and assign it to one of these capabilities. Acceptance
requires that no safety decision outside session begin/finish code depends
directly on the raw depth.

### Add explicit streaming APIs

Add APIs with names that describe their recovery contract:

```zig
pub fn beginStreamingReplaySession(self: *HBCIndex) !void;

pub fn finishStreamingReplaySessionWithOptions(
    self: *HBCIndex,
    options: backend_types.BulkIngestFinishOptions,
) !void;

pub fn abortStreamingReplaySession(self: *HBCIndex) void;
```

The streaming finish should:

1. finish any batch-local HBC work
2. flush the LSM session to a durable reopenable boundary
3. refresh published search state
4. release bounded session workspace

It must not clear an incomplete marker because it must never create one.

`abortStreamingReplaySession` closes the LSM batching scope, releases session
workspace, and republishes a consistent view of already committed batches. It
does not roll back committed batches and does not advance the applied sequence.
Restart or the next catch-up pass may therefore reapply those batches.

The existing `beginBulkIngestSession` family retains the stronger publication
semantics for explicit bulk construction.

## IndexManager Changes

Expose the session distinction by index name:

```zig
pub fn beginDenseStreamingReplaySessionByName(
    self: *IndexManager,
    name: []const u8,
) !void;

pub fn finishDenseStreamingReplaySessionByNameWithOptions(
    self: *IndexManager,
    name: []const u8,
    options: backend_types.BulkIngestFinishOptions,
) !void;

pub fn abortDenseStreamingReplaySessionByName(
    self: *IndexManager,
    name: []const u8,
) void;
```

Keep `BatchOptions.mode = .bulk_ingest` for replay batches where benchmarks
show it is beneficial. That flag selects batch algorithms; it must no longer
implicitly mean that the index is in an unsafe publication window.

## Derived Replay Changes

Change `beginDerivedCatchUpSessionAsync` and
`finishDerivedCatchUpSessionAsync` to use the streaming APIs.

Session reuse remains useful:

- coalesce small tails briefly
- amortize LSM flush and manifest work
- apply a bounded number of replay windows
- finish after the configured idle period or a forced visibility boundary

The dense replay window remains bounded by records, items, bytes, and resource
manager pressure. These limits are operational backpressure controls, not
durability boundaries.

Ordinary writes must not merely *report* replay debt. After the primary commit,
`.write`, enrichment, full-text, and full-index paths consult the executor's
backlog-admission target; only the proposal-only path is exempt. The target is
owned by `ResourceManager`, exposed through the derived-executor vtable, and is
identical for manual and threaded backend runtimes.

Admission uses hysteresis rather than draining every producer to the newest
sequence. The current node policy stops a producer above 200 pending replay
sequences and waits only far enough to resume at 100. Encoded replay-byte
pressure computes an equivalent low-water target. If aggregate
`lsm.in_memory_state` reaches a pressure whose policy action is
`throttle_writes` or `reject_work`, the producer waits through at least the
oldest pending sequence; that lets the active bounded replay window publish and
release pinned LSM state before more foreground mutations are admitted.

Backlog accounting may not disappear under allocator pressure. If appending a
per-sequence accounting entry fails, the tracker switches to an allocation-free
overflow interval. It continues exact saturating byte accounting and returns
the newest overflow sequence as the admission target. Partial replay does not
release that aggregate early; reaching the interval tail releases its exact
bytes and restores precise per-sequence tracking. This deliberately trades a
temporarily larger drain for fail-closed memory and WAL admission.

These are internal resource-policy values, not index configuration or public
cache toggles. A composed runtime may supply one resource manager to multiple
DBs; the executor abstraction carries only the resulting target sequence, so
backend choice does not change correctness or admission behavior.

### Session resource bounds

Streaming sessions require both per-index and node-wide bounds. The current
dense LSM defaults combine a 128 MiB mutable threshold with a four-times bulk
multiplier, so retaining bulk-style LSM batching can otherwise permit a large
mutable working set per active dense index.

Force a streaming finish when any configured bound is reached:

- maximum session records or vector items
- maximum session input bytes
- maximum estimated HBC and LSM workspace bytes
- maximum session age or idle time
- maximum dense replay lag
- WAL retention soft or hard pressure
- resource-manager memory or I/O pressure
- a synchronous dense-visibility waiter

Add a node-wide admission limit for concurrently active dense replay sessions.
The resource manager may shorten a window or force a finish, but it must never
turn a streaming session into cross-batch publication.

At streaming finish, retain the current safety ordering:

1. durably finish the dense session
2. flush the pending applied sequence for that index
3. notify query visibility
4. allow replay truncation only after all managed indexes have persisted their
   required watermarks

## Idempotency Audit

Removing the incomplete marker from streaming replay depends on safe
reapplication. Before enabling the new mode, audit and test:

- inserting an already-present vector ID with the same value
- overwriting an existing vector ID
- moving a vector between leaves on overwrite
- deleting an already-absent vector ID
- delete followed by insert in one coalesced window
- multiple writes to one document collapsed to the final value
- coverage accounting when a batch is replayed
- applied-sequence sidecar and embedded HBC checkpoint agreement

If any operation is not idempotent, fix that operation or add a durable
per-batch commit identity before removing the marker.

## Interrupted Streaming Recovery

Expected restart behavior by interruption point:

| Interruption point | Required restart behavior |
| --- | --- |
| Before first HBC batch commit | replay the whole window |
| After one HBC batch commit | open successfully and replay from the old applied sequence |
| After several commits, before session flush | recover committed/WAL state and replay from the old applied sequence |
| After HBC flush, before applied-sequence persistence | open successfully and idempotently replay the window |
| After applied-sequence persistence | continue after the persisted sequence |
| During replay truncation | retain or recover enough replay history to honor every persisted index watermark |

None of these cases may produce `IncompleteBulkPublish`.

## Automatic Repair For True Incomplete Publications

Streaming replay prevents new ordinary-write incidents but does not remove the
need to recover:

- indexes created by older builds
- interrupted explicit bulk publications
- interrupted in-place rebuilds

Automatic restart repair is an independently landable track. It can ship before
the streaming-session change to eliminate wipe/re-ingest as the operational
recovery procedure, or after it as recovery for legacy and explicit-publication
state. It must not be confused with the generation-publication project: the
affected index remains unavailable while this repair runs.

### Recovery owner

Managed startup catch-up, rather than the HTTP repair-job store, owns automatic
restart repair.

Startup catch-up already:

- runs outside request handling in a background thread
- selects locally owned groups from placement and leadership state
- serializes group operations through the local write owner
- publishes startup, progress, and degraded status
- detects zero-progress retries and applies bounded exponential backoff

The HTTP repair-job store remains the operator-facing mechanism for explicit
repair, cancellation, and observability. Making it the correctness mechanism
for startup would introduce a second ownership and scheduling system, and its
table-scoped job shape does not currently identify the locally owned group that
startup is repairing. It does, however, durably retain unfinished operator
cancellation traversals. The API maintenance supervisor automatically resumes
the oldest queued traversal after process restart, one bounded pass at a time
through `BackendRuntime`; a client does not have to call the advance endpoint to
finish cancellation.

Automatic and operator-triggered repair must call the same DB repair engine and
use the same durable repair intent. They differ only in scheduling and policy.

### Load-failure classification

Startup should classify index load failures narrowly:

```zig
pub const IndexLoadRecoveryAction = enum {
    retry_open,
    rebuild_from_artifacts,
    manual_intervention,
};

pub fn loadFailureRecoveryAction(
    err_name: []const u8,
) IndexLoadRecoveryAction {
    if (std.mem.eql(u8, err_name, "IncompleteBulkPublish")) {
        return .rebuild_from_artifacts;
    }
    if (std.mem.eql(u8, err_name, "TableReadChurn")) {
        return .retry_open;
    }
    return .manual_intervention;
}
```

The initial automatic rebuild allowlist should contain only
`IncompleteBulkPublish`. Unknown corruption, unsupported versions, invalid
configuration, and missing source artifacts remain terminally degraded until
an operator intervenes.

The error classification authorizes a rebuild attempt; it does not by itself
authorize deleting any index root. Before creating a candidate, run a rebuild
capability preflight that verifies:

- the configured index kind has a deterministic artifact reprocessor
- the current configuration hash matches the intent or starts a new intent
- the required primary embedding or other derived artifact source is
  configured and its durable coverage metadata is consistent enough to start;
  full per-artifact validation remains part of the build
- sufficient local disk and resource-manager budget is available
- the local process still owns the group and is permitted to publish it

An allowlisted trigger with missing source artifacts becomes terminal repair
debt without modifying the quarantined root.

Transient open failures continue through the existing open-retry path; they do
not authorize deleting or rebuilding an index root.

### Durable repair intent

Before starting reconstruction, persist an index repair intent in the DB's
replica-local system-metadata store. It must share the local durability and
transaction boundary needed by replay pins, but it is not user data and is not
emitted by logical table replication:

```zig
pub const IndexRepairIntent = struct {
    version: u8 = 1,
    repair_id: u128,
    db_identity: u128,
    group_id: u64,
    replica_id: u128,
    root_generation: u64,
    index_name: []const u8,
    kind: types.IndexKind,
    config_hash: u64,
    trigger: enum {
        incomplete_bulk_publish,
        operator_generation_rebuild,
        root_generation_rebuild,
    },
    candidate_relative_path: ?[]const u8 = null,
    previous_pointer_captured: bool = false,
    // null after capture means the canonical root was previously active
    previous_active_relative_path: ?[]const u8 = null,
    detected_sequence: u64,
    build_floor_sequence: u64 = 0,
    candidate_applied_sequence: u64 = 0,
    target_sequence: u64,
    phase: enum {
        detected,
        preflight,
        building,
        catching_up,
        ready,
        waiting_for_convergence,
        activating,
        validating,
        cleanup,
        terminal,
    },
    attempt_count: u32 = 0,
    next_retry_at_ms: u64 = 0,
    started_at_ms: u64,
    updated_at_ms: u64,
    owner_epoch: u64,
    automation: enum {
        enabled,
        paused,
    } = .enabled,
    last_error: ?[]const u8 = null,
};
```

Suggested key:

```text
\x00\x00__metadata__:index_repair_intent:<index-name>
```

The internal prefix keeps repair state out of the user-document namespace.
Persisted string fields use owned encoding with length and total-size limits;
unknown intent versions or enum values fail closed.

The intent is authoritative across restart of the same replica.
`db_identity`, `group_id`, `replica_id`, and `root_generation` bind it to the
local derived-state root that owns the candidate; `repair_id` and
`candidate_relative_path` identify exactly which shadow may be resumed or
discarded. Persisted retry metadata prevents repeated process restart from
resetting a failing repair loop. Candidate paths must be canonical relative
paths below the DB repair-shadow root; absolute paths, `..`, and paths outside
that root are rejected before filesystem access.

Repair intents, replay pins, candidate paths, and active-root pointers are
durable replica-local projection metadata, not replicated table data. They are
excluded from logical HA replication, split/move payloads, and logical backups.
On leadership promotion, the new local owner discovers and advances only the
intents already belonging to that replica. A new replica, a moved/split group,
or a logical restore reconstructs its own projection and creates a new repair
intent if its local load state requires one.

A physical restore may retain an intent only when all four identity fields and
the active root-generation marker match. Any mismatch invalidates the candidate
path without opening it; safe cleanup is restricted to the restored repair
shadow namespace, and reconstruction starts with a new local identity. Status
aggregation reports repair state per group/replica so a healthy replica cannot
mask another replica's unavailable projection.

Replica lifecycle handling is explicit:

| Event | Required behavior |
| --- | --- |
| Restart with matching identity | Reload intent and pin before truncation; resume by phase |
| Leadership/placement loss | Fence and stop local execution; retain local intent for possible return, but forbid activation |
| Promotion of another existing replica | Discover that replica's own load state and intent; never open the former owner's candidate |
| Replica replacement or root-generation change | Delete inactive old candidates. If an old candidate is pointer-selected, persist fresh new-root reconstruction debt and keep the index fail-closed; never make it serviceable by merely discarding the old intent |
| Group move or split | Do not copy repair state or candidate files; fence source work and build destination/children from their durable logical state |
| Logical backup/restore | Omit repair state and rebuild derived indexes under new identities |
| Physical backup/restore | Resume only after exact identity, generation, config, checkpoint, and candidate validation |
| Index/group deletion | Atomically make the pin non-authoritative with deletion, then clean local candidates asynchronously |

The durable sequence is:

```text
detect IncompleteBulkPublish
    -> persist repair intent in replica-local system metadata
    -> mark the projection repair_required
    -> preflight source artifacts, reserve disk/resources, and verify ownership/configuration
    -> leave the poisoned root and load failure quarantined
    -> create and persist the candidate path
    -> atomically acquire a pinned primary snapshot and its replay floor
    -> build a shadow replacement from a primary snapshot
    -> catch the shadow up to the current derived sequence
    -> durably mark the candidate ready
    -> converge below the bounded activation thresholds
    -> revalidate ownership and configuration under a time-bounded apply barrier
    -> perform bounded final catch-up
    -> durably record the exact previous active-root pointer
    -> atomically swap the active-root pointer
    -> open and validate the replacement
    -> persist a clean projection checkpoint and clear the load failure
    -> atomically release the replay-retention pin and delete the repair intent
    -> garbage-collect the poisoned root and unused candidates
```

Do not call `reopenQuarantinedIndexForArtifactRebuild` on this path. The shadow
builder must be able to register a replacement directly from the status-only
configuration. Until validation succeeds, explicit queries against the index
continue to fail with the quarantined-index error; they must never observe an
empty replacement opened only to facilitate rebuilding.

Manual index repair must use the same intent. An interrupted operator repair
therefore becomes eligible for safe automatic continuation after restart.
On provisioned storage, the request handler performs only the bounded durable
intent transition and exact-group enqueue while the group lifecycle guard is
held. It never scans the corpus inline and never creates a second repair owner.
Standalone DB use may synchronously advance the same state machine when no
managed scheduler exists.

If startup finds both a healthy active replacement and a stale intent from a
crash after pointer activation, it validates the configuration hash and clean
checkpoint, then clears the intent without rebuilding again.

Every pointer-selected root must validate its durable ready manifest and
configuration hash before a storage backend is opened. A missing directory must
not be recreated as an empty index. If the selected replacement cannot be
opened or validated, restart restores the previous pointer recorded by the
intent when that predecessor remains valid; otherwise the index stays
quarantined and reconstruction resumes.

If startup finds an intent and a candidate path:

- a valid ready candidate resumes at activation or validation
- a valid building candidate resumes only when its checkpoint and artifact
  format explicitly support resumption
- an incomplete or corrupt candidate is deleted and rebuilt
- a candidate not referenced by an active intent is garbage-collected after a
  bounded grace period

### Repair availability gate

Load-failure state alone is not sufficient to gate queries because pointer
activation and process restart can make a candidate openable before the repair
state machine has validated it. Index serviceability is therefore:

```text
runtime index loaded
    and no quarantined load failure
    and no repair intent in detected..validating
    and clean projection checkpoint matches the active config and generation
```

An intent in `cleanup` does not block queries after the replacement has passed
validation and the clean checkpoint is durable. A crash after pointer swap but
before validation leaves the intent in `activating` or `validating`, so restart
continues to return `index_rebuilding` even if the candidate opens successfully.
If candidate validation fails, pointer activation is rolled back when a prior
valid pointer exists; otherwise the index remains quarantined and the intent is
retried or marked terminal.

The serviceability decision is cached in the index manager and updated on load
failure, intent, pointer, and checkpoint transitions. Queries must not read the
primary metadata store on every request merely to evaluate this gate.

### Replay retention during repair

The quarantined index is intentionally absent from normal managed replay, so
its old applied sequence cannot protect the replay journal. Before releasing
the primary snapshot used for shadow construction, persist a replay-retention
pin:

```zig
pub const IndexRepairReplayPin = struct {
    version: u8 = 1,
    repair_id: u128,
    db_identity: u128,
    replica_id: u128,
    root_generation: u64,
    index_name: []const u8,
    retain_after_sequence: u64,
};
```

Capturing a snapshot floor and then persisting a pin is racy: truncation could
advance between those operations. Callers must not hand-roll that sequence.
Provide one DB primitive:

```zig
pub fn beginPinnedIndexRepairSnapshot(
    self: *DB,
    repair_id: u128,
) !PinnedIndexRepairSnapshot;
```

It performs this protocol under the same exclusion used to calculate and
advance the replay truncation floor:

1. Persist a provisional pin with `retain_after_sequence = 0` for the bound
   replica and root generation. Zero explicitly means retain the entire
   available replay journal; it is not interpreted as "no pin."
2. Make that pin visible to the in-memory truncation registry before releasing
   the metadata transaction.
3. Open the primary snapshot and read its replay/build floor.
4. In one primary-store transaction, update the intent's
   `build_floor_sequence` and raise the pin from zero to that exact floor.
5. Refresh the truncation registry, then release the exclusion and return the
   pinned snapshot handle.

Replay truncation acquires the same exclusion and clamps to the minimum
sequence required by managed index watermarks, enrichment/resolution stages,
and both provisional and finalized repair pins. A crash at any point leaves
either the conservative zero pin or the finalized pin durable; startup reloads
all pins before enabling truncation. The pin is released only after successful
replacement activation or explicit terminal abandonment with candidate
cleanup. Pin and intent removal are one durable transition.

This permits foreground writes to continue while the shadow is built. If a
deployment cannot provide the durable pin, it must block writes to the group
for the entire snapshot-build and activation window and report that reduced
availability explicitly; that is a fallback, not the target contract.

Snapshots themselves can retain MVCC versions independently of replay bytes.
The resource manager therefore budgets snapshot age and pinned-version bytes as
well as replay-pin bytes. A repair that exceeds either soft budget yields and
reopens from a resumable checkpoint where supported; exceeding a hard budget
enters write backpressure or pauses the repair rather than allowing unbounded
disk growth.

### Disk-capacity ownership and admission

Disk admission is capacity policy and belongs to `ResourceManager`, not to the
executor. `BackendRuntime` owns the threads and I/O lanes that run an admitted
repair; a storage backend may install a capacity source while composing
`DB.OpenOptions`. That source returns a stable identity for the physical
volume/quota namespace plus a current optional capacity observation. Table,
group, index, repair, and logical store identifiers are not capacity domains
unless the deployment guarantees that each maps one-to-one to an independently
exhausted allocation.

Provisioned storage installs a live `statvfs` probe for its replica root into
the shared `ResourceManager`. The same observation feeds store heartbeats and
repair admission, so scheduling data and the final admission decision do not
diverge. A backend with a quota or non-filesystem capacity model replaces that
probe through the same capacity-source interface.

Capacity-source installation is immutable after runtime composition. DBs copy
the source when they open and reservations coordinate through its domain ID, so
runtime replacement would create inconsistent admission views. Idempotently
installing the exact same source is allowed; installing a different source or
installing after admission has begun is rejected.

The durable intent stores `planned_disk_bytes`. It is an estimate and does not
claim that an in-memory reservation survived restart. Each attempt measures any
existing candidate, refreshes the observation, reserves only remaining growth,
and retains a node-owned safety floor for WAL/checkpoint/foreground durability.
Unknown capacity remains accounting-only. Known but stale capacity fails
closed. Shadow construction reconciles actual bytes at bounded durable
publication and catch-up boundaries, refreshes the live capacity observation,
and grows the claim before proceeding when the plan is exceeded. Revalidation
uses the aggregate outstanding claim for the physical domain, so a fall in
available capacity stops the next bounded unit even when the original estimate
has not been exceeded. Full-text segment batches, graph batches, dense/sparse
artifact chunks, replay catch-up chunks, and each HBC deferred-publication
window all cross this fence. Admission never performs a recursive directory
walk on the foreground write path.

The reservation represents future growth, while filesystem availability
already reflects bytes written by the candidate. Every bounded repair boundary
first performs an O(1) capacity-source refresh and aggregate-claim check. If the
conservative claim would deny progress, the executor measures the complete
shadow generation (index artifacts, checkpoint, and publication markers),
shrinks the future-growth reservation by the materialized amount, and
revalidates before deciding. Major phase transitions also reconcile exactly.
This prevents false admission failures as a large rebuild approaches its
estimate without turning segment-heavy builds into repeated recursive walks.
The preliminary fit probe updates the capacity observation but does not record
an admission denial; only a stale observation or the final decision after exact
reconciliation increments denial metrics. Thus operator alerts describe work
that was actually stopped rather than conservative estimates corrected in the
same pass.
Ordinary ingest and query paths never perform this measurement.

Unsupported capacity observation is treated as unknown capacity rather than an
endless repair wait. On a backend that does support observation, transient probe
failure fails new repair admission closed and retries with durable backoff. Store
heartbeats retain the last successful capacity value across that transient
failure and expose a failure counter instead of publishing a false zero-capacity
sample.

Claims coordinate all DBs sharing a manager. They do not coordinate independent
processes on one volume. Production deployments must therefore enforce one
writer process per capacity domain or provide a capacity source backed by an
OS quota, preallocation scheme, or shared allocator. This invariant is part of
the storage backend contract, not an application-facing tuning switch.

### Cache ownership and admission

Dense cache policy is node resource policy, not index schema. Public and raw
index configuration must not expose cache enable/disable switches, entry
counts, or byte values. In particular, `max_cached_nodes`,
`max_cached_vectors`, and `max_cached_metadata` are not supported tuning
controls. Older persisted configuration containing those keys may continue to
load for compatibility, but the values are ignored. HBC-specific environment
switches and mutable runtime cache-cap setters are likewise outside the
supported control plane; node resource-manager policy is the sole production
authority.

The `ResourceManager` owns the cache policy and is authoritative for exact byte
admission across all indexes sharing the manager:

- retained HBC nodes, quantized state, vectors, and metadata use the shared
  `hbc_node_metadata_cache` byte budget
- replay-time decoded-vector and raw-read caches use the applicable dense
  working-set byte budget
- HBC count ceilings exist only as internal CLOCK-bookkeeping safety bounds;
  they are derived from the resource budget and cannot override it
- a provisioned node shares one manager and HBC cache across its groups and
  indexes
- a standalone DB installs one internal default manager before opening primary
  or index storage, so primary LSM state, dense LSM state, replay, and HBC share
  the same policy; direct standalone `IndexManager` use also gets a fallback

Shared-cache lifetime is part of this ownership contract. A node-owned shared
cache is constructed and explicitly bound by the same node resource manager
that outlives every DB using it. A per-DB fallback manager governs DB-local
working sets and internal caches only; it must never install itself into a
caller-owned LSM or HBC cache. Passing an explicit manager with a shared cache
asserts that the caller owns both lifetimes and keeps the manager alive until
the cache is detached or destroyed.

Startup catch-up and repair do not rewrite per-index cache caps. They compete
through the same byte reservations, pressure actions, and fairness policy as
foreground dense work. Operators tune node resource budgets; applications tune
index semantics and search quality, not storage-engine cache internals.

### Repair state transitions

Every transition is idempotent and compare-and-swaps a monotonically increasing
checkpoint revision in addition to `repair_id`, phase, configuration hash, root
generation, and ownership epoch. The revision advances on every mutation,
including same-phase progress, retry, pin, and automation changes, so a pause or
checkpoint cannot be overwritten by a stale snapshot. Filesystem effects happen
before the durable phase that claims they are complete; restart validates those
effects before advancing.

| Phase | Required durable facts | Allowed next phase | Restart action |
| --- | --- | --- | --- |
| `detected` | Bound intent exists; quarantined root is untouched | `preflight`, `terminal` | Reclassify trigger and identity |
| `preflight` | Source, ownership, reservation, and config checks recorded | `building`, `terminal` | Re-run checks; release stale reservations |
| `building` | Provisional/final replay pin and candidate identity exist | `catching_up`, `terminal` | Resume a validated build checkpoint or discard only the candidate |
| `catching_up` | Candidate checkpoint and applied sequence are durable | `ready`, `waiting_for_convergence`, `terminal` | Reopen candidate and replay from its checkpoint |
| `ready` | Candidate is durable, reopenable, and snapshot-complete | `waiting_for_convergence`, `activating` | Validate candidate and current ownership |
| `waiting_for_convergence` | Ready candidate and replay pin remain valid | `catching_up`, `activating`, `terminal` | Continue bounded catch-up outside the apply barrier |
| `activating` | Fenced activation intent and previous pointer are recorded | `validating` | Determine which pointer is active; complete or roll back idempotently |
| `validating` | New pointer is active; serviceability gate remains closed | `cleanup`, `terminal` | Reopen, validate, and publish the clean checkpoint |
| `cleanup` | Clean checkpoint is durable and index is serviceable | intent deletion | Remove pin/intent atomically, then garbage-collect asynchronously |
| `terminal` | Stable error and operator action are recorded | `detected` after explicit retry, or deletion | Remain fail-closed; do no automatic destructive work |

`automation = paused` is orthogonal to phase. It prevents the automatic
executor from claiming the intent while preserving all durable safety state.
Drop and configuration replacement use explicit terminal cleanup transitions;
they do not skip pin or candidate cleanup.

### Bounded convergence and activation

Foreground writes remain available during reconstruction, so activation must
not assume that a busy group will naturally become idle. Candidate catch-up
runs outside the final apply barrier until all configured admission thresholds
are satisfied:

- remaining replay sequences
- remaining replay bytes
- measured catch-up throughput relative to foreground write throughput
- estimated final catch-up time
- configured maximum write-pause duration

Only then may the executor acquire the final apply barrier. Under the barrier
it rechecks ownership, root generation, configuration, candidate durability,
and the current replay target. If the new estimate exceeds the maximum pause,
it releases the barrier without swapping, records
`waiting_for_convergence`, and continues background catch-up. No repair may
hold the write/apply barrier for an unbounded corpus scan or wait.

Activation uses one absolute monotonic deadline rather than independent
timeouts. Final replay receives a deadline-aware record/item window and reserves
a bounded tail for readiness validation, pointer publication, and the clean
checkpoint. The replay worker refuses to open a new apply window after its
deadline. A single backend operation remains non-preemptive, so admission uses
the slower observed per-sequence cost and production qualification must verify
that the largest permitted window fits the pause SLO.

Activation transfers the already-open replacement entry from the shadow manager
into the active manager for every index family. All allocations and candidate
validation occur before the pointer write; dense vector-load context is rebound
during the allocation-free handoff. The detached predecessor is retained while
the pointer and clean checkpoint commit, then closed only after the apply and
dense-query fences are released. This removes size-dependent close/reopen and
generation teardown from the service pause. Pointer commit and clean-checkpoint
publication remain an intentionally non-interruptible correctness tail. The
resource manager records actual fence-held duration separately from post-commit
cleanup, including failed attempts and over-budget events.

An activation error remains in `activating` until restart or the next pass has
read and validated the durable pointer. It cannot fall back to disk-admitted
rebuild work first. Pointer publication is the last fallible handoff operation:
if it fails, the predecessor remains installed and searchable. If the pointer
commits but later checkpoint publication is interrupted, restart reconciles the
selected candidate idempotently; an unverified rollback fails closed.

If sustained writes prevent convergence, the resource manager may apply
bounded dense-replay prioritization and then explicit write admission control.
At the hard replay-retention or disk-reservation limit, writes that would grow
the protected backlog fail with a stable retryable response:

```text
HTTP 429
code = dense_repair_backpressure
retryable = true
retry_after_ms
```

The response is removed automatically after pressure falls below the recovery
threshold. It must not be reported as a generic timeout or allow storage
exhaustion. Deployments may map node-wide emergency unavailability to HTTP 503,
but the structured code and retry contract remain the same.

Activation metrics include convergence lag in sequences and bytes, estimated
pause, actual barrier hold time, convergence retries, throttled writes, and
backpressure rejections.

### Repair engine and scheduler

Startup catch-up owns detection and policy, but it must not synchronously run a
multi-minute rebuild inside the serial table/group inspection loop. Separate
cheap discovery from expensive execution.

Add a bounded DB discovery pass:

```zig
pub const StartupIndexRepairDiscovery = struct {
    discovered: usize = 0,
    already_pending: usize = 0,
    terminal: usize = 0,
};

pub fn discoverRecoverableStartupIndexFailures(
    self: *DB,
    alloc: Allocator,
    limit: usize,
) !StartupIndexRepairDiscovery;
```

Discovery enumerates configured indexes, classifies load failures, validates or
creates durable intents, and publishes repair debt. It performs no corpus scan
and returns quickly so one broken group cannot head-of-line block inspection of
all later groups on the node.

A node-local repair executor consumes intents selected by the managed startup
owner. Notifications are latency hints, not a correctness mechanism: the
executor scans durable intents at startup and periodically thereafter, so a
lost notification, executor restart, or temporarily closed DB cannot strand
repair debt. It reopens the DB through the managed owner for each claim and
does not retain an unsafe raw DB pointer across placement or lifecycle changes.
Startup discovery and operator repair controls enqueue or remove the exact
affected group. They do not route through broad startup catch-up, and high-rate
ordinary document writes never mark the repair scanner dirty.
Named-index pause, resume, cancellation, and successful repair passes retain the
group in the queue until an aggregate owner-side audit has checked every index
in that group. This prevents an operation on one index from dropping independent
repair debt for another index; the aggregate audit performs the eventual O(1)
queue removal when the group is clean.
It calls the same refactored DB repair state machine used by explicit operator
repair:

```zig
pub fn advanceIndexRepairIntent(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    options: types.ArtifactRepairRunOptions,
) !IndexRepairAdvanceResult;
```

The existing repair engine supplies useful shadow creation, snapshot rebuild,
catch-up, final apply barrier, and pointer-swap code. Before reuse, refactor out
its destructive quarantined-root reopen and add the durable intent, candidate,
and replay-pin transitions defined above.

Start with these scheduler limits:

- at most one active index reconstruction per node
- at most one active reconstruction per group
- bounded round-robin selection across known groups; exact generation sizing
  and capacity admission happen inside the selected DB, avoiding node-wide
  status materialization merely to rank one repair slot
- resource-manager admission for memory and disk capacity, backend-runtime
  execution, and one-per-node scheduling bounds for disk I/O, compaction, and CPU
- a reconstructible disk plan plus a process-local reservation token that accounts for the
  candidate, retained poisoned root, WAL/replay growth, and cleanup reserve;
  free-space preflight alone is not admission because concurrent repairs can
  consume the same bytes
- cancellation and a final fencing check when placement or leadership changes
- a dedicated `BackendRuntime` maintenance owner and cooperative shutdown token;
  shutdown requests cancellation before draining the owner, and cancellation
  preserves resumable durable repair state without recording an attempt failure

The concurrency limit may become configurable after qualification, but
unbounded per-group concurrency is never allowed. Dense reconstruction is
time-sliced at reopenable publication boundaries: the source cursor is advanced
only after the candidate slice is durable, and the linked group cursor then
rotates the node slot. The production default is a 15-second scheduler budget;
the scan observes it only after a resource-sized batch, avoiding per-document
clock calls. Pre-activation replay observes the same budget after each durable
replay window. Final replay while activation fences are held does not yield; it
remains bounded by the activation deadline and aborts the swap if it cannot
converge. Other index families retain their existing bounded/cancellable builders
and require the same largest-incident SLO proof before higher concurrency is
enabled.

The scheduler publishes queue depth, oldest-intent age, bounded-scan work,
attempt outcomes, disk-admission waits, and current/peak resource-manager disk
claims. Detailed per-intent phase, retry deadline, sequences, reservation, and
last error remain available through authenticated operator diagnostics and
structured logs. Projected completion is intentionally omitted until measured
throughput makes it defensible. Initial concurrency is one reconstruction per
node. Higher configurable concurrency is enabled only after multi-group
qualification proves that memory, disk, replay retention, query latency, and
activation-pause budgets remain bounded. The periodic lost-wakeup interval and
repair-capacity SLO are internal node policy with conservative defaults. Known
debt is held in an intrusive linked hash queue: enqueue and removal are O(1),
each pass snapshots at most 32 entries, and durable-intent transitions directly
supply the group and table route. Creating, resuming, pausing, or removing an
intent therefore updates scheduling without an administrative snapshot. Metrics
inspect at most the same bounded window; larger queues conservatively keep the
executor runnable instead of performing a full scan.

Retry has two scopes. Failures returned by the DB state machine persist their
deadline in the durable intent. Failures outside that state machine, such as a
DB-open, routing, or allocation failure, use deterministic jittered exponential
backoff from 30 seconds through 10 minutes in the node-local monotonic clock.
Group-specific failures park only that group and preserve any later durable
deadline already observed. Executor-wide allocation and queue-snapshot failures
park the maintenance owner without rewriting every queue entry. Fallback-route
refresh has a separate deadline: its failure suppresses only lost-wakeup
discovery and never blocks an exact durable-intent notification that already
carries its table/group route. A successful pass clears the corresponding
failure state, and an explicit durable wake clears stale per-group scheduler
backoff. Consequently an unhealthy group or metadata fallback cannot turn the
five-second executor poll into a hot retry loop or delay unrelated runnable
groups.

A successful DB pass that still reports durable debt must first restore the
exact group queue entry. If that allocation fails, the executor records global
scheduler backoff and returns before publishing fallback cursor progress. This
ordering makes queue notification and cursor advancement one logical outcome
without putting the fallback cursor itself on the durability path. The periodic
discovery anchor is rewound on that failure, so the same cursor becomes eligible
as soon as scheduler backoff expires rather than waiting another discovery
interval. The same rewind applies when fallback candidate allocation or
failure-backoff enqueue cannot allocate.

Lost-wakeup reconciliation uses a compact node-local routing index rebuilt only
when the metadata epoch changes. Steady-state passes neither clone nor free the
cluster-wide administrative snapshot. The fallback window adapts between 16
and 256 routes per pass to target a 30-minute full rotation at the configured
discovery interval, and advances a separate cursor even when queued debt exists.
The 256-route ceiling bounds DB opens and metadata work; deployments beyond the
resulting supported envelope report an explicit rotation-SLO violation rather
than hiding unbounded recovery latency. If queued work consumes the repair slot,
a safe-consumed prefix advances past non-local or already-queued entries but
stops at the first uninspected local candidate. The fallback is a safety audit;
direct durable-intent notifications and startup reconciliation are the primary
discovery paths, so normal recovery latency does not scale with a full cursor
rotation.

Queue ownership decisions are tri-state. `attempt` schedules eligible debt,
`retry_unknown` retains it in-place while metadata and Raft leadership converge,
and only `skip_nonlocal` removes it. The live membership source produces that
decision only from a known voter set in which the local node is absent; a
known local voter without leadership remains queued through elections. The
lost-wakeup cursor is therefore a
notification safety net, not the correctness path for temporary ownership
ambiguity.

### Startup integration

After primary/derived startup replay stabilizes and before converting remaining
failures to `terminal_degraded`, startup performs discovery and wakes the repair
executor:

```zig
if (initial_index_load_failure or
    try db.hasPendingIndexRepairIntents(alloc))
{
    const discovery = try db.discoverRecoverableStartupIndexFailures(
        alloc,
        1,
    );
    repair_executor.notify(group_id);

    if (discovery.discovered != 0 or
        discovery.already_pending != 0)
    {
        return .{
            .had_debt = true,
            .made_progress = discovery.discovered != 0,
        };
    }
}
```

The terminal branch applies only to failures that have no runnable repair
intent or whose intent is terminal:

```zig
if (try managedDbHasUnrecoverableIndexLoadFailure(alloc, db)) {
    return .{
        .had_debt = true,
        .terminal_degraded = true,
        .made_progress = made_progress,
    };
}
```

This preserves the policy distinction:

- `IncompleteBulkPublish` with valid source artifacts becomes scheduled repair
  debt
- a transient open race is retried without reconstruction
- unknown, corrupt, unsupported, or unreconstructible state remains fail-closed

### Exclusion, fencing, and backoff

Existing group-operation exclusion and the index repair lease remain useful,
but they are not sufficient as the durable state machine. The executor must:

- claim only a locally owned group
- hold the per-index repair lease while advancing an intent
- hold broad group-operation exclusion only for bounded claim/open and final
  activation transitions, not for the full corpus scan
- capture an ownership/fencing epoch and revalidate it before pointer activation
- translate an active writer or repair lease into retryable `busy` debt
- persist `attempt_count`, `last_error`, and `next_retry_at_ms` on failure
- apply exponential backoff with bounded jitter across process restarts
- mark deterministic preflight or policy failures terminal immediately
- clear retry state after successful activation and validation

The startup catch-up backoff continues to protect discovery. The persisted
intent backoff protects expensive reconstruction and prevents restart thrash.

### Status and metrics

The normal table/index status response is a user-facing health surface, not a
dump of the repair state machine. It exposes one compact object, and only while
a repair exists:

```json
{
  "repair": {
    "state": "rebuilding",
    "action_required": false
  }
}
```

`state` is the stable, deliberately small vocabulary `rebuilding | waiting |
paused | failed`. `action_required` is true only for `paused` or `failed`.
Healthy indexes omit `repair` entirely. Do not add attempts, phase names,
timestamps, byte counters, repair IDs, paths, error strings, or speculative
percent-complete estimates to this response. Existing top-level index
availability and rebuilding fields remain authoritative, so clients do not
need to interpret internal phases.

Detailed diagnostics belong in the authenticated admin/operator surface and
structured logs, not the ordinary status response. They may include the repair
ID and trigger, internal phase, automation mode, attempts, timestamps, build and
replay sequences, retry deadline, wait reason, last error, queue age, throughput
estimate, retained replay bytes, pinned snapshot bytes, disk reservation, and
monotonic progress when it can be computed defensibly.
The operator endpoint can return these fields on explicit request and returns
the repair ID when an action is created or attached, allowing stale-action
fencing without making every status consumer carry it.

Prometheus metrics use bounded phase/outcome labels and aggregate counts or
durations; repair IDs, index names, candidate paths, and error strings must not
become unbounded metric labels.

The current node metrics cover queue depth and oldest age, groups inspected and
fallback groups scanned, pass duration, attempts/outcomes, disk waits, and
current/peak/admitted/denied resource-manager disk claims. ResourceManager metrics
cover the memory and retained-WAL pressure involved in repair. Further phase
histograms may be added with bounded labels; they are not part of the public
index response.

While rebuilding:

- primary document reads and ordinary writes remain available, subject to the
  explicit hard-pressure contract below
- unrelated indexes remain searchable
- the affected index returns a stable `index_rebuilding` error mapped to the
  existing `IndexUnavailable` class
- runnable or retryable startup status reports `artifact_rebuild`, not terminal
  degradation; a paused or terminal intent is reported distinctly
- process and node readiness remain healthy unless primary storage itself is
  unavailable. Table/index health still reports degradation, and deployments
  that require every configured index may opt into a strict readiness policy

Only a completed clean checkpoint and cleared load failure return the index to
service.

### Query and operator experience

The API must expose repair state through the normal table/index status response,
not only logs and Prometheus. An explicit query against the affected index
returns a structured retryable error containing:

```text
code = index_rebuilding
retryable = true
retry_after_ms, when known
```

Internal phase and repair identity are available through the authenticated
operator endpoint and correlated logs. They are intentionally omitted from the
normal query error because they do not help an application decide whether to
retry. The index name may be included when the request maps unambiguously to one
index, but clients must not require it for retry behavior (hybrid queries can
depend on more than one unavailable index).

Composed or hybrid searches fail the complete request when a required index is
rebuilding. Partial results are allowed only through an explicit request option
and must identify every omitted index; silent partial search is forbidden.

Writes continue to append primary data and replay records. A request requiring
dense visibility for the rebuilding index waits only up to its normal deadline,
then returns `index_rebuilding`; it does not wait for an unbounded corpus rebuild.
Replay retention and write backpressure continue to apply if the repair cannot
keep up with foreground traffic.

Operator actions follow these rules:

- an explicit repair request attaches to the existing intent instead of
  creating a competing rebuild
- `cancel_current_attempt` stops candidate work at a safe boundary, preserves
  the quarantined root and replay pin, releases admitted resources, records
  retryable debt, and permits the automatic executor to try again later
- `pause_automatic_repair` durably sets `automation = paused`; it stops the
  current attempt at a safe boundary and prevents automatic reclaim across
  restart until `resume_automatic_repair` clears the pause
- operator status distinguishes a paused repair from backoff, resource waiting,
  terminal failure, and active cancellation
- dropping the index cancels its repair, removes its replay pin and intent, and
  garbage-collects candidates
- changing the index configuration cancels the old intent and starts a new
  repair only after the new configuration is durable
- a leadership or placement change cancels local execution; the next eligible
  owner resumes from durable state
- a terminal repair remains visible with a documented operator retry or
  drop/recreate action
- `force` is edge-triggered for a named index: every group in the initial
  bounded cursor traversal receives the explicit generation request once, and
  subsequent convergence/observation passes cannot create another generation.
  Each group records the stable job identity before removing a completed intent,
  closing the crash gap between group completion and API job acknowledgement
- cancelling a named-index repair job means durably stopping that requested
  work, not merely changing the HTTP job record. The job remains nonterminal
  while bounded passes apply `pause_automatic_repair` across the table. The
  owner is signalled before the control waits for group-operation serialization,
  allowing an active rebuild to yield at its next durable boundary. Only after
  the traversal completes does the job become `cancelled`; resumption remains
  an explicit operator action. A durable FIFO of unfinished cancellation jobs
  lets the API maintenance supervisor redispatch them automatically after a
  process restart without scanning the complete job store
- a transient cancellation pass failure preserves `cancel_requested`, cursor,
  and accumulated results, then enters durable exponential backoff. The FIFO
  inspects a bounded window from a process-local round-robin cursor, advancing
  the cursor after every inspected entry. One delayed head window therefore
  cannot block unrelated due work; restart may reset the cursor because the
  durable FIFO and job records remain the source of truth

These actions are idempotent and scoped by `repair_id`. A stale action for an
old repair cannot pause, resume, cancel, or delete its replacement.

### Automatic repair test matrix

The minimum deterministic test matrix is:

1. Create an external dense index, persist vectors, leave
   `__bulk_publish_state`, run managed startup catch-up, and verify the index is
   automatically searchable without an API repair request. Repeat with the
   durable artifact target counter removed to exercise legacy snapshot-plus-
   delta bootstrap.
2. Stop after intent persistence and before candidate creation; restart must
   retain the quarantined root and resume.
3. Stop during snapshot construction; restart must either resume a validated
   checkpoint or discard only the candidate and rebuild it.
4. Write documents after the snapshot floor while the shadow is building;
   replay truncation must remain pinned and the final index must include them.
   For a counterless quarantined dense index, perform artifact insert,
   replacement, and deletion while the bootstrap snapshot is open; verify the
   signed delta, durable target count, and repaired search result.
   Repeat deletes through the normal document path and TTL cleanup, and route
   generated embedding writes/deletes through a remote document-child batch;
   each primary mutation, replay record, and counter delta must be atomic.
5. Stop after candidate readiness but before pointer swap; restart must validate
   and activate the identified candidate without rebuilding it.
6. Stop after pointer swap but before replacement validation, checkpoint
   publication, replay-pin removal, and intent deletion. Each restart must
   deterministically finish the remaining transition without losing the new
   active root.
7. Inject `UnsupportedVersion` and structural corruption; both must remain
   terminally degraded and must not delete the root automatically.
8. Remove or corrupt required source artifacts; preflight must become terminal
   without creating or activating a candidate.
9. Verify only the active local owner repairs a group, then transfer leadership
   during build and immediately before activation.
10. Fill the filesystem during candidate creation, build, readiness publication,
    activation, and cleanup. The active/quarantined state and intent must remain
    coherent after every failure.
11. Corrupt or truncate the intent and candidate marker. Unknown versions and
    malformed data must fail closed.
12. Cancel repair, drop the index, and replace its configuration during each
    long-running phase; verify replay-pin and candidate cleanup.
13. Run multiple broken indexes across multiple groups and verify the node-wide
    concurrency limit, fairness, and absence of startup inspection head-of-line
    blocking.
14. Reproduce a 100,000-document interrupted external-vector ingest; restart
   without an API call and verify document count, dense count, search results,
   zero remaining load failures, and removal of the repair intent.
15. Run at least one million documents with concurrent writes and queries,
    repeated forced termination, bounded memory/WAL/disk growth, and recall
    comparison against a clean build.
16. Pause truncation immediately before pinned-snapshot acquisition, race the
    truncator against snapshot creation, and verify that either the provisional
    zero pin or final floor protects every required replay record across crash.
17. Exercise every transition in the state table twice and crash between its
    filesystem effect and durable phase update; restart must converge without
    activating an unvalidated candidate or deleting the active root.
18. Run full durability and relaxed/`none` durability modes, including LMDB
    `no_sync`; verify no applied watermark or truncation advances before the
    explicit durable sync and that sync failure preserves replay debt.
19. Replace replica identity and root generation, promote an HA replica, move
    and split a group, and perform logical and physical restore. A local
    candidate may resume only on an exact identity match.
20. Sustain writes above and below candidate catch-up throughput. Barrier hold
    time must remain below the configured maximum; non-convergence must return
    to `waiting_for_convergence` and eventually apply documented backpressure.
21. Drop executor notifications and restart the executor while intents exist;
    adaptive bounded durable discovery must reclaim each eligible intent within the
    configured scan SLO without duplicate execution.
22. Cancel the current attempt, durably pause automatic repair, restart, and
    resume. Only cancel permits automatic retry; pause survives restart.
23. Exhaust soft and hard replay, snapshot-pin, and disk budgets. Verify
    reservation fairness, the stable `dense_repair_backpressure` response with
    `Retry-After`, hysteretic recovery, and absence of filesystem exhaustion.
24. Queue differently sized repairs long enough to exercise size-aware
    selection and aging. Assert bounded oldest-intent age and no starvation.
25. Hold enough dense-repair working-set capacity to cross the soft background
    budget without reaching the hard limit. Counter bootstrap and shadow batch
    planning must defer without consuming additional bytes or recording a hard
    rejection.
26. Under hard replay pressure, verify standalone writes and only the active
    managed-Raft leader reject uncommitted work with the compact retryable 429;
    committed entries must continue applying on every replica. Remove pressure
    and verify admission recovers without a configuration change.

This track prevents wipe/re-ingest and does not require changing the successful
normal-ingest path. The streaming-session track separately prevents ordinary
replay from creating this failure state in the first place.

## Generation Publication

Generation replacement is not the normal replay mechanism. It is appropriate
when an explicit operation needs both:

- permission to build cross-transaction structural state, and
- continued service from a previous complete index

Examples include a full rebuild of a live index or a large import replacing an
existing index.

Bulk publication defaults to an isolated candidate. In-place publication is
allowed only for a brand-new root that has never been advertised, an explicitly
offline index with no prior searchable generation, or a legacy compatibility
path. A live active index is never placed into an in-place incomplete
publication window.

The generation flow is:

```text
keep generation N active
    -> build generation N+1 in a shadow root
    -> catch it up to the current derived sequence
    -> durably mark it ready
    -> satisfy bounded convergence thresholds
    -> acquire the final apply barrier for at most the configured pause
    -> perform bounded final catch-up
    -> atomically update the active-root pointer
    -> retire generation N after readers release it
```

Generation N remains a durable rollback anchor through replacement open,
validation, clean-checkpoint publication, and repair-intent cleanup. Inactive
shadow roots and stale canonical-root contents are garbage-collected only after
no durable intent can refer to them. Startup orphan cleanup is suppressed when
repair state exists or is malformed.

The existing repair shadow build and active-root pointer swap should be
refactored into reusable build and publication primitives before adding a
second generation implementation.

A correctness-first implementation may rebuild a candidate from all primary
artifacts. That is acceptable for explicit rebuild/import but too expensive for
ordinary write bursts. An optimized implementation may later add a cheap LSM
generation fork that shares immutable runs while keeping separate manifests,
mutable state, and WAL ownership.

Generation publication uses the same pinned-snapshot, durability-mode,
identity/fencing, disk-reservation, and bounded-activation primitives as repair.
It must not introduce a parallel implementation of those correctness rules.

## Implementation Plan

The numbered phases describe dependencies, not a mandatory release order.
There are three independently reviewable workstreams:

- automatic restart repair may land before streaming replay, but its durable
  repair core and node-local scheduling/UX should be separate focused PRs
- streaming replay prevents new normal-write incidents and requires crash and
  performance qualification
- generation publication is a separate project for explicit rebuild/import
  continuity

### Phase 0: Prove and classify the current invariants

1. Inventory every HBC branch controlled by `bulk_ingest_session_depth`.
2. Classify each branch as LSM batching, cross-batch publication, metadata
   publication, search visibility, cache behavior, workspace lifetime, or
   maintenance behavior.
3. Add deterministic reopen tests at individual HBC batch boundaries.
4. Complete the insert, overwrite, move, delete, and coverage idempotency audit.

Acceptance:

- every raw depth decision has an explicit owner and recovery rationale
- a normal non-session HBC batch is proven structurally reopenable
- any failed idempotency case is fixed or has an approved durable batch-ID
  design before streaming rollout

### Phase 1: Establish the streaming contract

1. Add `WriteSessionKind` and capability predicates to HBC.
2. Separate LSM session batching from cross-batch HBC deferred state.
3. Make metadata, topology, Flat-RaBitQ node keys, and required quantized state
   publish atomically at streaming batch finish.
4. Add streaming replay begin/finish/abort APIs.
5. Add corresponding `IndexManager` APIs.
6. Switch asynchronous dense catch-up to streaming replay sessions.
7. Keep batch-level bulk optimizations enabled initially.
8. Add per-session and node-wide resource bounds.
9. Make the backend report whether finish established a durable checkpoint;
   preserve durable-finish-before-applied-sequence ordering in every mode.

Acceptance:

- normal dense catch-up never writes `__bulk_publish_state`
- every committed streaming batch is reopenable
- abort leaves a query-consistent, replayable state
- a killed process resumes through replay without rebuilding the index
- relaxed/`none` durability never advances a durable watermark until explicit
  sync succeeds
- existing dense visibility and sync-level tests continue to pass

### Phase 2: Crash and idempotency qualification

Add deterministic hooks and tests for every interruption point in the recovery
table. Exercise inserts, overwrites, deletes, mixed operations, external vectors,
managed vectors, full durability, and relaxed/`none` durability.

Acceptance:

- no duplicates or lost deletes after restart
- durable document and dense counts converge
- applied sequence never skips unapplied work
- nearest-neighbor results and recall match a clean ingest
- no streaming interruption yields `IncompleteBulkPublish`

### Phase 3: Performance qualification

Compare:

1. the current long-lived HBC bulk session
2. the proposed streaming session with LSM coalescing
3. independent durable replay batches with no session reuse

Measure:

- documents and vectors per second
- write p50, p95, and p99
- dense replay lag and maximum lag
- HBC apply and finish time
- LSM manifest writes and WAL bytes
- L0 run count and maintenance debt
- peak dense apply workspace
- restart and replay duration
- search latency and recall after ingest

Use at least:

- a 100,000-document external-vector ingest
- a one-million-document sustained-ingest and restart workload
- mixed overwrite/delete traffic
- small steady-state writes
- bursty writes below and above the replay coalescing threshold
- multiple dense indexes and multiple active groups per node
- forced termination during an active session
- repeated cooperative yield/reopen cycles during a candidate snapshot build
- repeated cooperative yield/reopen cycles during pre-activation replay catch-up

Record the baseline and candidate commit IDs, build mode, backend and durability
configuration, dataset/version and random seeds, CPU model/count, memory, disk,
filesystem, and kernel. Run each reported workload at least five times after a
documented warm-up; retain raw results and report median plus dispersion or a
confidence interval. Compare on the same isolated hardware, and explain rather
than silently discard outliers.

The current single-run development signal is 1,000,000 documents in 471.709
seconds under `ReleaseFast`, using 1,000-document client batches. It includes an
exact indexed count and replay checkpoint, durable reopen, and post-restart
search validation. Peak pressure working set was 874,287,160 bytes (peak RSS was
2,723,430,400 bytes), below the 2 GiB working-set gate. This is a qualification
datapoint, not a replacement for the repeated one-million-document
baseline/candidate comparison above.

Acceptance:

- streaming throughput is within 10% of the current bulk session and write p99
  is within 15%, unless an explicitly reviewed result accepts a different
  tradeoff
- it materially outperforms fully independent durable batches, or the simpler
  independent-batch design is selected instead
- peak memory, WAL growth, and L0 debt remain inside configured node budgets
- unpaced ingest cannot create a permanently unloadable dense index
- dense repair slicing stays within the accepted throughput threshold and a
  large candidate cannot monopolize the node repair slot beyond one slice plus
  one resource-sized batch

### Phase 4: Durable automatic repair core

1. Add load-failure recovery classification.
2. Add rebuildability, artifact, disk, configuration, and ownership preflight.
3. Add the durable repair intent, candidate identity, and replay-retention pin.
   Bind all local artifacts to DB, group, replica, and root-generation identity
   and acquire snapshots through the atomic pinned-snapshot primitive.
4. Refactor shadow construction so it builds directly from a status-only
   quarantined configuration without deleting or reopening the poisoned root.
5. Add the complete restart transition table, ready, bounded convergence,
   activation, validation, cleanup, and stale-candidate recovery.
6. Make operator repair use the same state machine.
7. Add crash tests for every durable transition.

Acceptance:

- an existing `IncompleteBulkPublish` index can be repaired through the shared
  durable engine without wipe/re-ingest
- queries never observe an empty or incomplete replacement
- concurrent writes after the build floor are retained and included
- the quarantined root remains untouched until a validated replacement is
  active
- unknown failures remain fail-closed
- malformed intents, missing artifacts, and insufficient disk fail safely
- replica replacement, move/split, promotion, and backup/restore cannot resume
  a candidate belonging to a different local root

### Phase 5: Repair scheduling and production UX

1. Add fast startup discovery and the bounded node-local repair executor.
2. Add fair scheduling, node-wide resource admission, persistent retry/backoff,
   disk reservation, periodic lost-wakeup discovery, and ownership fencing.
3. Add structured query errors, status API fields, metrics, and readiness rules.
4. Add cancel-attempt, durable pause/resume, drop, configuration-change,
   leadership-transfer, and hard-backpressure handling.
5. Qualify multiple broken indexes across multiple tables and groups.

Acceptance:

- one long repair does not block startup inspection of later groups
- default concurrency is one reconstruction per node and never exceeds the
  configured resource budget
- direct durable-intent notifications enqueue the exact group without a scan;
  after restart, managed startup catch-up rediscovers every owned group, and
  the bounded periodic cursor supplies a final lost-wakeup audit. The supported
  groups-per-node envelope must qualify that cursor's full-rotation time rather
  than treating the per-pass discovery interval as an end-to-end SLO
- queue age remains within the qualified repair-capacity SLO, with no
  starvation under bounded round-robin selection
- process restart does not reset retry backoff or duplicate a repair
- an existing `IncompleteBulkPublish` index repairs after restart without an
  explicit API call
- primary reads and ordinary writes remain available during repair
- the affected index consistently returns `index_rebuilding` until validation
- all repair state, candidates, and replay pins are eventually cleaned up
- reversible activation work aborts before the configured absolute deadline;
  dense publication performs no size-dependent close/reopen under the fence,
  and the non-interruptible post-pointer durability tail meets the qualified
  write-pause SLO

### Phase 6: Shadow generation publication

1. Refactor repair shadow construction into reusable replacement-generation
   APIs.
2. Add a durable candidate-generation manifest and ready marker.
3. Retain the previous generation until active readers release it.
4. Add crash tests before and after candidate readiness and pointer activation.
5. Use this path only for explicit operations that require continued service.

Acceptance:

- the previous complete generation remains searchable until activation
- restart deterministically chooses the active complete generation
- incomplete candidates never become active
- retired generations are garbage-collected safely

## Rollout

Streaming replay is the ordinary-write correctness path, not a user-selectable
index behavior. It is enabled unconditionally and has no public or
resource-configuration toggle. Production rollout uses the normal staged binary
deployment and canary process; rollback means deploying the prior binary rather
than retaining the unsafe ordinary-replay bulk-publication path in the runtime.

Emit counters for:

- streaming sessions opened, finished, and aborted
- bulk publication sessions opened, finished, and aborted
- streaming replay batches reapplied after restart
- incomplete publication quarantines
- automatic reconstruction attempts and outcomes
- repair executor queue depth, oldest age, admission delay, projected
  completion, durable-rescan recoveries, and active count
- repair replay-pin age and retained replay bytes
- repair snapshot age and pinned-version bytes
- candidate actual/reserved bytes, stale candidates removed, and cleanup
  failures
- repair phase duration, retry count, and terminal outcomes
- convergence lag, estimated and actual activation pause, and barrier retries
- dense-repair throttling and backpressure rejections
- session finish latency and maximum replay lag

Rollout order:

1. tests and local benchmarks
2. fault-injection deployment canary with forced process termination
3. staged binary rollout with ordinary dense replay using streaming sessions
4. removal of any unreachable legacy ordinary-replay bulk-session plumbing
   after qualification

Automatic repair rolls out separately:

1. classification and status reporting without automatic execution
2. operator-triggered execution through the new state machine
3. automatic execution in fault-injection and canary environments
4. automatic execution by default with concurrency one
5. higher concurrency only after multi-group resource qualification

General availability requires the deterministic crash matrix to pass on every
supported durable backend mode, no unresolved correctness-severity failures,
reproducible performance results within the accepted thresholds, and evidence
that the largest supported incident stays within memory, disk, replay,
snapshot, activation-pause, queue-age, slice-overhead, and projected-recovery
SLOs. Dense reconstruction already has durable resumable scan checkpoints;
incident qualification must verify their fairness and throughput. Any remaining
nonpreemptive index-family builder that cannot meet those bounds requires the
same checkpointed-yield treatment before GA.

## Non-Goals

This work does not:

- make external-vector query embedding a server responsibility
- remove the derived replay log or add a second per-index WAL
- make every index load error automatically repairable
- use full generation replacement for routine writes
- guarantee synchronous dense visibility for `sync_level: write`
- eliminate resource backpressure or bounded replay windows
- keep a previously broken in-place generation searchable while it is repaired

## Final Architecture

```text
ordinary write
    -> durable primary document + derived replay record
    -> bounded dense streaming replay session
    -> structurally valid batch commits
    -> backend-confirmed durable finish or explicit sync
    -> applied-sequence publication
    -> crash recovery by idempotent replay

explicit import/rebuild
    -> bulk publication in an isolated candidate when prior service matters
    -> bounded convergence and final catch-up
    -> atomic active-generation swap
    -> retired-generation cleanup

legacy/interrupted in-place bulk publication
    -> quarantine
    -> allowlisted discovery + rebuildability preflight
    -> replica-bound durable repair intent + atomic pinned snapshot
    -> resource-admitted shadow reconstruction
    -> bounded convergence + time-bounded fenced pointer activation
    -> replacement validation
    -> pin/intent/candidate cleanup
```

The design keeps bulk algorithms where they provide throughput, but removes the
assumption that ordinary write batching is an all-or-nothing index publication.
It also keeps a damaged generation quarantined until a complete replacement is
durably active, so automatic recovery cannot turn an explicit unavailable error
into silent empty or partial search results.
