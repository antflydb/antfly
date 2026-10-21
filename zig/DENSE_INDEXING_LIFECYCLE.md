# Dense Indexing Lifecycle

## Status

Implemented. Ordinary dense replay uses reopenable streaming sessions, and
interrupted bulk publications are repaired automatically through a durable,
restart-resumable shadow-generation state machine. Single-run qualifications at
50,000, 100,000, and 1,000,000 documents (including a provisioned-path
external-vector guardrail) pass exact count/checkpoint, bounded memory, durable
reopen, and post-restart search, and confirm that normal writes never enter
bulk publication or repair. Rollout gates still open: repeated
baseline/candidate performance runs ([Phase 3](#phase-3-performance-qualification))
and the full deterministic fault matrix
([Automatic repair test matrix](#automatic-repair-test-matrix)).

Where each implemented behavior is specified:

| Area | Section |
| --- | --- |
| Streaming vs. bulk-publication sessions | [Session Model](#session-model), [HBC session capabilities](#hbc-session-capabilities) |
| Derived-backlog admission (200/100 hysteresis, 16-sequence window) | [Derived Replay](#derived-replay) |
| Load-failure allowlist and rebuild preflight | [Load-failure classification](#load-failure-classification) |
| Artifact counter bootstrap, atomic delete/TTL counters, stale-upsert classification | [Artifact counters and coverage](#artifact-counters-and-coverage) |
| Durable intent, replay pin, identity binding | [Durable repair intent](#durable-repair-intent), [Replay retention during repair](#replay-retention-during-repair) |
| Disk and cache admission | [Disk-capacity ownership and admission](#disk-capacity-ownership-and-admission), [Cache ownership and admission](#cache-ownership-and-admission) |
| Phase transitions, audit grants, convergence, activation | [Repair state transitions](#repair-state-transitions), [Bounded convergence and activation](#bounded-convergence-and-activation) |
| Executor, fair queue, lost-wakeup cursor, backoff | [Repair engine and scheduler](#repair-engine-and-scheduler) |
| Compact status, metrics, operator actions, job quarantine | [Status and metrics](#status-and-metrics), [Query and operator experience](#query-and-operator-experience) |
| Split/merge status and split-key discovery | [Group lifecycle interactions](#group-lifecycle-interactions) |
| Generation replacement and retirement | [Generation Publication](#generation-publication) |
| Native-v2 physical versioning, rollback, backup | [Physical index versioning and rolling upgrades](#physical-index-versioning-and-rolling-upgrades) |

> **Relocated:** The per-feature implementation-status bullets, completed
> implementation phases 0, 1, 2, 4, and 6, the physical-versioning fix
> narratives, and the r128 measurement →
> [history file](../docs/design/dense-indexing/history/lifecycle-implementation-log.md).

This document defines how ordinary dense replay, explicit bulk construction,
and interrupted HBC publication differ. Normal writes are crash-replayable
without giving up the batching and LSM coalescing that make dense ingest fast.

The core decision is:

- normal asynchronous dense indexing uses a streaming replay session
- true bulk construction uses a bulk publication session
- only bulk publication may leave cross-batch HBC state that requires an
  incomplete-publication marker
- generation replacement is reserved for explicit import/rebuild operations,
  not used for every ordinary write burst

## Problem

Before this design, the asynchronous dense worker opened an HBC bulk-ingest
session while catching up normal writes, although normal API writes never enter
an explicit table bulk-ingest window (those are reserved for rebuild and
import). Opening that session durably writes `__bulk_publish_state =
incomplete`, and HBC refuses to reopen an index while that marker exists. That
is correct for a session with deferred structural mutations that are unsafe
until final publication, but unnecessarily strong for normal replay, where every
committed batch can be structurally reopenable. A process interruption turned
ordinary writes into `IncompleteBulkPublish` and an index quarantined until
explicit reconstruction.

The primary documents and embedding artifacts stay durable, so the index is
reconstructible; the defect was the write-session contract, not data loss.
Streaming sessions remove the ordinary-write cause, and automatic repair
handles the remaining legitimate incomplete publications.

## Terminology

"Bulk ingest" used to cover several independent optimizations and safety
properties. This design separates them:

- **Bulk-optimized batch**: one bounded apply call using write optimizations
  (grouped centroid routing, coalesced leaf mutation, batch-finish leaf
  splitting, quantized routing, reduced flush and manifest overhead, and
  empty-index bulk building when the batch is large enough). It does not by
  itself require an index-wide incomplete-publication marker.
- **Streaming replay session**: a short-lived sequence of bulk-optimized,
  independently valid batches. The LSM backend may coalesce work and defer
  flush/maintenance across the session, but each committed HBC batch leaves a
  structurally valid state; an interrupted session recovers by replaying from
  the last durable applied sequence.
- **Bulk publication session**: a construction session for explicit import,
  rebuild, or true bulk construction that may defer HBC structural state across
  committed transactions until a final publication boundary. An interrupted
  session may be unsafe to reopen and needs either an incomplete-publication
  marker plus artifact reconstruction, or isolation in a shadow generation that
  is not yet active.

## Current Code Shape

Paths are relative to `zig/`:

| File | Role |
| --- | --- |
| `pkg/antfly/src/api/table_writes.zig` | Ordinary uploads start no explicit table bulk windows; `catchUpManagedDb` runs managed startup catch-up and repair discovery |
| `pkg/antfly-embedded/src/storage/db/derived/catch_up_policy.zig` | Dense replay coalescing, session reuse, and window limits |
| `pkg/antfly-embedded/src/storage/db/derived/runtime_types.zig` | Opens and closes per-index catch-up state |
| `pkg/antfly-embedded/src/storage/db/db.zig` | `beginDerivedCatchUpSessionAsync` / `finishDerivedCatchUpSessionAsync`; repair entry points `discoverRecoverableStartupIndexFailures`, `advanceIndexRepairIntent`, `beginPinnedIndexRepairSnapshot` |
| `pkg/antfly-embedded/src/storage/db/mutation_materialization.zig` | `applyDerivedBatchToIndexContextProfiled` |
| `pkg/antfly-embedded/src/storage/db/catalog/index_manager.zig` | Maps storage batch mode to HBC batch options, exposes streaming-session APIs by index name, classifies load failures |
| `pkg/antfly-embedded/src/storage/db/derived/index_repair_state.zig` | Replica-local repair intent and replay-pin checkpoint |
| `pkg/antfly-embedded/src/storage/hbc_adapter.zig` | `WriteSessionKind`, session depth, deferred state, and `__bulk_publish_state` |

The dense apply path distinguishes batch-level optimizations from
session-finish deferrals: leaf splits finish at the batch boundary, and neither
leaf splits nor quantized rebuilds are deferred to a bulk-session finish by
default. That is what lets streaming replay keep the useful batch and LSM
behavior without index-wide incomplete-publication state.

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

## Session Model

HBC has an explicit write-session kind (`hbc_adapter.zig`):

```zig
pub const WriteSessionKind = enum {
    streaming_replay,
    bulk_publication,
};
```

Mixed nested session kinds are rejected (`WriteSessionKindMismatch`) rather
than silently combined.

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

Managed index create/recreate keeps enrichment quiesced until its synchronous
generation build and replay plan close, so generated writes cannot mix
streaming-replay and bulk-publication sessions on the same HBC index. Primary
writes and unrelated query serving remain available meanwhile.

## HBC session capabilities

LSM batching and HBC cross-batch deferred state are separate concerns. HBC
tracks `write_session_depth` and `write_session_kind`, and safety decisions use
the `HBCIndex` capability predicates `lsmSessionBatchingActive`,
`crossBatchPublicationActive`, `mustPublishMetadataPerBatch`,
`shouldPublishSearchStatePerBatch`, and `shouldSuppressRoutineMaintenance`. No
safety decision outside session begin/finish code may depend on the raw depth.

- LSM batch mode, mutable-state coalescing, deferred commit flush, and bounded
  routine-maintenance deferral may be enabled for either session kind.
- `stageNodeKeyPut`, `stageNodeKeyDelete`,
  `shouldDeferQuantizedRebuildToBulkFinish`, and
  `shouldDeferLeafSplitToBulkFinish` apply only while
  `crossBatchPublicationActive()`.
- `persistBulkPublishState` and `clearBulkPublishStateTxn` are used only for an
  in-place `bulk_publication`.
- `flushMetadata` publishes reopen-critical metadata in every streaming batch;
  it may suppress that only when the matching topology and node-key changes are
  also isolated behind a bulk publication boundary. Flat-RaBitQ
  centroid-directory node keys publish at streaming batch finish rather than
  session finish.
- Search caches, published root/count state, workspace clearing, and
  maintenance suppression are classified independently; none inherits
  publication semantics merely because LSM coalescing is active.

### Streaming session APIs

`HBCIndex` exposes `beginStreamingReplaySession()`,
`finishStreamingReplaySessionWithOptions(options:
backend_types.BulkIngestFinishOptions)`, and `abortStreamingReplaySession()`.
The finish completes batch-local HBC work, flushes the LSM session to a durable
reopenable boundary, refreshes published search state, and releases bounded
session workspace; it never clears an incomplete marker because it never
creates one. Abort closes the LSM batching scope, releases workspace, and
republishes a consistent view of already committed batches. It neither rolls
back committed batches nor advances the applied sequence, so restart or the
next catch-up pass may reapply them. The `beginBulkIngestSession` family keeps
the stronger publication semantics for explicit bulk construction.

`IndexManager` exposes the same distinction by index name
(`beginDenseStreamingReplaySessionByName`,
`finishDenseStreamingReplaySessionByNameWithOptions`,
`abortDenseStreamingReplaySessionByName`). Replay batches keep
`BatchOptions.mode = .bulk_ingest` where beneficial; that flag selects batch
algorithms and does not imply an unsafe publication window.

## Derived Replay

`beginDerivedCatchUpSessionAsync` and `finishDerivedCatchUpSessionAsync` use
the streaming APIs. Session reuse remains useful to:

- coalesce small tails briefly
- amortize LSM flush and manifest work
- apply a bounded number of replay windows
- finish after the configured idle period or a forced visibility boundary

The dense replay window is bounded by records, items, bytes, and resource
manager pressure. These limits are operational backpressure controls, not
durability boundaries.

### Derived-backlog admission

Ordinary writes do not merely *report* replay debt. After the primary commit,
`.write`, enrichment, full-text, and full-index paths consult the executor's
backlog-admission target; only the proposal-only path is exempt. The target is
owned by `ResourceManager`, exposed through the derived-executor vtable, and is
identical for manual and threaded backend runtimes.

Admission uses hysteresis rather than draining every producer to the newest
sequence. The node policy (`ResourceManager.derived_backlog_high_sequences` /
`derived_backlog_resume_sequences`) stops a producer above 200 pending replay
sequences and waits only far enough to resume at 100. When sequence-only debt
crosses the high watermark, the asynchronous urgency target is bounded to a
16-sequence window (`derived_backlog_throttle_window_sequences`); foreground
requests never drain that window themselves. Encoded replay-byte pressure
(the `derived.backlog` budget, 128 MiB soft / 192 MiB hard) computes an
equivalent low-water target. If aggregate `lsm.in_memory_state` reaches a
pressure whose policy action is `throttle_writes` or `reject_work`, the
producer waits through at least the oldest pending sequence, letting the
active bounded replay window publish and release pinned LSM state before more
foreground mutations are admitted.

Backlog accounting may not disappear under allocator pressure. If appending a
per-sequence accounting entry fails, the tracker switches to an allocation-free
overflow interval. It continues exact saturating byte accounting and returns
the newest overflow sequence as the admission target. Partial replay does not
release that aggregate early; reaching the interval tail releases its exact
bytes and restores precise per-sequence tracking. This deliberately trades a
temporarily larger drain for fail-closed memory and WAL admission.

Managed-Raft writes apply this admission on the confirmed leader before
proposal. Followers forward normally and committed apply always bypasses the
gate, preserving replica determinism. The normal path performs one combined
`ResourceManager` pressure check per distinct manager and scans the exact
group/table writer cache only after hard replay pressure is present.

These are internal resource-policy values, not index configuration or public
cache toggles. A composed runtime may supply one resource manager to multiple
DBs; the executor abstraction carries only the resulting target sequence, so
backend choice does not change correctness or admission behavior.

### Session resource bounds

Streaming sessions have per-index and node-wide bounds; the dense LSM defaults
(128 MiB mutable threshold with a four-times bulk multiplier) would otherwise
let bulk-style batching retain a large mutable working set per active index. A
streaming finish is forced at any configured bound: session records or vector
items, session input bytes, estimated HBC and LSM workspace bytes, session age
or idle time, dense replay lag, WAL retention soft or hard pressure,
resource-manager memory or I/O pressure, or a synchronous dense-visibility
waiter. A node-wide admission limit bounds concurrently active dense replay
sessions. The resource manager may shorten a window or force a finish, but
never turns a streaming session into cross-batch publication.

Streaming finish keeps this order: durably finish the dense session, flush that
index's pending applied sequence, notify query visibility, and allow replay
truncation only after all managed indexes have persisted their required
watermarks.

### Group lifecycle interactions

- Control-plane group status never cold-opens a root participating in an active
  split or merge. It reports durable transition readiness plus the most recent
  merged/store snapshot and live Raft routing; an ownership race detected after
  the cache probe falls back to the same non-opening path instead of aborting
  the complete node status refresh.
- Split-key discovery borrows the existing managed writer and reads primary
  keys without draining enrichment or derived-index maintenance. It finalizes
  only a pending primary-store auto-bulk boundary needed for primary
  visibility; full replay/index draining is reserved for a DB that is actually
  closing.
- Mutable LSM snapshots have generation-specific reader references, so an old
  replay scan cannot retain unrelated later snapshots; write transactions keep
  backend-close fencing without pinning versions unless they open a cursor.

## Idempotency Audit

Streaming replay without an incomplete marker depends on safe reapplication.
The audit and its tests cover:

- inserting an already-present vector ID with the same value
- overwriting an existing vector ID
- moving a vector between leaves on overwrite
- deleting an already-absent vector ID
- delete followed by insert in one coalesced window
- multiple writes to one document collapsed to the final value
- coverage accounting when a batch is replayed
- applied-sequence sidecar and embedded HBC checkpoint agreement

Any operation found not to be idempotent must be fixed or given a durable
per-batch commit identity; it must not reintroduce the incomplete marker.

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

- previously interrupted or structurally invalid local generations
- interrupted explicit bulk publications
- interrupted in-place rebuilds

Automatic restart repair replaces wipe/re-ingest as the recovery procedure for
these states. It is distinct from generation publication: the affected index
remains unavailable while this repair runs.

### Recovery owner

Managed startup catch-up, not the HTTP repair-job store, owns automatic
restart repair. It already runs outside request handling, selects locally
owned groups from placement and leadership state, serializes group operations
through the local write owner, publishes startup/progress/degraded status, and
applies bounded exponential backoff to zero-progress retries. Making the job
store the correctness mechanism would add a second ownership and scheduling
system, and its table-scoped job shape does not identify the locally owned
group being repaired.

The job store remains the operator mechanism for explicit repair,
cancellation, and observability, and it durably retains unfinished operator
cancellation traversals: the API maintenance supervisor resumes the oldest
queued traversal after restart, one bounded pass at a time through
`BackendRuntime`, without a client calling the advance endpoint. Automatic and
operator-triggered repair call the same DB repair engine and durable intent;
they differ only in scheduling and policy.

### Load-failure classification

Startup classifies index load failures narrowly
(`IndexManager.loadFailureRecoveryAction` in
`storage/db/catalog/index_manager.zig`):

| Load error | Action |
| --- | --- |
| `IncompleteBulkPublish`, `MissingPostingCheckpoint` | `rebuild_from_artifacts` (dense HBC indexes only; other kinds map to `manual_intervention`) |
| `TableReadChurn` | `retry_open` |
| anything else | `manual_intervention` |

Unknown corruption, unsupported versions, invalid configuration, and missing
source artifacts stay terminally degraded until an operator intervenes; the
queue classification is as narrow as the execution allowlist so an unfamiliar
future producer cannot create an endlessly runnable automatic-repair entry.
Transient open failures stay on the existing open-retry path.

Classification authorizes a rebuild attempt, never deletion of an index root.
Before creating a candidate, a rebuild preflight verifies that the index kind
has a deterministic artifact reprocessor; the configuration hash matches the
intent (or starts a new one); the required primary embedding or other derived
artifact source is configured with coverage metadata consistent enough to start
(full per-artifact validation stays part of the build); local disk and
resource-manager budget suffice; and the local process still owns the group and
may publish it. An allowlisted trigger with missing source artifacts becomes
terminal repair debt without modifying the quarantined root.

### Artifact counters and coverage

Artifact-backed dense coverage uses the durable source counter as the
authoritative cardinality invariant at snapshot, replay advancement, and final
fenced activation boundaries:

- An absent counter is explicit bootstrap debt (`artifact_counter_missing`); a
  separately committed status snapshot is never substituted as a correctness
  target.
- Bootstrap derives the target count from a stable snapshot plus a signed
  concurrent-write delta. Only the two short marker/finalization boundaries hold
  the apply lock, and restart safely repeats the scan under the existing durable
  repair intent. A random per-attempt nonce prevents a stale concurrent scanner
  from finalizing another attempt's delta. The bootstrap scan is admitted from
  the same repair slice before it publishes a marker or opens its snapshot.
- Counter routing is derived from the complete index catalog, including
  quarantined status-only configs, so writes remain accounted while the
  affected runtime index is absent. Document-only commits bypass catalog
  construction entirely.
- Document deletion, TTL cleanup, and remote document-child artifact batches
  commit artifact writes/deletes, the derived journal record, and dense
  target-counter mutations in one primary-store transaction. Duplicate keys and
  delete-then-rewrite batches are normalized to their final state before the
  counter delta is calculated. No path deletes artifacts in a separate
  transaction.
- Dense and sparse replay distinguish a genuinely missing source artifact from
  one removed by a later document deletion. A stale upsert whose source
  document is already absent advances without creating repair debt; a missing
  or corrupt artifact for a live source document fails closed.
- Ordinary counter/index differences are ignored while derived replay is behind
  its source target. After replay convergence, a deficit may be filled in place.
  A surplus, missing counter, configuration mismatch, `repair_required`, or
  allowlisted backend structural-validation failure schedules a
  shadow-generation rebuild and fails the affected index closed. Only an
  explicit operator rebuild of a proven healthy generation keeps serving during
  construction.
- Shadow activation rejects either deficit or surplus. Cardinality detects
  missing/additional entries but is not a cryptographic proof of key-set
  identity; key-set correctness derives from constructing an empty shadow
  exclusively from the stable source snapshot plus fenced replay. A per-write
  cryptographic digest is not justified unless fault injection demonstrates an
  equal-count substitution that bypasses the backend's own integrity checks.

### Durable repair intent

Generation paths are relative to the index namespace, which can differ from
its primary container path. External roots configure `generation_storage` for
shadow directories, readiness manifests, active-root pointers, replay sidecars,
and retired index deletion. Lite keeps that publication state in the same
`.aflite` file as its index data; publication must not create host-side shadow
directories. Orphan discovery, canonical-root pruning, and candidate byte
accounting use the same backend. Empty algebraic generation directories retain
a durable marker until paged primary-key cleanup finishes, including across
restart. Capacity reservations subtract materialized candidate bytes so they
cover only remaining growth and cleanup headroom.
External dense readiness requires complete artifact publication, independently
of whether every document supplies a vector.

Before starting reconstruction, persist an index repair intent in the DB's
replica-local repair checkpoint. It shares the local durability boundary needed
by replay pins, but it is not user data and is not emitted by logical table
replication.

The authoritative definition is `IndexRepairIntent` in
[`index_repair_state.zig`](pkg/antfly-embedded/src/storage/db/derived/index_repair_state.zig).
Intents and their `IndexRepairReplayPin`s live in one checksummed
`index_repair.checkpoint` file (magic `AFIDXRP1`), rewritten atomically
(fsync+rename natively; equivalent atomic publication for externally owned
roots such as `.aflite`), so restart never observes an intent without its pin
or vice versa. The intent records:

- identity: `repair_id`, `db_identity`, `group_id`, `replica_id`,
  `root_generation`, `index_name`, `kind`, `config_hash`
- cause: `trigger` (`incomplete_bulk_publish`, `operator_generation_rebuild`,
  `root_generation_rebuild`, `artifact_coverage_mismatch`,
  `artifact_counter_missing`, `projection_generation_invalid`,
  `operator_generation_validation`, `replay_artifact_unavailable`,
  `storage_format_migration`, `catalog_admission`,
  `artifact_baseline_adoption`) and `work_class` (`repair` or `initial_build`)
- CAS state: monotonic `revision` and checkpoint-wide `control_revision`
- operator job binding: `operator_job_id`, `operator_job_created_at_ms`
- candidate and rollback anchor: `candidate_relative_path`,
  `previous_pointer_captured`, `previous_active_relative_path`
- sequences and cursors: `detected_sequence`, `build_floor_sequence`,
  `build_source_guard`, `build_resume_key`/`build_reprocessed`,
  `source_replay_resume_key`/`source_replay_reprocessed`/`source_replay_state`,
  `candidate_applied_sequence`, `target_sequence`
- disk plan: `estimated_candidate_bytes`, `planned_disk_bytes`
- lifecycle: `phase` (`detected` … `terminal`, plus `rolling_back`),
  `attempt_count`, `failure_streak`, `next_retry_at_ms`, `started_at_ms`,
  `updated_at_ms`, `owner_epoch`, `automation` (`enabled`/`paused`),
  `last_error`

Persisted strings have length and total-size limits; unknown checkpoint
versions or enum values fail closed. Explicit generation rebuilds for dense,
sparse, graph, and full-text indexes use the same durable intent and
post-pointer restart reconciliation. Embedded/Lite databases that omit the
managed durable-repair checkpoint treat repair-intent discovery as empty, so
normal index deletion stays available; creating managed durable repair state
there still fails closed.

The intent is authoritative across restart of the same replica. `db_identity`,
`group_id`, `replica_id`, and `root_generation` bind it to the local
derived-state root that owns the candidate; `repair_id` and
`candidate_relative_path` identify exactly which shadow may be resumed or
discarded; persisted retry metadata keeps repeated restarts from resetting a
failing repair loop. Candidate paths must be canonical relative paths below the
DB repair-shadow root; absolute paths, `..`, and paths outside it are rejected
before filesystem access.

Repair intents, replay pins, candidate paths, and active-root pointers are
durable replica-local projection metadata, not replicated table data, and are
excluded from logical HA replication, split/move payloads, and logical backups.
On promotion the new local owner advances only intents already belonging to
that replica; a new replica, moved/split group, or logical restore rebuilds its
own projection and creates a new intent if its load state requires one.

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
| Replica replacement or root-generation change | Delete inactive old candidates. If an old candidate is pointer-selected, persist fresh new-root reconstruction debt (identity and all pointer-selected replacement debts replaced in one checkpoint) and keep the index fail-closed; never make it serviceable by merely discarding the old intent |
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

Load-failure state alone cannot gate queries, because pointer activation and
restart can make a candidate openable before the repair state machine has
validated it. Index serviceability is therefore:

```text
runtime index loaded
    and no quarantined load failure
    and no blocking repair intent in detected..validating
    and clean projection checkpoint matches the active config and generation
```

- The managed progressive-admission intent is the narrow exception: while it
  stays in `detected`, the canonical generation may serve only after the
  generation-scoped partial-publication proof in
  [DB.md](DB.md#publication-policy-and-readiness) succeeds (matching catalog,
  coverage, admission, and durable projection identities plus the
  engine-specific physical invariant). It never makes a shadow candidate or an
  arbitrary repair intent queryable.
- An intent in `cleanup` does not block queries once the replacement has passed
  validation and the clean checkpoint is durable. A crash after pointer swap
  but before validation leaves the intent in `activating` or `validating`, so
  restart keeps returning `index_rebuilding` even if the candidate opens. If
  validation fails, activation rolls back when a prior valid pointer exists;
  otherwise the index stays quarantined and the intent is retried or marked
  terminal.
- The decision is cached in the index manager and updated on load-failure,
  intent, pointer, and checkpoint transitions; queries never read the primary
  metadata store per request to evaluate it.

### Replay retention during repair

The quarantined index is intentionally absent from normal managed replay, so
its old applied sequence cannot protect the replay journal. Before releasing
the primary snapshot used for shadow construction, persist an
`IndexRepairReplayPin` (`version`, `repair_id`, `db_identity`, `replica_id`,
`root_generation`, `index_name`, `retain_after_sequence`).

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
advance the replay truncation floor (shared by concurrent repairs of
different indexes and by the in-memory pressure gate):

1. Persist a provisional pin with `retain_after_sequence = 0` for the bound
   replica and root generation. Zero explicitly means retain the entire
   available replay journal; it is not interpreted as "no pin."
2. Make that pin visible to the in-memory truncation registry before releasing
   the metadata transaction.
3. Open the primary snapshot and read its replay/build floor.
4. In one atomic repair-checkpoint publication, update the intent's
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

Disk admission is capacity policy owned by `ResourceManager`, not the
executor; `BackendRuntime` owns the threads and I/O lanes that run admitted
repair. A storage backend may install a capacity source while composing
`DB.OpenOptions`; it returns a stable identity for the physical volume/quota
namespace plus an optional current capacity observation. Table, group, index,
repair, and logical store identifiers are not capacity domains unless the
deployment guarantees each maps one-to-one to an independently exhausted
allocation.

- Provisioned storage installs a live `statvfs` probe for its replica root into
  the shared `ResourceManager`; the same observation feeds store heartbeats and
  repair admission, so scheduling data and admission never diverge. A quota or
  non-filesystem backend replaces the probe through the same interface.
- Installation is immutable after runtime composition: DBs copy the source on
  open and reservations coordinate through its domain ID. Reinstalling the
  exact same source is idempotent; a different source, or installation after
  admission has begun, is rejected.
- The intent's `planned_disk_bytes` is an estimate, not a reservation that
  survived restart. Each attempt measures any existing candidate, refreshes the
  observation, reserves only remaining growth, and keeps a node-owned safety
  floor for WAL/checkpoint/foreground durability. Unknown capacity is
  accounting-only; known but stale capacity fails closed.
- Shadow construction reconciles actual bytes at bounded durable publication
  and catch-up boundaries, refreshes the observation, and grows the claim
  before proceeding when the plan is exceeded. Revalidation uses the aggregate
  outstanding claim for the physical domain, so falling capacity stops the next
  bounded unit even under the original estimate. Full-text segment batches,
  graph batches, dense/sparse artifact chunks, replay catch-up chunks, and each
  HBC deferred-publication window cross this fence.
- Every bounded boundary first performs an O(1) refresh and aggregate-claim
  check. If the conservative claim would deny progress, the executor measures
  the complete shadow generation (index artifacts, checkpoint, publication
  markers), shrinks the future-growth reservation by the materialized amount,
  and revalidates; major phase transitions also reconcile exactly. This avoids
  false denials near the estimate without repeated recursive walks. The
  preliminary fit probe updates the observation but records no denial; only a
  stale observation or the final post-reconciliation decision increments denial
  metrics, so alerts describe work actually stopped. Ordinary ingest and query
  paths never measure, and admission never walks directories on the foreground
  write path.
- Unsupported observation is unknown capacity, not an endless repair wait. On a
  backend that supports observation, a transient probe failure fails new repair
  admission closed with durable backoff; heartbeats keep the last good value and
  expose a failure counter instead of a false zero-capacity sample.
- Claims coordinate all DBs sharing a manager, not independent processes on one
  volume. Production deployments must enforce one writer process per capacity
  domain or supply a capacity source backed by an OS quota, preallocation
  scheme, or shared allocator. This is part of the storage backend contract, not
  an application-facing tuning switch.

### Cache ownership and admission

Dense cache policy is node resource policy, not index schema. Public and raw
index configuration expose no cache enable/disable switches, entry counts, or
byte values: `max_cached_nodes`, `max_cached_vectors`, and
`max_cached_metadata` are unsupported (older persisted configuration with those
keys still loads, but the values are ignored), and HBC-specific environment
switches and mutable runtime cache-cap setters are outside the supported control
plane. Node resource-manager policy is the sole production authority.

The `ResourceManager` owns cache policy and exact byte admission across all
indexes sharing it:

- retained HBC nodes, quantized state, vectors, and metadata use the shared
  `hbc_node_metadata_cache` byte budget
- replay-time decoded-vector and raw-read caches use the applicable dense
  working-set byte budget
- HBC count ceilings are internal CLOCK-bookkeeping safety bounds derived from
  the resource budget and cannot override it
- a provisioned node shares one manager and HBC cache across its groups and
  indexes
- a standalone DB installs one internal default manager before opening primary
  or index storage, so primary LSM, dense LSM, replay, and HBC share one policy;
  direct standalone `IndexManager` use also gets a fallback

A node-owned shared cache is constructed and bound by the same node resource
manager that outlives every DB using it. A per-DB fallback manager governs only
DB-local working sets and internal caches and never installs itself into a
caller-owned LSM or HBC cache; passing an explicit manager with a shared cache
asserts that the caller owns both lifetimes and keeps the manager alive until
the cache is detached or destroyed. Startup catch-up and repair never rewrite
per-index cache caps; they compete through the same byte reservations,
pressure actions, and fairness as foreground dense work. Operators tune node
resource budgets; applications tune index semantics and search quality.

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

Progressive, queryable generations wait on source progress with a bounded audit
deadline. Each selected audit grant carries the durable intent revision and is
consumed by an expected-revision checkpoint transition under the repair control
mutex. Owner handoff, pause/resume, replay, and retry mutations invalidate an
older grant; successful consumption advances the revision and clears the
resident token. This durable write occurs only on the audit path; ordinary
deferred scheduling remains process-local. An audit still respects the
producer-completion fence before starting reconstruction.

Generation completion and source-artifact debt are separate durable outcomes.
Once a replacement pointer and clean checkpoint commit, the generation intent
is complete even if corrupt or missing source artifacts remain in the bounded
artifact-repair queue; those issues stay visible without triggering redundant
full-generation rebuilds.

### Bounded convergence and activation

Foreground writes stay available during reconstruction, so activation cannot
assume a busy group will go idle. Candidate catch-up runs outside the final
apply barrier until every admission threshold is met: remaining replay
sequences and bytes, catch-up throughput relative to foreground write
throughput, estimated final catch-up time, and the configured maximum
write-pause duration. Only then may the executor take the final apply barrier,
under which it rechecks ownership, root generation, configuration, candidate
durability, and the current replay target. If the new estimate exceeds the
maximum pause it releases the barrier without swapping, records
`waiting_for_convergence`, and keeps catching up in the background. No repair
holds the write/apply barrier for an unbounded scan or wait.

- Activation uses one absolute monotonic deadline. Final replay gets a
  deadline-aware record/item window and reserves a bounded tail for readiness
  validation, pointer publication, and the clean checkpoint; the replay worker
  opens no new apply window after its deadline. A single backend operation is
  non-preemptive, so admission uses the slower observed per-sequence cost and
  qualification must show the largest permitted window fits the pause SLO.
- Activation transfers the already-open replacement entry from the shadow
  manager into the active manager for every index family. All allocations and
  validation happen before the pointer write; dense vector-load context is
  rebound during the allocation-free handoff. The detached predecessor is kept
  while the pointer and clean checkpoint commit and closed only after the apply
  and dense-query fences release, so no size-dependent close/reopen or teardown
  sits inside the pause. Pointer commit and clean-checkpoint publication are an
  intentionally non-interruptible tail; the resource manager records
  fence-held duration separately from post-commit cleanup, including failed
  attempts and over-budget events.
- An activation error stays in `activating` until restart or the next pass has
  read and validated the durable pointer; it cannot fall back to disk-admitted
  rebuild work first. Pointer publication is the last fallible handoff step: if
  it fails, the predecessor stays installed and searchable. If the pointer
  commits but checkpoint publication is interrupted, restart reconciles the
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

The response clears automatically once pressure falls below the recovery
threshold; it is never reported as a generic timeout and never allows storage
exhaustion. Deployments may map node-wide emergency unavailability to HTTP 503
with the same structured code and retry contract. Activation metrics include
convergence lag in sequences and bytes, estimated pause, actual barrier hold
time, convergence retries, throttled writes, and backpressure rejections.

### Repair engine and scheduler

Startup catch-up owns detection and policy but never runs a multi-minute
rebuild inside the serial table/group inspection loop; cheap discovery is
separate from expensive execution.

`DB.discoverRecoverableStartupIndexFailures(alloc, limit)` returns
`StartupIndexRepairDiscovery { discovered, already_pending, terminal }`. It
enumerates configured indexes, classifies load failures, validates or creates
durable intents, and publishes repair debt without scanning the corpus, so one
broken group cannot head-of-line block inspection of later groups on the node.

A node-local executor consumes intents selected by the managed startup owner
and calls the same DB state machine as explicit operator repair,
`DB.advanceIndexRepairIntent(alloc, repair_id, options:
types.ArtifactRepairRunOptions) !IndexRepairAdvanceResult`. That engine supplies
shadow creation, snapshot rebuild, catch-up, the final apply barrier, and the
pointer swap, without the destructive quarantined-root reopen.

- Notifications are latency hints, not correctness. The executor scans durable
  intents at startup and periodically, so a lost notification, executor
  restart, or temporarily closed DB cannot strand debt. It reopens the DB
  through the managed owner for each claim and never retains a raw DB pointer
  across placement or lifecycle changes.
- Startup discovery and operator controls enqueue or remove the exact affected
  group, never via broad startup catch-up; high-rate ordinary writes never mark
  the scanner dirty.
- Named-index pause, resume, cancellation, and successful passes keep the group
  queued until an aggregate owner-side audit has checked every index in it, so
  an operation on one index cannot drop another index's debt; that audit
  performs the O(1) queue removal once the group is clean.

Scheduler limits:

- At most one active reconstruction per node and per group. Concurrency may
  become configurable only after multi-group qualification proves memory, disk,
  replay retention, query latency, and activation-pause budgets stay bounded;
  unbounded per-group concurrency is never allowed.
- Bounded round-robin selection across known groups; exact generation sizing
  and capacity admission happen inside the selected DB, avoiding node-wide
  status materialization merely to rank one repair slot.
- `ResourceManager` admission for memory and disk, backend-runtime execution,
  and one-per-node bounds for disk I/O, compaction, and CPU. A reconstructible
  disk plan plus a process-local reservation token covers the candidate,
  retained poisoned root, WAL/replay growth, and cleanup reserve; free-space
  preflight alone is not admission because concurrent repairs can consume the
  same bytes.
- Cancellation and a final fencing check on placement or leadership change. A
  dedicated `BackendRuntime` maintenance owner and cooperative shutdown token:
  shutdown requests cancellation before draining the owner, and cancellation
  preserves resumable durable state without recording an attempt failure.
- Dense snapshot batches are sized from vector dimensions and the currently
  available `ResourceManager` repair budget. Background planning stays within
  the soft budget; the hard budget is the authoritative race/safety fence.
- Dense reconstruction is time-sliced at reopenable publication boundaries:
  the source cursor advances only after the candidate slice is durable, then
  the linked group cursor rotates the node slot. The production default is a
  15-second scheduler budget, observed only after a resource-sized batch (no
  per-document clock calls); pre-activation replay observes it after each
  durable replay window. Final replay under activation fences does not yield:
  it is bounded by the activation deadline and aborts the swap if it cannot
  converge. Other index families keep their bounded/cancellable builders and
  need the same largest-incident SLO proof before higher concurrency.
- Known debt lives in an intrusive linked hash queue: O(1) enqueue and removal,
  at most 32 entries snapshotted per pass, and durable-intent transitions supply
  the group and table route directly. Creating, resuming, pausing, or removing
  an intent therefore updates scheduling without an administrative snapshot.
  Metrics inspect the same bounded window; larger queues keep the executor
  runnable instead of scanning.
- The scheduler publishes queue depth, oldest-intent age, bounded-scan work,
  attempt outcomes, disk-admission waits, and current/peak resource-manager
  disk claims. Per-intent phase, retry deadline, sequences, reservation, and
  last error are in authenticated operator diagnostics and structured logs.
  Projected completion is omitted until measured throughput makes it
  defensible. The lost-wakeup interval and repair-capacity SLO are internal
  node policy with conservative defaults.

Retry has two scopes:

- Failures returned by the DB state machine persist their deadline in the
  durable intent. Failures outside it (DB open, routing, allocation) use
  deterministic jittered exponential backoff from 30 seconds to 10 minutes on
  the node-local monotonic clock. Group-specific failures park only that group
  and keep any later durable deadline already observed; executor-wide
  allocation and queue-snapshot failures park the maintenance owner without
  rewriting every queue entry.
- Fallback-route refresh has its own deadline: its failure suppresses only
  lost-wakeup discovery and never blocks an exact durable-intent notification
  that already carries its table/group route. A successful pass clears the
  matching failure state, and an explicit durable wake clears stale per-group
  backoff, so an unhealthy group or metadata fallback cannot turn the
  five-second executor poll into a hot retry loop or delay unrelated groups.
- Durable retry deadlines are realtime so they survive restart; they are
  translated once into monotonic deadlines on insertion into the process-local
  scheduler, and the two clock domains are never compared directly.
  Filesystem-observation freshness is monotonic end to end. Transient
  repair-job dispatch, ownership, routing, and storage failures return the
  durable API job to bounded exponential backoff; only malformed or
  semantically invalid requests become terminal.
- A successful DB pass that still reports durable debt first restores the
  exact group queue entry. If that allocation fails, the executor records
  global scheduler backoff and returns before publishing fallback cursor
  progress, making notification and cursor advancement one logical outcome
  without putting the cursor on the durability path. The periodic discovery
  anchor is rewound so the cursor is eligible as soon as backoff expires; the
  same rewind applies when fallback candidate allocation or failure-backoff
  enqueue cannot allocate.

Lost-wakeup reconciliation uses a compact node-local routing index rebuilt only
when the metadata epoch changes; steady-state passes neither clone nor free the
cluster-wide administrative snapshot. The fallback window adapts between 16 and
256 routes per pass to target a 30-minute full rotation at the configured
discovery interval, and advances its own cursor even when debt is queued. The
256-route ceiling bounds DB opens and metadata work; deployments beyond the
resulting envelope report an explicit rotation-SLO violation rather than
hiding unbounded recovery latency. If queued work holds the slot, a
safe-consumed prefix advances past non-local or already-queued entries but
stops at the first uninspected local candidate. The fallback is a safety
audit: direct durable-intent notifications and startup reconciliation are the
primary discovery paths, so normal recovery latency does not scale with a full
rotation.

Queue ownership decisions are tri-state: `attempt` schedules eligible debt,
`retry_unknown` keeps it queued while metadata and Raft leadership converge,
and only `skip_nonlocal` removes it. The live membership source produces
`skip_nonlocal` only from a known voter set that excludes the local node; a
known local voter without leadership stays queued through elections.

> **Relocated:** The pre-condensation text of this section → [history file](../docs/design/dense-indexing/history/lifecycle-condensed-sections-2026-10.md).

### Startup integration

Managed startup catch-up (`catchUpManagedDb` in
[`table_writes.zig`](pkg/antfly/src/api/table_writes.zig)) classifies repair
debt after primary/derived startup replay stabilizes and before converting
remaining failures to `terminal_degraded`:

1. It records whether the DB has an index load failure
   (`managedDbHasIndexLoadFailure`) or pending durable intents
   (`DB.hasPendingIndexRepairIntents`). An unavailable checkpoint is not proof
   that debt is absent; the exact route stays queued under normal backoff.
2. For a load failure it runs the bounded
   `DB.discoverRecoverableStartupIndexFailures(alloc, 1)` pass, which creates or
   validates intents without scanning the corpus.
3. If `DB.indexRepairIntentSummary` reports runnable intents, startup publishes
   the `artifact_rebuild` phase and either returns pending debt for the
   executor or advances one bounded unit via
   `DB.repairRecoverableStartupIndexFailures`.
4. Only failures with no runnable intent, or a terminal one, report
   `terminal_degraded`. A restore-repair marker with a quarantined generation
   also stays fail-closed until the repair owner (or an operator) recovers it.

So an allowlisted failure with valid source artifacts becomes scheduled repair
debt, a transient open race is retried without reconstruction, and unknown,
non-allowlisted, unsupported, or unreconstructible state stays fail-closed for
operator intervention.

### Exclusion, fencing, and backoff

Group-operation exclusion and the index repair lease are not the durable state
machine. The executor claims only a locally owned group; holds the per-index
repair lease while advancing an intent; holds broad group-operation exclusion
only for bounded claim/open and final activation, never the full corpus scan;
captures an ownership/fencing epoch and revalidates it before pointer
activation; turns an active writer or repair lease into retryable `busy` debt;
persists `attempt_count`, `last_error`, and `next_retry_at_ms` on failure with
bounded-jitter exponential backoff across restarts; marks deterministic
preflight or policy failures terminal immediately; and clears retry state after
successful activation and validation. Startup catch-up backoff protects
discovery; persisted intent backoff protects expensive reconstruction and
prevents restart thrash.

### Status and metrics

The normal table/index status response is a user-facing health surface, not a
dump of the repair state machine. It exposes one compact object, only while a
repair exists:

```json
{
  "repair": {
    "state": "rebuilding",
    "action_required": false
  }
}
```

`state` is the stable vocabulary `rebuilding | waiting | paused | failed`;
`action_required` is true only for `paused` or `failed`; healthy indexes omit
`repair`. Do not add attempts, phase names, timestamps, byte counters, repair
IDs, paths, error strings, or speculative percent-complete estimates. Existing
top-level availability and rebuilding fields stay authoritative, and malformed
durable repair state maps to `failed` rather than disappearing.

Detailed diagnostics (repair ID and trigger, internal phase, automation mode,
attempts, timestamps, build and replay sequences, retry deadline, wait reason,
last error, queue age, throughput estimate, retained replay bytes, pinned
snapshot bytes, disk reservation, and defensible monotonic progress) belong in
the authenticated operator surface and structured logs. The operator endpoint
returns them on explicit request and returns the repair ID when an action is
created or attached, enabling stale-action fencing.

Prometheus metrics use bounded phase/outcome labels and aggregate counts or
durations; repair IDs, index names, candidate paths, and error strings are
never labels. Node metrics cover queue depth and oldest age, groups inspected
and fallback groups scanned, pass duration, attempts/outcomes, disk waits, and
current/peak/admitted/denied resource-manager disk claims; ResourceManager
metrics cover the memory and retained-WAL pressure involved in repair. Further
phase histograms may be added with bounded labels, outside the public index
response.

Index status separately includes `hbc_posting.refresh_pending`, a
constant-cost observation of ANN posting freshness, not repair progress. The
bounded refresh scanner publishes an atomic certificate only after a clean
sweep at the current mutation epoch; a partial or changed sweep stays pending,
a write or abort invalidates the certificate, and reopen starts uncertified.
Lightweight status, cached-status overlays, and detailed diagnostics expose the
same observation without counting dirty postings. Shard aggregation stays
pending if any shard is pending or lacks the observation, and read-only
verification progress invalidates runtime status so the final clean transition
publishes without another repair. Query readiness and optional-maintenance
scheduling are separate from this field.

While rebuilding:

- primary document reads and ordinary writes stay available, subject to the
  hard-pressure contract below, and unrelated indexes stay searchable
- an affected index with missing or false correctness proof returns a stable
  `index_rebuilding` error mapped to `IndexUnavailable`; only an
  operator-requested rebuild of a proven healthy generation stays queryable
  until the short activation/validation fence
- runnable or retryable startup status reports `artifact_rebuild`, not terminal
  degradation; a paused or terminal intent is reported distinctly
- process and node readiness stay healthy unless primary storage is
  unavailable; table/index health still reports degradation, and deployments
  that require every configured index may opt into a strict readiness policy

Only a completed clean checkpoint and cleared load failure return a fail-closed
index to service.

### Query and operator experience

The API exposes repair state through the normal table/index status response,
not only logs and Prometheus. An explicit query against the affected index
returns a structured retryable error:

```text
code = index_rebuilding
retryable = true
retry_after_ms, when known
```

Internal phase and repair identity stay on the authenticated operator endpoint
and correlated logs, because they do not help an application decide whether to
retry. The index name may be included when the request maps unambiguously to
one index, but retry behavior must not require it (hybrid queries can depend on
several unavailable indexes). Composed or hybrid searches fail the whole
request when a required index is rebuilding; partial results need an explicit
request option and must identify every omitted index. Writes keep appending
primary data and replay records; a request requiring dense visibility for the
rebuilding index waits only up to its normal deadline and then returns
`index_rebuilding`. Replay retention and write backpressure still apply if
repair cannot keep up with foreground traffic.

Operator actions follow these rules:

- an explicit repair request attaches to the existing intent instead of
  creating a competing rebuild. The newest `(created_at, job_id)` is attached
  atomically even when the existing intent was created by automatic coverage
  repair, and intent completion records that job before deletion. A forced
  request first performs the bounded current-generation classification, so it
  cannot relabel missing coverage proof as a healthy operator rebuild. A new
  dense request then remains fail-closed in `operator_generation_validation`;
  the admitted BackendRuntime worker checks replay convergence, exact source
  coverage, and stored structure before promoting it to an online operator
  rebuild. The potentially linear structural walk never runs on the HTTP path
  and observes cooperative owner cancellation
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
- cancelling a named-index repair job durably stops the requested work, not
  just the HTTP job record. The job stays nonterminal while bounded passes apply
  `pause_automatic_repair` across the table; the owner is signalled before the
  control waits for group-operation serialization, so an active rebuild yields
  at its next durable boundary. Only after the traversal completes does the job
  become `cancelled`; resumption is an explicit operator action
- every nonterminal job is indexed atomically by a fixed-width, job-ID-ordered
  active marker. Restart scans only that bounded index, validates each key
  against the authoritative, size-bounded primary job record (job ID, phase,
  target, status, limit, bounded strings), deletes orphan markers, rebuilds the
  unfinished-cancellation FIFO in creation order, and loads terminal history
  lazily. Malformed marker keys or records move atomically to a bounded,
  content-addressed forensic quarantine and are logged, with their active and
  primary keys removed so they cannot poison later restarts; valid job recovery
  and API startup continue independently
- a transient cancellation pass failure keeps `cancel_requested`, the cursor,
  and accumulated results, then enters durable exponential backoff. The FIFO
  inspects a bounded window from a process-local round-robin cursor that
  advances after every inspected entry, so a delayed head cannot block
  unrelated due work; restart may reset the cursor because the durable FIFO and
  job records are the source of truth

These actions are idempotent and scoped by `repair_id`. A stale action for an
old repair cannot pause, resume, cancel, or delete its replacement.

### Automatic repair test matrix

The minimum deterministic matrix (the original numbered descriptions are in
the [history file](../docs/design/dense-indexing/history/lifecycle-condensed-sections-2026-10.md)):

| # | Scenario | Required outcome |
| --- | --- | --- |
| 1 | External dense index with persisted vectors and `__bulk_publish_state` left set, then managed startup catch-up; repeat with the durable artifact target counter removed (snapshot-plus-delta bootstrap) | Searchable automatically, with no API repair request |
| 2 | Stop after intent persistence, before candidate creation | Quarantined root retained; repair resumes |
| 3 | Stop during snapshot construction | Resume a validated checkpoint, or discard only the candidate and rebuild |
| 4 | Writes after the snapshot floor during the shadow build; for a missing-counter dense index, artifact insert/replace/delete while the bootstrap snapshot is open, deletes via the document path and TTL cleanup, generated embedding writes/deletes via a remote document-child batch | Replay truncation stays pinned and the final index includes the writes; signed delta, durable target count, and search are correct; each primary mutation, replay record, and counter delta is atomic |
| 5 | Stop after candidate readiness, before pointer swap | Validate and activate the identified candidate without rebuilding |
| 6 | Stop after pointer swap, before validation, checkpoint publication, pin removal, or intent deletion | Each restart deterministically finishes without losing the new active root |
| 7 | Inject `UnsupportedVersion`; inject allowlisted `Corrupted`, `NotFound`, and `FileNotFound` backend validation failures | `UnsupportedVersion` stays terminally degraded with no automatic root deletion; the others fail closed and rebuild a separate generation without mutating the poisoned root |
| 8 | Remove or corrupt required source artifacts | Preflight goes terminal; no candidate is created or activated |
| 9 | Transfer leadership during build and immediately before activation | Only the active local owner repairs |
| 10 | Fill the filesystem during candidate creation, build, readiness publication, activation, and cleanup | Active/quarantined state and intent stay coherent after every failure |
| 11 | Corrupt or truncate the intent and candidate marker | Unknown versions and malformed data fail closed |
| 12 | Cancel repair, drop the index, or replace its configuration during each long phase | Replay pin and candidate are cleaned up |
| 13 | Multiple broken indexes across multiple groups | Node-wide concurrency limit and fairness hold; no startup head-of-line blocking |
| 14 | 100,000-document interrupted external-vector ingest, restarted with no API call | Correct document count, dense count, and search; zero load failures; intent removed |
| 15 | At least one million documents with concurrent writes and queries and repeated forced termination | Bounded memory/WAL/disk growth; recall matches a clean build |
| 16 | Pause truncation just before pinned-snapshot acquisition; race the truncator against snapshot creation | The provisional zero pin or final floor protects every required replay record across crash |
| 17 | Every state-table transition twice, crashing between its filesystem effect and durable phase update | Converges without activating an unvalidated candidate or deleting the active root |
| 18 | Full and relaxed/`none` durability, including LMDB `no_sync` | No applied watermark or truncation advances before the explicit durable sync; sync failure preserves replay debt |
| 19 | Replace replica identity and root generation, promote an HA replica, move and split a group, logical and physical restore | A local candidate resumes only on an exact identity match |
| 20 | Sustained writes above and below candidate catch-up throughput | Barrier hold stays below the configured maximum; non-convergence returns to `waiting_for_convergence` and eventually applies documented backpressure |
| 21 | Drop executor notifications and restart the executor while intents exist | Adaptive bounded durable discovery reclaims each eligible intent within the configured scan SLO without duplicate execution |
| 22 | Cancel the current attempt; durably pause, restart, and resume | Only cancel permits automatic retry; pause survives restart |
| 23 | Exhaust soft and hard replay, snapshot-pin, and disk budgets | Reservation fairness, stable `dense_repair_backpressure` with `Retry-After`, hysteretic recovery, no filesystem exhaustion |
| 24 | Queue differently sized repairs long enough for size-aware selection and aging | Bounded oldest-intent age; no starvation |
| 25 | Hold dense-repair working set above the soft background budget but below the hard limit | Counter bootstrap and shadow batch planning defer without consuming bytes or recording a hard rejection |
| 26 | Hard replay pressure | Standalone writes and only the active managed-Raft leader reject uncommitted work with the compact retryable 429; committed entries keep applying on every replica; admission recovers without a configuration change |

This track prevents wipe/re-ingest without changing the successful
normal-ingest path; streaming sessions separately keep ordinary replay from
creating the failure state.

## Generation Publication

Generation replacement is not the normal replay mechanism. It is for explicit
operations, such as a full rebuild of a live index or a large import replacing
an existing one, that need both permission to build cross-transaction
structural state and continued service from a previous complete index.
Progressive initial admission is not generation replacement: it publishes
durable checkpoints of the canonical generation under the stricter managed
admission proof in [DB.md](DB.md#publication-policy-and-readiness); atomic
initial admission and replacement rebuilds use the isolated candidate flow
below.

Bulk publication defaults to an isolated candidate. In-place publication is
allowed only for a brand-new never-advertised root, an explicitly offline index
with no prior searchable generation, or an explicitly isolated maintenance
path; a live active index never enters an in-place incomplete publication
window.

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

- Generation N stays a durable rollback anchor through replacement open,
  validation, clean-checkpoint publication, and repair-intent cleanup. Inactive
  shadow roots and stale canonical-root contents are collected only after no
  durable intent can refer to them; startup orphan cleanup is suppressed while
  repair state exists or is malformed.
- Repair and explicit rebuilds share one shadow-build and active-root
  pointer-swap implementation, and the same pinned-snapshot, durability-mode,
  identity/fencing, disk-reservation, and bounded-activation primitives; there
  is no parallel implementation of those rules.
- Retired-generation collection is deduplicated `BackendRuntime` cleanup work;
  a rerun latch covers retirements that race an active scan, and DB shutdown
  drains the cleanup owner before destroying the index manager.
- Rebuilding a candidate from all primary artifacts is acceptable for explicit
  rebuild/import but too expensive for ordinary write bursts. A later
  optimization may add a cheap LSM generation fork that shares immutable runs
  while keeping separate manifests, mutable state, and WAL ownership.

## Physical index versioning and rolling upgrades

Native HBC is a physical vector-index version, not an in-place
reinterpretation of the logical index directory; the logical catalog name and
configuration stay stable.

- **Managed activation.** Committed store records advertise the native-v2
  recovery capability through the rolling metadata protocol, and authority
  stays closed until every table-serving store advertises it. The Raft apply
  that observes the complete capable set records a monotonic activation version
  with the metadata incarnation; data stores open their local authority gates
  only from that durable value, never from an observed membership snapshot. Any
  stale legacy store registration then becomes a deterministic no-op, closing
  the proposal/apply race during the pre-promotion shadow build. Activation
  survives leader changes, restart, and snapshot restore.
- **Online migration.** A legacy index keeps serving while the durable repair
  state machine builds a native shadow, replays it to a bounded activation gap,
  validates coverage and structure, and publishes it atomically.
  Generation-manifest v2 records `dense_native_v2`, the active-root pointer uses
  a deliberately incompatible v2 header, and reopen requires the checksummed
  manifest and the crash-sticky HBC `AUTHORITY` marker, so an older binary fails
  closed instead of silently opening stale compatibility LSM state. Manifest v1
  stays readable as `legacy_lsm`; existing indexes need no offline rewrite.
- **Retirement** is a separate catalog phase. Initial v1 files stay on disk
  after v2 promotion and can be selected by the captured rollback pointer;
  native reopen does not delete them. An explicit catalog-fenced retirement call
  reclaims them only after the downgrade/rollback window advances. Newly created
  managed indexes use the same shadow/pointer machinery, so there is no
  migration-only serving path. Standalone/Lite databases, which own their whole
  compatibility domain, may authorize local native publication.
- **Fresh admission** selects the v2 end state directly once the durable
  capability floor permits it: creation stages an unpublished private root with
  a checksummed construction manifest, establishes an O(1) empty native
  authority, rewrites the root pointer with the v2 header, and commits the
  logical catalog last. Managed/public admission never builds a corpus in the
  compatibility HBC LSM before scheduling its durable rebuild outbox; the first
  user mutation is WAL-native. The construction capability is immutable and
  scoped to that entry, so it cannot authorize native transition of an
  unrelated live v1 index. Before capability activation, fresh managed indexes
  stay v1 and pre-existing v1 indexes use online shadow migration.
- **Standalone backfill** uses the same lifecycle through one pinned primary
  read transaction: the native capture records exactly that transaction's
  replay sequence, writes the applied-sequence checkpoint, and certifies the v2
  generation at the same boundary; rows committed afterward are ordinary replay
  debt. A construction marker remains until the logical catalog is durable, and
  explicit re-creation can reclaim a broken orphan pointer after a crash.
- **One corpus scan.** Fresh v2 construction establishes the shared
  exact-vector base before posting capture, so both halves of authority agree.
  The first managed index publishes a real empty generation at source sequence
  zero with no shard files or primary scan; an existing exact table-wide
  generation adds a zero-count artifact scope through a checksummed
  `CURRENT`-only transaction, preserving immutable blocks and the committed WAL
  prefix; synchronous backfill materializes the same index-managed source
  artifact as foreground direct-field writes, appends its exact vector to the
  native WAL, and builds postings from the one pinned transaction; stable-tip
  publication compacts the captured native delta instead of rescanning. The
  wider snapshot sequence is accepted only while the generation is unpublished;
  promotion permanently closes that construction capability. Regressions
  require zero primary vector snapshot builds for managed empty admission and
  standalone backfill, exact scoped coverage after a second index joins a shared
  generation, an unchanged WAL generation/size across a metadata-only scope
  update, and rejection of the construction-sequence override after v2
  publication.
- **Orphan liveness.** Repair-shadow orphan collection never derives liveness
  from one manager's in-memory catalog: it scans all canonical `ACTIVE_ROOT`
  pointers first and treats their targets as live, without using payload health
  as deletion authority. The construction marker protects a unique root before
  pointer publication and the durable pointer afterward; the inventory is a
  filter, not deletion authority (see
  [Orphan deletion is reauthorized](#orphan-deletion-is-reauthorized)).
- **Shared source artifacts (future).** Reusable embeddings and other source
  artifacts should get a catalog-managed immutable artifact identity that
  indexes reference rather than own by convention. Index-created artifacts stay
  producer-scoped until an explicit user promotion makes them managed/shared;
  promotion verifies schema/model/dimensions/source-generation identity and
  adds a durable reference before producer-index deletion can release
  ownership. Physical HBC posting/tree generations are index-specific and are
  never promoted as shared source artifacts.

These controls add heartbeat/status fields, maintenance-time migration checks,
and O(1) manifest/pointer reads on open or promotion; they add no work to
candidate routing, scoring, exact completion, or foreground mutation loops.

### Native generation lifecycle hardening

- Native backup manifest v5 authenticates both the portable snapshot path and
  an explicit runtime `install_path`. Shared vector acceleration stays under the
  snapshot ownership namespace `indexes/vector-blocks` but installs at the
  runtime-owned `vector-blocks` root; duplicate or noncanonical install targets
  are rejected before any generated state is admitted.
- Snapshot admission acquires stable file-descriptor leases for exact committed
  posting/vector WAL prefixes. WAL copying, hashing, and fsync happen after
  apply, replay, and structural mutation admission reopen, so fence work is
  descriptor acquisition plus immutable hardlink metadata, not O(committed WAL
  bytes); replacing the live WAL after the lease still recovers the selected
  prefix.
- Posting and vector generation directories reconcile strict native filenames
  against `CURRENT` at startup and publication boundaries. Known retirees are
  deleted directly; inventory sweeps recover crash-before-publication orphans
  and retry failed unlinks, reporting `observed_debt`, `removed`, and
  `remaining_debt`. Unrelated files are preserved, ordinary observational opens
  never reclaim concurrently staged generations, and publication adds one flat,
  filename-only inventory scan (no segment reads or recursion).
- Native authority never appears as a side effect of an ordinary v1 mutation:
  HBC requires an explicit authority-transition capability, which the catalog
  grants only to an inactive candidate or an already-selected v2 generation.
  Standalone storage skips distributed capability negotiation but uses the same
  manifest plus incompatible pointer publication.
- An authenticated native restore is rehomed into a deterministic v2 generation
  with an atomic directory rename, checksummed ready manifest, directory fsyncs,
  and pointer publication last. Retry validates or completes the same
  generation without copying vector/index files or replaying the corpus.
- Compatibility LSM files inside the active v2 generation are restart-stable
  cleanup debt, removed by the durable cleanup lane only after native authority
  and the catalog capability floor are both proven (for standalone storage,
  after the v2 pointer has made downgrade fail closed).

None of this touches serving or mutation loops: authority gating adds no
steady-state branch after the persisted-authority fast return, restore is
O(index count) metadata plus directory renames, and legacy retirement runs in
the background cleanup lane. A full DB lifecycle test proves v1 stays queryable
during shadow construction, v2 promotion precedes retirement, native
backup/restore needs no embedder, and the restored read-only index has neither
format-migration nor repair debt.

### V1 mirror versus native-v2 authority

Storage mode is explicit and never inferred from the presence of an optional
posting generation:

- A v1 index stays LSM-authoritative, matching the v0.2.0 lifecycle. It may
  publish an immutable posting mirror for query acceleration, but mutations and
  their inverse rollback go through the HBC LSM.
- Only a capability-authorized private candidate or an index selected by the
  incompatible v2 pointer may route mutations exclusively through the native
  posting WAL; enabling that path without transition authority is an
  authorization error.
- A native capture pins its immutable base, and cancellation restores that base
  whether or not the candidate has published its irreversible authority marker;
  marker publication and rollback ownership are separate states.
- A delayed source callback below the capture's base coverage fails closed and
  cannot relabel mutations at the current epoch. Legacy owners apply their true
  LSM inverse before cancellation; native owners restore the pinned generation.

`posting sidecar` predates this distinction and is ambiguous. In v2 the posting
segment store is the index authority and only the compatibility HBC LSM is
retired; future API cleanup should use `native_posting_store` for v2 and
reserve `sidecar` for the optional v1 mirror.

#### Rollback capability is capture-local

Mutation-store mode is not rollback authority. An authorized bootstrap or
repair candidate can select native mode before its first complete immutable
generation exists; mutations then still reach the compatibility LSM and
cancellation has no native base to restore, so skipping the inverse merely
because native mode was selected could keep a phantom derived vector after the
primary mapping transaction failed. Rollback therefore asks whether the active
capture owns a pinned immutable base: a base-owning capture restores it in
constant time, while a base-less candidate and a v1 mirror perform the real LSM
inverse (tests cover all three). Native-store enablement returns a runtime
authorization error rather than relying on `std.debug.assert`, keeping the
authority fence in production `ReleaseFast` builds without hot-loop cost.

#### Artifact identity is a set

The exact-vector projection never infers one artifact family from
`embedding_name` or the index name: indexes may union several embedding
sources, and the external single-source form may use an artifact name that
differs from its index name. Backfill, mutation-WAL publication, mmap
projection reads, topology projection builds, snapshot construction, and
readiness share one contract:

- a single-source index owns exactly its configured `embedding_name`, falling
  back to the index name only when none was configured;
- a multi-source index owns its configured set of artifact families, and each
  posting member keeps its exact source artifact key;
- table-wide generation manifests declare the union of those scopes;
- base cardinality readiness sums the per-source certificates for the logical
  index instead of consulting an index-name surrogate.

Single-source lookup stays one derived key with no added query I/O;
multi-source lookup uses the already-materialized member key. Tests publish two
source families through the native mutation WAL and certify their combined
immutable base coverage.

#### Orphan deletion is reauthorized

The canonical-pointer inventory is an efficient filter, not deletion authority:
another manager can publish a repair generation after the scan and clear its
construction marker. Cleanup re-reads all canonical pointers for each unmarked
deletion candidate immediately before touching its filesystem or algebraic
state, so the construction marker protects the pre-pointer window and the
revalidated pointer protects the post-publication window. Invalid pointers fail
closed. A deterministic hook test publishes and clears the marker exactly
between inventory and deletion and verifies the live generation survives.

> **Relocated:** The physical-versioning fix narratives and the r128 50K
> correctness-only qualification →
> [history file](../docs/design/dense-indexing/history/lifecycle-implementation-log.md).

## Implementation Plan

There are three independently reviewable workstreams: automatic restart repair
(durable core plus node-local scheduling/UX), streaming replay (requires crash
and performance qualification), and generation publication for explicit
rebuild/import continuity. Phases 0, 1, 2, 4, and 6 are complete; their
acceptance criteria are now the contracts in the sections above.

> **Relocated:** Completed phases 0 (invariant inventory), 1 (streaming
> contract), 2 (crash/idempotency qualification), 4 (durable repair core), and
> 6 (shadow generation publication) →
> [history file](../docs/design/dense-indexing/history/lifecycle-implementation-log.md).

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

#### Memory measurement methodology

Use Circus's native `footprint_sampler.py` against the Antfly server process
tree and capture the wired-memory baseline immediately before server start.
Datasets must already be cached. A valid publication number requires three
fresh lifecycles and reports mean plus range.

For native macOS runs, the primary demand number is the process tree's
`phys_footprint` ledger high-water. System-wide wired growth is reported as a
separate conservative diagnostic because unrelated host activity cannot be
attributed to Antfly. RSS remains the cache-inclusive point-in-time view. Do not
poll native `vmmap` during a timed phase: invoke the sampler once immediately
afterward and use the kernel-maintained footprint high-water for the phase peak.
The qualification runner captures live and restarted processes separately.
Historical scripts invoked `vmmap` every 200--300 ms and materially contaminated
both load throughput and query tails; those timings are not publication data.

Sampling dataset download after establishing the wired baseline contaminates
the system-wide wired delta; keep dataset acquisition strictly before the
baseline capture, and treat any sample that violates this ordering as
diagnostic only, not publication data.

A single-run one-million-document development signal under `ReleaseFast`
passes exact indexed count and replay checkpoint, durable reopen, and
post-restart search validation, with peak pressure working set comfortably
below the 2 GiB working-set gate. This is a qualification datapoint, not a
replacement for the repeated one-million-document baseline/candidate
comparison above.

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

### Phase 5: Repair scheduling and production UX

Scope: fast startup discovery and the bounded node-local executor; fair
scheduling, node-wide resource admission, persistent retry/backoff, disk
reservation, periodic lost-wakeup discovery, and ownership fencing; structured
query errors, status fields, metrics, and readiness rules; cancel-attempt,
durable pause/resume, drop, configuration-change, leadership-transfer, and
hard-backpressure handling; and qualification with multiple broken indexes
across tables and groups.

Acceptance:

- one long repair does not block startup inspection of later groups
- default concurrency is one reconstruction per node and never exceeds the
  configured resource budget
- direct durable-intent notifications enqueue the exact group without a scan;
  after restart, managed startup catch-up rediscovers every owned group, and the
  bounded periodic cursor supplies a final lost-wakeup audit. The supported
  groups-per-node envelope must qualify that cursor's full-rotation time rather
  than treating the per-pass discovery interval as an end-to-end SLO
- queue age stays within the qualified repair-capacity SLO, with no starvation
  under bounded round-robin selection
- process restart neither resets retry backoff nor duplicates a repair
- an existing `IncompleteBulkPublish` index repairs after restart without an
  explicit API call
- primary reads and ordinary writes stay available; the affected index returns
  `index_rebuilding` until validation; all repair state, candidates, and replay
  pins are eventually cleaned up
- reversible activation work aborts before the configured absolute deadline;
  dense publication performs no size-dependent close/reopen under the fence, and
  the non-interruptible post-pointer durability tail meets the qualified
  write-pause SLO

## Rollout

Streaming replay is the ordinary-write correctness path, not a user-selectable
index behavior. It is enabled unconditionally and has no public or
resource-configuration toggle. Production rollout uses the normal staged binary
deployment and canary process; rollback means deploying the prior binary rather
than retaining the unsafe ordinary-replay bulk-publication path in the runtime.

Emit counters for: streaming and bulk-publication sessions opened, finished,
and aborted; streaming replay batches reapplied after restart; incomplete
publication quarantines; automatic reconstruction attempts and outcomes;
repair executor queue depth, oldest age, admission delay, durable-rescan
recoveries, and active count (projected completion only once measured
throughput makes it defensible, per the scheduler section); repair replay-pin
age and retained replay bytes; repair snapshot age and pinned-version bytes;
candidate actual/reserved bytes, stale candidates removed, and cleanup
failures; repair phase duration, retry count, and terminal outcomes;
convergence lag, estimated and actual activation pause, and barrier retries;
dense-repair throttling and backpressure rejections; and session finish latency
and maximum replay lag.

Streaming sessions are the only ordinary-replay path; any unreachable
ordinary-replay bulk-session plumbing is removed after qualification. Automatic
repair runs by default with concurrency one per node; higher concurrency is
enabled only after multi-group resource qualification.

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

interrupted in-place bulk publication
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
