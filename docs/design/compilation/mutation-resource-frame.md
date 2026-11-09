# Prepared mutation resource frame

This is the P5 lifetime inventory and migration design for
`storage/db/mutation_commit.zig`. It does not accept a compiler optimization.
Earlier outlining and diagnostic measurements are summarized in the
[qualification history](history/production-owners-2026-11-02.md).

The worker has overlapping request, catalog, locked-state and publication
lifetimes. Any migration must inventory the current source before changing
cleanup ownership; a lexical declaration inventory is not a control-flow graph.

## Lifetime and fence strata

The table is in acquisition order. Destruction proceeds in reverse, except
for the explicit early releases/transfers listed below. Nested block-local
transactions, arenas, scans and temporary encodings stay in those blocks.

| Stratum | Owned state and borrows | Required boundary |
| --- | --- | --- |
| Exit notifications | Maintenance-attempt hint, replication shared lease, recovery scheduling flag and total-time accounting | Recovery scheduling/profile completion runs before the replication lease is released; maintenance wake is last. No early release during publication or visibility waits. |
| Request preparation | Foreground-write lease, transform snapshot, effective operations, merged graph arrays | Foreground admission precedes provider/preparation work; transform read versions remain available for locked revalidation. |
| Catalog preparation | Request schema lease, relational index snapshot, readiness array, prepared index keys, writer, prepared row/effect/generated-plan resources | Plans/rows release before catalog leases and arena storage. Keep the current `PreparedBatchResources` destructor; it already is non-inlined. |
| Apply workspace | Preencoded replication bytes, partially initialized extracted writes, overwrite/changed flags, probe keys/values, semantic-noop map | Allocated before apply admission; extracted entries use their original DB allocator for element cleanup and the preparation allocator for the slice. |
| Snapshot and cleanup admission | Snapshot mutation lease, prepared merge artifacts, filtered cleanup edges/jobs/replay writes, cleanup page, partially initialized graph deletes | These cleanup registrations precede the apply-error unlock registration. On failure they are destroyed after unlocking apply, before snapshot admission and earlier workspace teardown. |
| Locked state | Apply fence, merge-page/catalog bytes, graph publication lease, apply schema lease, read transaction, staged relational writes, prior membership and physical write buffers | Revalidation remains under the original fence. On error, locked state is destroyed before unlocking apply. On successful early replay/fence returns, retain the existing explicit unlock/release sites. |
| Effects and replay | Graph key maps/owned strings, owner jobs, artifact-deletion keys, generated results, child-range dispatch/outbox arrays, sync targets, materialized effects, merge state/plan, replay bytes and range/catalog values | Preserve each allocator and ownership suffix. Physical `MutationWriteBuffers` already have a non-inlined destructor. No extra JSON/row copies. |
| Primary commit admission | Coverage guard, backlog reservation, authored vector acceptance | Last policy deadline checks stay before the store call. Backlog admission cancels only unused reservation; durable backlog transfer still occurs at its existing postcommit point. |
| Publication | Deferred replication transition gates and original publication/wait/output paths | Release transitions on every exit, retain durable outboxes until durability plus authority checks, and release foreground/apply admission at their existing explicit sites before waits. |

## Concrete frame shape

The future `PreparedCommitFrame` should be a stable local object with concrete
subframes for the strata above, rather than a type-erased cleanup callback
stack. Optional owned fields start absent; arrays start empty; initialized
entry counts start zero. Ownership must be registered immediately after each
successful acquisition and before the next fallible expression. Borrowed plan,
row and generated-artifact slices retain separate owned/borrowed counts.

Keep `PreparedRowAllocator` at its existing stable caller-owned address. Its
captured I/O receiver and locking executor cannot move with a returned frame.
Construct any arena-backed subframe in place before borrowing its allocator.
Use bounded, non-inlined destruction for each concrete subframe, with one
ordered frame destructor. The apply-error unlock separates the locked and
prelock cleanup strata; a single unconditional final unlock would change the
current lifetime ordering.

Explicit transfers must clear the corresponding ownership slot:

- `sync_targets` moves into a visibility completion and resets to empty.
- Persisted range buffers move into the core and reset to null.
- Prepared artifact prefixes remain borrowed from prepared rows until their
  current release points.
- Backlog admission transfers through `commitBacklogAdmission`; its cancellation
  destructor must retain the current consumed-versus-reserved semantics.
- Early graph-publication, coverage, snapshot, foreground and apply releases
  remain at their existing statements and leave idempotently empty frame slots.

No resource phase migration is accepted until normal-runner declaration/pass
attribution supports it. Start any such migration with a complete ordinary
write/transform effect family, not another pair of cold merge helpers. There
is no new archive in this design.

## Irreversible disposition

The irreversible operation remains `putBatchWithReplayAndParticipant`, or the
successful equivalent transaction-intent resolution. Only after that call
returns does the worker set the caller's existing `primary_committed` marker
and its new per-attempt `PreparedCommitDisposition`. An already-applied
transaction resolution is also committed for this purpose.

The preparation retry loop previously retried the names
`PreparedGenerationChanged` and `PreparedReadSetChanged` without consulting
whether the worker had committed. A postcommit observer can return either
name, causing a second application of a non-idempotent transform. The new
per-attempt disposition permits those retries only before primary commit.
The outer DB error mapping, caller progress marker, replay receipt and exact
error identity remain authoritative; disposition does not invent a rollback
or reinterpret an ambiguous store error as a confirmed commit.

Regression coverage must perform real durable `$inc` writes and inject
retry-shaped errors, OOM and cancellation after commit. Each attempt must
execute once, preserve the caller's progress marker and original error, leave
the count at one, and release admission for the next write. Check ordered-receipt
replay across DB close/reopen and preparation allocation failures as well.
Larger ownership migrations additionally require the existing production-LSM
and simulation crash suites. Dated passing counts, the failing negative control
and partial compiler diagnostics are retained in the qualification history.
