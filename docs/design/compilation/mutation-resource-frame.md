# Prepared mutation resource frame

This is the P5 lifetime inventory and migration design for
`storage/db/mutation_commit.zig`. It does not accept a compiler optimization.
The earlier cold merge outlining experiment remains rejected: 137 changed
lines saved 24,304 archive bytes without a controlled performance result.

The current worker has 67 function-scope cleanup registrations. The source-
hashed diagnostic inventory in `mutation-resource-audit.json` records their
positions, 148 function-scope bindings and every lexical fallible/exit site.
That inventory includes nested sites; it is not a compiler control-flow graph.

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

A private-overlay negative control disables only the disposition gate. It
reproduces three observer calls instead of one and swallows the ordered-receipt
publication error by returning success on the retry. Two tests fail and the
allocation sweep still passes, confirming these are behavioral regressions
rather than tests that merely restate the new state machine. Raw nonzero exit
and diagnostics are retained in `prepared-commit-disposition-negative-debug`.

The focused regression performs real durable `$inc` mutations and returns both
retry-shaped errors, OOM and cancellation from the committed observer. Each
must observe the caller's marker, execute once, leave count one and release
admission so a subsequent write succeeds. Cancellation keeps the existing
`CommitVisibilityNotSatisfied` outer classification. The allocation-failure sweep additionally checks durable state and released
admission at each failed preparation allocation. The restart test records the
original ordered receipt, closes/reopens the DB and replays that entry without
applying the increment twice. The final Debug qualification passes these three tests plus twelve existing
row-preparation, admission, allocation and transaction-recovery tests (67/67
build steps in `prepared-commit-final-debug.log`). This is close/reopen and
receipt recovery coverage, not a process-kill crash test. Existing production-
LSM and simulation crash suites remain required for a larger ownership migration.


## Diagnostic compiler evidence

Root captured a frozen `7bedd` Debug declaration/pass report in
`worker-declaration-debug-7bedd.json`. The pinned compiler emitted statistics but
failed its final archive-link step looking for `antfly-storage-engine_zcu.o`;
therefore this is diagnostic evidence, not an accepted build or benchmark.
The worker's declaration reported about 685 ms semantic analysis and 522 ms
code generation, against module totals of about 74.46 s and 49.04 s. Its
frontend share is about one percent, despite its large emitted function size.

The LLVM report is module-wide, without per-function attribution. Its pass
report totals 135.31 s wall time and 91.46 s CPU. Leading wall costs include
assembly printing (20.28 s), IR translation (17.73 s), debug-value analysis
(15.12 s), unreachable-block removal (9.32 s) and legalization (8.94 s).
Those totals do not establish that mutation cleanup dominates LLVM work.
Consequently this change accepts the independently tested disposition fix and
records the complete resource-frame design; it does not claim a compiler gain
or migrate ordinary effect phases without further attribution.
