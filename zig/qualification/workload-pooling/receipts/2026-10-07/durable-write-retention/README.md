# Write completion baseline and acceptance-path audit

All 29 runnable scheduler and bounded-admission tests pass on native macOS and
pinned Nix/glibc GNU AArch64. The new regression verifies that cancellation does
not reclaim a running write grant, health can use its reserved slot at full
ordinary demand, and shutdown retains CPU/operation/byte charges until owning
finish. It does not simulate physical WAL/Raft acceptance or implement parking
accepted writes. Source hashes and full terminal logs are preserved.

The audited production path preserves stronger outcome semantics than a plain
request timeout. SQL ordinary writes withhold read suspension hooks in
api/sql_pgwire.zig and use serial SQL backend execution. api/sql_execution.zig
mutateInternal sends native batches and preserves uncertain outcomes instead of
inviting replay. Explicit COMMIT in api/sql_session.zig uses a registry execution
lease, persists sealExecutionPlan before native dispatch, and records terminal
or pending decisions under the same transaction id. api/transactions.zig
sealExecutionPlan persists commit_execution_started and the complete plan.
A durable sealed plan is a recovery obligation, not proof that Raft committed.
engine/db.zig batchWithContext checks authority before native handoff but exposes
no accepted-write execution handoff or physical wait callback.

Scheduler.close preserves live running execution for caller join, while stopping
queued/blocked entries; block after close stops a running entry, and ordinary
unblock/wait cannot resume it. Therefore existing write grants must stay held
until an independently valid completion authority is implemented. Reusing a
foreground entry after clearing its cancellation/deadline would not supply that
shutdown contract.

The audit also found a broader accounting seam: native and hosted
api/table_writes.zig commit coordinators supply fanout_io to
api/distributed_txn.zig ExecuteOptions. Its default max_parallel_participants is
eight; runBeginFanout and analogous phases submit concurrent tasks. ExecuteOptions
has no runnable execution port. Bounded participant count does not establish
that participant CPU belongs to the parent scheduler grant. This is a required
execution-ownership integration, not a reason to narrow the scheduler design.

The next implementation must carry execution authority through table-write
contracts and participant workers; distinguish pre-acceptance foreground work,
durable sealed-plan recovery and authoritative storage decisions; reserve bounded
completion ownership before accepting obligations; park only actual physical
waits; admit all participant CPU under explicit grants; and keep completion valid
through foreground cancellation and shutdown without a new foreground root.
Retained allocations/leases stay owned until all consumers join or a verified
durable recovery transfer completes. Fault schedules must cover stalled local
WAL and hosted quorum/participant calls, cancellation, shutdown, unknown outcomes,
health progress, and eventual reconciliation without duplicate decisions.

The current full production pair is separately queued from the frozen
pooling-control-cleanup-owner worktree. This isolated worktree inherits its
uncommitted candidate as the supplemental patch so later implementation can
continue without changing compiler inputs. The audit changes no production
behavior and does not enable pooling by default.
