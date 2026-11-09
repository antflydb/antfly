// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Diagnostic-only write-amplification counting layer for Lite.
//!
//! Every public function here is gated by `enabled` (== `builtin.is_test`),
//! a comptime-known constant, so every call site compiles to nothing at all
//! outside test binaries. This module exists solely to back the
//! `zig build lite-write-amplification` harness
//! (`zig/pkg/antfly-embedded/src/storage/lite/write_amplification_bench_test.zig`);
//! see `docs/design/lite-performance/README.md` for how to run it and read
//! its report.
//!
//! A handful of call sites elsewhere in `storage/` push a `Stage` tag onto a
//! thread-local stack before doing durable I/O and pop it afterward
//! (`scope`/`Scope.end`). The actual syscalls happen deep inside the shared
//! `std.Io` implementation the harness supplies, so the harness's `std.Io`
//! wrapper (not production code) calls `recordSync`/`recordWrite` at the
//! real leaf, and those read whatever `Stage` is currently on top of the
//! calling thread's stack.

const std = @import("std");
const builtin = @import("builtin");

/// True only inside test binaries. Every function below is a no-op (and,
/// because this is comptime-known, dead code eliminated) when false.
pub const enabled = builtin.is_test;

pub const Stage = enum(u8) {
    primary_commit,
    artifact_stage,
    artifact_link,
    artifact_pin,
    lsm_wal,
    progress,
    status_snapshot,
    replay_retirement,
    artifact_cleanup,
    // WS4 attribution: the Lite-only virtual-filesystem-on-native-catalog
    // call sites in `lite/index_storage.zig` that the LSM text/dense/sparse
    // backends use to store their logical files. These are the real sites
    // behind most of the previously-unattributed `other` bucket for an
    // indexed write; see docs/design/lite-performance/write-amplification-attributed.json.
    vfs_write,
    vfs_append,
    vfs_rename,
    vfs_delete,
    vfs_sync,
    vfs_finish,
    // The one-time durable lease-pointer commit `artifact_source.Registry.open`
    // issues on a non-pooled, non-reader-fenced artifact open.
    artifact_lease,
    // `PersistentIndex.MainTxn.commit`: the generic main-backend commit that
    // (today) resolves to an LSM-backend write-transaction commit for every
    // full-text/dense/sparse/graph index on Lite.
    main_txn_commit,
    // The LSM backend's own internal WAL append, inside `lsm_backend.zig`,
    // distinct from `PersistentIndex`'s own segment-admission WAL (`.lsm_wal`
    // above).
    lsm_backend_wal,
    // graph/graph.zig's GraphIndex.applyMutationPage: the outgoing
    // (forward-adjacency) store's commit and the reverse (incoming-
    // adjacency) store's commit. Distinct tags so the two-store commit
    // shape (one native commit per store today, collapsing toward one
    // physical sync under `.native_catalog` via ride-along) is visible
    // separately in the report. Only this function's own commits are
    // tagged; GraphIndex's other ~140 `beginWriteReverseBatch`/
    // `beginWriteOutgoingBatch` call sites (maintenance, metric builds,
    // topology compaction, ownership transfer) are background work and
    // fall through to whatever generic tag their backend's own leaf I/O
    // already pushes (`lsm_backend_wal`/`vfs_*`/`other`).
    graph_forward_commit,
    graph_reverse_commit,
    // storage/db/maintenance/graph_metric_runtime.zig's per-tick worker
    // checkpoint commit.
    graph_metric_commit,
    // Graph artifact-sourced edge materialization (asset/chunk-derived
    // extraction graph ingestion), analogous to `.artifact_stage`/
    // `.artifact_link` for text segments. Zero for the write-amplification
    // harness's caller-supplied (no-inference) edge writes.
    graph_artifact,
    // Dense/HBC attribution (lite-single-commit-write-path.md §3.3a): a
    // single-document, caller-supplied-vector write on Lite's
    // `.native_catalog` dense backend reaches THREE distinct native commits
    // beyond the primary document write itself (`.primary_commit`) --
    // `hbc_adapter.zig`'s `batchInsertWithMetadataOptions`/`batchApplyOptions`
    // call (vector row + posting-list update + any root/centroid node
    // rewrite, all staged into ONE native transaction by the HBC library
    // itself) and `saveProjectionCheckpointMetadata`'s own separate commit.
    // Pushed at the antfly-embedded call site that invokes into the HBC
    // library/vectorindex adapter, so it wins over
    // `native_catalog_backend.zig`'s generic `.main_txn_commit` fallback
    // (see `scopeDefault`) without needing any vectorindex-library change.
    hbc_vector_commit,
    // `index_manager.zig`'s `commitDenseVectorMappingsWithRollback`: the
    // dense-vector-id <-> document-key mapping record and `_embeddings`
    // artifact write, against the PRIMARY docstore (not HBC's own
    // `.native_catalog` backend) -- a second, separate round trip per
    // write, distinct from `.primary_commit`'s own original-document commit.
    hbc_metadata_commit,
    // `hbc_adapter.zig`'s `saveProjectionCheckpointMetadata`: the dense
    // index's projection-checkpoint/catch-up-progress KV record, against
    // HBC's own `.native_catalog` backend. Safe to lose and redo per
    // §3.2a's ride-along table ("Catch-up progress"); see
    // `HBCIndex.saveProjectionCheckpointMetadata`'s ride-along durability.
    hbc_checkpoint_commit,
    // Relational/transactional write-surface attribution (lite-single-
    // commit-write-path.md §3.2/§3.2a/§3.3c): the two-phase intent
    // protocol's three durability-bearing steps, each pushed with
    // `scopeDefault` so a more specific caller tag (none exist on this
    // path today) would still win. The tag must wrap the eventual
    // `ticket.wait()`, not just the staged call: a group-commit member is
    // only applied -- and so only fsynced -- once something drives/waits
    // its group, not at `enqueue` time, so the blocking wrapper that calls
    // `wait()` pushes the tag (`storage/transactions.zig`'s
    // `TxnManager.initTransactionWithParticipantsCreatedAtRoleAndRetentionExtraBatch`/
    // `writeIntentsExtraBatch`/`resolveIntentsWithExtraBatch`, and
    // `storage/db/db.zig`'s `DB.writeTransaction`/
    // `writeReplicatedTransactionAtOrderedReceipt`, which call the Staged
    // core/TxnManager primitives directly). `relational_txn_begin` is the
    // transaction record and participant sidecar; `relational_intent_write`
    // is intents plus admission/reservation bookkeeping, index-span
    // reservations, and any FK/unique check records folded into the same
    // batch; `relational_intent_resolve` is intents replaced by real rows,
    // the terminal decision, and participant-resolved bookkeeping.
    relational_txn_begin,
    relational_intent_write,
    relational_intent_resolve,
    // `capi/sql_commit.zig`'s cross-database two-phase commit coordinator:
    // the root database's own recovery-hint record (`store`, written before
    // preparing any participant and cleared after every participant
    // resolves) and the root's own coordinator-role begin/decision
    // transaction, both distinct from any participant table's own
    // `relational_txn_begin`/`relational_intent_write`/`relational_intent_resolve`.
    // Only engaged when a coordinated-constraint write spans more than one
    // physical store (`sql.zig`'s single-store fast path bypasses this
    // coordinator entirely for the common single-table case).
    relational_coordinator_hint,
    // Algebraic index write attribution (single-commit write path, algebraic
    // workstream): `algebraic_apply` is the foreground per-write fact commit
    // (`Index.applyBatchWritesLocked`/`applyAppendOnlyBatchWithOptions`),
    // `algebraic_bulk_finish` is bulk-ingest session finalization
    // (`finishBulkIngestSessionWithOptions`), `algebraic_hll_maintenance` is
    // background HLL sketch maintenance/backfill, and `algebraic_adaptive` is
    // adaptive-materialization bookkeeping (candidate/progress/lifecycle
    // persistence) outside the foreground apply commit.
    algebraic_apply,
    algebraic_bulk_finish,
    algebraic_hll_maintenance,
    algebraic_adaptive,
    // lite-single-commit-write-path.md §3.2a/§3.3c (task 4): docstore.zig's
    // `rideAlongFlushLoop` idle-flush timer (barrier (a)) -- the background
    // thread that bounds how long a ride-along write can sit undurable when
    // no foreground commit comes along to carry it. This is the expected,
    // amortized cost of ride-along's staleness bound, not an avoidable
    // per-write sync; distinguishing it from a genuinely unattributed
    // `.other` background sync is the point of this tag.
    ride_along_flush,
    other,
};
const stage_count = std.meta.tags(Stage).len;

pub const Op = enum(u8) {
    source_turn,
    maintenance_lease_acquired,
    serving_runtime_refresh,
    derived_worker_turn,
    // Diagnostic breakdown of why `mutation_commit.zig`'s graph-edge inline
    // publication fast path did or didn't engage for a given write -- added
    // to find which gate was rejecting the write-amplification harness's
    // own caller-supplied-edge graph mode (it never showed 1 commit/write).
    // `graph_inline_engaged` is the only "it worked" outcome; every other
    // `graph_inline_*` tag is mutually exclusive with it and with each
    // other for one write, so their sum across one mode run always equals
    // that mode's update count.
    graph_inline_engaged,
    // The caller-level shape gate in `mutation_commit.zig`
    // (`batchInternalPreparedOwned`) never even attempted
    // `prepareInlineGraphPublication` for this write. Split by which
    // specific sub-condition of that AND-chain was false (more than one can
    // be false for the same write; each false sub-condition increments its
    // own counter, so these can sum to more than the write count).
    graph_inline_caller_gate_failed,
    graph_inline_gate_not_append_derived_replay,
    graph_inline_gate_sync_level_not_full_index,
    graph_inline_gate_transaction_resolution,
    graph_inline_gate_multiple_graph_writes,
    graph_inline_gate_graph_deletes_present,
    graph_inline_gate_graph_doc_clears_present,
    graph_inline_gate_changed_artifact_keys_present,
    graph_inline_gate_deleted_keys_present,
    // `IndexManager.graphEdgeWriteEligibleForInlinePublication` rejected the
    // write before `prepareInlineEdgePublication` was even called.
    graph_inline_eligibility_ttl,
    graph_inline_eligibility_contributors,
    graph_inline_eligibility_owner,
    graph_inline_eligibility_out_of_range,
    // `graph/graph.zig`'s `GraphIndex.prepareInlineEdgePublication` itself
    // rejected the write despite passing the `IndexManager`-layer gate
    // above.
    graph_inline_prepare_backend_mismatch,
    graph_inline_prepare_owner,
    graph_inline_prepare_ttl,
    graph_inline_prepare_metrics,
    // Any other error out of `prepareInlineGraphPublication` (allocator
    // failure, a real bug, a validation error on a malformed write) --
    // caught the same way `error.InlineGraphPublicationUnsupported` is at
    // the call site, so distinguishing it here is the only way to see it
    // was not one of the expected "fall back to the slow path" reasons.
    graph_inline_prepare_other_error,
};
const op_count = std.meta.tags(Op).len;

pub const Lane = enum(u8) { calling_thread, background };
const lane_count = std.meta.tags(Lane).len;

fn zeroed(comptime T: type, comptime n: usize) [n]T {
    var out: [n]T = undefined;
    for (&out) |*slot| {
        slot.* = T.init(0);
    }
    return out;
}

fn zeroed2D(comptime T: type, comptime a: usize, comptime b: usize) [a][b]T {
    var out: [a][b]T = undefined;
    for (0..a) |i| {
        for (0..b) |j| {
            out[i][j] = T.init(0);
        }
    }
    return out;
}

var sync_calls: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var write_calls: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var write_bytes: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var durable_commits: [lane_count][stage_count]std.atomic.Value(u64) = zeroed2D(std.atomic.Value(u64), lane_count, stage_count);
var background_ops: [op_count]std.atomic.Value(u64) = zeroed(std.atomic.Value(u64), op_count);

const no_calling_thread: u64 = 0;
var calling_threads: [16]std.atomic.Value(u64) = zeroed(std.atomic.Value(u64), 16);

threadlocal var stage_stack: [16]Stage = undefined;
threadlocal var stage_depth: usize = 0;

/// Resets every counter and clears the calling-thread registry. Call once,
/// from the harness, before each measured run.
pub fn reset() void {
    if (!enabled) return;
    for (&sync_calls) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&write_calls) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&write_bytes) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&durable_commits) |*lane| for (lane) |*cell| cell.store(0, .monotonic);
    for (&background_ops) |*cell| cell.store(0, .monotonic);
    for (&calling_threads) |*slot| slot.store(no_calling_thread, .monotonic);
}

/// Marks the current OS thread as a "calling thread" (the thread that issued
/// an update and is waiting on its acknowledgement), as opposed to a
/// background maintenance/derived-worker thread. Call once per writer
/// thread; safe to call redundantly. Lock-free: registration only grows a
/// small fixed table, so a plain CAS loop is enough.
pub fn registerCallingThread() void {
    if (!enabled) return;
    const id: u64 = @intCast(std.Thread.getCurrentId());
    for (&calling_threads) |*slot| if (slot.load(.monotonic) == id) return;
    for (&calling_threads) |*slot| {
        if (slot.cmpxchgStrong(no_calling_thread, id, .monotonic, .monotonic) == null) return;
    }
}

fn currentLane() Lane {
    const id: u64 = @intCast(std.Thread.getCurrentId());
    for (&calling_threads) |*slot| if (slot.load(.monotonic) == id) return .calling_thread;
    return .background;
}

/// A scoped stage tag. Hold for the duration of one logical durability
/// stage (e.g. one `wal.append` call) on the current thread only.
pub const Scope = struct {
    active: bool,
    pub fn end(self: Scope) void {
        if (self.active) popStage();
    }
};

pub fn scope(stage: Stage) Scope {
    if (!enabled) return .{ .active = false };
    pushStage(stage);
    return .{ .active = true };
}

/// Like `scope`, but only pushes `stage` when no scope is already active on
/// this thread -- i.e. it is a *fallback* tag, not an override. Use this for
/// a generic/shared commit site (one that many different callers reach,
/// like `native_catalog_backend.zig`'s `NamespaceTxn.commit` or
/// `docstore.zig`'s `commitStaged`) so that any caller who already pushed a
/// more specific tag (e.g. `.hbc_vector_commit`, `.progress`) keeps
/// attribution, instead of the generic site's own push shadowing it.
/// `currentStage()` always returns the innermost (most recently pushed) tag,
/// so without this, a caller's specific tag pushed before calling into a
/// shared commit helper would otherwise be overwritten by that helper's own
/// generic push for the duration of the actual sync/write/commit. Returns a
/// `Scope` with `active = false` when deferring, so `Scope.end()` correctly
/// does nothing for a scope that never pushed.
pub fn scopeDefault(stage: Stage) Scope {
    if (!enabled) return .{ .active = false };
    if (stage_depth != 0) return .{ .active = false };
    pushStage(stage);
    return .{ .active = true };
}

fn pushStage(stage: Stage) void {
    if (stage_depth < stage_stack.len) stage_stack[stage_depth] = stage;
    stage_depth += 1;
}

fn popStage() void {
    if (stage_depth > 0) stage_depth -= 1;
}

fn currentStage() Stage {
    if (stage_depth == 0) return .other;
    return stage_stack[@min(stage_depth, stage_stack.len) - 1];
}

/// Record one durable sync (fsync/fdatasync/F_FULLFSYNC/msync) attributed to
/// the calling thread's current stage tag.
pub fn recordSync() void {
    if (!enabled) return;
    const lane = currentLane();
    const stage = currentStage();
    _ = sync_calls[@backingInt(lane)][@backingInt(stage)].fetchAdd(1, .monotonic);
}

/// Record one write/pwrite submission of `bytes` attributed to the calling
/// thread's current stage tag.
pub fn recordWrite(bytes: u64) void {
    if (!enabled) return;
    const lane = currentLane();
    const stage = @backingInt(currentStage());
    _ = write_calls[@backingInt(lane)][stage].fetchAdd(1, .monotonic);
    _ = write_bytes[@backingInt(lane)][stage].fetchAdd(bytes, .monotonic);
}

/// Record one durable commit (native store commit_sequence advance, or LSM
/// WAL append) attributed to the calling thread's current stage tag.
pub fn recordCommit() void {
    if (!enabled) return;
    const lane = currentLane();
    _ = durable_commits[@backingInt(lane)][@backingInt(currentStage())].fetchAdd(1, .monotonic);
}

/// Record one background/maintenance operation, independent of stage.
pub fn recordOp(op: Op) void {
    if (!enabled) return;
    _ = background_ops[@backingInt(op)].fetchAdd(1, .monotonic);
}

pub const StageCounts = struct {
    sync_calling_thread: u64,
    sync_background: u64,
    write_calls_calling_thread: u64,
    write_calls_background: u64,
    write_bytes_calling_thread: u64,
    write_bytes_background: u64,
    commits_calling_thread: u64,
    commits_background: u64,
};

pub const Report = struct {
    stages: [stage_count]StageCounts,
    ops: [op_count]u64,

    pub fn stage(self: *const Report, s: Stage) StageCounts {
        return self.stages[@backingInt(s)];
    }

    pub fn op(self: *const Report, o: Op) u64 {
        return self.ops[@backingInt(o)];
    }

    pub fn totalSyncs(self: *const Report, lane: Lane) u64 {
        var total: u64 = 0;
        for (self.stages) |counts| total += switch (lane) {
            .calling_thread => counts.sync_calling_thread,
            .background => counts.sync_background,
        };
        return total;
    }
};

pub fn stageName(s: Stage) []const u8 {
    return @tagName(s);
}

pub fn opName(o: Op) []const u8 {
    return @tagName(o);
}

/// Snapshot every counter. Does not reset them.
pub fn snapshot() Report {
    var report: Report = undefined;
    for (0..stage_count) |i| {
        report.stages[i] = .{
            .sync_calling_thread = sync_calls[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .sync_background = sync_calls[@backingInt(Lane.background)][i].load(.monotonic),
            .write_calls_calling_thread = write_calls[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .write_calls_background = write_calls[@backingInt(Lane.background)][i].load(.monotonic),
            .write_bytes_calling_thread = write_bytes[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .write_bytes_background = write_bytes[@backingInt(Lane.background)][i].load(.monotonic),
            .commits_calling_thread = durable_commits[@backingInt(Lane.calling_thread)][i].load(.monotonic),
            .commits_background = durable_commits[@backingInt(Lane.background)][i].load(.monotonic),
        };
    }
    for (0..op_count) |i| report.ops[i] = background_ops[i].load(.monotonic);
    return report;
}

test "write amplification trace scopes attribute to the right stage and lane" {
    reset();
    registerCallingThread();
    {
        const s = scope(.primary_commit);
        defer s.end();
        recordSync();
        recordWrite(128);
        recordCommit();
    }
    {
        const s = scope(.lsm_wal);
        defer s.end();
        recordSync();
    }
    recordOp(.source_turn);
    recordOp(.source_turn);

    const report = snapshot();
    try std.testing.expectEqual(@as(u64, 1), report.stage(.primary_commit).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 0), report.stage(.primary_commit).sync_background);
    try std.testing.expectEqual(@as(u64, 128), report.stage(.primary_commit).write_bytes_calling_thread);
    try std.testing.expectEqual(@as(u64, 1), report.stage(.primary_commit).commits_calling_thread);
    try std.testing.expectEqual(@as(u64, 1), report.stage(.lsm_wal).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 0), report.stage(.other).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 2), report.op(.source_turn));

    const background = struct {
        fn run() void {
            const s = scope(.replay_retirement);
            defer s.end();
            recordSync();
        }
    };
    const thread = try std.Thread.spawn(.{}, background.run, .{});
    thread.join();
    const after = snapshot();
    try std.testing.expectEqual(@as(u64, 1), after.stage(.replay_retirement).sync_background);
    try std.testing.expectEqual(@as(u64, 0), after.stage(.replay_retirement).sync_calling_thread);
}

test "scopeDefault defers to an already-active caller tag instead of shadowing it" {
    // Regression test for the attribution bug `scopeDefault` fixes: a
    // generic/shared commit helper (e.g. `native_catalog_backend.zig`'s
    // `NamespaceTxn.commit`, `docstore.zig`'s `commitStaged`) used to push
    // its own tag unconditionally with plain `scope`, which -- because
    // `currentStage()` returns the innermost (most recently pushed) tag --
    // always shadowed whatever more specific tag a caller had already
    // pushed before calling into it. `scopeDefault` only pushes when no
    // scope is active, so the caller's own tag survives.
    reset();
    registerCallingThread();
    {
        const outer = scope(.hbc_checkpoint_commit);
        defer outer.end();
        {
            // Simulates a shared commit helper's own fallback push while a
            // caller-specific tag is already active: must be a no-op.
            const inner = scopeDefault(.main_txn_commit);
            defer inner.end();
            recordSync();
            recordCommit();
        }
    }
    const report = snapshot();
    try std.testing.expectEqual(@as(u64, 1), report.stage(.hbc_checkpoint_commit).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 1), report.stage(.hbc_checkpoint_commit).commits_calling_thread);
    try std.testing.expectEqual(@as(u64, 0), report.stage(.main_txn_commit).sync_calling_thread);
    try std.testing.expectEqual(@as(u64, 0), report.stage(.main_txn_commit).commits_calling_thread);

    // With nothing already active, scopeDefault pushes normally (the
    // sparse/dense-without-an-outer-wrap case: nothing shadows it, so the
    // generic tag is exactly right).
    {
        const only = scopeDefault(.main_txn_commit);
        defer only.end();
        recordSync();
    }
    const after = snapshot();
    try std.testing.expectEqual(@as(u64, 1), after.stage(.main_txn_commit).sync_calling_thread);
}
