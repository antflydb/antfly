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

const std = @import("std");
const local = @import("antfly_local_sources");
const platform = @import("antfly_platform");
const admission = local.common_read_admission;
const lsm = local.storage_lsm_backend_mod;
const DB = local.storage_db_mod.DB;
const foreground = local.storage_lsm_backend_compaction_publication.manifest_edit_preparation.foreground_append;
pub const consumer_tests_only = true;
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const capi_dependencies = @import("capi_dependencies.zig");

test {
    _ = admission;
    _ = local.common_cleanup_wait;
    _ = local.storage_lsm_backend_state;
}

fn drain(backend: *lsm.Backend) !void {
    const until = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    while (true) {
        if (backend.mu.tryLock()) {
            const done = backend.retired_manifest_preparations == null and !backend.manifest_preparation_reclaim_in_flight and !backend.maintenance_job_in_flight;
            backend.mu.unlock();
            if (done) return;
        }
        if (platform.time.monotonicNs() >= until) return error.TestUnexpectedResult;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
}

const Mode = enum { legacy, owned, cancel_preparation, cancel_read, cancel_append, deadline_append, handoff_read, handoff_return_timeout };

const Entry = enum { db, table, worker, handoff };

/// Use the same callback dispatch and stack-owned controls as bound HTTP/SQL
/// table commits. The direct DB cases remain useful for legacy-policy coverage.
fn commit(entry: Entry, db: *DB, writes: []const local.storage_db_mod.types.BatchWrite, controls: local.storage_db_apply_rw_lock.ApplyRwLock.Acquisition, original: local.api_local_transaction_contract.PreDecisionContext, borrow: ?@import("antfly_runtime_abi").runnable_abi.Borrowed) !void {
    if (entry == .db) return db.batchWithAdmissionControls(.{ .writes = writes }, controls, .none);
    const a = std.testing.allocator;
    const converted = try a.alloc(local.storage_db_mod.types.TransactionWrite, writes.len);
    defer a.free(converted);
    for (converted, writes) |*target, source| target.* = .{ .key = source.key, .value = source.value };
    var bound = local.api_local_table_writes.BoundTableWriteSource.init("docs", db);
    var team: @import("antfly_runtime_abi").runnable_abi.Team = .{ .borrow = borrow.?, .maximum_slots = 1, .allocator = a };
    var context = original;
    if (entry == .worker) context.execution_runnable = borrow;
    const outcome = (try bound.source().commitBatchWithControl(a, &.{.{ .table_name = "docs", .writes = converted }}, .write, original.cancellation, .{ .pre_decision = context, .team = if (entry == .worker) null else &team })) orelse return error.TableCommitNotHandled;
    switch (outcome) {
        .committed => |done| try std.testing.expectEqual(@as(usize, 1), done.participant_count),
        .conflict => return error.UnexpectedCommitConflict,
    }
}

fn guardedCommit(mode: Mode) !void {
    return guardedCommitVia(mode, .db);
}

fn guardedCommitVia(mode: Mode, entry: Entry) !void {
    const owned_io = mode != .legacy;
    const committed_return_failure = mode == .handoff_return_timeout;
    const rejected = mode != .legacy and mode != .owned and mode != .handoff_read;
    const paused_io = mode == .cancel_read or mode == .cancel_append or mode == .deadline_append or mode == .handoff_read or mode == .handoff_return_timeout;
    const uncertain = mode == .cancel_append or mode == .deadline_append or committed_return_failure;
    const io_mod = local.storage_lsm_backend_storage_io;
    const native = io_mod.NativeSyncTestControl;
    const a = std.testing.allocator;
    const Resources = @import("storage/physical_resources.zig").PhysicalStorageResources;
    const budgets = @import("storage/memory_budget.zig");
    const owners = @import("api/runnable_owner_execution.zig");
    const scheduler = local.common_request_admission.runnable_scheduler;
    var state: owners.State = .{ .owner_alloc = a, .owner = try scheduler.NodeOwner.init(.{ .slots = if (entry == .handoff) 4 else 3, .control_slots = 1, .cleanup_slots = 1, .publication_slots = if (entry == .handoff) 1 else 0 }), .native_now_ns = platform.time.monotonicNs };
    defer state.owner.deinit();
    var resources = try Resources.initWithBudgets(a, budgets.smartResourceBudgetsForTotal(1 << 30));
    defer resources.deinit();
    resources.attachResourceManager();
    defer while (resources.resource_manager.payload_retirement_ready.load(.acquire) != 0) {
        _ = resources.resource_manager.drainPayloadRetirementQuiesced();
    };
    var backend_runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .runnable_owner = state.port(), .memory_parent = resources.resource_manager.memoryParentPort(), .lane_limits = .{
        .durable_background = 8,
        .api = 1,
        .request_forward = 6,
        .raft_inbound = 1,
        .raft_outbound = 1,
        .inference = 1,
        .control = 1,
        .pdf_render = 1,
        .worker_capacity = 0,
        .sql_worker_capacity = 0,
    } });
    defer backend_runtime.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("guarded-managed-db-commit");
    defer directory.cleanup();
    const options: local.storage_db_mod.OpenOptions = .{
        .backend_runtime = backend_runtime.runtime,
        .resource_manager = &resources.resource_manager,
        .lsm_cache = &resources.lsm_cache,
        .hbc_cache = &resources.hbc_cache,
        .start_index_workers = false,
        .start_independent_maintenance = false,
        .start_optional_runtimes = false,
        .start_resolver_workers = false,
    };
    var db = try DB.open(a, directory.path(), options);
    var open = true;
    defer if (open) db.close();
    try db.ensureDurableReplicationStartupBarrier();
    try db.batch(.{ .writes = &.{.{ .key = "seed", .value = "{\"old\":true}" }} });
    const backend = db.core.primary_store_owner.lsm.handle.backend;
    try drain(backend);
    const H = struct {
        var execution: *owners.State = undefined;
        var database: *DB = undefined;
        var selected: Mode = .legacy;
        var guarded: bool = false;
        var resumed: std.atomic.Value(bool) = .init(false);
        var return_started: std.atomic.Value(bool) = .init(false);
        var original_finish: *const fn (*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status = undefined;
        fn finish(raw: *anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return_started.store(true, .release);
            return original_finish(raw);
        }
        var deadline_ns: u64 = 0;
        var injected: std.atomic.Value(bool) = .init(false);
        var joins: std.atomic.Value(usize) = .init(0);
        var join_authorized: std.atomic.Value(bool) = .init(false);
        var health: std.atomic.Value(usize) = .init(0);
        var native_retire: std.atomic.Value(usize) = .init(0);
        var steps: std.atomic.Value(usize) = .init(0);
        var manager: *@TypeOf(resources.resource_manager) = undefined;
        var retained_peak: std.atomic.Value(u64) = .init(0);
        var refused: std.atomic.Value(bool) = .init(false);
        var foreground_calls: std.atomic.Value(usize) = .init(0);
        var canceled: std.atomic.Value(bool) = .init(false);
        // Keep execution lifetime live when only the original API context is
        // canceled. Otherwise the owner token could hide a lost request fence.
        var execution_canceled: std.atomic.Value(bool) = .init(false);
        fn cancellation(raw: ?*const anyopaque) callconv(.c) u8 {
            const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return @intFromBool(flag.load(.acquire));
        }
        fn retire(stage: usize, _: usize, original: admission.Port) !void {
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.owner.scheduler.stats().cleanup_runnable_slots);
            if (stage == 0) {
                _ = steps.fetchAdd(1, .monotonic);
                _ = retained_peak.fetchMax(manager.sliceStats(.lsm_table_builder_working_set).used_bytes, .monotonic);
                if (!refused.swap(true, .acq_rel)) return error.ResourceBudgetExceeded;
            }
        }
        fn prepare(_: *foreground.Owner, original: admission.Port) !void {
            try original.checkpoint();
            try std.testing.expect(!try original.hasExecutionWait());
            try std.testing.expect(try original.allowsRetainedPhysicalWait());
            if (database.core.apply_mutex.tryLockShared()) {
                database.core.apply_mutex.unlockShared();
                return error.MissingGuardedApplyFence;
            }
            if (database.core.snapshot_admission.tryAcquireCapture()) |captured| {
                var lease = captured;
                lease.release();
                return error.MissingGuardedSnapshotFence;
            }
            _ = foreground_calls.fetchAdd(1, .monotonic);
            if (selected == .cancel_preparation and !injected.swap(true, .acq_rel)) {
                canceled.store(true, .release);
                try original.checkpoint();
            }
            if ((selected == .cancel_read or selected == .handoff_read or selected == .handoff_return_timeout) and !injected.swap(true, .acq_rel)) pause();
        }
        fn pause() void {
            native.started.store(false, .release);
            native.release.store(false, .release);
            native.pause.store(true, .release);
        }
        fn append(operation: *foreground.Operation, original: admission.Port) !void {
            try original.checkpoint();
            if (guarded and (selected == .handoff_read or selected == .handoff_return_timeout) and operation.stage == .append and !resumed.load(.acquire)) {
                try std.testing.expectEqual(@as(usize, 1), execution.owner.scheduler.stats().publication_runnable_slots);
                resumed.store(true, .release);
            }
            if ((selected == .cancel_append or selected == .deadline_append) and operation.stage == .append and !injected.swap(true, .acq_rel)) pause();
        }
        fn join(original: admission.Port) !void {
            _ = joins.fetchAdd(1, .release);
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.owner.scheduler.stats().cleanup_runnable_slots);
            join_authorized.store(true, .release);
        }
        fn retireNative(work: *foreground.Owner, original: admission.Port) !void {
            try original.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), execution.owner.scheduler.stats().cleanup_runnable_slots);
            _ = retained_peak.fetchMax(manager.sliceStats(.lsm_table_builder_working_set).used_bytes, .monotonic);
            if (work.input_index != null) _ = native_retire.fetchAdd(1, .monotonic);
        }
    };
    H.execution = &state;
    H.selected = mode;
    H.guarded = entry == .handoff;
    H.resumed.store(false, .release);
    H.return_started.store(false, .release);
    H.injected.store(false, .release);
    H.joins.store(0, .release);
    H.join_authorized.store(false, .release);
    H.health.store(0, .release);
    native.pause.store(false, .release);
    native.started.store(false, .release);
    native.release.store(true, .release);
    H.database = &db;
    H.native_retire.store(0, .release);
    H.manager = &resources.resource_manager;
    H.retained_peak.store(0, .release);
    H.refused.store(false, .release);
    H.steps.store(0, .release);
    H.foreground_calls.store(0, .release);
    H.canceled.store(false, .release);
    H.execution_canceled.store(false, .release);
    local.storage_lsm_backend_runtime.test_write_retirement_hook = H.retire;
    foreground.test_hook = H.prepare;
    foreground.test_retire_hook = H.retireNative;
    foreground.test_abort_hook = H.join;
    foreground.Operation.test_hook = H.append;
    defer {
        native.release.store(true, .release);
        native.pause.store(false, .release);
        if (open) {
            db.close();
            open = false;
        }
        local.storage_lsm_backend_runtime.test_write_retirement_hook = null;
        foreground.test_hook = null;
        foreground.test_retire_hook = null;
        foreground.test_abort_hook = null;
        foreground.Operation.test_hook = null;
    }
    const FenceWaiter = struct {
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.drive() catch |cause| {
                self.failure = cause;
            };
        }
        fn drive(_: *@This()) !void {
            const io = std.testing.io;
            const runtime = @import("antfly_runtime_abi");
            const io_borrow = runtime.io_abi.Borrow.init(&io);
            const owner = H.execution.port();
            var handle: ?*anyopaque = null;
            const started = owner.start(owner.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = platform.time.monotonicNs() +| 15 * std.time.ns_per_s, .out_handle = &handle });
            if (!started.isOk()) return runtime.error_abi.errorFromStatus(started);
            defer owner.finish(handle.?);
            try std.testing.expect((try H.database.vectorMigrationStatus(std.testing.allocator)) == null);
            const value = (try H.database.get(std.testing.allocator, "doc:0000")) orelse return error.PublishedValueMissing;
            defer std.testing.allocator.free(value);
            try std.testing.expectEqualStrings("{\"value\":1}", value);
            if (H.selected == .handoff_return_timeout) {
                // A slow ordinary consumer keeps its already admitted CPU.
                // The committed publisher must respect bounded return waiting
                // without claiming that its durable mutation was rejected.
                const observed_until = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
                while (!H.return_started.load(.acquire)) {
                    if (platform.time.monotonicNs() >= observed_until) return error.PublicationReturnNotObserved;
                    std.atomic.spinLoopHint();
                }
                const until = platform.time.monotonicNs() +| 500 * std.time.ns_per_ms;
                while (platform.time.monotonicNs() < until) std.atomic.spinLoopHint();
            }
        }
    };
    const Coordinator = struct {
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            defer native.release.store(true, .release);
            self.drive() catch |cause| {
                self.failure = cause;
            };
        }
        fn drive(_: *@This()) !void {
            const until = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
            while (!native.started.load(.acquire)) {
                if (platform.time.monotonicNs() >= until) return error.PhysicalWaitNotObserved;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            {
                const runtime = @import("antfly_runtime_abi");
                const io = std.testing.io;
                const borrow = runtime.native_abi.IoBorrow.init(&io);
                const owner = H.execution.port();
                var handle: ?*anyopaque = null;
                const status = owner.start(owner.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .has_deadline = 1, .native_deadline = 1, .deadline_ns = until, .out_handle = &handle });
                try std.testing.expect(status.isOk());
                defer owner.finish(handle.?);
                _ = H.health.fetchAdd(1, .release);
            }
            if (H.selected == .handoff_read or H.selected == .handoff_return_timeout) {
                var waiter: FenceWaiter = .{};
                var thread: ?std.Thread = try std.Thread.spawn(.{}, FenceWaiter.run, .{&waiter});
                defer if (thread) |live| {
                    native.release.store(true, .release);
                    live.join();
                };
                while (H.database.core.apply_mutex.shared_waiters.load(.acquire) == 0) {
                    if (platform.time.monotonicNs() >= until) return error.OrdinaryFenceWaitNotObserved;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                }
                try std.testing.expectEqual(@as(usize, 1), H.execution.owner.scheduler.stats().runnable_slots);
                native.release.store(true, .release);
                thread.?.join();
                thread = null;
                if (waiter.failure) |cause| return cause;
                return;
            }
            if (H.selected == .deadline_append) {
                while (platform.time.monotonicNs() < H.deadline_ns)
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            } else H.canceled.store(true, .release);
            const join_until = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
            while (H.joins.load(.acquire) == 0) {
                if (platform.time.monotonicNs() >= join_until) return error.ProtectedJoinNotObserved;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const writes = try arena.allocator().alloc(local.storage_db_mod.types.BatchWrite, if (rejected or entry == .worker or entry == .handoff) 64 else 1024);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "doc:{d:0>4}", .{i}), .value = "{\"value\":1}" };
    {
        var pass = try local.common_background_execution.Pass.init(state.port(), &std.testing.io, platform.time.monotonicNs() +| 60 * std.time.ns_per_s, .{ .context = &H.execution_canceled, .is_cancelled = H.cancellation });
        defer pass.deinit();
        if (committed_return_failure) {
            H.original_finish = pass.borrow.?.finish_publication.?;
            pass.borrow.?.finish_publication = H.finish;
        }
        const api_controls = local.api_mutation_admission_controls;
        H.deadline_ns = if (mode == .deadline_append) platform.time.monotonicNs() +| 5 * std.time.ns_per_s else pass.deadline_ns;
        const original: local.api_local_transaction_contract.PreDecisionContext = .{ .deadline_ns = H.deadline_ns, .cancellation = local.storage_db_mod.types.CancellationToken.fromAtomic(&H.canceled) };
        var budget = api_controls.init(original, pass.borrow, std.testing.io);
        var controls = api_controls.acquisition(&budget);
        if (!owned_io) controls.retained_physical_wait = false;
        var coordinator: Coordinator = .{};
        var thread: ?std.Thread = if (paused_io) try std.Thread.spawn(.{}, Coordinator.run, .{&coordinator}) else null;
        defer if (thread) |live| {
            native.release.store(true, .release);
            live.join();
        };
        if (rejected) {
            const before = backend.snapshotWriteStats();
            const result = commit(entry, &db, writes, controls, original, pass.borrow);
            if (thread) |live| {
                live.join();
                thread = null;
            }
            if (coordinator.failure) |cause| return cause;
            if (uncertain) try std.testing.expectError(error.DurabilityOutcomeUnknown, result) else try std.testing.expectError(error.Canceled, result);
            try std.testing.expectEqual(uncertain and !committed_return_failure, backend.manifest_recovery_required);
            if (committed_return_failure)
                try std.testing.expect(backend.snapshotWriteStats().wal_append_records > before.wal_append_records)
            else
                try std.testing.expectEqual(before.wal_append_records, backend.snapshotWriteStats().wal_append_records);
            try std.testing.expect(H.injected.load(.acquire));
            if (paused_io and !committed_return_failure) {
                try std.testing.expect(H.joins.load(.acquire) != 0);
                try std.testing.expect(H.join_authorized.load(.acquire));
                try std.testing.expectEqual(@as(usize, 1), H.health.load(.acquire));
            }
            try drain(backend);
        } else for (0..2) |round| {
            const before = H.steps.load(.acquire);
            if (round == 1) for (writes) |*write| {
                write.value = "{\"value\":2}";
            };
            try commit(entry, &db, writes, controls, original, pass.borrow);
            if (thread) |live| {
                live.join();
                thread = null;
                if (coordinator.failure) |cause| return cause;
                try std.testing.expect(H.resumed.load(.acquire));
            }
            // The public DB path retains apply/snapshot fences. Physical
            // cleanup must finish using its dedicated CPU while this pass
            // still owns the sole ordinary slot, including overwritten roots.
            try drain(backend);
            if (owned_io) {
                try std.testing.expect(H.foreground_calls.load(.acquire) != 0);
                try std.testing.expect(H.native_retire.load(.acquire) != 0);
            } else {
                try std.testing.expect(H.steps.load(.acquire) - before >= writes.len);
                try std.testing.expectEqual(@as(usize, 0), H.foreground_calls.load(.acquire));
            }
            try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().publication_runnable_slots);
            try std.testing.expect(state.owner.scheduler.publication_owner == null);
        }
        if (entry == .handoff and rejected) {
            // A denied guarded return stops logical execution while retaining
            // its physical owners. It cannot revive under the still-live node
            // token after the independent request fence has rejected it.
            if (pass.borrow.?.checkLifetime()) |_| return error.RejectedPublicationResumed else |_| {}
            try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().publication_runnable_slots);
            try std.testing.expect(state.owner.scheduler.publication_owner == null);
        } else try pass.borrow.?.checkLifetime();
        if (!rejected) {
            try std.testing.expect(H.refused.load(.acquire));
            try std.testing.expect(H.retained_peak.load(.acquire) >= writes.len * 32);
            const before = backend.snapshotWriteStats();
            H.canceled.store(true, .release);
            try std.testing.expectError(error.Canceled, commit(entry, &db, &.{.{ .key = "rejected", .value = "{}" }}, controls, original, pass.borrow));
            try std.testing.expectEqual(before.wal_append_records, backend.snapshotWriteStats().wal_append_records);
        }
    }
    try std.testing.expect(db.core.tryLockApplyExclusive());
    db.core.unlockApply();
    var capture = db.core.snapshot_admission.tryAcquireCapture() orelse return error.LeakedMutationAdmission;
    capture.release();
    if (!rejected) {
        const value = (try db.get(a, "doc:0000")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("{\"value\":2}", value);
        try std.testing.expect((try db.get(a, "rejected")) == null);
    }
    if (uncertain and !committed_return_failure) @import("antfly_test_error_logs").expectErrorLogs(1);
    db.close();
    open = false;
    try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
    var reopened = try DB.open(a, directory.path(), options);
    defer reopened.close();
    const seed = (try reopened.get(a, "seed")).?;
    defer a.free(seed);
    try std.testing.expectEqualStrings("{\"old\":true}", seed);
    if (rejected and !uncertain) {
        try std.testing.expect((try reopened.get(a, writes[writes.len - 1].key)) == null);
    } else {
        const recovered = (try reopened.get(a, writes[writes.len - 1].key)).?;
        defer a.free(recovered);
        try std.testing.expectEqualStrings(if (uncertain) "{\"value\":1}" else "{\"value\":2}", recovered);
    }
}

test "guarded managed DB commit retires complete staging and prior roots while foreground owns the only ordinary CPU" {
    try guardedCommit(.legacy);
}

test "guarded managed DB commit preserves original finite API controls through owned native WAL IO" {
    try guardedCommit(.owned);
}

test "guarded managed DB commit rejects canceled owned preparation before WAL mutation" {
    try guardedCommit(.cancel_preparation);
}

test "guarded managed DB commit joins accepted read after cancellation while health controls progress" {
    try guardedCommit(.cancel_read);
}

test "guarded managed DB commit preserves append uncertainty and recovery after cancellation" {
    try guardedCommit(.cancel_append);
}

test "guarded managed DB commit preserves original deadline and append uncertainty through protected join" {
    try guardedCommit(.deadline_append);
}

test "guarded managed DB commit table callback preserves owned WAL and complete root retirement" {
    try guardedCommitVia(.owned, .table);
}

test "guarded managed DB commit table callback preserves cancellation before WAL publication" {
    try guardedCommitVia(.cancel_read, .table);
}

test "guarded managed DB commit table callback preserves uncertain append through original cancellation" {
    try guardedCommitVia(.cancel_append, .table);
}

test "guarded managed DB commit table callback preserves original deadline through uncertain append" {
    try guardedCommitVia(.deadline_append, .table);
}

test "guarded managed DB commit preserves context worker authority without a coordinator team" {
    try guardedCommitVia(.owned, .worker);
}

test "guarded managed DB commit context worker preserves independent request cancellation" {
    try guardedCommitVia(.cancel_read, .worker);
}

test "guarded managed DB commit context worker joins uncertain append after request cancellation" {
    try guardedCommitVia(.cancel_append, .worker);
}

test "guarded managed DB commit context worker preserves original deadline after accepted append" {
    try guardedCommitVia(.deadline_append, .worker);
}

test "guarded managed DB commit returns on publication CPU while an ordinary status request waits for its apply fence" {
    try guardedCommitVia(.handoff_read, .handoff);
}

test "guarded managed DB commit guarded return preserves independent cancellation and joins physical read" {
    try guardedCommitVia(.cancel_read, .handoff);
}

test "guarded managed DB commit guarded return preserves uncertain append and protected cleanup on cancellation" {
    try guardedCommitVia(.cancel_append, .handoff);
}

test "guarded managed DB commit guarded return preserves original deadline and uncertain append recovery" {
    try guardedCommitVia(.deadline_append, .handoff);
}

test "guarded managed DB commit reports durable uncertainty when ordinary return times out after successful publication" {
    try guardedCommitVia(.handoff_return_timeout, .handoff);
}

test "guarded incremental publication memory bound covers measured peak across private inserts and overwrites" {
    const Allocator = std.mem.Allocator;
    const ActiveMemTable = @FieldType(lsm.Backend, "mutable");
    const Tracker = struct {
        live: usize = 0,
        peak: usize = 0,
        fn allocator(self: *@This()) Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = std.testing.allocator.rawAlloc(len, alignment, ret) orelse return null;
            self.live += len;
            self.peak = @max(self.peak, self.live);
            return result;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.live -= bytes.len;
            std.testing.allocator.rawFree(bytes, alignment, ret);
        }
        fn check(_: *anyopaque) !void {}
    };
    const Port = admission;
    const Case = struct { base: usize, edits: usize };
    for ([_]Case{ .{ .base = 0, .edits = 4096 }, .{ .base = 1024, .edits = 4096 }, .{ .base = 4096, .edits = 1 }, .{ .base = 4096, .edits = 64 } }) |case| {
        var tracker: Tracker = .{};
        const a = tracker.allocator();
        {
            var live: ActiveMemTable = .{};
            defer live.deinit(a);
            var incoming: ActiveMemTable = .{ .ordered_enabled = false };
            defer incoming.deinit(a);
            for (0..case.base) |i| {
                var key: [32]u8 = undefined;
                try live.upsert(a, .{}, try std.fmt.bufPrint(&key, "key:{d}", .{i}), "old", false);
            }
            for (0..case.edits) |i| {
                // Visit every tree region, mixing replacements and inserts.
                const spacing = if (case.edits < case.base) case.base / case.edits else 1;
                const index = ((i * 1031) % case.edits) * spacing;
                var key: [32]u8 = undefined;
                try incoming.upsert(a, .{}, try std.fmt.bufPrint(&key, "key:{d}", .{index}), "new", false);
            }
            const payload: Port.Payload = .{ .ptr = &tracker, .vtable = &.{ .checkpoint = Tracker.check } };
            const original = Port.Port.init(&payload);
            var preparation = ActiveMemTable.PublicationPreparation.initBorrowed(&live, &incoming);
            defer preparation.deinit(a);
            const baseline = tracker.live;
            tracker.peak = baseline;
            const bound = try preparation.memoryBoundWithAdmission(original);
            try preparation.resumeWithAdmission(a, original);
            try std.testing.expect(tracker.peak - baseline <= bound);
            preparation.publish(&live);
            try preparation.retireWithAdmission(a, original);
            try std.testing.expect(live.entryCount() >= @max(case.base, case.edits));
        }
        try std.testing.expectEqual(@as(usize, 0), tracker.live);
    }
}

test "retained physical wait polls independent readiness but cannot wait for a service mutex" {
    const Owner = struct {
        checks: usize = 0,
        observations: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
        fn retained(_: *anyopaque) !bool {
            return true;
        }
        fn ready(raw: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observations += 1;
            return self.observations >= 3;
        }
    };
    var owner: Owner = .{};
    const payload: admission.Payload = .{ .ptr = &owner, .vtable = &.{ .checkpoint = Owner.check, .check_lifetime = Owner.check, .retained_physical_wait = Owner.retained } };
    const original = admission.Port.init(&payload);
    try local.common_cleanup_wait.untilPhysical(std.testing.io, original, &owner, Owner.ready, null);
    try std.testing.expect(owner.checks >= owner.observations);
    var mutex: std.atomic.Mutex = .locked;
    try std.testing.expectError(error.ExecutionWaitUnsupported, local.common_cleanup_wait.lockAtomic(&mutex, std.testing.io, original));
    mutex.unlock();
}

// The hosted server supplies shared execution services, with no local threaded
// executor. Keep the producer alive through both database and consumer joins.
test "guarded managed DB commit hosted shared services open commit and reopen with index workers enabled" {
    const a = std.testing.allocator;
    const Resources = @import("storage/physical_resources.zig").PhysicalStorageResources;
    const budgets = @import("storage/memory_budget.zig");
    var resources = try Resources.initWithBudgets(a, budgets.smartResourceBudgetsForTotal(1 << 30));
    defer resources.deinit();
    resources.attachResourceManager();
    var producer = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{
        .backend = .io_threaded,
        .memory_parent = resources.resource_manager.memoryParentPort(),
        .lane_limits = .{
            .durable_background = 8,
            .api = 1,
            .request_forward = 6,
            .raft_inbound = 1,
            .raft_outbound = 1,
            .inference = 1,
            .control = 1,
            .pdf_render = 1,
            .worker_capacity = 0,
            .sql_worker_capacity = 0,
        },
    });
    defer producer.deinit();
    var offer = local.storage_execution_services.Offer.init(producer.runtime);
    var consumer = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{
        .backend = .manual,
        .execution_services = &offer,
        .memory_parent = resources.resource_manager.memoryParentPort(),
    });
    defer consumer.deinit();
    try std.testing.expect(consumer.runtime.io_impl == null);
    var directory = try local.common_test_directory.TestDirectory.init("hosted-derived-runtime");
    defer directory.cleanup();
    const options: local.storage_db_mod.OpenOptions = .{
        .backend_runtime = consumer.runtime,
        .resource_manager = &resources.resource_manager,
        .lsm_cache = &resources.lsm_cache,
        .hbc_cache = &resources.hbc_cache,
        .start_index_workers = true,
        .start_independent_maintenance = false,
        .start_optional_runtimes = false,
        .start_resolver_workers = false,
    };
    {
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        try db.ensureDurableReplicationStartupBarrier();
        try db.batch(.{ .writes = &.{.{ .key = "hosted", .value = "{\"value\":1}" }} });
    }
    {
        var db = try DB.open(a, directory.path(), options);
        defer db.close();
        const value = (try db.get(a, "hosted")).?;
        defer a.free(value);
        try std.testing.expectEqualStrings("{\"value\":1}", value);
    }
}
