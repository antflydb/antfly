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

const Mode = enum { legacy, owned, cancel_preparation, cancel_read, cancel_append, deadline_append };

fn guardedCommit(mode: Mode) !void {
    const owned_io = mode != .legacy;
    const rejected = mode != .legacy and mode != .owned;
    const paused_io = mode == .cancel_read or mode == .cancel_append or mode == .deadline_append;
    const uncertain = mode == .cancel_append or mode == .deadline_append;
    const io_mod = local.storage_lsm_backend_storage_io;
    const native = io_mod.NativeSyncTestControl;
    const a = std.testing.allocator;
    const Resources = @import("storage/physical_resources.zig").PhysicalStorageResources;
    const budgets = @import("storage/memory_budget.zig");
    const owners = @import("api/runnable_owner_execution.zig");
    const scheduler = local.common_request_admission.runnable_scheduler;
    var state: owners.State = .{ .owner_alloc = a, .owner = try scheduler.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = platform.time.monotonicNs };
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
            if (selected == .cancel_read and !injected.swap(true, .acq_rel)) pause();
        }
        fn pause() void {
            native.started.store(false, .release);
            native.release.store(false, .release);
            native.pause.store(true, .release);
        }
        fn append(operation: *foreground.Operation, original: admission.Port) !void {
            try original.checkpoint();
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
    const writes = try arena.allocator().alloc(local.storage_db_mod.types.BatchWrite, if (rejected) 64 else 1024);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "doc:{d:0>4}", .{i}), .value = "{\"value\":1}" };
    {
        var pass = try local.common_background_execution.Pass.init(state.port(), &std.testing.io, platform.time.monotonicNs() +| 60 * std.time.ns_per_s, .{ .context = &H.canceled, .is_cancelled = H.cancellation });
        defer pass.deinit();
        const api_controls = local.api_mutation_admission_controls;
        H.deadline_ns = if (mode == .deadline_append) platform.time.monotonicNs() +| 5 * std.time.ns_per_s else pass.deadline_ns;
        var budget = api_controls.init(.{ .deadline_ns = H.deadline_ns, .cancellation = local.storage_db_mod.types.CancellationToken.fromAtomic(&H.canceled) }, pass.borrow, std.testing.io);
        var controls = api_controls.acquisition(&budget);
        if (!owned_io) controls.retained_physical_wait = false;
        if (rejected) {
            var coordinator: Coordinator = .{};
            var thread: ?std.Thread = if (paused_io) try std.Thread.spawn(.{}, Coordinator.run, .{&coordinator}) else null;
            defer if (thread) |live| {
                native.release.store(true, .release);
                live.join();
            };
            const before = backend.snapshotWriteStats();
            const result = db.batchWithAdmissionControls(.{ .writes = writes }, controls, .none);
            if (thread) |live| {
                live.join();
                thread = null;
            }
            if (coordinator.failure) |cause| return cause;
            if (uncertain) try std.testing.expectError(error.DurabilityOutcomeUnknown, result) else try std.testing.expectError(error.Canceled, result);
            try std.testing.expectEqual(uncertain, backend.manifest_recovery_required);
            try std.testing.expectEqual(before.wal_append_records, backend.snapshotWriteStats().wal_append_records);
            try std.testing.expect(H.injected.load(.acquire));
            if (paused_io) {
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
            try db.batchWithAdmissionControls(.{ .writes = writes }, controls, .none);
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
        }
        if (!rejected) {
            try std.testing.expect(H.refused.load(.acquire));
            try std.testing.expect(H.retained_peak.load(.acquire) >= writes.len * 32);
            const before = backend.snapshotWriteStats();
            H.canceled.store(true, .release);
            try std.testing.expectError(error.Canceled, db.batchWithAdmissionControls(.{ .writes = &.{.{ .key = "rejected", .value = "{}" }} }, controls, .none));
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
    if (uncertain) @import("antfly_test_error_logs").expectErrorLogs(1);
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
        try std.testing.expect((try reopened.get(a, if (rejected) "doc:0063" else "doc:1023")) == null);
    } else {
        const recovered = (try reopened.get(a, if (rejected) "doc:0063" else "doc:1023")).?;
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
