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

test "guarded managed DB commit retires complete staging and prior roots while foreground owns the only ordinary CPU" {
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
    const backend = db.core.primary_store_owner.lsm.handle.backend;
    try drain(backend);
    const H = struct {
        var execution: *owners.State = undefined;
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
        fn prepare(_: *foreground.Owner, _: admission.Port) !void {
            _ = foreground_calls.fetchAdd(1, .monotonic);
        }
        fn lifetime(raw: *const anyopaque) !void {
            const port: *const admission.Port = @ptrCast(@alignCast(raw));
            try port.checkLifetime();
        }
    };
    H.execution = &state;
    H.manager = &resources.resource_manager;
    H.retained_peak.store(0, .release);
    H.refused.store(false, .release);
    H.steps.store(0, .release);
    H.foreground_calls.store(0, .release);
    H.canceled.store(false, .release);
    local.storage_lsm_backend_runtime.test_write_retirement_hook = H.retire;
    foreground.test_hook = H.prepare;
    defer {
        if (open) {
            db.close();
            open = false;
        }
        local.storage_lsm_backend_runtime.test_write_retirement_hook = null;
        foreground.test_hook = null;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const writes = try arena.allocator().alloc(local.storage_db_mod.types.BatchWrite, 1024);
    for (writes, 0..) |*write, i| write.* = .{ .key = try std.fmt.allocPrint(arena.allocator(), "doc:{d:0>4}", .{i}), .value = "{\"value\":1}" };
    {
        var pass = try local.common_background_execution.Pass.init(state.port(), &std.testing.io, platform.time.monotonicNs() +| 30 * std.time.ns_per_s, .{ .context = &H.canceled, .is_cancelled = H.cancellation });
        defer pass.deinit();
        var scope = try pass.admissionScope(std.testing.io);
        const payload = scope.payload();
        const original = admission.Port.init(&payload);
        const controls: local.storage_db_apply_rw_lock.ApplyRwLock.Acquisition = .{ .lifetime = .{ .context = &original, .check_fn = H.lifetime }, .execution_wait = payload.execution_wait };
        for (0..2) |round| {
            const before = H.steps.load(.acquire);
            if (round == 1) for (writes) |*write| {
                write.value = "{\"value\":2}";
            };
            try db.batchWithAdmissionControls(.{ .writes = writes }, controls, .none);
            // The public DB path retains apply/snapshot fences. Physical
            // cleanup must finish using its dedicated CPU while this pass
            // still owns the sole ordinary slot, including overwritten roots.
            try drain(backend);
            try std.testing.expect(H.steps.load(.acquire) - before >= writes.len);
            try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 0), H.foreground_calls.load(.acquire));
        }
        try std.testing.expect(H.refused.load(.acquire));
        try std.testing.expect(H.retained_peak.load(.acquire) >= writes.len * 32);
        const before = backend.snapshotWriteStats();
        H.canceled.store(true, .release);
        try std.testing.expectError(error.Canceled, db.batchWithAdmissionControls(.{ .writes = &.{.{ .key = "rejected", .value = "{}" }} }, controls, .none));
        try std.testing.expectEqual(before.wal_append_records, backend.snapshotWriteStats().wal_append_records);
    }
    try std.testing.expect(db.core.tryLockApplyExclusive());
    db.core.unlockApply();
    var capture = db.core.snapshot_admission.tryAcquireCapture() orelse return error.LeakedMutationAdmission;
    capture.release();
    const value = (try db.get(a, "doc:0000")).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("{\"value\":2}", value);
    try std.testing.expect((try db.get(a, "rejected")) == null);
    db.close();
    open = false;
    try std.testing.expectEqual(@as(u64, 0), resources.resource_manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
    var reopened = try DB.open(a, directory.path(), options);
    defer reopened.close();
    const recovered = (try reopened.get(a, "doc:1023")).?;
    defer a.free(recovered);
    try std.testing.expectEqualStrings("{\"value\":2}", recovered);
}
