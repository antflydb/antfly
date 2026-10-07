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

//! Runtime document-store adapter over native `.aflite` document pages.

const native_platform = @import("antfly_platform");
const std = @import("std");

const antfly_platform = @import("antfly_platform");
const platform_sync = antfly_platform.sync;
const backend_adapter = @import("../backend_adapter.zig");
const backend_erased = @import("../backend_erased.zig");
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("../db/derived/change_journal.zig");
const internal_keys = @import("../internal_keys.zig");
const native = @import("native.zig");
const resource_manager_mod = @import("../resource_manager.zig");
pub const reclamation = @import("reclamation.zig");
const maintenance = @import("../maintenance.zig");

const Allocator = std.mem.Allocator;
const bounded_cursor_test_documents: usize = 512;

test "lite reclamation stale quota estimates expire after roots shrink" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-stale-quota.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .page_reuse = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 128 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    for (0..64) |_| try store.file.putDocument("large", value);
    try store.file.putDocument("keep", value);
    const physical = (try store.file.file.stat(native_platform.testing.io)).size;
    const old_compact = (try store.file.liveStats(null)).compact_size;
    const cap = physical + old_compact * 2 - 64 * 1024;
    store.maintenance_policy = try reclamation.Policy.init(.{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 128 * 1024, .max_storage_bytes = cap });
    try store.maintainOnce(false);
    try std.testing.expectEqual(reclamation.Reason.storage_budget, store.maintenance_policy.status.reason);
    var write = try store.beginWrite();
    try write.delete("large");
    try write.commit();
    const after = (try store.file.file.stat(native_platform.testing.io)).size;
    const compact = (try store.file.liveStats(null)).compact_size;
    try std.testing.expect(after + compact * 2 < cap);
    try std.testing.expect(store.maintenance_policy.worthwhile(after, compact));
    for (0..5) |_| try store.maintainOnce(true);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    try std.testing.expectEqual(@as(u64, 0), store.maintenance_policy.status.rewrite_count);
    try std.testing.expectEqual(reclamation.Reason.storage_budget, store.maintenance_policy.status.reason);
    // Advance the private monotonic deadline rather than sleeping in a test.
    store.assessment_cache.?.revalidate_at = .zero;
    try store.maintainOnce(true);
    try std.testing.expectEqual(@as(u64, 2), store.maintenance_policy.status.assessment_count);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.rewrite_count);
    try std.testing.expect(store.assessment_cache == null);
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get("large"));
    try std.testing.expectEqualSlices(u8, value, try read.get("keep"));
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation writer busy retries gate image copies until publication is available" {
    const Hook = struct {
        var owner: ?*Store = null;
        var writes: u64 = 0;
        fn write(userdata: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
            const n = try native_platform.testing.io.vtable.fileWritePositional(userdata, file, header, data, splat, offset);
            if (owner) |store| if (file.handle != store.file.file.handle) {
                writes += 1;
            };
            return n;
        }
    };
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-busy-copy.aflite");
    defer a.free(path);
    var vtable = native_platform.testing.io.vtable.*;
    vtable.fileWritePositional = Hook.write;
    const io = std.Io{ .userdata = native_platform.testing.io.userdata, .vtable = &vtable };
    var store = try Store.createWithOptions(a, path, .{ .io = io, .no_sync = true, .reclamation = .{ .enabled = false, .page_reuse = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    for (0..64) |_| try store.file.putDocument("doc", "value");
    store.maintenance_policy = try reclamation.Policy.init(.{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 128 * 1024 });
    var pending = try store.beginWrite();
    var pending_open = true;
    defer if (pending_open) pending.abort();
    Hook.owner = &store;
    defer Hook.owner = null;
    const checkpoint = store.file.activeCheckpoint();
    for (0..4) |_| {
        const before = Hook.writes;
        const retry = store.maintenance_policy.status.state == .deferred;
        store.maintainOnceWithCancel(retry, &store.maintenance_cancel) catch |err| {
            try std.testing.expectEqual(error.FileBusy, err);
            store.recordMaintenanceError(err);
        };
        if (before == 0) try std.testing.expect(Hook.writes > before + 2) else try std.testing.expectEqual(before, Hook.writes);
        try std.testing.expectEqualDeep(checkpoint, store.file.activeCheckpoint());
        try std.testing.expectEqual(reclamation.Reason.writer_busy, store.maintenance_policy.status.reason);
    }
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    const before = Hook.writes;
    pending.abort();
    pending_open = false;
    // A queued yielding writer also owns the next publication opportunity.
    store.next_writer_ticket +%= 1;
    try store.maintainOnce(true);
    try std.testing.expectEqual(before, Hook.writes);
    try std.testing.expectEqual(reclamation.Reason.writer_busy, store.maintenance_policy.status.reason);
    store.serving_writer_ticket +%= 1;
    try store.maintainOnce(true);
    try std.testing.expect(Hook.writes > before);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.rewrite_count);
    try std.testing.expect(!store.shrink_waiting_for_writer);
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("value", try read.get("doc"));
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation prepared admission cancels cold reserve replay before publication" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "admission-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "admission-prepared.aflite");
    defer a.free(staged_path);
    const options: CreateOptions = .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } };
    var live = try Store.createWithOptions(a, path, options);
    defer live.close();
    live.maintenance_start_suppressed = true;
    var staged = try Store.createWithOptions(a, staged_path, options);
    defer staged.close();
    staged.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    var write = try staged.beginWrite();
    try write.put("large", value);
    try write.commit();
    try staged.file.beginTransaction();
    staged.file.abortTransaction();
    const live_checkpoint = live.file.activeCheckpoint();
    const staged_checkpoint = staged.file.activeCheckpoint();
    const reads = staged.file.test_page_reads.load(.monotonic);
    var cancel = maintenance.CancelToken{};
    staged.file.test_cancel_on_read = &cancel;
    defer staged.file.test_cancel_on_read = null;
    const old_bytes = (try live.file.file.stat(native_platform.testing.io)).size;
    try std.testing.expectError(error.MaintenanceCanceled, live.admitPreparedGenerationAssumeLockedWithCancel(&staged.file, old_bytes, &cancel));
    try std.testing.expect(staged.file.test_page_reads.load(.monotonic) - reads <= 2);
    try std.testing.expectEqualDeep(live_checkpoint, live.file.activeCheckpoint());
    try std.testing.expectEqualDeep(staged_checkpoint, staged.file.activeCheckpoint());
    try std.testing.expect(staged.file.ledger == null);
    staged.file.test_cancel_on_read = null;
    cancel.requested.store(false, .release);
    try live.admitPreparedGenerationAssumeLockedWithCancel(&staged.file, old_bytes, &cancel);
    try std.testing.expectEqualDeep(live_checkpoint, live.file.activeCheckpoint());
    try std.testing.expectEqualDeep(staged_checkpoint, staged.file.activeCheckpoint());
}

test "lite reclamation retries reuse estimates and reassess changed roots when admitted" {
    const Probe = struct {
        bytes: u64 = 0,
        fn available(ptr: *anyopaque, _: []const u8) !u64 {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).bytes;
        }
    };
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-retry-scan.aflite");
    defer a.free(path);
    var probe = Probe{};
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    for (0..64) |_| try store.file.putDocument("doc", "value");
    store.maintenance_policy = try reclamation.Policy.init(.{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 32 * 4096, .disk_headroom_bytes = 4096, .capacity_probe = .{ .context = &probe, .available = Probe.available } });
    try store.maintainOnce(false);
    const checkpoint = store.file.activeCheckpoint();
    for (0..5) |_| {
        try std.testing.expectEqual(reclamation.State.deferred, store.maintenance_policy.status.state);
        const retry = store.maintenance_policy.status.state == .deferred or store.maintenance_policy.status.state == .failed;
        try store.maintainOnceWithCancel(retry, &store.maintenance_cancel);
        try std.testing.expectEqualDeep(checkpoint, store.file.activeCheckpoint());
    }
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    store.assessment_cache.?.revalidate_at = .zero;
    try store.maintainOnce(true);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    const baseline = store.maintenance_policy.assessed_retirement_bytes;
    store.maintenance_policy.options.max_storage_bytes = (try store.reclamationStatus()).totalBytes() + 4096;
    for (0..5) |_| try store.maintainOnce(true);
    try std.testing.expectEqual(reclamation.Reason.storage_budget, store.maintenance_policy.status.reason);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    try std.testing.expectEqual(baseline, store.maintenance_policy.assessed_retirement_bytes);
    store.maintenance_policy.options.max_storage_bytes = 0;
    store.assessment_cache.?.revalidate_at = std.Io.Clock.awake.now(native_platform.testing.io).addDuration(std.Io.Duration.fromSeconds(30));
    // Changed checkpoints must not force repeated scans while resources remain
    // blocked and proportional activity has not exhausted the scan budget.
    for (0..5) |_| {
        try store.file.putDocument("doc", "changed");
        try store.maintainOnce(true);
    }
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    try std.testing.expect(!std.meta.eql(checkpoint, store.file.activeCheckpoint()));
    store.assessment_cache.?.revalidate_at = .zero;
    try store.maintainOnce(true);
    try std.testing.expectEqual(@as(u64, 2), store.maintenance_policy.status.assessment_count);
    try std.testing.expectEqual(reclamation.Reason.low_disk, store.maintenance_policy.status.reason);
    try std.testing.expect(store.assessment_cache.?.revalidate_at.nanoseconds >= std.Io.Clock.awake.now(native_platform.testing.io).addDuration(std.Io.Duration.fromSeconds(29)).nanoseconds);
    for (0..5) |_| {
        try store.file.putDocument("doc", "changed");
        try store.maintainOnce(true);
    }
    try std.testing.expectEqual(@as(u64, 2), store.maintenance_policy.status.assessment_count);
    probe.bytes = 16 * 1024 * 1024;
    try store.maintainOnce(true);
    try std.testing.expectEqual(@as(u64, 3), store.maintenance_policy.status.assessment_count);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.rewrite_count);
    try std.testing.expect(store.assessment_cache == null);
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("changed", try read.get("doc"));
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation worker cancels post assessment status replay after foreground abort" {
    const Hook = struct {
        var owner: ?*Store = null;
        var owner_reads: u64 = 0;
        var checkpoint: native.CheckpointSlot = undefined;
        pub fn read(userdata: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
            const n = try native_platform.testing.io.vtable.fileReadPositional(userdata, file, data, offset);
            if (owner) |store| {
                if (file.handle == store.file.file.handle and store.maintenance_cancel.requested.load(.acquire)) owner_reads += 1;
                if (file.handle != store.file.file.handle and store.maintenance_running and offset >= 4096 and store.file.test_cancel_on_read == null) {
                    lockStore(store);
                    defer store.mutex.unlock();
                    store.file.beginTransaction() catch unreachable;
                    store.file.abortTransaction();
                    checkpoint = store.file.activeCheckpoint();
                    store.file.page_cache_enabled.store(false, .monotonic);
                    store.file.test_cancel_on_read = &store.maintenance_cancel;
                }
            }
            return n;
        }
        fn available(_: *anyopaque, _: []const u8) !u64 {
            return 0;
        }
    };
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-status-cancel.aflite");
    defer a.free(path);
    var vtable = native_platform.testing.io.vtable.*;
    vtable.fileReadPositional = Hook.read;
    const io = std.Io{ .userdata = native_platform.testing.io.userdata, .vtable = &vtable };
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = io, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 256 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    for (0..20) |_| {
        var write = try store.beginWrite();
        try write.put("large", value);
        try write.commit();
    }
    var dummy: u8 = 0;
    store.maintenance_policy = try reclamation.Policy.init(.{ .minimum_reclaim_bytes = 4096, .assessment_bytes = 4096, .retirement_work_pages = 1, .capacity_probe = .{ .context = &dummy, .available = Hook.available } });
    Hook.owner = &store;
    defer Hook.owner = null;
    defer store.file.test_cancel_on_read = null;
    try std.testing.expectError(error.MaintenanceCanceled, store.maintainOnceWithCancel(false, &store.maintenance_cancel));
    try std.testing.expect(store.maintenance_cancel.requested.load(.acquire));
    try std.testing.expect(Hook.owner_reads <= 2);
    try std.testing.expectEqualDeep(Hook.checkpoint, store.file.activeCheckpoint());
    try std.testing.expect(store.file.ledger == null);
    try std.testing.expect(store.file.allocator_cancel_token == null);
    Hook.owner = null;
    store.file.test_cancel_on_read = null;
    // The worker token stays requested; independent foreground mutations must
    // still publish normally, without inheriting maintenance cancellation.
    var write = try store.beginWrite();
    try write.put("foreground", "accepted");
    try write.commit();
    store.maintenance_cancel.requested.store(false, .release);
    try store.maintainOnce(true);
    try std.testing.expectEqual(reclamation.Reason.low_disk, store.maintenance_policy.status.reason);
}

test "lite reclamation worker cancels cold debt replay after vacuum adoption" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "worker-cold-debt.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    var write = try store.beginWrite();
    try write.put("large", value);
    try write.commit();
    _ = try store.vacuumWithCancel(null);
    try std.testing.expect(store.file.ledger == null);
    const checkpoint = store.file.activeCheckpoint();
    const reads = store.file.test_page_reads.load(.monotonic);
    store.file.test_cancel_on_read = &store.maintenance_cancel;
    defer store.file.test_cancel_on_read = null;
    try std.testing.expectError(error.MaintenanceCanceled, store.maintenanceNeedsService());
    try std.testing.expect(store.file.test_page_reads.load(.monotonic) - reads <= 2);
    try std.testing.expectEqualDeep(checkpoint, store.file.activeCheckpoint());
    try std.testing.expect(store.file.ledger == null);
    // Cancellation already pending on mutex acquisition must do no I/O.
    const canceled_reads = store.file.test_page_reads.load(.monotonic);
    try std.testing.expectError(error.MaintenanceCanceled, store.maintenanceNeedsService());
    try std.testing.expectEqual(canceled_reads, store.file.test_page_reads.load(.monotonic));
    store.file.test_cancel_on_read = null;
    store.maintenance_cancel.requested.store(false, .release);
    _ = try store.maintenanceNeedsService();
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualSlices(u8, value, try read.get("large"));
}

test "lite reclamation vacuum preserves collector capacity with retained readers" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "vacuum-collector-capacity.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 4096 } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const large = try a.alloc(u8, 400 * 1024);
    defer a.free(large);
    @memset(large, 'v');
    try store.putCatalogRecord("small", large);
    try store.putCatalogRecord("small", "kept");
    for (0..100) |_| {
        if (!try store.file.retirementNeedsService()) break;
        try store.maintainOnce(false);
    }
    var pinned = try store.beginRead();
    var pin_active = true;
    defer if (pin_active) pinned.abort();
    const before = store.file.activeCheckpoint();
    const before_handle = store.file.file.handle;
    const size = (try store.file.file.stat(native_platform.testing.io)).size;
    const compact = (try store.file.liveStats(null)).compact_size;
    store.maintenance_policy.options.max_storage_bytes = size + compact * 2;
    const initial = try store.reclamationStatus();
    try std.testing.expect(compact * 2 + initial.reusable_pages * 4096 >= try store.file.retirementReserveBytes());
    try std.testing.expectError(error.LiteStorageBudgetExceeded, store.vacuum());
    try std.testing.expectEqualDeep(before, store.file.activeCheckpoint());
    try std.testing.expectEqual(before_handle, store.file.file.handle);
    try store.putCatalogRecord("next", "value");
    pinned.abort();
    pin_active = false;
    _ = try store.vacuum();
    try store.putCatalogRecord("after", "still writable");
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation vacuum rechecks disk headroom before adoption" {
    const Probe = struct {
        calls: usize = 0,
        drop: bool = false,
        fn available(context: *anyopaque, _: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.calls += 1;
            return if (self.drop and self.calls >= 2) 4095 else 1024 * 1024 * 1024;
        }
    };
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "vacuum-publication-disk.aflite");
    defer a.free(path);
    var probe = Probe{};
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .disk_headroom_bytes = 4096, .capacity_probe = .{ .context = &probe, .available = Probe.available } } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    try store.putCatalogRecord("key", "value");
    const before = store.file.activeCheckpoint();
    const handle = store.file.file.handle;
    probe.calls = 0;
    probe.drop = true;
    try std.testing.expectError(error.LiteInsufficientDiskSpace, store.vacuum());
    try std.testing.expectEqualDeep(before, store.file.activeCheckpoint());
    try std.testing.expectEqual(handle, store.file.file.handle);
    probe.drop = false;
    _ = try store.vacuum();
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation restore adoption rejects oversized generations" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "restore-budget-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "restore-budget-staged.aflite");
    defer a.free(staged_path);
    var live = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .max_storage_bytes = 128 * 4096 } });
    defer live.close();
    live.maintenance_start_suppressed = true;
    var staged = try Store.createWithOptions(a, staged_path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer staged.close();
    staged.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 600 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    try staged.putCatalogRecord("large", value);
    const before = live.file.activeCheckpoint();
    try std.testing.expectError(error.LiteStorageBudgetExceeded, live.replaceWithPreparedGeneration(&staged));
    try std.testing.expectEqualDeep(before, live.file.activeCheckpoint());
    try std.testing.expect((try live.getCatalogRecordAlloc(a, "large")) == null);
}

test "lite reclamation restore workspace bounds staging and releases admission" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "workspace-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "workspace-staged.aflite");
    defer a.free(staged_path);
    const limit = 256 * 4096;
    var live = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .max_storage_bytes = limit } });
    defer live.close();
    live.maintenance_start_suppressed = true;
    var workspace = try live.reserveGenerationWorkspace();
    var reserved = true;
    defer if (reserved) workspace.deinit();
    try std.testing.expectEqual(limit, (try live.reclamationStatus()).totalBytes());
    try std.testing.expectError(error.FileBusy, live.reserveGenerationWorkspace());
    try std.testing.expectError(error.FileBusy, live.vacuum());
    var staged = try Store.createWithOptions(a, staged_path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = workspace.options });
    defer staged.close();
    staged.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, limit);
    defer a.free(value);
    @memset(value, 'v');
    const before = staged.file.activeCheckpoint();
    const before_size = (try staged.file.file.stat(native_platform.testing.io)).size;
    try std.testing.expectError(error.LiteStorageBudgetExceeded, staged.putCatalogRecord("oversized", value));
    try std.testing.expectEqualDeep(before, staged.file.activeCheckpoint());
    try std.testing.expectEqual(before_size, (try staged.file.file.stat(native_platform.testing.io)).size);
    try staged.putCatalogRecord("small", "accepted");
    var pinned = try live.beginRead();
    defer pinned.abort();
    _ = try live.replaceWithPreparedGeneration(&staged);
    workspace.deinit();
    reserved = false;
    const status = try live.reclamationStatus();
    try std.testing.expectEqual(@as(u64, 0), status.temporary_bytes);
    try std.testing.expectEqual(before_size, status.retired_file_bytes);
    try std.testing.expect(status.totalBytes() <= limit);
    const actual = (try live.getCatalogRecordAlloc(a, "small")).?;
    defer a.free(actual);
    try std.testing.expectEqualStrings("accepted", actual);
}

test "lite reclamation restore rechecks disk capacity at publication" {
    const Probe = struct {
        fn available(context: *anyopaque, _: []const u8) !u64 {
            const bytes: *u64 = @ptrCast(@alignCast(context));
            return bytes.*;
        }
    };
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "restore-disk-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "restore-disk-staged.aflite");
    defer a.free(staged_path);
    var available: u64 = 1024 * 1024;
    var live = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .disk_headroom_bytes = 4096, .capacity_probe = .{ .context = &available, .available = Probe.available } } });
    defer live.close();
    live.maintenance_start_suppressed = true;
    var workspace = try live.reserveGenerationWorkspace();
    defer workspace.deinit();
    var staged = try Store.createWithOptions(a, staged_path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = workspace.options });
    defer staged.close();
    staged.maintenance_start_suppressed = true;
    try staged.putCatalogRecord("small", "value");
    available = 4095;
    const before = live.file.activeCheckpoint();
    try std.testing.expectError(error.LiteInsufficientDiskSpace, live.replaceWithPreparedGeneration(&staged));
    try std.testing.expectEqualDeep(before, live.file.activeCheckpoint());
}

test "lite reclamation shrinking observes deletions without growth" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "shrink-activity.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .minimum_reclaim_bytes = 16 * 1024, .assessment_bytes = 64 * 1024 * 1024 } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 128 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    var write = try store.beginWrite();
    try write.put("large", value);
    try write.commit();
    try store.maintainOnce(false);
    try std.testing.expectEqual(@as(u64, 1), store.maintenance_policy.status.assessment_count);
    write = try store.beginWrite();
    try write.delete("large");
    try write.commit();
    for (0..32) |_| try store.maintainOnce(false);
    const status = try store.reclamationStatus();
    try std.testing.expectEqual(@as(u64, 1), status.rewrite_count);
    try std.testing.expect(status.current_file_bytes < 64 * 1024);
    const assessments = status.assessment_count;
    for (0..32) |_| try store.maintainOnce(false);
    // Collector metadata alone must not trigger repeated full assessments.
    try std.testing.expectEqual(assessments, (try store.reclamationStatus()).assessment_count);
}

test "lite reclamation shrinking observes partially live packed records" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "shrink-packed.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .minimum_reclaim_bytes = 32 * 1024, .assessment_bytes = 64 * 1024 * 1024 } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const value: [1536]u8 = @splat('v');
    var write = try store.beginWrite();
    for (0..128) |i| {
        var key: [16]u8 = undefined;
        try write.put(try std.fmt.bufPrint(&key, "key-{d:0>4}", .{i}), &value);
    }
    try write.commit();
    try store.maintainOnce(false);
    write = try store.beginWrite();
    // Two adjacent records share each bundle. Leave one record alive in
    // every bundle: most dead inline bytes do not release a physical page.
    for (0..64) |i| {
        var key: [16]u8 = undefined;
        try write.delete(try std.fmt.bufPrint(&key, "key-{d:0>4}", .{i * 2}));
    }
    try write.commit();
    const before = (try store.reclamationStatus()).current_file_bytes;
    for (0..32) |_| try store.maintainOnce(false);
    const status = try store.reclamationStatus();
    try std.testing.expectEqual(@as(u64, 1), status.rewrite_count);
    try std.testing.expect(status.current_file_bytes < before);
    var read = try store.beginRead();
    defer read.abort();
    for (0..64) |i| {
        var key: [16]u8 = undefined;
        try std.testing.expectEqualSlices(u8, &value, try read.get(try std.fmt.bufPrint(&key, "key-{d:0>4}", .{i * 2 + 1})));
    }
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation capacity reserve sustains bounded overwrites" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "retirement-capacity.aflite");
    defer a.free(path);
    const limit = 128 * 4096;
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .enabled = false, .max_storage_bytes = limit } });
    defer store.close();
    // Exercise cooperative service deterministically, including pressure that
    // arrives before the normal sparse-journal checkpoint threshold.
    store.maintenance_cancel.request();
    var value: [8192]u8 = @splat('v');
    const key: [700]u8 = @splat('k');
    for (0..128) |version| {
        value[0] = @intCast(version);
        var accepted = false;
        for (0..256) |_| {
            var write = try store.beginWrite();
            var write_active = true;
            errdefer if (write_active) write.abort();
            try write.put(&key, &value);
            if (write.commit()) |_| {
                write_active = false;
                accepted = true;
                break;
            } else |err| {
                write.abort();
                write_active = false;
                if (err != error.LiteStorageBudgetExceeded) return err;
            }
            try std.testing.expect(try store.file.retirementNeedsService());
            try store.maintainOnce(false);
        }
        try std.testing.expect(accepted);
        try std.testing.expect((try store.file.file.stat(native_platform.testing.io)).size <= limit);
    }
    try std.testing.expect((try store.file.allocatorStats()).?.reused_pages > 0);
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqual(@as(u8, 127), (try read.get(&key))[0]);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation creation checks budget before replacement" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "creation-capacity.aflite");
    defer a.free(path);
    {
        var file = try native.NativeFile.createWithIo(a, native_platform.testing.io, path, .{});
        defer file.close();
        try file.putCatalogRecord("stable", "original");
    }
    try std.testing.expectError(error.LiteStorageBudgetExceeded, Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .max_storage_bytes = 4096 } }));
    var file = try native.NativeFile.openWithIo(a, native_platform.testing.io, path, .{ .read_only = true });
    defer file.close();
    const value = (try file.getCatalogRecordAlloc(a, "stable")).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("original", value);
}

test "lite reclamation capacity reserve drains wide value graphs" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "wide-retirement-capacity.aflite");
    defer a.free(path);
    const limit = 256 * 4096;
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .enabled = false, .max_storage_bytes = limit } });
    defer store.close();
    store.maintenance_cancel.request();
    const value = try a.alloc(u8, 256 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    for (0..12) |version| {
        value[0] = @intCast(version);
        var accepted = false;
        for (0..256) |_| {
            if (store.putCatalogRecord("meta", value)) |_| {
                accepted = true;
                break;
            } else |err| {
                if (err != error.LiteStorageBudgetExceeded) return err;
            }
            try std.testing.expect(try store.file.retirementNeedsService());
            try store.maintainOnce(false);
        }
        try std.testing.expect(accepted);
        try std.testing.expect((try store.file.file.stat(native_platform.testing.io)).size <= limit);
    }
    const stored = (try store.getCatalogRecordAlloc(a, "meta")).?;
    defer a.free(stored);
    try std.testing.expectEqualSlices(u8, value, stored);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation owner vacuum reports final publication size" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "vacuum-final-size.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_cancel.request();
    for (0..32) |_| try store.putCatalogRecord("meta", "value");
    const report = try store.vacuum();
    const size = (try store.file.file.stat(native_platform.testing.io)).size;
    try std.testing.expectEqual(size, report.after_size);
    try std.testing.expectEqual(report.before_size -| size, report.reclaimed_bytes);
    try std.testing.expectEqual(report.reclaimed_bytes, (try store.reclamationStatus()).last_reclaimed_bytes);
}

test "lite reclamation creation checks disk before replacing an artifact" {
    const Probe = struct {
        fn available(_: *anyopaque, _: []const u8) !u64 {
            return native.NativeFile.minimumOwnerStorageBytes(true) - 1;
        }
    };
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "creation-disk.aflite");
    defer a.free(path);
    var context: u8 = 0;
    try std.testing.expectError(error.LiteInsufficientDiskSpace, Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .reclamation = .{ .disk_headroom_bytes = 0, .capacity_probe = .{ .context = &context, .available = Probe.available } } }));
    try std.testing.expectError(error.FileNotFound, native.NativeFile.openWithIo(a, native_platform.testing.io, path, .{}));
}

test "lite reclamation counts retired inodes until pinned readers release" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-retained.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .enabled = false } });
    defer store.close();
    var write = try store.beginWrite();
    try write.put("doc", "old");
    try write.commit();
    var old = try store.beginRead();
    var old_open = true;
    defer if (old_open) old.abort();
    write = try store.beginWrite();
    try write.put("doc", "new");
    try write.commit();
    const before = (try store.reclamationStatus()).current_file_bytes;
    _ = try store.vacuum();
    var status = try store.reclamationStatus();
    try std.testing.expectEqual(before, status.retired_file_bytes);
    try std.testing.expectEqual(@as(u64, 1), status.retired_generations);
    try std.testing.expectEqualStrings("old", try old.get("doc"));
    // The old inode consumes the aggregate budget even though the visible
    // pathname is compact. Releasing its reader restores mutation capacity.
    store.maintenance_policy.options.max_storage_bytes = status.totalBytes() + (try store.file.retirementReserveBytes()) + 4096;
    const large: [4 * 4096]u8 = @splat('x');
    write = try store.beginWrite();
    try write.put("capacity", &large);
    try std.testing.expectError(error.LiteStorageBudgetExceeded, write.commit());
    write.abort();
    var latest = try store.beginRead();
    defer latest.abort();
    try std.testing.expectEqualStrings("new", try latest.get("doc"));
    old.abort();
    old_open = false;
    status = try store.reclamationStatus();
    try std.testing.expectEqual(@as(u64, 0), status.retired_file_bytes);
    try std.testing.expectEqual(@as(u64, 0), status.retired_generations);
    try std.testing.expectEqual(@as(u64, 1), status.retained_readers);
    write = try store.beginWrite();
    try write.put("capacity", &large);
    try write.commit();
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation writable reopen services debt and read only reopen does not" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-reopen.aflite");
    defer alloc.free(path);
    const options: reclamation.Options = .{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 32 * 4096 };
    var peak: u64 = 0;
    for (0..6) |cycle| {
        // Populate without a worker, representing a legacy/maintenance-disabled client.
        {
            var store = Store.openWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } }) catch |err| switch (err) {
                error.FileNotFound => try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } }),
                else => return err,
            };
            defer store.close();
            for (0..64) |version| {
                var value: [32]u8 = undefined;
                const bytes = try std.fmt.bufPrint(&value, "{d}/{d}", .{ cycle, version });
                var write = try store.beginWrite();
                errdefer write.abort();
                try write.put("doc", bytes);
                try write.commit();
            }
            peak = @max(peak, (try store.reclamationStatus()).current_file_bytes);
        }
        {
            var reader = try Store.openWithOptions(alloc, path, .{ .read_only = true, .io = native_platform.testing.io, .reclamation = options });
            defer reader.close();
            try std.testing.expectEqual(@as(u64, 0), (try reader.reclamationStatus()).rewrite_count);
        }
        var owner = try Store.openWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = options });
        defer owner.close();
        const status = try owner.reclamationStatus();
        try std.testing.expectEqual(@as(u64, 1), status.rewrite_count);
        try std.testing.expect(status.current_file_bytes < 32 * 4096);
        var read = try owner.beginRead();
        defer read.abort();
        var value: [32]u8 = undefined;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&value, "{d}/63", .{cycle}), try read.get("doc"));
        try std.testing.expect((try owner.checkWithCancel(null)).valid);
    }
    // The same fixed work each session has a fixed envelope, rather than
    // accumulating six sessions of history.
    try std.testing.expect(peak < 512 * 4096);
}

test "lite reclamation budget rejects a mutation before exhausting capacity" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-budget.aflite");
    defer alloc.free(path);
    const limit = 64 * 4096;
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false, .max_storage_bytes = limit } });
    defer store.close();
    var value: [8192]u8 = @splat('a');
    var accepted: usize = 0;
    for (0..128) |version| {
        value[0] = @intCast(version);
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("doc", &value);
        write.commit() catch |err| {
            try std.testing.expectEqual(error.LiteStorageBudgetExceeded, err);
            write.abort();
            break;
        };
        accepted += 1;
    }
    try std.testing.expect(accepted > 0 and accepted < 128);
    try std.testing.expect((try store.reclamationStatus()).current_file_bytes <= limit);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
    var read = try store.beginRead();
    defer read.abort();
    const stored = try read.get("doc");
    try std.testing.expectEqual(@as(u8, @intCast(accepted - 1)), stored[0]);
}

test "lite reclamation low disk defers rewriting and resumes when capacity returns" {
    const Probe = struct {
        bytes: u64 = 0,
        fn available(ptr: *anyopaque, _: []const u8) !u64 {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).bytes;
        }
    };
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-disk.aflite");
    defer alloc.free(path);
    var probe = Probe{};
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } });
    defer store.close();
    for (0..64) |_| try store.file.putDocument("doc", "value");
    store.maintenance_policy = try reclamation.Policy.init(.{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 32 * 4096, .disk_headroom_bytes = 4096, .capacity_probe = .{ .context = &probe, .available = Probe.available } });
    try store.maintainOnce(false);
    try std.testing.expectEqual(reclamation.Reason.low_disk, (try store.reclamationStatus()).reason);
    try std.testing.expectError(error.LiteInsufficientDiskSpace, store.vacuum());
    probe.bytes = 1024 * 1024;
    try store.maintainOnce(true);
    try std.testing.expectEqual(@as(u64, 1), (try store.reclamationStatus()).rewrite_count);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation automatic worker bounds sustained overwrite history" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-worker.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false, .assessment_bytes = 8 * 4096, .minimum_reclaim_bytes = 32 * 4096, .retry_ms = 1 } });
    defer store.close();
    var peak: u64 = 0;
    for (0..4) |cycle| {
        const before = (try store.reclamationStatus()).rewrite_count;
        for (0..128) |version| {
            var value: [32]u8 = undefined;
            var write = try store.beginWriteYielding();
            errdefer write.abort();
            try write.put("doc", try std.fmt.bufPrint(&value, "{d}/{d}", .{ cycle, version }));
            try write.commit();
            peak = @max(peak, (try store.reclamationStatus()).current_file_bytes);
        }
        // No manual maintenance call: wait only for the owner's automatic task.
        const deadline = std.Io.Clock.awake.now(native_platform.testing.io).addDuration(std.Io.Duration.fromSeconds(5));
        while (true) {
            const status = try store.reclamationStatus();
            // A successful online rewrite can retain bounded catch-up history;
            // the contract is an envelope, not a fully packed file per cycle.
            if (status.rewrite_count > before and status.state == .idle and status.current_file_bytes < 64 * 4096) break;
            if (std.Io.Clock.awake.now(native_platform.testing.io).nanoseconds >= deadline.nanoseconds) {
                std.debug.print("maintenance state={s} reason={s} error={?s} size={d}\n", .{ @tagName(status.state), @tagName(status.reason), status.last_error, status.current_file_bytes });
                return error.TestUnexpectedResult;
            }
            try native_platform.testing.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
        }
        var read = try store.beginRead();
        defer read.abort();
        var value: [32]u8 = undefined;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&value, "{d}/127", .{cycle}), try read.get("doc"));
    }
    try std.testing.expect(peak < 1024 * 4096);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation staged generation joins maintenance before owner adoption" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const live_path = try testPath(alloc, tmp, "reclamation-adopt-live.aflite");
    defer alloc.free(live_path);
    const staged_path = try testPath(alloc, tmp, "reclamation-adopt-stage.aflite");
    defer alloc.free(staged_path);
    var live = try Store.createWithOptions(alloc, live_path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer live.close();
    var staged = try Store.createWithOptions(alloc, staged_path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{
        .assessment_bytes = 4096,
        .minimum_reclaim_bytes = 4 * 4096,
        .retry_ms = 1,
    } });
    defer staged.close();
    var write = try staged.beginWrite();
    try write.put("doc", "staged");
    try write.commit();
    try std.testing.expect(staged.maintenance_future != null);
    _ = try live.replaceWithPreparedGeneration(&staged);
    try std.testing.expect(staged.maintenance_future == null);
    staged.startMaintenance();
    try std.testing.expect(staged.maintenance_future == null);
    var read = try live.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("staged", try read.get("doc"));
    try std.testing.expect((try live.checkWithCancel(null)).valid);
}

test "lite allocator v4 owner pins readers and reuses pages with shrinking disabled" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-owner.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var write = try store.beginWrite();
    try write.put("doc", "old");
    try write.commit();
    var old = try store.beginRead();
    var old_open = true;
    defer if (old_open) old.abort();
    for (0..120) |_| {
        write = try store.beginWriteYielding();
        try write.put("doc", "new");
        try write.commit();
    }
    try std.testing.expectEqualStrings("old", try old.get("doc"));
    var newer = try store.beginRead();
    var newer_open = true;
    defer if (newer_open) newer.abort();
    old.abort();
    old_open = false;
    for (0..200) |_| try store.maintainOnce(false);
    try std.testing.expectEqualStrings("new", try newer.get("doc"));
    try std.testing.expect((try store.reclamationStatus()).retired_objects_serviced > 0);
    newer.abort();
    newer_open = false;
    const plateau = store.file.activeCheckpoint().page_count;
    for (0..120) |_| {
        write = try store.beginWriteYielding();
        try write.put("doc", "latest");
        try write.commit();
    }
    try std.testing.expect(store.file.activeCheckpoint().page_count < plateau + 64);
    const status = try store.reclamationStatus();
    try std.testing.expectEqual(@as(u64, 0), status.rewrite_count);
    try std.testing.expect(status.reused_pages > 0);
    var current = try store.beginRead();
    defer current.abort();
    try std.testing.expectEqualStrings("latest", try current.get("doc"));
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite allocator v4 migrates legacy owners and preserves read only files" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "allocator-migration.aflite");
    defer a.free(path);
    const value: [8192]u8 = @splat('v');
    var key: [1024]u8 = @splat('k');
    key[0] = 'a';
    {
        var legacy = try native.NativeFile.createWithIo(a, native_platform.testing.io, path, .{});
        defer legacy.close();
        try legacy.putDocument(&key, &value);
        try legacy.putCatalogRecord("metadata", &value);
        try legacy.putIndexCatalogRecord("index", &value);
    }
    {
        var reader = try Store.openWithOptions(a, path, .{ .read_only = true, .io = native_platform.testing.io });
        defer reader.close();
        try std.testing.expect(!reader.file.header.indexed_reclamation);
    }
    var owner = try Store.openWithOptions(a, path, .{ .io = native_platform.testing.io, .reclamation = .{ .enabled = false } });
    defer owner.close();
    try std.testing.expect(owner.file.header.indexed_reclamation);
    var txn = try owner.beginRead();
    defer txn.abort();
    try std.testing.expectEqualSlices(u8, &value, try txn.get(&key));
    try std.testing.expect((try owner.checkWithCancel(null)).valid);
}

test "lite allocator v4 releasing the last old reader starts idle retirement" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-idle-retirement.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var oldest = try store.beginRead();
    var oldest_open = true;
    defer if (oldest_open) oldest.abort();
    const value = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    var write = try store.beginWrite();
    try write.put("large", value);
    try write.commit();
    write = try store.beginWrite();
    try write.delete("large");
    try write.commit();
    try std.testing.expect(store.maintenance_future == null);
    oldest.abort();
    oldest_open = false;
    const deadline = std.Io.Clock.awake.now(native_platform.testing.io).addDuration(std.Io.Duration.fromSeconds(10));
    while (true) {
        const status = try store.reclamationStatus();
        if (status.pending_data_retirement_objects == 0 and status.retired_objects_serviced > 1000) {
            try std.testing.expectEqual(@as(u64, 0), status.rewrite_count);
            break;
        }
        if (std.Io.Clock.awake.now(native_platform.testing.io).nanoseconds >= deadline.nanoseconds) return error.TestUnexpectedResult;
        try native_platform.testing.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

pub const OpenOptions = struct {
    reclamation: reclamation.Options = .{},
    read_only: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    io: ?std.Io = null,
};

pub const CreateOptions = struct {
    reclamation: reclamation.Options = .{},
    exclusive: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    writer_lock_marker: []const u8 = "",
    io: ?std.Io = null,
};

const MutationRequest = struct {
    context: *anyopaque,
    apply: *const fn (*anyopaque, *native.NativeFile) anyerror!void,
    next: ?*MutationRequest = null,
    done: bool = false,
    leader: bool = false,
    durable: bool = true,
    result: anyerror!void = {},
};

const ReadPin = struct {
    sequence: u64,
    pinned_at: std.Io.Timestamp,
    previous: ?*ReadPin = null,
    next: ?*ReadPin = null,
};

const ReadGeneration = struct {
    first_pin: ?*ReadPin = null,
    last_pin: ?*ReadPin = null,
    file: native.NativeFile,
    references: usize = 0,
    retired: bool = false,
    next_retired: ?*ReadGeneration = null,
    retained_bytes: u64 = 0,
    pinned_at: std.Io.Timestamp = .zero,
};

pub const Store = struct {
    allocator: Allocator,
    file: native.NativeFile,
    read_generation: ?*ReadGeneration = null,
    retired_generations: ?*ReadGeneration = null,
    /// Cached under mutex so foreground admission is independent of the number
    /// of retained generations. Status may walk them for reader diagnostics.
    retired_file_bytes: u64 = 0,
    maintenance_policy: reclamation.Policy = .{},
    // Valid only for this inode, exact checkpoint, and destination format.
    // Resource retries do not consume or reset the proportional scan budget.
    assessment_cache: ?struct {
        generation: u64,
        checkpoint: native.CheckpointSlot,
        indexed: bool,
        stats: native.NativeFile.LiveStats,
        revalidate_at: std.Io.Timestamp,
    } = null,
    assessment_generation: u64 = 0,
    // Independent of retirement status, which can return to idle while a
    // prepared generation is still waiting for a publication opportunity.
    shrink_waiting_for_writer: bool = false,
    maintenance_future: ?std.Io.Future(void) = null,
    maintenance_started: std.atomic.Value(bool) = .init(false),
    maintenance_wake: std.Io.Event = .unset,
    maintenance_cancel: maintenance.CancelToken = .{},
    maintenance_running: bool = false,
    generation_workspace_active: bool = false,
    maintenance_start_suppressed: bool = false,
    test_maintenance_cycles: if (@import("builtin").is_test) std.atomic.Value(u64) else void = if (@import("builtin").is_test) .init(0) else {},
    admission_refresh_size: u64 = 0,
    disk_admission_limit: ?u64 = null,
    read_only: bool = false,
    /// Guarded by mutex. Secret publication failures fence every adapter until reopen.
    secret_store_uncertain: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    writer_mutex: std.Io.Mutex = .init,
    writer_ready: std.Io.Condition = .init,
    generation_lock: std.Io.RwLock = .init,
    commit_mutex: std.Io.Mutex = .init,
    commit_ready: std.Io.Condition = .init,
    commit_head: ?*MutationRequest = null,
    commit_tail: ?*MutationRequest = null,
    commit_draining: bool = false,
    writer_active: bool = false,
    writer_ticketed: bool = false,
    next_writer_ticket: u64 = 0,
    serving_writer_ticket: u64 = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !Store {
        return try openWithOptions(allocator, path, .{ .read_only = read_only });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, opts: OpenOptions) !Store {
        const policy = try reclamation.Policy.init(opts.reclamation);
        const native_opts = native.OpenOptions{
            .read_only = opts.read_only,
            .wait_for_reader_lock = opts.read_only,
            .no_sync = opts.no_sync,
            .resource_manager = opts.resource_manager,
        };
        const file = if (opts.io) |io|
            try native.NativeFile.openWithIo(allocator, io, path, native_opts)
        else
            try native.NativeFile.openWithOptions(allocator, path, native_opts);
        var result: Store = .{
            .allocator = allocator,
            .file = file,
            .read_only = opts.read_only,
            .resource_manager = opts.resource_manager,
            .maintenance_policy = policy,
            .maintenance_start_suppressed = true,
        };
        errdefer result.close();
        // An owner may live for only one CLI invocation. Reopen services debt
        // synchronously so repeated short sessions cannot starve reclamation.
        result.file.retirement_work_pages = opts.reclamation.retirement_work_pages;
        result.file.reserve_retirement_capacity = true;
        result.file.vacuum_target_indexed = opts.reclamation.page_reuse;
        if (!opts.read_only and opts.reclamation.page_reuse and !result.file.header.indexed_reclamation) {
            _ = try result.vacuumWithCancel(null);
        }
        if (!opts.read_only) result.maintainOnce(false) catch |err| result.recordMaintenanceError(err);
        result.maintenance_start_suppressed = false;
        return result;
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Store {
        return try createWithOptions(allocator, path, .{ .exclusive = exclusive });
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !Store {
        const policy = try reclamation.Policy.init(opts.reclamation);
        const minimum = native.NativeFile.minimumOwnerStorageBytes(opts.reclamation.page_reuse);
        if (opts.reclamation.max_storage_bytes != 0 and minimum > opts.reclamation.max_storage_bytes) return error.LiteStorageBudgetExceeded;
        const available = if (opts.reclamation.capacity_probe) |probe|
            try probe.available(probe.context, path)
        else if (opts.io == null and antfly_platform.filesystem.capacity_supported)
            (try antfly_platform.filesystem.capacity(std.fs.path.dirname(path) orelse ".")).available_bytes
        else
            null;
        if (available) |bytes| if (bytes < minimum +| opts.reclamation.disk_headroom_bytes) return error.LiteInsufficientDiskSpace;
        const native_opts = native.CreateOptions{
            .exclusive = opts.exclusive,
            .no_sync = opts.no_sync,
            .resource_manager = opts.resource_manager,
            .writer_lock_marker = opts.writer_lock_marker,
            .indexed_reclamation = opts.reclamation.page_reuse,
        };
        var file = if (opts.io) |io|
            try native.NativeFile.createWithIo(allocator, io, path, native_opts)
        else
            try native.NativeFile.createWithOptions(allocator, path, native_opts);
        file.retirement_work_pages = opts.reclamation.retirement_work_pages;
        file.reserve_retirement_capacity = true;
        if (opts.reclamation.max_storage_bytes != 0) file.max_file_bytes = opts.reclamation.max_storage_bytes;
        file.vacuum_target_indexed = opts.reclamation.page_reuse;
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = false,
            .resource_manager = opts.resource_manager,
            .maintenance_policy = policy,
        };
    }

    fn stopMaintenance(self: *Store) void {
        self.maintenance_cancel.request();
        self.maintenance_wake.set(self.file.runtime());
        if (self.maintenance_future) |*future| future.await(self.file.runtime());
        self.maintenance_future = null;
        self.maintenance_started.store(false, .release);
    }

    pub fn close(self: *Store) void {
        self.stopMaintenance();
        std.debug.assert(self.retired_generations == null);
        if (self.read_generation) |generation| std.debug.assert(generation.references == 0);
        self.retireReadGeneration(0);
        self.file.close();
        self.* = undefined;
    }

    // Called with mutex held. A generation owns a separate read descriptor,
    // so publication can retire the old inode without interrupting snapshots.
    fn pinReadGeneration(self: *Store, pin: *ReadPin) !*ReadGeneration {
        if (self.read_generation == null) {
            const generation = try self.allocator.create(ReadGeneration);
            errdefer self.allocator.destroy(generation);
            generation.* = .{ .file = try native.NativeFile.openWithIo(self.allocator, self.file.runtime(), self.file.path, .{ .read_only = true, .internal_reader = self.file.header.indexed_reclamation, .resource_manager = self.resource_manager }) };
            self.read_generation = generation;
            self.file.secondary_page_cache = &generation.file.page_cache;
        }
        const generation = self.read_generation.?;
        if (generation.references == 0) generation.pinned_at = pin.pinned_at;
        generation.references += 1;
        pin.previous = generation.last_pin;
        if (generation.last_pin) |last| last.next = pin else generation.first_pin = pin;
        generation.last_pin = pin;
        self.file.minimum_reader_sequence = generation.first_pin.?.sequence;
        return generation;
    }

    fn retireReadGeneration(self: *Store, old_bytes: u64) void {
        self.assessment_generation +%= 1;
        self.assessment_cache = null;
        self.shrink_waiting_for_writer = false;
        self.maintenance_policy.status.estimated_at_sequence = null;
        self.admission_refresh_size = 0;
        const generation = self.read_generation orelse return;
        self.read_generation = null;
        self.file.minimum_reader_sequence = null;
        self.file.secondary_page_cache = null;
        generation.retired = true;
        if (generation.references == 0) {
            generation.file.close();
            self.allocator.destroy(generation);
        } else {
            generation.retained_bytes = old_bytes;
            generation.next_retired = self.retired_generations;
            self.retired_generations = generation;
            self.retired_file_bytes +|= generation.retained_bytes;
        }
    }

    fn releaseReadGeneration(self: *Store, generation: *ReadGeneration, pin: *ReadPin) void {
        lockStore(self);
        defer {
            self.mutex.unlock();
            self.startMaintenance();
        }
        std.debug.assert(generation.references > 0);
        if (pin.previous) |previous| previous.next = pin.next else generation.first_pin = pin.next;
        if (pin.next) |next| next.previous = pin.previous else generation.last_pin = pin.previous;
        self.allocator.destroy(pin);
        generation.references -= 1;
        if (generation.first_pin) |first| generation.pinned_at = first.pinned_at;
        if (!generation.retired) self.file.minimum_reader_sequence = if (generation.first_pin) |first| first.sequence else null;
        if (generation.references == 0 and generation.retired) {
            var link = &self.retired_generations;
            while (link.* != generation) link = &link.*.?.next_retired;
            link.* = generation.next_retired;
            self.retired_file_bytes -|= generation.retained_bytes;
            generation.file.close();
            self.allocator.destroy(generation);
            self.admission_refresh_size = 0;
        }
        self.maintenance_wake.set(self.file.runtime());
    }

    pub fn getCatalogRecordAlloc(self: *Store, a: Allocator, key: []const u8) !?[]u8 {
        lockStore(self);
        defer self.mutex.unlock();
        if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
        return self.file.getCatalogRecordAlloc(a, key);
    }

    /// Catalog metadata follows the same owner publication fence as documents
    /// and index files; background retirement can run even on small databases.
    pub fn putCatalogRecord(self: *Store, key: []const u8, value: []const u8) !void {
        const Request = struct {
            key: []const u8,
            value: []const u8,
            fn apply(context: *anyopaque, file: *native.NativeFile) !void {
                const request: *@This() = @ptrCast(@alignCast(context));
                try file.putCatalogRecord(request.key, request.value);
            }
        };
        var request = Request{ .key = key, .value = value };
        try self.submitMutation(&request, Request.apply);
    }

    pub fn backendStore(self: *Store) NativeBackendStore {
        return NativeBackendStore.init(self);
    }

    /// Starts only after the owner has reached its permanent address. A Store
    /// returned by value from open/create must never start a self-referencing task.
    pub fn startMaintenance(self: *Store) void {
        // Notifications already wake the existing worker. Avoid taking the
        // publication mutex again on every reader/writer release: a continuous
        // group-commit writer could otherwise starve vacuum's return path.
        if (self.maintenance_started.load(.acquire)) return;
        lockStore(self);
        defer self.mutex.unlock();
        if (self.read_only or self.maintenance_start_suppressed or self.maintenance_cancel.requested.load(.acquire) or
            self.maintenance_future != null) return;
        const retirement = self.file.retirementNeedsServiceWithCancel(&self.maintenance_cancel) catch false;
        if (!retirement and (!self.maintenance_policy.options.enabled or self.file.activeCheckpoint().page_count *| self.file.header.page_size < self.maintenance_policy.options.minimum_reclaim_bytes)) return;
        self.maintenance_future = self.file.runtime().concurrent(maintenanceLoop, .{self}) catch |err| {
            self.maintenance_policy.status.state = .deferred;
            self.maintenance_policy.status.reason = .concurrency_unavailable;
            self.maintenance_policy.status.last_error = @errorName(err);
            return;
        };
        self.maintenance_started.store(true, .release);
    }

    fn maintenanceLoop(self: *Store) void {
        const io = self.file.runtime();
        var retry_at = std.Io.Timestamp.zero;
        while (!self.maintenance_cancel.requested.load(.acquire)) {
            // Reset before looking at debt. A mutation racing the assessment
            // either contributes to that snapshot or leaves the event set.
            self.maintenance_wake.reset();
            const now = std.Io.Clock.awake.now(io);
            if (now.nanoseconds < retry_at.nanoseconds) {
                // Mutation notifications must not turn a deferred rewrite into
                // a foreground-rate full metadata scan. Honor the retry budget
                // even when a continuously writing client keeps waking us.
                self.maintenance_wake.waitTimeout(io, .{ .duration = .{ .raw = now.durationTo(retry_at), .clock = .awake } }) catch {};
                continue;
            }
            lockStore(self);
            const retry = self.maintenance_policy.status.state == .deferred or self.maintenance_policy.status.state == .failed;
            self.mutex.unlock();
            if (@import("builtin").is_test) _ = self.test_maintenance_cycles.fetchAdd(1, .release);
            var failed = false;
            self.maintainOnceWithCancel(retry, &self.maintenance_cancel) catch |err| {
                failed = true;
                self.recordMaintenanceError(err);
            };
            if (self.maintenance_cancel.requested.load(.acquire)) break;
            lockStore(self);
            const deferred = failed or self.maintenance_policy.status.state == .deferred;
            self.mutex.unlock();
            if (deferred) {
                retry_at = std.Io.Clock.awake.now(io).addDuration(std.Io.Duration.fromMilliseconds(self.maintenance_policy.options.retry_ms));
                self.maintenance_wake.waitTimeout(io, .{ .duration = .{
                    .raw = std.Io.Duration.fromMilliseconds(self.maintenance_policy.options.retry_ms),
                    .clock = .awake,
                } }) catch {};
            } else {
                retry_at = .zero;
                const more = self.maintenanceNeedsService() catch false;
                if (self.maintenance_cancel.requested.load(.acquire)) break;
                if (!more) self.maintenance_wake.waitUncancelable(io);
            }
        }
    }

    // Adoption discards the cached ledger. This probe can therefore replay a
    // whole allocator snapshot; it must share the worker's cancellation fence
    // even though the preceding maintenance cycle has restored its token.
    fn maintenanceNeedsService(self: *Store) !bool {
        lockStore(self);
        defer self.mutex.unlock();
        try self.maintenance_cancel.check();
        if (self.maintenance_running or self.generation_workspace_active or self.file.change_capture != null) return false;
        return self.file.retirementNeedsServiceWithCancel(&self.maintenance_cancel);
    }

    /// One owner-controlled assessment/cycle. Public for cooperative runtimes
    /// without a concurrency lane and deterministic qualification. Ordinary
    /// clients need no maintenance loop. `force` retries existing debt.
    pub fn maintainOnce(self: *Store, force: bool) !void {
        return self.maintainOnceWithCancel(force, null);
    }

    fn maintainOnceWithCancel(self: *Store, force: bool, cancel: ?*const maintenance.CancelToken) !void {
        if (self.read_only) return;
        {
            lockStore(self);
            defer self.mutex.unlock();
            if (self.maintenance_running or self.generation_workspace_active or self.file.change_capture != null) return;
            if (cancel) |token| try token.check();
            const previous_cancel = self.file.allocator_cancel_token;
            self.file.allocator_cancel_token = cancel;
            defer self.file.allocator_cancel_token = previous_cancel;
            if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
            if (try self.file.retirementNeedsService()) {
                try self.refreshAdmissionAssumeLocked();
                _ = try self.file.reclaimPagesWithCancel(self.maintenance_policy.options.retirement_work_pages, cancel);
                self.maintenance_policy.status.state = if (self.maintenance_policy.options.enabled) .idle else .disabled;
                self.maintenance_policy.status.reason = if (self.maintenance_policy.options.enabled) .none else .disabled;
                self.maintenance_policy.status.last_error = null;
            }
        }
        if (!self.maintenance_policy.options.enabled) return;
        var snapshot: ?native.NativeFile = null;
        defer if (snapshot) |*source| source.close();
        var cached_stats: ?native.NativeFile.LiveStats = null;
        var assessed_checkpoint: native.CheckpointSlot = undefined;
        var assessed_size: u64 = undefined;
        var assessed_indexed: bool = undefined;
        var assessed_generation: u64 = undefined;
        {
            lockStore(self);
            defer self.mutex.unlock();
            if (self.maintenance_running or self.generation_workspace_active or self.file.change_capture != null) return;
            if (cancel) |token| try token.check();
            if (self.shrink_waiting_for_writer) {
                if (!self.writerSlotAvailable()) {
                    self.maintenance_policy.status.state = .deferred;
                    self.maintenance_policy.status.reason = .writer_busy;
                    return;
                }
                self.shrink_waiting_for_writer = false;
            }
            const size = (try self.file.file.stat(self.file.runtime())).size;
            self.maintenance_policy.retirement_bytes = self.file.retirement_activity_bytes;
            if (!force and !self.maintenance_policy.due(size)) return;
            if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
            assessed_checkpoint = self.file.activeCheckpoint();
            assessed_size = size;
            assessed_indexed = self.file.header.indexed_reclamation or self.file.vacuum_target_indexed;
            assessed_generation = self.assessment_generation;
            if (force) {
                if (self.assessment_cache) |cache| {
                    if (cache.generation == assessed_generation and cache.indexed == assessed_indexed and std.meta.eql(cache.checkpoint, assessed_checkpoint)) {
                        cached_stats = cache.stats;
                    } else if (cache.generation == assessed_generation and cache.indexed == assessed_indexed and !self.maintenance_policy.due(size)) {
                        // A stale workspace is only a temporary admission hint:
                        // deletions can make a rewrite fit without exhausting
                        // the growth/activity budget. Bound revalidation by
                        // time and scan cost instead of scanning every retry.
                        if (std.Io.Clock.awake.now(self.file.runtime()).nanoseconds < cache.revalidate_at.nanoseconds and
                            try self.admitShrinkWorkspaceAssumeLocked(cache.stats.compact_size, cancel) == null) return;
                    }
                }
            }
            if (cached_stats == null) {
                snapshot = try native.NativeFile.openWithIo(self.allocator, self.file.runtime(), self.file.path, .{ .read_only = true, .resource_manager = self.resource_manager });
                snapshot.?.header = self.file.header;
                snapshot.?.page_cache_policy = .metadata_only;
            }
            self.maintenance_running = true;
            self.maintenance_policy.status.state = .assessing;
        }
        defer {
            lockStore(self);
            self.maintenance_running = false;
            self.maintenance_wake.set(self.file.runtime());
            self.mutex.unlock();
        }
        const scan_started = std.Io.Clock.awake.now(self.file.runtime());
        const stats = cached_stats orelse try snapshot.?.liveStats(cancel);
        const scan_finished = std.Io.Clock.awake.now(self.file.runtime());
        const rewrite = blk: {
            lockStore(self);
            defer self.mutex.unlock();
            if (cancel) |token| try token.check();
            if (self.assessment_generation != assessed_generation) return error.FileBusy;
            const due = if (cached_stats != null)
                self.maintenance_policy.worthwhile(assessed_size, stats.compact_size)
            else
                self.maintenance_policy.assessed(assessed_size, stats.compact_size, stats.bytes, assessed_checkpoint.commit_sequence);
            if (cached_stats == null) {
                // Spend at most ~1/64 of elapsed time rescanning changed roots
                // under sustained pressure, with a 30-second minimum interval.
                // Exact unchanged checkpoints never need time-based rescans.
                const interval_ns = @max(@as(i96, 30 * std.time.ns_per_s), scan_started.durationTo(scan_finished).nanoseconds *| 64);
                self.assessment_cache = .{ .generation = assessed_generation, .checkpoint = assessed_checkpoint, .indexed = assessed_indexed, .stats = stats, .revalidate_at = scan_finished.addDuration(.{ .nanoseconds = interval_ns }) };
            }
            self.maintenance_policy.status.state = .idle;
            self.maintenance_policy.status.reason = .none;
            self.maintenance_policy.status.last_error = null;
            if (!due) break :blk false;
            const workspace = (try self.admitShrinkWorkspaceAssumeLocked(stats.compact_size, cancel)) orelse break :blk false;
            self.maintenance_policy.status.state = .rewriting;
            self.maintenance_policy.status.temporary_bytes = workspace;
            break :blk true;
        };
        if (!rewrite) return;
        defer {
            lockStore(self);
            self.maintenance_policy.status.temporary_bytes = 0;
            self.mutex.unlock();
        }
        // The assessment descriptor names the old inode. Release its shared
        // lock before vacuum; the owner takes its own capture snapshot.
        if (snapshot) |*source| source.close();
        snapshot = null;
        const report = self.performVacuum(cancel, true) catch |err| {
            if (err == error.FileBusy or err == error.WouldBlock) {
                lockStore(self);
                self.shrink_waiting_for_writer = true;
                self.mutex.unlock();
            }
            return err;
        };
        lockStore(self);
        defer self.mutex.unlock();
        self.maintenance_policy.retirement_bytes = self.file.retirement_activity_bytes;
        self.maintenance_policy.completed(report.after_size, report.reclaimed_bytes);
    }

    fn admitShrinkWorkspaceAssumeLocked(self: *Store, compact: u64, cancel: ?*const maintenance.CancelToken) !?u64 {
        const accounting = try self.reclamationStatusAssumeLockedWithCancel(cancel);
        const workspace = @max(compact *| 2, self.maintenance_policy.options.assessment_bytes);
        const budget = self.maintenance_policy.options.max_storage_bytes;
        if (budget != 0 and accounting.totalBytes() +| workspace > budget) {
            self.maintenance_policy.status.state = .deferred;
            self.maintenance_policy.status.reason = .storage_budget;
            return null;
        }
        self.admitEstimatedGenerationAssumeLocked(compact, accounting.current_file_bytes) catch |err| {
            if (err != error.LiteStorageBudgetExceeded) return err;
            self.maintenance_policy.status.state = .deferred;
            self.maintenance_policy.status.reason = .storage_budget;
            return null;
        };
        if (try self.availableDiskBytes()) |available| {
            if (cancel) |token| try token.check();
            self.maintenance_policy.status.available_disk_bytes = available;
            if (available < workspace +| self.maintenance_policy.options.disk_headroom_bytes) {
                self.maintenance_policy.status.state = .deferred;
                self.maintenance_policy.status.reason = .low_disk;
                return null;
            }
        }
        return workspace;
    }

    fn recordMaintenanceError(self: *Store, err: anyerror) void {
        lockStore(self);
        defer self.mutex.unlock();
        const status = &self.maintenance_policy.status;
        status.last_error = @errorName(err);
        status.reason = switch (err) {
            error.FileBusy, error.WouldBlock => .writer_busy,
            error.MaintenanceCanceled, error.Canceled => .canceled,
            error.LiteStorageBudgetExceeded => .storage_budget,
            error.LiteInsufficientDiskSpace => .low_disk,
            else => .maintenance_error,
        };
        status.state = if (status.reason == .writer_busy or status.reason == .storage_budget or status.reason == .low_disk) .deferred else .failed;
    }

    fn availableDiskBytes(self: *Store) !?u64 {
        if (self.maintenance_policy.options.capacity_probe) |probe| return try probe.available(probe.context, self.file.path);
        if (self.file.borrowed_io != null or !antfly_platform.filesystem.capacity_supported) return null;
        return (try antfly_platform.filesystem.capacity(self.file.path)).available_bytes;
    }

    /// Called by serialized adapters before direct native publication.
    pub fn refreshAdmissionAssumeLocked(self: *Store) !void {
        const options = self.maintenance_policy.options;
        const budget_limit: ?u64 = if (options.max_storage_bytes == 0) null else options.max_storage_bytes -| self.retired_file_bytes -| self.maintenance_policy.status.temporary_bytes;
        const size = self.file.activeCheckpoint().page_count *| self.file.header.page_size;
        if (size >= self.admission_refresh_size) {
            if (try self.availableDiskBytes()) |available| {
                self.maintenance_policy.status.available_disk_bytes = available;
                self.disk_admission_limit = size +| (available -| options.disk_headroom_bytes);
            }
            self.admission_refresh_size = size +| options.assessment_bytes;
        }
        // Keep the prepared image's reserved capacity unavailable to tail
        // appends in the current generation throughout copy and catch-up.
        const disk_limit = if (self.disk_admission_limit) |limit| limit -| self.maintenance_policy.status.temporary_bytes else null;
        self.file.max_file_bytes = disk_limit;
        if (budget_limit) |limit| self.file.max_file_bytes = @min(disk_limit orelse limit, limit);
    }

    pub fn reclamationStatus(self: *Store) !reclamation.Status {
        lockStore(self);
        defer self.mutex.unlock();
        return try self.reclamationStatusAssumeLocked();
    }

    pub fn reclamationStatusAssumeLocked(self: *Store) !reclamation.Status {
        return self.reclamationStatusAssumeLockedWithCancel(null);
    }

    fn reclamationStatusAssumeLockedWithCancel(self: *Store, cancel: ?*const maintenance.CancelToken) !reclamation.Status {
        if (cancel) |token| try token.check();
        var status = self.maintenance_policy.status;
        status.allocator_enabled = self.file.header.indexed_reclamation;
        status.shrinking_enabled = self.maintenance_policy.options.enabled;
        status.retirement_activity_bytes = self.file.retirement_activity_bytes;
        status.retired_file_bytes = self.retired_file_bytes;
        status.current_file_bytes = (try self.file.file.stat(self.file.runtime())).size;
        const now = std.Io.Clock.awake.now(self.file.runtime());
        if (self.read_generation) |generation| {
            status.retained_readers += generation.references;
            if (generation.references > 0) status.oldest_reader_age_ms = @intCast(@max(0, generation.pinned_at.durationTo(now).toMilliseconds()));
        }
        var next = self.retired_generations;
        while (next) |generation| : (next = generation.next_retired) {
            if (cancel) |token| try token.check();
            status.retired_generations += 1;
            status.retained_readers += generation.references;
            status.oldest_reader_age_ms = @max(status.oldest_reader_age_ms, @as(u64, @intCast(@max(0, generation.pinned_at.durationTo(now).toMilliseconds()))));
        }
        if (try self.file.allocatorStatsWithCancel(cancel)) |stats| {
            status.reusable_pages = stats.reusable_pages;
            status.pending_retirement_objects = stats.pending_objects;
            status.pending_data_retirement_objects = stats.pending_data_objects;
            status.reused_pages = stats.reused_pages;
            status.retired_objects_serviced = stats.serviced_objects;
        }
        return status;
    }

    pub fn runtimeStore(self: *Store, allocator: Allocator) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, RuntimeStore{ .store = self });
    }

    /// Returns a DB runtime store isolated under a caller-owned key prefix, or
    /// the embedded root when `prefix` is empty. Non-empty prefixes must remain
    /// alive for the erased store's lifetime and end in a zero byte so range
    /// scans have an unambiguous namespace boundary.
    pub fn runtimeStoreWithPrefix(self: *Store, allocator: Allocator, prefix: []const u8) !backend_erased.Store {
        if (prefix.len > 0 and prefix[prefix.len - 1] != 0) return error.InvalidArgument;
        return try backend_erased.storeFrom(allocator, RuntimeStore{ .store = self, .prefix = prefix });
    }

    pub fn checkWithCancel(self: *Store, cancel: ?*const @import("../maintenance.zig").CancelToken) !native.CheckReport {
        if (self.read_only) return self.file.checkWithCancel(cancel);
        var snapshot, const size = blk: {
            lockStore(self);
            defer self.mutex.unlock();
            if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
            var file = try native.NativeFile.openWithIo(self.allocator, self.file.runtime(), self.file.path, .{ .read_only = true, .resource_manager = self.resource_manager });
            errdefer file.close();
            file.page_cache_policy = .metadata_only;
            file.header = self.file.header;
            break :blk .{ file, (try file.file.stat(file.runtime())).size };
        };
        defer snapshot.close();
        return snapshot.checkAtFileSizeWithCancel(size, cancel);
    }

    pub fn vacuum(self: *Store) !native.VacuumReport {
        return try self.vacuumWithCancel(null);
    }

    pub fn vacuumWithCancel(self: *Store, cancel: ?*const @import("../maintenance.zig").CancelToken) !native.VacuumReport {
        const report = try self.performVacuum(cancel, false);
        lockStore(self);
        defer self.mutex.unlock();
        self.maintenance_policy.retirement_bytes = self.file.retirement_activity_bytes;
        self.maintenance_policy.completed(report.after_size, report.reclaimed_bytes);
        return report;
    }

    fn performVacuum(self: *Store, cancel: ?*const maintenance.CancelToken, automatic: bool) !native.VacuumReport {
        if (self.read_only) return error.ReadOnly;
        const io = self.file.runtime();
        var capture = native.ChangeCapture{};
        defer capture.deinit(self.allocator);
        var source = blk: {
            lockStore(self);
            defer self.mutex.unlock();
            if (cancel) |token| try token.check();
            if (self.generation_workspace_active or self.file.change_capture != null or (self.maintenance_running and !automatic)) {
                std.log.warn("lite vacuum refused: another maintenance cycle owns the workspace", .{});
                return error.FileBusy;
            }
            if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
            var snapshot = try native.NativeFile.openWithIo(self.allocator, io, self.file.path, .{ .read_only = true, .no_sync = self.file.no_sync, .resource_manager = self.resource_manager });
            snapshot.page_cache_policy = .metadata_only;
            snapshot.header = self.file.header;
            snapshot.vacuum_target_indexed = self.file.vacuum_target_indexed;
            self.file.change_capture = &capture;
            break :blk snapshot;
        };
        var source_open = true;
        defer if (source_open) source.close();
        defer {
            lockStore(self);
            self.file.change_capture = null;
            self.maintenance_wake.set(io);
            self.mutex.unlock();
        }
        // Manual and automatic rewrites share resource admission. Reserve the
        // workspace before copying and enforce it on the prepared allocator,
        // including concurrent-change replay, rather than checking afterwards.
        const workspace_reservation = blk: {
            lockStore(self);
            defer self.mutex.unlock();
            break :blk self.maintenance_policy.status.temporary_bytes;
        };
        const compact = if (workspace_reservation == 0) (try source.liveStats(cancel)).compact_size else 0;
        {
            lockStore(self);
            defer self.mutex.unlock();
            const status = try self.reclamationStatusAssumeLockedWithCancel(cancel);
            const workspace = if (status.temporary_bytes != 0) status.temporary_bytes else @max(compact *| 2, self.maintenance_policy.options.assessment_bytes);
            const budget = self.maintenance_policy.options.max_storage_bytes;
            if (budget != 0 and status.current_file_bytes +| status.retired_file_bytes +| workspace > budget) return error.LiteStorageBudgetExceeded;
            try self.admitEstimatedGenerationAssumeLocked(if (compact != 0) compact else self.maintenance_policy.status.compact_size_estimate orelse workspace / 2, status.current_file_bytes);
            if (try self.availableDiskBytes()) |available| {
                self.maintenance_policy.status.available_disk_bytes = available;
                if (available < workspace +| self.maintenance_policy.options.disk_headroom_bytes) return error.LiteInsufficientDiskSpace;
                source.vacuum_workspace_limit = workspace;
            }
            self.maintenance_policy.status.temporary_bytes = workspace;
            if (budget != 0) source.vacuum_workspace_limit = workspace;
        }
        defer {
            // The image defer closes its retired descriptor first. Release
            // this additional old-inode descriptor before returning capacity.
            source.close();
            source_open = false;
            lockStore(self);
            self.maintenance_policy.status.temporary_bytes = 0;
            self.admission_refresh_size = 0;
            self.mutex.unlock();
        }
        var image = try source.prepareVacuum(cancel);
        defer image.deinit();
        image.prepared.no_sync = true;
        image.prepared.allocator_cancel_token = cancel;
        const initial_sequence = blk: {
            lockStore(self);
            defer self.mutex.unlock();
            break :blk self.file.activeCheckpoint().commit_sequence + 1;
        };
        image.prepared.private_retirement_epoch = 0;
        try image.prepared.preparePublicationSequence(initial_sequence);
        // Catch-up, including the final fenced residual, only journals its
        // metadata. The adopted owner services any new checkpoint debt later.
        image.prepared.incremental_checkpoints = true;
        // Every mutation -- transaction commits and the disk index's
        // out-of-band catalog publications alike -- is applied through the
        // group-commit queue under the store mutex, so that mutex is the one
        // fence that stops a new change from landing between a catch-up and
        // the publish. The writer slot only excludes an open transaction.
        //
        // Each round takes the writer slot without blocking. With the slot
        // and no captured changes, it publishes under the generation lock
        // and the store mutex. With the slot but captured changes, it
        // releases everything and catches the image up outside every
        // foreground lock, so the large copy never stalls writers. Without
        // the slot it backs off briefly so a background transaction that is
        // merely mid-commit can finish; a caller holding its own writer never
        // releases it, and the bounded backoff still ends in FileBusy.
        //
        // The final round holds the store mutex across the catch-up as well
        // as the publish: the residual is then exactly what was captured and
        // nothing can be applied behind it. Holding the writer slot alone is
        // not that fence -- the x86_64 CI runner saw one index publication
        // land after every slot-held catch-up and gave up busy on all eight
        // rounds -- and waiting on the slot without bound deadlocked the
        // caller that holds a writer and expects FileBusy.
        const max_rounds = 8;
        var backoff_ms: u64 = 1;
        for (0..max_rounds) |round| {
            if (cancel) |token| try token.check();
            const final_round = round + 1 == max_rounds;
            if (!self.file.no_sync) try image.prepared.file.sync(io);
            const reserved = blk: {
                self.reserveWriterSlot() catch |err| switch (err) {
                    error.FileBusy => break :blk false,
                    else => return err,
                };
                break :blk true;
            };
            if (reserved) {
                defer self.releaseWriterSlot();
                self.generation_lock.lockUncancelable(io);
                defer self.generation_lock.unlock(io);
                lockStore(self);
                defer self.mutex.unlock();
                if (cancel) |token| try token.check();
                if (capture.overflow) {
                    std.log.warn("lite vacuum gave up: change capture overflowed while holding the writer slot round={d} captured={d}", .{ round, capture.count });
                    return error.FileBusy;
                }
                if (final_round and capture.count > 0) {
                    try self.applyResidualVacuumChangesAssumeLocked(&image, &capture, cancel);
                    if (!self.file.no_sync) try image.prepared.file.sync(io);
                }
                if (capture.count == 0) {
                    if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
                    image.report.before_size = (try self.file.file.stat(io)).size;
                    image.report.reclaimed_bytes = image.report.before_size -| image.report.after_size;
                    image.prepared.no_sync = self.file.no_sync;
                    try image.prepared.finishPublicationSequence(self.file.activeCheckpoint().commit_sequence + 1, image.prepared.activeCheckpoint());
                    image.report.after_size = (try image.prepared.file.stat(io)).size;
                    image.report.reclaimed_bytes = image.report.before_size -| image.report.after_size;
                    try self.admitPreparedGenerationAssumeLockedWithCancel(&image.prepared, image.report.before_size, cancel);
                    const old_handle = self.file.file.handle;
                    const retained_old = if (self.read_generation) |generation| generation.references != 0 else false;
                    defer if (self.file.file.handle != old_handle) {
                        self.retireReadGeneration(image.report.before_size);
                        // After rename the live file is the prepared image.
                        // Charge the old descriptors until their cleanup runs;
                        // retained read generations already account for them.
                        self.maintenance_policy.status.temporary_bytes = if (retained_old) 0 else image.report.before_size;
                    };
                    try self.file.publishVacuum(&image);
                    return image.report;
                }
            } else if (!final_round) {
                io.sleep(std.Io.Duration.fromMilliseconds(@intCast(backoff_ms)), .awake) catch {};
                backoff_ms = @min(backoff_ms * 2, 64);
            }
            if (final_round) break;
            try self.applyResidualVacuumChanges(&image, &capture, cancel);
        }
        std.log.warn("lite vacuum gave up: the writer slot stayed busy through every round captured={d} overflow={}", .{ capture.count, capture.overflow });
        return error.FileBusy;
    }

    /// Catch the prepared vacuum image up to every change captured since the
    /// last catch-up. Takes the store mutex only to snapshot the live header
    /// and swap the capture out; the copy itself runs outside foreground
    /// locks.
    fn applyResidualVacuumChanges(
        self: *Store,
        image: *native.VacuumImage,
        capture: *native.ChangeCapture,
        cancel: ?*const @import("../maintenance.zig").CancelToken,
    ) !void {
        var changes = native.ChangeCapture{};
        defer changes.deinit(self.allocator);
        var latest = blk: {
            lockStore(self);
            defer self.mutex.unlock();
            break :blk try self.takeResidualVacuumChangesAssumeLocked(capture, &changes);
        };
        defer latest.close();
        try latest.applyCapturedChanges(&image.prepared, &changes, &image.report, cancel);
    }

    /// The final-round variant: the caller already holds the store mutex and
    /// keeps it through the publish, so no mutation can be applied while this
    /// copies the residual, and the capture is empty afterwards for as long
    /// as the caller holds the lock.
    fn applyResidualVacuumChangesAssumeLocked(
        self: *Store,
        image: *native.VacuumImage,
        capture: *native.ChangeCapture,
        cancel: ?*const @import("../maintenance.zig").CancelToken,
    ) !void {
        var changes = native.ChangeCapture{};
        defer changes.deinit(self.allocator);
        var latest = try self.takeResidualVacuumChangesAssumeLocked(capture, &changes);
        defer latest.close();
        try latest.applyCapturedChanges(&image.prepared, &changes, &image.report, cancel);
    }

    /// Open a read-only snapshot at the live header and move the captured
    /// changes out of `capture` into `changes`. Caller holds the store mutex.
    fn takeResidualVacuumChangesAssumeLocked(self: *Store, capture: *native.ChangeCapture, changes: *native.ChangeCapture) !native.NativeFile {
        const io = self.file.runtime();
        if (capture.overflow) {
            std.log.warn("lite vacuum gave up: change capture overflowed before catch-up captured={d} key_bytes={d}", .{ capture.count, capture.key_bytes });
            return error.FileBusy;
        }
        var snapshot = try native.NativeFile.openWithIo(self.allocator, io, self.file.path, .{ .read_only = true, .no_sync = true, .resource_manager = self.resource_manager });
        snapshot.page_cache_policy = .metadata_only;
        snapshot.header = self.file.header;
        std.mem.swap(native.ChangeCapture, changes, capture);
        return snapshot;
    }

    /// One owner reservation covers the entire disposable generation. It
    /// excludes competing rewrites and protects capacity from live appends.
    pub const GenerationWorkspace = struct {
        owner: *Store,
        options: reclamation.Options,

        pub fn deinit(self: *GenerationWorkspace) void {
            const owner = self.owner;
            lockStore(owner);
            owner.generation_workspace_active = false;
            owner.maintenance_policy.status.temporary_bytes = 0;
            owner.admission_refresh_size = 0;
            owner.maintenance_wake.set(owner.file.runtime());
            owner.mutex.unlock();
            owner.startMaintenance();
            self.* = undefined;
        }
    };

    pub fn reserveGenerationWorkspace(self: *Store) !GenerationWorkspace {
        if (self.read_only) return error.ReadOnly;
        lockStore(self);
        defer self.mutex.unlock();
        if (self.generation_workspace_active or self.maintenance_running or self.file.change_capture != null) return error.FileBusy;
        if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
        const status = try self.reclamationStatusAssumeLocked();
        var options = self.maintenance_policy.options;
        const minimum = native.NativeFile.minimumOwnerStorageBytes(options.page_reuse);
        var limit: ?u64 = null;
        if (options.max_storage_bytes != 0) {
            limit = options.max_storage_bytes -| status.current_file_bytes -| status.retired_file_bytes;
            if (limit.? < minimum) return error.LiteStorageBudgetExceeded;
        }
        if (try self.availableDiskBytes()) |available| {
            const disk_limit = available -| options.disk_headroom_bytes;
            if (disk_limit < minimum) return error.LiteInsufficientDiskSpace;
            limit = @min(limit orelse disk_limit, disk_limit);
        }
        options.enabled = false; // No second rewrite workspace inside staging.
        options.max_storage_bytes = limit orelse 0;
        self.generation_workspace_active = true;
        self.maintenance_policy.status.temporary_bytes = limit orelse 0;
        self.admission_refresh_size = 0;
        return .{ .owner = self, .options = options };
    }

    /// Publishes an offline, fully finalized store generation while fencing
    /// all live readers and writers. The prepared store remains valid only so
    /// its retired descriptor can be closed by normal teardown.
    pub fn replaceWithPreparedGeneration(self: *Store, prepared: *Store) !native.GenerationPublicationOutcome {
        if (self == prepared) return error.InvalidArgument;
        // The finalized disposable owner is about to surrender its descriptor.
        // Join before taking its mutex: maintenance can need that mutex while
        // canceling capture/copy or finishing an atomic publication boundary.
        prepared.stopMaintenance();
        try self.reserveWriterSlot();
        defer self.releaseWriterSlot();

        const io = self.file.runtime();
        self.generation_lock.lockUncancelable(io);
        defer self.generation_lock.unlock(io);

        lockStore(self);
        defer self.mutex.unlock();
        lockStore(prepared);
        defer prepared.mutex.unlock();
        // Portable archives omit secrets. Check at publication while reserving
        // the writer slot so a secret committed during preparation cannot be lost.
        if (self.file.change_capture != null) return error.FileBusy;
        if (self.secret_store_uncertain or self.file.checkpoint_publication_uncertain) return error.OutcomeUnknown;
        if (try self.file.hasSecretState()) return error.LiteImportTargetNotEmpty;
        const old_bytes = (try self.file.file.stat(io)).size;
        try self.admitPreparedGenerationAssumeLocked(&prepared.file, old_bytes);
        const old_handle = self.file.file.handle;
        defer if (self.file.file.handle != old_handle) {
            self.retireReadGeneration(old_bytes);
            self.maintenance_policy.next_assessment_size = 0;
            self.maintenance_policy.assessed_retirement_bytes = self.file.retirement_activity_bytes;
        };
        return try self.file.replaceWithPreparedGeneration(&prepared.file);
    }

    /// Refuse a predictably inadmissible rewrite before allocating/copying its
    /// image. Final admission still checks actual catch-up bytes and queue debt.
    fn admitEstimatedGenerationAssumeLocked(self: *Store, compact_bytes: u64, old_bytes: u64) !void {
        const budget = self.maintenance_policy.options.max_storage_bytes;
        if (budget == 0) return;
        const retained_old = if (self.read_generation) |generation| (if (generation.references != 0) old_bytes else 0) else 0;
        if (retained_old +| self.retired_file_bytes +| compact_bytes +| self.file.estimatedGenerationReserveBytes(compact_bytes) > budget) return error.LiteStorageBudgetExceeded;
    }

    /// Shared final adoption admission. Prepared bytes replace the workspace
    /// reservation; the old inode remains charged while its readers are pinned.
    fn admitPreparedGenerationAssumeLocked(self: *Store, prepared: *native.NativeFile, old_bytes: u64) !void {
        return self.admitPreparedGenerationAssumeLockedWithCancel(prepared, old_bytes, null);
    }

    fn admitPreparedGenerationAssumeLockedWithCancel(self: *Store, prepared: *native.NativeFile, old_bytes: u64, cancel: ?*const maintenance.CancelToken) !void {
        if (cancel) |token| try token.check();
        const prepared_bytes = (try prepared.file.stat(prepared.runtime())).size;
        const options = self.maintenance_policy.options;
        const reserve = try prepared.retirementReserveBytesWithCancel(cancel);
        const reusable = if (try prepared.allocatorStatsWithCancel(cancel)) |stats| stats.reusable_pages *| prepared.header.page_size else 0;
        const total = old_bytes +| self.retired_file_bytes +| prepared_bytes;
        const retained_old = if (self.read_generation) |generation| (if (generation.references != 0) old_bytes else 0) else 0;
        const adopted_total = retained_old +| self.retired_file_bytes +| prepared_bytes;
        if (options.max_storage_bytes != 0) {
            if (total > options.max_storage_bytes or options.max_storage_bytes -| adopted_total +| reusable < reserve) return error.LiteStorageBudgetExceeded;
        }
        if (try self.availableDiskBytes()) |available| {
            if (cancel) |token| try token.check();
            if (available < options.disk_headroom_bytes +| (reserve -| reusable)) return error.LiteInsufficientDiskSpace;
        }
    }

    /// Synchronous group commit. Callbacks run in queue order under the store
    /// mutex, and every member completes only after the shared checkpoint is
    /// durable. Requests arriving during I/O form the next bounded group.
    /// A failed group publishes none of its mutations (or reports an uncertain
    /// publication to all its members). Callback-owned buffers stay borrowed.
    pub fn submitMutation(self: *Store, context: *anyopaque, apply: *const fn (*anyopaque, *native.NativeFile) anyerror!void) !void {
        return self.submitMutationWithDurability(context, apply, true);
    }

    pub fn submitMutationWithDurability(self: *Store, context: *anyopaque, apply: *const fn (*anyopaque, *native.NativeFile) anyerror!void, durable: bool) !void {
        if (self.read_only) return error.ReadOnly;
        defer {
            // Start after publication: even one large final mutation crossing
            // the threshold must launch maintenance without another write.
            self.startMaintenance();
            self.maintenance_wake.set(self.file.runtime());
        }
        const io = self.file.runtime();
        var request = MutationRequest{ .context = context, .apply = apply, .durable = durable };
        self.commit_mutex.lockUncancelable(io);
        if (self.commit_tail) |tail| tail.next = &request else self.commit_head = &request;
        self.commit_tail = &request;
        if (self.commit_draining) {
            while (!request.done and !request.leader) self.commit_ready.waitUncancelable(io, &self.commit_mutex);
            if (request.done) {
                self.commit_mutex.unlock(io);
                return request.result;
            }
        }
        self.commit_draining = true;
        const head = self.commit_head.?;
        var tail = head;
        var count: usize = 1;
        while (count < 64) : (count += 1) tail = tail.next orelse break;
        self.commit_head = tail.next;
        if (self.commit_head == null) self.commit_tail = null;
        tail.next = null;
        self.commit_mutex.unlock(io);
        const result = self.applyMutationGroup(head);
        self.commit_mutex.lockUncancelable(io);
        var current: ?*MutationRequest = head;
        while (current) |item| {
            item.result = result;
            item.done = true;
            current = item.next;
        }
        if (self.commit_head) |next| next.leader = true else self.commit_draining = false;
        self.commit_ready.broadcast(io);
        self.commit_mutex.unlock(io);
        if (result) |_| {} else |err| {
            if (err == error.LiteStorageBudgetExceeded) self.recordMaintenanceError(err);
        }
        return request.result;
    }

    fn applyMutationGroup(self: *Store, head: *MutationRequest) !void {
        lockStore(self);
        defer self.mutex.unlock();
        if (self.secret_store_uncertain) return error.OutcomeUnknown;
        try self.refreshAdmissionAssumeLocked();
        try self.file.beginTransaction();
        errdefer self.file.abortTransaction();
        var current: ?*MutationRequest = head;
        var durable = false;
        while (current) |item| {
            durable = durable or item.durable;
            try item.apply(item.context, &self.file);
            current = item.next;
        }
        try self.file.commitTransactionWithDurability(durable);
    }

    fn writerSlotAvailable(self: *Store) bool {
        const io = self.file.runtime();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        // This is an admission hint, not a reservation. Online capture still
        // permits foreground writers throughout the subsequent image copy.
        return !self.writer_active and self.next_writer_ticket == self.serving_writer_ticket;
    }

    pub fn reserveWriterSlot(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        const io = self.file.runtime();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        if (self.writer_active or self.next_writer_ticket != self.serving_writer_ticket) return error.FileBusy;
        self.writer_active = true;
        self.writer_ticketed = false;
    }

    pub fn reserveWriterSlotYielding(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        const io = self.file.runtime();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        const ticket = self.next_writer_ticket;
        self.next_writer_ticket +%= 1;
        while (self.writer_active or ticket != self.serving_writer_ticket) {
            self.writer_ready.waitUncancelable(io, &self.writer_mutex);
        }
        self.writer_active = true;
        self.writer_ticketed = true;
    }

    pub fn releaseWriterSlot(self: *Store) void {
        const io = self.file.runtime();
        self.writer_mutex.lockUncancelable(io);
        defer {
            self.writer_mutex.unlock(io);
            self.maintenance_wake.set(io);
            self.startMaintenance();
        }
        std.debug.assert(self.writer_active);
        self.writer_active = false;
        if (self.writer_ticketed) self.serving_writer_ticket +%= 1;
        self.writer_ticketed = false;
        self.writer_ready.broadcast(io);
    }

    const NativeBackendStore = backend_adapter.Store(Store, Txn, Txn, Txn, .{
        .capabilities = capabilities,
        .begin_read = beginRead,
        .begin_write = beginWrite,
        .begin_batch = beginBatch,
        .begin_batch_with_options = beginBatchWithOptions,
    });

    pub fn capabilities(_: *Store) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .native_namespaces = false,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn beginRead(self: *Store) !Txn {
        return try Txn.openRead(self);
    }

    /// Pin both checkpoint and file generation while inspecting metadata.
    /// Online vacuum may replace the writer's inode during the probe.
    pub fn hasLiveDocumentOutsidePrefix(self: *Store, excluded_prefix: []const u8) !bool {
        var txn = try self.beginRead();
        defer txn.abort();
        return try (try txn.readFile()).hasLiveDocumentOutsidePrefix(txn.checkpoint, excluded_prefix);
    }

    pub fn beginWrite(self: *Store) !Txn {
        if (self.read_only) return error.ReadOnly;
        return try Txn.openWrite(self);
    }

    pub fn beginWriteYielding(self: *Store) !Txn {
        if (self.read_only) return error.ReadOnly;
        return try Txn.openWriteYielding(self);
    }

    pub fn beginBatch(self: *Store) !Txn {
        return try self.beginWrite();
    }

    pub fn beginBatchYielding(self: *Store) !Txn {
        return try self.beginWriteYielding();
    }

    pub fn beginBatchWithOptions(self: *Store, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try self.beginBatch();
    }

    pub fn beginBatchWithOptionsYielding(self: *Store, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try self.beginBatchYielding();
    }

    pub fn lastReplaySequence(self: *Store, fallback_last: u64) u64 {
        const next = self.nextReplaySequence(fallback_last + 1);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn nextReplaySequence(self: *Store, fallback_next: u64) u64 {
        var read = self.beginRead() catch return fallback_next;
        defer read.abort();
        const raw = read.get(internal_keys.replay_meta_next_sequence_key[0..]) catch return fallback_next;
        if (raw.len != 8) return fallback_next;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    pub fn appendReplayOpaque(self: *Store, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var txn = try self.beginWrite();
        errdefer txn.abort();
        try txn.setReplayOpaque(sequence, payload);
        try txn.commit();
    }

    pub fn appendReplayOpaqueYielding(self: *Store, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var txn = try self.beginWriteYielding();
        errdefer txn.abort();
        try txn.setReplayOpaque(sequence, payload);
        try txn.commit();
    }

    pub fn iterateReplayFrom(self: *Store, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        return try collectReplayEntries(self, "", alloc, from_sequence);
    }

    pub fn forEachReplayLaneFrom(
        self: *Store,
        kind_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        callback_ctx: *anyopaque,
        callback: backend_erased.Store.ReplayCallback,
    ) !backend_types.ReplayLaneIterationStats {
        return try forEachReplayLane(self, "", kind_ordinal, from_sequence, max_entries, callback_ctx, callback);
    }

    pub fn truncateReplayUpTo(self: *Store, alloc: Allocator, up_to_sequence: u64) !void {
        _ = alloc;
        try truncateReplay(self, "", false, up_to_sequence);
    }

    pub fn truncateReplayUpToYielding(self: *Store, alloc: Allocator, up_to_sequence: u64) !void {
        _ = alloc;
        try truncateReplay(self, "", true, up_to_sequence);
    }
};

const RuntimeStore = struct {
    store: *Store,
    prefix: []const u8 = "",

    pub fn capabilities(self: *RuntimeStore) backend_types.Capabilities {
        return Store.capabilities(self.store);
    }

    pub fn beginRead(self: *RuntimeStore) !Txn {
        return try Txn.openReadWithPrefix(self.store, self.prefix);
    }

    pub fn beginWrite(self: *RuntimeStore) !Txn {
        return try Txn.openWriteYieldingWithPrefix(self.store, self.prefix);
    }

    pub fn beginBatch(self: *RuntimeStore) !Txn {
        return try Txn.openWriteYieldingWithPrefix(self.store, self.prefix);
    }

    pub fn beginBatchWithOptions(self: *RuntimeStore, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try Txn.openWriteYieldingWithPrefix(self.store, self.prefix);
    }

    pub fn lastReplaySequence(self: *RuntimeStore, fallback_last: u64) u64 {
        const next = self.nextReplaySequence(fallback_last + 1);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn nextReplaySequence(self: *RuntimeStore, fallback_next: u64) u64 {
        var read = self.beginRead() catch return fallback_next;
        defer read.abort();
        const raw = read.get(internal_keys.replay_meta_next_sequence_key[0..]) catch return fallback_next;
        if (raw.len != 8) return fallback_next;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    pub fn appendReplayOpaque(self: *RuntimeStore, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var txn = try self.beginWrite();
        errdefer txn.abort();
        try txn.setReplayOpaque(sequence, payload);
        try txn.commit();
    }

    pub fn iterateReplayFrom(self: *RuntimeStore, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        return try collectReplayEntries(self.store, self.prefix, alloc, from_sequence);
    }

    pub fn forEachReplayLaneFrom(
        self: *RuntimeStore,
        kind_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        callback_ctx: *anyopaque,
        callback: backend_erased.Store.ReplayCallback,
    ) !backend_types.ReplayLaneIterationStats {
        return try forEachReplayLane(self.store, self.prefix, kind_ordinal, from_sequence, max_entries, callback_ctx, callback);
    }

    pub fn truncateReplayUpTo(self: *RuntimeStore, alloc: Allocator, up_to_sequence: u64) !void {
        _ = alloc;
        try truncateReplay(self.store, self.prefix, true, up_to_sequence);
    }
};

const PendingTree = std.Treap([]const u8, struct {
    fn compare(a: []const u8, b: []const u8) std.math.Order {
        return std.mem.order(u8, a, b);
    }
}.compare);

const PendingNode = struct {
    tree: PendingTree.Node = undefined,
    ordinal: usize,
};

const PendingMutation = struct {
    key: []u8,
    value: ?[]u8 = null,
    borrowed: bool = false,
};

pub const Txn = struct {
    allocator: Allocator,
    store: ?*Store = null,
    pending: std.ArrayListUnmanaged(PendingMutation) = .empty,
    pending_tree: PendingTree = .{},
    pending_count: usize = 0,
    borrowed_versions: std.ArrayListUnmanaged([]u8) = .empty,
    read_only: bool = true,
    writer_reserved: bool = false,
    prefix: []const u8 = "",
    // Immutable snapshot hits and misses are owned once per transaction.
    // Pending mutations take precedence without invalidating borrowed values.
    snapshot_reads: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    read_generation: ?*ReadGeneration = null,
    read_pin: ?*ReadPin = null,
    checkpoint: native.CheckpointSlot = .{},

    pub fn openRead(store: *Store) !Txn {
        return try openReadWithPrefix(store, "");
    }

    pub fn openReadWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        lockStore(store);
        defer store.mutex.unlock();
        const checkpoint = store.file.activeCheckpoint();
        const pin = if (store.read_only) null else try store.allocator.create(ReadPin);
        errdefer if (pin) |owned| store.allocator.destroy(owned);
        if (pin) |owned| owned.* = .{ .sequence = checkpoint.commit_sequence, .pinned_at = std.Io.Clock.awake.now(store.file.runtime()) };
        return .{
            .allocator = store.allocator,
            .store = store,
            .read_only = true,
            .read_pin = pin,
            .prefix = prefix,
            // Read-only stores already own their immutable generation. Reopening
            // the pathname here could select a replacement inode after vacuum.
            .read_generation = if (store.read_only) null else try store.pinReadGeneration(pin.?),
            .checkpoint = checkpoint,
        };
    }

    pub fn openWrite(store: *Store) !Txn {
        return try openWriteWithPrefix(store, "");
    }

    pub fn openWriteWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        try store.reserveWriterSlot();
        errdefer store.releaseWriterSlot();

        lockStore(store);
        defer store.mutex.unlock();

        const checkpoint = store.file.activeCheckpoint();
        const pin = if (store.file.header.indexed_reclamation) try store.allocator.create(ReadPin) else null;
        errdefer if (pin) |owned| store.allocator.destroy(owned);
        if (pin) |owned| owned.* = .{ .sequence = checkpoint.commit_sequence, .pinned_at = std.Io.Clock.awake.now(store.file.runtime()) };
        const generation = if (pin) |owned| try store.pinReadGeneration(owned) else null;
        return .{
            .allocator = store.allocator,
            .store = store,
            .read_only = false,
            .writer_reserved = true,
            .read_pin = pin,
            .read_generation = generation,
            .prefix = prefix,
            .checkpoint = checkpoint,
        };
    }

    pub fn openWriteYielding(store: *Store) !Txn {
        return try openWriteYieldingWithPrefix(store, "");
    }

    pub fn openWriteYieldingWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        try store.reserveWriterSlotYielding();
        errdefer store.releaseWriterSlot();

        lockStore(store);
        defer store.mutex.unlock();

        const checkpoint = store.file.activeCheckpoint();
        const pin = if (store.file.header.indexed_reclamation) try store.allocator.create(ReadPin) else null;
        errdefer if (pin) |owned| store.allocator.destroy(owned);
        if (pin) |owned| owned.* = .{ .sequence = checkpoint.commit_sequence, .pinned_at = std.Io.Clock.awake.now(store.file.runtime()) };
        const generation = if (pin) |owned| try store.pinReadGeneration(owned) else null;
        return .{
            .allocator = store.allocator,
            .store = store,
            .read_only = false,
            .writer_reserved = true,
            .read_pin = pin,
            .read_generation = generation,
            .prefix = prefix,
            .checkpoint = checkpoint,
        };
    }

    pub fn abort(self: *Txn) void {
        self.freePending();
        self.freeOwnedReads();
        self.releaseGenerationReadLock();
        self.releaseWriterSlot();
        self.* = undefined;
    }

    pub fn commit(self: *Txn) !void {
        if (self.read_only) return error.ReadOnly;
        const store = self.store orelse return error.ReadOnly;
        const allocator = self.allocator;
        var mutations = try allocator.alloc(native.DocumentMutation, self.pending_count);
        defer allocator.free(mutations);
        var node = self.pending_tree.getMin();
        var i: usize = 0;
        while (node) |current| : (i += 1) {
            const pending = self.pending.items[pendingNode(current).ordinal];
            mutations[i] = .{ .key = pending.key, .value = pending.value orelse "", .is_delete = pending.value == null };
            node = current.next();
        }

        const Commit = struct {
            mutations: []const native.DocumentMutation,
            fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
                const context: *@This() = @ptrCast(@alignCast(ptr));
                try file.putDocumentBatch(context.mutations);
            }
        };
        var context = Commit{ .mutations = mutations };
        errdefer {
            if (self.writer_reserved) {
                store.releaseWriterSlot();
                self.writer_reserved = false;
            }
        }
        try store.submitMutation(&context, Commit.apply);
        self.releaseGenerationReadLock();
        if (self.writer_reserved) {
            store.releaseWriterSlot();
            self.writer_reserved = false;
        }

        self.freePending();
        self.freeOwnedReads();
        self.* = undefined;
    }

    pub fn get(self: *Txn, key: []const u8) ![]const u8 {
        const lookup_key = try self.prefixedKey(key);
        defer if (self.prefix.len > 0) self.allocator.free(lookup_key);
        if (self.pending_tree.getEntryFor(lookup_key).node) |node| {
            const pending = &self.pending.items[pendingNode(node).ordinal];
            const value = pending.value orelse return error.NotFound;
            pending.borrowed = true;
            return value;
        }
        if (self.snapshot_reads.get(lookup_key)) |cached| return cached orelse error.NotFound;
        const value = blk: {
            const owned_key = try self.allocator.dupe(u8, lookup_key);
            errdefer self.allocator.free(owned_key);
            try self.snapshot_reads.ensureUnusedCapacity(self.allocator, 1);
            const loaded = try (try self.readFile()).getDocumentAtCheckpointAlloc(self.allocator, self.checkpoint, lookup_key);
            self.snapshot_reads.putAssumeCapacity(owned_key, loaded);
            break :blk loaded;
        };
        return value orelse error.NotFound;
    }

    pub fn getManySorted(self: *Txn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        for (keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        }
        const alloc = self.allocator;
        var misses: std.ArrayList([]const u8) = .empty;
        defer {
            for (misses.items) |key| alloc.free(key);
            misses.deinit(alloc);
        }
        var positions: std.ArrayList(usize) = .empty;
        defer positions.deinit(alloc);
        var i: usize = 0;
        while (i < keys.len) {
            var end = i + 1;
            while (end < keys.len and std.mem.eql(u8, keys[i], keys[end])) : (end += 1) {}
            const full = try self.prefixedKey(keys[i]);
            defer if (self.prefix.len > 0) alloc.free(full);
            if (self.pending_tree.getEntryFor(full).node) |node| {
                const pending = &self.pending.items[pendingNode(node).ordinal];
                pending.borrowed = pending.value != null;
                @memset(values[i..end], pending.value);
            } else if (self.snapshot_reads.get(full)) |cached| {
                @memset(values[i..end], cached);
            } else {
                try misses.ensureUnusedCapacity(alloc, 1);
                try positions.ensureUnusedCapacity(alloc, 1);
                const owned_key = try alloc.dupe(u8, full);
                misses.appendAssumeCapacity(owned_key);
                positions.appendAssumeCapacity(i);
            }
            i = end;
        }
        if (misses.items.len == 0) return;
        // Reserve all cache slots before acquiring payloads. Once the batch
        // succeeds, transferring key/value ownership cannot fail.
        try self.snapshot_reads.ensureUnusedCapacity(alloc, std.math.cast(u32, misses.items.len) orelse return error.RecordTooLarge);
        const loaded = try alloc.alloc(?[]const u8, misses.items.len);
        defer alloc.free(loaded);
        try (try self.readFile()).getDocumentsAtCheckpointAlloc(alloc, self.checkpoint, misses.items, loaded);
        for (misses.items, loaded, positions.items) |*key, value, position| {
            self.snapshot_reads.putAssumeCapacity(key.*, value);
            key.* = "";
            var end = position + 1;
            while (end < keys.len and std.mem.eql(u8, keys[position], keys[end])) : (end += 1) {}
            @memset(values[position..end], value);
        }
    }

    pub fn put(self: *Txn, key: []const u8, value: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = if (self.prefix.len == 0)
            try self.allocator.dupe(u8, key)
        else
            try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.appendPending(.{ .key = owned_key, .value = owned_value });
    }

    pub fn delete(self: *Txn, key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = if (self.prefix.len == 0)
            try self.allocator.dupe(u8, key)
        else
            try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
        errdefer self.allocator.free(owned_key);
        try self.appendPending(.{ .key = owned_key });
    }

    fn pendingNode(node: *PendingTree.Node) *PendingNode {
        return @fieldParentPtr("tree", node);
    }

    // Keep one active slot per key. Only values returned by the borrowed-read
    // APIs survive replacement; cursors own their copies independently.
    fn appendPending(self: *Txn, mutation: PendingMutation) !void {
        var entry = self.pending_tree.getEntryFor(mutation.key);
        if (entry.node) |existing| {
            const current = &self.pending.items[pendingNode(existing).ordinal];
            if (current.value) |value| {
                if (current.borrowed) {
                    // Reserve before changing any ownership so OOM leaves the
                    // pending value and every existing borrow intact.
                    try self.borrowed_versions.append(self.allocator, value);
                } else self.allocator.free(value);
            }
            self.allocator.free(mutation.key);
            current.value = mutation.value;
            current.borrowed = false;
            return;
        }
        const node = try self.allocator.create(PendingNode);
        errdefer self.allocator.destroy(node);
        try self.pending.append(self.allocator, mutation);
        node.ordinal = self.pending.items.len - 1;
        entry.set(&node.tree);
        self.pending_count += 1;
    }

    pub fn setReplayOpaque(self: *Txn, sequence: u64, payload: []const u8) !void {
        try writeReplayEntries(self.allocator, self, sequence, payload);
    }

    pub fn openCursor(self: *Txn) !Cursor {
        return .{
            .txn = self,
            .index_cursor = native.DocumentIndexCursor.init(try self.readFile(), self.checkpoint),
        };
    }

    pub fn openKeyCursor(self: *Txn) !KeyCursor {
        var cursor = try self.openCursor();
        cursor.load_values = false;
        return .{ .inner = cursor };
    }

    fn freePending(self: *Txn) void {
        while (self.pending_tree.getMin()) |node| {
            var entry = self.pending_tree.getEntryForExisting(node);
            entry.set(null);
            self.allocator.destroy(pendingNode(node));
        }
        self.pending_count = 0;
        for (self.pending.items) |pending| {
            self.allocator.free(pending.key);
            if (pending.value) |value| self.allocator.free(value);
        }
        self.pending.deinit(self.allocator);
        self.pending = .empty;
        for (self.borrowed_versions.items) |value| self.allocator.free(value);
        self.borrowed_versions.deinit(self.allocator);
        self.borrowed_versions = .empty;
    }

    fn freeOwnedReads(self: *Txn) void {
        var entries = self.snapshot_reads.iterator();
        while (entries.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            if (entry.value_ptr.*) |value| self.allocator.free(value);
        }
        self.snapshot_reads.deinit(self.allocator);
        self.snapshot_reads = .empty;
    }

    fn readFile(self: *Txn) !*native.NativeFile {
        if (self.read_generation) |generation| return &generation.file;
        const store = self.store orelse return error.InvalidTransactionState;
        return &store.file;
    }

    fn releaseGenerationReadLock(self: *Txn) void {
        const generation = self.read_generation orelse return;
        const store = self.store orelse return;
        self.read_generation = null;
        store.releaseReadGeneration(generation, self.read_pin.?);
        self.read_pin = null;
    }

    fn releaseWriterSlot(self: *Txn) void {
        if (!self.writer_reserved) return;
        const store = self.store orelse return;
        store.releaseWriterSlot();
        self.writer_reserved = false;
    }

    fn prefixedKey(self: *Txn, key: []const u8) ![]const u8 {
        if (self.prefix.len == 0) return key;
        return try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
    }
};

fn validatePrefix(prefix: []const u8) !void {
    if (prefix.len > 0 and prefix[prefix.len - 1] != 0) return error.InvalidArgument;
}

/// Keys borrow cursor storage until the next move or close, just like Cursor.
/// Uses the same pinned snapshot, prefix bounds, and pending-write overlay,
/// but never allocates or reads external document values.
pub const KeyCursor = struct {
    inner: Cursor,

    pub fn close(self: *KeyCursor) void {
        self.inner.close();
    }

    pub fn setUpperBound(self: *KeyCursor, upper: ?[]const u8) void {
        self.inner.setUpperBound(upper);
    }

    pub fn first(self: *KeyCursor) ![]const u8 {
        return (try self.inner.first()).key;
    }

    pub fn last(self: *KeyCursor) ![]const u8 {
        return (try self.inner.last()).key;
    }

    pub fn next(self: *KeyCursor) ![]const u8 {
        return (try self.inner.next()).key;
    }

    pub fn prev(self: *KeyCursor) ![]const u8 {
        return (try self.inner.prev()).key;
    }

    pub fn seekAtOrAfter(self: *KeyCursor, key: []const u8) ![]const u8 {
        return (try self.inner.seekAtOrAfter(key)).key;
    }

    pub fn seekAtOrBefore(self: *KeyCursor, key: []const u8) ![]const u8 {
        return (try self.inner.seekAtOrBefore(key)).key;
    }
};

pub const Cursor = struct {
    const Direction = enum { forward, backward };

    txn: *Txn,
    index_cursor: native.DocumentIndexCursor,
    records: native.RecordPageReader = .{},
    load_values: bool = true,
    current_key: ?[]u8 = null,
    upper_bound: ?[]const u8 = null,
    owned_value: ?[]u8 = null,
    disk_candidate: ?native.DocumentIndexEntry = null,
    last_direction: ?Direction = null,

    pub fn close(self: *Cursor) void {
        self.records.deinit(self.txn.allocator);
        self.index_cursor.deinit();
        if (self.disk_candidate) |*candidate| candidate.deinit(self.txn.allocator);
        if (self.current_key) |key| self.txn.allocator.free(key);
        if (self.owned_value) |value| self.txn.allocator.free(value);
        self.current_key = null;
        self.owned_value = null;
        self.disk_candidate = null;
    }

    pub fn first(self: *Cursor) !backend_adapter.Entry {
        self.clearBufferedDisk();
        const disk = if (self.txn.prefix.len == 0)
            try self.index_cursor.first()
        else
            try self.index_cursor.seekAtOrAfter(self.txn.prefix, false);
        return try self.resolveMerged(disk, self.overlayAtOrAfter(self.txn.prefix, false), .forward);
    }

    pub fn last(self: *Cursor) !backend_adapter.Entry {
        self.clearBufferedDisk();
        if (self.upper_bound) |upper_bound| {
            const upper = try self.txn.prefixedKey(upper_bound);
            defer if (self.txn.prefix.len > 0) self.txn.allocator.free(upper);
            return try self.resolveMerged(
                try self.index_cursor.seekAtOrBefore(upper, true),
                self.overlayAtOrBefore(upper, true),
                .backward,
            );
        }
        if (self.txn.prefix.len == 0) {
            return try self.resolveMerged(
                try self.index_cursor.last(),
                self.txn.pending_tree.getMax(),
                .backward,
            );
        } else {
            const upper = try self.namespaceUpperBound();
            defer self.txn.allocator.free(upper);
            return try self.resolveMerged(
                try self.index_cursor.seekAtOrBefore(upper, true),
                self.overlayAtOrBefore(upper, true),
                .backward,
            );
        }
    }

    pub fn next(self: *Cursor) !backend_adapter.Entry {
        const current = self.current_key orelse return error.NotFound;
        const disk = if (self.last_direction == .forward)
            self.takeBufferedDisk() orelse try self.index_cursor.next()
        else blk: {
            self.clearBufferedDisk();
            break :blk try self.index_cursor.seekAtOrAfter(current, true);
        };
        return try self.resolveMerged(disk, self.overlayAtOrAfter(current, true), .forward);
    }

    pub fn prev(self: *Cursor) !backend_adapter.Entry {
        const current = self.current_key orelse return error.NotFound;
        const disk = if (self.last_direction == .backward)
            self.takeBufferedDisk() orelse try self.index_cursor.prev()
        else blk: {
            self.clearBufferedDisk();
            break :blk try self.index_cursor.seekAtOrBefore(current, true);
        };
        return try self.resolveMerged(disk, self.overlayAtOrBefore(current, true), .backward);
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const lookup_key = try self.txn.prefixedKey(key);
        defer if (self.txn.prefix.len > 0) self.txn.allocator.free(lookup_key);
        self.clearBufferedDisk();
        return try self.resolveMerged(
            try self.index_cursor.seekAtOrAfter(lookup_key, false),
            self.overlayAtOrAfter(lookup_key, false),
            .forward,
        );
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const lookup_key = try self.txn.prefixedKey(key);
        defer if (self.txn.prefix.len > 0) self.txn.allocator.free(lookup_key);
        self.clearBufferedDisk();
        if (self.upper_bound) |upper_bound| {
            const upper = try self.txn.prefixedKey(upper_bound);
            defer if (self.txn.prefix.len > 0) self.txn.allocator.free(upper);
            if (std.mem.order(u8, lookup_key, upper) != .lt) {
                return try self.resolveMerged(
                    try self.index_cursor.seekAtOrBefore(upper, true),
                    self.overlayAtOrBefore(upper, true),
                    .backward,
                );
            }
        }
        return try self.resolveMerged(
            try self.index_cursor.seekAtOrBefore(lookup_key, false),
            self.overlayAtOrBefore(lookup_key, false),
            .backward,
        );
    }

    pub fn setUpperBound(self: *Cursor, upper: ?[]const u8) void {
        self.clearBufferedDisk();
        self.last_direction = null;
        self.upper_bound = upper;
    }

    fn resolveMerged(
        self: *Cursor,
        initial_disk: ?native.DocumentIndexEntry,
        initial_overlay_index: ?*PendingTree.Node,
        direction: Direction,
    ) !backend_adapter.Entry {
        const file = try self.txn.readFile();
        var disk = initial_disk;
        errdefer if (disk) |*candidate| candidate.deinit(self.txn.allocator);
        var overlay_index = initial_overlay_index;

        while (true) {
            if (disk) |candidate| {
                if (!self.keyInRange(candidate.key)) {
                    var out_of_range = candidate;
                    out_of_range.deinit(self.txn.allocator);
                    disk = null;
                }
            }
            const overlay_candidate = if (overlay_index) |index| blk: {
                const candidate = self.txn.pending.items[Txn.pendingNode(index).ordinal];
                if (!self.keyInRange(candidate.key)) {
                    overlay_index = null;
                    break :blk null;
                }
                break :blk candidate;
            } else null;

            const choose_overlay = if (overlay_candidate) |pending| blk: {
                const indexed = disk orelse break :blk true;
                const order = std.mem.order(u8, pending.key, indexed.key);
                break :blk switch (direction) {
                    .forward => order != .gt,
                    .backward => order != .lt,
                };
            } else false;

            if (choose_overlay) {
                const pending = overlay_candidate.?;
                if (disk) |indexed| {
                    if (std.mem.eql(u8, pending.key, indexed.key)) {
                        var consumed = indexed;
                        consumed.deinit(self.txn.allocator);
                        disk = null;
                        disk = try self.advanceDisk(direction);
                    }
                }
                overlay_index = self.advanceOverlayIndex(overlay_index.?, direction);
                const value = pending.value orelse continue;
                const buffered_disk = disk;
                disk = null;
                return try self.installPending(pending.key, value, buffered_disk, direction);
            }

            if (disk) |indexed| {
                var consumed = indexed;
                disk = null;
                const value = self.readValue(file, consumed) catch |err| {
                    consumed.deinit(self.txn.allocator);
                    return err;
                };
                if (value) |owned_value| return self.installDisk(consumed, owned_value, direction);
                consumed.deinit(self.txn.allocator);
                disk = try self.advanceDisk(direction);
                continue;
            }
            return error.NotFound;
        }
    }

    fn readValue(self: *Cursor, file: *native.NativeFile, indexed: native.DocumentIndexEntry) !?[]u8 {
        if (self.load_values)
            return try self.records.documentValueAlloc(file, self.txn.allocator, self.txn.checkpoint, indexed);
        if (!try self.records.documentIsLive(file, self.txn.allocator, self.txn.checkpoint, indexed)) return null;
        return try self.txn.allocator.alloc(u8, 0);
    }

    fn overlayAtOrAfter(self: *const Cursor, key: []const u8, strict: bool) ?*PendingTree.Node {
        var node = self.txn.pending_tree.root;
        var result: ?*PendingTree.Node = null;
        while (node) |current| {
            const order = std.mem.order(u8, current.key, key);
            if (order == .gt or (!strict and order == .eq)) {
                result = current;
                node = current.children[0];
            } else node = current.children[1];
        }
        return result;
    }

    fn overlayAtOrBefore(self: *const Cursor, key: []const u8, strict: bool) ?*PendingTree.Node {
        var node = self.txn.pending_tree.root;
        var result: ?*PendingTree.Node = null;
        while (node) |current| {
            const order = std.mem.order(u8, current.key, key);
            if (order == .lt or (!strict and order == .eq)) {
                result = current;
                node = current.children[1];
            } else node = current.children[0];
        }
        return result;
    }

    fn advanceOverlayIndex(_: *const Cursor, node: *PendingTree.Node, direction: Direction) ?*PendingTree.Node {
        return switch (direction) {
            .forward => node.next(),
            .backward => node.prev(),
        };
    }

    fn advanceDisk(self: *Cursor, direction: Direction) !?native.DocumentIndexEntry {
        return switch (direction) {
            .forward => try self.index_cursor.next(),
            .backward => try self.index_cursor.prev(),
        };
    }

    fn keyInRange(self: *const Cursor, full_key: []const u8) bool {
        if (!std.mem.startsWith(u8, full_key, self.txn.prefix)) return false;
        if (self.upper_bound) |upper| {
            if (std.mem.order(u8, full_key[self.txn.prefix.len..], upper) != .lt) return false;
        }
        return true;
    }

    fn installPending(
        self: *Cursor,
        full_key: []const u8,
        value: []const u8,
        buffered_disk: ?native.DocumentIndexEntry,
        direction: Direction,
    ) !backend_adapter.Entry {
        var disk = buffered_disk;
        errdefer if (disk) |*candidate| candidate.deinit(self.txn.allocator);
        const owned_key = try self.txn.allocator.dupe(u8, full_key);
        errdefer self.txn.allocator.free(owned_key);
        const owned_value = try self.txn.allocator.dupe(u8, if (self.load_values) value else "");
        errdefer self.txn.allocator.free(owned_value);
        self.clearCurrent();
        self.current_key = owned_key;
        self.owned_value = owned_value;
        self.disk_candidate = disk;
        disk = null;
        self.last_direction = direction;
        return .{ .key = owned_key[self.txn.prefix.len..], .value = owned_value };
    }

    fn installDisk(self: *Cursor, indexed: native.DocumentIndexEntry, value: []u8, direction: Direction) backend_adapter.Entry {
        self.clearCurrent();
        self.current_key = indexed.key;
        self.owned_value = value;
        self.last_direction = direction;
        return .{ .key = indexed.key[self.txn.prefix.len..], .value = value };
    }

    fn clearCurrent(self: *Cursor) void {
        if (self.current_key) |key| self.txn.allocator.free(key);
        if (self.owned_value) |value| self.txn.allocator.free(value);
        self.current_key = null;
        self.owned_value = null;
    }

    fn clearBufferedDisk(self: *Cursor) void {
        if (self.disk_candidate) |*candidate| candidate.deinit(self.txn.allocator);
        self.disk_candidate = null;
    }

    fn takeBufferedDisk(self: *Cursor) ?native.DocumentIndexEntry {
        const candidate = self.disk_candidate;
        self.disk_candidate = null;
        return candidate;
    }

    fn namespaceUpperBound(self: *Cursor) ![]u8 {
        const upper = try self.txn.allocator.dupe(u8, self.txn.prefix);
        std.debug.assert(upper.len > 0 and upper[upper.len - 1] == 0);
        upper[upper.len - 1] = 1;
        return upper;
    }
};

const replay_hints = [_]change_journal_mod.TargetHint{
    .enrichment,
    .full_text,
    .dense_vector,
    .sparse_vector,
    .graph,
    .algebraic,
    .resolution,
    .promotion,
};

fn replayHintOrdinal(hint: change_journal_mod.TargetHint) u8 {
    return @intCast(@backingInt(hint));
}

fn encodeReplaySequence(sequence: u64) [8]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, sequence, .little);
    return raw;
}

fn isEmbeddingReplayArtifactKey(key: []const u8) bool {
    return internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key);
}

fn appendReplayArtifactsForHint(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    artifact_keys: []const []const u8,
    hint: change_journal_mod.TargetHint,
) !void {
    for (artifact_keys) |key| {
        const keep = switch (hint) {
            .dense_vector, .sparse_vector => isEmbeddingReplayArtifactKey(key),
            .graph => internal_keys.isGraphEdgeArtifactKey(key) or
                internal_keys.isAssetArtifactKey(key) or
                internal_keys.isResolutionArtifactKey(key),
            .resolution => internal_keys.isAssetArtifactKey(key),
            .promotion => internal_keys.isResolutionArtifactKey(key),
            .enrichment, .full_text, .algebraic => false,
        };
        if (keep) try out.append(alloc, key);
    }
}

fn encodeReplayPayloadForHint(
    alloc: Allocator,
    record: change_journal_mod.Record,
    hint: change_journal_mod.TargetHint,
) ![]u8 {
    var target_hints = [_]change_journal_mod.TargetHint{hint};
    var artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer artifact_keys.deinit(alloc);
    try appendReplayArtifactsForHint(alloc, &artifact_keys, record.changed_artifact_keys, hint);

    var filtered = change_journal_mod.Record{
        .version = record.version,
        .sequence = record.sequence,
        .target_hints = target_hints[0..],
    };
    switch (hint) {
        .enrichment => {
            filtered.changed_doc_keys = record.changed_doc_keys;
        },
        .full_text, .algebraic => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
        },
        .dense_vector, .sparse_vector => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .graph, .resolution, .promotion => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
    }
    return try change_journal_mod.encodeRecord(alloc, filtered);
}

fn writeOriginalReplayHintEntries(txn: anytype, sequence: u64, mask: u8, payload: []const u8) !void {
    const latest_raw = encodeReplaySequence(sequence);
    for (replay_hints) |hint| {
        if ((mask & change_journal_mod.singleHintMask(hint)) == 0) continue;
        const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), sequence);
        try txn.put(key[0..], payload);
        const latest_key = internal_keys.replayLatestSequenceKey(replayHintOrdinal(hint));
        try txn.put(latest_key[0..], latest_raw[0..]);
    }
}

fn writeReplayEntries(alloc: Allocator, txn: anytype, sequence: u64, payload: []const u8) !void {
    try txn.put(internal_keys.replay_meta_init_key[0..], "");
    const next_raw = encodeReplaySequence(sequence + 1);
    try txn.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
    const latest_raw = encodeReplaySequence(sequence);

    const all_key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, sequence);
    try txn.put(all_key[0..], payload);
    const all_latest_key = internal_keys.replayLatestSequenceKey(internal_keys.replay_all_kind);
    try txn.put(all_latest_key[0..], latest_raw[0..]);

    const mask = change_journal_mod.encodedRecordHintMask(payload) catch return;
    if (mask == 0) return;

    var decoded = change_journal_mod.decodeRecord(alloc, payload) catch {
        try writeOriginalReplayHintEntries(txn, sequence, mask, payload);
        return;
    };
    defer decoded.deinit();

    for (replay_hints) |hint| {
        if ((mask & change_journal_mod.singleHintMask(hint)) == 0) continue;
        const lane_payload = try encodeReplayPayloadForHint(alloc, decoded.record, hint);
        defer alloc.free(lane_payload);
        const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), sequence);
        try txn.put(key[0..], lane_payload);
        const latest_key = internal_keys.replayLatestSequenceKey(replayHintOrdinal(hint));
        try txn.put(latest_key[0..], latest_raw[0..]);
    }
}

// Both runtime adapters share traversal and ownership rules. Only NotFound
// means exhaustion; failed reads and malformed keys must reach replay workers.
fn forEachReplayLane(
    store: *Store,
    prefix: []const u8,
    kind: u8,
    from_sequence: u64,
    max_entries: usize,
    context: *anyopaque,
    callback: backend_erased.Store.ReplayCallback,
) !backend_types.ReplayLaneIterationStats {
    var read = try Txn.openReadWithPrefix(store, prefix);
    defer read.abort();
    _ = read.get(&internal_keys.replay_meta_init_key) catch |err| switch (err) {
        error.NotFound => return error.ReplayIndexUnavailable,
        else => return err,
    };
    var cursor = try read.openCursor();
    defer cursor.close();
    const lower = internal_keys.replayRangeLower(kind, from_sequence);
    const upper = internal_keys.replayRangeUpper(kind);
    cursor.setUpperBound(&upper);
    var stats = backend_types.ReplayLaneIterationStats{ .scan_batches = 1 };
    var entry = cursor.seekAtOrAfter(&lower) catch |err| switch (err) {
        error.NotFound => return stats,
        else => return err,
    };
    while (true) {
        const sequence = internal_keys.parseReplayEntrySequence(entry.key, kind) orelse return error.InvalidReplayEntryKey;
        try callback(context, sequence, entry.value);
        stats.scanned_entries += 1;
        stats.matched_entries += 1;
        stats.last_sequence = sequence;
        if (max_entries != 0 and stats.matched_entries >= max_entries) return stats;
        entry = cursor.next() catch |err| switch (err) {
            error.NotFound => return stats,
            else => return err,
        };
    }
}

fn collectReplayEntries(store: *Store, prefix: []const u8, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
    const Collector = struct {
        allocator: Allocator,
        entries: std.ArrayListUnmanaged(backend_types.ReplayEntry) = .empty,

        pub fn collect(ptr: *anyopaque, sequence: u64, payload: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Reserve before acquiring the payload: insertion cannot fail
            // once the collector takes ownership of the duplicated bytes.
            try self.entries.ensureUnusedCapacity(self.allocator, 1);
            const owned = try self.allocator.dupe(u8, payload);
            self.entries.appendAssumeCapacity(.{ .sequence = sequence, .payload = owned });
        }
    };
    var collector = Collector{ .allocator = alloc };
    errdefer {
        for (collector.entries.items) |*entry| entry.deinit(alloc);
        collector.entries.deinit(alloc);
    }
    _ = try forEachReplayLane(store, prefix, internal_keys.replay_all_kind, from_sequence, 0, &collector, Collector.collect);
    return try collector.entries.toOwnedSlice(alloc);
}

const replay_cleanup_max_keys = 512;
const replay_cleanup_max_key_bytes = 256 * 1024;

/// Cleanup is resumable: each bounded chunk commits atomically, and any later
/// error is returned to the caller. A retry safely continues the remaining work.
/// Reserve the writer while scanning and deleting, so no concurrent replacement
/// can be deleted using an older read snapshot. Release it between chunks.
fn truncateReplay(store: *Store, prefix: []const u8, yielding: bool, up_to_sequence: u64) !void {
    if (up_to_sequence == 0) return;
    try truncateReplayLane(store, prefix, yielding, internal_keys.replay_all_kind, up_to_sequence);
    for (replay_hints) |hint|
        try truncateReplayLane(store, prefix, yielding, replayHintOrdinal(hint), up_to_sequence);
}

fn truncateReplayLane(store: *Store, prefix: []const u8, yielding: bool, kind: u8, up_to_sequence: u64) !void {
    var resume_sequence: u64 = 0;
    while (resume_sequence < up_to_sequence) {
        var write = if (yielding)
            try Txn.openWriteYieldingWithPrefix(store, prefix)
        else
            try Txn.openWriteWithPrefix(store, prefix);
        var committed = false;
        defer if (!committed) write.abort();
        _ = write.get(internal_keys.replay_meta_init_key[0..]) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        var count: usize = 0;
        var bytes: usize = 0;
        {
            var cursor = try write.openKeyCursor();
            defer cursor.close();
            const lower = internal_keys.replayRangeLower(kind, resume_sequence);
            const upper = internal_keys.replayEntryKey(kind, up_to_sequence);
            cursor.setUpperBound(&upper);
            var key = cursor.seekAtOrAfter(&lower) catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            while (true) {
                const sequence = internal_keys.parseReplayEntrySequence(key, kind) orelse return error.InvalidReplayEntryKey;
                try write.delete(key);
                count += 1;
                bytes += prefix.len + key.len;
                resume_sequence = sequence + 1; // Exclusive bound prevents overflow.
                if (count >= replay_cleanup_max_keys or bytes >= replay_cleanup_max_key_bytes) break;
                key = cursor.next() catch |err| switch (err) {
                    error.NotFound => break,
                    else => return err,
                };
            }
        }
        try write.commit();
        committed = true;
        if (count < replay_cleanup_max_keys and bytes < replay_cleanup_max_key_bytes) return;
    }
}

fn lockStore(store: *Store) void {
    platform_sync.lockYielding(&store.mutex);
}

fn testPath(allocator: Allocator, tmp: native_platform.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native docstore runtime persists atomic batch" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore.aflite");
    defer allocator.free(path);

    {
        var store = try Store.create(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        var batch = try runtime.beginBatch();
        try batch.put("doc:b", "second");
        try batch.put("doc:a", "first");
        try batch.put("doc:b", "newer second");
        try batch.put("doc:c", "deleted");
        try batch.delete("doc:c");
        try batch.commit();
    }

    var reopened = try Store.open(allocator, path, true);
    defer reopened.close();

    var runtime = try reopened.runtimeStore(allocator);
    defer runtime.deinit();

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("first", try read.get("doc:a"));
    try std.testing.expectEqualStrings("newer second", try read.get("doc:b"));
    try std.testing.expectError(error.NotFound, read.get("doc:c"));
}

test "lite native docstore prefixed runtimes isolate keys cursors and replay" {
    const allocator = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-prefixed.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };
    var runtime_a = try store.runtimeStoreWithPrefix(allocator, &prefix_a);
    defer runtime_a.deinit();
    var runtime_b = try store.runtimeStoreWithPrefix(allocator, &prefix_b);
    defer runtime_b.deinit();

    var write_a = try runtime_a.beginBatch();
    try write_a.put("doc:same", "a");
    try write_a.put("doc:only-a", "a-only");
    try write_a.commit();
    var write_b = try runtime_b.beginBatch();
    try write_b.put("doc:same", "b");
    try write_b.commit();

    var read_a = try runtime_a.beginRead();
    defer read_a.abort();
    try std.testing.expectEqualStrings("a", try read_a.get("doc:same"));
    var cursor_a = try read_a.openCursor();
    defer cursor_a.close();
    try std.testing.expectEqualStrings("doc:only-a", (try cursor_a.first()).?.key);
    try std.testing.expectEqualStrings("doc:same", (try cursor_a.next()).?.key);
    try std.testing.expect((try cursor_a.next()) == null);

    var read_b = try runtime_b.beginRead();
    defer read_b.abort();
    try std.testing.expectEqualStrings("b", try read_b.get("doc:same"));
    try std.testing.expectError(error.NotFound, read_b.get("doc:only-a"));

    try runtime_a.appendReplayOpaque(allocator, 7, "a-replay");
    try runtime_b.appendReplayOpaque(allocator, 2, "b-replay");
    try std.testing.expectEqual(@as(u64, 8), runtime_a.nextReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 3), runtime_b.nextReplaySequence(0));
}

test "lite native docstore keeps disjoint namespace cursors isolated without snapshots" {
    const allocator = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-namespace-cache.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };

    var write_a = try Txn.openWriteYieldingWithPrefix(&store, &prefix_a);
    try write_a.put("doc:a", "a1");
    try write_a.commit();
    var write_b = try Txn.openWriteYieldingWithPrefix(&store, &prefix_b);
    try write_b.put("doc:b", "b1");
    try write_b.commit();

    var read_b = try Txn.openReadWithPrefix(&store, &prefix_b);
    var cursor_b = try read_b.openCursor();
    try std.testing.expectEqualStrings("doc:b", (try cursor_b.first()).key);
    cursor_b.close();
    read_b.abort();

    write_a = try Txn.openWriteYieldingWithPrefix(&store, &prefix_a);
    try write_a.put("doc:a", "a2");
    try write_a.commit();

    read_b = try Txn.openReadWithPrefix(&store, &prefix_b);
    defer read_b.abort();
    cursor_b = try read_b.openCursor();
    defer cursor_b.close();
    try std.testing.expectEqualStrings("doc:b", (try cursor_b.first()).key);
    try std.testing.expectEqualStrings("b1", try read_b.get("doc:b"));
}

test "lite native docstore runtime scans ordered snapshot" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-scan.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    var runtime = try store.runtimeStore(allocator);
    defer runtime.deinit();

    {
        var batch = try runtime.beginBatch();
        try batch.put("doc:b", "second");
        try batch.put("doc:a", "first");
        try batch.put("doc:c", "third");
        try batch.commit();
    }

    var read = try runtime.beginRead();
    defer read.abort();

    var cursor = try read.openCursor();
    defer cursor.close();
    const first = (try cursor.first()).?;
    try std.testing.expectEqualStrings("doc:a", first.key);
    const next = (try cursor.next()).?;
    try std.testing.expectEqualStrings("doc:b", next.key);
    const seek = (try cursor.seekAtOrAfter("doc:bb")).?;
    try std.testing.expectEqualStrings("doc:c", seek.key);
}

test "lite native write cursors merge pending writes and deletes in order" {
    const allocator = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-write-cursor.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    var seed = try Txn.openWrite(&store);
    try seed.put("doc:a", "disk-a");
    try seed.put("doc:c", "disk-c");
    try seed.put("doc:e", "disk-e");
    try seed.commit();

    var write = try Txn.openWrite(&store);
    defer write.abort();
    try write.put("doc:b", "pending-b");
    try write.put("doc:c", "pending-c-old");
    try write.put("doc:c", "pending-c");
    try write.delete("doc:e");
    try write.put("doc:d", "pending-d-old");
    try write.delete("doc:d");
    try write.put("doc:d", "pending-d");

    var cursor = try write.openCursor();
    defer cursor.close();
    var entry = try cursor.first();
    try std.testing.expectEqualStrings("doc:a", entry.key);
    try std.testing.expectEqualStrings("disk-a", entry.value);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:b", entry.key);
    try std.testing.expectEqualStrings("pending-b", entry.value);
    entry = try cursor.prev();
    try std.testing.expectEqualStrings("doc:a", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:b", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:c", entry.key);
    try std.testing.expectEqualStrings("pending-c", entry.value);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:d", entry.key);
    try std.testing.expectEqualStrings("pending-d", entry.value);
    try std.testing.expectError(error.NotFound, cursor.next());

    entry = try cursor.last();
    try std.testing.expectEqualStrings("doc:d", entry.key);
    entry = try cursor.prev();
    try std.testing.expectEqualStrings("doc:c", entry.key);
    entry = try cursor.seekAtOrAfter("doc:bb");
    try std.testing.expectEqualStrings("doc:c", entry.key);
    entry = try cursor.seekAtOrBefore("doc:bb");
    try std.testing.expectEqualStrings("doc:b", entry.key);
    cursor.setUpperBound("doc:d");
    entry = try cursor.seekAtOrAfter("doc:c");
    try std.testing.expectEqualStrings("doc:c", entry.key);
    try std.testing.expectError(error.NotFound, cursor.next());
    entry = try cursor.last();
    try std.testing.expectEqualStrings("doc:c", entry.key);
    entry = try cursor.seekAtOrBefore("doc:z");
    try std.testing.expectEqualStrings("doc:c", entry.key);
    cursor.setUpperBound(null);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:d", entry.key);

    // A cursor opened before later mutations refreshes only its small sorted
    // overlay; it does not rematerialize the durable namespace.
    try write.put("doc:aa", "pending-aa");
    try write.delete("doc:c");
    entry = try cursor.seekAtOrAfter("doc:a");
    try std.testing.expectEqualStrings("doc:a", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:aa", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:b", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:d", entry.key);
    try std.testing.expectError(error.NotFound, cursor.next());
}

test "lite native docstore persists replay lanes across reopen and truncation" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-replay.aflite");
    defer allocator.free(path);

    const changed_doc_keys = [_][]const u8{"doc:a"};
    const deleted_doc_keys = [_][]const u8{"doc:gone"};
    const hints = [_]change_journal_mod.TargetHint{ .full_text, .dense_vector };
    const payload = try change_journal_mod.encodeRecord(allocator, .{
        .sequence = 1,
        .changed_doc_keys = changed_doc_keys[0..],
        .deleted_doc_keys = deleted_doc_keys[0..],
        .target_hints = hints[0..],
    });
    defer allocator.free(payload);

    {
        var store = try Store.create(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        try runtime.appendReplayOpaque(allocator, 1, payload);
        try std.testing.expectEqual(@as(u64, 1), runtime.lastReplaySequence(0));
        try std.testing.expectEqual(@as(u64, 2), runtime.nextReplaySequence(0));
    }

    {
        var store = try Store.open(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        const entries = try runtime.iterateReplayFrom(allocator, 1);
        defer {
            for (entries) |*entry| entry.deinit(allocator);
            allocator.free(entries);
        }
        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqual(@as(u64, 1), entries[0].sequence);
        try std.testing.expectEqualSlices(u8, payload, entries[0].payload);

        const LaneContext = struct {
            allocator: Allocator,
            expected_hint: change_journal_mod.TargetHint,
            count: usize = 0,

            fn handle(ctx: *@This(), sequence: u64, lane_payload: []const u8) !void {
                try std.testing.expectEqual(@as(u64, 1), sequence);
                var decoded = try change_journal_mod.decodeRecord(ctx.allocator, lane_payload);
                defer decoded.deinit();
                try std.testing.expectEqual(@as(usize, 1), decoded.record.target_hints.len);
                try std.testing.expectEqual(ctx.expected_hint, decoded.record.target_hints[0]);
                ctx.count += 1;
            }
        };

        var full_text_ctx = LaneContext{ .allocator = allocator, .expected_hint = .full_text };
        const full_text_stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.full_text), 1, 0, &full_text_ctx, LaneContext.handle);
        try std.testing.expectEqual(@as(usize, 1), full_text_ctx.count);
        try std.testing.expectEqual(@as(u64, 1), full_text_stats.last_sequence);

        var dense_ctx = LaneContext{ .allocator = allocator, .expected_hint = .dense_vector };
        const dense_stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.dense_vector), 1, 1, &dense_ctx, LaneContext.handle);
        try std.testing.expectEqual(@as(usize, 1), dense_ctx.count);
        try std.testing.expectEqual(@as(u64, 1), dense_stats.last_sequence);
    }

    {
        var store = try Store.open(allocator, path, false);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        try runtime.truncateReplayUpTo(allocator, 2);

        const entries = try runtime.iterateReplayFrom(allocator, 1);
        defer {
            for (entries) |*entry| entry.deinit(allocator);
            allocator.free(entries);
        }
        try std.testing.expectEqual(@as(usize, 0), entries.len);

        const EmptyContext = struct {
            fn handle(_: *@This(), _: u64, _: []const u8) !void {
                return error.UnexpectedReplayRecord;
            }
        };
        var empty_ctx = EmptyContext{};
        const stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.full_text), 1, 0, &empty_ctx, EmptyContext.handle);
        try std.testing.expectEqual(@as(u64, 0), stats.matched_entries);
    }
}

test "lite native docstore reserves one writer until abort or commit" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-single-writer.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    var writer = try store.beginWrite();
    try writer.put("doc:a", "first");
    try std.testing.expectError(error.FileBusy, store.beginWrite());
    try std.testing.expectError(error.FileBusy, store.vacuum());

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get("doc:a"));

    writer.abort();

    var committed = try store.beginWrite();
    try committed.put("doc:a", "committed");
    try committed.commit();

    var next_writer = try store.beginWrite();
    defer next_writer.abort();
    try std.testing.expectEqualStrings("committed", try next_writer.get("doc:a"));
}

fn expectIndexCursorMatchesDiskRebuild(store: *Store) !void {
    const allocator = std.testing.allocator;
    var read = try store.beginRead();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();

    const rebuilt = try store.file.snapshotDocumentsAlloc(allocator);
    defer native.NativeFile.freeSnapshotDocuments(allocator, rebuilt);
    if (rebuilt.len == 0) {
        try std.testing.expectError(error.NotFound, cursor.first());
    } else for (rebuilt, 0..) |expected, i| {
        const actual = if (i == 0) try cursor.first() else try cursor.next();
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqualStrings(expected.value, actual.value);
    }
    if (rebuilt.len > 0) try std.testing.expectError(error.NotFound, cursor.next());
}

test "lite native docstore disk index matches rebuild across mixed commits" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-applied-snapshot.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:b", "b1");
        try batch.put("doc:a", "a1");
        try batch.put("doc:c", "c1");
        try batch.put("doc:b", "b2-last-wins");
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    {
        var batch = try store.beginWrite();
        try batch.delete("doc:c");
        try batch.delete("doc:never-existed");
        try batch.put("doc:d", "d1");
        try batch.put("doc:a", "a2");
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    {
        // Put-then-delete and delete-then-put of the same key in one batch.
        var batch = try store.beginWrite();
        try batch.put("doc:e", "e1");
        try batch.delete("doc:e");
        try batch.delete("doc:d");
        try batch.put("doc:d", "d2-resurrected");
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    // A large value that spills to external value pages.
    {
        const big = try allocator.alloc(u8, 3 * native.default_page_size);
        defer allocator.free(big);
        @memset(big, 'x');
        var batch = try store.beginWrite();
        try batch.put("doc:big", big);
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    // Empty commit publishes nothing and keeps the cache current.
    {
        var batch = try store.beginWrite();
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("a2", try read.get("doc:a"));
    try std.testing.expectEqualStrings("b2-last-wins", try read.get("doc:b"));
    try std.testing.expectError(error.NotFound, read.get("doc:c"));
    try std.testing.expectEqualStrings("d2-resurrected", try read.get("doc:d"));
    try std.testing.expectError(error.NotFound, read.get("doc:e"));
}

test "lite native docstore disk cursor loads values lazily" {
    const allocator = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-shared-payloads.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    {
        var batch = try store.beginWrite();
        errdefer batch.abort();
        try batch.put("doc:a", "a-value-that-must-not-be-copied");
        try batch.put("doc:b", "b-v1");
        try batch.commit();
    }
    {
        var read = try store.beginRead();
        var cursor = try read.openCursor();
        cursor.close();
        read.abort();
    }
    {
        var batch = try store.beginWrite();
        errdefer batch.abort();
        try batch.put("doc:b", "b-v2");
        try batch.commit();
    }
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("a-value-that-must-not-be-copied", try read.get("doc:a"));
    var cursor = try read.openCursor();
    defer cursor.close();
    try std.testing.expectEqualStrings("doc:a", (try cursor.first()).key);
    try std.testing.expectEqualStrings("a-value-that-must-not-be-copied", (try cursor.first()).value);
}

test "lite native docstore read transactions pin their snapshot across commits" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-pinned-snapshot.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:pin", "v1");
        try batch.commit();
    }

    var pinned = try store.beginRead();
    defer pinned.abort();
    try std.testing.expectEqualStrings("v1", try pinned.get("doc:pin"));

    {
        var batch = try store.beginWrite();
        try batch.put("doc:pin", "v2");
        try batch.put("doc:new", "n1");
        try batch.commit();
    }

    // The pinned reader still sees its snapshot; a fresh reader sees the
    // committed state.
    try std.testing.expectEqualStrings("v1", try pinned.get("doc:pin"));
    try std.testing.expectError(error.NotFound, pinned.get("doc:new"));

    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings("v2", try fresh.get("doc:pin"));
    try std.testing.expectEqualStrings("n1", try fresh.get("doc:new"));
}

test "lite native docstore disk index survives out-of-band catalog commits and vacuum" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-cache-oob.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:oob", "v1");
        try batch.commit();
    }

    // A catalog commit bumps the checkpoint without touching documents; the
    // next read must key-miss, rebuild, and still see identical content.
    try store.file.putCatalogRecord("catalog:key", "catalog-value");
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("v1", try read.get("doc:oob"));
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    // Update churn then vacuum: the file is rewritten in place and the cache
    // key changes with the vacuum checkpoint.
    var round: usize = 0;
    while (round < 10) : (round += 1) {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "churn-{d}", .{round});
        var batch = try store.beginWrite();
        try batch.put("doc:oob", value);
        try batch.commit();
    }
    _ = try store.vacuum();
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("churn-9", try read.get("doc:oob"));
    }
    try expectIndexCursorMatchesDiskRebuild(&store);
}

test "lite native docstore cold writes and large disk-index cursors stay bounded" {
    const allocator = std.testing.allocator;

    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-lazy-writes.aflite");
    defer allocator.free(path);

    var store = try Store.createWithOptions(allocator, path, .{ .no_sync = true });
    defer store.close();

    var seed = try store.beginWrite();
    var key_buffer: [32]u8 = undefined;
    var i: usize = 0;
    while (i < bounded_cursor_test_documents) : (i += 1) {
        const key = try std.fmt.bufPrint(&key_buffer, "doc:{d:0>5}", .{i});
        try seed.put(key, "v1");
    }
    try seed.commit();

    // Point reads and ordered cursors both use the disk-resident index.
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("v1", try read.get("doc:00000"));
        var cursor = try read.openCursor();
        defer cursor.close();
        var count: usize = 1;
        try std.testing.expectEqualStrings("doc:00000", (try cursor.first()).key);
        while (true) {
            _ = cursor.next() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
            count += 1;
        }
        try std.testing.expectEqual(bounded_cursor_test_documents, count);
        try std.testing.expectEqualStrings("doc:00511", (try cursor.last()).key);
        var reverse_count: usize = 1;
        while (true) {
            _ = cursor.prev() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
            reverse_count += 1;
        }
        try std.testing.expectEqual(bounded_cursor_test_documents, reverse_count);
        try std.testing.expectEqualStrings("doc:00256", (try cursor.seekAtOrAfter("doc:00256")).key);
    }

    // Publishing a small write does not rebuild or retain a table-sized cache.
    var update = try store.beginWrite();
    try update.put("doc:00000", "v2");
    try update.commit();

    var verify = try store.beginRead();
    defer verify.abort();
    try std.testing.expectEqualStrings("v2", try verify.get("doc:00000"));
}

test "lite transaction indexed overlay preserves borrowed versions and sorted multi reads" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "indexed-overlay.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    var seed = try store.beginWrite();
    try seed.put("a", "disk-a");
    try seed.put("c", "disk-c");
    try seed.commit();
    var txn = try store.beginWrite();
    errdefer txn.abort();
    try txn.put("a", "first");
    const borrowed = try txn.get("a");
    var cursor = try txn.openCursor();
    {
        defer cursor.close();
        for (0..4096) |i| {
            var buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&buf, "b{d:0>6}", .{i});
            try std.testing.expectError(error.NotFound, txn.get(key));
            try txn.put(key, "value");
            const entry = try cursor.seekAtOrAfter(key);
            try std.testing.expectEqualStrings(key, entry.key);
        }
        try txn.delete("c");
        try txn.put("a", "final");
        try std.testing.expectEqualStrings("first", borrowed);
        const keys = [_][]const u8{ "a", "a", "b000012", "c", "missing" };
        var values: [keys.len]?[]const u8 = undefined;
        try txn.getManySorted(&keys, &values);
        try std.testing.expectEqualStrings("final", values[0].?);
        try std.testing.expectEqualStrings("final", values[1].?);
        try std.testing.expectEqualStrings("value", values[2].?);
        try std.testing.expect(values[3] == null and values[4] == null);
    }
    try std.testing.expectEqual(@as(usize, 4098), txn.pending_count);
    try txn.commit();
    var read = try store.beginRead();
    defer read.abort();
    const keys = [_][]const u8{ "a", "b000000", "b000001", "b000002", "b004095", "c", "missing" };
    var values: [keys.len]?[]const u8 = undefined;
    try read.getManySorted(&keys, &values);
    try std.testing.expectEqualStrings("final", values[0].?);
    for (values[1..5]) |value| try std.testing.expectEqualStrings("value", value.?);
    try std.testing.expect(values[5] == null and values[6] == null);
}

test "lite online vacuum retires generations without waiting for pinned readers" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reader-generation-vacuum.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    var write = try store.beginWrite();
    try write.put("doc", "old");
    try write.commit();
    var pinned = try store.beginRead();
    defer pinned.abort();
    write = try store.beginWrite();
    try write.put("doc", "new");
    try write.commit();
    _ = try store.vacuum();
    try std.testing.expect(pinned.read_generation.?.retired);
    try std.testing.expectEqualStrings("old", try pinned.get("doc"));
    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings("new", try fresh.get("doc"));
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite group commit hands leadership to a bounded queued group" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "queued-group-commit.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io, .reclamation = .{ .page_reuse = false } });
    defer store.close();
    const Gate = struct {
        started: std.atomic.Value(bool) = .init(false),
        proceed: std.atomic.Value(bool) = .init(false),
    };
    const Worker = struct {
        store: *Store,
        gate: *Gate,
        id: usize,
        result: anyerror!void = {},
        fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.id == 0) {
                self.gate.started.store(true, .release);
                while (!self.gate.proceed.load(.acquire)) native_platform.time.yieldNow();
            }
            var buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&buf, "queued-{d}", .{self.id});
            try file.putIndexCatalogRecord(key, "committed");
        }
        fn run(self: *@This()) void {
            self.result = self.store.submitMutation(self, apply);
        }
    };
    var gate = Gate{};
    var workers: [9]Worker = undefined;
    var threads: [9]std.Thread = undefined;
    var spawned: usize = 0;
    defer {
        gate.proceed.store(true, .release);
        for (threads[0..spawned]) |thread| thread.join();
    }
    for (&workers, 0..) |*worker, i| {
        worker.* = .{ .store = &store, .gate = &gate, .id = i };
        threads[i] = try std.Thread.spawn(.{}, Worker.run, .{worker});
        spawned += 1;
        if (i == 0) while (!gate.started.load(.acquire)) native_platform.time.yieldNow();
    }
    while (true) {
        store.commit_mutex.lockUncancelable(native_platform.testing.io);
        var queued: usize = 0;
        var item = store.commit_head;
        while (item) |request| {
            queued += 1;
            item = request.next;
        }
        store.commit_mutex.unlock(native_platform.testing.io);
        if (queued == 8) break;
        native_platform.time.yieldNow();
    }
    gate.proceed.store(true, .release);
    for (threads[0..spawned]) |thread| thread.join();
    spawned = 0;
    for (workers) |worker| try worker.result;
    try std.testing.expectEqual(@as(u64, 2), store.file.activeCheckpoint().commit_sequence);
    for (0..workers.len) |i| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "queued-{d}", .{i});
        const value = (try store.file.getIndexCatalogRecordAlloc(alloc, key)).?;
        defer alloc.free(value);
        try std.testing.expectEqualStrings("committed", value);
    }
}

test "lite failed commit group discards every root and allows a clean retry" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "failed-commit-group.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    const Context = struct {
        fail: bool,
        fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.InjectedGroupFailure;
            try file.putDocument("doc", "unpublished");
            try file.putIndexCatalogRecord("index", "unpublished");
        }
    };
    var good = Context{ .fail = false };
    var bad = Context{ .fail = true };
    var second = MutationRequest{ .context = &bad, .apply = Context.apply };
    var first = MutationRequest{ .context = &good, .apply = Context.apply, .next = &second };
    try std.testing.expectError(error.InjectedGroupFailure, store.applyMutationGroup(&first));
    try std.testing.expect((try store.file.getDocumentAlloc(alloc, "doc")) == null);
    try std.testing.expect((try store.file.getIndexCatalogRecordAlloc(alloc, "index")) == null);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
    try store.submitMutation(&good, Context.apply);
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("unpublished", try read.get("doc"));
    }
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite online vacuum publishes while group-commit mutations keep landing" {
    // The disk index publishes its catalog record through the group-commit
    // queue without ever taking the writer slot, so a vacuum that fenced
    // its final catch-up with the slot alone could see a new change land
    // after every catch-up and give up busy. Keep such mutations landing for
    // the whole vacuum and require it to publish with all of them intact.
    const Hammer = struct {
        store: *Store,
        stop: std.atomic.Value(bool) = .init(false),
        submitted: std.atomic.Value(usize) = .init(0),
        result: anyerror!void = {},

        fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "oob:{d}", .{self.submitted.load(.monotonic)});
            try file.putDocumentBatch(&.{.{ .key = key, .value = "landed", .is_delete = false }});
        }

        fn run(self: *@This()) void {
            while (!self.stop.load(.acquire)) {
                self.store.submitMutation(self, apply) catch |err| {
                    self.result = err;
                    return;
                };
                _ = self.submitted.fetchAdd(1, .monotonic);
            }
        }
    };
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "vacuum-under-group-commits.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{});
    defer store.close();
    {
        var write = try store.beginWrite();
        try write.put("doc", "seed");
        try write.commit();
    }
    var hammer = Hammer{ .store = &store };
    const thread = try std.Thread.spawn(.{}, Hammer.run, .{&hammer});
    var joined = false;
    defer {
        hammer.stop.store(true, .release);
        if (!joined) thread.join();
    }
    while (hammer.submitted.load(.acquire) < 4) native_platform.time.yieldNow();
    _ = try store.vacuum();
    hammer.stop.store(true, .release);
    thread.join();
    joined = true;
    try hammer.result;
    const landed = hammer.submitted.load(.acquire);
    try std.testing.expect(landed >= 4);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("seed", try read.get("doc"));
    var key_buf: [32]u8 = undefined;
    for (0..landed) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "oob:{d}", .{i});
        try std.testing.expectEqualStrings("landed", try read.get(key));
    }
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite online vacuum catches foreground commits while its copy is blocked" {
    const Gate = struct {
        var live_handle: std.Io.File.Handle = undefined;
        var armed: std.atomic.Value(bool) = .init(false);
        var started: std.atomic.Value(bool) = .init(false);
        var proceed: std.atomic.Value(bool) = .init(false);
        pub fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            if (armed.load(.acquire) and file.handle != live_handle and armed.swap(false, .acq_rel)) {
                started.store(true, .release);
                while (!proceed.load(.acquire)) native_platform.time.yieldNow();
            }
            return native_platform.debug_io.vtable.fileSync(userdata, file);
        }
    };
    const Worker = struct {
        store: *Store,
        result: anyerror!void = {},
        fn run(self: *@This()) void {
            _ = self.store.vacuum() catch |err| {
                self.result = err;
                return;
            };
        }
    };
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "foreground-vacuum.aflite");
    defer alloc.free(path);
    var vtable = native_platform.debug_io.vtable.*;
    vtable.fileSync = Gate.sync;
    const io = std.Io{ .userdata = native_platform.debug_io.userdata, .vtable = &vtable };
    const storage_limit = 128 * 4096;
    var store = try Store.createWithOptions(alloc, path, .{ .io = io, .reclamation = .{
        .enabled = false,
        .assessment_bytes = 16 * 4096,
        .max_storage_bytes = storage_limit,
    } });
    defer store.close();
    Gate.live_handle = store.file.file.handle;
    var write = try store.beginWrite();
    try write.put("doc", "before");
    try write.commit();
    var reader = try store.beginRead();
    defer reader.abort();
    var worker = Worker{ .store = &store };
    Gate.started.store(false, .release);
    Gate.proceed.store(false, .release);
    Gate.armed.store(true, .release);
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    var joined = false;
    defer {
        Gate.proceed.store(true, .release);
        if (!joined) thread.join();
        Gate.armed.store(false, .release);
    }
    while (!Gate.started.load(.acquire)) native_platform.time.yieldNow();
    // A paused rewrite reserves capacity before its first publication. A
    // large foreground mutation must leave that workspace available and
    // preserve the committed roots when admission rejects its tail pages.
    const reservation = (try store.reclamationStatus()).temporary_bytes;
    try std.testing.expect(reservation > 0);
    const oversized: [128 * 4096]u8 = @splat('x');
    write = try store.beginWrite();
    try write.put("oversized", &oversized);
    try std.testing.expectError(error.LiteStorageBudgetExceeded, write.commit());
    write.abort();
    try std.testing.expect((try store.reclamationStatus()).current_file_bytes <= storage_limit - reservation);
    try std.testing.expectError(error.FileBusy, store.vacuum());
    write = try store.beginWrite();
    try write.put("doc", "during copy");
    try write.put("new", "also during copy");
    try write.commit();
    Gate.proceed.store(true, .release);
    thread.join();
    joined = true;
    try worker.result;
    try std.testing.expectEqualStrings("before", try reader.get("doc"));
    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings("during copy", try fresh.get("doc"));
    try std.testing.expectEqualStrings("also during copy", try fresh.get("new"));
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite grouped durability failures recover all roots at one checkpoint" {
    const Fault = struct {
        var remaining: usize = 0;
        pub fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            if (remaining != 0) {
                remaining -= 1;
                if (remaining == 0) return error.InputOutput;
            }
            return native_platform.debug_io.vtable.fileSync(userdata, file);
        }
        fn apply(_: *anyopaque, file: *native.NativeFile) !void {
            try file.putDocument("doc", "after");
            try file.putIndexCatalogRecord("index", "after");
            try file.putCatalogRecord("meta", "after");
        }
    };
    const alloc = std.testing.allocator;
    var vtable = native_platform.debug_io.vtable.*;
    vtable.fileSync = Fault.sync;
    const io = std.Io{ .userdata = native_platform.debug_io.userdata, .vtable = &vtable };
    for (1..4) |barrier| {
        var tmp = native_platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(alloc, tmp, "group-sync-failure.aflite");
        defer alloc.free(path);
        {
            var store = try Store.createWithOptions(alloc, path, .{ .io = io });
            defer store.close();
            try store.file.putDocument("doc", "before");
            try store.file.putIndexCatalogRecord("index", "before");
            try store.file.putCatalogRecord("meta", "before");
            Fault.remaining = barrier;
            defer Fault.remaining = 0;
            var context: u8 = 0;
            try std.testing.expectError(error.InputOutput, store.submitMutation(&context, Fault.apply));
            try std.testing.expectEqual(@as(usize, 0), Fault.remaining);
            if (barrier > 1) try std.testing.expectError(error.OutcomeUnknown, store.submitMutation(&context, Fault.apply));
        }
        var reopened = try Store.open(alloc, path, true);
        defer reopened.close();
        const doc = (try reopened.file.getDocumentAlloc(alloc, "doc")).?;
        defer alloc.free(doc);
        const index = (try reopened.file.getIndexCatalogRecordAlloc(alloc, "index")).?;
        defer alloc.free(index);
        const meta = (try reopened.file.getCatalogRecordAlloc(alloc, "meta")).?;
        defer alloc.free(meta);
        try std.testing.expect(std.mem.eql(u8, doc, "before") or std.mem.eql(u8, doc, "after"));
        try std.testing.expectEqualStrings(doc, index);
        try std.testing.expectEqualStrings(doc, meta);
        try std.testing.expect((try reopened.file.check()).valid);
    }
}

test "lite packed document cursors read each physical bundle once per scan" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "packed-cursor.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    var write = try store.beginWrite();
    var buffer: [32]u8 = undefined;
    const count = 2048;
    for (0..count) |i| try write.put(try std.fmt.bufPrint(&buffer, "key-{d:0>8}", .{i}), "payload");
    try write.commit();
    var txn = try store.beginRead();
    defer txn.abort();
    const file = try txn.readFile();
    var cursor = try txn.openCursor();
    defer cursor.close();
    for ([_]bool{ false, true }) |reverse| {
        const before = file.test_page_reads.load(.monotonic);
        var entry = if (reverse) try cursor.last() else try cursor.first();
        for (0..count) |i| {
            const key = try std.fmt.bufPrint(&buffer, "key-{d:0>8}", .{if (reverse) count - 1 - i else i});
            try std.testing.expectEqualStrings(key, entry.key);
            try std.testing.expectEqualStrings("payload", entry.value);
            if (i + 1 < count) entry = if (reverse) try cursor.prev() else try cursor.next();
        }
        if (reverse) try std.testing.expectError(error.NotFound, cursor.prev()) else try std.testing.expectError(error.NotFound, cursor.next());
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before < count / 8);
    }
    try std.testing.expectEqualStrings("key-00001000", (try cursor.seekAtOrAfter("key-00001000")).key);
    try std.testing.expectEqualStrings("key-00000999", (try cursor.prev()).key);
    try std.testing.expectEqualStrings("key-00001000", (try cursor.next()).key);
}

test "lite maintenance snapshots release shared cache accounting on cancellation and publication" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "maintenance-budget.aflite");
    defer alloc.free(path);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 32768, .hard_limit_bytes = 65536 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    {
        var store = try Store.createWithOptions(alloc, path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = native_platform.testing.io, .resource_manager = &manager });
        defer store.close();
        var write = try store.beginWrite();
        var buffer: [32]u8 = undefined;
        for (0..2048) |i| try write.put(try std.fmt.bufPrint(&buffer, "key-{d:0>8}", .{i}), "payload");
        try write.commit();
        var cancel = @import("../maintenance.zig").CancelToken{};
        cancel.request();
        try std.testing.expectError(error.MaintenanceCanceled, store.vacuumWithCancel(&cancel));
        try std.testing.expectError(error.MaintenanceCanceled, store.checkWithCancel(&cancel));
        try std.testing.expect((try store.checkWithCancel(null)).valid);
        _ = try store.vacuum();
        try std.testing.expectEqual(store.file.page_cache.total_bytes, manager.sliceStats(.lite_native_page_cache).used_bytes);
        try std.testing.expect(manager.sliceStats(.lite_native_page_cache).used_bytes <= 65536);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_page_cache).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_native_link_cache).used_bytes);
}

test "lite key-only cursor preserves prefix overlay bounds and pinned snapshots" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "key-cursor.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        for ([_][]const u8{ "a\x00a", "a\x00c", "a\x00e", "b\x00b" }) |key| try write.put(key, "value");
        try write.commit();
    }
    var pinned = try Txn.openReadWithPrefix(&store, "a\x00");
    defer pinned.abort();
    {
        var write = try Txn.openWriteWithPrefix(&store, "a\x00");
        errdefer write.abort();
        try write.delete("a");
        try write.put("b", "new");
        try write.put("c", "updated");
        var cursor = try write.openKeyCursor();
        {
            defer cursor.close();
            cursor.setUpperBound("e");
            try std.testing.expectEqualStrings("b", try cursor.first());
            try std.testing.expectEqualStrings("c", try cursor.next());
            try std.testing.expectEqualStrings("b", try cursor.prev());
            try std.testing.expectEqualStrings("c", try cursor.last());
            try std.testing.expectError(error.NotFound, cursor.next());
            try std.testing.expectEqualStrings("b", try cursor.seekAtOrAfter("a"));
            try std.testing.expectEqualStrings("c", try cursor.seekAtOrBefore("d"));
            cursor.setUpperBound(null);
            try std.testing.expectEqualStrings("e", try cursor.last());
            try std.testing.expectEqualStrings("c", try cursor.prev());
        }
        try write.commit();
    }
    var cursor = try pinned.openKeyCursor();
    defer cursor.close();
    try std.testing.expectEqualStrings("a", try cursor.first());
    try std.testing.expectEqualStrings("c", try cursor.next());
    try std.testing.expectEqualStrings("e", try cursor.next());
    try std.testing.expectError(error.NotFound, cursor.next());
    try std.testing.expectEqualStrings("value", try pinned.get("a"));
}

test "lite replay cleanup avoids external values and propagates allocation errors" {
    const a = std.testing.allocator;
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-key-only.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(budget.allocator(), path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    store.file.page_cache_enabled.store(false, .monotonic);
    const value = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(value);
    @memset(value, 'x');
    const key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 1);
    try store.file.putDocumentBatch(&.{
        .{ .key = &internal_keys.replay_meta_init_key, .value = "" },
        .{ .key = &key, .value = value },
    });
    budget.limit = budget.live;
    try std.testing.expectError(error.OutOfMemory, store.truncateReplayUpTo(a, 2));
    budget.limit = std.math.maxInt(usize);
    // A failed cleanup must release the writer and retain the entry for retry.
    const checkpoint = store.file.activeCheckpoint();
    budget.peak = budget.live;
    const baseline = budget.live;
    const reads = store.file.test_page_reads.load(.monotonic);
    budget.limit = baseline + 1024 * 1024;
    try store.truncateReplayUpTo(a, 2);
    budget.limit = std.math.maxInt(usize);
    try std.testing.expect(budget.peak - baseline < 1024 * 1024);
    try std.testing.expect(store.file.test_page_reads.load(.monotonic) - reads < 64);
    try std.testing.expect((try store.file.getDocumentAlloc(a, &key)) == null);
    // Removal from the new root leaves the previous value intact.
    const old = (try store.file.getDocumentAtCheckpointAlloc(a, checkpoint, &key)).?;
    defer a.free(old);
    try std.testing.expectEqualSlices(u8, value, old);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite replay cleanup commits bounded chunks isolates namespaces and retries" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-chunks.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    const count = replay_cleanup_max_keys * 2 + 3;
    const kind = internal_keys.replay_all_kind;
    // Include a malformed key in chunk two to prove chunk one is durable,
    // chunk two aborts, and a retry finishes after the bad key is repaired.
    const bad_base = internal_keys.replayEntryKey(kind, replay_cleanup_max_keys + 2);
    const bad_key = bad_base ++ "bad";
    {
        var write = try Txn.openWriteWithPrefix(&store, "a\x00");
        errdefer write.abort();
        try write.put(&internal_keys.replay_meta_init_key, "");
        for (0..count + 1) |i| {
            const key = internal_keys.replayEntryKey(kind, i);
            try write.put(&key, "payload");
        }
        for (replay_hints) |hint| {
            const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), 1);
            try write.put(&key, "lane");
        }
        try write.put(bad_key, "malformed");
        try write.commit();
    }
    var other = RuntimeStore{ .store = &store, .prefix = "b\x00" };
    try other.appendReplayOpaque(a, 1, "other");
    var scoped = RuntimeStore{ .store = &store, .prefix = "a\x00" };
    try std.testing.expectError(error.InvalidReplayEntryKey, scoped.truncateReplayUpTo(a, count));
    {
        var read = try scoped.beginRead();
        defer read.abort();
        const first = internal_keys.replayEntryKey(kind, 0);
        const second_chunk = internal_keys.replayEntryKey(kind, replay_cleanup_max_keys);
        try std.testing.expectError(error.NotFound, read.get(&first));
        try std.testing.expectEqualStrings("payload", try read.get(&second_chunk));
    }
    {
        var write = try scoped.beginWrite();
        errdefer write.abort();
        try write.delete(bad_key);
        try write.commit();
    }
    try scoped.truncateReplayUpTo(a, count);
    {
        var read = try scoped.beginRead();
        defer read.abort();
        var cursor = try read.openKeyCursor();
        defer cursor.close();
        const first = internal_keys.replayEntryKey(kind, 0);
        const remaining = internal_keys.replayEntryKey(kind, count);
        try std.testing.expectEqualSlices(u8, &remaining, try cursor.seekAtOrAfter(&first));
        for (replay_hints) |hint| {
            const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), 1);
            try std.testing.expectError(error.NotFound, read.get(&key));
        }
    }
    {
        var read = try other.beginRead();
        defer read.abort();
        const key = internal_keys.replayEntryKey(kind, 1);
        try std.testing.expectEqualStrings("other", try read.get(&key));
    }
    // The unscoped yielding entry point shares the same exclusive cutoff.
    try store.appendReplayOpaque(a, 0, "zero");
    try store.appendReplayOpaque(a, std.math.maxInt(u64) - 1, "penultimate");
    // Avoid appendReplayOpaque's next-sequence metadata overflow at u64 max.
    const last = internal_keys.replayEntryKey(kind, std.math.maxInt(u64));
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(&last, "last");
        try write.commit();
    }
    try store.truncateReplayUpToYielding(a, std.math.maxInt(u64));
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("last", try read.get(&last));
    }
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite replay cleanup propagates record corruption without deleting a partial chunk" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-corrupt.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    store.file.page_cache_enabled.store(false, .monotonic);
    const first = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 1);
    const second = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 2);
    // Individual native writes keep these record pages separate.
    try store.file.putDocument(&internal_keys.replay_meta_init_key, "");
    try store.file.putDocument(&first, "first");
    try store.file.putDocument(&second, "second");
    const checkpoint = store.file.activeCheckpoint();
    const offset = checkpoint.document_root_page * native.default_page_size + native.page_header_size;
    var original: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try store.file.file.readPositionalAll(native_platform.testing.io, &original, offset));
    try store.file.file.writePositionalAll(native_platform.testing.io, &.{original[0] ^ 1}, offset);
    try std.testing.expectError(error.NativePageChecksumMismatch, store.truncateReplayUpTo(a, 3));
    try std.testing.expectEqual(checkpoint.commit_sequence, store.file.activeCheckpoint().commit_sequence);
    const retained = (try store.file.getDocumentAlloc(a, &first)).?;
    defer a.free(retained);
    try std.testing.expectEqualStrings("first", retained);
    try store.file.file.writePositionalAll(native_platform.testing.io, &original, offset);
    try store.truncateReplayUpTo(a, 3);
    try std.testing.expect((try store.file.getDocumentAlloc(a, &first)) == null);
    try std.testing.expect((try store.file.getDocumentAlloc(a, &second)) == null);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite replay readers propagate allocation errors before and after callbacks" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "scope\x00" }) |prefix| {
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var tmp = native_platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "replay-read-errors.aflite");
        defer a.free(path);
        var store = try Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = native_platform.testing.io });
        defer store.close();
        const large = try a.alloc(u8, 4 * 1024 * 1024);
        defer a.free(large);
        @memset(large, 'v');
        var runtime = RuntimeStore{ .store = &store, .prefix = prefix };
        try runtime.appendReplayOpaque(a, 1, "small");
        try runtime.appendReplayOpaque(a, 2, large);
        {
            var read = try runtime.beginRead();
            read.abort();
        }
        store.read_generation.?.file.page_cache_enabled.store(false, .monotonic);
        const Context = struct {
            count: usize = 0,
            fn handle(ptr: *anyopaque, _: u64, _: []const u8) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.count += 1;
            }
        };
        var ctx = Context{};
        budget.limit = budget.live;
        try std.testing.expectError(error.OutOfMemory, runtime.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
        budget.limit = budget.live + 1024 * 1024;
        if (prefix.len == 0) {
            try std.testing.expectError(error.OutOfMemory, store.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
        } else {
            try std.testing.expectError(error.OutOfMemory, runtime.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
        }
        try std.testing.expectEqual(@as(usize, 1), ctx.count);
        try std.testing.expectError(error.OutOfMemory, runtime.forEachReplayLaneFrom(internal_keys.replay_all_kind, 2, 0, &ctx, Context.handle));
        budget.limit = std.math.maxInt(usize);
        ctx = .{};
        const result = try runtime.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle);
        try std.testing.expectEqual(@as(usize, 2), result.matched_entries);
        try std.testing.expectEqual(@as(usize, 2), ctx.count);
        const empty = try runtime.forEachReplayLaneFrom(internal_keys.replay_all_kind, 3, 0, &ctx, Context.handle);
        try std.testing.expectEqual(@as(usize, 0), empty.matched_entries);
    }
}

test "lite replay collection owns every payload through allocation failures" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-owned-results.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    for ([_][]const u8{ "", "scope\x00" }) |prefix| {
        var runtime = RuntimeStore{ .store = &store, .prefix = prefix };
        for (1..20) |i| try runtime.appendReplayOpaque(a, i, "payload");
        var succeeded = false;
        for (0..64) |fail_index| {
            var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
            var failing = std.testing.FailingAllocator.init(budget.allocator(), .{ .fail_index = fail_index });
            const result = if (prefix.len == 0) store.iterateReplayFrom(failing.allocator(), 1) else runtime.iterateReplayFrom(failing.allocator(), 1);
            if (result) |entries| {
                try std.testing.expectEqual(@as(usize, 19), entries.len);
                for (entries) |*entry| entry.deinit(failing.allocator());
                failing.allocator().free(entries);
                succeeded = !failing.has_induced_failure;
            } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            if (succeeded) break;
        }
        try std.testing.expect(succeeded);
    }
}

test "lite replay readers propagate corruption malformed keys and callback errors" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-corrupt-read.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    const first = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 1);
    const second = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 2);
    try store.file.putDocument(&internal_keys.replay_meta_init_key, "");
    try store.file.putDocument(&first, "first");
    try store.file.putDocument(&second, "second");
    const offset = store.file.activeCheckpoint().document_root_page * native.default_page_size + native.page_header_size;
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try store.file.file.readPositionalAll(native_platform.testing.io, &byte, offset));
    try store.file.file.writePositionalAll(native_platform.testing.io, &.{byte[0] ^ 1}, offset);
    const Context = struct {
        count: usize = 0,
        stop: bool = false,
        fn handle(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
            if (self.stop) return error.ConsumerStopped;
        }
    };
    var ctx = Context{};
    {
        var read = try store.beginRead();
        read.abort();
    }
    store.read_generation.?.file.page_cache_enabled.store(false, .monotonic);
    try std.testing.expectError(error.NativePageChecksumMismatch, store.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
    try std.testing.expectEqual(@as(usize, 1), ctx.count);
    const limited = try store.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 1, &ctx, Context.handle);
    try std.testing.expectEqual(@as(usize, 1), limited.matched_entries);
    ctx.stop = true;
    try std.testing.expectError(error.ConsumerStopped, store.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
    try store.file.file.writePositionalAll(native_platform.testing.io, &byte, offset);
    try store.file.putDocument(second ++ "bad", "malformed");
    ctx.stop = false;
    try std.testing.expectError(error.InvalidReplayEntryKey, store.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
}

test "lite transaction snapshot cache shares hits misses and sorted duplicate reads" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "scope\x00" }) |prefix| {
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var tmp = native_platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "snapshot-read-cache.aflite");
        defer a.free(path);
        var store = try Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = native_platform.testing.io });
        defer store.close();
        const value = try a.alloc(u8, 256 * 1024);
        defer a.free(value);
        @memset(value, 'v');
        {
            var writer = try Txn.openWriteWithPrefix(&store, prefix);
            errdefer writer.abort();
            try writer.put("hit", value);
            try writer.put("other", "second");
            try writer.commit();
        }
        var read = try Txn.openReadWithPrefix(&store, prefix);
        defer read.abort();
        const file = try read.readFile();
        file.page_cache_enabled.store(false, .monotonic);
        const baseline = budget.live;
        budget.peak = baseline;
        budget.limit = baseline + 512 * 1024;
        defer budget.limit = std.math.maxInt(usize);
        const before = file.test_page_reads.load(.monotonic);
        const first = try read.get("hit");
        for (0..16) |_| {
            const again = try read.get("hit");
            try std.testing.expect(first.ptr == again.ptr);
        }
        try std.testing.expectEqualSlices(u8, value, first);
        try std.testing.expect(file.test_page_reads.load(.monotonic) - before < 80);
        const keys = [_][]const u8{ "hit", "hit", "missing", "missing", "other", "other" };
        var values: [keys.len]?[]const u8 = undefined;
        try read.getManySorted(&keys, &values);
        try std.testing.expect(values[0].?.ptr == first.ptr and values[1].?.ptr == first.ptr);
        try std.testing.expect(values[2] == null and values[3] == null);
        try std.testing.expectEqualStrings("second", values[4].?);
        try std.testing.expect(values[4].?.ptr == values[5].?.ptr);
        const cached_reads = file.test_page_reads.load(.monotonic);
        try std.testing.expectError(error.NotFound, read.get("missing"));
        try std.testing.expectError(error.NotFound, read.get("missing"));
        try std.testing.expect((try read.get("other")).ptr == values[4].?.ptr);
        try read.getManySorted(&keys, &values);
        try std.testing.expectEqual(cached_reads, file.test_page_reads.load(.monotonic));
        try std.testing.expect(budget.peak - baseline < 512 * 1024);
    }
}

test "lite transaction snapshot cache preserves pending versions and pinned generations" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "snapshot-cache-generations.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    try store.file.putDocument("key", "old");
    try store.file.putDocument("uncached", "old-generation");
    var pinned = try store.beginRead();
    defer pinned.abort();
    const old = try pinned.get("key");
    try std.testing.expectError(error.NotFound, pinned.get("missing"));
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        const borrowed = try write.get("key");
        try std.testing.expectError(error.NotFound, write.get("missing"));
        try write.put("key", "new");
        try write.put("missing", "created");
        const pending = try write.get("key");
        try write.put("key", "newest");
        try std.testing.expectEqualStrings("old", borrowed);
        try std.testing.expectEqualStrings("new", pending);
        var values: [2]?[]const u8 = undefined;
        try write.getManySorted(&.{ "key", "missing" }, &values);
        try std.testing.expectEqualStrings("newest", values[0].?);
        try std.testing.expectEqualStrings("created", values[1].?);
        try write.delete("missing");
        try write.getManySorted(&.{ "key", "missing" }, &values);
        try std.testing.expect(values[1] == null);
        try write.put("missing", "published");
        try write.commit();
    }
    _ = try store.vacuum();
    try std.testing.expectEqualStrings("old", old);
    try std.testing.expect((try pinned.get("key")).ptr == old.ptr);
    try std.testing.expectError(error.NotFound, pinned.get("missing"));
    try std.testing.expectEqualStrings("old-generation", try pinned.get("uncached"));
    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings("newest", try fresh.get("key"));
    try std.testing.expectEqualStrings("published", try fresh.get("missing"));
}

test "lite transaction snapshot cache allocation failures release ownership and retry" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "snapshot-cache-failures.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    {
        var setup = try Txn.openWriteWithPrefix(&store, "scope\x00");
        errdefer setup.abort();
        try setup.put("a", "first");
        try setup.put("b", &(@as([8192]u8, @splat('v'))));
        try setup.commit();
    }
    var exhausted = false;
    for (0..128) |fail_index| {
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var failing = std.testing.FailingAllocator.init(budget.allocator(), .{ .fail_index = fail_index });
        {
            var read = try Txn.openReadWithPrefix(&store, "scope\x00");
            read.allocator = failing.allocator();
            defer read.abort();
            const Run = struct {
                fn apply(txn: *Txn) !void {
                    try std.testing.expectEqualStrings("first", try txn.get("a"));
                    var values: [5]?[]const u8 = undefined;
                    try txn.getManySorted(&.{ "a", "b", "b", "missing", "missing" }, &values);
                    try std.testing.expectEqual(@as(usize, 8192), values[1].?.len);
                    try std.testing.expect(values[1].?.ptr == values[2].?.ptr);
                    try std.testing.expect(values[3] == null and values[4] == null);
                    if (txn.get("missing")) |_| return error.TestUnexpectedResult else |err| {
                        if (err != error.NotFound) return err;
                    }
                }
            };
            if (Run.apply(&read)) |_| {
                exhausted = !failing.has_induced_failure;
            } else |err| {
                try std.testing.expectEqual(error.OutOfMemory, err);
                failing.fail_index = std.math.maxInt(usize);
                failing.resize_fail_index = std.math.maxInt(usize);
                try Run.apply(&read);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        if (exhausted) break;
    }
    try std.testing.expect(exhausted);
}

test "lite pending overwrites retain only borrowed versions" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pending-retention.aflite");
    defer a.free(path);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var store = try Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    const value = try a.alloc(u8, 256 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    var txn = try store.beginWrite();
    defer txn.abort();
    const baseline = budget.live;
    budget.limit = baseline + 600 * 1024;
    defer budget.limit = std.math.maxInt(usize);
    for (0..64) |i| {
        value[0] = @intCast(i);
        try txn.put("key", value);
    }
    try std.testing.expectEqual(@as(usize, 1), txn.pending.items.len);
    try std.testing.expectEqual(@as(usize, 0), txn.borrowed_versions.items.len);
    try std.testing.expect(budget.live - baseline < 300 * 1024);
    const first = try txn.get("key");
    budget.limit = baseline + 1024 * 1024;
    value[0] = 100;
    try txn.put("key", value);
    var values: [2]?[]const u8 = undefined;
    try txn.getManySorted(&.{ "key", "key" }, &values);
    try std.testing.expect(values[0].?.ptr == values[1].?.ptr);
    try txn.delete("key");
    try std.testing.expectError(error.NotFound, txn.get("key"));
    try txn.put("key", "newest");
    try std.testing.expectEqual(@as(u8, 63), first[0]);
    try std.testing.expectEqual(@as(u8, 100), values[0].?[0]);
    try std.testing.expectEqual(@as(usize, 2), txn.borrowed_versions.items.len);
    try std.testing.expectEqualStrings("newest", try txn.get("key"));
}

test "lite pending replacement allocation failures preserve borrowed values" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pending-failures.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    var exhausted = false;
    for (0..32) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{});
        var txn = try store.beginWrite();
        txn.allocator = failing.allocator();
        defer txn.abort();
        try txn.put("key", "old");
        const borrowed = try txn.get("key");
        failing.fail_index = failing.alloc_index + fail_index;
        if (txn.put("key", "new")) |_| {
            exhausted = !failing.has_induced_failure;
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            failing.fail_index = std.math.maxInt(usize);
            failing.resize_fail_index = std.math.maxInt(usize);
            try std.testing.expectEqualStrings("old", try txn.get("key"));
            try txn.put("key", "new");
        }
        try std.testing.expectEqualStrings("old", borrowed);
        try std.testing.expectEqualStrings("new", try txn.get("key"));
        if (exhausted) break;
    }
    try std.testing.expect(exhausted);
}

test "lite reclamation worker waits while online capture owns publication" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "worker-capture-wait.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.file.retirement_work_pages = 0;
    try store.file.putDocument("key", "old");
    try store.file.putDocument("key", "new");
    try store.file.putDocument("advance", "frontier");
    try std.testing.expect(try store.file.retirementNeedsService());
    var capture = native.ChangeCapture{};
    defer capture.deinit(a);
    store.file.change_capture = &capture;
    defer {
        lockStore(&store);
        store.file.change_capture = null;
        store.maintenance_wake.set(store.file.runtime());
        store.mutex.unlock();
    }
    store.startMaintenance();
    if (store.maintenance_future == null) return error.SkipZigTest;
    const started_deadline = std.Io.Clock.awake.now(store.file.runtime()).addDuration(std.Io.Duration.fromSeconds(3));
    while (store.test_maintenance_cycles.load(.acquire) == 0) {
        if (std.Io.Clock.awake.now(store.file.runtime()).nanoseconds >= started_deadline.nanoseconds) return error.TestUnexpectedResult;
        try std.Io.sleep(store.file.runtime(), .fromMilliseconds(1), .awake);
    }
    const before = store.test_maintenance_cycles.load(.acquire);
    try std.Io.sleep(store.file.runtime(), .fromMilliseconds(50), .awake);
    try std.testing.expect(before <= 1);
    try std.testing.expectEqual(before, store.test_maintenance_cycles.load(.acquire));
}

test "lite reclamation worker backs off failed publication fences" {
    const a = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "worker-error-backoff.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = native_platform.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .retry_ms = 500 } });
    defer store.close();
    store.file.retirement_work_pages = 0;
    try store.file.putDocument("key", "old");
    try store.file.putDocument("key", "new");
    try store.file.putDocument("advance", "frontier");
    store.file.checkpoint_publication_uncertain = true;
    store.startMaintenance();
    if (store.maintenance_future == null) return error.SkipZigTest;
    const started_deadline = std.Io.Clock.awake.now(store.file.runtime()).addDuration(std.Io.Duration.fromSeconds(3));
    while (store.test_maintenance_cycles.load(.acquire) == 0) {
        if (std.Io.Clock.awake.now(store.file.runtime()).nanoseconds >= started_deadline.nanoseconds) return error.TestUnexpectedResult;
        try std.Io.sleep(store.file.runtime(), .fromMilliseconds(1), .awake);
    }
    const before = store.test_maintenance_cycles.load(.acquire);
    try std.Io.sleep(store.file.runtime(), .fromMilliseconds(50), .awake);
    try std.testing.expect(before <= 1);
    try std.testing.expectEqual(before, store.test_maintenance_cycles.load(.acquire));
    lockStore(&store);
    defer store.mutex.unlock();
    try std.testing.expectEqual(reclamation.State.failed, store.maintenance_policy.status.state);
    try std.testing.expectEqualStrings("OutcomeUnknown", store.maintenance_policy.status.last_error.?);
}
