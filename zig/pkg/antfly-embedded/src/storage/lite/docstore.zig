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
const ReadWorkspace = @import("read_workspace.zig").Workspace;
const read_scratch = @import("../../segment_source.zig").Scratch;
const bounded_cursor_test_documents: usize = 512;

test "lite detached writer admission fences documents and index catalogs" {
    const Admission = struct {
        denied: bool = false,
        entered: usize = 0,
        active: usize = 0,
        fn enter(ptr: *anyopaque, _: PublicationScope) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.denied) return error.EnrichmentSourceChanged;
            self.entered += 1;
            self.active += 1;
        }
        fn leave(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.active -= 1;
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "detached-admission.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var admission: Admission = .{};
    store.writer_admission = .{ .ptr = &admission, .enter = Admission.enter, .leave = Admission.leave };
    defer store.writer_admission = null;
    var write = try store.beginWrite();
    try write.put("document", "value");
    try write.commit();
    try std.testing.expectEqual(@as(usize, 0), admission.active);
    try std.testing.expectEqual(@as(usize, 2), admission.entered);
    var indexes = @import("index_storage.zig").Store.init(a, &store);
    try indexes.storage().writeFileAbsolute("/index", "segment");
    try std.testing.expectEqual(@as(usize, 0), admission.active);
    try std.testing.expectEqual(@as(usize, 3), admission.entered);
    const PendingIndex = struct {
        fn apply(_: *anyopaque, file: *native.NativeFile) !void {
            try file.putIndexCatalogRecord("/pending-index", "published");
        }
    };
    try store.submitIndexMutation(&admission, PendingIndex.apply, false);
    try std.testing.expect(store.file.durable_header == null);
    var next_owner = try native.NativeFile.openWithIo(a, std.testing.io, path, .{ .read_only = true, .internal_reader = true });
    defer next_owner.close();
    const visible = (try next_owner.getIndexCatalogRecordAlloc(a, "/pending-index")).?;
    defer a.free(visible);
    try std.testing.expectEqualStrings("published", visible);
    const checkpoint = store.file.activeCheckpoint();
    admission.denied = true;
    try std.testing.expectError(error.EnrichmentSourceChanged, store.beginWrite());
    try std.testing.expectError(error.EnrichmentSourceChanged, indexes.storage().writeFileAbsolute("/index", "stale"));
    try std.testing.expectEqualDeep(checkpoint, store.file.activeCheckpoint());
    admission.denied = false;
    var next = try store.beginWrite();
    next.abort();
    try std.testing.expectEqual(@as(usize, 0), admission.active);
}

test "lite reclamation stale quota estimates expire after roots shrink" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-stale-quota.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .page_reuse = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, 128 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    for (0..64) |_| try store.file.putDocument("large", value);
    try store.file.putDocument("keep", value);
    const physical = (try store.file.file.stat(std.testing.io)).size;
    const old_compact = (try store.file.liveStats(null)).compact_size;
    const cap = physical + old_compact * 2 - 64 * 1024;
    store.maintenance_policy = try reclamation.Policy.init(.{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 128 * 1024, .max_storage_bytes = cap });
    try store.maintainOnce(false);
    try std.testing.expectEqual(reclamation.Reason.storage_budget, store.maintenance_policy.status.reason);
    var write = try store.beginWrite();
    try write.delete("large");
    try write.commit();
    const after = (try store.file.file.stat(std.testing.io)).size;
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
            const n = try std.testing.io.vtable.fileWritePositional(userdata, file, header, data, splat, offset);
            if (owner) |store| if (file.handle != store.file.file.handle) {
                writes += 1;
            };
            return n;
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-busy-copy.aflite");
    defer a.free(path);
    var vtable = std.testing.io.vtable.*;
    vtable.fileWritePositional = Hook.write;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "admission-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "admission-prepared.aflite");
    defer a.free(staged_path);
    const options: CreateOptions = .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } };
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
    const old_bytes = (try live.file.file.stat(std.testing.io)).size;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-retry-scan.aflite");
    defer a.free(path);
    var probe = Probe{};
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } });
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
    store.assessment_cache.?.revalidate_at = std.Io.Clock.awake.now(std.testing.io).addDuration(std.Io.Duration.fromSeconds(30));
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
    try std.testing.expect(store.assessment_cache.?.revalidate_at.nanoseconds >= std.Io.Clock.awake.now(std.testing.io).addDuration(std.Io.Duration.fromSeconds(29)).nanoseconds);
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
            const n = try std.testing.io.vtable.fileReadPositional(userdata, file, data, offset);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-status-cancel.aflite");
    defer a.free(path);
    var vtable = std.testing.io.vtable.*;
    vtable.fileReadPositional = Hook.read;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "worker-cold-debt.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "vacuum-collector-capacity.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 4096 } });
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
    const size = (try store.file.file.stat(std.testing.io)).size;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "vacuum-publication-disk.aflite");
    defer a.free(path);
    var probe = Probe{};
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .disk_headroom_bytes = 4096, .capacity_probe = .{ .context = &probe, .available = Probe.available } } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "restore-budget-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "restore-budget-staged.aflite");
    defer a.free(staged_path);
    var live = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .max_storage_bytes = 128 * 4096 } });
    defer live.close();
    live.maintenance_start_suppressed = true;
    var staged = try Store.createWithOptions(a, staged_path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "workspace-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "workspace-staged.aflite");
    defer a.free(staged_path);
    const limit = 256 * 4096;
    var live = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .max_storage_bytes = limit } });
    defer live.close();
    live.maintenance_start_suppressed = true;
    var workspace = try live.reserveGenerationWorkspace();
    var reserved = true;
    defer if (reserved) workspace.deinit();
    try std.testing.expectEqual(limit, (try live.reclamationStatus()).totalBytes());
    try std.testing.expectError(error.FileBusy, live.reserveGenerationWorkspace());
    try std.testing.expectError(error.FileBusy, live.vacuum());
    var staged = try Store.createWithOptions(a, staged_path, .{ .io = std.testing.io, .no_sync = true, .reclamation = workspace.options });
    defer staged.close();
    staged.maintenance_start_suppressed = true;
    const value = try a.alloc(u8, limit);
    defer a.free(value);
    @memset(value, 'v');
    const before = staged.file.activeCheckpoint();
    const before_size = (try staged.file.file.stat(std.testing.io)).size;
    try std.testing.expectError(error.LiteStorageBudgetExceeded, staged.putCatalogRecord("oversized", value));
    try std.testing.expectEqualDeep(before, staged.file.activeCheckpoint());
    try std.testing.expectEqual(before_size, (try staged.file.file.stat(std.testing.io)).size);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "restore-disk-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "restore-disk-staged.aflite");
    defer a.free(staged_path);
    var available: u64 = 1024 * 1024;
    var live = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .disk_headroom_bytes = 4096, .capacity_probe = .{ .context = &available, .available = Probe.available } } });
    defer live.close();
    live.maintenance_start_suppressed = true;
    var workspace = try live.reserveGenerationWorkspace();
    defer workspace.deinit();
    var staged = try Store.createWithOptions(a, staged_path, .{ .io = std.testing.io, .no_sync = true, .reclamation = workspace.options });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "shrink-activity.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .minimum_reclaim_bytes = 16 * 1024, .assessment_bytes = 64 * 1024 * 1024 } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "shrink-packed.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .minimum_reclaim_bytes = 32 * 1024, .assessment_bytes = 64 * 1024 * 1024 } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "retirement-capacity.aflite");
    defer a.free(path);
    const limit = 128 * 4096;
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false, .max_storage_bytes = limit } });
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
        try std.testing.expect((try store.file.file.stat(std.testing.io)).size <= limit);
    }
    try std.testing.expect((try store.file.allocatorStats()).?.reused_pages > 0);
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqual(@as(u8, 127), (try read.get(&key))[0]);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation creation checks budget before replacement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "creation-capacity.aflite");
    defer a.free(path);
    {
        var file = try native.NativeFile.createWithIo(a, std.testing.io, path, .{});
        defer file.close();
        try file.putCatalogRecord("stable", "original");
    }
    try std.testing.expectError(error.LiteStorageBudgetExceeded, Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .max_storage_bytes = 4096 } }));
    var file = try native.NativeFile.openWithIo(a, std.testing.io, path, .{ .read_only = true });
    defer file.close();
    const value = (try file.getCatalogRecordAlloc(a, "stable")).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("original", value);
}

test "lite reclamation capacity reserve drains wide value graphs" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "wide-retirement-capacity.aflite");
    defer a.free(path);
    const limit = 256 * 4096;
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false, .max_storage_bytes = limit } });
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
        try std.testing.expect((try store.file.file.stat(std.testing.io)).size <= limit);
    }
    const stored = (try store.getCatalogRecordAlloc(a, "meta")).?;
    defer a.free(stored);
    try std.testing.expectEqualSlices(u8, value, stored);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite reclamation owner vacuum reports final publication size" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "vacuum-final-size.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_cancel.request();
    for (0..32) |_| try store.putCatalogRecord("meta", "value");
    const report = try store.vacuum();
    const size = (try store.file.file.stat(std.testing.io)).size;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "creation-disk.aflite");
    defer a.free(path);
    var context: u8 = 0;
    try std.testing.expectError(error.LiteInsufficientDiskSpace, Store.createWithOptions(a, path, .{ .io = std.testing.io, .reclamation = .{ .disk_headroom_bytes = 0, .capacity_probe = .{ .context = &context, .available = Probe.available } } }));
    try std.testing.expectError(error.FileNotFound, native.NativeFile.openWithIo(a, std.testing.io, path, .{}));
}

test "lite reclamation counts retired inodes until pinned readers release" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-retained.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .enabled = false } });
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

test "lite reclamation externally locked refresh defers debt to its maintenance owner" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-connection-refresh.aflite");
    defer alloc.free(path);
    const policy: reclamation.Options = .{ .enabled = false, .retirement_work_pages = 1 };
    var initial = try Store.createWithOptions(alloc, path, .{
        .io = std.testing.io,
        .no_sync = true,
        .externally_locked = true,
        .reclamation = policy,
    });
    initial.file.releaseWriterOwnership();
    var lease = try native.lockWriterPathWithIo(alloc, std.testing.io, path);
    defer lease.close();
    {
        defer initial.close();
        for (0..32) |version| {
            var value: [32]u8 = undefined;
            var write = try initial.beginWrite();
            errdefer write.abort();
            try write.put("doc", try std.fmt.bufPrint(&value, "version-{d}", .{version}));
            try write.commit();
        }
        try std.testing.expect(try initial.file.retirementNeedsService());
    }
    for (0..3) |_| {
        var probe = try native.NativeFile.openWithIo(alloc, std.testing.io, path, .{ .read_only = true, .internal_reader = true });
        defer probe.close();
        const checkpoint = probe.activeCheckpoint();
        var connection = try Store.openWithOptions(alloc, path, .{
            .io = std.testing.io,
            .no_sync = true,
            .externally_locked = true,
            .reclamation = policy,
        });
        defer connection.close();
        try std.testing.expect(std.meta.eql(checkpoint, connection.file.activeCheckpoint()));
        var read = try connection.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("version-31", try read.get("doc"));
    }
    var owner = try Store.openWithOptions(alloc, path, .{
        .io = std.testing.io,
        .no_sync = true,
        .externally_locked = true,
        .reclamation = policy,
    });
    defer owner.close();
    const before = owner.file.activeCheckpoint().commit_sequence;
    try owner.maintainOnce(false);
    try std.testing.expect(owner.file.activeCheckpoint().commit_sequence > before);
    try std.testing.expect((try owner.checkWithCancel(null)).valid);
}

test "lite externally locked reopen defers allocator conversion and reclamation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "external-owner-open.aflite");
    defer alloc.free(path);
    {
        var legacy = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } });
        defer legacy.close();
        var write = try legacy.beginWrite();
        errdefer write.abort();
        try write.put("doc", "original");
        try write.commit();
    }
    var lease = try native.lockWriterPathWithIo(alloc, std.testing.io, path);
    defer lease.close();
    var owner = try Store.openWithOptions(alloc, path, .{ .io = std.testing.io, .externally_locked = true });
    defer owner.close();
    try std.testing.expect(!owner.file.header.indexed_reclamation);
    try std.testing.expectEqual(@as(u64, 0), owner.maintenance_policy.status.assessment_count);
    try std.testing.expectEqual(@as(u64, 0), owner.maintenance_policy.status.rewrite_count);
    // Ordinary writes remain valid before the scheduled format conversion.
    var write = try owner.beginWrite();
    errdefer write.abort();
    try write.put("doc", "updated");
    try write.commit();
    try owner.maintainOnce(false);
    try std.testing.expect(owner.file.header.indexed_reclamation);
    try std.testing.expectEqual(@as(u64, 1), owner.maintenance_policy.status.rewrite_count);
    var read = try owner.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("updated", try read.get("doc"));
}

test "lite reclamation writable reopen services debt and read only reopen does not" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-reopen.aflite");
    defer alloc.free(path);
    const options: reclamation.Options = .{ .page_reuse = false, .assessment_bytes = 4096, .minimum_reclaim_bytes = 32 * 4096 };
    var peak: u64 = 0;
    for (0..6) |cycle| {
        // Populate without a worker, representing a legacy/maintenance-disabled client.
        {
            var store = Store.openWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } }) catch |err| switch (err) {
                error.FileNotFound => try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } }),
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
            var reader = try Store.openWithOptions(alloc, path, .{ .read_only = true, .io = std.testing.io, .reclamation = options });
            defer reader.close();
            try std.testing.expectEqual(@as(u64, 0), (try reader.reclamationStatus()).rewrite_count);
        }
        var owner = try Store.openWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = options });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-budget.aflite");
    defer alloc.free(path);
    const limit = 64 * 4096;
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false, .max_storage_bytes = limit } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-disk.aflite");
    defer alloc.free(path);
    var probe = Probe{};
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false, .enabled = false } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reclamation-worker.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false, .assessment_bytes = 8 * 4096, .minimum_reclaim_bytes = 32 * 4096, .retry_ms = 1 } });
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
        const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(std.Io.Duration.fromSeconds(5));
        while (true) {
            const status = try store.reclamationStatus();
            // A successful online rewrite can retain bounded catch-up history;
            // the contract is an envelope, not a fully packed file per cycle.
            if (status.rewrite_count > before and status.state == .idle and status.current_file_bytes < 64 * 4096) break;
            if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= deadline.nanoseconds) {
                std.debug.print("maintenance state={s} reason={s} error={?s} size={d}\n", .{ @tagName(status.state), @tagName(status.reason), status.last_error, status.current_file_bytes });
                return error.TestUnexpectedResult;
            }
            try std.testing.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const live_path = try testPath(alloc, tmp, "reclamation-adopt-live.aflite");
    defer alloc.free(live_path);
    const staged_path = try testPath(alloc, tmp, "reclamation-adopt-stage.aflite");
    defer alloc.free(staged_path);
    var live = try Store.createWithOptions(alloc, live_path, .{ .no_sync = true, .io = std.testing.io });
    defer live.close();
    var staged = try Store.createWithOptions(alloc, staged_path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-owner.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "allocator-migration.aflite");
    defer a.free(path);
    const value: [8192]u8 = @splat('v');
    var key: [1024]u8 = @splat('k');
    key[0] = 'a';
    {
        var legacy = try native.NativeFile.createWithIo(a, std.testing.io, path, .{});
        defer legacy.close();
        try legacy.putDocument(&key, &value);
        try legacy.putCatalogRecord("metadata", &value);
        try legacy.putIndexCatalogRecord("index", &value);
    }
    {
        var reader = try Store.openWithOptions(a, path, .{ .read_only = true, .io = std.testing.io });
        defer reader.close();
        try std.testing.expect(!reader.file.header.indexed_reclamation);
    }
    var owner = try Store.openWithOptions(a, path, .{ .io = std.testing.io, .reclamation = .{ .enabled = false } });
    defer owner.close();
    try std.testing.expect(owner.file.header.indexed_reclamation);
    var txn = try owner.beginRead();
    defer txn.abort();
    try std.testing.expectEqualSlices(u8, &value, try txn.get(&key));
    try std.testing.expect((try owner.checkWithCancel(null)).valid);
}

test "lite allocator v4 releasing the last old reader starts idle retirement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reuse-idle-retirement.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
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
    const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(std.Io.Duration.fromSeconds(10));
    while (true) {
        const status = try store.reclamationStatus();
        if (status.pending_data_retirement_objects == 0 and status.retired_objects_serviced > 1000) {
            try std.testing.expectEqual(@as(u64, 0), status.rewrite_count);
            break;
        }
        if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= deadline.nanoseconds) return error.TestUnexpectedResult;
        try std.testing.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

pub const OpenOptions = struct {
    externally_locked: bool = false,
    reclamation: reclamation.Options = .{},
    read_only: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    io: ?std.Io = null,
};

pub const CreateOptions = struct {
    externally_locked: bool = false,
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
    index_only: bool = false,
    result: anyerror!void = {},
};

const ReadPin = struct {
    sequence: u64,
    pinned_at: std.Io.Timestamp,
    kernel_reader: bool = false,
    previous: ?*ReadPin = null,
    next: ?*ReadPin = null,
};

const artifact_source = @import("artifact_source.zig");

const ReadGeneration = struct {
    owner_generation: u64 = 0,
    first_pin: ?*ReadPin = null,
    last_pin: ?*ReadPin = null,
    file: native.NativeFile,
    checkpoint: native.CheckpointSlot = .{},
    references: usize = 0,
    scoped_reader_lock: bool = false,
    kernel_readers: usize = 0,
    retired: bool = false,
    next_retired: ?*ReadGeneration = null,
    retained_bytes: u64 = 0,
    pinned_at: std.Io.Timestamp = .zero,
};

/// Independent of Store lifetime: snapshots may hold mapped artifacts after
/// closing the database. Only the atomic counter and allocator survive then.
const ArtifactAccount = struct {
    allocator: Allocator,
    references: std.atomic.Value(usize) = .init(1),
    bytes: std.atomic.Value(u64) = .init(0),
    future_bytes: std.atomic.Value(u64) = .init(0),
    revision: std.atomic.Value(u64) = .init(0),

    fn release(self: *ArtifactAccount) void {
        if (self.references.fetchSub(1, .acq_rel) == 1) {
            std.debug.assert(self.bytes.load(.acquire) == 0);
            std.debug.assert(self.future_bytes.load(.acquire) == 0);
            self.allocator.destroy(self);
        }
    }
};

pub const ArtifactLease = struct {
    allocator: Allocator,
    account: *ArtifactAccount,
    bytes: u64 = 0,
    materialized_bytes: u64 = 0,

    pub fn noteMaterialized(self: *ArtifactLease, length: u64) void {
        const rounded = std.mem.alignForward(u64, length, 4096);
        std.debug.assert(rounded <= self.bytes);
        if (rounded <= self.materialized_bytes) return;
        _ = self.account.future_bytes.fetchSub(rounded - self.materialized_bytes, .acq_rel);
        self.materialized_bytes = rounded;
        _ = self.account.revision.fetchAdd(1, .release);
    }

    pub fn release(ptr: *anyopaque) void {
        const self: *ArtifactLease = @ptrCast(@alignCast(ptr));
        const account = self.account;
        _ = account.future_bytes.fetchSub(self.bytes - self.materialized_bytes, .acq_rel);
        _ = account.bytes.fetchSub(self.bytes, .acq_rel);
        _ = account.revision.fetchAdd(1, .release);
        const allocator = self.allocator;
        allocator.destroy(self);
        account.release();
    }
};

var next_score_cache_owner: std.atomic.Value(u64) = .init(1);
/// Optional owner admission around every document/index publication.
/// Detached computation can retain this Store without retaining a file
/// writer lease; admission must validate its checkpoint before mutation.
pub const PublicationScope = enum { primary, index, reclamation };

pub const WriterAdmission = struct {
    ptr: *anyopaque,
    enter: *const fn (*anyopaque, PublicationScope) anyerror!void,
    leave: *const fn (*anyopaque) void,
};

pub const Store = struct {
    score_cache_owner: u64 = 0,
    allocator: Allocator,
    workspace_reclaimer: ?u64 = null,
    metadata_pool: @import("metadata_pool.zig").MetadataPool = .{},
    metadata_ready: std.atomic.Value(bool) = .init(false),
    workspace_mutex: std.atomic.Mutex = .unlocked,
    workspace_pending: [2]bool = .{ false, false },
    workspaces: [2]?*ReadWorkspace = .{ null, null },
    file: native.NativeFile,
    read_generation: ?*ReadGeneration = null,
    // Admission and release never wait for the writer's disk I/O.
    read_mutex: std.Io.Mutex = .init,
    reader_frontier: std.atomic.Value(u64) = .init(std.math.maxInt(u64)),
    read_outcome_unknown: bool = false,
    retired_generations: ?*ReadGeneration = null,
    /// Cached under mutex so foreground admission is independent of the number
    /// of retained generations. Status may walk them for reader diagnostics.
    retired_file_bytes: u64 = 0,
    artifact_account: ?*ArtifactAccount = null,
    artifact_registry: ?*artifact_source.Registry = null,
    /// Short-lived capture runtimes fence artifact readers instead of changing
    /// the durable catalog merely to open their existing immutable inputs.
    artifact_reader_fence: bool = false,
    artifact_cleanup_cursor: artifact_source.SweepCursor = .{},
    artifact_marker_sweep: artifact_source.MarkerSweep = .{},
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
    test_maintenance_hook: if (@import("builtin").is_test) ?MaintenanceTestHook else void = if (@import("builtin").is_test) null else {},
    test_maintenance_cycles: if (@import("builtin").is_test) std.atomic.Value(u64) else void = if (@import("builtin").is_test) .init(0) else {},
    admission_refresh_size: u64 = 0,
    admission_artifact_revision: u64 = 0,
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
    writer_admission: ?WriterAdmission = null,
    writer_active: bool = false,
    writer_ticketed: bool = false,
    next_writer_ticket: u64 = 0,
    serving_writer_ticket: u64 = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,

    pub const MaintenancePhase = enum { assessment, copy };
    pub const MaintenanceTestHook = struct {
        ptr: *anyopaque,
        notify: *const fn (*anyopaque, MaintenancePhase) anyerror!void,
    };

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !Store {
        return try openWithOptions(allocator, path, .{ .read_only = read_only });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, opts: OpenOptions) !Store {
        const policy = try reclamation.Policy.init(opts.reclamation);
        const native_opts = native.OpenOptions{
            .externally_locked = opts.externally_locked,
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
            .score_cache_owner = next_score_cache_owner.fetchAdd(1, .monotonic),
            .allocator = allocator,
            .file = file,
            .read_only = opts.read_only,
            .resource_manager = opts.resource_manager,
            .maintenance_policy = policy,
            .maintenance_start_suppressed = true,
        };
        errdefer result.close();
        // A standalone owner may live for only one CLI invocation, so reopen
        // services its debt. External owners schedule this work themselves:
        // their opens/refreshes run under a connection capture fence and must
        // never perform a full assessment or generation rewrite there.
        result.file.retirement_work_pages = opts.reclamation.retirement_work_pages;
        result.file.reserve_retirement_capacity = true;
        result.file.vacuum_target_indexed = opts.reclamation.page_reuse;
        if (!opts.read_only and !opts.externally_locked) {
            if (opts.reclamation.page_reuse and !result.file.header.indexed_reclamation) {
                _ = try result.vacuumWithCancel(null);
            }
            result.maintainOnce(false) catch |err| result.recordMaintenanceError(err);
        }
        result.maintenance_start_suppressed = opts.externally_locked;
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
        file.externally_locked = opts.externally_locked;
        file.reserve_retirement_capacity = true;
        if (opts.reclamation.max_storage_bytes != 0) file.max_file_bytes = opts.reclamation.max_storage_bytes;
        file.vacuum_target_indexed = opts.reclamation.page_reuse;
        return .{
            .score_cache_owner = next_score_cache_owner.fetchAdd(1, .monotonic),
            .allocator = allocator,
            .file = file,
            .read_only = false,
            .resource_manager = opts.resource_manager,
            .maintenance_policy = policy,
            .maintenance_start_suppressed = opts.externally_locked,
        };
    }

    fn stopMaintenance(self: *Store) void {
        self.maintenance_cancel.request();
        self.maintenance_wake.set(self.file.runtime());
        if (self.maintenance_future) |*future| future.await(self.file.runtime());
        self.maintenance_future = null;
        self.maintenance_started.store(false, .release);
    }

    fn readMetadataAllocator(self: *Store) Allocator {
        if (self.metadata_ready.load(.acquire)) return self.metadata_pool.allocator(self.allocator, self.resource_manager);
        platform_sync.lockYielding(&self.workspace_mutex);
        if (self.workspace_reclaimer == null) {
            if (self.resource_manager) |manager| self.workspace_reclaimer = manager.registerReclaimer(.lite_docstore_snapshot_cache, self, reclaimIdleWorkspaces) catch null;
        }
        if (self.resource_manager == null or self.workspace_reclaimer != null) self.metadata_ready.store(true, .release);
        self.workspace_mutex.unlock();
        return self.metadata_pool.allocator(self.allocator, self.resource_manager);
    }

    fn reclaimIdleWorkspaces(context: *anyopaque, target_bytes: u64) u64 {
        const self: *Store = @ptrCast(@alignCast(context));
        const metadata = self.metadata_pool.reclaimDetailed(target_bytes);
        var freed = metadata.physical;
        var reported = metadata.total();
        if (reported != 0) if (resource_manager_mod.BudgetedAllocator.fromAllocator(self.allocator)) |budget| {
            _ = budget.releaseUnusedCreditThreadSafe();
        };
        while (freed < target_bytes) {
            if (!self.workspace_mutex.tryLock()) break;
            const found: ?*ReadWorkspace = blk: {
                for (&self.workspaces) |*slot| if (slot.*) |workspace| {
                    if (workspace.busy) continue;
                    slot.* = null;
                    break :blk workspace;
                };
                break :blk null;
            };
            self.workspace_mutex.unlock();
            const workspace = found orelse break;
            const physical = workspace.live + @sizeOf(ReadWorkspace);
            const bytes = if (workspace.budget) |*budget| budget.reservation.bytes +| @sizeOf(ReadWorkspace) else physical;
            const backing_budget = resource_manager_mod.BudgetedAllocator.fromAllocator(workspace.backing);
            workspace.destroy();
            if (backing_budget) |budget| _ = budget.releaseUnusedCreditThreadSafe();
            freed +|= physical;
            reported +|= bytes;
        }
        return reported;
    }

    fn checkoutWorkspace(self: *Store, caller: Allocator) !?*ReadWorkspace {
        const query_budget = resource_manager_mod.BudgetedAllocator.fromAllocator(caller);
        if (query_budget == null and !(caller.ptr == self.allocator.ptr and caller.vtable == self.allocator.vtable)) return null;
        platform_sync.lockYielding(&self.workspace_mutex);
        if (self.workspace_reclaimer == null) {
            if (self.resource_manager) |manager| self.workspace_reclaimer = manager.registerReclaimer(.lite_docstore_snapshot_cache, self, reclaimIdleWorkspaces) catch {
                self.workspace_mutex.unlock();
                return null;
            };
        }
        for (&self.workspaces, 0..) |*slot, index| {
            if (slot.*) |workspace| {
                if (workspace.busy) continue;
                workspace.busy = true;
                self.workspace_mutex.unlock();
                // Admission may reclaim other idle slots. Never hold the pool
                // lock during allocator, reservation, or arena operations.
                workspace.begin(query_budget) catch |err| {
                    platform_sync.lockYielding(&self.workspace_mutex);
                    self.workspaces[index] = null;
                    self.workspace_mutex.unlock();
                    workspace.destroy();
                    // Cached capacity must not make a smaller query fail.
                    if (err == error.ResourceBudgetExceeded) return null;
                    return err;
                };
                return workspace;
            }
            if (self.workspace_pending[index]) continue;
            self.workspace_pending[index] = true;
            self.workspace_mutex.unlock();
            const workspace = ReadWorkspace.create(self.allocator, self.resource_manager) catch {
                platform_sync.lockYielding(&self.workspace_mutex);
                self.workspace_pending[index] = false;
                self.workspace_mutex.unlock();
                return null;
            };
            workspace.busy = true;
            workspace.begin(query_budget) catch |err| {
                workspace.destroy();
                platform_sync.lockYielding(&self.workspace_mutex);
                self.workspace_pending[index] = false;
                self.workspace_mutex.unlock();
                if (err == error.ResourceBudgetExceeded) return null;
                return err;
            };
            platform_sync.lockYielding(&self.workspace_mutex);
            self.workspace_pending[index] = false;
            self.workspaces[index] = workspace;
            self.workspace_mutex.unlock();
            return workspace;
        }
        self.workspace_mutex.unlock();
        // Concurrency beyond the two retained slots uses query-owned scratch.
        return null;
    }

    fn returnWorkspace(self: *Store, workspace: *ReadWorkspace) void {
        workspace.end();
        const pressure = if (self.resource_manager) |manager| manager.pressureDecision(.lite_docstore_snapshot_cache).pressure == .hard else false;
        platform_sync.lockYielding(&self.workspace_mutex);
        if (pressure) {
            for (&self.workspaces) |*slot| if (slot.* == workspace) {
                slot.* = null;
                break;
            };
        } else workspace.busy = false;
        self.workspace_mutex.unlock();
        if (pressure) workspace.destroy();
    }

    pub fn close(self: *Store) void {
        if (self.artifact_registry) |registry| registry.detach();
        self.stopMaintenance();
        if (self.workspace_reclaimer) |identity| self.resource_manager.?.unregisterReclaimer(identity);
        std.debug.assert(self.retired_generations == null);
        if (self.read_generation) |generation| std.debug.assert(generation.references == 0);
        for (self.workspace_pending) |pending| std.debug.assert(!pending);
        for (&self.workspaces) |*slot| if (slot.*) |workspace| {
            std.debug.assert(!workspace.busy);
            workspace.destroy();
            slot.* = null;
        };
        self.metadata_pool.deinit();
        self.retireReadGeneration(0);
        self.artifact_marker_sweep.deinit(self.file.runtime());
        self.file.close();
        if (self.artifact_account) |account| account.release();
        if (self.artifact_registry) |registry| registry.releaseOwner();
        self.* = undefined;
    }

    /// Initialize after the returned Store has its stable address. Sources
    /// independently own their runtime and descriptor.
    pub fn openArtifactSource(self: *Store, key: []const u8) !@import("../../segment_source.zig").Source {
        lockStore(self);
        const registry = self.artifact_registry orelse blk: {
            const created = artifact_source.Registry.create(self.allocator, self) catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.artifact_registry = created;
            break :blk created;
        };
        self.mutex.unlock();
        return registry.open(key);
    }

    fn currentGenerationHasReaders(self: *Store) bool {
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        if (self.read_generation) |generation| if (generation.references != 0) return true;
        return if (self.artifact_registry) |registry| registry.hasGeneration(self.assessment_generation) else false;
    }

    fn totalRetiredFileBytes(self: *Store) u64 {
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        return self.retired_file_bytes +| (if (self.artifact_registry) |registry| registry.retired_bytes.load(.acquire) else 0);
    }

    // Called with the owner mutex held, before beginning writer I/O.
    fn initializeReadGenerationAssumeLocked(self: *Store) !void {
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        if (self.read_generation != null) return;
        const generation = try self.allocator.create(ReadGeneration);
        errdefer self.allocator.destroy(generation);
        generation.* = .{ .owner_generation = self.assessment_generation, .checkpoint = self.file.activeCheckpoint(), .scoped_reader_lock = self.file.externally_locked, .file = try native.NativeFile.openWithIo(self.allocator, io, self.file.path, .{ .read_only = true, .internal_reader = self.file.header.indexed_reclamation or self.file.externally_locked, .resource_manager = self.resource_manager }) };
        self.read_generation = generation;
        self.file.secondary_page_cache = &generation.file.page_cache;
        self.file.reader_frontier = &self.reader_frontier;
        self.file.publication_observer = .{ .context = self, .notify = observeReadCheckpoint };
        self.read_outcome_unknown = self.file.checkpoint_publication_uncertain or self.secret_store_uncertain;
    }

    fn observeReadCheckpoint(context: *anyopaque, checkpoint: native.CheckpointSlot, uncertain: bool) void {
        const self: *Store = @ptrCast(@alignCast(context));
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        if (self.read_generation) |generation| generation.checkpoint = checkpoint;
        self.read_outcome_unknown = uncertain or self.secret_store_uncertain;
    }

    fn publishReadCheckpointAssumeLocked(self: *Store) void {
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        if (self.read_generation) |generation| generation.checkpoint = self.file.activeCheckpoint();
        self.read_outcome_unknown = self.file.checkpoint_publication_uncertain or self.secret_store_uncertain;
    }

    // Called with read_mutex held. Pins are ordered by published sequence.
    fn pinInitializedReadGeneration(self: *Store, pin: *ReadPin) !*ReadGeneration {
        const generation = self.read_generation.?;
        // An idle connection must not fence reclamation. Only actual read
        // transactions hold the inode lock, including streams between calls.
        if (generation.scoped_reader_lock and pin.kernel_reader) {
            if (generation.kernel_readers == 0) {
                // Own publication is excluded by the store mutex during pin
                // admission. A forked child can retain a completed writer's
                // inode fence until exec/close, even after its parent closed
                // the descriptor. Wait for that fence instead of failing an
                // otherwise uncontended same-owner read.
                try generation.file.file.lock(generation.file.runtime(), .shared);
            }
            generation.kernel_readers += 1;
        }
        if (generation.references == 0) generation.pinned_at = pin.pinned_at;
        generation.references += 1;
        pin.previous = generation.last_pin;
        if (generation.last_pin) |last| last.next = pin else generation.first_pin = pin;
        generation.last_pin = pin;
        self.reader_frontier.store(generation.first_pin.?.sequence, .release);
        return generation;
    }

    fn pinReadGeneration(self: *Store, pin: *ReadPin) !*ReadGeneration {
        try self.initializeReadGenerationAssumeLocked();
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        return try self.pinInitializedReadGeneration(pin);
    }

    fn retireReadGeneration(self: *Store, old_bytes: u64) void {
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        defer self.read_mutex.unlock(io);
        const ordinary_pinned = if (self.read_generation) |generation| generation.references != 0 else false;
        if (self.artifact_registry) |registry| registry.retire(self.assessment_generation, old_bytes, ordinary_pinned);
        self.assessment_generation +%= 1;
        self.assessment_cache = null;
        self.shrink_waiting_for_writer = false;
        self.maintenance_policy.status.estimated_at_sequence = null;
        self.admission_refresh_size = 0;
        const generation = self.read_generation orelse return;
        self.read_generation = null;
        self.reader_frontier.store(std.math.maxInt(u64), .release);
        self.file.reader_frontier = null;
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
        const io = self.file.runtime();
        self.read_mutex.lockUncancelable(io);
        std.debug.assert(generation.references > 0);
        if (pin.previous) |previous| previous.next = pin.next else generation.first_pin = pin.next;
        if (pin.next) |next| next.previous = pin.previous else generation.last_pin = pin.previous;
        if (generation.scoped_reader_lock and pin.kernel_reader) {
            generation.kernel_readers -= 1;
            if (generation.kernel_readers == 0) generation.file.file.unlock(generation.file.runtime());
        }
        generation.references -= 1;
        if (generation.first_pin) |first| generation.pinned_at = first.pinned_at;
        if (!generation.retired) self.reader_frontier.store(if (generation.first_pin) |first| first.sequence else std.math.maxInt(u64), .release);
        const reclaim = generation.references == 0 and generation.retired;
        self.read_mutex.unlock(io);
        self.readMetadataAllocator().destroy(pin);
        if (reclaim) {
            // Only the last releaser owns this unreachable generation. Preserve
            // owner -> reader lock ordering for admission/accounting updates.
            lockStore(self);
            self.read_mutex.lockUncancelable(io);
            var link = &self.retired_generations;
            while (link.* != generation) link = &link.*.?.next_retired;
            link.* = generation.next_retired;
            self.retired_file_bytes -|= generation.retained_bytes;
            if (self.artifact_registry) |registry| registry.releaseReaderFence(generation.owner_generation);
            generation.file.close();
            self.allocator.destroy(generation);
            self.admission_refresh_size = 0;
            self.read_mutex.unlock(io);
            self.mutex.unlock();
        }
        self.maintenance_wake.set(io);
        // Kick a cold maintenance worker without waiting for publication.
        if (!self.maintenance_started.load(.acquire) and self.mutex.tryLock()) {
            self.startMaintenanceAssumeLocked();
            self.mutex.unlock();
        }
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

    /// Owner-only rebinding after proving immutable index inputs unchanged.
    /// Primary and index publications share the newest allocator/checkpoint;
    /// existing read pins keep their original immutable roots.
    pub fn refreshIndexPublicationCheckpoint(self: *Store, header: native.Header) !void {
        lockStore(self);
        defer self.mutex.unlock();
        if (self.file.transaction_header != null or self.file.durable_header != null or
            self.file.checkpoint_publication_uncertain or self.secret_store_uncertain or
            self.file.header.page_size != header.page_size or
            self.file.header.indexed_reclamation != header.indexed_reclamation) return error.EnrichmentWaitCanceled;
        try self.file.adoptIndexPublicationHeader(header);
        self.assessment_cache = null;
        self.publishReadCheckpointAssumeLocked();
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
        self.startMaintenanceAssumeLocked();
    }

    fn startMaintenanceAssumeLocked(self: *Store) void {
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
        // External owners also defer requested allocator-format conversion to
        // this detached maintenance turn. Vacuum uses the same short capture
        // and checked publication boundaries as an ordinary reclamation copy.
        if (self.maintenance_policy.options.page_reuse and !self.file.header.indexed_reclamation) {
            _ = try self.vacuumWithCancel(cancel);
            return;
        }
        {
            if (self.writer_admission) |admission| try admission.enter(admission.ptr, .reclamation);
            defer if (self.writer_admission) |admission| admission.leave(admission.ptr);
            lockStore(self);
            defer self.mutex.unlock();
            if (self.maintenance_running or self.generation_workspace_active or self.file.change_capture != null) return;
            if (cancel) |token| try token.check();
            const previous_cancel = self.file.allocator_cancel_token;
            self.file.allocator_cancel_token = cancel;
            defer self.file.allocator_cancel_token = previous_cancel;
            if (self.file.checkpoint_publication_uncertain or self.secret_store_uncertain) return error.OutcomeUnknown;
            _ = try artifact_source.cleanupOrphans(&self.file, &self.artifact_cleanup_cursor);
            try self.artifact_marker_sweep.run(&self.file);
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
            if (self.writer_admission) |admission| try admission.enter(admission.ptr, .reclamation);
            defer if (self.writer_admission) |admission| admission.leave(admission.ptr);
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
        if (@import("builtin").is_test) if (self.test_maintenance_hook) |hook| try hook.notify(hook.ptr, .assessment);
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
            if (available < workspace +| self.futureArtifactBytes() +| self.maintenance_policy.options.disk_headroom_bytes) {
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

    fn futureArtifactBytes(self: *const Store) u64 {
        return if (self.artifact_account) |account| account.future_bytes.load(.acquire) else 0;
    }

    pub fn artifactBytes(self: *const Store) u64 {
        return if (self.artifact_account) |account| account.bytes.load(.acquire) else 0;
    }

    /// Retained leases use the storage allocator, never a caller's task-local
    /// budgeting arena: mapped merge output can outlive that task and Store.
    pub fn reserveArtifactBytes(self: *Store, bytes: u64) !*ArtifactLease {
        const allocator = self.allocator;
        lockStore(self);
        defer self.mutex.unlock();
        if (self.artifact_account == null) {
            const account = try self.allocator.create(ArtifactAccount);
            account.* = .{ .allocator = self.allocator };
            self.artifact_account = account;
        }
        const lease = try allocator.create(ArtifactLease);
        errdefer allocator.destroy(lease);
        lease.* = .{ .allocator = allocator, .account = self.artifact_account.? };
        try self.growArtifactBytesAssumeLocked(lease, bytes);
        _ = lease.account.references.fetchAdd(1, .monotonic);
        return lease;
    }

    pub fn growArtifactBytes(self: *Store, lease: *ArtifactLease, bytes: u64) !void {
        if (bytes <= lease.bytes) return;
        lockStore(self);
        defer self.mutex.unlock();
        try self.growArtifactBytesAssumeLocked(lease, bytes);
    }

    fn growArtifactBytesAssumeLocked(self: *Store, lease: *ArtifactLease, bytes: u64) !void {
        if (bytes > std.math.maxInt(u64) - 4095) return error.LiteStorageBudgetExceeded;
        const rounded = std.mem.alignForward(u64, bytes, 4096);
        if (rounded <= lease.bytes) return;
        const additional = rounded - lease.bytes;
        const options = self.maintenance_policy.options;
        const current = (try self.file.file.stat(self.file.runtime())).size;
        if (options.max_storage_bytes != 0 and current +| self.totalRetiredFileBytes() +| self.maintenance_policy.status.temporary_bytes +| self.artifactBytes() +| additional > options.max_storage_bytes) return error.LiteStorageBudgetExceeded;
        if (try self.availableDiskBytes()) |available| {
            const future = self.futureArtifactBytes();
            if (available < future +| additional +| self.maintenance_policy.status.temporary_bytes +| options.disk_headroom_bytes) return error.LiteInsufficientDiskSpace;
        }
        const previous_limit = self.file.max_file_bytes;
        const previous_bytes = lease.bytes;
        _ = lease.account.bytes.fetchAdd(additional, .acq_rel);
        _ = lease.account.future_bytes.fetchAdd(additional, .acq_rel);
        _ = lease.account.revision.fetchAdd(1, .release);
        lease.bytes = rounded;
        errdefer {
            _ = lease.account.bytes.fetchSub(additional, .acq_rel);
            _ = lease.account.future_bytes.fetchSub(additional, .acq_rel);
            _ = lease.account.revision.fetchAdd(1, .release);
            lease.bytes = previous_bytes;
            self.file.max_file_bytes = previous_limit;
        }
        // Native publication must leave this private artifact's capacity free.
        try self.refreshAdmissionAssumeLocked();
    }

    fn availableDiskBytes(self: *Store) !?u64 {
        if (self.maintenance_policy.options.capacity_probe) |probe| return try probe.available(probe.context, self.file.path);
        if (self.file.borrowed_io != null or !antfly_platform.filesystem.capacity_supported) return null;
        return (try antfly_platform.filesystem.capacity(self.file.path)).available_bytes;
    }

    /// Called by serialized adapters before direct native publication.
    pub fn refreshAdmissionAssumeLocked(self: *Store) !void {
        const options = self.maintenance_policy.options;
        self.maintenance_policy.status.artifact_file_bytes = self.artifactBytes();
        const budget_limit: ?u64 = if (options.max_storage_bytes == 0) null else options.max_storage_bytes -| self.totalRetiredFileBytes() -| self.maintenance_policy.status.temporary_bytes -| self.artifactBytes();
        const size = self.file.activeCheckpoint().page_count *| self.file.header.page_size;
        const artifact_revision = if (self.artifact_account) |account| account.revision.load(.acquire) else 0;
        if (size >= self.admission_refresh_size or artifact_revision != self.admission_artifact_revision) {
            if (try self.availableDiskBytes()) |available| {
                self.maintenance_policy.status.available_disk_bytes = available;
                self.disk_admission_limit = size +| (available -| options.disk_headroom_bytes);
            }
            self.admission_refresh_size = size +| options.assessment_bytes;
            self.admission_artifact_revision = artifact_revision;
        }
        // Keep the prepared image's reserved capacity unavailable to tail
        // appends in the current generation throughout copy and catch-up.
        const future_artifact_bytes = self.futureArtifactBytes();
        const disk_limit = if (self.disk_admission_limit) |limit| limit -| self.maintenance_policy.status.temporary_bytes -| future_artifact_bytes else null;
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
        status.retired_file_bytes = self.totalRetiredFileBytes();
        status.artifact_file_bytes = self.artifactBytes();
        status.current_file_bytes = (try self.file.file.stat(self.file.runtime())).size;
        const now = std.Io.Clock.awake.now(self.file.runtime());
        self.read_mutex.lockUncancelable(self.file.runtime());
        {
            defer self.read_mutex.unlock(self.file.runtime());
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
            if (self.writer_admission) |admission| try admission.enter(admission.ptr, .reclamation);
            defer if (self.writer_admission) |admission| admission.leave(admission.ptr);
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
            if (budget != 0 and status.current_file_bytes +| status.retired_file_bytes +| self.artifactBytes() +| workspace > budget) return error.LiteStorageBudgetExceeded;
            try self.admitEstimatedGenerationAssumeLocked(if (compact != 0) compact else self.maintenance_policy.status.compact_size_estimate orelse workspace / 2, status.current_file_bytes);
            if (try self.availableDiskBytes()) |available| {
                self.maintenance_policy.status.available_disk_bytes = available;
                if (available < workspace +| self.futureArtifactBytes() +| self.maintenance_policy.options.disk_headroom_bytes) return error.LiteInsufficientDiskSpace;
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
        if (@import("builtin").is_test) if (self.test_maintenance_hook) |hook| try hook.notify(hook.ptr, .copy);
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
                    const retained_old = self.currentGenerationHasReaders();
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
        if (self.writer_admission) |admission| try admission.enter(admission.ptr, .reclamation);
        defer if (self.writer_admission) |admission| admission.leave(admission.ptr);
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
            limit = options.max_storage_bytes -| status.current_file_bytes -| status.retired_file_bytes -| self.artifactBytes();
            if (limit.? < minimum) return error.LiteStorageBudgetExceeded;
        }
        if (try self.availableDiskBytes()) |available| {
            const disk_limit = available -| options.disk_headroom_bytes -| self.futureArtifactBytes();
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
        if (self.artifact_registry) |registry| registry.expireIdle(self.assessment_generation);
        const retained_old: u64 = if (self.currentGenerationHasReaders()) old_bytes else 0;
        if (retained_old +| self.totalRetiredFileBytes() +| self.artifactBytes() +| compact_bytes +| self.file.estimatedGenerationReserveBytes(compact_bytes) > budget) return error.LiteStorageBudgetExceeded;
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
        const total = old_bytes +| self.totalRetiredFileBytes() +| self.artifactBytes() +| prepared_bytes;
        // Idle cache ownership must not reserve a retired inode. This runs
        // inside the generation fence, so expired sources can only reopen
        // after adoption; active uses remain charged by hasGeneration.
        if (self.artifact_registry) |registry| registry.expireIdle(self.assessment_generation);
        const retained_old: u64 = if (self.currentGenerationHasReaders()) old_bytes else 0;
        const adopted_total = retained_old +| self.totalRetiredFileBytes() +| self.artifactBytes() +| prepared_bytes;
        if (options.max_storage_bytes != 0) {
            if (total > options.max_storage_bytes or options.max_storage_bytes -| adopted_total +| reusable < reserve) return error.LiteStorageBudgetExceeded;
        }
        if (try self.availableDiskBytes()) |available| {
            if (cancel) |token| try token.check();
            if (available < options.disk_headroom_bytes +| self.futureArtifactBytes() +| (reserve -| reusable)) return error.LiteInsufficientDiskSpace;
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

    pub fn submitIndexMutation(self: *Store, context: *anyopaque, apply: *const fn (*anyopaque, *native.NativeFile) anyerror!void, durable: bool) !void {
        return self.submitScopedMutation(context, apply, durable, true);
    }

    pub fn submitMutationWithDurability(self: *Store, context: *anyopaque, apply: *const fn (*anyopaque, *native.NativeFile) anyerror!void, durable: bool) !void {
        return self.submitScopedMutation(context, apply, durable, false);
    }

    fn submitScopedMutation(self: *Store, context: *anyopaque, apply: *const fn (*anyopaque, *native.NativeFile) anyerror!void, durable: bool, index_only: bool) !void {
        if (self.read_only) return error.ReadOnly;
        defer {
            // Start after publication: even one large final mutation crossing
            // the threshold must launch maintenance without another write.
            self.startMaintenance();
            self.maintenance_wake.set(self.file.runtime());
        }
        const io = self.file.runtime();
        var request = MutationRequest{ .context = context, .apply = apply, .durable = durable, .index_only = index_only };
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
        var index_only = true;
        var member: ?*MutationRequest = head;
        while (member) |item| : (member = item.next) index_only = index_only and item.index_only;
        if (self.writer_admission) |admission| try admission.enter(admission.ptr, if (index_only) .index else .primary);
        defer if (self.writer_admission) |admission| admission.leave(admission.ptr);
        lockStore(self);
        defer self.mutex.unlock();
        if (self.secret_store_uncertain) return error.OutcomeUnknown;
        try self.refreshAdmissionAssumeLocked();
        try self.initializeReadGenerationAssumeLocked();
        errdefer self.publishReadCheckpointAssumeLocked();
        try self.file.reclaimRetirementPressure();
        try self.file.beginTransaction();
        errdefer self.file.abortTransaction();
        var current: ?*MutationRequest = head;
        // Detached writers release their kernel lease after this group. The
        // next owner must see its allocator and index roots on disk; a volatile
        // durable_header cannot safely survive that ownership handoff.
        var durable = self.writer_admission != null;
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
        if (self.writer_admission) |admission| try admission.enter(admission.ptr, .primary);
        errdefer if (self.writer_admission) |admission| admission.leave(admission.ptr);
        const io = self.file.runtime();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        if (self.writer_active or self.next_writer_ticket != self.serving_writer_ticket) return error.FileBusy;
        self.writer_active = true;
        self.writer_ticketed = false;
    }

    pub fn reserveWriterSlotYielding(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        if (self.writer_admission) |admission| try admission.enter(admission.ptr, .primary);
        errdefer if (self.writer_admission) |admission| admission.leave(admission.ptr);
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
            if (self.writer_admission) |admission| admission.leave(admission.ptr);
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

    pub fn decodedResultsNamespace(self: *const Txn) []const u8 {
        return self.prefix;
    }

    pub fn canReuseDecodedResults(self: *Txn) bool {
        if (!self.read_only or self.pending_count != 0) return false;
        const file = self.readFile() catch return false;
        return file.page_cache_enabled.load(.monotonic) and file.page_cache_bypass.load(.monotonic) == 0;
    }

    pub fn metadataAllocator(self: *const Txn, fallback: Allocator) Allocator {
        return if (self.store) |store| store.readMetadataAllocator() else fallback;
    }

    pub fn openRead(store: *Store) !Txn {
        return try openReadWithPrefix(store, "");
    }

    pub fn openReadWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        if (store.read_only and !store.file.externally_locked) return .{ .allocator = store.allocator, .store = store, .prefix = prefix, .checkpoint = store.file.activeCheckpoint() };
        const io = store.file.runtime();
        store.generation_lock.lockSharedUncancelable(io);
        defer store.generation_lock.unlockShared(io);
        // Scoped inode pins share the complete native publication mutex.
        // Keep it while waiting for the kernel fence, so an own writer cannot
        // wait for read_mutex in its checkpoint observer behind that wait.
        // Lifetime-owned readers already retain an inode fence and preserve
        // admission during commit fsync.
        const serialized_admission = store.file.externally_locked;
        if (serialized_admission) lockStore(store);
        defer if (serialized_admission) store.mutex.unlock();
        const pin = try store.readMetadataAllocator().create(ReadPin);
        errdefer store.readMetadataAllocator().destroy(pin);
        store.read_mutex.lockUncancelable(io);
        if (store.read_generation == null) {
            store.read_mutex.unlock(io);
            if (!serialized_admission) lockStore(store);
            store.initializeReadGenerationAssumeLocked() catch |err| {
                if (!serialized_admission) store.mutex.unlock();
                return err;
            };
            if (!serialized_admission) store.mutex.unlock();
            store.read_mutex.lockUncancelable(io);
        }
        defer store.read_mutex.unlock(io);
        if (store.read_outcome_unknown) return error.OutcomeUnknown;
        const checkpoint = store.read_generation.?.checkpoint;
        // Nested reads borrow the current writer's publication lease. Taking
        // another inode lock here would conflict with its page allocator's
        // exclusive reuse lock. Persistent SQL streams open outside a writer
        // transaction and retain their own kernel reader fence instead.
        store.writer_mutex.lockUncancelable(io);
        const kernel_reader = !store.file.externally_locked or !store.writer_active;
        store.writer_mutex.unlock(io);
        pin.* = .{ .sequence = checkpoint.commit_sequence, .pinned_at = std.Io.Clock.awake.now(io), .kernel_reader = kernel_reader };
        return .{ .allocator = store.allocator, .store = store, .read_pin = pin, .prefix = prefix, .read_generation = try store.pinInitializedReadGeneration(pin), .checkpoint = checkpoint };
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
        const pin = if (store.file.header.indexed_reclamation) try store.readMetadataAllocator().create(ReadPin) else null;
        errdefer if (pin) |owned| store.readMetadataAllocator().destroy(owned);
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
        const pin = if (store.file.header.indexed_reclamation) try store.readMetadataAllocator().create(ReadPin) else null;
        errdefer if (pin) |owned| store.readMetadataAllocator().destroy(owned);
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

    /// Caller owns the result. Unlike get(), this does not retain keys or
    /// values in the transaction. Pending mutations still take precedence.
    /// Pending mutations take precedence; persisted presence validates only
    /// record headers and keys, never external value pages.
    pub fn contains(self: *Txn, key: []const u8) !bool {
        var cursor = try self.openKeyCursor();
        defer cursor.close();
        const found = cursor.seekAtOrAfter(key) catch |err| return if (err == error.NotFound) false else err;
        return std.mem.eql(u8, found, key);
    }

    pub fn getOwned(self: *Txn, allocator: Allocator, key: []const u8) ![]u8 {
        const lookup_key = if (self.prefix.len == 0) key else try std.mem.concat(allocator, u8, &.{ self.prefix, key });
        defer if (self.prefix.len > 0) allocator.free(lookup_key);
        if (self.pending_tree.getEntryFor(lookup_key).node) |node| {
            const value = self.pending.items[pendingNode(node).ordinal].value orelse return error.NotFound;
            return allocator.dupe(u8, value);
        }
        return (try (try self.readFile()).getDocumentAtCheckpointAlloc(allocator, self.checkpoint, lookup_key)) orelse error.NotFound;
    }

    pub fn getInto(self: *Txn, key: []const u8, out: []u8) ![]const u8 {
        const full = try self.prefixedKey(key);
        defer if (self.prefix.len > 0) self.allocator.free(full);
        if (self.pending_tree.getEntryFor(full).node) |node| {
            const value = self.pending.items[pendingNode(node).ordinal].value orelse return error.NotFound;
            if (value.len > out.len) return error.BufferTooSmall;
            @memcpy(out[0..value.len], value);
            return out[0..value.len];
        }
        return (try self.readFile()).readDocumentInto(self.checkpoint, full, out);
    }

    pub const ReadScope = struct {
        parent: *Txn,
        workspace: ?*ReadWorkspace = null,
        scratch: @import("result_scratch.zig").Scratch,
        fallback: ?*Fallback = null,
        discard_workspace: bool = false,
        score_working: ?@import("result_scratch.zig").Scratch = null,
        const Fallback = struct {
            allocator: Allocator,
            batch: read_scratch,
            native_state: native.DocumentBatchReadState,
        };
        fn fallbackState(self: *@This()) !*Fallback {
            if (self.fallback) |state| return state;
            const a = self.scratch.arena.preferred_allocator orelse self.scratch.arena.child_allocator;
            const state = try a.create(Fallback);
            state.* = .{ .allocator = a, .batch = read_scratch.init(a, 256 * 1024), .native_state = .{ .allocator = a } };
            self.fallback = state;
            return state;
        }

        pub fn metadataAllocator(self: *const @This(), fallback: Allocator) Allocator {
            // Opaque caller allocators keep their own accounting/failure policy.
            if (self.workspace == null) return fallback;
            return self.parent.metadataAllocator(fallback);
        }
        pub fn metadataResidentManager(self: *const @This()) ?*resource_manager_mod.ResourceManager {
            const store = self.parent.store orelse return null;
            return store.metadata_pool.residentManager();
        }

        pub fn preparedScorePlan(self: *@This()) ?*@import("../prepared_score_plan.zig").PreparedScorePlan {
            const w = self.workspace orelse return null;
            // The exposed plan may supply keys to legacy visitors too. Protect
            // it for the entire lease, including allocations between reads.
            w.score_plan_borrowed = true;
            return &w.score_plan;
        }

        pub fn preparedScoreAllocator(self: *@This()) Allocator {
            return self.workspace.?.optionalAllocator();
        }

        pub fn workingAllocator(self: *@This()) Allocator {
            if (self.workspace) |w| return w.graph.allocator();
            if (self.score_working) |*working| return working.allocator();
            return self.scratch.arena.preferred_allocator orelse self.scratch.arena.child_allocator;
        }
        fn batchScratch(self: *@This()) !*read_scratch {
            return if (self.workspace) |w| &w.batch else &(try self.fallbackState()).batch;
        }
        fn resetBatchScratch(self: *@This()) void {
            if (self.workspace) |w| w.resetBatch() else if (self.fallback) |state| state.batch.reset();
        }
        fn nativeState(self: *@This()) !*native.DocumentBatchReadState {
            const state = if (self.workspace) |w| &w.native_state else &(try self.fallbackState()).native_state;
            if (self.parent.store) |store| state.binding = .{
                .owner = store.score_cache_owner,
                .generation = if (self.parent.read_generation) |g| g.owner_generation else store.assessment_generation,
            };
            return state;
        }

        fn resultAllocator(self: *@This()) Allocator {
            if (self.workspace) |w| if (self.scratch.arena.preferred_allocator == null) {
                const backing = self.scratch.arena.child_allocator;
                self.scratch.arena.preferred_allocator = backing;
                self.scratch.arena.child_allocator = w.resultAllocator(backing);
            };
            return self.scratch.allocator();
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.parent.getOwned(self.resultAllocator(), key);
        }

        pub fn getInto(self: *@This(), key: []const u8, out: []u8) ![]const u8 {
            if (self.parent.prefix.len == 0) return self.parent.getInto(key, out);
            defer self.resetBatchScratch();
            const full = try std.mem.concat((try self.batchScratch()).allocator(), u8, &.{ self.parent.prefix, key });
            if (self.parent.pending_tree.getEntryFor(full).node) |node| {
                const value = self.parent.pending.items[pendingNode(node).ordinal].value orelse return error.NotFound;
                if (value.len > out.len) return error.BufferTooSmall;
                @memcpy(out[0..value.len], value);
                return out[0..value.len];
            }
            return (try self.parent.readFile()).readDocumentInto(self.parent.checkpoint, full, out);
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            return self.readBatch(keys, null, values);
        }

        /// Output buffers must not overlap each other or the input keys.
        /// Output belongs to the caller, independently of scope reset/close.
        pub fn getManySortedInto(self: *@This(), keys: []const []const u8, buffers: []const []u8, values: []?[]const u8) !void {
            return self.readBatch(keys, buffers, values);
        }

        pub fn readSortedScoreColumn(self: *@This(), prefix: []const u8, nodes: []const []const u8, values: []f64) !usize {
            if (nodes.len != values.len or nodes.len > 4096) return error.InvalidBatch;
            @memset(values, std.math.nan(f64));
            errdefer @memset(values, std.math.nan(f64));
            for (nodes, 0..) |node, i| if (i != 0 and std.mem.order(u8, nodes[i - 1], node) != .lt) return error.InvalidBatch;
            var offset: usize = 0;
            var batches: usize = 0;
            while (offset < nodes.len) {
                var end = offset;
                var bytes: usize = 0;
                while (end < nodes.len) : (end += 1) {
                    const size = try std.math.add(usize, self.parent.prefix.len, try std.math.add(usize, prefix.len, @import("../internal_keys.zig").encodedComponentLen(nodes[end])));
                    if (end != offset and size > 1024 * 1024 -| bytes) break;
                    bytes = try std.math.add(usize, bytes, size);
                }
                const previous_failure = if (self.workspace) |w| w.allocation_failed else false;
                self.readSortedScoreColumnInner(prefix, nodes[offset..end], values[offset..end]) catch |err| {
                    if (err == error.OutOfMemory or err == error.ResourceBudgetExceeded) self.discard_workspace = true;
                    return err;
                };
                if (self.workspace) |w| if (w.resolved_scores.recovered_optional_failure) {
                    w.resolved_scores.recovered_optional_failure = false;
                    w.allocation_failed = previous_failure;
                };
                offset = end;
                batches += 1;
            }
            return batches;
        }
        fn readSortedScoreColumnInner(self: *@This(), prefix: []const u8, nodes: []const []const u8, values: []f64) !void {
            defer self.resetBatchScratch();
            const temporary = (try self.batchScratch()).allocator();
            const encoder = self.workingAllocator();
            const encoding = @import("../internal_keys.zig");
            const namespace = if (self.parent.pending_count != 0) "" else self.parent.prefix;
            var bytes: usize = 0;
            for (nodes) |node| {
                bytes = try std.math.add(usize, bytes, try std.math.add(usize, namespace.len, try std.math.add(usize, prefix.len, encoding.encodedComponentLen(node))));
            }
            // One oversized key may progress under allocator admission, matching
            // the generic reader. Multi-key spans stay byte bounded as well.
            if (nodes.len > 1 and bytes > 1024 * 1024) return error.InvalidBatch;
            // One allocation keeps release LIFO even when the arena crosses
            // chunks. Separate key/byte owners can leave earlier chunks live
            // for every span of a large ordinary read.
            const metadata = try std.math.mul(usize, nodes.len, @sizeOf([]const u8));
            const storage_size = try std.math.add(usize, metadata, bytes);
            const storage = if (self.workspace) |w| try w.scoreKeyStorage(storage_size) else try encoder.alignedAlloc(u8, .fromByteUnits(@alignOf([]const u8)), storage_size);
            defer if (self.workspace == null) encoder.free(storage);
            const keys = @as([*][]const u8, @ptrCast(@alignCast(storage.ptr)))[0..nodes.len];
            var encoded = std.ArrayListUnmanaged(u8).empty;
            encoded.items = storage[metadata..][0..0];
            encoded.capacity = bytes;
            for (nodes, keys) |node, *key| {
                const begin = encoded.items.len;
                encoded.appendSliceAssumeCapacity(namespace);
                encoded.appendSliceAssumeCapacity(prefix);
                try encoding.appendEncodedComponent(&encoded, encoder, node);
                key.* = encoded.items[begin..];
            }
            if (self.parent.pending_count != 0) {
                // Overlay visibility keeps the established complete-reader path.
                // visitScores adds the transaction namespace to these keys.
                const Output = struct {
                    values: []f64,
                    fn write(ctx: *anyopaque, index: usize, value: ?f64) void {
                        const out: *@This() = @ptrCast(@alignCast(ctx));
                        out.values[index] = value orelse std.math.nan(f64);
                    }
                };
                var output = Output{ .values = values };
                return self.visitScores(keys, backend_types.CompleteScoreSink{ .context = &output, .write = Output.write });
            }
            const file = try self.parent.readFile();
            try file.readDecodedScoresAtCheckpointWithState(temporary, self.parent.checkpoint, keys, values, try self.nativeState());
        }

        pub fn visitManySortedScores(self: *@This(), keys: []const []const u8, sink: backend_types.ScoreSink) !void {
            const previous_failure = if (self.workspace) |w| w.allocation_failed else false;
            self.visitScores(keys, sink) catch |err| {
                if (err == error.OutOfMemory or err == error.ResourceBudgetExceeded) self.discard_workspace = true;
                return err;
            };
            if (self.workspace) |w| if (w.resolved_scores.recovered_optional_failure) {
                w.resolved_scores.recovered_optional_failure = false;
                w.allocation_failed = previous_failure;
            };
        }

        /// Accept only this lease's owned plan, never an arbitrary sorted flag.
        pub fn visitPreparedScores(self: *@This(), plan: *@import("../prepared_score_plan.zig").PreparedScorePlan, sink: backend_types.CompleteScoreSink) !void {
            if (self.preparedScorePlan() != plan or !plan.valid) return error.InvalidBatch;
            const previous_failure = self.workspace.?.allocation_failed;
            self.readPreparedScores(plan, sink) catch |err| {
                if (err == error.OutOfMemory or err == error.ResourceBudgetExceeded) self.discard_workspace = true;
                return err;
            };
            const w = self.workspace.?;
            if (w.resolved_scores.recovered_optional_failure) {
                w.resolved_scores.recovered_optional_failure = false;
                w.allocation_failed = previous_failure;
            }
        }

        /// Caller-owned requests remain immutable for this call. The native
        /// cache owns its identity and decoded scores; it never retains the
        /// request pointer.
        pub fn visitPreparedRequest(self: *@This(), request: *const @import("../prepared_score_request.zig").PreparedScoreRequest, sink: backend_types.CompleteScoreSink) !void {
            if (request.canonicalIdentity() == 0) return error.InvalidBatch;
            if (request.readKeys().len == 0) return;
            const previous_failure = if (self.workspace) |w| w.allocation_failed else false;
            self.readOwnedScores(request, sink) catch |err| {
                if (err == error.OutOfMemory or err == error.ResourceBudgetExceeded) self.discard_workspace = true;
                return err;
            };
            if (self.workspace) |w| if (w.resolved_scores.recovered_optional_failure) {
                w.resolved_scores.recovered_optional_failure = false;
                w.allocation_failed = previous_failure;
            };
        }

        fn readOwnedScores(self: *@This(), request: *const @import("../prepared_score_request.zig").PreparedScoreRequest, sink: backend_types.CompleteScoreSink) !void {
            if (self.parent.pending_count != 0) return self.visitScores(request.readKeys(), sink);
            defer self.resetBatchScratch();
            const file = try self.parent.readFile();
            return file.visitOwnedScoresAtCheckpointWithState((try self.batchScratch()).allocator(), self.parent.checkpoint, request, self.parent.prefix, sink, try self.nativeState());
        }

        fn readPreparedScores(self: *@This(), plan: *const @import("../prepared_score_plan.zig").PreparedScorePlan, sink: backend_types.CompleteScoreSink) !void {
            if (self.parent.pending_count != 0) return self.visitScores(plan.read_keys, sink);
            defer self.resetBatchScratch();
            const file = try self.parent.readFile();
            return file.visitPreparedScoresAtCheckpointWithState((try self.batchScratch()).allocator(), self.parent.checkpoint, plan, self.parent.prefix, sink, try self.nativeState());
        }

        fn visitScores(self: *@This(), keys: []const []const u8, sink: anytype) !void {
            const complete = comptime @TypeOf(sink) == backend_types.CompleteScoreSink;
            var unique = true;
            if (!complete) for (keys, 0..) |key, i| {
                if (i == 0) continue;
                switch (std.mem.order(u8, keys[i - 1], key)) {
                    .gt => return error.InvalidBatch,
                    .eq => unique = false,
                    .lt => {},
                }
            };
            if (unique and self.parent.prefix.len == 0 and self.parent.pending_count == 0) {
                defer self.resetBatchScratch();
                const file = try self.parent.readFile();
                const a = (try self.batchScratch()).allocator();
                const state = try self.nativeState();
                if (complete) return file.visitCompleteScoresAtCheckpointWithState(a, self.parent.checkpoint, keys, sink, state);
                return file.visitScoresAtCheckpointWithState(a, self.parent.checkpoint, keys, sink, state);
            }
            defer self.resetBatchScratch();
            const temporary = (try self.batchScratch()).allocator();
            const physical_keys = try temporary.alloc([]const u8, keys.len);
            const positions = try temporary.alloc(usize, keys.len);
            var namespace_bytes: usize = 0;
            if (self.parent.prefix.len != 0) for (keys, 0..) |key, index| {
                if (!complete and index != 0 and std.mem.eql(u8, keys[index - 1], key)) continue;
                namespace_bytes = std.math.add(usize, namespace_bytes, self.parent.prefix.len) catch return error.InvalidBatch;
                namespace_bytes = std.math.add(usize, namespace_bytes, key.len) catch return error.InvalidBatch;
            };
            const encoded = try temporary.alloc(u8, namespace_bytes);
            var encoded_offset: usize = 0;
            var count: usize = 0;
            var i: usize = 0;
            while (i < keys.len) {
                var end = i + 1;
                if (!complete) while (end < keys.len and std.mem.eql(u8, keys[i], keys[end])) : (end += 1) {};
                const full = if (self.parent.prefix.len == 0) keys[i] else blk: {
                    const begin = encoded_offset;
                    @memcpy(encoded[encoded_offset..][0..self.parent.prefix.len], self.parent.prefix);
                    encoded_offset += self.parent.prefix.len;
                    @memcpy(encoded[encoded_offset..][0..keys[i].len], keys[i]);
                    encoded_offset += keys[i].len;
                    break :blk encoded[begin..encoded_offset];
                };
                if (self.parent.pending_tree.getEntryFor(full).node) |node| {
                    if (self.parent.pending.items[pendingNode(node).ordinal].value) |raw| {
                        const score = try native.decodeScore(raw);
                        for (i..end) |index| sink.write(sink.context, index, score);
                    } else if (complete) {
                        for (i..end) |index| sink.write(sink.context, index, null);
                    }
                } else {
                    physical_keys[count] = full;
                    positions[count] = i;
                    count += 1;
                }
                i = end;
            }
            if (count == 0) return;
            const Output = struct {
                keys: []const []const u8,
                positions: []const usize,
                sink: @TypeOf(sink),
                fn write(ctx: *anyopaque, index: usize, score: if (complete) ?f64 else f64) void {
                    const out: *@This() = @ptrCast(@alignCast(ctx));
                    const start = out.positions[index];
                    var end = start + 1;
                    if (!complete) while (end < out.keys.len and std.mem.eql(u8, out.keys[start], out.keys[end])) : (end += 1) {};
                    for (start..end) |logical| out.sink.write(out.sink.context, logical, score);
                }
            };
            var output = Output{ .keys = keys, .positions = positions[0..count], .sink = sink };
            const file = try self.parent.readFile();
            const state = try self.nativeState();
            if (complete) return file.visitCompleteScoresAtCheckpointWithState(temporary, self.parent.checkpoint, physical_keys[0..count], .{ .context = &output, .write = Output.write }, state);
            return file.visitScoresAtCheckpointWithState(temporary, self.parent.checkpoint, physical_keys[0..count], .{ .context = &output, .write = Output.write }, state);
        }

        pub fn getManySortedScores(self: *@This(), keys: []const []const u8, scores: []?f64) !void {
            if (keys.len != scores.len) return error.InvalidBatch;
            @memset(scores, null);
            errdefer @memset(scores, null);
            const Output = struct {
                scores: []?f64,
                fn write(ctx: *anyopaque, index: usize, score: f64) void {
                    const out: *@This() = @ptrCast(@alignCast(ctx));
                    out.scores[index] = score;
                }
            };
            var output = Output{ .scores = scores };
            return self.visitManySortedScores(keys, .{ .context = &output, .write = Output.write });
        }

        fn readBatch(self: *@This(), keys: []const []const u8, buffers: ?[]const []u8, values: []?[]const u8) !void {
            if (buffers) |out| if (out.len != keys.len) return error.InvalidBatch;
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);
            errdefer @memset(values, null);
            if (keys.len == 0) return;
            for (keys, 0..) |key, i| {
                if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
            }
            var unique_count: usize = 0;
            for (keys, 0..) |key, i| {
                if (i == 0 or !std.mem.eql(u8, keys[i - 1], key)) unique_count += 1;
            }
            defer self.resetBatchScratch();
            const temporary = (try self.batchScratch()).allocator();
            const alloc = if (buffers == null) self.resultAllocator() else self.scratch.allocator();
            if (unique_count == keys.len and self.parent.prefix.len == 0 and self.parent.pending_count == 0) {
                return (try self.parent.readFile()).getDocumentsAtCheckpointWithState(alloc, temporary, self.parent.checkpoint, keys, buffers, values, try self.nativeState());
            }
            const pending_keys = try temporary.alloc([]const u8, unique_count);
            const positions = try temporary.alloc(usize, unique_count);
            const loaded = try temporary.alloc(?[]const u8, unique_count);
            const pending_buffers = if (buffers != null) try temporary.alloc([]u8, unique_count) else null;
            var count: usize = 0;
            var i: usize = 0;
            while (i < keys.len) {
                var end = i + 1;
                while (end < keys.len and std.mem.eql(u8, keys[i], keys[end])) : (end += 1) {}
                const full = if (self.parent.prefix.len == 0) keys[i] else try std.mem.concat(temporary, u8, &.{ self.parent.prefix, keys[i] });
                if (self.parent.pending_tree.getEntryFor(full).node) |node| {
                    if (self.parent.pending.items[pendingNode(node).ordinal].value) |value| {
                        if (buffers) |out| {
                            for (out[i..end], values[i..end]) |buffer, *result| {
                                if (value.len > buffer.len) return error.BufferTooSmall;
                                @memcpy(buffer[0..value.len], value);
                                result.* = buffer[0..value.len];
                            }
                        } else @memset(values[i..end], try alloc.dupe(u8, value));
                    }
                } else {
                    pending_keys[count] = full;
                    positions[count] = i;
                    if (pending_buffers) |out| out[count] = buffers.?[i];
                    count += 1;
                }
                i = end;
            }
            if (count == 0) return;
            try (try self.parent.readFile()).getDocumentsAtCheckpointWithState(alloc, temporary, self.parent.checkpoint, pending_keys[0..count], if (pending_buffers) |out| out[0..count] else null, loaded[0..count], try self.nativeState());
            for (loaded[0..count], positions[0..count]) |value, position| {
                var end = position + 1;
                while (end < keys.len and std.mem.eql(u8, keys[position], keys[end])) : (end += 1) {}
                if (buffers) |out| {
                    if (value) |bytes| for (position..end) |index| {
                        if (bytes.len > out[index].len) return error.BufferTooSmall;
                        if (index != position) @memcpy(out[index][0..bytes.len], bytes);
                        values[index] = out[index][0..bytes.len];
                    };
                } else @memset(values[position..end], value);
            }
        }

        /// Invalidates all previously returned values, retaining bounded scratch.
        pub fn reset(self: *@This()) void {
            self.scratch.reset();
        }

        pub fn beginScoreBatch(self: *@This()) void {
            if (self.workspace == null and self.score_working == null) {
                self.score_working = @import("result_scratch.zig").Scratch.init(self.scratch.arena.preferred_allocator orelse self.scratch.arena.child_allocator, 256 * 1024);
            }
        }

        /// Score callers retain no result slices or active prepared-plan borrow
        /// across this boundary. Release leases and reset both working arenas.
        pub fn resetScoreBatch(self: *@This()) void {
            self.scratch.reset();
            if (self.workspace) |w| {
                if (self.discard_workspace) {
                    w.score_plan_borrowed = false;
                    w.discardRetainedStorage();
                    self.discard_workspace = false;
                }
                w.resetScoreBatch();
            } else if (self.fallback) |state| {
                state.native_state.detach();
                state.batch.reset();
            }
            if (self.score_working) |*working| working.reset();
        }

        pub fn close(self: *@This()) void {
            self.scratch.deinit();
            if (self.fallback) |state| {
                state.native_state.deinit();
                state.batch.deinit();
                state.allocator.destroy(state);
            }
            if (self.score_working) |*working| working.deinit();
            if (self.workspace) |w| {
                w.score_plan_borrowed = false;
                if (self.discard_workspace) w.discardRetainedStorage();
                self.parent.store.?.returnWorkspace(w);
            }
            self.* = undefined;
        }
    };

    /// Raw callers must close scopes before moving or ending the parent.
    /// Erased adapters retain the parent automatically. Scope results are
    /// owned copies, so subsequent pending mutations cannot invalidate them.
    pub fn openReadScope(self: *Txn, allocator: Allocator) !ReadScope {
        const workspace = if (self.store) |store| try store.checkoutWorkspace(allocator) else null;
        return .{ .parent = self, .workspace = workspace, .scratch = @import("result_scratch.zig").Scratch.init(allocator, 256 * 1024) };
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

    pub fn openIndexValue(self: *Txn, allocator: Allocator, key: []const u8) !native.NativeFile.IndexValue {
        return (try self.readFile()).openIndexValue(allocator, key, self.checkpoint);
    }

    pub fn readIndexValueInto(self: *Txn, value: native.NativeFile.IndexValue, offset: u64, out: []u8) !void {
        return (try self.readFile()).readIndexValueInto(value, offset, out, self.checkpoint);
    }

    pub fn checksumIndexValue(self: *Txn, value: native.NativeFile.IndexValue, offset: u64, length: u64) !u32 {
        return (try self.readFile()).checksumIndexValue(value, offset, length, self.checkpoint);
    }

    pub fn readAuthenticatedIndexValue(self: *Txn, value: native.NativeFile.IndexValue, offset: u64, length: u64, within: usize, out: []u8, expected: ?u32) !void {
        return (try self.readFile()).readAuthenticatedIndexValue(value, offset, length, within, out, expected, self.checkpoint);
    }

    pub fn copyIndexValueTo(self: *Txn, value: native.NativeFile.IndexValue, destination: std.Io.File) !void {
        return (try self.readFile()).copyIndexValueTo(value, destination, self.checkpoint);
    }

    /// The pinned descriptor is authoritative even after inode adoption.
    pub fn readFile(self: *Txn) !*native.NativeFile {
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

    pub fn setKeysOnly(self: *Cursor) void {
        self.load_values = false;
    }

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

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native docstore runtime persists atomic batch" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
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
    var tmp = std.testing.tmpDir(.{});
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
    var tmp = std.testing.tmpDir(.{});
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

    var tmp = std.testing.tmpDir(.{});
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
    var tmp = std.testing.tmpDir(.{});
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

    var tmp = std.testing.tmpDir(.{});
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

test "lite externally locked artifact capture publishes under its generation lease" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "artifact-publication-admission.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{
        .io = std.testing.io,
        .no_sync = true,
        .externally_locked = true,
    });
    defer store.close();
    try store.file.putIndexCatalogRecord("index/run", "contents");
    var source = try store.openArtifactSource("index/run");
    defer source.close();
    var bytes: [8]u8 = undefined;
    try source.readInto(0, &bytes);
    try std.testing.expectEqualStrings("contents", &bytes);
}

test "lite externally locked read pins wait for catalog and document publication" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "publication-read-admission.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{
        .io = std.testing.io,
        .no_sync = true,
        .externally_locked = true,
    });
    defer store.close();
    var seed = try store.beginWrite();
    try seed.put("doc", "before");
    try seed.commit();
    const Writer = struct {
        store: *Store,
        publishing: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try file.putDocument("doc", "after");
            try file.putCatalogRecord("index-metadata", "after");
            self.publishing.store(true, .release);
            while (!self.release.load(.acquire))
                file.runtime().sleep(.fromMilliseconds(1), .awake) catch {};
        }
        fn run(self: *@This()) void {
            self.store.submitMutationWithDurability(self, apply, false) catch |err| {
                self.failure = err;
                self.publishing.store(true, .release);
            };
        }
    };
    const Reader = struct {
        store: *Store,
        started: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.read() catch |err| {
                self.failure = err;
            };
        }
        fn read(self: *@This()) !void {
            var txn = try self.store.beginRead();
            defer txn.abort();
            try std.testing.expectEqualStrings("after", try txn.get("doc"));
        }
    };
    var writer = Writer{ .store = &store };
    const writer_thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Writer.run, .{&writer});
    defer {
        writer.release.store(true, .release);
        writer_thread.join();
    }
    while (!writer.publishing.load(.acquire)) std.atomic.spinLoopHint();
    if (writer.failure) |err| return err;
    var reader = Reader{ .store = &store };
    const reader_thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Reader.run, .{&reader});
    var reader_joined = false;
    defer if (!reader_joined) {
        writer.release.store(true, .release);
        reader_thread.join();
    };
    while (!reader.started.load(.acquire)) std.atomic.spinLoopHint();
    // The writer retains its native reuse lock across this interval. A read
    // arriving here must queue, not turn an internal collision into FileBusy.
    try std.testing.io.sleep(.fromMilliseconds(20), .awake);
    writer.release.store(true, .release);
    reader_thread.join();
    reader_joined = true;
    if (reader.failure) |err| return err;
}

test "lite externally locked read pins wait for fork-inherited publication fences" {
    const os = @import("builtin").os.tag;
    if (os != .macos and os != .linux) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "inherited-publication-fence.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{
        .io = io,
        .no_sync = true,
        .externally_locked = true,
    });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var seed = try store.beginWrite();
    try seed.put("doc", "committed");
    try seed.commit();
    store.file.releaseWriterOwnership();

    var pipe: [2]std.c.fd_t = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.pipe(&pipe));
    defer _ = std.c.close(pipe[0]);
    defer _ = std.c.close(pipe[1]);
    const fence = try std.Io.Dir.cwd().openFile(io, path, .{
        .lock = .exclusive,
        .lock_nonblocking = true,
    });
    var parent_fence_open = true;
    defer if (parent_fence_open) fence.close(io);
    const child = std.c.fork();
    if (child == 0) {
        // Only async-signal-safe calls after fork: the child inherits the
        // writer's open file description, just as a concurrent process spawn
        // does until exec closes its CLOEXEC descriptors.
        var byte: [1]u8 = undefined;
        const received = std.c.read(pipe[0], &byte, 1);
        _ = std.c.close(fence.handle);
        std.c._exit(if (received == 1) 0 else 1);
    }
    try std.testing.expect(child > 0);
    fence.close(io);
    parent_fence_open = false;
    var child_released = false;
    defer {
        if (!child_released) _ = std.c.write(pipe[1], "x", 1);
        var status: c_int = undefined;
        _ = std.c.waitpid(child, &status, 0);
    }

    const Reader = struct {
        store: *Store,
        started: std.atomic.Value(bool) = .init(false),
        finished: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.read() catch |err| {
                self.failure = err;
            };
            self.finished.store(true, .release);
        }
        fn read(self: *@This()) !void {
            var txn = try self.store.beginRead();
            defer txn.abort();
            try std.testing.expectEqualStrings("committed", try txn.get("doc"));
        }
    };
    var reader = Reader{ .store = &store };
    const thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Reader.run, .{&reader});
    defer {
        if (!child_released) {
            _ = std.c.write(pipe[1], "x", 1);
            child_released = true;
        }
        thread.join();
    }
    while (!reader.started.load(.acquire)) std.atomic.spinLoopHint();
    try io.sleep(.fromMilliseconds(20), .awake);
    // The parent has no exclusive descriptor left, but the child still
    // fences the inode. Read admission must wait instead of returning busy.
    try std.testing.expect(!reader.finished.load(.acquire));
    try std.testing.expectEqual(@as(isize, 1), std.c.write(pipe[1], "x", 1));
    child_released = true;
    while (!reader.finished.load(.acquire)) std.atomic.spinLoopHint();
    if (reader.failure) |err| return err;
}

test "lite native docstore reserves one writer until abort or commit" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
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

    var tmp = std.testing.tmpDir(.{});
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
    var tmp = std.testing.tmpDir(.{});
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

    var tmp = std.testing.tmpDir(.{});
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

    var tmp = std.testing.tmpDir(.{});
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

    var tmp = std.testing.tmpDir(.{});
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "indexed-overlay.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "reader-generation-vacuum.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "queued-group-commit.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io, .reclamation = .{ .page_reuse = false } });
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
                while (!self.gate.proceed.load(.acquire)) @import("antfly_platform").time.yieldNow();
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
        if (i == 0) while (!gate.started.load(.acquire)) @import("antfly_platform").time.yieldNow();
    }
    while (true) {
        store.commit_mutex.lockUncancelable(std.testing.io);
        var queued: usize = 0;
        var item = store.commit_head;
        while (item) |request| {
            queued += 1;
            item = request.next;
        }
        store.commit_mutex.unlock(std.testing.io);
        if (queued == 8) break;
        @import("antfly_platform").time.yieldNow();
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "failed-commit-group.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
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
    while (hammer.submitted.load(.acquire) < 4) @import("antfly_platform").time.yieldNow();
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
                while (!proceed.load(.acquire)) @import("antfly_platform").time.yieldNow();
            }
            return std.Options.debug_io.vtable.fileSync(userdata, file);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "foreground-vacuum.aflite");
    defer alloc.free(path);
    var vtable = std.Options.debug_io.vtable.*;
    vtable.fileSync = Gate.sync;
    const io = std.Io{ .userdata = std.Options.debug_io.userdata, .vtable = &vtable };
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
    while (!Gate.started.load(.acquire)) @import("antfly_platform").time.yieldNow();
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
            return std.Options.debug_io.vtable.fileSync(userdata, file);
        }
        fn apply(_: *anyopaque, file: *native.NativeFile) !void {
            try file.putDocument("doc", "after");
            try file.putIndexCatalogRecord("index", "after");
            try file.putCatalogRecord("meta", "after");
        }
    };
    const alloc = std.testing.allocator;
    var vtable = std.Options.debug_io.vtable.*;
    vtable.fileSync = Fault.sync;
    const io = std.Io{ .userdata = std.Options.debug_io.userdata, .vtable = &vtable };
    for (1..4) |barrier| {
        var tmp = std.testing.tmpDir(.{});
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "packed-cursor.aflite");
    defer alloc.free(path);
    var store = try Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "maintenance-budget.aflite");
    defer alloc.free(path);
    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)] = .{ .soft_limit_bytes = 32768, .hard_limit_bytes = 65536 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    {
        var store = try Store.createWithOptions(alloc, path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = std.testing.io, .resource_manager = &manager });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "key-cursor.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-key-only.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(budget.allocator(), path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = std.testing.io });
    // Allocation ceilings describe foreground work, independently of retirement.
    store.maintenance_start_suppressed = true;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-chunks.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-corrupt.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = std.testing.io });
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
    try std.testing.expectEqual(@as(usize, 1), try store.file.file.readPositionalAll(std.testing.io, &original, offset));
    try store.file.file.writePositionalAll(std.testing.io, &.{original[0] ^ 1}, offset);
    try std.testing.expectError(error.NativePageChecksumMismatch, store.truncateReplayUpTo(a, 3));
    try std.testing.expectEqual(checkpoint.commit_sequence, store.file.activeCheckpoint().commit_sequence);
    const retained = (try store.file.getDocumentAlloc(a, &first)).?;
    defer a.free(retained);
    try std.testing.expectEqualStrings("first", retained);
    try store.file.file.writePositionalAll(std.testing.io, &original, offset);
    try store.truncateReplayUpTo(a, 3);
    try std.testing.expect((try store.file.getDocumentAlloc(a, &first)) == null);
    try std.testing.expect((try store.file.getDocumentAlloc(a, &second)) == null);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
}

test "lite replay readers propagate allocation errors before and after callbacks" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "scope\x00" }) |prefix| {
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "replay-read-errors.aflite");
        defer a.free(path);
        var store = try Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = std.testing.io });
        // Allocation ceilings describe foreground work, independently of retirement.
        store.maintenance_start_suppressed = true;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-owned-results.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "replay-corrupt-read.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .reclamation = .{ .page_reuse = false }, .no_sync = true, .io = std.testing.io });
    defer store.close();
    const first = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 1);
    const second = internal_keys.replayEntryKey(internal_keys.replay_all_kind, 2);
    try store.file.putDocument(&internal_keys.replay_meta_init_key, "");
    try store.file.putDocument(&first, "first");
    try store.file.putDocument(&second, "second");
    const offset = store.file.activeCheckpoint().document_root_page * native.default_page_size + native.page_header_size;
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try store.file.file.readPositionalAll(std.testing.io, &byte, offset));
    try store.file.file.writePositionalAll(std.testing.io, &.{byte[0] ^ 1}, offset);
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
    try store.file.file.writePositionalAll(std.testing.io, &byte, offset);
    try store.file.putDocument(second ++ "bad", "malformed");
    ctx.stop = false;
    try std.testing.expectError(error.InvalidReplayEntryKey, store.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &ctx, Context.handle));
}

test "lite transaction snapshot cache shares hits misses and sorted duplicate reads" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "scope\x00" }) |prefix| {
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "snapshot-read-cache.aflite");
        defer a.free(path);
        var store = try Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = std.testing.io });
        // Allocation ceilings describe foreground work, independently of retirement.
        store.maintenance_start_suppressed = true;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "snapshot-cache-generations.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "snapshot-cache-failures.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pending-retention.aflite");
    defer a.free(path);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var store = try Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = std.testing.io });
    // Allocation ceilings describe foreground work, independently of retirement.
    store.maintenance_start_suppressed = true;
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pending-failures.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "worker-capture-wait.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "worker-error-backoff.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .retry_ms = 500 } });
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

test "lite scoped reads release values without growing parent point reads" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scoped-reads.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const payload = try a.alloc(u8, 64 * 1024);
    defer a.free(payload);
    @memset(payload, 'v');
    try store.file.putDocument("doc", payload);
    const out = try a.alloc(u8, payload.len);
    defer a.free(out);
    var txn = try store.beginRead();
    defer txn.abort();
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    for (0..128) |_| {
        var scope = try txn.openReadScope(budget.allocator());
        try std.testing.expectEqualSlices(u8, payload, try scope.getInto("doc", out));
        try std.testing.expectError(error.BufferTooSmall, scope.getInto("doc", out[0..1]));
        const value = try scope.get("doc");
        try std.testing.expectEqualSlices(u8, payload, value);
        try std.testing.expectError(error.NotFound, scope.get("missing"));
        scope.close();
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        try std.testing.expectEqual(@as(u32, 0), txn.snapshot_reads.count());
    }
    try std.testing.expect(budget.peak < 256 * 1024);
}

test "lite scoped batch buffers preserve external values after reset and close" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scoped-batch-external.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const payload = try a.alloc(u8, 65536);
    defer a.free(payload);
    for (payload, 0..) |*byte, i| byte.* = @truncate(i *% 13);
    try store.file.putDocument("doc", payload);
    var txn = try backend_erased.readTxnFrom(a, try store.beginRead());
    var scope = try txn.openReadScope(a);
    txn.abort();
    const output = try a.alloc(u8, payload.len * 2);
    defer a.free(output);
    const buffers: []const []u8 = &.{ output[0..payload.len], output[payload.len..], &.{} };
    var values: [3]?[]const u8 = undefined;
    try scope.getManySortedInto(&.{ "doc", "doc", "missing" }, buffers, &values);
    try scope.reset();
    scope.close();
    try std.testing.expectEqualSlices(u8, payload, values[0].?);
    try std.testing.expectEqualSlices(u8, payload, values[1].?);
    try std.testing.expect(values[2] == null);
}

test "lite scoped write reads preserve copied values across pending mutations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scoped-write.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    try store.file.putDocument("doc", "disk");
    var txn = try store.beginWrite();
    defer txn.abort();
    var scope = try txn.openReadScope(a);
    defer scope.close();
    const disk = try scope.get("doc");
    try txn.put("doc", "pending");
    const pending = try scope.get("doc");
    try txn.delete("doc");
    try std.testing.expectError(error.NotFound, scope.get("doc"));
    try std.testing.expectEqualStrings("disk", disk);
    try std.testing.expectEqualStrings("pending", pending);
    try std.testing.expectEqual(@as(u32, 0), txn.snapshot_reads.count());
}

test "lite erased scoped readers pin parent after abort" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scoped-pin.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    try store.file.putDocument("doc", "stable");
    var txn = try backend_erased.readTxnFrom(a, try store.beginRead());
    var scope = try txn.openReadScope(a);
    defer scope.close();
    txn.abort();
    try std.testing.expectEqualStrings("stable", try scope.get("doc"));
}

test "lite scoped sorted reads reset scratch and preserve duplicates overlays and missing values" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scoped-sorted.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    try store.file.putDocument("ns\x00a", "disk");
    try store.file.putDocument("ns\x00b", "delete");
    try store.file.putDocument("ns\x00d", "disk-d");
    var txn = try Txn.openWriteWithPrefix(&store, "ns\x00");
    defer txn.abort();
    try txn.put("a", "pending");
    try txn.delete("b");
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var scope = try txn.openReadScope(budget.allocator());
    defer scope.close();
    const keys: []const []const u8 = &.{ "a", "a", "b", "c", "d", "d" };
    var values: [6]?[]const u8 = undefined;
    for (0..100) |_| {
        try scope.getManySorted(keys, &values);
        try std.testing.expectEqualStrings("pending", values[0].?);
        try std.testing.expectEqualStrings("pending", values[1].?);
        try std.testing.expect(values[2] == null and values[3] == null);
        try std.testing.expectEqualStrings("disk-d", values[4].?);
        try std.testing.expectEqual(values[4].?.ptr, values[5].?.ptr);
        try std.testing.expectEqual(values[0].?.ptr, values[1].?.ptr);
        scope.reset();
        try std.testing.expect(scope.scratch.arena.queryCapacity() <= 256 * 1024);
        try std.testing.expectEqual(@as(u32, 0), txn.snapshot_reads.count());
    }
    var result_buffers: [6][16]u8 = undefined;
    var buffers: [6][]u8 = undefined;
    for (&result_buffers, &buffers) |*buffer, *out| out.* = buffer;
    try scope.getManySortedInto(keys, &buffers, &values);
    scope.reset();
    const before = budget.alloc_calls;
    for (0..100) |_| {
        try scope.getManySortedInto(keys, &buffers, &values);
        try std.testing.expectEqualStrings("pending", values[0].?);
        try std.testing.expectEqualStrings("pending", values[1].?);
        try std.testing.expect(values[2] == null and values[3] == null);
        try std.testing.expectEqualStrings("disk-d", values[4].?);
        try std.testing.expectEqualStrings("disk-d", values[5].?);
        for (values, buffers) |value, buffer| if (value) |bytes| try std.testing.expectEqual(buffer.ptr, bytes.ptr);
        scope.reset();
        try std.testing.expectEqualStrings("disk-d", values[4].?);
    }
    try std.testing.expectEqual(before, budget.alloc_calls);
    std.debug.print("LITE_BATCH_REUSE batches=100 new_allocations={d} retained_bytes={d}\n", .{ budget.alloc_calls - before, budget.live });
    buffers[5] = buffers[5][0..1];
    try std.testing.expectError(error.BufferTooSmall, scope.getManySortedInto(keys, &buffers, &values));
    for (values) |value| try std.testing.expect(value == null);
    buffers[5] = &result_buffers[5];
    var out: [16]u8 = undefined;
    try std.testing.expectEqualStrings("pending", try scope.getInto("a", &out));
    try std.testing.expectError(error.BufferTooSmall, scope.getInto("a", out[0..1]));
    try std.testing.expectError(error.NotFound, scope.getInto("b", &out));
    try std.testing.expectError(error.InvalidBatch, scope.getManySorted(&.{ "c", "a", "b", "a", "d", "d" }, &values));
    for (values) |value| try std.testing.expect(value == null);
}

test "lite erased write scopes defer abort and prevent commit until release" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "write-scope-pin.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var txn = try backend_erased.writeTxnFrom(a, try store.beginWrite());
    errdefer txn.abort();
    try txn.put("doc", "pending");
    var scope = try txn.openReadScope(a);
    var scope_open = true;
    defer if (scope_open) scope.close();
    try std.testing.expectError(error.TransactionCursorActive, txn.commit());
    try std.testing.expectEqualStrings("pending", try scope.get("doc"));
    try scope.reset();
    scope.close();
    scope_open = false;
    try txn.commit();
}

test "lite erased write scopes preserve pending values after parent abort" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "write-scope-abort.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var txn = try backend_erased.writeTxnFrom(a, try store.beginWrite());
    var txn_open = true;
    defer if (txn_open) txn.abort();
    try txn.put("doc", "pending");
    var scope = try txn.openReadScope(a);
    var scope_open = true;
    defer if (scope_open) scope.close();
    txn.abort();
    txn_open = false;
    try std.testing.expectEqualStrings("pending", try scope.get("doc"));
    try scope.reset();
    scope.close();
    scope_open = false;
    // Last-child release aborts the underlying writer and releases its slot.
    var next = try store.beginWrite();
    defer next.abort();
    try std.testing.expectError(error.NotFound, next.get("doc"));
}

test "lite scoped read allocation measurement bounds distinct document workloads" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "scoped-measure.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const payload = try a.alloc(u8, 64 * 1024);
    defer a.free(payload);
    @memset(payload, 'v');
    var keys: [32][8]u8 = undefined;
    for (&keys, 0..) |*key, i| {
        @memset(key, 'k');
        std.mem.writeInt(u64, key, i, .big);
        try store.file.putDocument(key, payload);
    }
    const Budget = @import("test_allocator.zig").BudgetAllocator;
    var baseline = Budget{ .backing = a };
    var retained = try store.beginRead();
    retained.allocator = baseline.allocator();
    for (&keys) |*key| _ = try retained.get(key);
    const retained_live = baseline.live;
    const retained_peak = baseline.peak;
    retained.abort();
    try std.testing.expectEqual(@as(usize, 0), baseline.live);
    var scoped = Budget{ .backing = a };
    var streaming = try store.beginRead();
    defer streaming.abort();
    var scope = try streaming.openReadScope(scoped.allocator());
    for (&keys) |*key| {
        try std.testing.expectEqualSlices(u8, payload, try scope.get(key));
        scope.reset();
    }
    const scoped_peak = scoped.peak;
    const scoped_retained = scoped.live;
    scope.close();
    try std.testing.expectEqual(@as(usize, 0), scoped.live);
    try std.testing.expect(retained_live >= payload.len * keys.len);
    try std.testing.expect(scoped_retained <= 256 * 1024);
    try std.testing.expect(scoped_peak < retained_peak / 4);
    std.debug.print("LITE_SCOPED_READS documents={d} value_bytes={d} borrowed_live={d} borrowed_peak={d} scoped_retained={d} scoped_peak={d}\n", .{ keys.len, payload.len, retained_live, retained_peak, scoped_retained, scoped_peak });
}

test "lite reclamation workspace subtracts artifact storage and future disk claims" {
    const Probe = struct {
        available_bytes: u64 = 1024 * 1024,
        fn available(ptr: *anyopaque, _: []const u8) !u64 {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).available_bytes;
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "workspace-artifacts.aflite");
    defer a.free(path);
    const limit: u64 = 1024 * 1024;
    var probe = Probe{};
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false, .max_storage_bytes = limit, .disk_headroom_bytes = 0, .capacity_probe = .{ .context = &probe, .available = Probe.available } } });
    defer store.close();
    const artifact = try store.reserveArtifactBytes(256 * 1024);
    defer ArtifactLease.release(artifact);
    {
        var workspace = try store.reserveGenerationWorkspace();
        defer workspace.deinit();
        try std.testing.expectEqual(limit, (try store.reclamationStatus()).totalBytes());
    }
    // Disk free space excludes materialized files, but still includes future
    // writes. Reserve only the remainder, and return it after workspace close.
    store.maintenance_policy.options.max_storage_bytes = 0;
    probe.available_bytes = 1024 * 1024;
    {
        var workspace = try store.reserveGenerationWorkspace();
        defer workspace.deinit();
        try std.testing.expectEqual(@as(u64, 768 * 1024), workspace.options.max_storage_bytes);
    }
    artifact.noteMaterialized(artifact.bytes);
    probe.available_bytes -= artifact.bytes;
    {
        var workspace = try store.reserveGenerationWorkspace();
        defer workspace.deinit();
        try std.testing.expectEqual(probe.available_bytes, workspace.options.max_storage_bytes);
    }
}

test "lite reader admission and release proceed while commit fsync is blocked" {
    const Hook = struct {
        var owner: ?*Store = null;
        var entered: std.Io.Event = .unset;
        var release: std.Io.Event = .unset;
        var readers_done: std.Io.Event = .unset;
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            if (owner) |store| if (file.handle == store.file.file.handle) {
                entered.set(std.testing.io);
                release.waitUncancelable(std.testing.io);
            };
            return std.testing.io.vtable.fileSync(userdata, file);
        }
        fn write(store: *Store) !void {
            var txn = try store.beginWrite();
            var committed = false;
            defer if (!committed) txn.abort();
            try txn.put("key", "new");
            try txn.commit();
            committed = true;
        }
        fn read(store: *Store) !void {
            defer readers_done.set(std.testing.io);
            for (0..128) |_| {
                var txn = try store.beginRead();
                defer txn.abort();
                try std.testing.expectEqualStrings("old", try txn.get("key"));
            }
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reader-admission-fsync.aflite");
    defer a.free(path);
    var vtable = std.testing.io.vtable.*;
    vtable.fileSync = Hook.sync;
    const io = std.Io{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    var store = try Store.createWithOptions(a, path, .{ .io = io, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    var initial = try store.beginWrite();
    try initial.put("key", "old");
    try initial.commit();
    var pinned = try store.beginRead();
    defer pinned.abort();
    Hook.entered.reset();
    Hook.release.reset();
    Hook.readers_done.reset();
    Hook.owner = &store;
    defer Hook.owner = null;
    var writer = try std.testing.io.concurrent(Hook.write, .{&store});
    defer {
        Hook.release.set(std.testing.io);
        writer.await(std.testing.io) catch {};
    }
    try Hook.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    var readers = try std.testing.io.concurrent(Hook.read, .{&store});
    const progress = Hook.readers_done.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    Hook.release.set(std.testing.io);
    try readers.await(std.testing.io);
    try writer.await(std.testing.io);
    std.debug.print("lite reader probe: 128 readers completed during blocked sync = {any}\n", .{if (progress) |_| true else |_| false});
    try progress;
    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings("new", try fresh.get("key"));
    try std.testing.expectEqualStrings("old", try pinned.get("key"));
}

test "lite prepared admission expires idle cached sources but preserves active uses" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "admission-cached-live.aflite");
    defer a.free(path);
    const staged_path = try testPath(a, tmp, "admission-cached-prepared.aflite");
    defer a.free(staged_path);
    const options: CreateOptions = .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } };
    var live = try Store.createWithOptions(a, path, options);
    defer live.close();
    live.maintenance_start_suppressed = true;
    var staged = try Store.createWithOptions(a, staged_path, options);
    defer staged.close();
    staged.maintenance_start_suppressed = true;
    try live.file.putIndexCatalogRecord("run", "unchanged");
    var source = try live.openArtifactSource("run");
    defer source.close();
    try std.testing.expect(source.acquireUse());
    var active = true;
    defer if (active) source.releaseUse();
    source.enableIdleExpiry();
    const old_bytes = (try live.file.file.stat(std.testing.io)).size;
    const prepared_bytes = (try staged.file.file.stat(std.testing.io)).size;
    const reserve = try staged.file.retirementReserveBytes();
    live.maintenance_policy.options.max_storage_bytes = prepared_bytes + @max(old_bytes, reserve);
    // Match the production adoption lock order; new source opens cannot enter
    // between expiry and the final retained-inode accounting decision.
    live.generation_lock.lockUncancelable(std.testing.io);
    defer live.generation_lock.unlock(std.testing.io);
    lockStore(&live);
    defer live.mutex.unlock();
    try std.testing.expectError(error.LiteStorageBudgetExceeded, live.admitPreparedGenerationAssumeLocked(&staged.file, old_bytes));
    var bytes: [9]u8 = undefined;
    try source.readInto(0, &bytes);
    try std.testing.expectEqualStrings("unchanged", &bytes);
    source.releaseUse();
    active = false;
    try live.admitPreparedGenerationAssumeLocked(&staged.file, old_bytes);
    try std.testing.expect(!source.acquireUse());
    try std.testing.expectEqual(@as(u64, 0), live.totalRetiredFileBytes());
}

test "lite cached source teardown and retirement progress while publication is blocked" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "source-close-retirement.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    try store.file.putIndexCatalogRecord("run", "unchanged");
    var source = try store.openArtifactSource("run");
    var source_open = true;
    defer if (source_open) source.close();
    try std.testing.expect(source.acquireUse());
    source.enableIdleExpiry();
    source.releaseUse();
    const registry = store.artifact_registry.?;
    const old_bytes = (try store.file.file.stat(io)).size;
    const Worker = struct {
        fn close(owned: *@import("../../segment_source.zig").Source) void {
            owned.close();
        }
    };
    lockStore(&store);
    var locked = true;
    defer if (locked) store.mutex.unlock();
    const thread = try std.Thread.spawn(.{}, Worker.close, .{&source});
    source_open = false;
    defer thread.join();
    // The final source release is queued behind the owner mutex. Retiring the
    // same source must progress without waiting for that publication callback.
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    var callback_started = false;
    while (!callback_started and std.Io.Clock.awake.now(io).nanoseconds < deadline.nanoseconds) {
        registry.mutex.lockUncancelable(io);
        callback_started = registry.callbacks != 0;
        registry.mutex.unlock(io);
        if (!callback_started) io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
    if (callback_started) {
        registry.expireIdle(store.assessment_generation);
        registry.retire(store.assessment_generation, old_bytes, false);
    }
    store.mutex.unlock();
    locked = false;
    try std.testing.expect(callback_started);
    try std.testing.expectEqual(@as(u64, 0), registry.retired_bytes.load(.acquire));
}

test "lite budget allocator accounts concurrent allocation resize and cleanup" {
    const a = std.testing.allocator;
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const Worker = struct {
        fn run(allocator: Allocator) anyerror!void {
            for (0..1024) |i| {
                var bytes = try allocator.alloc(u8, 256);
                defer allocator.free(bytes);
                @memset(bytes, 17);
                bytes = try allocator.realloc(bytes, 64 + i % 128);
                try std.testing.expectEqual(@as(u8, 17), bytes[0]);
            }
        }
    };
    var workers: [4]std.Io.Future(anyerror!void) = undefined;
    var started: usize = 0;
    defer for (workers[0..started]) |*worker| {
        worker.await(std.testing.io) catch {};
    };
    for (&workers) |*worker| {
        worker.* = try std.testing.io.concurrent(Worker.run, .{budget.allocator()});
        started += 1;
    }
    for (&workers) |*worker| try worker.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak > 0);
    try std.testing.expect(budget.alloc_calls >= 4 * 1024);
}

test "lite scoped native batch state reuses all working allocations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "batch-scratch-perf.aflite");
    defer a.free(path);
    var file_budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var store = try Store.createWithOptions(file_budget.allocator(), path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var requested: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    for (&docs, &key_storage, &nodes, &requested, 0..) |*doc, *key, *node, *request, i| {
        node.* = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        request.* = key;
        doc.* = .{ .key = key, .value = &score };
    }
    try store.file.putDocumentBatch(&docs);
    var txn = try store.beginRead();
    defer txn.abort();
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var scope = try txn.openReadScope(budget.allocator());
    defer scope.close();
    var slots: [512][8]u8 = undefined;
    var buffers: [512][]u8 = undefined;
    for (&slots, &buffers) |*slot, *buffer| buffer.* = slot;
    var values: [512]?[]const u8 = undefined;
    const direct = @hasDecl(@TypeOf(scope), "getManySortedInto");
    if (direct) try scope.getManySortedInto(&requested, &buffers, &values) else try scope.getManySorted(&requested, &values);
    scope.reset();
    for (0..4) |_| {
        try scope.getManySortedInto(&requested, &buffers, &values);
        scope.reset();
    }
    const alloc_before = budget.alloc_calls;
    const file_alloc_before = file_budget.alloc_calls;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..2000) |_| {
        if (direct) try scope.getManySortedInto(&requested, &buffers, &values) else try scope.getManySorted(&requested, &values);
        for (values) |value| try std.testing.expectEqualSlices(u8, &score, value.?);
        scope.reset();
    }
    const batch_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started;
    const batch_allocs = budget.alloc_calls - alloc_before;
    const retained = budget.live;
    try std.testing.expectEqual(alloc_before, budget.alloc_calls);
    try std.testing.expectEqual(file_alloc_before, file_budget.alloc_calls);
    std.debug.print("REVIEW_TOTAL file_allocations={d} scope_allocations={d} batches=2000\n", .{ file_budget.alloc_calls - file_alloc_before, batch_allocs });
    var columns: [512]?f64 = undefined;
    const graph_started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var retained_entries: usize = 0;
    for (0..500) |_| {
        var query = try store.beginRead();
        defer query.abort();
        _ = try @import("../../graph/score_read.zig").populate(a, &query, &.{""}, &nodes, &.{&columns});
        for (columns) |value| try std.testing.expectEqual(@as(?f64, 3), value);
        retained_entries += query.snapshot_reads.count();
    }
    const graph_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - graph_started;
    std.debug.print("LITE_BATCH_SCRATCH_TIMING keys=512 batches=2000 direct={any} batch_ns={d} scratch_allocations={d} retained_bytes={d} graph_queries=500 graph_ns={d} graph_retained_entries={d}\n", .{ direct, batch_ns, batch_allocs, retained, graph_ns, retained_entries });
}

test "lite pooled graph reads reuse scratch across fresh queries and isolate scopes" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pooled-graph.aflite");
    defer a.free(path);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = budget.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 7)), .little);
    var key_bytes: [512][10]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    for (&key_bytes, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        node.* = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [512]?f64 = undefined;
    const score_read = @import("../../graph/score_read.zig");
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..105) |i| {
        var txn = try store.beginRead();
        defer txn.abort();
        const before = budget.alloc_calls;
        _ = try score_read.populate(alloc, &txn, &.{""}, &nodes, &.{&columns});
        for (columns) |score| try std.testing.expectEqual(@as(?f64, 7), score);
        if (i >= 5) try std.testing.expectEqual(before, budget.alloc_calls);
        try std.testing.expectEqual(@as(usize, 0), txn.snapshot_reads.count());
    }
    std.debug.print("LITE_POOLED_GRAPH queries=100 keys=512 scratch_allocations=0 elapsed_ns={d}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
    var txn = try store.beginRead();
    defer txn.abort();
    var first = try txn.openReadScope(alloc);
    defer first.close();
    var second = try txn.openReadScope(alloc);
    defer second.close();
    var third = try txn.openReadScope(alloc);
    defer third.close();
    try std.testing.expect(first.workspace != null and second.workspace != null);
    try std.testing.expect(first.workspace != second.workspace);
    try std.testing.expect(third.workspace == null);
    const retained = try first.get(docs[0].key);
    var score: [1]?f64 = undefined;
    try second.getManySortedScores(&.{docs[0].key}, &score);
    second.reset();
    try std.testing.expectEqualSlices(u8, &raw, retained);
}

test "lite pooled query budgets admit reuse and preserve unknown allocator limits" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "pooled-budget.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var txn = try store.beginRead();
    defer txn.abort();
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var budget = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer budget.deinit();
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    var scope = try txn.openReadScope(arena.allocator());
    try std.testing.expect(scope.workspace != null);
    const bytes = try scope.workingAllocator().alloc(u8, 4096);
    @memset(bytes, 1);
    try std.testing.expect(budget.live_bytes >= 4096);
    scope.close();
    try std.testing.expectEqual(@as(u64, 0), budget.reservation_floor);
    try std.testing.expectEqual(@as(u64, 0), budget.live_bytes);
    var first = try txn.openReadScope(budget.allocator());
    var second = try txn.openReadScope(budget.allocator());
    _ = try first.workingAllocator().alloc(u8, 1024);
    _ = try second.workingAllocator().alloc(u8, 2048);
    first.close();
    try std.testing.expect(budget.live_bytes >= 2048);
    second.close();
    try std.testing.expectEqual(@as(u64, 0), budget.live_bytes);
    var limits = resource_manager_mod.Options.defaultBudgets();
    limits[@backingInt(resource_manager_mod.Slice.dense_search_working_set)] = .{ .hard_limit_bytes = 1 };
    var denied_manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer denied_manager.deinit(a);
    var denied = resource_manager_mod.BudgetedAllocator.init(&denied_manager, .dense_search_working_set, a, 1);
    defer denied.deinit();
    var small = try txn.openReadScope(denied.allocator());
    try std.testing.expect(small.workspace == null);
    const byte = try small.workingAllocator().alloc(u8, 1);
    small.workingAllocator().free(byte);
    small.close();
    try std.testing.expectEqual(@as(u64, 0), denied.live_bytes);
    try std.testing.expectEqual(@as(u64, 0), denied.reservation_floor);
    var tiny = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    var limited = try txn.openReadScope(tiny.allocator());
    defer limited.close();
    try std.testing.expect(limited.workspace == null);
    try std.testing.expectError(error.OutOfMemory, limited.workingAllocator().alloc(u8, 1));
}

test "lite fused scores validate overlays duplicates prefixes and malformed values" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "fused-score.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 2)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p\x00a", .value = &raw }, .{ .key = "p\x00b", .value = "bad" }, .{ .key = "p\x00d", .value = &raw } });
    var txn = try Txn.openWriteWithPrefix(&store, "p\x00");
    defer txn.abort();
    try txn.put("c", &raw);
    try txn.delete("a");
    var scope = try txn.openReadScope(a);
    defer scope.close();
    var scores: [3]?f64 = undefined;
    try scope.getManySortedScores(&.{ "a", "c", "c" }, &scores);
    try std.testing.expectEqualSlices(?f64, &.{ null, 2, 2 }, &scores);
    const Output = struct {
        scores: []?f64,
        fn write(ctx: *anyopaque, index: usize, score: f64) void {
            const out: *@This() = @ptrCast(@alignCast(ctx));
            out.scores[index] = score;
        }
    };
    var output = Output{ .scores = &scores };
    const sink = backend_types.ScoreSink{ .context = &output, .write = Output.write };
    @memset(&scores, null);
    try scope.visitManySortedScores(&.{ "a", "c", "c" }, sink);
    try std.testing.expectEqualSlices(?f64, &.{ null, 2, 2 }, &scores);
    @memset(&scores, null);
    try scope.visitManySortedScores(&.{ "c", "d", "d" }, sink);
    try std.testing.expectEqualSlices(?f64, &.{ 2, 2, 2 }, &scores);
    try std.testing.expectError(error.InvalidGraphMetricScore, scope.visitManySortedScores(&.{ "b", "c", "c" }, sink));
    try std.testing.expectError(error.InvalidBatch, scope.visitManySortedScores(&.{ "c", "b", "a" }, sink));
    try std.testing.expectError(error.InvalidGraphMetricScore, scope.getManySortedScores(&.{ "b", "c", "c" }, &scores));
    for (scores) |score| try std.testing.expect(score == null);
    try std.testing.expectError(error.InvalidBatch, scope.getManySortedScores(&.{ "c", "b", "a" }, &scores));
    std.mem.writeInt(u64, &raw, @bitCast(std.math.nan(f64)), .little);
    try txn.put("c", &raw);
    try std.testing.expectError(error.InvalidGraphMetricScore, scope.getManySortedScores(&.{ "a", "c", "c" }, &scores));
    try std.testing.expectError(error.InvalidGraphMetricScore, scope.visitManySortedScores(&.{ "a", "c", "c" }, sink));
}

test "lite fused graph visitors preserve reordered rows duplicate columns and erased scopes" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "graph-sink-fanout.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [4][8]u8 = undefined;
    const numbers = [_]f64{ 2, 5, 3, 7 };
    for (&raw, numbers) |*value, n| std.mem.writeInt(u64, value, @bitCast(n), .little);
    try store.file.putDocumentBatch(&.{
        .{ .key = "p/a\x00\x00", .value = &raw[0] },
        .{ .key = "p/b\x00\x00", .value = &raw[1] },
        .{ .key = "q/a\x00\x00", .value = &raw[2] },
        .{ .key = "q/b\x00\x00", .value = &raw[3] },
    });
    var txn = try store.beginRead();
    defer txn.abort();
    const Wrapped = struct {
        inner: *Txn,
        pub fn openReadScope(self: *@This(), alloc: Allocator) !backend_erased.ReadScope {
            return backend_erased.readScopeFrom(alloc, try self.inner.openReadScope(alloc));
        }
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var wrapped = Wrapped{ .inner = &txn };
    var scores: [4][4]?f64 = undefined;
    const columns = [_][]?f64{ &scores[0], &scores[1], &scores[2], &scores[3] };
    _ = try @import("../../graph/score_read.zig").populate(a, &wrapped, &.{ "q/", "p/", "q/", null }, &.{ "b", "a", "b", "missing" }, &columns);
    try std.testing.expectEqualSlices(?f64, &.{ 7, 3, 7, null }, &scores[0]);
    try std.testing.expectEqualSlices(?f64, &.{ 5, 2, 5, null }, &scores[1]);
    try std.testing.expectEqualSlices(?f64, &scores[0], &scores[2]);
    for (scores[3]) |score| try std.testing.expect(score == null);
    const buffer = store.workspaces[0].?.score_plan.storage.?.ptr;
    _ = try @import("../../graph/score_read.zig").populate(a, &wrapped, &.{ "q/", "p/", "q/", null }, &.{ "a", "b", "missing", "a" }, &columns);
    try std.testing.expectEqual(buffer, store.workspaces[0].?.score_plan.storage.?.ptr);
    try std.testing.expectEqualSlices(?f64, &.{ 3, 7, null, 3 }, &scores[0]);
    try std.testing.expectEqualSlices(?f64, &.{ 2, 5, null, 2 }, &scores[1]);
    try std.testing.expectEqualSlices(?f64, &scores[0], &scores[2]);
}

test "lite pooled workspace pressure reclaims idle slots and skips live scopes" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "workspace-pressure.aflite");
    defer a.free(path);
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 4 * 1024 * 1024 } });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var txn = try store.beginRead();
    defer txn.abort();
    var first = try txn.openReadScope(a);
    var second = try txn.openReadScope(a);
    defer second.close();
    _ = try first.workingAllocator().alloc(u8, 4096);
    _ = try second.workingAllocator().alloc(u8, 4096);
    first.close();
    const before = manager.sliceStats(.lite_docstore_snapshot_cache).used_bytes;
    try std.testing.expect(before > 0);
    const freed = manager.reclaimForAllocation(.lite_docstore_snapshot_cache, 4 * 1024 * 1024);
    try std.testing.expect(freed > 0);
    try std.testing.expect(manager.sliceStats(.lite_docstore_snapshot_cache).used_bytes < before);
    try std.testing.expect(second.workspace.?.busy);
    try std.testing.expectEqual(@as(u64, 0), manager.reclaimForAllocation(.lite_docstore_snapshot_cache, 4 * 1024 * 1024));
}

test "lite workspace failed allocation returns query credit" {
    const resources = resource_manager_mod;
    const Workspace = ReadWorkspace;
    const a = std.testing.allocator;
    var backing = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const workspace = try Workspace.create(backing.allocator(), null);
    defer workspace.destroy();
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    var query = resources.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    try workspace.begin(&query);
    defer workspace.end();
    backing.limit = backing.live;
    try std.testing.expectError(error.OutOfMemory, workspace.allocator().alloc(u8, 4096));
    try std.testing.expectEqual(@as(u64, 0), query.live_bytes);
}

test "lite workspace failed resize returns query credit" {
    const resources = resource_manager_mod;
    const Workspace = ReadWorkspace;
    const a = std.testing.allocator;
    var backing = @import("test_allocator.zig").NoResizeAllocator{ .backing = a };
    const workspace = try Workspace.create(backing.allocator(), null);
    defer workspace.destroy();
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    var query = resources.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    try workspace.begin(&query);
    defer workspace.end();
    const bytes = try workspace.allocator().alloc(u8, 32);
    defer workspace.allocator().free(bytes);
    try std.testing.expect(!workspace.allocator().resize(bytes, 64));
    try std.testing.expectEqual(@as(u64, 32), query.live_bytes);
}

test "lite workspace residency is not charged twice to node budget" {
    const resources = resource_manager_mod;
    const Workspace = ReadWorkspace;
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
    defer manager.deinit(a);
    const workspace = try Workspace.create(a, &manager);
    defer workspace.destroy();
    var query = resources.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    try workspace.begin(&query);
    defer workspace.end();
    const bytes = workspace.allocator().alloc(u8, 96 * 1024) catch |err| {
        std.debug.print("REVIEW_DOUBLE_CHARGE physical={d} query_charged={d} node_limit=131072 requested=98304\n", .{ workspace.live, query.live_bytes });
        return err;
    };
    defer workspace.allocator().free(bytes);
}

test "lite reclaiming budgeted store backing does not recurse" {
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var backing = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .lite_docstore_snapshot_cache, a, 1);
    defer backing.deinit();
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "reclaiming-backing.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(backing.threadSafeAllocator(), path, .{ .io = std.testing.io, .no_sync = true, .resource_manager = &manager, .reclamation = .{ .enabled = false } });
    defer store.close();
    var txn = try store.beginRead();
    defer txn.abort();
    var scope = try txn.openReadScope(query.allocator());
    _ = try scope.workingAllocator().alloc(u8, 4096);
    scope.close();
    _ = backing.releaseUnusedCreditThreadSafe();
    manager.memory.budget.hard_limit_bytes = manager.memory.used_bytes;
    const bytes = try backing.threadSafeAllocator().alloc(u8, 4096);
    defer backing.threadSafeAllocator().free(bytes);
}

test "lite prepared graph plans own inputs rebind outputs and resolve new snapshots" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "prepared-snapshot.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 2)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p/a\x00\x00", .value = &raw }, .{ .key = "q/a\x00\x00", .value = &raw } });
    var old = try store.beginRead();
    defer old.abort();
    var first: [2]?f64 = undefined;
    const reader = @import("../../graph/score_read.zig");
    _ = try reader.populate(a, &old, &.{"p/"}, &.{ "a", "a" }, &.{&first});
    var key: [1]u8 = .{'a'};
    var prefix: [2]u8 = .{ 'p', '/' };
    var second: [2]?f64 = undefined;
    _ = try reader.populate(a, &old, &.{&prefix}, &.{ &key, &key }, &.{&second});
    key[0] = 'b';
    prefix[0] = 'q';
    try std.testing.expectEqualSlices(?f64, &.{ 2, 2 }, &second);
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("p/a\x00\x00", &raw);
        try write.delete("q/a\x00\x00");
        try write.commit();
    }
    var fresh = try store.beginRead();
    defer fresh.abort();
    _ = try reader.populate(a, &fresh, &.{"p/"}, &.{ "a", "a" }, &.{&second});
    try std.testing.expectEqualSlices(?f64, &.{ 9, 9 }, &second);
    _ = try reader.populate(a, &old, &.{"p/"}, &.{ "a", "a" }, &.{&first});
    try std.testing.expectEqualSlices(?f64, &.{ 2, 2 }, &first);
    _ = try reader.populate(a, &fresh, &.{"q/"}, &.{ "a", "b" }, &.{&second});
    try std.testing.expectEqualSlices(?f64, &.{ null, null }, &second);
}

test "lite metadata pool bounds retained nodes survives overflow and reclaims" {
    const a = std.testing.allocator;
    var backing = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var pool = @import("metadata_pool.zig").MetadataPool{};
    defer pool.deinit();
    const alloc = pool.allocator(backing.allocator(), null);
    var nodes: [40][]u8 = undefined;
    for (&nodes) |*node| node.* = try alloc.alloc(u8, 16);
    for (nodes) |node| alloc.free(node);
    try std.testing.expectEqual(@as(usize, 16), pool.idle_count);
    const calls = backing.alloc_calls;
    const bytes = try alloc.alloc(u8, 16);
    try std.testing.expectEqual(calls, backing.alloc_calls);
    try std.testing.expect(alloc.resize(bytes, 32));
    alloc.free(bytes.ptr[0..32]);
    const large = try alloc.alloc(u8, 2048);
    try std.testing.expect(!alloc.resize(large, 16));
    alloc.free(large);
    try std.testing.expect(pool.reclaim(std.math.maxInt(u64)) > 0);
    try std.testing.expectEqual(@as(usize, 0), backing.live);
}

test "lite resident admission combines borrowed and allocated query demand" {
    const a = std.testing.allocator;
    var limits = resource_manager_mod.Options.defaultBudgets();
    limits[@backingInt(resource_manager_mod.Slice.dense_search_working_set)] = .{ .hard_limit_bytes = 128 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 2);
    defer query.deinit();
    var borrowed = try query.borrowResidentScratch(96, &manager);
    defer borrowed.release();
    const bytes = try query.allocator().alloc(u8, 128);
    defer query.allocator().free(bytes);
    try std.testing.expectEqual(@as(u64, 224), query.live_bytes);
    try std.testing.expectError(error.ResourceBudgetExceeded, borrowed.growTo(129));
    borrowed.shrinkTo(32);
    try std.testing.expectEqual(@as(u64, 160), query.live_bytes);
    try std.testing.expectEqual(@as(u64, 128), manager.snapshot().memory.used_bytes);
}

test "lite prepared plan allocation failures preserve ownership and bounded fallback" {
    const Plan = @import("../prepared_score_plan.zig").PreparedScorePlan;
    const Runner = struct {
        fn run(a: Allocator) !void {
            var plan = Plan{};
            defer plan.deinit();
            try std.testing.expect(try plan.prepare(a, &.{ "q/", null, "q/" }, &.{ "b", "a\x00", "b" }));
            try std.testing.expectEqual(@as(usize, 2), plan.read_keys.len);
            try std.testing.expect(plan.storage.?.len <= Plan.max_bytes);
            const pointer = plan.storage.?.ptr;
            try std.testing.expect(try plan.prepare(a, &.{ "q/", null, "q/" }, &.{ "b", "a\x00", "b" }));
            try std.testing.expectEqual(pointer, plan.storage.?.ptr);
            try std.testing.expect(try plan.prepare(a, &.{ "p/", null, "q/" }, &.{ "b", "a\x00", "b" }));
            var huge: [5000][]const u8 = undefined;
            @memset(&huge, "a");
            try std.testing.expect(!try plan.prepare(a, &.{"p/"}, &huge));
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "lite prepared fresh raw and erased graph allocation measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "prepared-measure.aflite");
    defer a.free(path);
    var file_budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = file_budget.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    for (&docs, &key_storage, &nodes, 0..) |*doc, *key, *node, i| {
        node.* = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        doc.* = .{ .key = key, .value = &score };
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [512]?f64 = undefined;
    const reader = @import("../../graph/score_read.zig");
    var reversed = nodes;
    std.mem.reverse([]const u8, &reversed);
    var shuffled = nodes;
    var random = std.Random.DefaultPrng.init(42);
    random.random().shuffle([]const u8, &shuffled);
    inline for (.{ 0, 1, 2 }) |varying| {
        inline for (.{ false, true }) |erased| {
            for (0..5) |_| {
                const raw = try store.beginRead();
                var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw) else raw;
                defer txn.abort();
                _ = try reader.populate(alloc, &txn, &.{""}, &nodes, &.{&columns});
            }
            const before = file_budget.alloc_calls;
            const cpu_before = std.posix.getrusage(0);
            for (0..5000) |i| {
                const requested_nodes = if (varying == 0 or i % 2 == 1) &nodes else if (varying == 1) &reversed else &shuffled;
                const raw = try store.beginRead();
                var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw) else raw;
                defer txn.abort();
                _ = try reader.populate(alloc, &txn, &.{""}, requested_nodes, &.{&columns});
                for (columns) |value| try std.testing.expectEqual(@as(?f64, 3), value);
            }
            const cpu_after = std.posix.getrusage(0);
            const cpu_us = (cpu_after.utime.sec - cpu_before.utime.sec + cpu_after.stime.sec - cpu_before.stime.sec) * 1000000 + cpu_after.utime.usec - cpu_before.utime.usec + cpu_after.stime.usec - cpu_before.stime.usec;
            std.debug.print("LITE_PREPARED_MEASURE erased={any} varying={any} queries=5000 keys=512 cpu_us={d} file_allocations={d} scope_bytes={d}\n", .{ erased, varying, cpu_us, file_budget.alloc_calls - before, @sizeOf(Txn.ReadScope) });
            try std.testing.expectEqual(before, file_budget.alloc_calls);
        }
    }
}

test "lite metadata residency is budgeted and independent of query admission" {
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 4096 } });
    defer manager.deinit(a);
    var pool = @import("metadata_pool.zig").MetadataPool{};
    defer pool.deinit();
    const alloc = pool.allocator(a, &manager);
    const bytes = try alloc.alloc(u8, 32);
    alloc.free(bytes);
    try std.testing.expect(manager.snapshot().memory.used_bytes > 32);
    try std.testing.expect(pool.residentManager() == &manager);
    _ = pool.reclaim(std.math.maxInt(u64));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "lite borrowed admission cannot release copied credit or adopt physical ownership" {
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var first = try query.borrowResidentScratch(32, &manager);
    var stale = first;
    var second = try query.borrowResidentScratch(64, &manager);
    defer second.release();
    var observed: u64 = 0;
    try std.testing.expectError(error.ResourceAccountingMismatch, manager.adoptReservationUsage(&first.reservation, .lite_docstore_snapshot_cache, &observed, 32));
    try std.testing.expectEqual(@as(u64, 0), observed);
    first.release();
    stale.release();
    try std.testing.expectEqual(@as(u64, 64), query.live_bytes);
    try std.testing.expectEqual(@as(u64, 64), manager.sliceStats(.dense_search_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectError(error.ReservationReleased, first.growTo(1));
}

test "lite workspace nested residency charges host memory once across slices" {
    const a = std.testing.allocator;
    for ([_]resource_manager_mod.Slice{ .lite_docstore_snapshot_cache, .dense_source_payload_state }) |slice| {
        var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
        defer manager.deinit(a);
        var backing = resource_manager_mod.BudgetedAllocator.init(&manager, slice, a, 1);
        defer backing.deinit();
        const workspace = try ReadWorkspace.create(backing.threadSafeAllocator(), &manager);
        defer workspace.destroy();
        var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
        defer query.deinit();
        try workspace.begin(&query);
        defer workspace.end();
        const bytes = try workspace.allocator().alloc(u8, 96 * 1024);
        defer workspace.allocator().free(bytes);
        try std.testing.expectEqual(backing.reservation.bytes, manager.snapshot().memory.used_bytes);
        try std.testing.expectEqual(@as(u64, bytes.len), query.live_bytes);
    }
}

test "lite metadata pool concurrent reuse and pressure release preserve ownership" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
    defer manager.deinit(a);
    var pool = @import("metadata_pool.zig").MetadataPool{};
    defer pool.deinit();
    const alloc = pool.allocator(a, &manager);
    var failed = std.atomic.Value(bool).init(false);
    const Worker = struct {
        fn run(allocator: Allocator, result: *std.atomic.Value(bool)) void {
            for (0..100) |_| {
                const bytes = allocator.alloc(u8, 32) catch {
                    result.store(true, .release);
                    return;
                };
                @memset(bytes, 42);
                if (bytes[31] != 42) result.store(true, .release);
                allocator.free(bytes);
            }
        }
    };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{ alloc, &failed });
        started += 1;
    }
    for (0..100) |_| _ = pool.reclaim(1024);
    for (threads) |thread| thread.join();
    started = 0;
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.active);
    try std.testing.expect(pool.idle_count <= 16);
    _ = pool.reclaim(std.math.maxInt(u64));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "lite prepared plan buffer reuse on misses and recovery after allocation failure" {
    const a = std.testing.allocator;
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    var plan = @import("../prepared_score_plan.zig").PreparedScorePlan{};
    defer plan.deinit();
    try std.testing.expectError(error.OutOfMemory, plan.prepare(budget.allocator(), &.{"p/"}, &.{"a"}));
    budget.limit = 65536;
    try std.testing.expect(try plan.prepare(budget.allocator(), &.{"p/"}, &.{"a"}));
    const before = budget.alloc_calls;
    const ptr = plan.storage.?.ptr;
    try std.testing.expect(try plan.prepare(budget.allocator(), &.{"q/"}, &.{"b"}));
    try std.testing.expectEqual(before, budget.alloc_calls);
    try std.testing.expectEqual(ptr, plan.storage.?.ptr);
    try std.testing.expect(std.mem.startsWith(u8, plan.read_keys[0], "q/b"));
}

test "lite prepared plans preserve reads that fit a small query budget" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "plan-query-budget.aflite");
    defer a.free(path);
    var limits = resource_manager_mod.Options.defaultBudgets();
    limits[@backingInt(resource_manager_mod.Slice.dense_search_working_set)] = .{ .hard_limit_bytes = 64 * 1024 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var keys: [512][10]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&keys, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        node.* = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [512]?f64 = undefined;
    for (0..3) |_| {
        var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
        defer query.deinit();
        var txn = try store.beginRead();
        defer txn.abort();
        _ = try @import("../../graph/score_read.zig").populate(query.allocator(), &txn, &.{""}, &nodes, &.{&columns});
        for (columns) |score| try std.testing.expectEqual(@as(?f64, 3), score);
        try std.testing.expectEqual(@as(u64, 0), query.live_bytes);
    }
}

test "lite prepared plans sort overlapping prefixes and fan out shared keys" {
    var plan = @import("../prepared_score_plan.zig").PreparedScorePlan{};
    defer plan.deinit();
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{ "", "a" }, &.{ "b", "ab" }));
    try std.testing.expectEqual(@as(usize, 3), plan.read_keys.len);
    for (plan.read_keys[1..], plan.read_keys[0 .. plan.read_keys.len - 1]) |next, previous| {
        try std.testing.expect(std.mem.order(u8, previous, next) == .lt);
    }
    var first: [2]?f64 = @splat(null);
    var second: [2]?f64 = @splat(null);
    var output = @import("../prepared_score_plan.zig").PreparedScorePlan.Output{ .plan = &plan, .columns = &.{ &first, &second } };
    const sink = output.sink();
    for (0..plan.read_keys.len) |i| sink.write(sink.context, i, @floatFromInt(i + 1));
    try std.testing.expectEqualSlices(?f64, &.{ 3, 2 }, &first);
    try std.testing.expectEqualSlices(?f64, &.{ 2, 1 }, &second);
}

test "lite metadata nested backing pressure skips busy owners and retries after reclaim" {
    const Pool = @import("metadata_pool.zig").MetadataPool;
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var backing = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .lsm_block_table_cache, a, 1);
    defer backing.deinit();
    var pool = Pool{};
    defer pool.deinit();
    const alloc = pool.allocator(backing.threadSafeAllocator(), &manager);
    const Callback = struct {
        fn reclaim(ctx: *anyopaque, bytes: u64) u64 {
            const p: *Pool = @ptrCast(@alignCast(ctx));
            return p.reclaim(bytes);
        }
    };
    const registration = try manager.registerReclaimer(.lite_docstore_snapshot_cache, &pool, Callback.reclaim);
    defer manager.unregisterReclaimer(registration);
    const first = try alloc.alloc(u8, 16);
    const second = try alloc.alloc(u8, 16);
    alloc.free(first);
    alloc.free(second);
    _ = backing.releaseUnusedCreditThreadSafe();
    manager.memory.budget.hard_limit_bytes = manager.snapshot().memory.used_bytes;
    // The child holds its allocator lock while the backing allocation asks
    // the manager to reclaim the child's idle nodes. It must fail promptly.
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 2048));
    try std.testing.expectEqual(@as(usize, 2), pool.idle_count);
    try std.testing.expect(manager.reclaimForAllocation(.lsm_block_table_cache, 2048) > 0);
    const bytes = try alloc.alloc(u8, 2048);
    alloc.free(bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "lite prepared logical cache credit is not physical reclaim progress" {
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var backing = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .lsm_block_table_cache, a, 1);
    defer backing.deinit();
    var pool = @import("metadata_pool.zig").MetadataPool{};
    defer pool.deinit();
    const alloc = pool.allocator(backing.threadSafeAllocator(), &manager);
    const node = try alloc.alloc(u8, 16);
    const active = try alloc.alloc(u8, 16);
    defer alloc.free(active);
    alloc.free(node);
    _ = backing.releaseUnusedCreditThreadSafe();
    var second_budget = resource_manager_mod.BudgetedAllocator.init(&manager, .lite_docstore_snapshot_cache, a, 1);
    defer second_budget.deinit();
    const Other = struct {
        budget: *resource_manager_mod.BudgetedAllocator,
        bytes: ?[]u8,
        fn reclaim(ctx: *anyopaque, _: u64) u64 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const bytes = self.bytes orelse return 0;
            self.budget.allocator().free(bytes);
            self.bytes = null;
            return bytes.len;
        }
    };
    var other = Other{ .budget = &second_budget, .bytes = try second_budget.allocator().alloc(u8, 32 * 1024) };
    defer if (other.bytes) |bytes| second_budget.allocator().free(bytes);
    _ = second_budget.releaseUnusedCreditThreadSafe();
    const First = struct {
        fn reclaim(ctx: *anyopaque, bytes: u64) u64 {
            const p: *@import("metadata_pool.zig").MetadataPool = @ptrCast(@alignCast(ctx));
            const freed = p.reclaim(bytes);
            if (freed != 0) if (resource_manager_mod.BudgetedAllocator.fromAllocator(p.backing.?)) |owner| {
                _ = owner.releaseUnusedCreditThreadSafe();
            };
            return freed;
        }
    };
    const first_id = try manager.registerReclaimer(.lite_docstore_snapshot_cache, &pool, First.reclaim);
    defer manager.unregisterReclaimer(first_id);
    const second_id = try manager.registerReclaimer(.lite_docstore_snapshot_cache, &other, Other.reclaim);
    defer manager.unregisterReclaimer(second_id);
    manager.memory.budget.hard_limit_bytes = manager.snapshot().memory.used_bytes;
    const before = manager.snapshot().memory.used_bytes;
    var request = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .dense_search_working_set, a, 1);
    defer request.deinit();
    const result = request.threadSafeAllocator().alloc(u8, 8192);
    if (result) |bytes| request.threadSafeAllocator().free(bytes) else |_| {}
    const after = manager.snapshot().memory.used_bytes;
    std.debug.print("FRESH_RECLAIM request_ok={any} physical_freed={d} remaining_idle={any} requests={d}\n", .{ if (result) |_| true else |_| false, before - after, other.bytes != null, manager.reclaim_requests.load(.monotonic) });
    _ = try result;
    try std.testing.expect(other.bytes == null);
    try std.testing.expect(before - after >= 8192);
    _ = Other.reclaim(&other, 8192);
    const bytes = try request.threadSafeAllocator().alloc(u8, 8192);
    request.threadSafeAllocator().free(bytes);
}

test "lite prepared namespaced prepared fallback memory floor" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var keys: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&keys, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    const PlainScope = struct {
        inner: Txn.ReadScope,
        pub fn close(self: *@This()) void {
            self.inner.close();
        }
        pub fn reset(self: *@This()) void {
            self.inner.reset();
        }
        pub fn getManySorted(self: *@This(), k: []const []const u8, values: []?[]const u8) !void {
            return self.inner.getManySorted(k, values);
        }
        pub fn workingAllocator(self: *@This()) Allocator {
            return self.inner.workingAllocator();
        }
        pub fn visitManySortedScores(self: *@This(), k: []const []const u8, sink: backend_types.ScoreSink) !void {
            return self.inner.visitManySortedScores(k, sink);
        }
    };
    const PlainTxn = struct {
        inner: *Txn,
        pub fn getManySorted(self: *@This(), k: []const []const u8, values: []?[]const u8) !void {
            return self.inner.getManySorted(k, values);
        }
        pub fn openReadScope(self: *@This(), allocator: Allocator) !PlainScope {
            return .{ .inner = try self.inner.openReadScope(allocator) };
        }
    };
    var request = try @import("../../graph/score_read.zig").PreparedRequest.init(a, &.{""}, &nodes);
    defer request.deinit();
    var regressions: usize = 0;
    for (24..81) |step| {
        const limit = step * 2048;
        var successes: [3]bool = undefined;
        for (0..3) |mode| {
            const name = try std.fmt.allocPrint(a, "ns-{d}-{d}.aflite", .{ limit, mode });
            defer a.free(name);
            const path = try testPath(a, tmp, name);
            defer a.free(path);
            var limits = resource_manager_mod.Options.defaultBudgets();
            limits[@backingInt(resource_manager_mod.Slice.dense_search_working_set)] = .{ .hard_limit_bytes = limit };
            var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
            defer manager.deinit(a);
            var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
            defer store.close();
            try store.file.putDocumentBatch(&docs);
            var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
            defer query.deinit();
            var txn = try Txn.openReadWithPrefix(&store, "ns\x00");
            defer txn.abort();
            var columns: [512]?f64 = undefined;
            var plain = PlainTxn{ .inner = &txn };
            const result = if (mode == 0) @import("../../graph/score_read.zig").populate(query.allocator(), &plain, &.{""}, &nodes, &.{&columns}) else if (mode == 1) @import("../../graph/score_read.zig").populate(query.allocator(), &txn, &.{""}, &nodes, &.{&columns}) else @import("../../graph/score_read.zig").populatePrepared(query.allocator(), &txn, &request, &.{&columns});
            successes[mode] = if (result) |_| true else |err| switch (err) {
                error.OutOfMemory, error.ResourceBudgetExceeded => false,
                else => return err,
            };
            if (successes[mode] and mode == 2) {
                // Exercise compact staging as well as cold reads. Optional
                // residency must not increase the streaming memory floor.
                for (0..2) |_| {
                    _ = @import("../../graph/score_read.zig").populatePrepared(query.allocator(), &txn, &request, &.{&columns}) catch |err| switch (@as(anyerror, err)) {
                        error.OutOfMemory, error.ResourceBudgetExceeded => {
                            successes[mode] = false;
                            break;
                        },
                        else => return err,
                    };
                }
            }
            if (successes[mode]) for (columns) |score| try std.testing.expectEqual(@as(?f64, 3), score);
        }
        if (successes[0] and (!successes[1] or !successes[2])) {
            regressions += 1;
            std.debug.print("FRESH_NS_FLOOR limit={d} baseline_ok=true mutable_ok={any} owned_ok={any}\n", .{ limit, successes[1], successes[2] });
        }
    }
    try std.testing.expectEqual(@as(usize, 0), regressions);
}

test "lite prepared namespaced graph measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "prepared-measure.aflite");
    defer a.free(path);
    var file_budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = file_budget.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    for (&docs, &key_storage, &nodes, 0..) |*doc, *key, *node, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &score };
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [512]?f64 = undefined;
    const reader = @import("../../graph/score_read.zig");
    var reversed = nodes;
    std.mem.reverse([]const u8, &reversed);
    var shuffled = nodes;
    var random = std.Random.DefaultPrng.init(42);
    random.random().shuffle([]const u8, &shuffled);
    inline for (.{0}) |varying| {
        inline for (.{ false, true }) |erased| {
            for (0..5) |_| {
                const raw = try Txn.openReadWithPrefix(&store, "ns\x00");
                var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw) else raw;
                defer txn.abort();
                _ = try reader.populate(alloc, &txn, &.{""}, &nodes, &.{&columns});
            }
            const before = file_budget.alloc_calls;
            const cpu_before = std.posix.getrusage(0);
            for (0..5000) |i| {
                const requested_nodes = if (varying == 0 or i % 2 == 1) &nodes else if (varying == 1) &reversed else &shuffled;
                const raw = try Txn.openReadWithPrefix(&store, "ns\x00");
                var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw) else raw;
                defer txn.abort();
                _ = try reader.populate(alloc, &txn, &.{""}, requested_nodes, &.{&columns});
                for (columns) |value| try std.testing.expectEqual(@as(?f64, 3), value);
            }
            const cpu_after = std.posix.getrusage(0);
            const cpu_us = (cpu_after.utime.sec - cpu_before.utime.sec + cpu_after.stime.sec - cpu_before.stime.sec) * 1000000 + cpu_after.utime.usec - cpu_before.utime.usec + cpu_after.stime.usec - cpu_before.stime.usec;
            std.debug.print("LITE_NAMESPACED_MEASURE erased={any} varying={any} queries=5000 keys=512 cpu_us={d} file_allocations={d} scope_bytes={d}\n", .{ erased, varying, cpu_us, file_budget.alloc_calls - before, @sizeOf(Txn.ReadScope) });
            try std.testing.expectEqual(@as(usize, 0), file_budget.alloc_calls - before);
        }
    }
}

test "lite prepared resolved scores fence namespaces vacuum and reopened owners" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "resolved-generations.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    var store_open = true;
    defer if (store_open) store.close();
    const reader = @import("../../graph/score_read.zig");
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 2)), .little);
    try store.file.putDocumentBatch(&.{.{ .key = "p\x00a\x00\x00", .value = &raw }});
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocumentBatch(&.{.{ .key = "q\x00a\x00\x00", .value = &raw }});
    var scores: [2]?f64 = undefined;
    var old = try Txn.openReadWithPrefix(&store, "p\x00");
    var old_open = true;
    defer if (old_open) old.abort();
    _ = try reader.populate(a, &old, &.{""}, &.{ "a", "missing" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 2, null }, &scores);
    {
        var other = try Txn.openReadWithPrefix(&store, "q\x00");
        defer other.abort();
        _ = try reader.populate(a, &other, &.{""}, &.{ "a", "missing" }, &.{&scores});
        try std.testing.expectEqualSlices(?f64, &.{ 9, null }, &scores);
    }
    {
        var write = try Txn.openWriteWithPrefix(&store, "p\x00");
        errdefer write.abort();
        try write.put("a\x00\x00", &raw);
        try write.put("missing\x00\x00", &raw);
        try write.commit();
    }
    _ = try store.vacuum();
    {
        var fresh = try Txn.openReadWithPrefix(&store, "p\x00");
        defer fresh.abort();
        _ = try reader.populate(a, &fresh, &.{""}, &.{ "a", "missing" }, &.{&scores});
        try std.testing.expectEqualSlices(?f64, &.{ 9, 9 }, &scores);
    }
    _ = try reader.populate(a, &old, &.{""}, &.{ "a", "missing" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 2, null }, &scores);
    old.abort();
    old_open = false;
    const owner = store.score_cache_owner;
    store.close();
    store_open = false;
    store = try Store.openWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    store_open = true;
    try std.testing.expect(store.score_cache_owner != owner);
    var reopened = try Txn.openReadWithPrefix(&store, "p\x00");
    defer reopened.abort();
    _ = try reader.populate(a, &reopened, &.{""}, &.{ "a", "missing" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 9, 9 }, &scores);
}

test "lite prepared optional promotion preserves successful scratch" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-retention.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    const limits = resource_manager_mod.Options.defaultBudgets();
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    for (8..65) |step| {
        _ = Store.reclaimIdleWorkspaces(&store, std.math.maxInt(u64));
        manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = step * 2048;
        var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
        defer query.deinit();
        var successes: usize = 0;
        const before = counter.alloc_calls;
        var retained: usize = 0;
        for (0..3) |_| {
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            var scope = try txn.openReadScope(query.allocator());
            var scores: [512]?f64 = undefined;
            const result = scope.getManySortedScores(&keys, &scores);
            if (result) |_| {
                successes += 1;
                for (scores) |v| try std.testing.expectEqual(@as(?f64, 3), v);
            } else |err| switch (err) {
                error.OutOfMemory, error.ResourceBudgetExceeded => {},
                else => return err,
            }
            scope.close();
            retained = if (store.workspaces[0]) |w| w.live else 0;
        }
        if (successes == 3) try std.testing.expect(retained != 0);
        if (successes == 3 and step == 64) std.debug.print("REVIEW_RETENTION limit={d} successful={d} allocations={d} retained={d} denials={d}\n", .{ step * 2048, successes, counter.alloc_calls - before, retained, query.denial_generation });
    }
}

test "lite prepared mixed document cache measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "prepared-measure.aflite");
    defer a.free(path);
    var file_budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = file_budget.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    for (&docs, &key_storage, &nodes, 0..) |*doc, *key, *node, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &score };
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [512]?f64 = undefined;
    const reader = @import("../../graph/score_read.zig");

    inline for (.{ 0, 1 }) |varying| {
        inline for (.{false}) |erased| {
            for (0..5) |_| {
                const raw = try Txn.openReadWithPrefix(&store, "ns\x00");
                var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw) else raw;
                defer txn.abort();
                _ = try reader.populate(alloc, &txn, &.{""}, &nodes, &.{&columns});
                if (varying == 1) {
                    var scope = try txn.openReadScope(alloc);
                    defer scope.close();
                    var buf: [8]u8 = undefined;
                    var values: [1]?[]const u8 = undefined;
                    try scope.getManySortedInto(&.{key_storage[0][3..]}, &.{&buf}, &values);
                }
            }
            const before = file_budget.alloc_calls;
            const cpu_before = std.posix.getrusage(0);
            for (0..5000) |i| {
                const requested_nodes = &nodes;
                _ = i;
                const raw = try Txn.openReadWithPrefix(&store, "ns\x00");
                var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw) else raw;
                defer txn.abort();
                _ = try reader.populate(alloc, &txn, &.{""}, requested_nodes, &.{&columns});
                if (varying == 1) {
                    var scope = try txn.openReadScope(alloc);
                    defer scope.close();
                    var buf: [8]u8 = undefined;
                    var values: [1]?[]const u8 = undefined;
                    try scope.getManySortedInto(&.{key_storage[0][3..]}, &.{&buf}, &values);
                    try std.testing.expectEqual(@as(usize, 8), values[0].?.len);
                }
                for (columns) |value| try std.testing.expectEqual(@as(?f64, 3), value);
            }
            const cpu_after = std.posix.getrusage(0);
            const cpu_us = (cpu_after.utime.sec - cpu_before.utime.sec + cpu_after.stime.sec - cpu_before.stime.sec) * 1000000 + cpu_after.utime.usec - cpu_before.utime.usec + cpu_after.stime.usec - cpu_before.stime.usec;
            std.debug.print("REVIEW_MIXED_MEASURE erased={any} varying={any} queries=5000 keys=512 cpu_us={d} file_allocations={d} scope_bytes={d}\n", .{ erased, varying, cpu_us, file_budget.alloc_calls - before, @sizeOf(Txn.ReadScope) });
            try std.testing.expectEqual(@as(usize, 0), file_budget.alloc_calls - before);
        }
    }
}

test "lite prepared one store reclaim frees physical workspace" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-one-reclaimer.aflite");
    defer a.free(path);
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var backing = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .lsm_block_table_cache, a, 1);
    defer backing.deinit();
    var store = try Store.createWithOptions(backing.threadSafeAllocator(), path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const metadata = store.readMetadataAllocator();
    const node = try metadata.alloc(u8, 16);
    const active = try metadata.alloc(u8, 16);
    defer metadata.free(active);
    metadata.free(node);
    const w = try ReadWorkspace.create(store.allocator, &manager);
    w.score_plan.backing = w.allocator();
    w.score_plan.storage = try w.allocator().alignedAlloc(u8, .of(usize), 32 * 1024);
    store.workspaces[0] = w;
    _ = backing.releaseUnusedCreditThreadSafe();
    const before = manager.snapshot().memory.used_bytes;
    manager.memory.budget.hard_limit_bytes = before;
    var request = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .dense_search_working_set, a, 1);
    defer request.deinit();
    const first = request.threadSafeAllocator().alloc(u8, 8192);
    if (first) |bytes| request.threadSafeAllocator().free(bytes) else |_| {}
    const after = manager.snapshot().memory.used_bytes;
    std.debug.print("REVIEW_ONE_OWNER success={any} physical_freed={d} idle_workspace={any}\n", .{ if (first) |_| true else |_| false, before - after, store.workspaces[0] != null });
    _ = try first;
    try std.testing.expect(store.workspaces[0] == null);
    _ = Store.reclaimIdleWorkspaces(&store, std.math.maxInt(u64));
    const second = try request.threadSafeAllocator().alloc(u8, 8192);
    request.threadSafeAllocator().free(second);
}

test "lite prepared mixed document cache evicts under tight scope budgets" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "mixed-pressure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    const limits = resource_manager_mod.Options.defaultBudgets();
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var repeated: [1024][]const u8 = undefined;
    for (keys, 0..) |key, i| {
        repeated[2 * i] = key;
        repeated[2 * i + 1] = key;
    }
    var storage: [1024][8]u8 = undefined;
    var buffers: [1024][]u8 = undefined;
    for (&buffers, &storage) |*out, *buf| out.* = buf;
    var values: [1024]?[]const u8 = undefined;
    var evictions: usize = 0;
    for (40..161) |step| {
        if (step % 8 != 0) continue;
        var successes: [2]bool = undefined;
        for (0..2) |mode| {
            _ = Store.reclaimIdleWorkspaces(&store, std.math.maxInt(u64));
            manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = 0;
            var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
            defer query.deinit();
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            var scope = try txn.openReadScope(query.allocator());
            defer scope.close();
            var scores: [512]?f64 = undefined;
            try scope.getManySortedScores(&keys, &scores);
            const w = scope.workspace.?;
            try std.testing.expect(w.resolved_scores.storage != null);
            if (mode == 0) w.resolved_scores.deinit(w.allocator());
            manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = step * 1024;
            const result = scope.getManySortedInto(&repeated, &buffers, &values);
            successes[mode] = if (result) |_| true else |err| switch (err) {
                error.OutOfMemory, error.ResourceBudgetExceeded => false,
                else => return err,
            };
            if (successes[mode]) {
                for (values) |value| try std.testing.expectEqualSlices(u8, &raw, value.?);
                try std.testing.expect(!w.allocation_failed);
                if (mode == 1 and w.resolved_scores.storage == null) evictions += 1;
            }
        }
        if (successes[0]) try std.testing.expect(successes[1]);
    }
    try std.testing.expect(evictions > 0);
}

test "lite prepared small budget streaming reuses scratch after optional plan denial" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "prepared-measure.aflite");
    defer a.free(path);
    var file_budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = file_budget.allocator();
    var limits = resource_manager_mod.Options.defaultBudgets();
    limits[@backingInt(resource_manager_mod.Slice.dense_search_working_set)] = .{ .hard_limit_bytes = 48 * 1024 };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    for (&docs, &key_storage, &nodes, 0..) |*doc, *key, *node, i| {
        node.* = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        doc.* = .{ .key = key, .value = &score };
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [512]?f64 = undefined;
    for (0..10) |i| {
        const before = file_budget.alloc_calls;
        var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
        defer query.deinit();
        var txn = try store.beginRead();
        defer txn.abort();
        _ = try @import("../../graph/score_read.zig").populate(query.allocator(), &txn, &.{""}, &nodes, &.{&columns});
        for (columns) |value| try std.testing.expectEqual(@as(?f64, 3), value);
        if (i >= 5) try std.testing.expectEqual(before, file_budget.alloc_calls);
    }
}

test "lite prepared cached scores retry after page cache eviction" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-retention.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    const limits = resource_manager_mod.Options.defaultBudgets();
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var buffer: [8]u8 = undefined;
    for (keys) |key| _ = try txn.getInto(key, &buffer);
    var scope = try txn.openReadScope(query.allocator());
    defer scope.close();
    var scores: [512]?f64 = undefined;
    try scope.getManySortedScores(&keys, &scores);
    const w = scope.workspace.?;
    try std.testing.expect(w.resolved_scores.storage != null);
    try std.testing.expect(w.native_state.window.bytes == null);
    // Exercise the supported offset-only cache path, independently of the
    // decoded hit that intentionally requires no reader growth.
    w.resolved_scores.decoded_ready = false;
    const file = try txn.readFile();
    try std.testing.expect(file.page_cache.total_bytes > 0);

    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = w.live + 1024;
    const Output = struct {
        manager: *resource_manager_mod.ResourceManager,
        counts: [512]u8 = @splat(0),
        scores: *[512]?f64,
        first: bool = true,
        fn write(ctx: *anyopaque, index: usize, score: f64) void {
            const out: *@This() = @ptrCast(@alignCast(ctx));
            out.counts[index] += 1;
            out.scores[index] = score;
            if (out.first) {
                out.first = false;
                out.manager.slices[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)].budget.hard_limit_bytes = 1;
            }
        }
    };
    var output = Output{ .manager = &manager, .scores = &scores };
    const result = scope.visitManySortedScores(&keys, .{ .context = &output, .write = Output.write });
    try result;
    for (output.counts) |count| try std.testing.expectEqual(@as(u8, 1), count);
    try std.testing.expect(!w.allocation_failed);
    try std.testing.expect(!scope.discard_workspace);
    for (scores) |score| try std.testing.expectEqual(@as(?f64, 3), score);
    const before = counter.alloc_calls;
    try scope.getManySortedScores(&keys, &scores);
    try std.testing.expectEqual(before, counter.alloc_calls);
}

test "lite prepared point versus batch scope measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "point-review.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][11]u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    for (&docs, &key_storage, 0..) |*doc, *key, i| {
        @memcpy(key[0..3], "ns\x00");
        _ = try std.fmt.bufPrint(key[3..], "{d:0>8}", .{i});
        doc.* = .{ .key = key, .value = "document" };
    }
    try store.file.putDocumentBatch(&docs);
    inline for (.{ false, true }) |batch| {
        for (0..5) |_| {
            var txn = try Txn.openReadWithPrefix(&store, "ns\x00");
            defer txn.abort();
            var scope = try txn.openReadScope(alloc);
            defer scope.close();
            var buffer: [8]u8 = undefined;
            if (batch) {
                var values: [1]?[]const u8 = undefined;
                try scope.getManySortedInto(&.{key_storage[0][3..]}, &.{&buffer}, &values);
                try std.testing.expectEqualStrings("document", values[0].?);
            } else try std.testing.expectEqualStrings("document", try scope.getInto(key_storage[0][3..], &buffer));
        }
        const before = counter.alloc_calls;
        const cpu_before = std.posix.getrusage(0);
        for (0..5000) |_| {
            var txn = try Txn.openReadWithPrefix(&store, "ns\x00");
            defer txn.abort();
            var scope = try txn.openReadScope(alloc);
            defer scope.close();
            var buffer: [8]u8 = undefined;
            if (batch) {
                var values: [1]?[]const u8 = undefined;
                try scope.getManySortedInto(&.{key_storage[0][3..]}, &.{&buffer}, &values);
                try std.testing.expectEqualStrings("document", values[0].?);
            } else try std.testing.expectEqualStrings("document", try scope.getInto(key_storage[0][3..], &buffer));
        }
        const cpu_after = std.posix.getrusage(0);
        const cpu_us = (cpu_after.utime.sec - cpu_before.utime.sec + cpu_after.stime.sec - cpu_before.stime.sec) * 1000000 + cpu_after.utime.usec - cpu_before.utime.usec + cpu_after.stime.usec - cpu_before.stime.usec;
        try std.testing.expectEqual(@as(usize, 0), counter.alloc_calls - before);
        std.debug.print("POINT_REVIEW batch={any} cpu_us={d} allocations={d}\n", .{ batch, cpu_us, counter.alloc_calls - before });
    }
}

test "lite prepared returned documents cache pressure" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "mixed-pressure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    const limits = resource_manager_mod.Options.defaultBudgets();
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var repeated: [1024][]const u8 = undefined;
    for (keys, 0..) |key, i| {
        repeated[2 * i] = key;
        repeated[2 * i + 1] = key;
    }
    var storage: [1024][8]u8 = undefined;
    var buffers: [1024][]u8 = undefined;
    for (&buffers, &storage) |*out, *buf| out.* = buf;
    var values: [1024]?[]const u8 = undefined;
    var evictions: usize = 0;

    for (40..161) |step| {
        if (step % 8 != 0) continue;
        var successes: [2]bool = undefined;
        for (0..2) |mode| {
            _ = Store.reclaimIdleWorkspaces(&store, std.math.maxInt(u64));
            manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = 0;
            var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
            defer query.deinit();
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            var scope = try txn.openReadScope(query.allocator());
            defer scope.close();
            var scores: [512]?f64 = undefined;
            try scope.getManySortedScores(&keys, &scores);
            const w = scope.workspace.?;
            try std.testing.expect(w.resolved_scores.storage != null);
            if (mode == 0) w.resolved_scores.deinit(w.allocator());
            manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = step * 1024;
            const result = scope.getManySorted(&repeated, &values);
            successes[mode] = if (result) |_| true else |err| switch (err) {
                error.OutOfMemory, error.ResourceBudgetExceeded => false,
                else => return err,
            };
            if (successes[mode]) {
                for (values) |value| try std.testing.expectEqualSlices(u8, &raw, value.?);
                try std.testing.expect(!w.allocation_failed);
                if (mode == 1 and w.resolved_scores.storage == null) evictions += 1;
            }
        }
        if (successes[0]) try std.testing.expect(successes[1]);
    }
    try std.testing.expect(evictions > 0);
}

test "lite prepared prefix permutation measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "prefix-review.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [1024][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [1024]native.DocumentMutation = undefined;
    var score: [2][8]u8 = undefined;
    const prefixes = [_]?[]const u8{ "m1/", "m2/" };
    for (&score, 0..) |*raw, column| std.mem.writeInt(u64, raw, @bitCast(@as(f64, @floatFromInt(column + 3))), .little);
    for (prefixes, 0..) |prefix, column| {
        for (0..512) |i| {
            const key = &key_storage[column * 512 + i];
            @memcpy(key[0..3], prefix.?);
            const node = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
            key[11] = 0;
            key[12] = 0;
            if (column == 0) nodes[i] = node;
            docs[column * 512 + i] = .{ .key = key, .value = &score[column] };
        }
    }
    try store.file.putDocumentBatch(&docs);
    var columns: [2][512]?f64 = undefined;
    inline for (.{ false, true }) |varying| {
        for (0..5) |_| {
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            _ = try @import("../../graph/score_read.zig").populate(alloc, &txn, &prefixes, &nodes, &.{ &columns[0], &columns[1] });
        }
        const before = counter.alloc_calls;
        const cpu_before = std.posix.getrusage(0);
        for (0..5000) |iteration| {
            const swap = varying and iteration % 2 == 1;
            const requested = if (swap) [_]?[]const u8{ prefixes[1], prefixes[0] } else prefixes;
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            _ = try @import("../../graph/score_read.zig").populate(alloc, &txn, &requested, &nodes, &.{ &columns[0], &columns[1] });
            for (columns, 0..) |values, column| for (values) |value| try std.testing.expectEqual(@as(?f64, @floatFromInt(3 + (if (swap) 1 - column else column))), value);
        }
        const cpu_after = std.posix.getrusage(0);
        const cpu_us = (cpu_after.utime.sec - cpu_before.utime.sec + cpu_after.stime.sec - cpu_before.stime.sec) * 1000000 + cpu_after.utime.usec - cpu_before.utime.usec + cpu_after.stime.usec - cpu_before.stime.usec;
        try std.testing.expectEqual(@as(usize, 0), counter.alloc_calls - before);
        std.debug.print("PREFIX_REVIEW varying={any} cpu_us={d} allocations={d}\n", .{ varying, cpu_us, counter.alloc_calls - before });
    }
}

test "lite prepared result eviction preserves earlier values and accounting" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "result-lifetime.aflite");
    defer a.free(path);
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const big = try a.alloc(u8, 32 * 1024);
    defer a.free(big);
    @memset(big, 'x');
    try store.file.putDocument("large", big);
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var scope = try txn.openReadScope(query.allocator());
    var scope_open = true;
    defer if (scope_open) scope.close();
    var scores: [512]?f64 = undefined;
    try scope.getManySortedScores(&keys, &scores);
    const earlier = try scope.get(keys[0]);
    const w = scope.workspace.?;
    try std.testing.expect(w.resolved_scores.storage != null);
    _ = query.releaseUnusedCredit();
    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = query.live_bytes + 24 * 1024;
    const returned = try scope.get("large");
    try std.testing.expectEqualSlices(u8, big, returned);
    try std.testing.expectEqualSlices(u8, &raw, earlier);
    try std.testing.expect(w.resolved_scores.storage == null);
    scope.close();
    scope_open = false;
    try std.testing.expectEqual(@as(u64, 0), query.live_bytes);
}

test "lite prepared unavailable resize preserves optional cache" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "result-lifetime.aflite");
    defer a.free(path);
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const big = try a.alloc(u8, 32 * 1024);
    defer a.free(big);
    @memset(big, 'x');
    try store.file.putDocument("large", big);
    const query_backing = Allocator{ .ptr = a.ptr, .vtable = &.{ .alloc = a.vtable.alloc, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = a.vtable.free } };
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, query_backing, 1);
    defer query.deinit();
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var scope = try txn.openReadScope(query.allocator());
    var scope_open = true;
    defer if (scope_open) scope.close();
    var scores: [512]?f64 = undefined;
    try scope.getManySortedScores(&keys, &scores);
    const earlier = try scope.get(keys[0]);
    const w = scope.workspace.?;
    try std.testing.expect(w.resolved_scores.storage != null);
    const returned = try scope.get("large");
    try std.testing.expectEqualSlices(u8, big, returned);
    try std.testing.expectEqualSlices(u8, &raw, earlier);
    try std.testing.expect(w.resolved_scores.storage != null);
    scope.close();
    scope_open = false;
    try std.testing.expectEqual(@as(u64, 0), query.live_bytes);
}
test "lite prepared minimum result growth preserves optional cache" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "result-lifetime.aflite");
    defer a.free(path);
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const big = try a.alloc(u8, 32 * 1024);
    defer a.free(big);
    @memset(big, 'x');
    try store.file.putDocument("large", big);
    const query_backing = Allocator{ .ptr = a.ptr, .vtable = &.{ .alloc = a.vtable.alloc, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = a.vtable.free } };
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, query_backing, 1);
    defer query.deinit();
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var scope = try txn.openReadScope(query.allocator());
    defer scope.close();
    var scores: [512]?f64 = undefined;
    try scope.getManySortedScores(&keys, &scores);
    const w = scope.workspace.?;
    const cache_bytes = w.resolved_scores.storage.?.len;
    _ = query.releaseUnusedCredit();
    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = query.live_bytes + 48;
    const minimum = try query.allocator().alloc(u8, 40);
    try std.testing.expect(w.resolved_scores.storage != null);
    query.allocator().free(minimum);
    _ = query.releaseUnusedCredit();
    const returned = try scope.get(keys[0]);
    try std.testing.expectEqualSlices(u8, &raw, returned);
    try std.testing.expect(w.resolved_scores.storage != null);
    try std.testing.expectEqual(cache_bytes, w.resolved_scores.storage.?.len);
}

test "lite prepared scope closes with LIFO query backing" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-lifo.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const small: [64]u8 = @splat('a');
    const big: [256]u8 = @splat('b');
    try store.file.putDocumentBatch(&.{ .{ .key = "a", .value = &small }, .{ .key = "b", .value = &big } });
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var buffer: [1024]u8 align(64) = undefined;
    var backing = std.heap.FixedBufferAllocator.init(&buffer);
    var completed: usize = 0;
    for (0..16) |_| {
        var scope = try txn.openReadScope(backing.allocator());
        defer scope.close();
        try std.testing.expect(scope.workspace == null);
        const first = scope.get("a") catch |err| {
            if (err == error.OutOfMemory) break else return err;
        };
        try std.testing.expectEqualSlices(u8, &small, first);
        const second = scope.get("b") catch |err| {
            if (err == error.OutOfMemory) break else return err;
        };
        try std.testing.expectEqualSlices(u8, &big, second);
        completed += 1;
    }
    try std.testing.expectEqual(@as(usize, 16), completed);
    try std.testing.expectEqual(@as(usize, 0), backing.end_index);
}

test "lite prepared owned document batch measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "owned-batch-measure.aflite");
    defer a.free(path);
    inline for (.{ 2048, 8192, 65536 }) |size| {
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        const alloc = counter.allocator();
        var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
        defer store.close();
        const count = 2 * 1024 * 1024 / size;
        var payload: [size]u8 = @splat('v');
        var key_storage: [count][8]u8 = undefined;
        var keys: [count][]const u8 = undefined;
        var docs: [count]native.DocumentMutation = undefined;
        for (&key_storage, &keys, &docs, 0..) |*key, *out, *doc, i| {
            std.mem.writeInt(u64, key, i, .big);
            out.* = key;
            doc.* = .{ .key = key, .value = &payload };
        }
        try store.file.putDocumentBatch(&docs);
        var values: [count]?[]const u8 = undefined;
        for (0..5) |_| {
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            var scope = try txn.openReadScope(alloc);
            defer scope.close();
            try scope.getManySorted(&keys, &values);
        }
        const live_before = counter.live;
        counter.peak = live_before;
        const before = counter.alloc_calls;
        const cpu_before = std.posix.getrusage(0);
        for (0..50) |_| {
            var txn = try Txn.openRead(&store);
            defer txn.abort();
            var scope = try txn.openReadScope(alloc);
            defer scope.close();
            try scope.getManySorted(&keys, &values);
            for (values) |value| try std.testing.expectEqualSlices(u8, &payload, value.?);
            scope.reset();
            try std.testing.expect(scope.scratch.arena.queryCapacity() <= 256 * 1024);
        }
        const cpu_after = std.posix.getrusage(0);
        const cpu_us = (cpu_after.utime.sec - cpu_before.utime.sec + cpu_after.stime.sec - cpu_before.stime.sec) * 1000000 + cpu_after.utime.usec - cpu_before.utime.usec + cpu_after.stime.usec - cpu_before.stime.usec;
        try std.testing.expectEqual(live_before, counter.live);
        std.debug.print("LITE_OWNED_MEASURE size={d} documents={d} queries=50 cpu_us={d} allocations={d} peak_bytes={d}\n", .{ size, count, cpu_us, counter.alloc_calls - before, counter.peak - live_before });
    }
}

test "lite prepared complete delivery overwrites missing deleted duplicate and permuted outputs" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "complete-prepared.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 2)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p/a\x00\x00", .value = &raw }, .{ .key = "ns\x00p/a\x00\x00", .value = &raw } });
    const reader = @import("../../graph/score_read.zig");
    inline for (.{ false, true }) |erased| {
        inline for (.{ "", "ns\x00" }) |namespace| {
            var txn = if (erased) try backend_erased.writeTxnFrom(a, try Txn.openWriteWithPrefix(&store, namespace)) else try Txn.openWriteWithPrefix(&store, namespace);
            defer txn.abort();
            var columns: [3][3]?f64 = undefined;
            for (0..3) |_| {
                for (&columns) |*column| @memset(column, 123);
                _ = try reader.populate(a, &txn, &.{ "p/", null, "p/" }, &.{ "b", "a", "a" }, &.{ &columns[0], &columns[1], &columns[2] });
                try std.testing.expectEqualSlices(?f64, &.{ null, 2, 2 }, &columns[0]);
                try std.testing.expectEqualSlices(?f64, &.{ null, null, null }, &columns[1]);
                try std.testing.expectEqualSlices(?f64, &columns[0], &columns[2]);
            }
            try txn.put("p/b\x00\x00", &raw);
            try txn.delete("p/a\x00\x00");
            for (&columns) |*column| @memset(column, 123);
            _ = try reader.populate(a, &txn, &.{ null, "p/", "p/" }, &.{ "a", "b", "a" }, &.{ &columns[0], &columns[1], &columns[2] });
            try std.testing.expectEqualSlices(?f64, &.{ null, null, null }, &columns[0]);
            try std.testing.expectEqualSlices(?f64, &.{ null, 2, null }, &columns[1]);
            try std.testing.expectEqualSlices(?f64, &columns[1], &columns[2]);
            try txn.put("p/b\x00\x00", "invalid");
            try std.testing.expectError(error.InvalidGraphMetricScore, reader.populate(a, &txn, &.{ null, "p/", "p/" }, &.{ "a", "b", "a" }, &.{ &columns[0], &columns[1], &columns[2] }));
            for (columns) |column| try std.testing.expectEqualSlices(?f64, &.{ null, null, null }, &column);
        }
    }
}

test "lite prepared nested query arena releases result accounting" {
    const a = std.testing.allocator;
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(a);
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "nested-result-budget.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .resource_manager = &manager, .reclamation = .{ .enabled = false } });
    defer store.close();
    const small: [64]u8 = @splat('a');
    const big: [256]u8 = @splat('b');
    try store.file.putDocumentBatch(&.{ .{ .key = "a", .value = &small }, .{ .key = "b", .value = &big } });
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    for (0..16) |_| {
        var arena = std.heap.ArenaAllocator.init(query.allocator());
        var scope = try txn.openReadScope(arena.allocator());
        const first = try scope.get("a");
        const second = try scope.get("b");
        try std.testing.expectEqualSlices(u8, &small, first);
        try std.testing.expectEqualSlices(u8, &big, second);
        scope.close();
        // Arena backing retains its own chunks until the query tears down.
        arena.deinit();
        try std.testing.expectEqual(@as(u64, 0), query.live_bytes);
    }
}

test "lite prepared lazy fallback clears failures and avoids empty invalid batch allocation" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "lazy-fallback-error.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    try store.file.putDocument("doc", "value");
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var buffer: [0]u8 = .{};
    var backing = std.heap.FixedBufferAllocator.init(&buffer);
    var scope = try txn.openReadScope(backing.allocator());
    defer scope.close();
    try std.testing.expect(scope.workspace == null);
    try scope.getManySorted(&.{}, &.{});
    try scope.getManySortedInto(&.{}, &.{}, &.{});
    var invalid: [2]?[]const u8 = .{ "stale", "stale" };
    try std.testing.expectError(error.InvalidBatch, scope.getManySorted(&.{ "b", "a" }, &invalid));
    try std.testing.expectEqual(@as(?[]const u8, null), invalid[0]);
    try std.testing.expectEqual(@as(?[]const u8, null), invalid[1]);
    try std.testing.expect(scope.fallback == null);
    var values: [1]?[]const u8 = .{"stale"};
    try std.testing.expectError(error.OutOfMemory, scope.getManySorted(&.{"doc"}, &values));
    try std.testing.expectEqual(@as(?[]const u8, null), values[0]);
    values[0] = "stale";
    var out: [8]u8 = undefined;
    try std.testing.expectError(error.OutOfMemory, scope.getManySortedInto(&.{"doc"}, &.{&out}, &values));
    try std.testing.expectEqual(@as(?[]const u8, null), values[0]);
}

test "lite prepared immutable request owns inputs and fences pending writes and cache bypass" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "immutable-request.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocumentBatch(&.{.{ .key = "ns\x00p/a\x00\x00", .value = &raw }});
    const reader = @import("../../graph/score_read.zig");
    var node: [1]u8 = .{'a'};
    var prefix: [2]u8 = .{ 'p', '/' };
    var request = try reader.PreparedRequest.init(a, &.{ &prefix, null, &prefix }, &.{ &node, "missing", &node });
    defer request.deinit();
    node[0] = 'b';
    prefix[0] = 'q';
    var columns: [3][3]?f64 = undefined;
    inline for (.{ false, true }) |erased| {
        const raw_txn = try Txn.openReadWithPrefix(&store, "ns\x00");
        var txn = if (erased) try backend_erased.readTxnFrom(alloc, raw_txn) else raw_txn;
        defer txn.abort();
        for (0..4) |_| _ = try reader.populatePrepared(alloc, &txn, &request, &.{ &columns[0], &columns[1], &columns[2] });
        const before = counter.alloc_attempts;
        _ = try reader.populatePrepared(alloc, &txn, &request, &.{ &columns[0], &columns[1], &columns[2] });
        try std.testing.expectEqual(before, counter.alloc_attempts);
        try std.testing.expectEqualSlices(?f64, &.{ 3, null, 3 }, &columns[0]);
        try std.testing.expectEqualSlices(?f64, &.{ null, null, null }, &columns[1]);
        try std.testing.expectEqualSlices(?f64, &.{ 3, null, 3 }, &columns[2]);
    }
    var txn = try Txn.openWriteWithPrefix(&store, "ns\x00");
    defer txn.abort();
    _ = try reader.populatePrepared(alloc, &txn, &request, &.{ &columns[0], &columns[1], &columns[2] });
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    try txn.put("p/a\x00\x00", &raw);
    _ = try reader.populatePrepared(alloc, &txn, &request, &.{ &columns[0], &columns[1], &columns[2] });
    try std.testing.expectEqualSlices(?f64, &.{ 9, null, 9 }, &columns[0]);
    try txn.delete("p/a\x00\x00");
    _ = try reader.populatePrepared(alloc, &txn, &request, &.{ &columns[0], &columns[1], &columns[2] });
    try std.testing.expectEqualSlices(?f64, &.{ null, null, null }, &columns[0]);
    // Bypass makes validation and record reads observable again, and must
    // invalidate decoded ownership even for the same immutable request.
    var read = try Txn.openReadWithPrefix(&store, "ns\x00");
    defer read.abort();
    const file = try read.readFile();
    file.page_cache_enabled.store(false, .monotonic);
    defer file.page_cache_enabled.store(true, .monotonic);
    _ = try reader.populatePrepared(alloc, &read, &request, &.{ &columns[0], &columns[1], &columns[2] });
    try std.testing.expectEqualSlices(?f64, &.{ 3, null, 3 }, &columns[0]);
    for (store.workspaces) |slot| if (slot) |w| try std.testing.expect(!w.resolved_scores.decoded_ready);
}

test "lite prepared invalid scores never publish a partial decoded batch" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "decoded-invalid.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var good: [8]u8 = undefined;
    var bad: [8]u8 = undefined;
    std.mem.writeInt(u64, &good, @bitCast(@as(f64, 3)), .little);
    std.mem.writeInt(u64, &bad, @bitCast(std.math.inf(f64)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "a\x00\x00", .value = &good }, .{ .key = "b\x00\x00", .value = &bad } });
    const reader = @import("../../graph/score_read.zig");
    var request = try reader.PreparedRequest.init(a, &.{""}, &.{ "a", "b" });
    defer request.deinit();
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var column: [2]?f64 = .{ 99, 99 };
    for (0..3) |_| {
        try std.testing.expectError(error.InvalidGraphMetricScore, reader.populatePrepared(a, &txn, &request, &.{&column}));
        try std.testing.expectEqualSlices(?f64, &.{ null, null }, &column);
        for (store.workspaces) |slot| if (slot) |w| try std.testing.expect(!w.resolved_scores.decoded_ready);
    }
}

test "lite prepared immutable request measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "immutable-measure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const reader = @import("../../graph/score_read.zig");
    var request = try reader.PreparedRequest.init(a, &.{""}, &nodes);
    defer request.deinit();
    var column: [512]?f64 = undefined;
    inline for (.{ false, true }) |immutable| {
        for (0..5) |_| {
            var txn = try Txn.openReadWithPrefix(&store, "ns\x00");
            defer txn.abort();
            if (immutable) {
                _ = try reader.populatePrepared(alloc, &txn, &request, &.{&column});
            } else {
                _ = try reader.populate(alloc, &txn, &.{""}, &nodes, &.{&column});
            }
        }
        const before = counter.alloc_calls;
        const started = std.posix.getrusage(0);
        for (0..5000) |_| {
            var txn = try Txn.openReadWithPrefix(&store, "ns\x00");
            defer txn.abort();
            if (immutable) {
                _ = try reader.populatePrepared(alloc, &txn, &request, &.{&column});
            } else {
                _ = try reader.populate(alloc, &txn, &.{""}, &nodes, &.{&column});
            }
            for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
        }
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        try std.testing.expectEqual(before, counter.alloc_calls);
        std.debug.print("LITE_IMMUTABLE immutable={any} cpu_us={d} allocations={d}\n", .{ immutable, cpu, counter.alloc_calls - before });
    }
}

test "lite prepared changing checkpoint measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "changing-checkpoint-measure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        node.* = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var column: [512]?f64 = undefined;
    const reader = @import("../../graph/score_read.zig");
    var cpu_us: i64 = 0;
    var allocations: usize = 0;
    for (0..200) |i| {
        std.mem.writeInt(u64, &raw, @bitCast(@as(f64, @floatFromInt(i))), .little);
        var write = try Txn.openWrite(&store);
        errdefer write.abort();
        try write.put(&key_storage[0], &raw);
        try write.commit();
        var txn = try Txn.openRead(&store);
        defer txn.abort();
        const before = counter.alloc_calls;
        const start = std.posix.getrusage(0);
        _ = try reader.populate(alloc, &txn, &.{""}, &nodes, &.{&column});
        const end = std.posix.getrusage(0);
        cpu_us += (end.utime.sec - start.utime.sec + end.stime.sec - start.stime.sec) * 1000000 + end.utime.usec - start.utime.usec + end.stime.usec - start.stime.usec;
        allocations += counter.alloc_calls - before;
        try std.testing.expectEqual(@as(?f64, @floatFromInt(i)), column[0]);
        for (column[1..]) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    }
    std.debug.print("LITE_CHANGING queries=200 keys=512 cpu_us={d} allocations={d}\n", .{ cpu_us, allocations });
}

test "lite prepared empty owned requests need no query allocations" {
    const a = std.testing.allocator;
    const reader = @import("../../graph/score_read.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "empty-owned-request.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    try store.file.putDocument("doc", "value");
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    var request = try reader.PreparedRequest.init(a, &.{null}, &.{"node"});
    defer request.deinit();
    var empty_request = try reader.PreparedRequest.init(a, &.{"p/"}, &.{});
    defer empty_request.deinit();
    inline for (.{ false, true }) |erased| {
        const raw = try Txn.openRead(&store);
        var txn = if (erased) try backend_erased.readTxnFrom(a, raw) else raw;
        defer txn.abort();
        var values = [_]?f64{123};
        const stats = try reader.populatePrepared(budget.allocator(), &txn, &request, &.{&values});
        try std.testing.expectEqual(@as(?f64, null), values[0]);
        try std.testing.expectEqual(@as(usize, 0), stats.keys);
        var empty: [0]?f64 = .{};
        _ = try reader.populatePrepared(budget.allocator(), &txn, &empty_request, &.{&empty});
        try std.testing.expectError(error.InvalidBatch, reader.populatePrepared(budget.allocator(), &txn, &request, &.{&empty}));
    }
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_attempts);
}

test "lite prepared compact decoded cache covers large namespaced batches" {
    const a = std.testing.allocator;
    const reader = @import("../../graph/score_read.zig");
    const count = 2500;
    const namespace = "a long namespace for compact scores\x00";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "compact-owned-request.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_bytes: [count][namespace.len + 10]u8 = undefined;
    var nodes: [count][]const u8 = undefined;
    var docs: [count - 1]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 7)), .little);
    for (&key_bytes, &nodes, 0..) |*key, *node, i| {
        @memcpy(key[0..namespace.len], namespace);
        node.* = try std.fmt.bufPrint(key[namespace.len..][0..8], "{d:0>8}", .{i});
        @memset(key[namespace.len + 8 ..], 0);
        if (i < docs.len) docs[i] = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var request = try reader.PreparedRequest.init(a, &.{""}, &nodes);
    defer request.deinit();
    var columns: [count]?f64 = undefined;
    var txn = try Txn.openReadWithPrefix(&store, namespace);
    defer txn.abort();
    for (0..3) |_| _ = try reader.populatePrepared(alloc, &txn, &request, &.{&columns});
    const before = counter.alloc_attempts;
    for (0..8) |_| _ = try reader.populatePrepared(alloc, &txn, &request, &.{&columns});
    try std.testing.expectEqual(before, counter.alloc_attempts);
    for (columns[0 .. count - 1]) |score| try std.testing.expectEqual(@as(?f64, 7), score);
    try std.testing.expectEqual(@as(?f64, null), columns[count - 1]);
    const cache = &store.workspaces[0].?.resolved_scores;
    try std.testing.expect(cache.compact and cache.decoded_ready);
    try std.testing.expectEqual(@as(usize, count), cache.decoded.len);
    try std.testing.expectEqual(@as(usize, 0), cache.keys.len);
    try std.testing.expectEqual(@as(usize, 0), cache.references.len);
    try std.testing.expect(cache.order == null);
    try std.testing.expect(cache.storage.?.len <= 32 * 1024);
    // Receivers without the bulk callback still get exactly one delivery/key.
    const Output = struct {
        counts: [count]u8 = @splat(0),
        fn write(ctx: *anyopaque, i: usize, score: ?f64) void {
            const out: *@This() = @ptrCast(@alignCast(ctx));
            out.counts[i] += 1;
            std.debug.assert(score == (if (i == count - 1) @as(?f64, null) else 7));
        }
    };
    var output = Output{};
    var scope = try txn.openReadScope(alloc);
    defer scope.close();
    try scope.visitPreparedRequest(&request, .{ .context = &output, .write = Output.write });
    for (output.counts) |deliveries| try std.testing.expectEqual(@as(u8, 1), deliveries);
}

test "lite prepared production metric sessions reuse owned inputs and recover optional denial" {
    const a = std.testing.allocator;
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-owned-session.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocument("p/a\x00\x00", &score);
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument("p/b\x00\x00", &score);
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
    statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
    const prefixes = try alloc.alloc(?[]const u8, 1);
    prefixes[0] = try alloc.dupe(u8, "p/");
    var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openRead(&store)), .statuses = statuses, .prefixes = prefixes };
    defer session.deinit();
    if (comptime @hasDecl(Graph.GraphMetricReadSession, "enablePreparedReuse")) session.enablePreparedReuse();
    var node = [_]u8{'a'};
    var column: [1]?f64 = undefined;
    try session.readColumns(alloc, &.{"metric"}, &.{&node}, &.{&column});
    try std.testing.expect(session.prepared_request == null);
    // Optional request construction is denied; the completed read stays valid.
    counter.limit = counter.live;
    try session.readColumns(alloc, &.{"metric"}, &.{&node}, &.{&column});
    try std.testing.expectEqual(@as(?f64, 3), column[0]);
    try std.testing.expect(session.prepared_request == null);
    counter.limit = std.math.maxInt(usize);
    for (0..5) |_| try session.readColumns(alloc, &.{"metric"}, &.{&node}, &.{&column});
    try std.testing.expect(session.prepared_request != null);
    const before = counter.alloc_attempts;
    for (0..8) |_| try session.readColumns(alloc, &.{"metric"}, &.{&node}, &.{&column});
    try std.testing.expectEqual(before, counter.alloc_attempts);
    // Mutating caller inputs selects the new request; ownership is independent.
    node[0] = 'b';
    try session.readColumns(alloc, &.{"metric"}, &.{&node}, &.{&column});
    try std.testing.expectEqual(@as(?f64, 9), column[0]);
    try std.testing.expectError(error.InvalidQueryRequest, session.readColumns(alloc, &.{"other"}, &.{&node}, &.{&column}));
}

test "lite prepared compact staging retries without duplicate delivery after page cache eviction" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-retention.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    const limits = resource_manager_mod.Options.defaultBudgets();
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = limits });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][10]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &keys, &docs, 0..) |*buf, *key, *doc, i| {
        _ = try std.fmt.bufPrint(buf[0..8], "{d:0>8}", .{i});
        buf[8] = 0;
        buf[9] = 0;
        key.* = buf;
        doc.* = .{ .key = buf, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    var nodes: [512][]const u8 = undefined;
    for (keys, &nodes) |key, *node| node.* = key[0..8];
    var request = try @import("../../graph/score_read.zig").PreparedRequest.init(a, &.{""}, &nodes);
    defer request.deinit();
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var buffer: [8]u8 = undefined;
    for (keys) |key| _ = try txn.getInto(key, &buffer);
    var scope = try txn.openReadScope(query.allocator());
    defer scope.close();
    var scores: [512]?f64 = undefined;
    var initial_output = @import("../prepared_score_plan.zig").PreparedScorePlan.Output{ .plan = request.plan(), .columns = &.{&scores} };
    try scope.visitPreparedRequest(&request, initial_output.completeSink());
    const w = scope.workspace.?;
    try std.testing.expect(w.resolved_scores.storage != null);
    try std.testing.expect(w.native_state.window.bytes == null);
    // Exercise the supported offset-only cache path, independently of the
    // decoded hit that intentionally requires no reader growth.
    w.resolved_scores.decoded_ready = false;
    const file = try txn.readFile();
    try std.testing.expect(file.page_cache.total_bytes > 0);

    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = w.live + 1024;
    const Output = struct {
        manager: *resource_manager_mod.ResourceManager,
        counts: [512]u8 = @splat(0),
        scores: *[512]?f64,
        first: bool = true,
        fn write(ctx: *anyopaque, index: usize, score: ?f64) void {
            const out: *@This() = @ptrCast(@alignCast(ctx));
            out.counts[index] += 1;
            out.scores[index] = score;
            if (out.first) {
                out.first = false;
                out.manager.slices[@backingInt(resource_manager_mod.Slice.lite_native_page_cache)].budget.hard_limit_bytes = 1;
            }
        }
    };
    var output = Output{ .manager = &manager, .scores = &scores };
    const result = scope.visitPreparedRequest(&request, .{ .context = &output, .write = Output.write });
    try result;
    for (output.counts) |count| try std.testing.expectEqual(@as(u8, 1), count);
    try std.testing.expect(!w.allocation_failed);
    try std.testing.expect(!scope.discard_workspace);
    for (scores) |score| try std.testing.expectEqual(@as(?f64, 3), score);
    // Restore normal admission before measuring warm reuse. The tight limit
    // above only guarantees the required record read, not cache residency.
    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = 0;
    for (0..3) |_| try scope.visitPreparedRequest(&request, initial_output.completeSink());
    const before = counter.alloc_calls;
    try scope.visitPreparedRequest(&request, initial_output.completeSink());
    try std.testing.expectEqual(before, counter.alloc_calls);
}

test "lite prepared production metric session measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-measure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
    statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
    const prefixes = try alloc.alloc(?[]const u8, 1);
    prefixes[0] = try alloc.dupe(u8, "");
    var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openReadWithPrefix(&store, "ns\x00")), .statuses = statuses, .prefixes = prefixes };
    defer session.deinit();
    if (comptime @hasDecl(Graph.GraphMetricReadSession, "enablePreparedReuse")) session.enablePreparedReuse();
    var column: [512]?f64 = undefined;
    for (0..10) |_| try session.readColumns(alloc, &.{"metric"}, &nodes, &.{&column});
    const before = counter.alloc_calls;
    const started = std.posix.getrusage(0);
    for (0..5000) |_| {
        try session.readColumns(alloc, &.{"metric"}, &nodes, &.{&column});
        for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    }
    const ended = std.posix.getrusage(0);
    const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
    std.debug.print("LITE_PRODUCTION cpu_us={d} allocations={d} cache_bytes={d}\n", .{ cpu, counter.alloc_calls - before, store.workspaces[0].?.resolved_scores.storage.?.len });
}

test "lite prepared fresh one shot sessions never promote shared plans" {
    const a = std.testing.allocator;
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-owned-session.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocument("p/a\x00\x00", &score);
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument("p/b\x00\x00", &score);
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var node = [_]u8{'a'};
    var column: [1]?f64 = undefined;
    for (0..2) |iteration| {
        const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
        statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
        const prefixes = try alloc.alloc(?[]const u8, 1);
        prefixes[0] = try alloc.dupe(u8, "p/");
        var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openRead(&store)), .statuses = statuses, .prefixes = prefixes };
        defer session.deinit();
        const before = counter.alloc_calls;
        try session.readColumns(alloc, &.{"metric"}, &.{&node}, &.{&column});
        std.debug.print("ONE_SHOT iteration={d} calls={d} owned={any} bytes={d}\n", .{ iteration, counter.alloc_calls - before, session.prepared_request != null, if (session.prepared_request) |r| r.plan().storage.?.len else @as(usize, 0) });
        try std.testing.expectEqual(@as(?f64, 3), column[0]);
        try std.testing.expect(session.prepared_request == null);
    }
}

test "lite prepared one shot session measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-measure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var column: [512]?f64 = undefined;
    for (0..3) |iteration| {
        const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
        statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
        const prefixes = try alloc.alloc(?[]const u8, 1);
        prefixes[0] = try alloc.dupe(u8, "");
        var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openReadWithPrefix(&store, "ns\x00")), .statuses = statuses, .prefixes = prefixes };
        defer session.deinit();
        const before = counter.alloc_calls;
        try session.readColumns(alloc, &.{"metric"}, &nodes, &.{&column});
        std.debug.print("ONE_SHOT_512 iteration={d} allocations={d} owned={any} plan_bytes={d}\n", .{ iteration, counter.alloc_calls - before, session.prepared_request != null, if (session.prepared_request) |r| r.plan().storage.?.len else @as(usize, 0) });
        for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    }
}

test "lite prepared namespace cold measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-measure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const checkpoint = try store.file.materializeTransactionCheckpoint();
    var request = try @import("../../graph/score_read.zig").PreparedRequest.init(a, &.{""}, &nodes);
    defer request.deinit();
    var state = native.DocumentBatchReadState{ .allocator = alloc };
    defer state.deinit();
    var column: [512]?f64 = undefined;
    var output = @import("../prepared_score_plan.zig").PreparedScorePlan.Output{ .plan = request.plan(), .columns = &.{&column} };
    for (0..5) |_| try store.file.visitPreparedScoresAtCheckpointWithState(alloc, checkpoint, request.plan(), "ns\x00", output.completeSink(), &state);
    const before = counter.alloc_calls;
    const live = counter.live;
    counter.peak = live;
    const started = std.posix.getrusage(0);
    for (0..500) |_| {
        try store.file.visitPreparedScoresAtCheckpointWithState(alloc, checkpoint, request.plan(), "ns\x00", output.completeSink(), &state);
        for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    }
    const ended = std.posix.getrusage(0);
    const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
    std.debug.print("LITE_PREFIX_COLD cpu_us={d} allocations={d} scratch_bytes={d}\n", .{ cpu, counter.alloc_calls - before, counter.peak - live });
}

test "lite prepared virtual namespaces preserve overflow keys and pinned snapshots" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "virtual-overflow-prefix.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var namespace: [768]u8 = @splat('x');
    namespace[100] = 0;
    namespace[600] = 0xff;
    namespace[767] = 0;
    var key_storage: [128][778]u8 = undefined;
    var nodes: [128][]const u8 = undefined;
    var docs: [96]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, 0..) |*key, *node, i| {
        @memcpy(key[0..namespace.len], &namespace);
        node.* = try std.fmt.bufPrint(key[namespace.len..][0..8], "{d:0>8}", .{i});
        key[776] = 0;
        key[777] = 0;
        if (i < docs.len) docs[i] = .{ .key = key, .value = &raw, .is_delete = i == 3 };
    }
    try store.file.putDocumentBatch(&docs);
    // Neighboring namespaces must not alias this request, even at overflow
    // separator boundaries. One indexed key is shorter than the namespace.
    try store.file.putDocument(namespace[0..100], &raw);
    var neighbor = key_storage[0];
    neighbor[766] += 1;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument(&neighbor, &raw);
    var request = try @import("../../graph/score_read.zig").PreparedRequest.init(a, &.{""}, &nodes);
    defer request.deinit();
    var old = try Txn.openReadWithPrefix(&store, &namespace);
    defer old.abort();
    var column: [128]?f64 = undefined;
    for (0..4) |_| _ = try @import("../../graph/score_read.zig").populatePrepared(a, &old, &request, &.{&column});
    for (column, 0..) |value, i| try std.testing.expectEqual(if (i >= docs.len or i == 3) @as(?f64, null) else @as(?f64, 3), value);
    try store.file.putDocument(&key_storage[0], &raw);
    var current = try Txn.openReadWithPrefix(&store, &namespace);
    defer current.abort();
    _ = try @import("../../graph/score_read.zig").populatePrepared(a, &current, &request, &.{&column});
    try std.testing.expectEqual(@as(?f64, 9), column[0]);
    _ = try @import("../../graph/score_read.zig").populatePrepared(a, &old, &request, &.{&column});
    try std.testing.expectEqual(@as(?f64, 3), column[0]);
}

test "lite prepared first capture and explicit promotion keep subsequent sessions warm" {
    const a = std.testing.allocator;
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-owned-session.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var score: [8]u8 = undefined;
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocument("p/a\x00\x00", &score);
    std.mem.writeInt(u64, &score, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument("p/b\x00\x00", &score);
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var column: [1]?f64 = undefined;
    const file = &store.file;
    for ([_]bool{ false, true, false }) |enabled| {
        const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
        statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
        const prefixes = try alloc.alloc(?[]const u8, 1);
        prefixes[0] = try alloc.dupe(u8, "p/");
        var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openRead(&store)), .statuses = statuses, .prefixes = prefixes };
        defer session.deinit();
        if (enabled) session.enablePreparedReuse();
        try session.readColumns(alloc, &.{"metric"}, &.{"a"}, &.{&column});
        const w = store.workspaces[0].?;
        try std.testing.expect(w.resolved_scores.decoded_ready);
        const identity = w.score_plan.identity;
        const before = file.test_index_comparisons.load(.monotonic);
        for (0..4) |_| try session.readColumns(alloc, &.{"metric"}, &.{"a"}, &.{&column});
        try std.testing.expectEqual(before, file.test_index_comparisons.load(.monotonic));
        try std.testing.expectEqual(enabled, session.prepared_request != null);
        try std.testing.expectEqual(identity, w.score_plan.identity);
        if (session.prepared_request) |request| try std.testing.expectEqual(identity, request.plan().identity);
        try std.testing.expectEqual(@as(?f64, 3), column[0]);
        var owned_node = [_]u8{'a'};
        var prepared = try session.prepareColumns(alloc, &.{"metric"}, &.{&owned_node});
        defer prepared.deinit();
        owned_node[0] = 'b';
        for (0..3) |_| try session.readPreparedColumns(alloc, &prepared, &.{&column});
        try std.testing.expectEqual(@as(?f64, 3), column[0]);
        const handle_identity = prepared.session_identity;
        prepared.session_identity = 0;
        try std.testing.expectError(error.InvalidQueryRequest, session.readPreparedColumns(alloc, &prepared, &.{&column}));
        prepared.session_identity = handle_identity;
    }
}

test "lite prepared short sessions promotion measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "production-measure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var key_storage: [512][13]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [512]native.DocumentMutation = undefined;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (&key_storage, &nodes, &docs, 0..) |*key, *node, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(&docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var column: [512]?f64 = undefined;
    for ([_]usize{ 2, 3, 5, 8, 32 }) |reads| {
        for ([_]u8{ 0, 1, 2 }) |mode| {
            var warm = try Txn.openReadWithPrefix(&store, "ns\x00");
            for (0..3) |_| _ = try @import("../../graph/score_read.zig").populate(alloc, &warm, &.{""}, &nodes, &.{&column});
            warm.abort();
            const before = counter.alloc_calls;
            const started = std.posix.getrusage(0);
            for (0..200) |_| {
                const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
                statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
                const prefixes = try alloc.alloc(?[]const u8, 1);
                prefixes[0] = try alloc.dupe(u8, "");
                var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openReadWithPrefix(&store, "ns\x00")), .statuses = statuses, .prefixes = prefixes };
                defer session.deinit();
                if (mode == 2) {
                    if (comptime @hasDecl(Graph.GraphMetricReadSession, "enablePreparedReuse")) session.enablePreparedReuse();
                }

                for (0..reads) |_| {
                    if (mode == 0) session.last_plan_identity = 0;
                    try session.readColumns(alloc, &.{"metric"}, &nodes, &.{&column});
                }
                for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
            }
            const ended = std.posix.getrusage(0);
            const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
            std.debug.print("SHORT_SESSIONS reads={d} mode={d} cpu_us={d} allocations={d}\n", .{ reads, mode, cpu, counter.alloc_calls - before });
        }
    }
}

test "lite prepared chunks preserve large namespaced snapshot and global duplicate fanout" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "large-chunks.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const count = 5000;
    const storage = try a.alloc([13]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count + 2);
    defer a.free(nodes);
    const docs = try a.alloc(native.DocumentMutation, count);
    defer a.free(docs);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (storage, docs, 0..) |*key, *doc, i| {
        @memcpy(key[0..3], "ns\x00");
        _ = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
        key[11] = 0;
        key[12] = 0;
        nodes[count - 1 - i] = key[3..11];
        doc.* = .{ .key = key, .value = &raw };
    }
    nodes[count] = nodes[0];
    nodes[count + 1] = "missing";
    try store.file.putDocumentBatch(docs);
    const reader = @import("../../graph/score_read.zig");
    var txn = try Txn.openReadWithPrefix(&store, "ns\x00");
    defer txn.abort();
    var compiled = try reader.PreparedChunks.init(a, &.{ "", null, "" }, nodes);
    defer compiled.deinit();
    try std.testing.expect(compiled.chunkPlans().len > 1);
    try std.testing.expectEqual(@as(usize, count + 1), compiled.uniqueRowCount());
    const first = try a.alloc(?f64, nodes.len);
    defer a.free(first);
    const missing = try a.alloc(?f64, nodes.len);
    defer a.free(missing);
    const duplicate = try a.alloc(?f64, nodes.len);
    defer a.free(duplicate);
    const stats = try reader.populateChunks(a, &txn, &compiled, &.{ first, missing, duplicate });
    try std.testing.expectEqual(@as(usize, count + 1), stats.keys);
    for (first[0 .. count + 1]) |score| try std.testing.expectEqual(@as(?f64, 3), score);
    try std.testing.expectEqual(@as(?f64, null), first[count + 1]);
    for (missing) |score| try std.testing.expectEqual(@as(?f64, null), score);
    try std.testing.expectEqualSlices(?f64, first, duplicate);
    // Commit a different score after compilation. The old txn and handle keep
    // their snapshot, while the same immutable key plan can serve a fresh txn.
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument(storage[count - 1][0..], &raw);
    _ = try reader.populateChunks(a, &txn, &compiled, &.{ first, missing, duplicate });
    try std.testing.expectEqual(@as(?f64, 3), first[0]);
    var fresh = try Txn.openReadWithPrefix(&store, "ns\x00");
    defer fresh.abort();
    _ = try reader.populateChunks(a, &fresh, &compiled, &.{ first, missing, duplicate });
    try std.testing.expectEqual(@as(?f64, 9), first[0]);
    try std.testing.expectEqual(@as(?f64, 9), first[count]);
    var erased = try backend_erased.readTxnFrom(a, try Txn.openReadWithPrefix(&store, "ns\x00"));
    defer erased.abort();
    _ = try reader.populateChunks(a, &erased, &compiled, &.{ first, missing, duplicate });
    try std.testing.expectEqual(@as(?f64, 9), first[0]);
    const file = try fresh.readFile();
    file.page_cache_enabled.store(false, .monotonic);
    defer file.page_cache_enabled.store(true, .monotonic);
    _ = try reader.populateChunks(a, &fresh, &compiled, &.{ first, missing, duplicate });
    try std.testing.expectEqual(@as(?f64, 9), first[0]);
    var write = try Txn.openWriteWithPrefix(&store, "ns\x00");
    defer write.abort();
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 11)), .little);
    try write.put(storage[count - 1][3..], &raw);
    _ = try reader.populateChunks(a, &write, &compiled, &.{ first, missing, duplicate });
    try std.testing.expectEqual(@as(?f64, 11), first[0]);
    try std.testing.expectEqual(@as(?f64, 11), first[count]);
    try write.delete(storage[count - 1][3..]);
    _ = try reader.populateChunks(a, &write, &compiled, &.{ first, missing, duplicate });
    try std.testing.expect(first[0] == null and first[count] == null);
    try write.put(storage[count - 1][3..], "invalid");
    try std.testing.expectError(error.InvalidGraphMetricScore, reader.populateChunks(a, &write, &compiled, &.{ first, missing, duplicate }));
    for (first) |value| try std.testing.expect(value == null);
}

test "lite prepared immutable handle and large chunks measurement" {
    const a = std.testing.allocator;
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    for ([_]usize{ 512, 8192, 50000 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(a, tmp, "handle-measure.aflite");
        defer a.free(path);
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        const alloc = counter.allocator();
        var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
        defer store.close();
        const key_storage = try a.alloc([13]u8, count);
        defer a.free(key_storage);
        const nodes = try a.alloc([]const u8, count);
        defer a.free(nodes);
        const docs = try a.alloc(native.DocumentMutation, count);
        defer a.free(docs);
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
        for (key_storage, nodes, docs, 0..) |*key, *node, *doc, i| {
            @memcpy(key[0..3], "ns\x00");
            node.* = try std.fmt.bufPrint(key[3..11], "{d:0>8}", .{i});
            key[11] = 0;
            key[12] = 0;
            doc.* = .{ .key = key, .value = &raw };
        }
        try store.file.putDocumentBatch(docs);
        var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
        defer graph.close();
        const column = try a.alloc(?f64, count);
        defer a.free(column);
        for ([_]bool{ false, true }) |direct| {
            const statuses = try alloc.alloc(Graph.GraphMetricStatus, 1);
            statuses[0] = .{ .name = try alloc.dupe(u8, "metric"), .published_generation = 1 };
            const prefixes = try alloc.alloc(?[]const u8, 1);
            prefixes[0] = try alloc.dupe(u8, "");
            var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openReadWithPrefix(&store, "ns\x00")), .statuses = statuses, .prefixes = prefixes };
            defer session.deinit();
            const has_handle = comptime @hasDecl(Graph.GraphMetricReadSession, "prepareColumns");
            const setup_before = counter.alloc_calls;
            const live_before = counter.live;
            counter.peak = counter.live;
            const build_started = std.posix.getrusage(0);
            var prepared = if (has_handle) (if (direct) try session.prepareColumns(alloc, &.{"metric"}, nodes) else null) else {};
            defer if (has_handle) if (prepared) |*handle| handle.deinit();
            const build_ended = std.posix.getrusage(0);
            const build_cpu = (build_ended.utime.sec - build_started.utime.sec + build_ended.stime.sec - build_started.stime.sec) * 1000000 + build_ended.utime.usec - build_started.utime.usec + build_ended.stime.usec - build_started.stime.usec;
            const owned_bytes = counter.live - live_before;
            const preparation_peak = counter.peak - live_before;
            const setup_allocations = counter.alloc_calls - setup_before;
            for (0..3) |_| {
                if (has_handle and direct) try session.readPreparedColumns(alloc, &prepared.?, &.{column}) else try session.readColumns(alloc, &.{"metric"}, nodes, &.{column});
            }
            const queries: usize = if (count == 512) 5000 else 20;
            const before = counter.alloc_calls;
            const started = std.posix.getrusage(0);
            for (0..queries) |_| {
                if (has_handle and direct) try session.readPreparedColumns(alloc, &prepared.?, &.{column}) else try session.readColumns(alloc, &.{"metric"}, nodes, &.{column});
            }
            const ended = std.posix.getrusage(0);
            const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
            for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
            std.debug.print("HANDLE_CHUNKS keys={d} direct={d} queries={d} cpu_us={d} allocations={d} build_cpu_us={d} setup_allocations={d} owned_bytes={d} preparation_peak={d}\n", .{ count, @intFromBool(direct), queries, cpu, counter.alloc_calls - before, build_cpu, setup_allocations, owned_bytes, preparation_peak });
        }
    }
}

test "lite prepared review multiple columns retain combined decoded reuse" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "review-multi-columns.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var keys: [2][512][12]u8 = undefined;
    var nodes: [512][]const u8 = undefined;
    var docs: [1024]native.DocumentMutation = undefined;
    var raw: [2][8]u8 = undefined;
    for (0..2) |c| {
        std.mem.writeInt(u64, &raw[c], @bitCast(@as(f64, if (c == 0) 3 else 9)), .little);
        for (&keys[c], 0..) |*key, i| {
            @memcpy(key[0..2], if (c == 0) "p/" else "q/");
            _ = try std.fmt.bufPrint(key[2..10], "{d:0>8}", .{i});
            key[10] = 0;
            key[11] = 0;
            if (c == 0) nodes[i] = key[2..10];
            docs[c * 512 + i] = .{ .key = key, .value = &raw[c] };
        }
    }
    try store.file.putDocumentBatch(&docs);
    const reader = @import("../../graph/score_read.zig");
    const prefixes = [_]?[]const u8{ "p/", "q/" };
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var compiled = try reader.PreparedChunks.init(alloc, &prefixes, &nodes);
    defer compiled.deinit();
    try std.testing.expect(compiled.combinedRequest() != null);
    try std.testing.expect(compiled.combinedRequest().?.retainDecoded());
    var owned = try reader.PreparedRequest.init(alloc, &prefixes, &nodes);
    defer owned.deinit();
    var columns: [2][512]?f64 = undefined;
    const output = [_][]?f64{ &columns[0], &columns[1] };
    for (0..3) |mode| {
        for (0..3) |_| switch (mode) {
            0 => _ = try reader.populate(alloc, &txn, &prefixes, &nodes, &output),
            1 => _ = try reader.populateChunks(alloc, &txn, &compiled, &output),
            2 => _ = try reader.populatePrepared(alloc, &txn, &owned, &output),
            else => unreachable,
        };
        if (mode == 1) {
            var captured = false;
            for (store.workspaces) |slot| if (slot) |w| {
                if (w.resolved_scores.decoded_ready) captured = true;
            };
            try std.testing.expect(captured);
        }
        const before = counter.alloc_calls;
        const started = std.posix.getrusage(0);
        for (0..2000) |_| switch (mode) {
            0 => _ = try reader.populate(alloc, &txn, &prefixes, &nodes, &output),
            1 => _ = try reader.populateChunks(alloc, &txn, &compiled, &output),
            2 => _ = try reader.populatePrepared(alloc, &txn, &owned, &output),
            else => unreachable,
        };
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        for (columns[0]) |score| try std.testing.expectEqual(@as(?f64, 3), score);
        for (columns[1]) |score| try std.testing.expectEqual(@as(?f64, 9), score);
        try std.testing.expectEqual(before, counter.alloc_calls);
        std.debug.print("REVIEW_MULTI mode={d} cpu_us={d} allocations={d}\n", .{ mode, cpu, counter.alloc_calls - before });
    }
}

test "lite prepared duplicate heavy single compact chunk retains decoded scores" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "single-compact.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocument("p/a\x00\x00", &raw);
    var nodes: [5000][]const u8 = @splat("a");
    nodes[0] = "missing";
    const reader = @import("../../graph/score_read.zig");
    var compiled = try reader.PreparedChunks.init(a, &.{ "p/", null, "p/" }, &nodes);
    defer compiled.deinit();
    try std.testing.expect(compiled.combinedRequest() == null);
    try std.testing.expectEqual(@as(usize, 1), compiled.chunkPlans().len);
    try std.testing.expect(compiled.chunkPlans()[0].request.retainDecoded());
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var columns: [3][5000]?f64 = undefined;
    for (0..3) |_| _ = try reader.populateChunks(alloc, &txn, &compiled, &.{ &columns[0], &columns[1], &columns[2] });
    const before = counter.alloc_attempts;
    _ = try reader.populateChunks(alloc, &txn, &compiled, &.{ &columns[0], &columns[1], &columns[2] });
    try std.testing.expectEqual(before, counter.alloc_attempts);
    try std.testing.expect(columns[0][0] == null);
    for (columns[0][1..]) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    for (columns[1]) |value| try std.testing.expect(value == null);
    try std.testing.expectEqualSlices(?f64, &columns[0], &columns[2]);
    var captured = false;
    for (store.workspaces) |slot| if (slot) |w| {
        if (w.resolved_scores.decoded_ready) captured = true;
    };
    try std.testing.expect(captured);
}

fn testResultSession(a: Allocator, graph: *@import("../../graph/graph.zig").GraphIndex, store: *Store) !@import("../../graph/graph.zig").GraphIndex.GraphMetricReadSession {
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    const statuses = try a.alloc(Graph.GraphMetricStatus, 1);
    errdefer a.free(statuses);
    statuses[0] = .{ .name = try a.dupe(u8, "metric"), .published_generation = 1 };
    errdefer statuses[0].deinit(a);
    const prefixes = try a.alloc(?[]const u8, 1);
    errdefer a.free(prefixes);
    prefixes[0] = try a.dupe(u8, "");
    errdefer a.free(prefixes[0].?);
    var txn = try Txn.openRead(store);
    errdefer txn.abort();
    return .{ .alloc = a, .graph_index = graph, .txn = try backend_erased.readTxnFrom(a, txn), .statuses = statuses, .prefixes = prefixes };
}

fn resultAllocationFailureCase(a: Allocator, request: *const @import("../../graph/score_read.zig").PreparedChunks, txn: *const backend_erased.ReadTxn) !void {
    var results = try @import("../../graph/score_read.zig").PreparedResults.init(a, request, txn);
    defer results.deinit();
}

test "lite prepared query results fence snapshots handles bypass and malformed capture" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "query-results.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocument("a\x00\x00", &raw);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var pinned = try Txn.openRead(&store);
    defer pinned.abort();
    const pinned_file = try pinned.readFile();
    var session = try testResultSession(alloc, &graph, &store);
    defer session.deinit();
    var prepared = try session.prepareColumns(alloc, &.{"metric"}, &.{ "a", "missing", "a" });
    defer prepared.deinit();
    try std.testing.expect(prepared.results != null);
    var column: [3]?f64 = undefined;
    try session.readPreparedColumns(alloc, &prepared, &.{&column});
    try std.testing.expectEqualSlices(?f64, &.{ 3, null, 3 }, &column);
    try std.testing.expect(prepared.results.?.handle.ready);
    const before = counter.alloc_attempts;
    try session.readPreparedColumns(alloc, &prepared, &.{&column});
    try std.testing.expectEqual(before, counter.alloc_attempts);
    const live_before = counter.live;
    var admission_probe = try session.prepareColumns(alloc, &.{"metric"}, &.{ "a", "missing", "a" });
    const keys_owned = counter.live - live_before - admission_probe.results.?.handle.values.len * @sizeOf(f64) - @sizeOf(@TypeOf(admission_probe.results.?.handle.*));
    admission_probe.deinit();
    counter.limit = counter.live + keys_owned;
    var denied = try session.prepareColumns(alloc, &.{"metric"}, &.{ "a", "missing", "a" });
    defer denied.deinit();
    counter.limit = std.math.maxInt(usize);
    try std.testing.expect(denied.results == null);
    try session.readPreparedColumns(alloc, &denied, &.{&column});
    try std.testing.expectEqualSlices(?f64, &.{ 3, null, 3 }, &column);
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument("a\x00\x00", &raw);
    try session.readPreparedColumns(alloc, &prepared, &.{&column});
    try std.testing.expectEqual(@as(?f64, 3), column[0]);
    var fresh = try testResultSession(alloc, &graph, &store);
    defer fresh.deinit();
    try std.testing.expectError(error.InvalidQueryRequest, fresh.readPreparedColumns(alloc, &prepared, &.{&column}));
    try std.testing.expectError(error.InvalidBatch, prepared.results.?.populate(alloc, &fresh.txn, &prepared.request, &.{&column}));
    var other = try session.prepareColumns(alloc, &.{"metric"}, &.{ "missing", "a", "a" });
    defer other.deinit();
    try std.testing.expectError(error.InvalidBatch, prepared.results.?.populate(alloc, &session.txn, &other.request, &.{&column}));
    try std.testing.checkAllAllocationFailures(a, resultAllocationFailureCase, .{ &prepared.request, &session.txn });
    var bad_shape: [2]?f64 = undefined;
    try std.testing.expectError(error.InvalidBatch, session.readPreparedColumns(alloc, &prepared, &.{&bad_shape}));
    try std.testing.expect(prepared.results.?.handle.ready);
    // Bypass must invalidate cached delivery and force authenticated reads.
    // Find the same pinned generation by opening a read before the update.
    const generation_file = pinned_file;
    generation_file.page_cache_bypass.store(1, .monotonic);
    try session.readPreparedColumns(alloc, &prepared, &.{&column});
    try std.testing.expect(!prepared.results.?.handle.ready);
    generation_file.page_cache_bypass.store(0, .monotonic);
    try session.readPreparedColumns(alloc, &prepared, &.{&column});
    try std.testing.expect(prepared.results.?.handle.ready);
    try store.file.putDocument("bad\x00\x00", "invalid");
    var invalid_session = try testResultSession(alloc, &graph, &store);
    defer invalid_session.deinit();
    var invalid = try invalid_session.prepareColumns(alloc, &.{"metric"}, &.{ "a", "bad", "a" });
    defer invalid.deinit();
    @memset(&column, 77);
    try std.testing.expectError(error.InvalidGraphMetricScore, invalid_session.readPreparedColumns(alloc, &invalid, &.{&column}));
    for (column) |value| try std.testing.expect(value == null);
    try std.testing.expect(invalid.results.?.handle.disabled);
    try std.testing.expectEqual(@as(usize, 0), invalid.results.?.handle.values.len);
    var write = try backend_erased.readTxnFrom(alloc, try Txn.openWrite(&store));
    defer write.abort();
    try std.testing.expect(!write.canReuseDecodedResults());
    try std.testing.expectError(error.Unsupported, @import("../../graph/score_read.zig").PreparedResults.init(a, &prepared.request, &write));
}

test "lite prepared query results release optional ownership before pressure fallback" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "query-results-pressure.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const count = 50000;
    const storage = try a.alloc([10]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count);
    defer a.free(nodes);
    const docs = try a.alloc(native.DocumentMutation, count);
    defer a.free(docs);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (storage, nodes, docs, 0..) |*key, *node, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        node.* = key[0..8];
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var session = try testResultSession(alloc, &graph, &store);
    defer session.deinit();
    var prepared = try session.prepareColumns(alloc, &.{"metric"}, nodes);
    defer prepared.deinit();
    const column = try a.alloc(?f64, count);
    defer a.free(column);
    counter.limit = counter.live;
    defer counter.limit = std.math.maxInt(usize);
    try session.readPreparedColumns(alloc, &prepared, &.{column});
    for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    try std.testing.expect(prepared.results.?.handle.disabled);
    try std.testing.expectEqual(@as(usize, 0), prepared.results.?.handle.values.len);
    try session.readPreparedColumns(alloc, &prepared, &.{column});
}

test "lite prepared fresh review duplicate multi column cache eligibility" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "fresh-duplicate-multi.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var keys: [2][512][12]u8 = undefined;
    var unique: [512][]const u8 = undefined;
    var nodes: [5000][]const u8 = undefined;
    var docs: [1024]native.DocumentMutation = undefined;
    var raw: [2][8]u8 = undefined;
    for (0..2) |c| {
        std.mem.writeInt(u64, &raw[c], @bitCast(@as(f64, if (c == 0) 3 else 9)), .little);
        for (&keys[c], 0..) |*key, i| {
            @memcpy(key[0..2], if (c == 0) "p/" else "q/");
            _ = try std.fmt.bufPrint(key[2..10], "{d:0>8}", .{i});
            key[10] = 0;
            key[11] = 0;
            if (c == 0) unique[i] = key[2..10];
            docs[c * 512 + i] = .{ .key = key, .value = &raw[c] };
        }
    }
    for (&nodes, 0..) |*node, i| node.* = unique[i % unique.len];
    try store.file.putDocumentBatch(&docs);
    const reader = @import("../../graph/score_read.zig");
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var compiled = try reader.PreparedChunks.init(alloc, &.{ "p/", "q/" }, &nodes);
    defer compiled.deinit();
    var combined = try reader.PreparedRequest.init(alloc, &.{ "p/", "q/" }, &unique);
    defer combined.deinit();
    var columns: [2][5000]?f64 = undefined;
    const Fanout = struct {
        request: *const reader.PreparedChunks,
        columns: *[2][5000]?f64,
        fn write(ctx: *anyopaque, i: usize, value: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const single_combined = self.request.chunkPlans().len == 1;
            var output = reader.PreparedChunks.Output{ .owner = self.request, .chunk = &self.request.chunkPlans()[if (single_combined) 0 else i / 512], .columns = &.{ &self.columns[0], &self.columns[1] } };
            const sink = output.sink();
            sink.write(sink.context, if (single_combined) i else i % 512, value);
        }
    };
    var fanout = Fanout{ .request = &compiled, .columns = &columns };
    for (0..2) |mode| {
        for (0..3) |_| {
            if (mode == 0) _ = try reader.populateChunks(alloc, &txn, &compiled, &.{ &columns[0], &columns[1] }) else _ = try reader.visitPrepared(alloc, &txn, &combined, .{ .context = &fanout, .write = Fanout.write });
        }
        const before = counter.alloc_calls;
        const started = std.posix.getrusage(0);
        for (0..2000) |_| {
            if (mode == 0) _ = try reader.populateChunks(alloc, &txn, &compiled, &.{ &columns[0], &columns[1] }) else _ = try reader.visitPrepared(alloc, &txn, &combined, .{ .context = &fanout, .write = Fanout.write });
        }
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        for (columns[0]) |score| try std.testing.expectEqual(@as(?f64, 3), score);
        for (columns[1]) |score| try std.testing.expectEqual(@as(?f64, 9), score);
        std.debug.print("REVIEW_DUP_MULTI mode={d} cpu_us={d} allocations={d}\n", .{ mode, cpu, counter.alloc_calls - before });
    }
}

test "lite prepared fresh review query owned decoded result opportunity" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "fresh-owned-results.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const count = 50000;
    const storage = try a.alloc([10]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count);
    defer a.free(nodes);
    const docs = try a.alloc(native.DocumentMutation, count);
    defer a.free(docs);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (storage, nodes, docs, 0..) |*key, *node, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        node.* = key[0..8];
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(docs);
    const reader = @import("../../graph/score_read.zig");
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var compiled = try reader.PreparedChunks.init(a, &.{""}, nodes);
    defer compiled.deinit();
    const column = try a.alloc(?f64, count);
    defer a.free(column);
    const scores = try a.alloc(f64, count);
    defer a.free(scores);
    const Capture = struct {
        values: []f64,
        fn write(ctx: *anyopaque, i: usize, value: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.values[i] = value orelse std.math.nan(f64);
        }
    };
    // Capture through the real read path at this pinned, read-only checkpoint.
    // This fixture is a design probe, not a production cache implementation.
    for (compiled.chunkPlans()) |*chunk| {
        var capture = Capture{ .values = scores[chunk.first..][0..chunk.request.readKeys().len] };
        _ = try reader.visitPrepared(a, &txn, &chunk.request, .{ .context = &capture, .write = Capture.write });
    }
    for (0..2) |mode| {
        for (0..3) |_| _ = try reader.populateChunks(a, &txn, &compiled, &.{column});
        const started = std.posix.getrusage(0);
        for (0..100) |_| {
            if (mode == 0) _ = try reader.populateChunks(a, &txn, &compiled, &.{column}) else {
                for (compiled.chunkPlans()) |*chunk| {
                    var output = reader.PreparedChunks.Output{ .owner = &compiled, .chunk = chunk, .columns = &.{column} };
                    output.sink().deliverDecoded(scores[chunk.first..][0..chunk.request.readKeys().len]);
                }
            }
        }
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
        std.debug.print("REVIEW_OWNED_RESULTS mode={d} cpu_us={d} added_bytes={d}\n", .{ mode, cpu, scores.len * @sizeOf(f64) });
    }
}

test "lite prepared fresh review preparation peak ownership" {
    const a = std.testing.allocator;
    for ([_]usize{ 8192, 50000 }) |count| {
        const storage = try a.alloc([8]u8, count);
        defer a.free(storage);
        const nodes = try a.alloc([]const u8, count);
        defer a.free(nodes);
        for (storage, nodes, 0..) |*bytes, *node, i| {
            _ = try std.fmt.bufPrint(bytes, "{d:0>8}", .{i});
            node.* = bytes;
        }
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        const started = std.posix.getrusage(0);
        var request = try @import("../prepared_score_request.zig").PreparedScoreChunks.init(counter.allocator(), &.{""}, nodes);
        defer request.deinit();
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        std.debug.print("REVIEW_PREPARATION keys={d} cpu_us={d} owned_bytes={d} peak_bytes={d} allocations={d}\n", .{ count, cpu, counter.live, counter.peak, counter.alloc_calls });
    }
}

test "lite prepared query result combined routes preserve duplicate and absent columns" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "query-combined-results.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [2][8]u8 = undefined;
    std.mem.writeInt(u64, &raw[0], @bitCast(@as(f64, 3)), .little);
    std.mem.writeInt(u64, &raw[1], @bitCast(@as(f64, 9)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p/a\x00\x00", .value = &raw[0] }, .{ .key = "q/a\x00\x00", .value = &raw[1] } });
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    const statuses = try alloc.alloc(Graph.GraphMetricStatus, 3);
    for (statuses, [_][]const u8{ "p", "q", "absent" }) |*status, name| status.* = .{ .name = try alloc.dupe(u8, name), .published_generation = 1 };
    const prefixes = try alloc.alloc(?[]const u8, 3);
    prefixes[0] = try alloc.dupe(u8, "p/");
    prefixes[1] = try alloc.dupe(u8, "q/");
    prefixes[2] = null;
    var session = Graph.GraphMetricReadSession{ .alloc = alloc, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(alloc, try Txn.openRead(&store)), .statuses = statuses, .prefixes = prefixes };
    defer session.deinit();
    var nodes: [5000][]const u8 = @splat("a");
    nodes[0] = "missing";
    var prepared = try session.prepareColumns(alloc, &.{ "p", "q", "absent", "p" }, &nodes);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 1), prepared.request.chunkPlans().len);
    try std.testing.expectEqual(@as(usize, 4), prepared.results.?.handle.values.len);
    var columns: [4][5000]?f64 = undefined;
    const output = [_][]?f64{ &columns[0], &columns[1], &columns[2], &columns[3] };
    try session.readPreparedColumns(alloc, &prepared, &output);
    try std.testing.expect(prepared.results.?.handle.ready);
    const before = counter.alloc_attempts;
    try session.readPreparedColumns(alloc, &prepared, &output);
    try std.testing.expectEqual(before, counter.alloc_attempts);
    try std.testing.expect(columns[0][0] == null and columns[1][0] == null);
    for (columns[0][1..]) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    for (columns[1][1..]) |value| try std.testing.expectEqual(@as(?f64, 9), value);
    for (columns[2]) |value| try std.testing.expect(value == null);
    try std.testing.expectEqualSlices(?f64, &columns[0], &columns[3]);
    var view = try session.readPreparedView(alloc, &prepared);
    defer view.deinit();
    const Check = struct {
        columns: []const []?f64,
        valid: bool = true,
        count: usize = 0,
        fn write(ctx: *anyopaque, col: usize, row: usize, value: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.columns[col][row] != value) self.valid = false;
            self.count += 1;
        }
    };
    var check = Check{ .columns = &output };
    view.visit(.{ .context = &check, .write = Check.write });
    try std.testing.expect(check.valid);
    try std.testing.expectEqual(@as(usize, 20000), check.count);
}

test "lite prepared query results fence mutations of borrowed transaction namespaces" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "query-namespace-results.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [2][8]u8 = undefined;
    std.mem.writeInt(u64, &raw[0], @bitCast(@as(f64, 3)), .little);
    std.mem.writeInt(u64, &raw[1], @bitCast(@as(f64, 9)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p\x00a\x00\x00", .value = &raw[0] }, .{ .key = "q\x00a\x00\x00", .value = &raw[1] } });
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var session = try testResultSession(a, &graph, &store);
    session.txn.abort();
    var namespace: [2]u8 = .{ 'p', 0 };
    session.txn = try backend_erased.readTxnFrom(a, try Txn.openReadWithPrefix(&store, &namespace));
    defer session.deinit();
    var prepared = try session.prepareColumns(a, &.{"metric"}, &.{"a"});
    defer prepared.deinit();
    var column: [1]?f64 = undefined;
    try session.readPreparedColumns(a, &prepared, &.{&column});
    try std.testing.expectEqual(@as(?f64, 3), column[0]);
    namespace[0] = 'q';
    try session.readPreparedColumns(a, &prepared, &.{&column});
    try std.testing.expectEqual(@as(?f64, 9), column[0]);
    try std.testing.expect(prepared.results.?.handle.disabled);
    try std.testing.expectEqual(@as(usize, 0), prepared.results.?.handle.values.len);
    try std.testing.checkAllAllocationFailures(a, resultAllocationFailureCase, .{ &prepared.request, &session.txn });
}

test "lite prepared leased views preserve routing bypass and owner lifetimes" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "leased-views.aflite");
    defer a.free(path);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var store = try Store.createWithOptions(alloc, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const count = 5000;
    const storage = try a.alloc([10]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count);
    defer a.free(nodes);
    const docs = try a.alloc(native.DocumentMutation, count);
    defer a.free(docs);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (storage, docs, 0..) |*key, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    const column = try a.alloc(?f64, count);
    defer a.free(column);
    const seen = try a.alloc(bool, count * 2);
    defer a.free(seen);
    const Check = struct {
        nodes: []const []const u8,
        seen: []bool,
        valid: bool = true,
        fn write(ctx: *anyopaque, col: usize, row: usize, value: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (col >= 2 or row >= self.nodes.len) {
                self.valid = false;
                return;
            }
            const expected: ?f64 = if (std.mem.eql(u8, self.nodes[row], "missing")) null else 3;
            if (value != expected or self.seen[col * self.nodes.len + row]) self.valid = false;
            self.seen[col * self.nodes.len + row] = true;
        }
    };
    for (0..4) |mode| {
        const rows: usize = if (mode == 0) 3 else count;
        for (nodes[0..rows], 0..) |*node, i| node.* = storage[if (mode == 2) rows - i - 1 else if (mode == 3) i % 7 else i][0..8];
        nodes[rows - 1] = "missing";
        var session = try testResultSession(alloc, &graph, &store);
        var prepared = try session.prepareColumns(alloc, &.{ "metric", "metric" }, nodes[0..rows]);
        var lease = try session.readPreparedView(alloc, &prepared);
        const before = counter.alloc_attempts;
        var second = try session.readPreparedView(alloc, &prepared);
        try std.testing.expectEqual(before, counter.alloc_attempts);
        @memset(seen[0 .. rows * 2], false);
        var check = Check{ .nodes = nodes[0..rows], .seen = seen[0 .. rows * 2] };
        lease.visit(.{ .context = &check, .write = Check.write });
        try std.testing.expect(check.valid);
        for (check.seen) |visited| try std.testing.expect(visited);
        if (mode == 1) {
            const dense = lease.denseColumn(0).?;
            try std.testing.expectEqual(rows, dense.len());
            try std.testing.expectEqual(@as(?f64, 3), dense.at(0));
            try std.testing.expect(dense.at(rows - 1) == null);
            try std.testing.expectEqual(dense.values.ptr, lease.denseColumn(1).?.values.ptr);
        } else if (mode >= 2) try std.testing.expect(lease.denseColumn(0) == null);
        // Exact shared output storage is also safe on the cached production path.
        try session.readPreparedColumns(alloc, &prepared, &.{ column[0..rows], column[0..rows] });
        try std.testing.expect(column[rows - 1] == null);
        var floats: [5000]f64 = undefined;
        try session.readDecodedColumns(alloc, &.{ "metric", "metric" }, nodes[0..rows], &.{ floats[0..rows], floats[0..rows] });
        for (floats[0..rows], column[0..rows]) |value, expected| {
            if (expected) |score| try std.testing.expectEqual(score, value) else try std.testing.expect(std.math.isNan(value));
        }
        var pinned = try Txn.openRead(&store);
        const file = try pinned.readFile();
        file.page_cache_bypass.store(1, .monotonic);
        try std.testing.expectError(error.Unsupported, session.readPreparedView(alloc, &prepared));
        try session.readPreparedColumns(alloc, &prepared, &.{ column[0..rows], column[0..rows] });
        file.page_cache_bypass.store(0, .monotonic);
        // Recapture must not overwrite active leases after bypass invalidation.
        try std.testing.expectError(error.Unsupported, session.readPreparedView(alloc, &prepared));
        pinned.abort();
        prepared.deinit();
        session.deinit();
        @memset(check.seen, false);
        lease.visit(.{ .context = &check, .write = Check.write });
        try std.testing.expect(check.valid);
        for (check.seen) |visited| try std.testing.expect(visited);
        second.deinit();
        lease.deinit();
    }
}

test "lite prepared leased views retain old namespace through invalidation" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "leased-namespace.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [2][8]u8 = undefined;
    std.mem.writeInt(u64, &raw[0], @bitCast(@as(f64, 3)), .little);
    std.mem.writeInt(u64, &raw[1], @bitCast(@as(f64, 9)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p\x00a\x00\x00", .value = &raw[0] }, .{ .key = "q\x00a\x00\x00", .value = &raw[1] } });
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var session = try testResultSession(a, &graph, &store);
    session.txn.abort();
    var namespace: [2]u8 = .{ 'p', 0 };
    session.txn = try backend_erased.readTxnFrom(a, try Txn.openReadWithPrefix(&store, &namespace));
    var prepared = try session.prepareColumns(a, &.{"metric"}, &.{"a"});
    var lease = try session.readPreparedView(a, &prepared);
    const old = lease.denseColumn(0).?;
    try std.testing.expectEqual(@as(?f64, 3), old.at(0));
    namespace[0] = 'q';
    try std.testing.expectError(error.Unsupported, session.readPreparedView(a, &prepared));
    var column: [1]?f64 = undefined;
    try session.readPreparedColumns(a, &prepared, &.{&column});
    try std.testing.expectEqual(@as(?f64, 9), column[0]);
    try std.testing.expectEqual(@as(?f64, 3), old.at(0));
    prepared.deinit();
    session.deinit();
    try std.testing.expectEqual(@as(?f64, 3), old.at(0));
    lease.deinit();
}

test "lite prepared leased dense view measurement" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "view-measurement.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const count = 50000;
    const storage = try a.alloc([10]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count);
    defer a.free(nodes);
    const docs = try a.alloc(native.DocumentMutation, count);
    defer a.free(docs);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (storage, nodes, docs, 0..) |*key, *node, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        node.* = key[0..8];
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var session = try testResultSession(alloc, &graph, &store);
    defer session.deinit();
    var prepared = try session.prepareColumns(alloc, &.{"metric"}, nodes);
    defer prepared.deinit();
    const has_view = @hasDecl(Graph.GraphMetricReadSession, "readPreparedView");
    for (0..2) |mode| {
        const use_view = has_view and mode == 1;
        const column: []?f64 = if (!use_view) try a.alloc(?f64, count) else &.{};
        defer a.free(column);
        if (has_view) {
            if (use_view) {
                var view = try session.readPreparedView(alloc, &prepared);
                view.deinit();
            } else try session.readPreparedColumns(alloc, &prepared, &.{column});
        } else try session.readPreparedColumns(alloc, &prepared, &.{column});
        var checksum: f64 = 0;
        const before = counter.alloc_calls;
        const started = std.posix.getrusage(0);
        for (0..100) |_| {
            if (has_view) {
                if (use_view) {
                    var view = try session.readPreparedView(alloc, &prepared);
                    const dense = view.denseColumn(0).?;
                    for (0..dense.len()) |row| checksum += dense.at(row) orelse 0;
                    view.deinit();
                } else {
                    try session.readPreparedColumns(alloc, &prepared, &.{column});
                    for (column) |value| checksum += value orelse 0;
                }
            } else {
                try session.readPreparedColumns(alloc, &prepared, &.{column});
                for (column) |value| checksum += value orelse 0;
            }
        }
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        try std.testing.expectEqual(@as(f64, 15000000), checksum);
        std.debug.print("DENSE_VIEW mode={d} cpu_us={d} allocations={d} output_bytes={d}\n", .{ mode, cpu, counter.alloc_calls - before, column.len * @sizeOf(?f64) });
    }
}

test "lite prepared fresh review unsorted construction ownership and mapping" {
    const a = std.testing.allocator;
    const count = 50000;
    const storage = try a.alloc([8]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count);
    defer a.free(nodes);
    const column = try a.alloc(?f64, count);
    defer a.free(column);
    const missing = try a.alloc(?f64, count);
    defer a.free(missing);
    const duplicate = try a.alloc(?f64, count);
    defer a.free(duplicate);
    for (storage, 0..) |*bytes, i| _ = try std.fmt.bufPrint(bytes, "{d:0>8}", .{i});
    for (0..3) |mode| {
        for (nodes, 0..) |*node, i| node.* = &storage[
            switch (mode) {
                0 => i,
                1 => count - i - 1,
                2 => (i * 7919) % count,
                else => unreachable,
            }
        ];
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        const started = std.posix.getrusage(0);
        var request = try @import("../prepared_score_request.zig").PreparedScoreChunks.init(counter.allocator(), &.{ "p/", null, "p/" }, nodes);
        defer request.deinit();
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        std.debug.print("REVIEW_UNSORTED mode={d} build_cpu_us={d} owned_bytes={d} peak_bytes={d} allocations={d}\n", .{ mode, cpu, counter.live, counter.peak, counter.alloc_calls });
        const scores = try a.alloc(f64, 4096);
        defer a.free(scores);
        request.clearAbsentColumns(&.{ column, missing, duplicate });
        for (request.chunkPlans()) |*chunk| {
            const length = chunk.request.readKeys().len;
            for (scores[0..length], 0..) |*score, i| score.* = @floatFromInt(chunk.first + i);
            var output = @import("../prepared_score_request.zig").PreparedScoreChunks.Output{ .owner = &request, .chunk = chunk, .columns = &.{ column, missing, duplicate } };
            output.sink().deliverDecoded(scores[0..length]);
        }
        request.finishColumns(&.{ column, missing, duplicate });
        for (nodes, column) |node, value| try std.testing.expectEqual(@as(?f64, @floatFromInt(try std.fmt.parseInt(usize, node, 10))), value);
        for (missing) |value| try std.testing.expect(value == null);
        try std.testing.expectEqualSlices(?f64, column, duplicate);
    }
}

test "lite prepared fresh review partially overlapping duplicate output" {
    const PreparedScoreChunks = @import("../prepared_score_request.zig").PreparedScoreChunks;
    const nodes: [5000][]const u8 = @splat("a");
    var request = try PreparedScoreChunks.init(std.testing.allocator, &.{ "p/", "p/" }, &nodes);
    defer request.deinit();
    var values: [5001]?f64 = @splat(3);
    const columns = [_][]?f64{ values[0..5000], values[1..5001] };
    try std.testing.expectError(error.InvalidBatch, request.validateColumns(&columns));
    for (values) |value| try std.testing.expectEqual(@as(?f64, 3), value);
}

test "lite prepared fresh review visitor column scaling" {
    const PreparedScoreChunks = @import("../prepared_score_request.zig").PreparedScoreChunks;
    const a = std.testing.allocator;
    const rows = 8192;
    const storage = try a.alloc([8]u8, rows);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, rows);
    defer a.free(nodes);
    for (storage, nodes, 0..) |*bytes, *node, i| {
        _ = try std.fmt.bufPrint(bytes, "{d:0>8}", .{i});
        node.* = bytes;
    }
    const Check = struct {
        count: usize = 0,
        sum: f64 = 0,
        fn write(ctx: *anyopaque, _: usize, _: usize, score: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.count += 1;
            self.sum += score orelse 0;
        }
    };
    for ([_]usize{ 1, 8, 32, 64 }) |cols| {
        var prefixes: [64]?[]const u8 = undefined;
        var names: [64][4]u8 = undefined;
        for (names[0..cols], prefixes[0..cols], 0..) |*name, *prefix, i| {
            _ = try std.fmt.bufPrint(name, "{d:0>3}/", .{i});
            prefix.* = name;
        }
        var request = try PreparedScoreChunks.init(a, prefixes[0..cols], nodes);
        defer request.deinit();
        const scores = try a.alloc(f64, rows * cols);
        defer a.free(scores);
        @memset(scores, 3);
        var check = Check{};
        const started = std.posix.getrusage(0);
        for (0..20) |_| request.visitDecoded(scores, .{ .context = &check, .write = Check.write });
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        try std.testing.expectEqual(rows * cols * 20, check.count);
        try std.testing.expectEqual(@as(f64, @floatFromInt(check.count * 3)), check.sum);
        std.debug.print("VISIT_SCALING columns={d} cells={d} cpu_us={d}\n", .{ cols, check.count, cpu });
    }
}

test "lite prepared fresh review detached view retention" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "view-measurement.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const count = 50000;
    const storage = try a.alloc([10]u8, count);
    defer a.free(storage);
    const nodes = try a.alloc([]const u8, count);
    defer a.free(nodes);
    const docs = try a.alloc(native.DocumentMutation, count);
    defer a.free(docs);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    for (storage, nodes, docs, 0..) |*key, *node, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        key[8] = 0;
        key[9] = 0;
        node.* = key[0..8];
        doc.* = .{ .key = key, .value = &raw };
    }
    try store.file.putDocumentBatch(docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const alloc = counter.allocator();
    var session = try testResultSession(alloc, &graph, &store);
    defer session.deinit();
    var prepared = try session.prepareColumns(alloc, &.{"metric"}, nodes);

    var key_bytes: usize = 0;
    for (prepared.request.chunkPlans()) |chunk| key_bytes += chunk.request.ownedKeyBytes();
    const score_bytes = prepared.results.?.handle.values.len * @sizeOf(f64);
    var view = try session.readPreparedView(alloc, &prepared);
    const before = counter.live;
    prepared.deinit();
    const after = counter.live;
    const dense = view.denseColumn(0).?;
    try std.testing.expectEqual(@as(?f64, 3), dense.at(count - 1));
    try std.testing.expect(after < before);
    try std.testing.expect(after < score_bytes + 4096);
    view.deinit();
    std.debug.print("RETAINED_VIEW key_bytes={d} score_bytes={d} before_owner_close={d} after_owner_close={d} after_view_close={d}\n", .{ key_bytes, score_bytes, before, after, counter.live });
}

test "lite prepared fresh review transaction wrapper address reuse" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "result-wrapper-reuse.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 3)), .little);
    try store.file.putDocument("a\x00\x00", &raw);
    const reader = @import("../../graph/score_read.zig");
    var request = try reader.PreparedChunks.init(a, &.{""}, &.{"a"});
    defer request.deinit();
    var old = try backend_erased.readTxnFrom(a, try Txn.openRead(&store));
    var results = try reader.PreparedResults.init(a, &request, &old);
    defer results.deinit();
    var column: [1]?f64 = undefined;
    _ = try results.populate(a, &old, &request, &.{&column});
    try std.testing.expectEqual(@as(?f64, 3), column[0]);
    const old_address = @intFromPtr(old.ptr);
    old.abort();
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocument("a\x00\x00", &raw);
    for (0..100) |attempt| {
        var fresh = try backend_erased.readTxnFrom(a, try Txn.openRead(&store));
        defer fresh.abort();
        if (@intFromPtr(fresh.ptr) != old_address) continue;
        _ = try reader.populateChunks(a, &fresh, &request, &.{&column});
        try std.testing.expectEqual(@as(?f64, 9), column[0]);
        try std.testing.expectError(error.InvalidBatch, results.populate(a, &fresh, &request, &.{&column}));
        try std.testing.expectError(error.InvalidBatch, results.readView(a, &fresh, &request));
        std.debug.print("WRAPPER_REUSE attempt={d} fresh_score=9 stale_cache_rejected=true\n", .{attempt});
        return;
    }
    return error.WrapperAddressWasNotReused;
}

fn testProductionMetricStages(limit: usize, runs: usize, unlimited: bool) !void {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "decoded-stages.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const rows = 50000;
    const cols = 4;
    const storage = try a.alloc([14]u8, rows * cols);
    defer a.free(storage);
    const raw = try a.alloc([8]u8, rows);
    defer a.free(raw);
    const docs = try a.alloc(native.DocumentMutation, rows * cols);
    defer a.free(docs);
    const nodes = try a.alloc(@import("../../graph/query.zig").GraphResultNode, rows);
    defer a.free(nodes);
    for (raw, 0..) |*value, row| std.mem.writeInt(u64, value, @bitCast(@as(f64, @floatFromInt(row % 100))), .little);
    for (storage, docs, 0..) |*key, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..12], "p{d:0>2}/{d:0>8}", .{ i / rows, i % rows });
        key[12] = 0;
        key[13] = 0;
        doc.* = .{ .key = key, .value = &raw[i % rows] };
        if (i < rows) nodes[i] = .{ .key = key[4..12], .depth = 0, .distance = 0 };
    }
    try store.file.putDocumentBatch(docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    const query = @import("../../graph/query.zig");
    const Engine = query.GraphQueryEngine;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    const names = [_][]const u8{ "m0", "m1", "m2", "m3" };
    const statuses = try a.alloc(Graph.GraphMetricStatus, cols);
    for (statuses, names) |*status, name| status.* = .{ .name = try a.dupe(u8, name), .published_generation = 1 };
    const prefixes = try a.alloc(?[]const u8, cols);
    for (prefixes, [_][]const u8{ "p00/", "p01/", "p02/", "p03/" }) |*prefix, name| prefix.* = try a.dupe(u8, name);
    var session = Graph.GraphMetricReadSession{ .alloc = a, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(a, try Txn.openRead(&store)), .statuses = statuses, .prefixes = prefixes };
    defer session.deinit();
    for (0..4) |mode| {
        if (mode == 2) {
            var random = std.Random.DefaultPrng.init(0x1080);
            random.random().shuffle(query.GraphResultNode, nodes);
        }
        const threshold: f64 = if (unlimited) 98 else 50;
        const filters = [_]query.GraphMetricFilter{ .{ .name = "m0", .op = .gte, .value = threshold }, .{ .name = "m1", .op = .gte, .value = threshold }, .{ .name = "m2", .op = .gte, .value = threshold }, .{ .name = "m3", .op = .gte, .value = threshold } };
        const request = query.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = if (unlimited) 0 else 1000 }, .metrics = &.{ .{ .name = "m0" }, .{ .name = "m1" }, .{ .name = "m2" }, .{ .name = "m3" } }, .order_by = if (unlimited and mode % 2 == 0) &.{} else &.{.{ .name = "m0" }}, .where_metric = filters[0..if (mode % 2 == 0) 1 else cols] };
        const plan = try query.MetricReadPlan.init(request);
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = limit };
        const alloc = counter.allocator();
        const started = std.posix.getrusage(0);
        var checksum: f64 = 0;
        var retained_bytes: usize = 0;
        for (0..runs) |_| {
            var work = try Engine.MetricStageWorkspace.init(alloc, plan, nodes.len);
            defer work.deinit();
            const fused = if (@hasDecl(Engine.MetricStageWorkspace, "selectReading")) try work.selectReading(&session, request, true, nodes) else false;
            if (!fused) {
                try work.ensure(&session, plan.filters.slice(), nodes);
                var filter = request;
                filter.metrics = &.{};
                filter.order_by = &.{};
                try work.select(filter, false, plan.filters.slice(), plan.orders.slice(), plan.projections.slice());
                try work.ensure(&session, plan.orders.slice(), nodes);
                var order = request;
                order.metrics = &.{};
                order.where_metric = &.{};
                try work.select(order, true, plan.orders.slice(), plan.projections.slice(), &.{});
            }
            try work.ensure(&session, plan.projections.slice(), nodes);
            try std.testing.expectEqual(@as(usize, 1000), work.rows.len);
            retained_bytes = 0;
            for (work.columns) |maybe| if (maybe) |slot| switch (slot.source) {
                .nullable => |values| retained_bytes += values.len * @sizeOf(?f64),
                .decoded => |values| retained_bytes += values.len * @sizeOf(f64),
                .paged => |page| retained_bytes += page.owner.storageBytes() / page.owner.columns,
            };
            const Column = if (@hasDecl(Engine, "MetricColumn")) Engine.MetricColumn else []?f64;
            var columns: [cols]Column = undefined;
            work.columnsFor(plan.projections.slice(), &columns);
            for (columns) |column| for (0..1000) |row| {
                const expected: f64 = if (request.order_by.len == 0) @floatFromInt((try std.fmt.parseInt(usize, nodes[work.rows[row]].key, 10)) % 100) else if (row < 500) 99 else 98;
                const value = (if (@hasDecl(Engine, "MetricColumn")) column.at(row) else column[row]).?;
                try std.testing.expectEqual(expected, value);
                checksum += value;
            };
        }
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        try std.testing.expectEqual(@as(f64, @floatFromInt(runs)) * 394000, checksum);
        try std.testing.expectEqual(@as(usize, 0), counter.live);
        std.debug.print("PRODUCTION_STAGES unlimited={} mode={d} rows={d} columns={d} cpu_us={d} peak_bytes={d} allocations={d} retained_bytes={d}\n", .{ unlimited, mode, rows, cols, cpu, counter.peak, counter.alloc_calls, retained_bytes });
    }
}

test "lite prepared production staged decoded measurement" {
    try testProductionMetricStages(std.math.maxInt(usize), 5, false);
}

test "lite prepared fused native stages fit one MiB working budget" {
    try testProductionMetricStages(1024 * 1024, 1, false);
}

test "lite prepared unlimited native filters fit one MiB working budget" {
    try testProductionMetricStages(1024 * 1024, 1, true);
}

test "lite prepared bounded topk measurement" {
    const a = std.testing.allocator;
    const query = @import("../../graph/query.zig");
    const rows = 50000;
    const values = try a.alloc(?f64, rows);
    defer a.free(values);
    for (values, 0..) |*value, i| value.* = @floatFromInt(i % 100);
    const request = query.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 1000 }, .order_by = &.{.{ .name = "rank" }} };
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    const started = std.posix.getrusage(0);
    for (0..100) |_| {
        const selected = try query.GraphQueryEngine.selectLoadedMetricCandidateIndexesAlloc(counter.allocator(), &.{"rank"}, &.{values}, request, true, rows);
        defer counter.allocator().free(selected);
        try std.testing.expectEqual(@as(usize, 1000), selected.len);
        try std.testing.expectEqual(@as(usize, 99), selected[0]);
        try std.testing.expectEqual(@as(usize, 49998), selected[999]);
    }
    const ended = std.posix.getrusage(0);
    const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    std.debug.print("BOUNDED_TOPK rows={d} limit=1000 cpu_us={d} peak_bytes={d} allocations={d}\n", .{ rows, cpu, counter.peak, counter.alloc_calls });
}

test "lite prepared decoded dense delivery preserves columns permutations and missing scores" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "decoded-dense.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const storage = try a.alloc([8]u8, 1024);
    defer a.free(storage);
    const raw = try a.alloc([8]u8, 1024);
    defer a.free(raw);
    const docs = try a.alloc(native.DocumentMutation, 1024);
    defer a.free(docs);
    const nodes = try a.alloc([]const u8, 512);
    defer a.free(nodes);
    for (storage, raw, docs, 0..) |*key, *value, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..6], "{s}{d:0>4}", .{ if (i < 512) @as([]const u8, "p/") else "q/", i % 512 });
        key[6] = 0;
        key[7] = 0;
        std.mem.writeInt(u64, value, @bitCast(@as(f64, @floatFromInt(i))), .little);
        doc.* = .{ .key = key, .value = value };
        if (i < 512) nodes[i] = key[2..6];
    }
    try store.file.putDocumentBatch(docs);
    var txn = try backend_erased.readTxnFrom(a, try Txn.openRead(&store));
    defer txn.abort();
    const x = try a.alloc(f64, 512);
    defer a.free(x);
    const y = try a.alloc(f64, 512);
    defer a.free(y);
    const z = try a.alloc(f64, 512);
    defer a.free(z);
    const score_read = @import("../../graph/score_read.zig");
    for (0..3) |pass| {
        if (pass == 2) std.mem.reverse([]const u8, nodes);
        _ = try score_read.populateFloatColumns(a, &txn, &.{ "q/", null, "p/" }, nodes, &.{ x, y, z });
        for (x, y, z, 0..) |vx, vy, vz, i| {
            const row = if (pass == 2) 511 - i else i;
            try std.testing.expectEqual(@as(f64, @floatFromInt(row + 512)), vx);
            try std.testing.expect(std.math.isNan(vy));
            try std.testing.expectEqual(@as(f64, @floatFromInt(row)), vz);
        }
    }
    nodes[17] = "missing";
    _ = try score_read.populateFloatColumns(a, &txn, &.{ "p/", "q/", "p/" }, nodes, &.{ x, y, z });
    for (x, y, z, 0..) |vx, vy, vz, i| {
        if (i == 17) {
            try std.testing.expect(std.math.isNan(vx) and std.math.isNan(vy) and std.math.isNan(vz));
        } else {
            try std.testing.expectEqual(@as(f64, @floatFromInt(511 - i)), vx);
            try std.testing.expectEqual(vx + 512, vy);
            try std.testing.expectEqual(vx, vz);
        }
    }
}

test "lite prepared sparse compaction measurement" {
    const a = std.testing.allocator;
    const q = @import("../../graph/query.zig");
    const rows = 50000;
    const nodes = try a.alloc(q.GraphResultNode, rows);
    defer a.free(nodes);
    const key_storage = try a.alloc([8]u8, rows);
    defer a.free(key_storage);
    for (nodes, key_storage, 0..) |*node, *key, row| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>8}", .{row}), .depth = 0, .distance = 0 };
    const Reader = struct {
        pub fn readDecodedColumns(_: *@This(), _: std.mem.Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            for (columns) |column| {
                for (keys, column) |key, *value| value.* = @floatFromInt(try std.fmt.parseInt(usize, key, 10));
            }
        }
    };
    const query = q.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 1 }, .metrics = &.{ .{ .name = "a" }, .{ .name = "b" }, .{ .name = "c" }, .{ .name = "d" } }, .order_by = &.{.{ .name = "a" }} };
    const plan = try q.MetricReadPlan.init(query);
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var work = try q.GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), plan, rows);
    var reader = Reader{};
    try work.ensure(&reader, plan.projections.slice(), nodes);
    const before = counter.live;
    try work.select(query, true, plan.projections.slice(), plan.projections.slice(), &.{});
    try std.testing.expectEqual(@as(usize, 1), work.rows.len);
    const after = counter.live;
    var columns: [4]q.GraphQueryEngine.MetricColumn = undefined;
    work.columnsFor(plan.projections.slice(), &columns);
    for (columns) |column| try std.testing.expectEqual(@as(?f64, 49999), column.at(0));
    work.deinit();
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    std.debug.print("SPARSE_COMPACTION rows={d} selected=1 before_bytes={d} after_bytes={d} peak_bytes={d}\n", .{ rows, before, after, counter.peak });
}

test "lite prepared decoded stream fences replaced transactions and bounds scratch reuse" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "stream-binding.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var p: [8]u8 = undefined;
    var q: [8]u8 = undefined;
    std.mem.writeInt(u64, &p, @bitCast(@as(f64, 3)), .little);
    std.mem.writeInt(u64, &q, @bitCast(@as(f64, 9)), .little);
    try store.file.putDocumentBatch(&.{ .{ .key = "p\x00a\x00\x00", .value = &p }, .{ .key = "q\x00a\x00\x00", .value = &q } });
    var namespace = [_]u8{ 'p', 0 };
    var txn = try backend_erased.readTxnFrom(a, try Txn.openReadWithPrefix(&store, &namespace));
    defer txn.abort();
    var stream = @import("../../graph/score_read.zig").FloatColumnStream.init(a, &txn);
    defer stream.deinit();
    var column: [1]f64 = undefined;
    for (0..20) |_| {
        _ = try stream.populate(&.{""}, &.{"a"}, &.{&column});
        try std.testing.expectEqual(@as(f64, 3), column[0]);
        try std.testing.expect(stream.scope != null);
    }
    namespace[0] = 'q';
    _ = try stream.populate(&.{""}, &.{"a"}, &.{&column});
    try std.testing.expectEqual(@as(f64, 9), column[0]);
    namespace[0] = 'p';
    _ = try stream.populate(&.{""}, &.{"a"}, &.{&column});
    try std.testing.expectEqual(@as(f64, 3), column[0]);
    txn.abort();
    txn = try backend_erased.readTxnFrom(a, try Txn.openReadWithPrefix(&store, "q\x00"));
    try std.testing.expectError(error.InvalidBatch, stream.populate(&.{""}, &.{"a"}, &.{&column}));
    _ = try @import("../../graph/score_read.zig").populateFloatColumns(a, &txn, &.{""}, &.{"a"}, &.{&column});
    try std.testing.expectEqual(@as(f64, 9), column[0]);
}

test "lite prepared sparse compaction falls back within original working budget" {
    const a = std.testing.allocator;
    const q = @import("../../graph/query.zig");
    const n = 16000;
    const nodes = try a.alloc(q.GraphResultNode, n);
    defer a.free(nodes);
    const key_storage = try a.alloc([8]u8, n);
    defer a.free(key_storage);
    for (nodes, key_storage, 0..) |*node, *key, row| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>8}", .{row}), .depth = 0, .distance = 0 };
    const names = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p" };
    var metrics: [16]q.GraphMetricRead = undefined;
    for (&metrics, names) |*metric, name| metric.* = .{ .name = name };
    var filters: [16]q.GraphMetricFilter = undefined;
    for (&filters, names) |*filter, name| filter.* = .{ .name = name, .op = .gte, .value = 12000 };
    const request = q.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 0 }, .metrics = &metrics, .where_metric = &filters };
    const plan = try q.MetricReadPlan.init(request);
    const Reader = struct {
        pub fn readDecodedColumns(_: *@This(), _: std.mem.Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            for (columns) |column| {
                for (keys, column) |key, *value| value.* = @floatFromInt(try std.fmt.parseInt(usize, key, 10));
            }
        }
    };
    for (0..2) |mode| {
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var work = try q.GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), plan, n);
        var reader = Reader{};
        try work.ensure(&reader, plan.projections.slice(), nodes);
        const before = counter.live;
        counter.peak = before;
        if (mode == 1) counter.limit = before + n * 16;
        try work.select(request, true, plan.projections.slice(), plan.projections.slice(), &.{});
        try std.testing.expectEqual(@as(usize, 4000), work.rows.len);
        var columns: [16]q.GraphQueryEngine.MetricColumn = undefined;
        work.columnsFor(plan.projections.slice(), &columns);
        for (columns) |column| for (0..4000) |row| {
            try std.testing.expectEqual(@as(?f64, @floatFromInt(12000 + row)), column.at(row));
        };
        if (mode == 1) {
            try std.testing.expect(counter.peak <= counter.limit);
            for (work.columns) |slot| try std.testing.expectEqual(@as(usize, n), slot.?.source.decoded.len);
        } else {
            for (work.columns) |slot| try std.testing.expectEqual(@as(usize, 4000), slot.?.source.decoded.len);
        }
        std.debug.print("COMPACTION_BUDGET mode={d} before={d} peak={d} after={d}\n", .{ mode, before, counter.peak, counter.live });
        work.deinit();
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "lite prepared filter limits stop IO and discard predicate-only heap scores" {
    const a = std.testing.allocator;
    const q = @import("../../graph/query.zig");
    const n = 50000;
    const storage = try a.alloc([5]u8, n);
    defer a.free(storage);
    const nodes = try a.alloc(q.GraphResultNode, n);
    defer a.free(nodes);
    for (storage, nodes, 0..) |*key, *node, i| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>5}", .{i}), .depth = 0, .distance = 0 };
    const names = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p" };
    var filters: [16]q.GraphMetricFilter = undefined;
    for (&filters, names) |*filter, name| filter.* = .{ .name = name, .op = .gte, .value = 0 };
    const Reader = struct {
        reads: usize = 0,
        calls: usize = 0,
        pub fn readDecodedColumns(self: *@This(), _: std.mem.Allocator, selected: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            self.reads += selected.len * keys.len;
            self.calls += 1;
            for (columns) |column| @memset(column, 1);
        }
    };
    for (0..2) |mode| {
        const request = q.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 1000 }, .where_metric = &filters, .order_by = if (mode == 0) &.{} else &.{.{ .name = "a" }} };
        const plan = try q.MetricReadPlan.init(request);
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var work = try q.GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), plan, n);
        var reader = Reader{};
        try std.testing.expect(try work.selectReading(&reader, request, true, nodes));
        try std.testing.expectEqual(@as(usize, 1000), work.rows.len);
        for (work.rows, 0..) |row, i| try std.testing.expectEqual(i, row);
        for (work.columns) |col| try std.testing.expect(col == null);
        if (mode == 0) {
            try std.testing.expectEqual(@as(usize, 4), reader.calls);
            try std.testing.expectEqual(@as(usize, 16384), reader.reads);
        } else {
            try std.testing.expectEqual(@as(usize, 800000), reader.reads);
        }
        try std.testing.expect(counter.peak < 500000);
        std.debug.print("FILTER_HEAP mode={d} reads={d} calls={d} peak={d} after={d}\n", .{ mode, reader.reads, reader.calls, counter.peak, counter.live });
        work.deinit();
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "lite prepared compaction reserves all group fallbacks before copying" {
    const a = std.testing.allocator;
    const q = @import("../../graph/query.zig");
    const n = 4097;
    const nodes = try a.alloc(q.GraphResultNode, n);
    defer a.free(nodes);
    const key_storage = try a.alloc([8]u8, n);
    defer a.free(key_storage);
    for (nodes, key_storage, 0..) |*node, *key, row| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>8}", .{row}), .depth = 0, .distance = 0 };
    const names = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l" };
    var metrics: [12]q.GraphMetricRead = undefined;
    for (&metrics, names) |*metric, name| metric.* = .{ .name = name };
    const request = q.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 1000 }, .metrics = &metrics, .order_by = &.{.{ .name = "a" }} };
    const plan = try q.MetricReadPlan.init(request);
    const Reader = struct {
        pub fn readDecodedColumns(_: *@This(), _: std.mem.Allocator, _: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            for (columns) |column| {
                for (keys, column) |key, *value| value.* = @floatFromInt(try std.fmt.parseInt(usize, key, 10));
            }
        }
    };
    var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
    var work = try q.GraphQueryEngine.MetricStageWorkspace.init(counter.allocator(), plan, n);
    var reader = Reader{};
    for (0..3) |i| try work.ensure(&reader, plan.projections.slice()[i * 4 ..][0..4], nodes);
    counter.peak = counter.live;
    counter.limit = counter.live + 56000;
    try work.select(request, true, plan.projections.slice(), plan.projections.slice(), &.{});
    var columns: [12]q.GraphQueryEngine.MetricColumn = undefined;
    work.columnsFor(plan.projections.slice(), &columns);
    for (columns) |column| for (0..1000) |row| {
        try std.testing.expectEqual(@as(?f64, @floatFromInt(n - 1 - row)), column.at(row));
    };
    try std.testing.expect(counter.peak <= counter.limit);
    work.deinit();
    try std.testing.expectEqual(@as(usize, 0), counter.live);
}

fn testReviewDenseMetricStages(limit: usize, runs: usize, unlimited: bool) !void {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "decoded-stages.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    const rows = 50000;
    const cols = 4;
    const storage = try a.alloc([14]u8, rows * cols);
    defer a.free(storage);
    const raw = try a.alloc([8]u8, rows);
    defer a.free(raw);
    const docs = try a.alloc(native.DocumentMutation, rows * cols);
    defer a.free(docs);
    const nodes = try a.alloc(@import("../../graph/query.zig").GraphResultNode, rows);
    defer a.free(nodes);
    for (raw, 0..) |*value, row| std.mem.writeInt(u64, value, @bitCast(@as(f64, @floatFromInt(row % 100))), .little);
    for (storage, docs, 0..) |*key, *doc, i| {
        _ = try std.fmt.bufPrint(key[0..12], "p{d:0>2}/{d:0>8}", .{ i / rows, i % rows });
        key[12] = 0;
        key[13] = 0;
        doc.* = .{ .key = key, .value = &raw[i % rows] };
        if (i < rows) nodes[i] = .{ .key = key[4..12], .depth = 0, .distance = 0 };
    }
    try store.file.putDocumentBatch(docs);
    const Graph = @import("../../graph/graph.zig").GraphIndex;
    const query = @import("../../graph/query.zig");
    const Engine = query.GraphQueryEngine;
    var graph = try Graph.openWithPrivateStores(a, "unused-out", "unused-in", "g", .{ .reverse_backend = .mem });
    defer graph.close();
    const names = [_][]const u8{ "m0", "m1", "m2", "m3" };
    const statuses = try a.alloc(Graph.GraphMetricStatus, cols);
    for (statuses, names) |*status, name| status.* = .{ .name = try a.dupe(u8, name), .published_generation = 1 };
    const prefixes = try a.alloc(?[]const u8, cols);
    for (prefixes, [_][]const u8{ "p00/", "p01/", "p02/", "p03/" }) |*prefix, name| prefix.* = try a.dupe(u8, name);
    var session = Graph.GraphMetricReadSession{ .alloc = a, .graph_index = &graph, .txn = try backend_erased.readTxnFrom(a, try Txn.openRead(&store)), .statuses = statuses, .prefixes = prefixes };
    defer session.deinit();
    for (0..4) |mode| {
        if (mode == 2) {
            var random = std.Random.DefaultPrng.init(0x1080);
            random.random().shuffle(query.GraphResultNode, nodes);
        }
        const threshold: f64 = if (unlimited) 0 else 50;
        const filters = [_]query.GraphMetricFilter{ .{ .name = "m0", .op = .gte, .value = threshold }, .{ .name = "m1", .op = .gte, .value = threshold }, .{ .name = "m2", .op = .gte, .value = threshold }, .{ .name = "m3", .op = .gte, .value = threshold } };
        const request = query.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = if (unlimited) 0 else 1000 }, .metrics = &.{ .{ .name = "m0" }, .{ .name = "m1" }, .{ .name = "m2" }, .{ .name = "m3" } }, .order_by = if (unlimited and mode % 2 == 0) &.{} else &.{.{ .name = "m0" }}, .where_metric = filters[0..if (mode % 2 == 0) 1 else cols] };
        const plan = try query.MetricReadPlan.init(request);
        var counter = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = limit };
        const alloc = counter.allocator();
        const started = std.posix.getrusage(0);
        var checksum: f64 = 0;
        var retained_bytes: usize = 0;
        for (0..runs) |_| {
            var work = try Engine.MetricStageWorkspace.init(alloc, plan, nodes.len);
            defer work.deinit();
            const fused = if (@hasDecl(Engine.MetricStageWorkspace, "selectReading")) try work.selectReading(&session, request, true, nodes) else false;
            if (!fused) {
                try work.ensure(&session, plan.filters.slice(), nodes);
                var filter = request;
                filter.metrics = &.{};
                filter.order_by = &.{};
                try work.select(filter, false, plan.filters.slice(), plan.orders.slice(), plan.projections.slice());
                if (plan.orders.len != 0) {
                    try work.ensure(&session, plan.orders.slice(), nodes);
                    var order = request;
                    order.metrics = &.{};
                    order.where_metric = &.{};
                    try work.select(order, true, plan.orders.slice(), plan.projections.slice(), &.{});
                }
            }
            try work.ensure(&session, plan.projections.slice(), nodes);
            try std.testing.expectEqual(@as(usize, rows), work.rows.len);
            retained_bytes = 0;
            for (work.columns) |maybe| if (maybe) |slot| switch (slot.source) {
                .nullable => |values| retained_bytes += values.len * @sizeOf(?f64),
                .decoded => |values| retained_bytes += values.len * @sizeOf(f64),
                .paged => |page| retained_bytes += page.owner.storageBytes() / page.owner.columns,
            };
            const Column = if (@hasDecl(Engine, "MetricColumn")) Engine.MetricColumn else []?f64;
            var columns: [cols]Column = undefined;
            work.columnsFor(plan.projections.slice(), &columns);
            for (columns) |column| for (0..rows) |row| {
                const expected: f64 = @floatFromInt((try std.fmt.parseInt(usize, nodes[work.rows[row]].key, 10)) % 100);
                const value = (if (@hasDecl(Engine, "MetricColumn")) column.at(row) else column[row]).?;
                try std.testing.expectEqual(expected, value);
                checksum += value;
            };
        }
        const ended = std.posix.getrusage(0);
        const cpu = (ended.utime.sec - started.utime.sec + ended.stime.sec - started.stime.sec) * 1000000 + ended.utime.usec - started.utime.usec + ended.stime.usec - started.stime.usec;
        try std.testing.expectEqual(@as(f64, @floatFromInt(runs)) * 9900000, checksum);
        try std.testing.expectEqual(@as(usize, 0), counter.live);
        std.debug.print("REVIEW_DENSE unlimited={} mode={d} rows={d} columns={d} cpu_us={d} peak_bytes={d} allocations={d} retained_bytes={d}\n", .{ unlimited, mode, rows, cols, cpu, counter.peak, counter.alloc_calls, retained_bytes });
    }
}

test "lite prepared dense unlimited native measurement" {
    try testReviewDenseMetricStages(std.math.maxInt(usize), 3, true);
}

test "lite prepared leading ordering bounds prune secondary reads" {
    const a = std.testing.allocator;
    const q = @import("../../graph/query.zig");
    const n = 50000;
    const storage = try a.alloc([5]u8, n);
    defer a.free(storage);
    const nodes = try a.alloc(q.GraphResultNode, n);
    defer a.free(nodes);
    for (storage, nodes, 0..) |*key, *node, i| node.* = .{ .key = try std.fmt.bufPrint(key, "{d:0>5}", .{i}), .depth = 0, .distance = 0 };
    const names = [_][]const u8{ "f", "o0", "o1", "o2", "o3", "o4", "o5", "o6" };
    var orders: [8]q.GraphMetricOrder = undefined;
    for (&orders, names) |*order, name| order.* = .{ .name = name };
    const request = q.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 100 }, .where_metric = &.{.{ .name = "f", .op = .gte, .value = 0 }}, .order_by = &orders };
    const plan = try q.MetricReadPlan.init(request);
    const Reader = struct {
        leading_reads: usize = 0,
        late_reads: usize = 0,
        pub fn readDecodedColumns(self: *@This(), _: std.mem.Allocator, selected: []const []const u8, keys: []const []const u8, columns: []const []f64) !void {
            for (selected, columns) |name, column| {
                if (name[0] == 'f') self.leading_reads += keys.len else self.late_reads += keys.len;
                for (keys, column) |key, *value| {
                    const row = try std.fmt.parseInt(usize, key, 10);
                    value.* = if (name[0] == 'f') @floatFromInt(n - 1 - row) else 1;
                }
            }
        }
    };
    var work = try q.GraphQueryEngine.MetricStageWorkspace.init(a, plan, n);
    defer work.deinit();
    var reader = Reader{};
    try std.testing.expect(try work.selectReading(&reader, request, true, nodes));
    try std.testing.expectEqual(@as(usize, 100), work.rows.len);
    for (work.rows, 0..) |row, i| try std.testing.expectEqual(i, row);
    try std.testing.expectEqual(@as(usize, 50000), reader.leading_reads);
    try std.testing.expectEqual(@as(usize, 3584), reader.late_reads);
    std.debug.print("REVIEW_LATE_ORDER leading={d} late={d} theoretical_late_after_first_batch={d}\n", .{ reader.leading_reads, reader.late_reads, @as(usize, 512 * 7) });
}

test "lite prepared paged scores forced relocation inventories every partial growth failure" {
    const Runner = struct {
        fn run(backing: std.mem.Allocator) !void {
            var relocate = @import("test_allocator.zig").NoResizeAllocator{ .backing = backing };
            const a = relocate.allocator();
            const q = @import("../../graph/query.zig");
            const nodes = try a.alloc(q.GraphResultNode, 5000);
            defer a.free(nodes);
            for (nodes, 0..) |*node, i| node.* = .{ .key = "node", .table = if (i % 13 == 0) "foreign" else null, .depth = 0, .distance = 0 };
            const query = q.GraphQuery{ .query_type = .neighbors, .index_name = "g", .start_nodes = .{ .keys = &.{} }, .params = .{ .max_results = 0 }, .where_metric = &.{ .{ .name = "f0", .op = .gte, .value = 0 }, .{ .name = "f1", .op = .gte, .value = 0 } }, .order_by = &.{ .{ .name = "o0" }, .{ .name = "o1" } }, .metrics = &.{ .{ .name = "f0" }, .{ .name = "f1" }, .{ .name = "o0" }, .{ .name = "o1" } } };
            const plan = try q.MetricReadPlan.init(query);
            var work = try q.GraphQueryEngine.MetricStageWorkspace.init(a, plan, nodes.len);
            defer work.deinit();
            const Reader = struct {
                pub fn readDecodedColumns(_: *@This(), _: std.mem.Allocator, _: []const []const u8, _: []const []const u8, columns: []const []f64) !void {
                    for (columns) |column| @memset(column, 1);
                }
            };
            var reader = Reader{};
            const streamed = work.selectReading(&reader, query, true, nodes) catch |err| {
                try std.testing.expectEqual(nodes.len, work.rowCount());
                for (0..work.rowCount()) |i| try std.testing.expectEqual(i, work.rowAt(i));
                for (work.columns) |column| try std.testing.expect(column == null);
                return err;
            };
            try std.testing.expect(streamed);
            try std.testing.expectEqual(@as(usize, 5000 - (5000 + 12) / 13), work.rows.len);
            var selected: usize = 0;
            for (nodes, 0..) |node, i| if (node.table == null) {
                try std.testing.expectEqual(i, work.rows[selected]);
                selected += 1;
            };
            var columns: [4]q.GraphQueryEngine.MetricColumn = undefined;
            work.columnsFor(plan.projections.slice(), &columns);
            for (columns) |column| for (0..work.rows.len) |row| try std.testing.expectEqual(@as(?f64, 1), column.at(row));
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "lite typed score spans preserve binary keys namespaces missing deletions and overlays" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "typed-score.aflite");
    defer a.free(path);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var one: [8]u8 = undefined;
    var two: [8]u8 = undefined;
    std.mem.writeInt(u64, &one, @bitCast(@as(f64, 1)), .little);
    std.mem.writeInt(u64, &two, @bitCast(@as(f64, 2)), .little);
    const encoding = @import("../internal_keys.zig");
    const keys = [_][]const u8{ "a\x00b", "c", "deleted", "missing", "z" };
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(a);
    const docs = try a.alloc(native.DocumentMutation, 4);
    defer a.free(docs);
    const offsets = [_]usize{ 0, 1, 2, 4 };
    for (offsets) |i| {
        try encoded.appendSlice(a, "ns\x00p/");
        try encoding.appendEncodedComponent(&encoded, a, keys[i]);
    }
    var at: usize = 0;
    for (offsets, docs) |i, *doc| {
        const len = "ns\x00p/".len + encoding.encodedComponentLen(keys[i]);
        doc.* = .{ .key = encoded.items[at..][0..len], .value = if (i == 2) &.{} else if (i == 1) &two else &one, .is_delete = i == 2 };
        at += len;
    }
    try store.file.putDocumentBatch(docs);
    var txn = try backend_erased.readTxnFrom(a, try Txn.openReadWithPrefix(&store, "ns\x00"));
    defer txn.abort();
    var scope = try txn.openReadScope(a);
    defer scope.close();
    try std.testing.expect(scope.supportsSortedScoreColumn());
    var values: [5]f64 = undefined;
    for (0..2) |_| {
        _ = try scope.readSortedScoreColumn("p/", &keys, &values);
        try std.testing.expectEqual(@as(f64, 1), values[0]);
        try std.testing.expectEqual(@as(f64, 2), values[1]);
        try std.testing.expect(std.math.isNan(values[2]) and std.math.isNan(values[3]));
        try std.testing.expectEqual(@as(f64, 1), values[4]);
    }
    try std.testing.expectError(error.InvalidBatch, scope.readSortedScoreColumn("p/", &.{ "z", "a" }, values[0..2]));
    try std.testing.expect(std.math.isNan(values[0]) and std.math.isNan(values[1]));
    const Runner = struct {
        fn run(backing: Allocator, parent: *Txn) !void {
            var no_resize = @import("test_allocator.zig").NoResizeAllocator{ .backing = backing };
            var owned = try parent.openReadScope(no_resize.allocator());
            defer owned.close();
            owned.beginScoreBatch();
            var result: [5]f64 = undefined;
            _ = try owned.readSortedScoreColumn("p/", &.{ "a\x00b", "c", "deleted", "missing", "z" }, &result);
            try std.testing.expectEqual(@as(f64, 1), result[0]);
            try std.testing.expectEqual(@as(f64, 2), result[1]);
            try std.testing.expect(std.math.isNan(result[2]) and std.math.isNan(result[3]));
            try std.testing.expectEqual(@as(f64, 1), result[4]);
        }
    };
    // Force private scope ownership and relocation on every inventory run.
    var parent = try Txn.openReadWithPrefix(&store, "ns\x00");
    defer parent.abort();
    try Runner.run(a, &parent);
    try std.testing.checkAllAllocationFailures(a, Runner.run, .{&parent});
    const long_a = try a.alloc(u8, 530000);
    defer a.free(long_a);
    const long_b = try a.alloc(u8, 530000);
    defer a.free(long_b);
    @memset(long_a, 'a');
    @memset(long_b, 'b');
    var missing: [2]f64 = undefined;
    try std.testing.expectEqual(@as(usize, 2), try scope.readSortedScoreColumn("p/", &.{ long_a, long_b }, &missing));
    try std.testing.expect(std.math.isNan(missing[0]) and std.math.isNan(missing[1]));
    var write = try Txn.openWriteWithPrefix(&store, "ns\x00");
    defer write.abort();
    try write.put("p/c\x00\x00", &one);
    try write.delete("p/z\x00\x00");
    var pending = try write.openReadScope(a);
    defer pending.close();
    _ = try pending.readSortedScoreColumn("p/", &keys, &values);
    try std.testing.expectEqual(@as(f64, 1), values[1]);
    try std.testing.expect(std.math.isNan(values[4]));
}

test "lite typed score spans admit exact encoder after warmed graph scratch" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(a, tmp, "warm-span.aflite");
    defer a.free(path);
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = resource_manager_mod.Options.defaultBudgets() });
    defer manager.deinit(a);
    var store = try Store.createWithOptions(a, path, .{ .io = std.testing.io, .resource_manager = &manager, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, @bitCast(@as(f64, 1)), .little);
    try store.file.putDocumentBatch(&.{.{ .key = "00000000\x00\x00", .value = &raw }});
    const storage = try a.alloc([10]u8, 4096);
    defer a.free(storage);
    const keys = try a.alloc([]const u8, 4096);
    defer a.free(keys);
    const nodes = try a.alloc([]const u8, 4096);
    defer a.free(nodes);
    for (storage, keys, nodes, 0..) |*bytes, *key, *node, i| {
        _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{i});
        bytes[8] = 0;
        bytes[9] = 0;
        key.* = bytes;
        node.* = bytes[0..8];
    }
    var txn = try Txn.openRead(&store);
    defer txn.abort();
    var warm_query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer warm_query.deinit();
    var warm = try txn.openReadScope(warm_query.allocator());
    var ignored: [4096]?f64 = undefined;
    try warm.getManySortedScores(keys, &ignored);
    const w = warm.workspace.?;
    w.resolved_scores.deinit(w.optionalAllocator());
    _ = try w.graph.allocator().alloc(u8, 20 * 1024);
    _ = try w.graph.allocator().alloc(u8, 20 * 1024);
    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = w.live + 1024;
    warm.close();
    const limit = 400 * 1024;
    manager.slices[@backingInt(resource_manager_mod.Slice.dense_search_working_set)].budget.hard_limit_bytes = limit;
    var query = resource_manager_mod.BudgetedAllocator.init(&manager, .dense_search_working_set, a, 1);
    defer query.deinit();
    var scope = try txn.openReadScope(query.allocator());
    defer scope.close();
    var values: [4096]f64 = undefined;
    _ = try scope.readSortedScoreColumn("", nodes, &values);
    try std.testing.expectEqual(@as(f64, 1), values[0]);
    for (values[1..]) |value| try std.testing.expect(std.math.isNan(value));
}
