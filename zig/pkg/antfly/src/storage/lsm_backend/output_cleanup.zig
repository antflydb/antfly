// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Allocate cleanup ownership before creating an SST. Abandonment is an
//! allocation-free queue transfer, never filesystem I/O from a destructor.
//! The backend transfers queued paths to its durable obsolete ledger before
//! freeing tickets; failed admission leaves the original ownership intact.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const sync = @import("antfly_platform").sync;

pub const Queue = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Ticket = null,
    tail: ?*Ticket = null,
    pending: std.atomic.Value(usize) = .init(0),
    live: std.atomic.Value(usize) = .init(0),
    bytes: std.atomic.Value(u64) = .init(0),
    manager: ?*resources.ResourceManager = null,
    wake_context: ?*anyopaque = null,
    wake_fn: ?*const fn (*anyopaque) void = null,

    /// Reserved ticket allocations have a single accounting owner. Without
    /// a manager, include them in observational estimates for diagnostics.
    pub fn observedMemoryBytes(self: *const Queue) u64 {
        return @sizeOf(Queue) +| if (self.manager == null) self.bytes.load(.acquire) else 0;
    }

    pub fn create(self: *Queue, path: []const u8) !*Ticket {
        var reservation: ?resources.Reservation = null;
        errdefer if (reservation) |*lease| lease.release();
        const bytes = @sizeOf(Ticket) + path.len;
        if (self.manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, bytes);
        const owned = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned);
        const ticket = try self.allocator.create(Ticket);
        ticket.* = .{ .queue = self, .path = owned, .reservation = reservation };
        _ = self.live.fetchAdd(1, .monotonic);
        _ = self.bytes.fetchAdd(bytes, .monotonic);
        return ticket;
    }

    pub fn append(self: *Queue, ticket: *Ticket) void {
        ticket.next = null;
        sync.lockYielding(&self.mutex);
        if (self.tail) |tail| tail.next = ticket else self.head = ticket;
        self.tail = ticket;
        _ = self.pending.fetchAdd(1, .release);
        self.mutex.unlock();
        if (self.wake_fn) |wake| wake(self.wake_context.?);
    }

    pub fn pop(self: *Queue) ?*Ticket {
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const ticket = self.head orelse return null;
        self.head = ticket.next;
        if (self.head == null) self.tail = null;
        _ = self.pending.fetchSub(1, .release);
        ticket.next = null;
        return ticket;
    }

    /// Used only after all builders/owners stop, including simulated crashes.
    /// Normal sync first transfers every pending ticket to the durable ledger.
    pub fn deinit(self: *Queue) void {
        while (self.pop()) |ticket| ticket.destroy();
        std.debug.assert(self.live.load(.acquire) == 0);
    }
};

pub const Ticket = struct {
    queue: *Queue,
    path: []u8,
    next: ?*Ticket = null,
    reservation: ?resources.Reservation = null,

    pub fn abandon(self: *Ticket) void {
        self.queue.append(self);
    }

    pub fn destroy(self: *Ticket) void {
        const queue = self.queue;
        _ = queue.live.fetchSub(1, .monotonic);
        _ = queue.bytes.fetchSub(@sizeOf(Ticket) + self.path.len, .monotonic);
        if (self.reservation) |*lease| lease.release();
        queue.allocator.free(self.path);
        queue.allocator.destroy(self);
    }
};

test "output cleanup tickets own paths through allocation failures and shared run retirement" {
    const Fixture = struct {
        fn check(allocator: std.mem.Allocator, commit: bool) !void {
            var manager = resources.ResourceManager.init(.{});
            defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
            var queue = Queue{ .allocator = allocator, .manager = &manager };
            defer queue.deinit();
            const ticket = try queue.create("unpublished.sst");
            var run: @import("repository.zig").Run = .{ .id = 1, .level = 0, .size_bytes = 1, .path = @constCast("unpublished.sst"), .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false, .output_ticket = ticket };
            var adopted = false;
            defer if (!adopted) run.deinit(allocator);
            var store: @import("run_store.zig").Store = .{};
            defer store.deinit(allocator);
            try store.append(allocator, run);
            adopted = true;
            var caller = store.at(0).retainOwned();
            defer caller.deinit(allocator);
            if (commit) caller.commitOutput() else try std.testing.expect(caller.abandonOutput());
            try std.testing.expectEqual(@as(usize, if (commit) 0 else 1), queue.pending.load(.acquire));
            // The candidate may remain pinned, but an abandoned file already
            // has an independent cleanup owner; it does not await retirement.
            if (!commit) {
                const pending = queue.pop().?;
                try std.testing.expectEqualStrings("unpublished.sst", pending.path);
                pending.destroy();
            }
        }
    };
    for ([_]bool{ false, true }) |commit| try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{commit});
}

test "output cleanup charges aggregate memory exactly once across ownership transitions" {
    const Backend = @import("../lsm_backend.zig").Backend;
    var manager = resources.ResourceManager.init(.{});
    var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager });
    defer backend.close();
    try backend.initOutputCleanup();
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
    const baseline = manager.snapshot().memory.used_bytes;
    const queue = backend.options.unpublished_outputs.?;
    for ([_]bool{ false, true }) |abandon| {
        const ticket = try queue.create("accounted-output.tbl");
        const bytes = queue.bytes.load(.acquire);
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        try std.testing.expectEqual(baseline + bytes, manager.snapshot().memory.used_bytes);
        if (abandon) {
            ticket.abandon();
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            try std.testing.expectEqual(baseline + bytes, manager.snapshot().memory.used_bytes);
            try std.testing.expectEqual(ticket, queue.pop().?);
        }
        ticket.destroy();
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        try std.testing.expectEqual(baseline, manager.snapshot().memory.used_bytes);
    }
}

test "output cleanup persists failed deletes and retries off-lock after reopen" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Memory = @import("storage_io.zig").MemoryStorage;
    const compaction = @import("compaction.zig");
    const allocator = std.testing.allocator;
    const Hook = struct {
        var backend: ?*Backend = null;
        var target: []const u8 = "";
        var attempts: usize = 0;
        var failures: bool = true;
        fn delete(ptr: *anyopaque, path: []const u8) !void {
            if (std.mem.eql(u8, path, target)) {
                if (backend) |current| {
                    try std.testing.expect(current.mu.tryLock());
                    current.mu.unlock();
                }
                attempts += 1;
                if (failures) return error.InjectedDeleteFailure;
            }
            const memory: *Memory = @ptrCast(@alignCast(ptr));
            return memory.storage().vtable.delete_file_absolute(ptr, path);
        }
    };
    var memory = Memory.init(allocator);
    defer memory.deinit();
    var storage = memory.storage();
    var vtable = storage.vtable.*;
    vtable.delete_file_absolute = Hook.delete;
    storage.vtable = &vtable;
    const options: @import("../lsm_backend.zig").Options = .{ .storage = storage, .wal_enabled = false, .obsolete_retention_ns = 0, .obsolete_delete_retry_ns = 0 };
    var backend = try Backend.open(allocator, "/output-cleanup-retry", options);
    var opened = true;
    defer if (opened) backend.close();
    Hook.backend = &backend;
    Hook.attempts = 0;
    Hook.failures = true;
    defer {
        Hook.backend = null;
        Hook.target = "";
    }
    var state: @import("state.zig").State = .{};
    defer state.deinit(allocator);
    try state.upsert(allocator, .{ .name = "docs" }, "a", "one", false);
    var path: []u8 = undefined;
    {
        try std.testing.expect(backend.mu.tryLock());
        defer backend.mu.unlock();
        var outputs = try compaction.makeRunsFromStateBorrowed(Backend, &backend, &state);
        errdefer compaction.discardOutputRuns(Backend, &backend, &outputs);
        try std.testing.expect(outputs.items[0].output_ticket != null);
        path = try allocator.dupe(u8, outputs.items[0].path.?);
        Hook.target = path;
        compaction.discardOutputRuns(Backend, &backend, &outputs);
        try std.testing.expectEqual(@as(usize, 0), Hook.attempts);
        try std.testing.expectEqual(@as(usize, 1), backend.options.unpublished_outputs.?.pending.load(.acquire));
    }
    defer allocator.free(path);
    backend.manifest_admitted_wire_bytes = @import("repository.zig").maxManifestReadBytes();
    try std.testing.expectError(error.ResourceBudgetExceeded, backend.sync(true));
    backend.manifest_admitted_wire_bytes = 0;
    try std.testing.expectEqual(@as(usize, 1), backend.options.unpublished_outputs.?.pending.load(.acquire));
    try backend.sync(true);
    try std.testing.expect(Hook.attempts != 0);
    try std.testing.expect(backend.obsolete_paths.contains(path));
    try std.testing.expectEqual(@as(usize, 0), backend.options.unpublished_outputs.?.pending.load(.acquire));
    _ = try storage.fileSize(path);
    Hook.backend = null;
    backend.abandonAfterCrash();
    opened = false;
    backend = try Backend.open(allocator, "/output-cleanup-retry", options);
    opened = true;
    Hook.backend = &backend;
    try std.testing.expect(backend.obsolete_paths.contains(path));
    Hook.failures = false;
    backend.obsolete_reclaim_retry_at_ns = 0;
    for (0..8) |_| {
        _ = try backend.runMaintenanceStep();
        if (!backend.obsolete_paths.contains(path)) break;
    }
    try std.testing.expect(!backend.obsolete_paths.contains(path));
    try std.testing.expectError(error.FileNotFound, storage.fileSize(path));
}

test "output cleanup owns partial outputs after cancellation and wakes bulk maintenance" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Memory = @import("storage_io.zig").MemoryStorage;
    const compaction = @import("compaction.zig");
    const allocator = std.testing.allocator;
    const Hook = struct {
        var tables: usize = 0;
        fn rename(ptr: *anyopaque, old: []const u8, new: []const u8) !void {
            const memory: *Memory = @ptrCast(@alignCast(ptr));
            try memory.storage().vtable.rename_absolute(ptr, old, new);
            if (std.mem.endsWith(u8, new, ".tbl")) {
                tables += 1;
                if (tables == 2) return error.Cancelled;
            }
        }
    };
    Hook.tables = 0;
    var memory = Memory.init(allocator);
    defer memory.deinit();
    var storage = memory.storage();
    var vtable = storage.vtable.*;
    vtable.rename_absolute = Hook.rename;
    storage.vtable = &vtable;
    var backend = try Backend.open(allocator, "/output-cleanup-cancel", .{ .storage = storage, .wal_enabled = false, .run_partition_prefix_bytes = 1 });
    defer backend.close();
    try backend.beginBulkIngestSession();
    var state: @import("state.zig").State = .{};
    defer state.deinit(allocator);
    for ([_][]const u8{ "a", "b", "c" }) |key| try state.upsert(allocator, .{ .name = "docs" }, key, "value", false);
    try std.testing.expectError(error.Cancelled, compaction.makeRunsFromStateBorrowed(Backend, &backend, &state));
    try std.testing.expectEqual(@as(usize, 2), Hook.tables);
    const queue = backend.options.unpublished_outputs.?;
    try std.testing.expectEqual(@as(usize, 2), queue.pending.load(.acquire));
    try std.testing.expectEqual(@as(?u64, 0), backend.nextMaintenanceWakeDelayNsBestEffort());
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), backend.obsolete_paths.count());
    // Bulk mode may defer physical deletion, but cannot lose cleanup intent.
    var cursor = backend.obsolete_paths.nextDueAfter(storage.nowNs(), null).?;
    _ = try storage.fileSize(cursor.path);
    cursor = backend.obsolete_paths.nextDueAfter(storage.nowNs(), cursor.path).?;
    _ = try storage.fileSize(cursor.path);
}

test "output cleanup off-lock handoff scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const Backend = @import("../lsm_backend.zig").Backend;
    const Run = @import("repository.zig").Run;
    const compaction = @import("compaction.zig");
    const allocator = std.testing.allocator;
    for ([_]usize{ 1000, 10000, 50000 }) |count| {
        var backend = Backend.init(allocator, .{ .wal_enabled = false });
        defer backend.close();
        try backend.initOutputCleanup();
        var outputs: std.ArrayListUnmanaged(Run) = .empty;
        defer compaction.discardOutputRuns(Backend, &backend, &outputs);
        try outputs.ensureTotalCapacity(allocator, count);
        for (0..count) |i| {
            const ticket = try backend.options.unpublished_outputs.?.create("unpublished.sst");
            outputs.appendAssumeCapacity(.{ .id = i + 1, .level = 0, .size_bytes = 1, .path = @constCast("unpublished.sst"), .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false, .output_ticket = ticket });
        }
        try std.testing.expect(backend.mu.tryLock());
        const start = std.Io.Clock.awake.now(std.testing.io);
        compaction.discardOutputRunsLocked(Backend, &backend, &outputs);
        const elapsed = start.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds();
        backend.mu.unlock();
        const queue = backend.options.unpublished_outputs.?;
        try std.testing.expectEqual(count, queue.pending.load(.acquire));
        std.debug.print("\nLSM cleanup outputs={d} offlock_handoff_ns={d} ticket_bytes={d}\n", .{ count, elapsed, queue.bytes.load(.acquire) });
        // This rootless fixture measures only the ownership handoff, not
        // durable ledger admission or physical deletion.
        while (queue.pop()) |ticket| ticket.destroy();
    }
}
