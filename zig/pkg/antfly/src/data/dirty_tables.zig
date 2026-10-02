// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: LicenseRef-Elastic-2.0

//! Consume dirty table names by swapping ownership, so notifications racing a
//! reconciliation pass remain queued for the following pass.
const std = @import("std");

pub const Queue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    pending: Batch = .{},
    const max_tables = 4096;

    pub const Batch = struct {
        names: std.StringHashMapUnmanaged(void) = .empty,

        pub fn contains(self: *const Batch, name: []const u8) bool {
            return self.names.contains(name);
        }

        pub fn count(self: *const Batch) usize {
            return self.names.count();
        }

        pub fn deinit(self: *Batch, alloc: std.mem.Allocator) void {
            var it = self.names.keyIterator();
            while (it.next()) |name| alloc.free(name.*);
            self.names.deinit(alloc);
            self.* = .{};
        }
    };

    fn lock(self: *Queue) void {
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
    }

    pub fn mark(self: *Queue, alloc: std.mem.Allocator, name: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.pending.contains(name)) return;
        if (self.pending.count() >= max_tables) return error.DirtyTableCapacityExceeded;
        const owned = try alloc.dupe(u8, name);
        errdefer alloc.free(owned);
        try self.pending.names.put(alloc, owned, {});
    }

    pub fn take(self: *Queue) Batch {
        self.lock();
        defer self.mutex.unlock();
        const batch = self.pending;
        self.pending = .{};
        return batch;
    }

    pub fn restore(self: *Queue, alloc: std.mem.Allocator, batch: *const Batch) !void {
        var it = batch.names.keyIterator();
        while (it.next()) |name| try self.mark(alloc, name.*);
    }

    pub fn deinit(self: *Queue, alloc: std.mem.Allocator) void {
        var batch = self.take();
        batch.deinit(alloc);
    }
};

test "dirty table consumption preserves racing writes and failed inspections" {
    const alloc = std.testing.allocator;
    var queue: Queue = .{};
    defer queue.deinit(alloc);
    try queue.mark(alloc, "active");
    try queue.mark(alloc, "active");
    var first = queue.take();
    defer first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), first.count());
    try std.testing.expect(!first.contains("idle"));
    // A new notification for the same table must survive completion of first.
    try queue.mark(alloc, "active");
    try queue.mark(alloc, "other");
    try queue.restore(alloc, &first);
    var second = queue.take();
    defer second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), second.count());
    try std.testing.expect(second.contains("active"));
    try std.testing.expect(second.contains("other"));
}

test "dirty table allocation failure leaves existing notifications intact" {
    const alloc = std.testing.allocator;
    var queue: Queue = .{};
    defer queue.deinit(alloc);
    try queue.mark(alloc, "existing");
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, queue.mark(failing.allocator(), "new"));
    var batch = queue.take();
    defer batch.deinit(alloc);
    try std.testing.expect(batch.contains("existing"));
    try std.testing.expect(!batch.contains("new"));
}
