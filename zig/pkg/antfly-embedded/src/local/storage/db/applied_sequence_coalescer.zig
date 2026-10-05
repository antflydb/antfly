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

const std = @import("std");
const Allocator = std.mem.Allocator;
pub const flush_interval_ns: u64 = 100 * std.time.ns_per_ms;
const applied_sequence_flush_interval_ns = flush_interval_ns;

/// Volatile watermark batching; durable checkpoints and serialization stay with DB.
pub const Coalescer = struct {
    pending: std.StringHashMapUnmanaged(u64) = .empty,
    last_flush_ns: u64 = 0,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        self.clearPending(alloc);
        self.pending.deinit(alloc);
        self.* = .{};
    }

    pub fn note(self: *@This(), alloc: Allocator, index_name: []const u8, sequence: u64) !void {
        const gop = try self.pending.getOrPut(alloc, index_name);
        if (gop.found_existing) {
            gop.value_ptr.* = @max(gop.value_ptr.*, sequence);
            return;
        }
        errdefer _ = self.pending.remove(index_name);
        gop.key_ptr.* = try alloc.dupe(u8, index_name);
        gop.value_ptr.* = sequence;
    }

    pub fn shouldFlush(self: *const @This(), now_ns: u64) bool {
        if (self.pending.count() == 0) return false;
        return self.last_flush_ns == 0 or now_ns -| self.last_flush_ns >= applied_sequence_flush_interval_ns;
    }

    pub fn clearPending(self: *@This(), alloc: Allocator) void {
        var it = self.pending.iterator();
        while (it.next()) |entry| alloc.free(@constCast(entry.key_ptr.*));
        self.pending.clearRetainingCapacity();
    }

    pub fn removePending(self: *@This(), alloc: Allocator, index_name: []const u8) void {
        const removed = self.pending.fetchRemove(index_name) orelse return;
        alloc.free(@constCast(removed.key));
    }

    pub fn takePending(self: *@This(), index_name: []const u8) ?struct { owned_name: []const u8, sequence: u64 } {
        const removed = self.pending.fetchRemove(index_name) orelse return null;
        return .{
            .owned_name = @constCast(removed.key),
            .sequence = removed.value,
        };
    }
};

test "applied sequence coalescer keeps max sequence per index" {
    const alloc = std.testing.allocator;

    var coalescer = Coalescer{};
    defer coalescer.deinit(alloc);

    try coalescer.note(alloc, "dv_v1", 10);
    try coalescer.note(alloc, "dv_v1", 7);
    try coalescer.note(alloc, "ft_v1", 4);
    try coalescer.note(alloc, "dv_v1", 12);

    try std.testing.expectEqual(@as(u32, 2), coalescer.pending.count());
    try std.testing.expectEqual(@as(u64, 12), coalescer.pending.get("dv_v1").?);
    try std.testing.expectEqual(@as(u64, 4), coalescer.pending.get("ft_v1").?);
    try std.testing.expect(coalescer.shouldFlush(applied_sequence_flush_interval_ns));

    coalescer.clearPending(alloc);
    try std.testing.expectEqual(@as(u32, 0), coalescer.pending.count());
}

test "applied sequence coalescer takePending removes only requested index" {
    const alloc = std.testing.allocator;

    var coalescer = Coalescer{};
    defer coalescer.deinit(alloc);

    try coalescer.note(alloc, "dv_v1", 10);
    try coalescer.note(alloc, "ft_v1", 4);

    const removed = coalescer.takePending("dv_v1").?;
    defer alloc.free(removed.owned_name);
    try std.testing.expectEqualStrings("dv_v1", removed.owned_name);
    try std.testing.expectEqual(@as(u64, 10), removed.sequence);
    try std.testing.expectEqual(@as(u32, 1), coalescer.pending.count());
    try std.testing.expectEqual(@as(u64, 4), coalescer.pending.get("ft_v1").?);
    try std.testing.expect(coalescer.pending.get("dv_v1") == null);
}

test "applied sequence coalescer allocation failure leaves no borrowed key" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var coalescer: Coalescer = .{};
            defer coalescer.deinit(alloc);
            try coalescer.note(alloc, "index", 10);
            try coalescer.note(alloc, "index", 9);
            try std.testing.expectEqual(@as(u64, 10), coalescer.pending.get("index").?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

test "applied sequence coalescer cadence tolerates backwards clock and empty batches" {
    var coalescer: Coalescer = .{};
    defer coalescer.deinit(std.testing.allocator);
    try std.testing.expect(!coalescer.shouldFlush(100));
    try coalescer.note(std.testing.allocator, "index", 1);
    coalescer.last_flush_ns = 100;
    try std.testing.expect(!coalescer.shouldFlush(99));
    try std.testing.expect(!coalescer.shouldFlush(100 + flush_interval_ns - 1));
    try std.testing.expect(coalescer.shouldFlush(100 + flush_interval_ns));
    coalescer.removePending(std.testing.allocator, "index");
    try std.testing.expect(!coalescer.shouldFlush(std.math.maxInt(u64)));
}
