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

/// Non-owning index into the CUDA owner's canonical swap-remove buffer list.
/// All access shares that owner's mutex. Failure disables the index; it never
/// changes device ownership, byte budgets, or the allocator's fallback policy.
pub const Index = struct {
    const none = std.math.maxInt(usize);
    const max_entries = 16384;
    const max_sizes = 4096;
    const Slot = struct { len: usize, prev: usize = none, next: usize = none };
    const Bin = struct { head: usize };
    slots: std.ArrayListUnmanaged(Slot) = .empty,
    sizes: std.ArrayListUnmanaged(usize) = .empty,
    bins: std.AutoHashMapUnmanaged(usize, Bin) = .empty,
    bytes: usize = 0,
    valid: bool = false,
    queries: usize = 0,
    hits: usize = 0,
    rebuilds: usize = 0,

    pub fn deinit(self: *Index, allocator: std.mem.Allocator) void {
        self.slots.deinit(allocator);
        self.sizes.deinit(allocator);
        self.bins.deinit(allocator);
        self.* = .{};
    }

    pub fn invalidate(self: *Index) void {
        self.valid = false;
    }

    fn lowerBound(self: *const Index, len: usize) usize {
        var lo: usize = 0;
        var hi = self.sizes.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.sizes.items[mid] < len) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    fn append(self: *Index, allocator: std.mem.Allocator, len: usize) !void {
        if (self.slots.items.len >= max_entries) return error.IndexLimit;
        try self.slots.ensureUnusedCapacity(allocator, 1);
        if (!self.bins.contains(len)) {
            if (self.sizes.items.len >= max_sizes) return error.IndexLimit;
            try self.sizes.ensureUnusedCapacity(allocator, 1);
            try self.bins.put(allocator, len, .{ .head = none });
            self.sizes.insertAssumeCapacity(self.lowerBound(len), len);
        }
        const bin = self.bins.getPtr(len).?;
        const i = self.slots.items.len;
        if (bin.head != none) self.slots.items[bin.head].prev = i;
        self.slots.appendAssumeCapacity(.{ .len = len, .next = bin.head });
        bin.head = i;
        self.bytes += len;
    }

    pub fn ensure(self: *Index, allocator: std.mem.Allocator, buffers: anytype) bool {
        if (self.valid) {
            std.debug.assert(self.slots.items.len == buffers.len);
            return true;
        }
        self.slots.clearRetainingCapacity();
        self.sizes.clearRetainingCapacity();
        self.bins.clearRetainingCapacity();
        self.bytes = 0;
        for (buffers) |buffer| self.append(allocator, buffer.len) catch return false;
        self.valid = true;
        self.rebuilds += 1;
        return true;
    }

    /// Called only after the canonical list successfully appends its buffer.
    pub fn appended(self: *Index, allocator: std.mem.Allocator, len: usize) void {
        if (!self.valid) return;
        self.append(allocator, len) catch self.invalidate();
    }

    pub fn find(self: *Index, len: usize, exact: bool) ?usize {
        std.debug.assert(self.valid);
        self.queries += 1;
        const at = self.lowerBound(len);
        if (at == self.sizes.items.len) return null;
        const size = self.sizes.items[at];
        if (exact and size != len) return null;
        self.hits += 1;
        return self.bins.get(size).?.head;
    }

    /// Mirrors canonical swapRemove, including a last entry in the same bin.
    pub fn removed(self: *Index, i: usize) void {
        if (!self.valid) return;
        const old = self.slots.items[i];
        const bin = self.bins.getPtr(old.len).?;
        if (old.prev != none) self.slots.items[old.prev].next = old.next else bin.head = old.next;
        if (old.next != none) self.slots.items[old.next].prev = old.prev;
        if (bin.head == none) {
            _ = self.bins.remove(old.len);
            _ = self.sizes.orderedRemove(self.lowerBound(old.len));
        }
        const last = self.slots.items.len - 1;
        if (i != last) {
            const moved = self.slots.items[last];
            self.slots.items[i] = moved;
            if (moved.prev != none) self.slots.items[moved.prev].next = i else self.bins.getPtr(moved.len).?.head = i;
            if (moved.next != none) self.slots.items[moved.next].prev = i;
        }
        _ = self.slots.pop();
        self.bytes -= old.len;
    }
};

test "LightOnOCR3 CUDA temp index matches independent best fit through churn and rebuilds" {
    const allocator = std.testing.allocator;
    var index: Index = .{};
    defer index.deinit(allocator);
    const Buffer = struct { len: usize, identity: usize };
    var buffers: std.ArrayListUnmanaged(Buffer) = .empty;
    defer buffers.deinit(allocator);
    var rng = std.Random.DefaultPrng.init(1729);
    const random = rng.random();
    for (0..20000) |step| {
        try std.testing.expect(index.ensure(allocator, buffers.items));
        if (buffers.items.len == 0 or (buffers.items.len < 1024 and random.boolean())) {
            const len = random.uintLessThan(usize, 97) * 256;
            try buffers.append(allocator, .{ .len = len, .identity = step });
            index.appended(allocator, len);
        } else {
            const len = random.uintLessThan(usize, 105) * 256;
            const exact = random.boolean();
            var expected: ?usize = null;
            var bytes: usize = 0;
            for (buffers.items) |buffer| {
                bytes += buffer.len;
                if ((if (exact) buffer.len == len else buffer.len >= len) and
                    (expected == null or buffer.len < expected.?)) expected = buffer.len;
            }
            try std.testing.expectEqual(bytes, index.bytes);
            const found = index.find(len, exact);
            try std.testing.expectEqual(expected, if (found) |i| buffers.items[i].len else null);
            if (found) |i| {
                index.removed(i);
                _ = buffers.swapRemove(i);
            }
        }
        if (step % 113 == 0) {
            // Exercise the legacy ordered-removal / arena invalidation path.
            index.invalidate();
            if (buffers.items.len > 0) _ = buffers.orderedRemove(0);
        }
    }
    while (buffers.items.len > 0) {
        try std.testing.expect(index.ensure(allocator, buffers.items));
        const i = random.uintLessThan(usize, buffers.items.len);
        index.removed(i);
        _ = buffers.swapRemove(i);
    }
    try std.testing.expectEqual(@as(usize, 0), index.bytes);
    try std.testing.expectEqual(@as(usize, 0), index.bins.count());
    try std.testing.expectEqual(@as(usize, 0), index.sizes.items.len);
}

test "LightOnOCR3 CUDA temp index metadata failure leaves canonical ownership intact" {
    var index: Index = .{};
    defer index.deinit(std.testing.allocator);
    const buffers = [_]struct { len: usize }{.{ .len = 1024 }};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expect(!index.ensure(failing.allocator(), &buffers));
    try std.testing.expect(!index.valid);
    try std.testing.expectEqual(@as(usize, 1024), buffers[0].len);
    try std.testing.expect(index.ensure(std.testing.allocator, &buffers));
    try std.testing.expectEqual(@as(usize, 1024), index.bytes);
    try std.testing.expectEqual(@as(?usize, 0), index.find(1024, true));
}

test "LightOnOCR3 CUDA temp index bounds historical size metadata and fails closed at limits" {
    var index: Index = .{};
    defer index.deinit(std.testing.allocator);
    const Buffer = struct { len: usize };
    for (0..20000) |i| {
        const empty: []const Buffer = &.{};
        try std.testing.expect(index.ensure(std.testing.allocator, empty));
        index.appended(std.testing.allocator, i + 1);
        try std.testing.expect(index.valid);
        index.removed(0);
        try std.testing.expectEqual(@as(usize, 0), index.bins.count());
    }
    const buffers = try std.testing.allocator.alloc(Buffer, Index.max_entries + 1);
    defer std.testing.allocator.free(buffers);
    @memset(buffers, .{ .len = 64 });
    index.invalidate();
    try std.testing.expect(!index.ensure(std.testing.allocator, buffers));
    try std.testing.expect(!index.valid);
    try std.testing.expectEqual(@as(usize, 64), buffers[buffers.len - 1].len);
    index.invalidate();
    for (buffers[0 .. Index.max_sizes + 1], 0..) |*buffer, i| buffer.len = i;
    try std.testing.expect(!index.ensure(std.testing.allocator, buffers[0 .. Index.max_sizes + 1]));
    try std.testing.expect(!index.valid);
    try std.testing.expectEqual(Index.max_sizes, index.sizes.items.len);
}
