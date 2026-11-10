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

//! Buffer-cache metadata with constant-time byte accounting. The caller owns
//! synchronization and physical retirement. Array order and reuse policy match
//! the ordinary ArrayList cache; no buffers are reserved for a particular use.
const std = @import("std");

pub fn Cache(comptime Buffer: type) type {
    return struct {
        const Self = @This();
        buffers: std.ArrayListUnmanaged(Buffer) = .empty,
        bytes: usize = 0,
        pub const empty: Self = .{};

        pub fn ensureUnusedCapacity(self: *Self, allocator: std.mem.Allocator, count: usize) !void {
            try self.buffers.ensureUnusedCapacity(allocator, count);
        }
        pub fn append(self: *Self, allocator: std.mem.Allocator, buffer: Buffer) !void {
            const total = try std.math.add(usize, self.bytes, buffer.len);
            try self.buffers.append(allocator, buffer);
            self.bytes = total;
        }
        pub fn appendAssumeCapacity(self: *Self, buffer: Buffer) void {
            std.debug.assert(buffer.len <= std.math.maxInt(usize) - self.bytes);
            self.buffers.appendAssumeCapacity(buffer);
            self.bytes += buffer.len;
        }
        pub fn swapRemove(self: *Self, index: usize) Buffer {
            const result = self.buffers.swapRemove(index);
            self.bytes -= result.len;
            return result;
        }
        pub fn orderedRemove(self: *Self, index: usize) Buffer {
            const result = self.buffers.orderedRemove(index);
            self.bytes -= result.len;
            return result;
        }
        pub fn pop(self: *Self) ?Buffer {
            const result = self.buffers.pop() orelse return null;
            self.bytes -= result.len;
            return result;
        }
        pub fn clearRetainingCapacity(self: *Self) void {
            self.buffers.clearRetainingCapacity();
            self.bytes = 0;
        }
        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.buffers.deinit(allocator);
            self.* = .empty;
        }
    };
}

const FakeBuffer = struct { id: usize, len: usize };

test "LightOnOCR3 CUDA buffer cache preserves ArrayList reuse order and byte totals through mixed mutations" {
    var cache: Cache(FakeBuffer) = .empty;
    defer cache.deinit(std.testing.allocator);
    var reference: std.ArrayListUnmanaged(FakeBuffer) = .empty;
    defer reference.deinit(std.testing.allocator);
    var seed: u64 = 23;
    var next: usize = 0;
    for (0..2000) |_| {
        seed = seed *% 6364136223846793005 +% 1;
        if (reference.items.len < 2 or seed % 3 == 0) {
            const item = FakeBuffer{ .id = next, .len = (next + 1) * 17 };
            if (seed & 8 != 0) {
                try cache.ensureUnusedCapacity(std.testing.allocator, 1);
                cache.appendAssumeCapacity(item);
            } else try cache.append(std.testing.allocator, item);
            try reference.append(std.testing.allocator, item);
            next += 1;
        } else {
            const index = seed % reference.items.len;
            const got = if (seed & 16 != 0) cache.swapRemove(index) else cache.orderedRemove(index);
            const want = if (seed & 16 != 0) reference.swapRemove(index) else reference.orderedRemove(index);
            try std.testing.expectEqualDeep(want, got);
        }
        try std.testing.expectEqualSlices(FakeBuffer, reference.items, cache.buffers.items);
        var expected_bytes: usize = 0;
        for (reference.items) |item| expected_bytes += item.len;
        try std.testing.expectEqual(expected_bytes, cache.bytes);
    }
    while (cache.pop()) |got| try std.testing.expectEqualDeep(reference.pop().?, got);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expectEqual(@as(usize, 0), reference.items.len);
}

test "LightOnOCR3 CUDA buffer cache allocation failures and overflow leave existing ownership intact" {
    var cache: Cache(FakeBuffer) = .empty;
    defer cache.deinit(std.testing.allocator);
    try std.testing.expectError(error.OutOfMemory, cache.append(std.testing.failing_allocator, .{ .id = 1, .len = 10 }));
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.buffers.items.len);
    try cache.append(std.testing.allocator, .{ .id = 1, .len = std.math.maxInt(usize) - 2 });
    try std.testing.expectError(error.Overflow, cache.append(std.testing.allocator, .{ .id = 2, .len = 3 }));
    try std.testing.expectEqual(@as(usize, 1), cache.buffers.items.len);
    try std.testing.expectEqual(@as(usize, std.math.maxInt(usize) - 2), cache.bytes);
    const capacity = cache.buffers.capacity;
    cache.clearRetainingCapacity();
    try std.testing.expectEqual(capacity, cache.buffers.capacity);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try cache.append(std.testing.allocator, .{ .id = 3, .len = 31 });
    try std.testing.expectEqual(@as(usize, 31), cache.bytes);
}
