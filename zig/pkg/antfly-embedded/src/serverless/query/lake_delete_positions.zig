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

//! Immutable 64-bit position membership. High bits select shared Roaring
//! blocks; canonical fingerprint state is computed once before publication.
const std = @import("std");
const Bitmap = @import("../../encoding/roaring.zig").RoaringBitmap;
const A = std.mem.Allocator;
pub const Set = struct {
    blocks: std.AutoHashMapUnmanaged(u32, Bitmap) = .empty,
    highs: []u32 = &.{},
    count: usize = 0,
    maximum: u64 = 0,
    fingerprint: ?std.crypto.hash.sha2.Sha256 = null,
    pub fn deinit(self: *Set, a: A) void {
        var values = self.blocks.valueIterator();
        while (values.next()) |value| value.deinit();
        self.blocks.deinit(a);
        a.free(self.highs);
    }
    pub fn add(self: *Set, a: A, ordinal: u64) !void {
        std.debug.assert(self.fingerprint == null);
        const slot = try self.blocks.getOrPut(a, @intCast(ordinal >> 32));
        if (!slot.found_existing) slot.value_ptr.* = Bitmap.init(a);
        const low: u32 = @truncate(ordinal);
        if (slot.value_ptr.contains(low)) return;
        try slot.value_ptr.add(low);
        self.count += 1;
        self.maximum = @max(self.maximum, ordinal);
    }
    pub fn contains(self: *const Set, ordinal: u64) bool {
        const block = self.blocks.getPtr(@intCast(ordinal >> 32)) orelse return false;
        return block.contains(@truncate(ordinal));
    }
    pub fn merge(self: *Set, a: A, other: *const Set) !void {
        std.debug.assert(self.fingerprint == null);
        var blocks = other.blocks.iterator();
        while (blocks.next()) |entry| {
            const slot = try self.blocks.getOrPut(a, entry.key_ptr.*);
            if (!slot.found_existing) slot.value_ptr.* = Bitmap.init(a);
            const before = slot.value_ptr.cardinality();
            try slot.value_ptr.orWith(entry.value_ptr);
            self.count += slot.value_ptr.cardinality() - before;
        }
        self.maximum = @max(self.maximum, other.maximum);
    }
    pub fn seal(self: *Set, a: A) !void {
        if (self.fingerprint != null) return;
        self.highs = try a.alloc(u32, self.blocks.count());
        var keys = self.blocks.keyIterator();
        for (self.highs) |*high| high.* = keys.next().?.*;
        std.mem.sort(u32, self.highs, {}, std.sort.asc(u32));
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("native-lake-file-deletes-v1");
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, self.count, .little);
        hash.update(&bytes);
        for (self.highs) |high| {
            const block = self.blocks.getPtr(high).?;
            try block.prepareRead();
            var iterator = block.iterator();
            while (iterator.next()) |low| {
                std.mem.writeInt(u64, &bytes, (@as(u64, high) << 32) | low, .little);
                hash.update(&bytes);
            }
        }
        self.fingerprint = hash;
    }
};
test "external lake metadata navigation shared positions retain high bits canonical unions and compact dense membership" {
    const a = std.testing.allocator;
    var first: Set = .{};
    defer first.deinit(a);
    var second: Set = .{};
    defer second.deinit(a);
    for (0..10000) |i| try first.add(a, i);
    try first.add(a, (@as(u64, 1) << 40) + 9);
    try first.add(a, 9);
    try first.seal(a);
    try std.testing.expectEqual(@as(usize, 10001), first.count);
    try std.testing.expect(first.contains((@as(u64, 1) << 40) + 9));
    try std.testing.expect(!first.contains((@as(u64, 1) << 32) + 9));
    try second.merge(a, &first);
    try second.merge(a, &first);
    try second.seal(a);
    try std.testing.expectEqual(first.count, second.count);
    var first_hash = first.fingerprint.?;
    var second_hash = second.fingerprint.?;
    const first_digest = first_hash.finalResult();
    try std.testing.expectEqualSlices(u8, &first_digest, &second_hash.finalResult());
    // Publication IDs remain compatible with the former sorted-u64 recipe.
    var legacy = std.crypto.hash.sha2.Sha256.init(.{});
    legacy.update("native-lake-file-deletes-v1");
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, 10001, .little);
    legacy.update(&bytes);
    for (0..10000) |i| {
        std.mem.writeInt(u64, &bytes, i, .little);
        legacy.update(&bytes);
    }
    std.mem.writeInt(u64, &bytes, (@as(u64, 1) << 40) + 9, .little);
    legacy.update(&bytes);
    try std.testing.expectEqualSlices(u8, &legacy.finalResult(), &first_digest);
}
