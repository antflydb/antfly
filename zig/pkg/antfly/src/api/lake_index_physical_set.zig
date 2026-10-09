// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Query-owned compressed physical row sets shared by text/vector predicates.
//! File identity is kept once. Group and high row bits select a Roaring block;
//! native vector keys are tested without expanding matches into key/hash lists.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Bitmap = local.encoding_roaring.RoaringBitmap;
const Key = struct { group: u32, high: u32 };
const Blocks = std.AutoHashMapUnmanaged(Key, Bitmap);
pub const Set = struct {
    a: A,
    files: std.StringHashMapUnmanaged(Blocks) = .empty,
    pub fn init(a: A) Set {
        return .{ .a = a };
    }
    pub fn deinit(self: *Set) void {
        var files = self.files.iterator();
        while (files.next()) |file| {
            var blocks = file.value_ptr.valueIterator();
            while (blocks.next()) |bitmap| bitmap.deinit();
            file.value_ptr.deinit(self.a);
            self.a.free(file.key_ptr.*);
        }
        self.files.deinit(self.a);
    }
    pub fn addRow(self: *Set, file: []const u8, group: u32, row: u64) !void {
        if (!self.files.contains(file)) {
            const owned = try self.a.dupe(u8, file);
            errdefer self.a.free(owned);
            try self.files.put(self.a, owned, .empty);
        }
        const blocks = self.files.getPtr(file).?;
        const block = try blocks.getOrPut(self.a, .{ .group = group, .high = @intCast(row >> 32) });
        if (!block.found_existing) block.value_ptr.* = Bitmap.init(self.a);
        try block.value_ptr.add(@truncate(row));
    }
    pub fn addBlock(self: *Set, block: @import("lake_index_predicate_blocks.zig").Block) !void {
        if (!self.files.contains(block.file)) {
            const owned = try self.a.dupe(u8, block.file);
            errdefer self.a.free(owned);
            try self.files.put(self.a, owned, .empty);
        }
        const target = self.files.getPtr(block.file).?;
        const entry = try target.getOrPut(self.a, .{ .group = block.group, .high = @intCast(block.base >> 32) });
        if (!entry.found_existing) entry.value_ptr.* = Bitmap.init(self.a);
        const offset: u32 = @truncate(block.base);
        switch (block.selection) {
            .interval => |span| try entry.value_ptr.addRange(try std.math.add(u32, offset, span.lower), @as(u64, offset) + span.lower + span.count),
            .bitmap => |bitmap| {
                var shifted = try bitmap.addOffset(offset);
                defer shifted.deinit();
                try entry.value_ptr.orWith(&shifted);
            },
        }
    }
    /// Bound planning work without enumerating selected physical rows.
    pub fn boundedCardinality(self: *const Set, limit: usize) ?usize {
        var total: usize = 0;
        var files = self.files.valueIterator();
        while (files.next()) |blocks| {
            var values = blocks.valueIterator();
            while (values.next()) |bitmap| {
                const count = bitmap.cardinality();
                if (count > limit - total) return null;
                total += count;
            }
        }
        return total;
    }
    pub fn contains(self: *const Set, file: []const u8, group: u32, row: u64) bool {
        const blocks = self.files.getPtr(file) orelse return false;
        const bitmap = blocks.getPtr(.{ .group = group, .high = @intCast(row >> 32) }) orelse return false;
        return bitmap.contains(@truncate(row));
    }
    pub fn andWith(self: *Set, other: *const Set) void {
        var files = self.files.iterator();
        const empty = Bitmap.init(self.a);
        while (files.next()) |file| {
            const other_blocks = other.files.getPtr(file.key_ptr.*);
            var blocks = file.value_ptr.iterator();
            while (blocks.next()) |block| {
                const rhs = if (other_blocks) |rhs_blocks| rhs_blocks.getPtr(block.key_ptr.*) else null;
                block.value_ptr.andWith(rhs orelse &empty);
            }
        }
    }
    pub fn orWith(self: *Set, other: *const Set) !void {
        var files = other.files.iterator();
        while (files.next()) |file| {
            if (!self.files.contains(file.key_ptr.*)) {
                const owned = try self.a.dupe(u8, file.key_ptr.*);
                errdefer self.a.free(owned);
                try self.files.put(self.a, owned, .empty);
            }
            const target = self.files.getPtr(file.key_ptr.*).?;
            var blocks = file.value_ptr.iterator();
            while (blocks.next()) |block| {
                const entry = try target.getOrPut(self.a, block.key_ptr.*);
                if (!entry.found_existing) entry.value_ptr.* = Bitmap.init(self.a);
                try entry.value_ptr.orWith(block.value_ptr);
            }
        }
    }
};

test "external lake physical sets intersect and union across files groups and 64-bit row blocks" {
    const a = std.testing.allocator;
    var left = Set.init(a);
    defer left.deinit();
    var right = Set.init(a);
    defer right.deinit();
    try left.addRow("file", 3, 1 << 40);
    try left.addRow("file", 3, 7);
    try right.addRow("file", 3, 1 << 40);
    try right.addRow("other", 0, 7);
    left.andWith(&right);
    try std.testing.expect(!left.contains("file", 3, 7));
    try std.testing.expect(left.contains("file", 3, 1 << 40));
    try left.orWith(&right);
    try std.testing.expect(left.contains("other", 0, 7));
    try std.testing.expect(!left.contains("file", 0, 7));
}
