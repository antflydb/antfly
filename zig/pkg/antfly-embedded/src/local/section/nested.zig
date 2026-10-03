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

//! Nested document edge list section.
//!
//! Stores parent↔child document relationships for nested/join queries.
//!
//! Wire format:
//!   [numParents: u32 LE]
//!   For each parent:
//!     [parentDocID: u32 LE]
//!     [numChildren: u32 LE]
//!     [childDocID_0: u32 LE]
//!     [childDocID_1: u32 LE]
//!     ...

const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================================
// Writer
// ============================================================================

pub const NestedDocsWriter = struct {
    alloc: Allocator,
    parents: std.ArrayListUnmanaged(ParentEntry),

    const ParentEntry = struct {
        parent_id: u32,
        children: std.ArrayListUnmanaged(u32),
    };

    pub fn init(alloc: Allocator) NestedDocsWriter {
        return .{ .alloc = alloc, .parents = .empty };
    }

    pub fn deinit(self: *NestedDocsWriter) void {
        for (self.parents.items) |*p| p.children.deinit(self.alloc);
        self.parents.deinit(self.alloc);
    }

    /// Add a parent→child relationship.
    pub fn addChild(self: *NestedDocsWriter, parent_id: u32, child_id: u32) !void {
        // Find or create parent entry
        for (self.parents.items) |*p| {
            if (p.parent_id == parent_id) {
                try p.children.append(self.alloc, child_id);
                return;
            }
        }
        var entry = ParentEntry{ .parent_id = parent_id, .children = .empty };
        try entry.children.append(self.alloc, child_id);
        try self.parents.append(self.alloc, entry);
    }

    /// Build serialized nested docs section. Caller owns returned bytes.
    pub fn build(self: *NestedDocsWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        const num_parents: u32 = @intCast(self.parents.items.len);
        try appendU32LE(self.alloc, &out, num_parents);

        for (self.parents.items) |*p| {
            try appendU32LE(self.alloc, &out, p.parent_id);
            try appendU32LE(self.alloc, &out, @intCast(p.children.items.len));
            for (p.children.items) |child_id| {
                try appendU32LE(self.alloc, &out, child_id);
            }
        }

        return try self.alloc.dupe(u8, out.items);
    }
};

// ============================================================================
// Reader
// ============================================================================

pub const NestedDocsReader = struct {
    data: []const u8,
    num_parents: u32,

    pub fn init(data: []const u8) !NestedDocsReader {
        if (data.len < 4) return error.InvalidData;
        return .{
            .data = data,
            .num_parents = std.mem.readInt(u32, data[0..4], .little),
        };
    }

    /// Get children for a parent doc. Returns null if parent not found.
    pub fn getChildren(self: *const NestedDocsReader, parent_id: u32) ?[]const u32 {
        var pos: usize = 4;
        for (0..self.num_parents) |_| {
            const pid = std.mem.readInt(u32, self.data[pos..][0..4], .little);
            pos += 4;
            const num_children = std.mem.readInt(u32, self.data[pos..][0..4], .little);
            pos += 4;
            const children_start = pos;
            pos += @as(usize, num_children) * 4;

            if (pid == parent_id) {
                const bytes = self.data[children_start..pos];
                comptime std.debug.assert(@import("builtin").cpu.arch.endian() == .little);
                return @as([*]const u32, @ptrCast(@alignCast(bytes.ptr)))[0..num_children];
            }
        }
        return null;
    }

    /// Iterate all parent→children relationships.
    pub fn iterator(self: *const NestedDocsReader) ParentIterator {
        return .{ .data = self.data, .pos = 4, .remaining = self.num_parents };
    }
};

pub const ParentIterator = struct {
    data: []const u8,
    pos: usize,
    remaining: u32,

    pub const Entry = struct { parent_id: u32, num_children: u32, children_data: []const u8 };

    pub fn next(self: *ParentIterator) ?Entry {
        if (self.remaining == 0) return null;
        self.remaining -= 1;

        const parent_id = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        const num_children = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        const children_bytes = @as(usize, num_children) * 4;
        const children_data = self.data[self.pos..][0..children_bytes];
        self.pos += children_bytes;

        return .{ .parent_id = parent_id, .num_children = num_children, .children_data = children_data };
    }
};

fn appendU32LE(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), val: u32) !void {
    try out.appendSlice(alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, val))));
}

// ============================================================================
// Tests
// ============================================================================

test "nested docs round-trip" {
    const alloc = std.testing.allocator;
    var writer = NestedDocsWriter.init(alloc);
    defer writer.deinit();

    try writer.addChild(0, 1);
    try writer.addChild(0, 2);
    try writer.addChild(3, 4);

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try NestedDocsReader.init(data);
    try std.testing.expectEqual(@as(u32, 2), reader.num_parents);

    // Parent 0 has children 1, 2
    const children0 = reader.getChildren(0) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), children0.len);
    try std.testing.expectEqual(@as(u32, 1), children0[0]);
    try std.testing.expectEqual(@as(u32, 2), children0[1]);

    // Parent 3 has child 4
    const children3 = reader.getChildren(3) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 1), children3.len);
    try std.testing.expectEqual(@as(u32, 4), children3[0]);

    // Non-existent parent
    try std.testing.expect(reader.getChildren(99) == null);
}

test "nested docs iterator" {
    const alloc = std.testing.allocator;
    var writer = NestedDocsWriter.init(alloc);
    defer writer.deinit();

    try writer.addChild(10, 11);
    try writer.addChild(10, 12);
    try writer.addChild(20, 21);

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try NestedDocsReader.init(data);
    var iter = reader.iterator();

    const e0 = iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 10), e0.parent_id);
    try std.testing.expectEqual(@as(u32, 2), e0.num_children);

    const e1 = iter.next() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 20), e1.parent_id);
    try std.testing.expectEqual(@as(u32, 1), e1.num_children);

    try std.testing.expect(iter.next() == null);
}

test "nested docs empty" {
    const alloc = std.testing.allocator;
    var writer = NestedDocsWriter.init(alloc);
    defer writer.deinit();

    const data = try writer.build();
    defer alloc.free(data);

    const reader = try NestedDocsReader.init(data);
    try std.testing.expectEqual(@as(u32, 0), reader.num_parents);
}
