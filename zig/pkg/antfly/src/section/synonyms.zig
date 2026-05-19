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

//! Synonym section using Vellum FST and Roaring bitmaps.
//!
//! Maps synonym terms to synonym groups. Each group contains a set of
//! equivalent term IDs (offsets into the main inverted index FST).
//!
//! Wire format:
//!   [numGroups: u32 LE]
//!   [group_0: roaring bitmap bytes (length-prefixed)]
//!   [group_1: roaring bitmap bytes (length-prefixed)]
//!   ...
//!   [vellumLen: u32 LE]
//!   [vellum FST data]  — maps synonym term → group ID
//!
//! Usage: look up a query term in the synonym FST. If found, get the
//! group ID, then load the roaring bitmap for that group to get all
//! equivalent term IDs.

const std = @import("std");
const Allocator = std.mem.Allocator;
const vellum = @import("antfly_vellum");
const roaring = @import("../encoding/roaring.zig");

// ============================================================================
// Writer
// ============================================================================

pub const SynonymWriter = struct {
    alloc: Allocator,
    groups: std.ArrayListUnmanaged(Group),
    term_to_group: std.StringHashMapUnmanaged(u32),

    const Group = struct {
        term_ids: std.ArrayListUnmanaged(u32),
    };

    pub fn init(alloc: Allocator) SynonymWriter {
        return .{
            .alloc = alloc,
            .groups = .empty,
            .term_to_group = .empty,
        };
    }

    pub fn deinit(self: *SynonymWriter) void {
        for (self.groups.items) |*g| g.term_ids.deinit(self.alloc);
        self.groups.deinit(self.alloc);
        var it = self.term_to_group.keyIterator();
        while (it.next()) |k| self.alloc.free(k.*);
        self.term_to_group.deinit(self.alloc);
    }

    /// Add a synonym group. All terms in the group are considered equivalent.
    /// `terms` are the synonym strings, `term_ids` are their corresponding
    /// inverted index term IDs (for fast bitmap lookup).
    pub fn addGroup(self: *SynonymWriter, terms: []const []const u8, term_ids: []const u32) !void {
        const group_id: u32 = @intCast(self.groups.items.len);

        var group = Group{ .term_ids = .empty };
        try group.term_ids.appendSlice(self.alloc, term_ids);
        try self.groups.append(self.alloc, group);

        for (terms) |term| {
            const key = try self.alloc.dupe(u8, term);
            try self.term_to_group.put(self.alloc, key, group_id);
        }
    }

    /// Build serialized synonym section. Caller owns returned bytes.
    pub fn build(self: *SynonymWriter) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(self.alloc);

        // 1. Number of groups
        const num_groups: u32 = @intCast(self.groups.items.len);
        try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, num_groups))));

        // 2. Each group as length-prefixed roaring bitmap
        for (self.groups.items) |*group| {
            var bitmap = roaring.RoaringBitmap.init(self.alloc);
            defer bitmap.deinit();
            for (group.term_ids.items) |tid| try bitmap.add(tid);

            const bitmap_bytes = try bitmap.toBytes(self.alloc);
            defer self.alloc.free(bitmap_bytes);

            try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(bitmap_bytes.len))))));
            try out.appendSlice(self.alloc, bitmap_bytes);
        }

        // 3. Build FST: term → group_id (sorted insertion required)
        const term_count = self.term_to_group.count();
        const sorted_terms = try self.alloc.alloc([]const u8, term_count);
        defer self.alloc.free(sorted_terms);
        {
            var it = self.term_to_group.keyIterator();
            var i: usize = 0;
            while (it.next()) |k| {
                sorted_terms[i] = k.*;
                i += 1;
            }
        }
        std.mem.sort([]const u8, sorted_terms, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);

        var fst_builder = try vellum.Builder.init(self.alloc, .{});
        defer fst_builder.deinit();
        for (sorted_terms) |term| {
            const gid = self.term_to_group.get(term).?;
            try fst_builder.insert(term, gid);
        }
        const fst_data = try fst_builder.finish();
        defer self.alloc.free(fst_data);

        // 4. Write FST length + data
        try out.appendSlice(self.alloc, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(fst_data.len))))));
        try out.appendSlice(self.alloc, fst_data);

        return try self.alloc.dupe(u8, out.items);
    }
};

// ============================================================================
// Reader
// ============================================================================

pub const SynonymReader = struct {
    alloc: Allocator,
    data: []const u8,
    num_groups: u32,
    groups_data_start: usize,
    group_offsets: []usize, // start offset of each group's bitmap data
    fst: vellum.FST,

    pub fn init(alloc: Allocator, data: []const u8) !SynonymReader {
        if (data.len < 4) return error.InvalidData;
        const num_groups = std.mem.readInt(u32, data[0..4], .little);

        // Parse group offsets
        var group_offsets = try alloc.alloc(usize, num_groups);
        errdefer alloc.free(group_offsets);

        var pos: usize = 4;
        for (0..num_groups) |i| {
            group_offsets[i] = pos;
            const bitmap_len = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4 + bitmap_len;
        }

        // Read FST
        const fst_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        const fst = try vellum.FST.load(data[pos..][0..fst_len]);

        return .{
            .alloc = alloc,
            .data = data,
            .num_groups = num_groups,
            .groups_data_start = 4,
            .group_offsets = group_offsets,
            .fst = fst,
        };
    }

    pub fn deinit(self: *SynonymReader) void {
        self.alloc.free(self.group_offsets);
    }

    /// Look up synonyms for a term. Returns the group's term IDs as a roaring bitmap.
    /// Caller must deinit the returned bitmap.
    pub fn lookup(self: *const SynonymReader, term: []const u8) !?roaring.RoaringBitmap {
        const result = self.fst.get(term) catch return null;
        if (!result.found) return null;
        return try self.getGroup(@intCast(result.val));
    }

    /// Expand a term to all its synonym strings (including itself).
    /// Returns null if the term has no synonyms. Caller owns returned slices.
    pub fn expandTerm(self: *const SynonymReader, alloc: Allocator, term: []const u8) !?[][]const u8 {
        const result = self.fst.get(term) catch return null;
        if (!result.found) return null;
        const group_id: u32 = @intCast(result.val);

        // Iterate the entire FST to find all terms with the same group_id
        var terms = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (terms.items) |t| alloc.free(t);
            terms.deinit(alloc);
        }

        var it = try self.fst.iterator(alloc, null, null);
        defer it.deinit();

        while (true) {
            const entry = it.current() orelse break;
            if (@as(u32, @intCast(entry.val)) == group_id) {
                try terms.append(alloc, try alloc.dupe(u8, entry.key));
            }
            _ = try it.nextEntry();
        }

        if (terms.items.len == 0) return null;
        const owned = try alloc.dupe([]const u8, terms.items);
        terms.deinit(alloc);
        return owned;
    }

    /// Get a synonym group's bitmap by group ID.
    fn getGroup(self: *const SynonymReader, group_id: u32) !roaring.RoaringBitmap {
        if (group_id >= self.num_groups) return error.InvalidGroup;
        const pos = self.group_offsets[group_id];
        const bitmap_len = std.mem.readInt(u32, self.data[pos..][0..4], .little);
        const bitmap_data = self.data[pos + 4 ..][0..bitmap_len];
        return roaring.RoaringBitmap.fromBytes(self.alloc, bitmap_data);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "synonym round-trip" {
    const alloc = std.testing.allocator;
    var writer = SynonymWriter.init(alloc);
    defer writer.deinit();

    // Group 0: "fast" and "quick" map to term IDs 10, 20
    try writer.addGroup(&.{ "fast", "quick" }, &.{ 10, 20 });
    // Group 1: "big" and "large" map to term IDs 30, 40, 50
    try writer.addGroup(&.{ "big", "large" }, &.{ 30, 40, 50 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try SynonymReader.init(alloc, data);
    defer reader.deinit();

    // Look up "fast" → should get group with IDs 10, 20
    var bitmap1 = (try reader.lookup("fast")) orelse return error.TestExpectedEqual;
    defer bitmap1.deinit();
    try std.testing.expect(bitmap1.contains(10));
    try std.testing.expect(bitmap1.contains(20));
    try std.testing.expect(!bitmap1.contains(30));

    // "quick" should return same group
    var bitmap2 = (try reader.lookup("quick")) orelse return error.TestExpectedEqual;
    defer bitmap2.deinit();
    try std.testing.expect(bitmap2.contains(10));
    try std.testing.expect(bitmap2.contains(20));

    // "large" → group with IDs 30, 40, 50
    var bitmap3 = (try reader.lookup("large")) orelse return error.TestExpectedEqual;
    defer bitmap3.deinit();
    try std.testing.expect(bitmap3.contains(30));
    try std.testing.expect(bitmap3.contains(40));
    try std.testing.expect(bitmap3.contains(50));
    try std.testing.expect(!bitmap3.contains(10));

    // Unknown term
    const missing = try reader.lookup("unknown");
    try std.testing.expect(missing == null);
}

test "synonym expandTerm" {
    const alloc = std.testing.allocator;
    var writer = SynonymWriter.init(alloc);
    defer writer.deinit();

    try writer.addGroup(&.{ "fast", "quick", "rapid" }, &.{ 10, 20, 30 });
    try writer.addGroup(&.{ "big", "large" }, &.{ 40, 50 });

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try SynonymReader.init(alloc, data);
    defer reader.deinit();

    // Expand "fast" → should get all three synonyms
    const expanded = (try reader.expandTerm(alloc, "fast")).?;
    defer {
        for (expanded) |t| alloc.free(t);
        alloc.free(expanded);
    }
    try std.testing.expectEqual(@as(usize, 3), expanded.len);
    // FST iterates in sorted order: fast, quick, rapid
    try std.testing.expectEqualStrings("fast", expanded[0]);
    try std.testing.expectEqualStrings("quick", expanded[1]);
    try std.testing.expectEqualStrings("rapid", expanded[2]);

    // Expand unknown term → null
    const missing = try reader.expandTerm(alloc, "slow");
    try std.testing.expect(missing == null);
}

test "synonym empty" {
    const alloc = std.testing.allocator;
    var writer = SynonymWriter.init(alloc);
    defer writer.deinit();

    const data = try writer.build();
    defer alloc.free(data);

    var reader = try SynonymReader.init(alloc, data);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u32, 0), reader.num_groups);
}
