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

//! Compact physical candidate selection, independent of score/rank order.
const std = @import("std");
const rows = @import("../../storage/rowsource/types.zig");
const external = @import("../external_source/types.zig");
const A = std.mem.Allocator;
pub const max_candidates = 65536;
pub const Coordinate = struct { file: usize, group: u32, row: u64 };
pub const Selection = struct {
    pub const Block = struct { file: usize, group: u32, high: u32, rows: *const @import("../../encoding/roaring.zig").RoaringBitmap };
    coordinates: []const Coordinate = &.{},
    /// Borrowed sorted blocks; retained by the caller for the scan lifetime.
    blocks: []const Block = &.{},
    pub fn blockLess(_: void, l: Block, r: Block) bool {
        return less({}, .{ .file = l.file, .group = l.group, .row = @as(u64, l.high) << 32 }, .{ .file = r.file, .group = r.group, .row = @as(u64, r.high) << 32 });
    }
    fn blockLower(self: Selection, file: usize, group: u32, high: u32) usize {
        var begin: usize = 0;
        var end = self.blocks.len;
        while (begin < end) {
            const mid = begin + (end - begin) / 2;
            if (blockLess({}, self.blocks[mid], .{ .file = file, .group = group, .high = high, .rows = self.blocks[mid].rows })) begin = mid + 1 else end = mid;
        }
        return begin;
    }

    /// Allocations belong to the caller's request arena. Strings need not be
    /// retained: validated references become inventory ordinals exactly once.
    pub fn init(a: A, inventory: external.Inventory, refs: []const rows.RowRef) !Selection {
        return initWithMap(a, inventory, refs, null);
    }
    pub fn initWithMap(a: A, inventory: external.Inventory, refs: []const rows.RowRef, known: ?*const std.StringHashMapUnmanaged(usize)) !Selection {
        if (refs.len > max_candidates) return error.LakeCandidateBudgetExceeded;
        var files: std.StringHashMapUnmanaged(usize) = .empty;
        defer files.deinit(a);
        if (known == null) {
            try files.ensureTotalCapacity(a, @intCast(inventory.files.len));
            for (inventory.files, 0..) |file, index| {
                const entry = files.getOrPutAssumeCapacity(file.file_id);
                if (entry.found_existing) return error.InvalidExternalLakeIndexCoverage;
                entry.value_ptr.* = index;
            }
        }
        const by_id = known orelse &files;
        const coordinates = try a.alloc(Coordinate, refs.len);
        errdefer a.free(coordinates);
        for (refs, coordinates) |ref, *coordinate| {
            if (ref != .external) return error.ExternalLakeSnapshotMismatch;
            const r = ref.external;
            if (!std.mem.eql(u8, r.source_id, inventory.source_id) or !std.mem.eql(u8, r.snapshot_id, inventory.snapshot_id)) return error.ExternalLakeSnapshotMismatch;
            const index = by_id.get(r.file_id) orelse return error.ExternalLakeSnapshotMismatch;
            coordinate.* = .{ .file = index, .group = r.row_group_ordinal, .row = r.row_ordinal };
            // Directory Parquet inventories discover groups lazily. Their
            // physical ordinal bounds are checked against the opened footer.
            if (inventory.files[index].row_groups.len != 0) try validateGroup(inventory.files[index], coordinate.*);
        }
        std.mem.sort(Coordinate, coordinates, {}, less);
        var unique: usize = 0;
        for (coordinates) |coordinate| {
            if (unique != 0 and std.meta.eql(coordinates[unique - 1], coordinate)) continue;
            coordinates[unique] = coordinate;
            unique += 1;
        }
        // Keep the original allocation owned by the arena; the compact view
        // exposes only unique coordinates.
        return .{ .coordinates = coordinates[0..unique] };
    }
    pub fn validateFile(self: Selection, index: usize, file: external.FileEntry) !void {
        var block_index = self.blockLower(index, 0, 0);
        while (block_index < self.blocks.len and self.blocks[block_index].file == index) : (block_index += 1) {
            const block = self.blocks[block_index];
            const group = for (file.row_groups) |group| {
                if (group.ordinal == block.group) break group;
            } else return error.InvalidLakeCandidateReference;
            const base = @as(u64, block.high) << 32;
            const available = group.row_count -| base;
            if (available < (1 << 32) and block.rows.rangeCardinality(@intCast(available), 1 << 32) != 0) return error.InvalidLakeCandidateReference;
        }
        var position = self.lower(.{ .file = index, .group = 0, .row = 0 });
        while (position < self.coordinates.len and self.coordinates[position].file == index) : (position += 1)
            try validateGroup(file, self.coordinates[position]);
    }
    fn validateGroup(file: external.FileEntry, coordinate: Coordinate) !void {
        const group = for (file.row_groups) |group| {
            if (group.ordinal == coordinate.group) break group;
        } else return error.InvalidLakeCandidateReference;
        if (coordinate.row >= group.row_count) return error.InvalidLakeCandidateReference;
    }
    fn less(_: void, l: Coordinate, r: Coordinate) bool {
        if (l.file != r.file) return l.file < r.file;
        if (l.group != r.group) return l.group < r.group;
        return l.row < r.row;
    }
    fn lower(self: Selection, key: Coordinate) usize {
        var begin: usize = 0;
        var end = self.coordinates.len;
        while (begin < end) {
            const mid = begin + (end - begin) / 2;
            if (less({}, self.coordinates[mid], key)) begin = mid + 1 else end = mid;
        }
        return begin;
    }
    pub fn fileMayMatch(self: Selection, file: usize) bool {
        const block = self.blockLower(file, 0, 0);
        if (block < self.blocks.len and self.blocks[block].file == file) return true;
        const index = self.lower(.{ .file = file, .group = 0, .row = 0 });
        return index < self.coordinates.len and self.coordinates[index].file == file;
    }
    pub fn rangeMayMatch(self: Selection, file: usize, group: u32, first: u64, count: u64) bool {
        if (count == 0) return false;
        const end = @as(u128, first) + count;
        var block_index = self.blockLower(file, group, @intCast(first >> 32));
        while (block_index < self.blocks.len) : (block_index += 1) {
            const block = self.blocks[block_index];
            const base = @as(u64, block.high) << 32;
            if (block.file != file or block.group != group or base >= end) break;
            const low_row: u32 = @intCast(first -| base);
            const upper: u64 = @intCast(@min(end - base, 1 << 32));
            if (block.rows.rangeCardinality(low_row, upper) != 0) return true;
        }
        const index = self.lower(.{ .file = file, .group = group, .row = first });
        if (index == self.coordinates.len) return false;
        const found = self.coordinates[index];
        return found.file == file and found.group == group and found.row -| first < count;
    }
    pub fn contains(self: Selection, file: usize, group: u32, row: u64) bool {
        return self.rangeMayMatch(file, group, row, 1);
    }
};

test "external lake physical candidates deduplicate and seek without losing exact ordinals" {
    const coordinates = [_]Coordinate{ .{ .file = 0, .group = 1, .row = 2 }, .{ .file = 0, .group = 1, .row = 9007199254740993 }, .{ .file = 3, .group = 0, .row = 1 } };
    const selection: Selection = .{ .coordinates = &coordinates };
    try std.testing.expect(selection.fileMayMatch(0));
    try std.testing.expect(!selection.fileMayMatch(1));
    try std.testing.expect(selection.contains(0, 1, 9007199254740993));
    try std.testing.expect(!selection.contains(0, 1, 9007199254740992));
    try std.testing.expect(!selection.rangeMayMatch(0, 1, 3, 5));
    try std.testing.expect(selection.rangeMayMatch(0, 1, 0, 3));
}

test "external lake compressed selections seek across block and u32 boundaries" {
    const Bitmap = @import("../../encoding/roaring.zig").RoaringBitmap;
    var selected_rows = Bitmap.init(std.testing.allocator);
    defer selected_rows.deinit();
    try selected_rows.add(0);
    try selected_rows.add(7);
    const blocks = [_]Selection.Block{ .{ .file = 0, .group = 1, .high = 0, .rows = &selected_rows }, .{ .file = 0, .group = 1, .high = 1, .rows = &selected_rows }, .{ .file = 3, .group = 0, .high = 0, .rows = &selected_rows } };
    const selection: Selection = .{ .blocks = &blocks };
    try std.testing.expect(selection.fileMayMatch(0));
    try std.testing.expect(!selection.fileMayMatch(1));
    try std.testing.expect(selection.contains(0, 1, (1 << 32) + 7));
    try std.testing.expect(!selection.rangeMayMatch(0, 1, 8, 8));
    try std.testing.expect(selection.rangeMayMatch(0, 1, std.math.maxInt(u32), 2));
    try std.testing.expect(!selection.rangeMayMatch(0, 1, 8, (1 << 32) - 8));
}
