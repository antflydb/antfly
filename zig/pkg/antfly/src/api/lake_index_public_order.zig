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

//! Public-ID ordered seeks over native tuple/file counts and physical rows.
//! A tie group visits only participating files and the requested row window.
//! Snapshot-bound public digests are computed once; retained row pages remain
//! reusable when a new snapshot changes the digest permutation of the files.
const std = @import("std");
const local = @import("antfly_local_sources");
const ordered = @import("lake_index_ordered_rows.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Cursor = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    group_arena: std.heap.ArenaAllocator,
    file_arena: std.heap.ArenaAllocator,
    directory: ?tree.Cursor = null,
    pending: ?tree.Cursor.Record = null,
    reader: *ordered.Reader,
    digests: []const [32]u8,
    lower: []const u8,
    upper: ?[]const u8,
    boundary: ?[]const u8,
    boundary_file: ?u32 = null,
    boundary_coordinate: [12]u8 = @splat(0),
    reverse: bool,
    reverse_ids: bool,
    group: []const u8 = "",
    files: []const u32 = &.{},
    file_position: usize = 0,
    rows: ?tree.Cursor = null,
    exhausted: bool = false,
    pub fn init(a: A, reader: *ordered.Reader, lower: []const u8, upper: ?[]const u8, boundary: ?[]const u8, id: ?[]const u8, id_descending: bool, before: bool) !Cursor {
        var result: Cursor = .{ .a = a, .arena = .init(a), .group_arena = .init(a), .file_arena = .init(a), .reader = reader, .digests = &.{}, .lower = undefined, .upper = null, .boundary = null, .reverse = before, .reverse_ids = id_descending != before };
        errdefer result.deinit();
        const ca = result.arena.allocator();
        result.lower = try ca.dupe(u8, lower);
        result.upper = if (upper) |end| try ca.dupe(u8, end) else null;
        if (reader.root.public_digests.len != reader.root.files.len) return error.InvalidNativeLakeRowIndex;
        const digests = reader.root.public_digests;
        result.digests = digests;
        if (boundary) |prefix| {
            const value = id orelse return error.InvalidRelationalIndexBound;
            if (value.len != 96 or !std.mem.startsWith(u8, value, "lake1:") or value[70] != ':' or value[79] != ':') return error.InvalidRelationalIndexBound;
            for (value, 0..) |byte, i| if (i >= 6 and i != 70 and i != 79 and !((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidRelationalIndexBound;
            const group = std.fmt.parseUnsigned(u32, value[71..79], 16) catch return error.InvalidRelationalIndexBound;
            const row = std.fmt.parseUnsigned(u64, value[80..96], 16) catch return error.InvalidRelationalIndexBound;
            for (digests, 0..) |digest, slot| if (std.mem.eql(u8, value[6..70], &std.fmt.bytesToHex(digest, .lower))) {
                result.boundary_file = @intCast(slot);
                break;
            };
            if (result.boundary_file == null) return error.InvalidRelationalIndexBound;
            result.boundary = try ca.dupe(u8, prefix);
            std.mem.writeInt(u32, result.boundary_coordinate[0..4], group, .big);
            std.mem.writeInt(u64, result.boundary_coordinate[4..12], row, .big);
            if (before) {
                if (try orderedSuccessor(ca, prefix)) |end| {
                    if (result.upper == null or std.mem.order(u8, end, result.upper.?) == .lt) result.upper = end;
                }
            } else if (std.mem.order(u8, prefix, result.lower) == .gt) result.lower = result.boundary.?;
        }
        if (result.upper) |end| if (std.mem.order(u8, result.lower, end) != .lt) {
            result.exhausted = true;
        };
        if (!result.exhausted) result.directory = if (before)
            try tree.Cursor.initReverse(a, reader.pages.store(), reader.root.ties, result.lower, result.upper)
        else
            try tree.Cursor.init(a, reader.pages.store(), reader.root.ties, result.lower, result.upper);
        return result;
    }
    pub fn deinit(self: *Cursor) void {
        if (self.rows) |*rows| rows.deinit();
        self.group_arena.deinit();
        self.file_arena.deinit();
        if (self.directory) |*directory| directory.deinit();
        self.arena.deinit();
    }
    fn loadGroup(self: *Cursor) !bool {
        if (self.exhausted) return false;
        _ = self.group_arena.reset(.retain_capacity);
        const ca = self.group_arena.allocator();
        const cursor = &self.directory.?;
        const first = (self.pending orelse try cursor.next()) orelse {
            self.exhausted = true;
            return false;
        };
        self.pending = null;
        if (first.key.len < 4) return error.InvalidNativeLakeRowIndex;
        self.group = try ca.dupe(u8, first.key[0 .. first.key.len - 4]);
        var files: std.ArrayList(u32) = .empty;
        var pending_record: ?tree.Cursor.Record = first;
        while (pending_record) |record| {
            if (record.key.len != self.group.len + 4 or !std.mem.eql(u8, record.key[0..self.group.len], self.group)) {
                self.pending = record;
                break;
            }
            if (record.value.len != 8 or std.mem.readInt(u64, record.value[0..8], .big) == 0) return error.InvalidNativeLakeRowIndex;
            const slot = std.mem.readInt(u32, record.key[record.key.len - 4 ..][0..4], .big);
            if (slot >= self.digests.len or files.items.len >= self.reader.root.files.len) return error.InvalidNativeLakeRowIndex;
            try files.append(ca, slot);
            pending_record = try cursor.next();
        }
        const Order = struct {
            digests: []const [32]u8,
            reverse: bool,
            fn less(order: @This(), x: u32, y: u32) bool {
                return std.mem.order(u8, &order.digests[x], &order.digests[y]) == (if (order.reverse) std.math.Order.gt else .lt);
            }
        };
        std.mem.sort(u32, files.items, Order{ .digests = self.digests, .reverse = self.reverse_ids }, Order.less);
        self.files = files.items;
        self.file_position = 0;
        return true;
    }
    fn openFile(self: *Cursor, slot: u32) !bool {
        _ = self.file_arena.reset(.retain_capacity);
        const ca = self.file_arena.allocator();
        var file: [4]u8 = undefined;
        std.mem.writeInt(u32, &file, slot, .big);
        var lower: []const u8 = try std.mem.concat(ca, u8, &.{ self.group, &file });
        var upper = try orderedSuccessor(ca, lower);
        if (self.boundary) |boundary| if (std.mem.eql(u8, self.group, boundary)) {
            const relation = std.mem.order(u8, &self.digests[slot], &self.digests[self.boundary_file.?]);
            if (relation == (if (self.reverse_ids) std.math.Order.gt else .lt)) return false;
            if (relation == .eq) {
                const key = try std.mem.concat(ca, u8, &.{ lower, &self.boundary_coordinate });
                if (self.reverse_ids) upper = key else lower = (try orderedSuccessor(ca, key)) orelse return false;
            }
        };
        self.rows = if (self.reverse_ids)
            try tree.Cursor.initReverse(self.a, self.reader.pages.store(), self.reader.root.page, lower, upper)
        else
            try tree.Cursor.init(self.a, self.reader.pages.store(), self.reader.root.page, lower, upper);
        return true;
    }
    pub fn next(self: *Cursor, a: A, count: usize) ![]const local.storage_rowsource_types.RowRef {
        if (count == 0 or count > 65536) return error.InvalidNativeLakeRowIndex;
        var result: std.ArrayList(local.storage_rowsource_types.RowRef) = .empty;
        errdefer result.deinit(a);
        while (result.items.len < count) {
            if (self.rows) |*rows| {
                if (try rows.next()) |record| {
                    try result.append(a, try self.reader.decode(record));
                    continue;
                }
                rows.deinit();
                self.rows = null;
            }
            if (self.file_position < self.files.len) {
                const slot = self.files[self.file_position];
                self.file_position += 1;
                _ = try self.openFile(slot);
                continue;
            }
            if (!try self.loadGroup()) break;
        }
        return result.toOwnedSlice(a);
    }
};
fn orderedSuccessor(a: A, prefix: []const u8) !?[]const u8 {
    var end = prefix.len;
    while (end != 0) {
        end -= 1;
        if (prefix[end] == 255) continue;
        const result = try a.dupe(u8, prefix[0 .. end + 1]);
        result[end] += 1;
        return result;
    }
    return null;
}
