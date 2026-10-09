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

//! Compressed physical selections keyed by logical tuple and physical block.
//! Fresh rows arrive in tuple order. Incremental deletes name only changed-file
//! blocks; the same bounded spill merge publishes copy-on-write posting pages.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
const Bitmap = local.encoding_roaring.RoaringBitmap;
pub const shift = 20;
pub const Selection = union(enum) { interval: struct { lower: u32, count: u32 }, bitmap: Bitmap };
pub const Block = struct { file: []const u8, group: u32, base: u64, selection: Selection };
pub fn key(a: A, forward: []const u8) ![]u8 {
    if (forward.len < 16) return error.InvalidNativeLakeRowIndex;
    const result = try a.dupe(u8, forward);
    const row = std.mem.readInt(u64, result[result.len - 8 ..][0..8], .big);
    std.mem.writeInt(u64, result[result.len - 8 ..][0..8], row & ~@as(u64, (1 << shift) - 1), .big);
    return result;
}
pub fn decode(a: A, bytes: []const u8) !Selection {
    if (bytes.len == 0) return error.InvalidNativeLakeRowIndex;
    if (bytes[0] == 0) {
        if (bytes.len != 9) return error.InvalidNativeLakeRowIndex;
        const lower = std.mem.readInt(u32, bytes[1..5], .big);
        const count = std.mem.readInt(u32, bytes[5..9], .big);
        if (lower >= 1 << shift or count == 0 or count > (1 << shift) - lower) return error.InvalidNativeLakeRowIndex;
        return .{ .interval = .{ .lower = lower, .count = count } };
    }
    if (bytes[0] != 1 or bytes.len > 256 * 1024) return error.InvalidNativeLakeRowIndex;
    var bitmap = try Bitmap.fromBytes(a, bytes[1..]);
    errdefer bitmap.deinit();
    if (bitmap.isEmpty() or bitmap.rank(1 << shift) != bitmap.cardinality()) return error.InvalidNativeLakeRowIndex;
    return .{ .bitmap = bitmap };
}
pub const Builder = struct {
    a: A,
    sort: local.sql_spill.Sort,
    bitmap: Bitmap,
    current: ?[]u8 = null,
    lower: u32 = 0,
    upper: u32 = 0,
    count: u32 = 0,
    pub fn init(a: A, manager: *local.sql_spill.Manager) Builder {
        return .{ .a = a, .sort = .init(a, manager, &.{ .{}, .{} }, 512 * 1024), .bitmap = Bitmap.init(a) };
    }
    pub fn deinit(self: *Builder) void {
        if (self.current) |current| self.a.free(current);
        self.bitmap.deinit();
        self.sort.deinit();
    }
    pub fn remove(self: *Builder, forward: []const u8) !void {
        const block = try key(self.a, forward);
        defer self.a.free(block);
        try self.sort.add(.{ .keys = &.{ local.sql_scalar.Datum.fromJson(.{ .string = block }), local.sql_scalar.Datum.fromJson(.{ .integer = 0 }) }, .values = &.{}, .ordinal = 0 });
    }
    pub fn add(self: *Builder, forward: []const u8) !void {
        const block = try key(self.a, forward);
        var owned = true;
        defer if (owned) self.a.free(block);
        if (self.current) |current| if (!std.mem.eql(u8, current, block)) try self.flush();
        const low: u32 = @intCast(std.mem.readInt(u64, forward[forward.len - 8 ..][0..8], .big) & ((1 << shift) - 1));
        if (self.current == null) {
            self.current = block;
            self.lower = low;
            owned = false;
        } else if (low <= self.upper) return error.InvalidNativeLakeRowIndex;
        try self.bitmap.add(low);
        self.upper = low;
        self.count += 1;
    }
    pub fn flush(self: *Builder) !void {
        const current = self.current orelse return;
        var interval: [9]u8 = undefined;
        var encoded: ?[]u8 = null;
        defer if (encoded) |bytes| self.a.free(bytes);
        const value: []const u8 = if (self.upper - self.lower + 1 == self.count) blk: {
            interval[0] = 0;
            std.mem.writeInt(u32, interval[1..5], self.lower, .big);
            std.mem.writeInt(u32, interval[5..9], self.count, .big);
            break :blk &interval;
        } else blk: {
            const bytes = try self.bitmap.toBytes(self.a);
            defer self.a.free(bytes);
            encoded = try std.mem.concat(self.a, u8, &.{ "\x01", bytes });
            break :blk encoded.?;
        };
        try self.sort.add(.{ .keys = &.{ local.sql_scalar.Datum.fromJson(.{ .string = current }), local.sql_scalar.Datum.fromJson(.{ .integer = 1 }) }, .values = &.{local.sql_scalar.Datum.fromJson(.{ .string = value })}, .ordinal = 0 });
        self.a.free(current);
        self.current = null;
        self.bitmap.deinit();
        self.bitmap = Bitmap.init(self.a);
        self.count = 0;
    }
    pub fn publish(self: *Builder, store: tree.Store, prior: ?tree.Ref) !?tree.Ref {
        try self.flush();
        var root = prior;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const Stream = struct {
            sort: *local.sql_spill.Sort,
            arena: std.heap.ArenaAllocator,
            pending: ?local.sql_operators.Row = null,
            fn next(stream: *@This(), out: A) !?tree.Mutation {
                const first = stream.pending orelse (try stream.sort.next(stream.arena.allocator())) orelse return null;
                stream.pending = null;
                const name = try out.dupe(u8, first.keys[0].value.string);
                var value: ?[]const u8 = if (first.values.len == 0) null else try out.dupe(u8, first.values[0].value.string);
                while (true) {
                    _ = stream.arena.reset(.free_all);
                    const row = try stream.sort.next(stream.arena.allocator()) orelse break;
                    if (!std.mem.eql(u8, name, row.keys[0].value.string)) {
                        stream.pending = row;
                        break;
                    }
                    if (row.values.len != 0) value = try out.dupe(u8, row.values[0].value.string);
                }
                return .{ .key = name, .value = value };
            }
        };
        var stream: Stream = .{ .sort = &self.sort, .arena = .init(self.a) };
        defer stream.arena.deinit();
        if (prior == null) {
            const Source = struct {
                stream: *Stream,
                arena: std.heap.ArenaAllocator,
                pub fn next(source: *@This()) !?tree.Cursor.Record {
                    _ = source.arena.reset(.free_all);
                    const change = try source.stream.next(source.arena.allocator()) orelse return null;
                    return .{ .key = change.key, .value = change.value orelse return error.InvalidNativeLakeRowIndex };
                }
            };
            var source: Source = .{ .stream = &stream, .arena = .init(self.a) };
            defer source.arena.deinit();
            return tree.buildSorted(self.a, store, &source);
        }
        while (true) {
            _ = arena.reset(.free_all);
            var changes: std.ArrayList(tree.Mutation) = .empty;
            var bytes: usize = 0;
            while (changes.items.len < 128 and bytes < 512 * 1024) {
                const change = try stream.next(a) orelse break;
                bytes += change.key.len + (if (change.value) |v| v.len else 0);
                try changes.append(a, change);
            }
            if (changes.items.len == 0) return root;
            root = try tree.apply(self.a, store, root, changes.items);
        }
    }
};

test "external lake predicate blocks validate extents without decoding row IDs" {
    const a = std.testing.allocator;
    var bytes: [9]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[1..5], 7, .big);
    std.mem.writeInt(u32, bytes[5..9], 100000, .big);
    const selection = try decode(a, &bytes);
    try std.testing.expectEqual(@as(u32, 100000), selection.interval.count);
    std.mem.writeInt(u32, bytes[5..9], 1 << shift, .big);
    try std.testing.expectError(error.InvalidNativeLakeRowIndex, decode(a, &bytes));
}
