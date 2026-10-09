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

//! Counts distinct logical-tuple/file combinations. Large equal-key groups
//! retain one directory entry per file, rather than one entry per source row.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
pub const Builder = struct {
    a: A,
    sort: local.sql_spill.Sort,
    pending: std.ArrayList(u8) = .empty,
    count: u64 = 0,
    pub fn init(a: A, manager: *local.sql_spill.Manager) Builder {
        var sort = local.sql_spill.Sort.init(a, manager, &.{.{}}, 512 * 1024);
        sort.run_limit = 4;
        return .{ .a = a, .sort = sort };
    }
    pub fn deinit(self: *Builder) void {
        self.pending.deinit(self.a);
        self.sort.deinit();
    }
    fn flush(self: *Builder) !void {
        if (self.count == 0) return;
        try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = self.pending.items })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .integer = std.math.cast(i64, self.count) orelse return error.InvalidNativeLakeRowIndex })}, .ordinal = 0 });
        self.count = 0;
    }
    pub fn add(self: *Builder, key: []const u8) !void {
        if (key.len < 16) return error.InvalidNativeLakeRowIndex;
        const prefix = key[0 .. key.len - 12];
        if (self.count != 0 and !std.mem.eql(u8, prefix, self.pending.items)) try self.flush();
        if (self.count == 0) {
            self.pending.clearRetainingCapacity();
            try self.pending.appendSlice(self.a, prefix);
        }
        self.count = try std.math.add(u64, self.count, 1);
    }
    pub fn finish(self: *Builder, store: tree.Store) !?tree.Ref {
        try self.flush();
        const Source = struct {
            sort: *local.sql_spill.Sort,
            arena: std.heap.ArenaAllocator,
            value: [8]u8 = undefined,
            pub fn next(source: *@This()) !?tree.Cursor.Record {
                _ = source.arena.reset(.retain_capacity);
                const row = try source.sort.next(source.arena.allocator()) orelse return null;
                if (row.keys.len != 1 or row.keys[0].value != .string or row.values.len != 1 or row.values[0].value != .integer or row.values[0].value.integer <= 0) return error.InvalidNativeLakeRowIndex;
                std.mem.writeInt(u64, &source.value, @intCast(row.values[0].value.integer), .big);
                return .{ .key = row.keys[0].value.string, .value = &source.value };
            }
        };
        var source: Source = .{ .sort = &self.sort, .arena = .init(self.a) };
        defer source.arena.deinit();
        return tree.buildSorted(self.a, store, &source);
    }
};
/// Apply bounded changed-row batches to authenticated file counts. The number
/// of operations depends on changed tuple/file pairs, never on retained rows.
pub fn apply(a: A, store: tree.Store, root: ?tree.Ref, rows: []const tree.Mutation) !?tree.Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var deltas: std.StringHashMapUnmanaged(i64) = .empty;
    for (rows) |row| {
        if (row.key.len < 16) return error.InvalidNativeLakeRowIndex;
        const key = row.key[0 .. row.key.len - 12];
        const entry = try deltas.getOrPut(ca, key);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += if (row.value == null) @as(i64, -1) else 1;
    }
    var changes: std.ArrayList(tree.Mutation) = .empty;
    var entries = deltas.iterator();
    while (entries.next()) |entry| {
        if (entry.value_ptr.* == 0) continue;
        var cursor = try tree.Cursor.init(a, store, root, entry.key_ptr.*, null);
        defer cursor.deinit();
        var count: i128 = 0;
        if (try cursor.next()) |record| if (std.mem.eql(u8, record.key, entry.key_ptr.*)) {
            if (record.value.len != 8) return error.InvalidNativeLakeRowIndex;
            count = std.mem.readInt(u64, record.value[0..8], .big);
        };
        count += entry.value_ptr.*;
        if (count < 0 or count > std.math.maxInt(u64)) return error.InvalidNativeLakeRowIndex;
        const value = if (count == 0) null else blk: {
            const bytes = try ca.alloc(u8, 8);
            std.mem.writeInt(u64, bytes[0..8], @intCast(count), .big);
            break :blk bytes;
        };
        try changes.append(ca, .{ .key = entry.key_ptr.*, .value = value });
    }
    std.mem.sort(tree.Mutation, changes.items, {}, struct {
        fn less(_: void, x: tree.Mutation, y: tree.Mutation) bool {
            return std.mem.order(u8, x.key, y.key) == .lt;
        }
    }.less);
    return tree.apply(a, store, root, changes.items);
}
