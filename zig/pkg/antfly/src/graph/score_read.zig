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

//! Snapshot-local score reads. Physical identity is the complete encoded
//! metric/generation prefix, never just equivalent configuration. Callers
//! resolve freshness policies before allocating results or entering this layer.
const std = @import("std");
const keys = @import("../storage/internal_keys.zig");
pub const max_keys_per_read = 4096;
pub const Stats = struct { keys: usize = 0, batches: usize = 0 };

pub fn populate(
    alloc: std.mem.Allocator,
    txn: anytype,
    prefixes: []const ?[]const u8,
    nodes: []const []const u8,
    columns: []const []?f64,
) !Stats {
    std.debug.assert(prefixes.len == columns.len);
    for (columns) |column| {
        std.debug.assert(column.len == nodes.len);
        @memset(column, null);
    }
    if (nodes.len == 0) return .{};
    const physical = try alloc.alloc(usize, prefixes.len);
    defer alloc.free(physical);
    var physical_count: usize = 0;
    for (prefixes, 0..) |prefix, i| if (prefix != null) {
        physical[physical_count] = i;
        physical_count += 1;
    };
    if (physical_count == 0) return .{};
    const Order = struct {
        prefixes: []const ?[]const u8,
        fn less(self: @This(), a: usize, b: usize) bool {
            const order = std.mem.order(u8, self.prefixes[a].?, self.prefixes[b].?);
            return order == .lt or (order == .eq and a < b);
        }
    };
    std.mem.sort(usize, physical[0..physical_count], Order{ .prefixes = prefixes }, Order.less);
    // Retain the sorted logical columns for the final independently-owned
    // result fanout; deduplicate physical columns in a separate compact list.
    const owners = try alloc.alloc(usize, physical_count);
    defer alloc.free(owners);
    var owner_count: usize = 0;
    for (physical[0..physical_count]) |column| {
        if (owner_count != 0 and std.mem.eql(u8, prefixes[owners[owner_count - 1]].?, prefixes[column].?)) continue;
        owners[owner_count] = column;
        owner_count += 1;
    }
    const rows = try alloc.alloc(usize, nodes.len);
    defer alloc.free(rows);
    for (rows, 0..) |*row, i| row.* = i;
    const RowOrder = struct {
        nodes: []const []const u8,
        fn less(self: @This(), a: usize, b: usize) bool {
            const order = std.mem.order(u8, self.nodes[a], self.nodes[b]);
            return order == .lt or (order == .eq and a < b);
        }
    };
    std.mem.sort(usize, rows, RowOrder{ .nodes = nodes }, RowOrder.less);
    var unique_count: usize = 1;
    for (rows[1..], rows[0 .. rows.len - 1]) |row, prior| {
        if (!std.mem.eql(u8, nodes[row], nodes[prior])) unique_count += 1;
    }
    const unique = if (unique_count == rows.len) rows else try alloc.alloc(usize, unique_count);
    defer if (unique_count != rows.len) alloc.free(unique);
    if (unique_count != rows.len) {
        var i: usize = 0;
        for (rows) |row| {
            if (i != 0 and std.mem.eql(u8, nodes[unique[i - 1]], nodes[row])) continue;
            unique[i] = row;
            i += 1;
        }
    }
    const total = std.math.mul(usize, owner_count, unique_count) catch return error.GraphMetricQueryBudgetExceeded;
    const batch_capacity = @min(total, max_keys_per_read);
    const read_keys = try alloc.alloc([]const u8, batch_capacity);
    defer alloc.free(read_keys);
    const read_values = try alloc.alloc(?[]const u8, batch_capacity);
    defer alloc.free(read_values);
    var key_bytes = std.ArrayListUnmanaged(u8).empty;
    defer key_bytes.deinit(alloc);
    var stats = Stats{ .keys = total };
    var offset: usize = 0;
    while (offset < total) {
        const len = @min(batch_capacity, total - offset);
        var bytes: usize = 0;
        for (0..len) |i| {
            const flat = offset + i;
            const prefix = prefixes[owners[flat / unique_count]].?;
            const node = nodes[unique[flat % unique_count]];
            bytes = std.math.add(usize, bytes, prefix.len) catch return error.GraphMetricQueryBudgetExceeded;
            bytes = std.math.add(usize, bytes, keys.encodedComponentLen(node)) catch return error.GraphMetricQueryBudgetExceeded;
        }
        key_bytes.clearRetainingCapacity();
        try key_bytes.ensureTotalCapacity(alloc, bytes);
        for (read_keys[0..len], 0..) |*key, i| {
            const flat = offset + i;
            const start = key_bytes.items.len;
            try key_bytes.appendSlice(alloc, prefixes[owners[flat / unique_count]].?);
            try keys.appendEncodedComponent(&key_bytes, alloc, nodes[unique[flat % unique_count]]);
            key.* = key_bytes.items[start..];
        }
        @memset(read_values[0..len], null);
        try txn.getManySorted(read_keys[0..len], read_values[0..len]);
        for (read_values[0..len], 0..) |maybe_raw, i| {
            const raw = maybe_raw orelse continue;
            if (raw.len != 8) return error.InvalidGraphMetricScore;
            const score: f64 = @bitCast(std.mem.readInt(u64, raw[0..8], .little));
            if (!std.math.isFinite(score)) return error.InvalidGraphMetricScore;
            const flat = offset + i;
            columns[owners[flat / unique_count]][unique[flat % unique_count]] = score;
        }
        offset += len;
        stats.batches += 1;
    }
    if (unique_count != rows.len) for (owners[0..owner_count]) |owner| {
        var representative = rows[0];
        for (rows[1..]) |row| {
            if (std.mem.eql(u8, nodes[row], nodes[representative])) {
                columns[owner][row] = columns[owner][representative];
            } else representative = row;
        }
    };
    var owner = physical[0];
    for (physical[1..physical_count]) |column| {
        if (std.mem.eql(u8, prefixes[owner].?, prefixes[column].?)) {
            @memcpy(columns[column], columns[owner]);
        } else owner = column;
    }
    return stats;
}

test "graph metric physical score reads deduplicate keys and retain logical ownership" {
    const Runner = struct {
        const Txn = struct {
            raw: [8]u8 = @bitCast(@as(f64, 1.5)),
            seen: usize = 0,
            pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
                for (requested, 0..) |key, i| {
                    if (i > 0) try std.testing.expect(std.mem.order(u8, requested[i - 1], key) == .lt);
                    values[i] = if (std.mem.endsWith(u8, key, "missing\x00\x00")) null else &self.raw;
                }
                self.seen += requested.len;
            }
        };
        fn run(alloc: std.mem.Allocator) !void {
            const nodes = [_][]const u8{ "z", "a\x00b", "z", "missing", "" };
            const prefixes = [_]?[]const u8{ "b\x00\x00", "a\x00\x00", "b\x00\x00", null };
            var columns: [prefixes.len][]?f64 = undefined;
            var initialized: usize = 0;
            defer for (columns[0..initialized]) |column| alloc.free(column);
            for (&columns) |*column| {
                column.* = try alloc.alloc(?f64, nodes.len);
                initialized += 1;
            }
            var txn = Txn{};
            const stats = try populate(alloc, &txn, &prefixes, &nodes, &columns);
            try std.testing.expectEqual(@as(usize, 8), stats.keys);
            try std.testing.expectEqual(stats.keys, txn.seen);
            for (columns[0..3]) |column| try std.testing.expectEqualSlices(?f64, &.{ 1.5, 1.5, 1.5, null, 1.5 }, column);
            for (columns[3]) |score| try std.testing.expectEqual(@as(?f64, null), score);
            columns[0][0] = 9;
            try std.testing.expectEqual(@as(?f64, 1.5), columns[2][0]);
            txn.raw = @bitCast(std.math.inf(f64));
            _ = populate(alloc, &txn, &prefixes, &nodes, &columns) catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.InvalidGraphMetricScore, err);
                return;
            };
            return error.TestUnexpectedResult;
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
