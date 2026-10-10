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

//! Snapshot-local score reads. Physical identity is the complete encoded
//! metric/generation prefix, never just equivalent configuration. Callers
//! resolve freshness policies before allocating results or entering this layer.
const std = @import("std");
const keys = @import("../storage/internal_keys.zig");
pub const max_keys_per_read = 4096;
pub const max_key_bytes_per_read = 1024 * 1024;
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
    const scoped = comptime @hasDecl(@typeInfo(@TypeOf(txn)).pointer.child, "openReadScope");
    const Scope = if (scoped) @typeInfo(@TypeOf(txn.openReadScope(alloc))).error_union.payload else void;
    // Probe-only wrappers expose the method but cannot open a pinned scope.
    // Keep their existing batched view, and propagate allocation/storage errors.
    var scope: ?Scope = if (scoped) txn.openReadScope(alloc) catch |err|
        (if (err == error.ReadOnly or err == error.Unsupported) null else return err) else null;
    defer if (scoped) {
        if (scope) |*active| active.close();
    };
    const direct = comptime if (scoped) @hasDecl(Scope, "getManySortedInto") else false;
    const use_buffers = direct and scope != null;
    const score_buffers: [][8]u8 = if (use_buffers) try alloc.alloc([8]u8, batch_capacity) else &.{};
    defer if (use_buffers) alloc.free(score_buffers);
    const output_buffers: [][]u8 = if (use_buffers) try alloc.alloc([]u8, batch_capacity) else &.{};
    defer if (use_buffers) alloc.free(output_buffers);
    for (score_buffers, output_buffers) |*buffer, *out| out.* = buffer;
    const read_values = try alloc.alloc(?[]const u8, batch_capacity);
    defer alloc.free(read_values);
    var key_bytes = std.ArrayListUnmanaged(u8).empty;
    defer key_bytes.deinit(alloc);
    var stats = Stats{ .keys = total };
    var offset: usize = 0;
    while (offset < total) {
        var len: usize = 0;
        var bytes: usize = 0;
        for (0..@min(batch_capacity, total - offset)) |i| {
            const flat = offset + i;
            const prefix = prefixes[owners[flat / unique_count]].?;
            const node = nodes[unique[flat % unique_count]];
            const key_len = std.math.add(usize, prefix.len, keys.encodedComponentLen(node)) catch return error.GraphMetricQueryBudgetExceeded;
            // One oversized key may progress, subject to the caller's live
            // allocation budget. Never multiply long IDs by the key-count cap.
            if (len != 0 and key_len > max_key_bytes_per_read -| bytes) break;
            bytes = std.math.add(usize, bytes, key_len) catch return error.GraphMetricQueryBudgetExceeded;
            len += 1;
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
        if (scoped and scope != null) {
            if (direct) {
                scope.?.getManySortedInto(read_keys[0..len], output_buffers[0..len], read_values[0..len]) catch |err| switch (err) {
                    error.BufferTooSmall => return error.InvalidGraphMetricScore,
                    else => return err,
                };
            } else try scope.?.getManySorted(read_keys[0..len], read_values[0..len]);
        } else try txn.getManySorted(read_keys[0..len], read_values[0..len]);
        for (read_values[0..len], 0..) |maybe_raw, i| {
            const raw = maybe_raw orelse continue;
            if (raw.len != 8) return error.InvalidGraphMetricScore;
            const score: f64 = @bitCast(std.mem.readInt(u64, raw[0..8], .little));
            if (!std.math.isFinite(score)) return error.InvalidGraphMetricScore;
            const flat = offset + i;
            columns[owners[flat / unique_count]][unique[flat % unique_count]] = score;
        }
        if (scoped and @hasDecl(Scope, "reset")) {
            if (scope) |*active| {
                if (@typeInfo(@TypeOf(active.reset())) == .error_union) {
                    active.reset() catch |err| {
                        if (err != error.ReadScopeResetUnsupported) return err;
                    };
                } else active.reset();
            }
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

test "graph metric physical score reads bound encoded bytes as well as key count" {
    const alloc = std.testing.allocator;
    const buffers = try alloc.alloc([4096]u8, 600);
    defer alloc.free(buffers);
    const nodes = try alloc.alloc([]const u8, buffers.len);
    defer alloc.free(nodes);
    for (buffers, nodes, 0..) |*buffer, *node, i| {
        @memset(buffer, 'x');
        _ = try std.fmt.bufPrint(buffer[4090..], "{d:0>6}", .{i});
        node.* = buffer;
    }
    const column = try alloc.alloc(?f64, nodes.len);
    defer alloc.free(column);
    const Txn = struct {
        raw: [8]u8 = @bitCast(@as(f64, 3)),
        pub fn getManySorted(self: *@This(), requested: []const []const u8, values: []?[]const u8) !void {
            var bytes: usize = 0;
            for (requested, values) |key, *value| {
                bytes += key.len;
                value.* = &self.raw;
            }
            try std.testing.expect(bytes <= max_key_bytes_per_read or requested.len == 1);
        }
    };
    var txn = Txn{};
    const result = try populate(alloc, &txn, &.{"prefix/"}, nodes, &.{column});
    try std.testing.expectEqual(@as(usize, 3), result.batches);
    try std.testing.expectEqual(nodes.len, result.keys);
    for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
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

test "graph metric physical score reads use caller buffers and close scope on errors" {
    const backend = @import("../storage/backend_erased.zig");
    const Txn = struct {
        calls: usize = 0,
        closed: usize = 0,
        resets: usize = 0,
        length: usize = 8,
        raw: [9]u8 = .{ 0, 0, 0, 0, 0, 0, 8, 64, 0 }, // 3.0, little endian
        const Self = @This();
        const Scope = struct {
            parent: *Self,
            pub fn get(_: *@This(), _: []const u8) ![]const u8 {
                return error.TestUnexpectedResult;
            }
            pub fn getManySortedInto(self: *@This(), requested: []const []const u8, buffers: []const []u8, values: []?[]const u8) !void {
                self.parent.calls += 1;
                for (requested, buffers, values) |_, buffer, *value| {
                    if (self.parent.length > buffer.len) return error.BufferTooSmall;
                    @memcpy(buffer[0..self.parent.length], self.parent.raw[0..self.parent.length]);
                    value.* = buffer[0..self.parent.length];
                }
            }
            pub fn reset(self: *@This()) void {
                self.parent.resets += 1;
            }
            pub fn close(self: *@This()) void {
                self.parent.closed += 1;
            }
        };
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
        pub fn openReadScope(self: *@This(), a: std.mem.Allocator) !backend.ReadScope {
            return backend.readScopeFrom(a, Scope{ .parent = self });
        }
    };
    var txn = Txn{};
    var scores: [3]?f64 = undefined;
    _ = try populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 3, 3, 3 }, &scores);
    try std.testing.expectEqual(@as(usize, 1), txn.calls);
    try std.testing.expectEqual(@as(usize, 1), txn.resets);
    try std.testing.expectEqual(@as(usize, 1), txn.closed);
    txn.length = 9;
    try std.testing.expectError(error.InvalidGraphMetricScore, populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores}));
    try std.testing.expectEqual(@as(usize, 2), txn.closed);
    txn.length = 7;
    try std.testing.expectError(error.InvalidGraphMetricScore, populate(std.testing.allocator, &txn, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores}));
    try std.testing.expectEqual(@as(usize, 3), txn.closed);
}

test "graph metric physical score reads preserve probe-only and legacy scope fallbacks" {
    const Probe = struct {
        pub fn openReadScope(_: *@This(), _: std.mem.Allocator) !@import("../storage/backend_erased.zig").ReadScope {
            return error.ReadOnly;
        }
        raw: [8]u8 = @bitCast(@as(f64, 3)),
        pub fn getManySorted(self: *@This(), _: []const []const u8, values: []?[]const u8) !void {
            for (values) |*value| value.* = &self.raw;
        }
    };
    var probe = Probe{};
    var scores: [2]?f64 = undefined;
    _ = try populate(std.testing.allocator, &probe, &.{"p/"}, &.{ "b", "a" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 3, 3 }, &scores);
    const Legacy = struct {
        const Scope = struct {
            raw: [8]u8 = @bitCast(@as(f64, 4)),
            pub fn getManySorted(self: *@This(), _: []const []const u8, values: []?[]const u8) !void {
                for (values) |*value| value.* = &self.raw;
            }
            pub fn reset(_: *@This()) void {}
            pub fn close(_: *@This()) void {}
        };
        pub fn openReadScope(_: *@This(), _: std.mem.Allocator) !Scope {
            return .{};
        }
        pub fn getManySorted(_: *@This(), _: []const []const u8, _: []?[]const u8) !void {
            return error.TestUnexpectedResult;
        }
    };
    var legacy = Legacy{};
    _ = try populate(std.testing.allocator, &legacy, &.{"p/"}, &.{ "b", "a" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 4, 4 }, &scores);
}
