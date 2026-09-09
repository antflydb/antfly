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

//! Construction-time ordinal graph. Identifiers are owned once, independent
//! of degree; only integer edges survive between input documents/batches.
const std = @import("std");
const wire = @import("packed.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const edge_type = @import("../../graph/edge_type.zig");
const Allocator = std.mem.Allocator;

const Dictionary = struct {
    values: std.StringArrayHashMapUnmanaged(bool) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.values.keys()) |key| alloc.free(key);
        self.values.deinit(alloc);
    }

    fn intern(self: *@This(), alloc: Allocator, key: []const u8, local: bool) !u32 {
        if (self.values.getIndex(key)) |i| {
            self.values.values()[i] = self.values.values()[i] or local;
            return @intCast(i);
        }
        const ordinal = std.math.cast(u32, self.values.count()) orelse return error.GraphSegmentTooLarge;
        const owned = try alloc.dupe(u8, key);
        errdefer alloc.free(owned);
        try self.values.put(alloc, owned, local);
        return ordinal;
    }

    fn orderAlloc(self: *const @This(), alloc: Allocator) ![]u32 {
        const order = try alloc.alloc(u32, self.values.count());
        for (order, 0..) |*value, i| value.* = @intCast(i);
        std.mem.sort(u32, order, self.values.keys(), struct {
            fn less(keys: []const []const u8, a: u32, b: u32) bool {
                return std.mem.order(u8, keys[a], keys[b]) == .lt;
            }
        }.less);
        return order;
    }
};

pub const Edge = struct {
    source: u32,
    target: u32,
    kind: u32,
    table: u32,
    weight: f32,

    fn less(_: void, a: Edge, b: Edge) bool {
        if (a.source != b.source) return a.source < b.source;
        if (a.kind != b.kind) return a.kind < b.kind;
        if (a.target != b.target) return a.target < b.target;
        if (a.weight != b.weight) return a.weight < b.weight;
        return a.table < b.table;
    }
};

pub const Builder = struct {
    alloc: Allocator,
    nodes: Dictionary = .{},
    kinds: Dictionary = .{},
    tables: Dictionary = .{},
    edges: std.ArrayListUnmanaged(Edge) = .empty,
    local_nodes: usize = 0,

    pub fn deinit(self: *@This()) void {
        self.nodes.deinit(self.alloc);
        self.kinds.deinit(self.alloc);
        self.tables.deinit(self.alloc);
        self.edges.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn nodeCount(self: *const @This()) usize {
        return self.local_nodes;
    }

    pub fn addNode(self: *@This(), node: []const u8) !void {
        _ = try self.internNode(node, true);
    }

    fn internNode(self: *@This(), node: []const u8, local: bool) !u32 {
        const was_local = self.nodes.values.get(node) orelse false;
        const id = try self.nodes.intern(self.alloc, node, local);
        if (local and !was_local) self.local_nodes += 1;
        return id;
    }

    pub fn addEdge(self: *@This(), source: []const u8, target: []const u8, kind: []const u8, weight: f32, table: ?[]const u8) !void {
        if (!std.math.isFinite(weight)) return error.InvalidGraphSegment;
        try edge_type.validateStored(kind);
        if (table) |name| if (name.len == 0) return error.InvalidGraphSegment;
        const src = try self.internNode(source, true);
        const dst = try self.internNode(target, table == null);
        const typ = try self.kinds.intern(self.alloc, kind, false);
        const tbl = if (table) |name| try self.tables.intern(self.alloc, name, false) else wire.no_table;
        try self.edges.append(self.alloc, .{ .source = src, .target = dst, .kind = typ, .table = tbl, .weight = weight });
    }

    /// Does not consume the builder. Dictionary/edge ordering is canonical,
    /// including table-qualified duplicate endpoints and input permutations.
    pub fn encodeAlloc(self: *const @This(), max_bytes: usize, cancellation: CancellationToken) ![]u8 {
        try cancellation.check();
        const node_order = try self.nodes.orderAlloc(self.alloc);
        defer self.alloc.free(node_order);
        const type_order = try self.kinds.orderAlloc(self.alloc);
        defer self.alloc.free(type_order);
        const table_order = try self.tables.orderAlloc(self.alloc);
        defer self.alloc.free(table_order);
        const node_map = try invert(self.alloc, node_order);
        defer self.alloc.free(node_map);
        const type_map = try invert(self.alloc, type_order);
        defer self.alloc.free(type_map);
        const table_map = try invert(self.alloc, table_order);
        defer self.alloc.free(table_map);
        var local_edges: usize = 0;
        for (self.edges.items) |edge| local_edges += @intFromBool(edge.table == wire.no_table);
        var size: usize = wire.header_len;
        for ([_]*const Dictionary{ &self.nodes, &self.kinds, &self.tables }) |dict| {
            for (dict.values.keys()) |key| {
                _ = std.math.cast(u32, key.len) orelse return error.GraphSegmentTooLarge;
                size = std.math.add(usize, size, std.math.add(usize, key.len, 4) catch return error.GraphSegmentTooLarge) catch return error.GraphSegmentTooLarge;
            }
        }
        const record_count = std.math.add(usize, self.edges.items.len, local_edges) catch return error.GraphSegmentTooLarge;
        size = std.math.add(usize, size, std.math.mul(usize, record_count, wire.edge_len) catch return error.GraphSegmentTooLarge) catch return error.GraphSegmentTooLarge;
        size = std.math.add(usize, size, std.math.mul(usize, self.nodeCount(), 12) catch return error.GraphSegmentTooLarge) catch return error.GraphSegmentTooLarge;
        if (size > max_bytes) return error.GraphSegmentTooLarge;
        const out = try self.alloc.alloc(Edge, self.edges.items.len);
        defer self.alloc.free(out);
        const in = try self.alloc.alloc(Edge, local_edges);
        defer self.alloc.free(in);
        var in_count: usize = 0;
        for (self.edges.items, out, 0..) |edge, *mapped, i| {
            if (i % 4096 == 0) try cancellation.check();
            mapped.* = .{ .source = node_map[edge.source], .target = node_map[edge.target], .kind = type_map[edge.kind], .table = if (edge.table == wire.no_table) wire.no_table else table_map[edge.table], .weight = edge.weight };
            if (edge.table == wire.no_table) {
                in[in_count] = mapped.*;
                std.mem.swap(u32, &in[in_count].source, &in[in_count].target);
                in_count += 1;
            }
        }
        std.mem.sort(Edge, out, {}, Edge.less);
        try cancellation.check();
        std.mem.sort(Edge, in, {}, Edge.less);
        try cancellation.check();
        const bytes = try self.alloc.alloc(u8, size);
        errdefer self.alloc.free(bytes);
        @memcpy(bytes[0..4], wire.wire_magic);
        std.mem.writeInt(u16, bytes[4..6], wire.wire_version, .little);
        var pos: usize = 6;
        put(bytes, &pos, @intCast(table_order.len));
        put(bytes, &pos, @intCast(node_order.len));
        put(bytes, &pos, @intCast(type_order.len));
        put(bytes, &pos, @intCast(self.nodeCount()));
        inline for (.{ .{ &self.tables, table_order }, .{ &self.nodes, node_order }, .{ &self.kinds, type_order } }) |pair| {
            for (pair[1]) |i| {
                const key = pair[0].values.keys()[i];
                put(bytes, &pos, @intCast(key.len));
                @memcpy(bytes[pos..][0..key.len], key);
                pos += key.len;
            }
        }
        var out_pos: usize = 0;
        var in_pos: usize = 0;
        for (node_order, 0..) |old_node, node| {
            if (node % 256 == 0) try cancellation.check();
            if (!self.nodes.values.values()[old_node]) continue;
            const out_start = out_pos;
            while (out_pos < out.len and out[out_pos].source == node) : (out_pos += 1) {}
            const in_start = in_pos;
            while (in_pos < in.len and in[in_pos].source == node) : (in_pos += 1) {}
            put(bytes, &pos, @intCast(node));
            put(bytes, &pos, std.math.cast(u32, out_pos - out_start) orelse return error.GraphSegmentTooLarge);
            put(bytes, &pos, std.math.cast(u32, in_pos - in_start) orelse return error.GraphSegmentTooLarge);
            for ([_][]const Edge{ out[out_start..out_pos], in[in_start..in_pos] }) |edges| for (edges, 0..) |edge, i| {
                if (i % 4096 == 0) try cancellation.check();
                put(bytes, &pos, edge.target);
                put(bytes, &pos, edge.kind);
                put(bytes, &pos, @bitCast(edge.weight));
                put(bytes, &pos, edge.table);
            };
        }
        std.debug.assert(pos == bytes.len and out_pos == out.len and in_pos == in.len);
        return bytes;
    }
};

fn invert(alloc: Allocator, order: []const u32) ![]u32 {
    const result = try alloc.alloc(u32, order.len);
    for (order, 0..) |old, new| result[old] = @intCast(new);
    return result;
}

fn put(bytes: []u8, pos: *usize, value: u32) void {
    std.mem.writeInt(u32, bytes[pos.*..][0..4], value, .little);
    pos.* += 4;
}

test "serverless ordinal graph builder owns identifiers and canonicalizes directions and qualified endpoints" {
    const alloc = std.testing.allocator;
    var first = Builder{ .alloc = alloc };
    defer first.deinit();
    var second = Builder{ .alloc = alloc };
    defer second.deinit();
    try first.addEdge("b", "a", "link", 1, null);
    try first.addEdge("a", "remote", "link", 2, "other");
    try first.addNode("isolated");
    try second.addNode("isolated");
    try second.addEdge("a", "remote", "link", 2, "other");
    try second.addEdge("b", "a", "link", 1, null);
    const a = try first.encodeAlloc(4096, .none);
    defer alloc.free(a);
    const b = try second.encodeAlloc(4096, .none);
    defer alloc.free(b);
    try std.testing.expectEqualSlices(u8, a, b);
    var view = try wire.viewAlloc(alloc, a, .{}, .none);
    defer view.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), view.adjacencies.len);
    try std.testing.expectEqual(@as(usize, 4), view.nodes.len);
    try std.testing.expectEqual(@as(usize, wire.edge_len), view.adjacencies[0].in.len);
    try std.testing.expectError(error.GraphSegmentTooLarge, first.encodeAlloc(a.len - 1, .none));
}

test "serverless ordinal graph builder allocation failure is recoverable by destruction" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: Allocator) !void {
            var builder = Builder{ .alloc = alloc };
            defer builder.deinit();
            try builder.addEdge("source", "target", "link", 1, null);
            try builder.addEdge("source", "remote", "link", 2, "table");
            const bytes = try builder.encodeAlloc(4096, .none);
            defer alloc.free(bytes);
        }
    }.run, .{});
}
