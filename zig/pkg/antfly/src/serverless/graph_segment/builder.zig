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
        const body_len = size;
        size = std.math.add(usize, size, wire.topologyDirectorySize(self.kinds.values.keys(), self.nodes.values.count(), self.nodeCount(), self.tables.values.count()) + wire.topology_trailer_len) catch return error.GraphSegmentTooLarge;
        if (size > max_bytes) return error.GraphSegmentTooLarge;
        // Count and scatter directly into final adjacency storage. No mapped
        // forward/reverse Edge arrays coexist with the immutable wire payload.
        const counts = try self.alloc.alloc([2]u32, node_order.len);
        defer self.alloc.free(counts);
        @memset(counts, .{ 0, 0 });
        for (self.edges.items, 0..) |edge, i| {
            if (i % 4096 == 0) try cancellation.check();
            const src = node_map[edge.source];
            counts[src][0] = std.math.add(u32, counts[src][0], 1) catch return error.GraphSegmentTooLarge;
            if (edge.table == wire.no_table) {
                const dst = node_map[edge.target];
                counts[dst][1] = std.math.add(u32, counts[dst][1], 1) catch return error.GraphSegmentTooLarge;
            }
        }
        const positions = try self.alloc.alloc([2]usize, node_order.len);
        defer self.alloc.free(positions);
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
        for (node_order, 0..) |old_node, node| {
            if (node % 256 == 0) try cancellation.check();
            if (!self.nodes.values.values()[old_node]) continue;
            put(bytes, &pos, @intCast(node));
            put(bytes, &pos, counts[node][0]);
            put(bytes, &pos, counts[node][1]);
            for (0..2) |direction| {
                positions[node][direction] = pos;
                pos += @as(usize, counts[node][direction]) * wire.edge_len;
            }
        }
        std.debug.assert(pos == body_len);
        for (self.edges.items, 0..) |edge, i| {
            if (i % 4096 == 0) try cancellation.check();
            const src = node_map[edge.source];
            const dst = node_map[edge.target];
            writeEdge(bytes, &positions[src][0], dst, type_map[edge.kind], edge.weight, if (edge.table == wire.no_table) wire.no_table else table_map[edge.table]);
            if (edge.table == wire.no_table) writeEdge(bytes, &positions[dst][1], src, type_map[edge.kind], edge.weight, wire.no_table);
        }
        for (node_order, 0..) |old_node, node| {
            if (!self.nodes.values.values()[old_node]) continue;
            for (0..2) |direction| {
                try cancellation.check();
                const end = positions[node][direction];
                const start = end - @as(usize, counts[node][direction]) * wire.edge_len;
                const records = std.mem.bytesAsSlice([wire.edge_len]u8, bytes[start..end]);
                std.mem.sort([wire.edge_len]u8, records, {}, wireEdgeLess);
            }
        }
        try wire.finishEncoding(self.alloc, bytes, body_len, cancellation);
        return bytes;
    }
};

fn writeEdge(bytes: []u8, pos: *usize, target: u32, kind: u32, weight: f32, table: u32) void {
    put(bytes, pos, target);
    put(bytes, pos, kind);
    put(bytes, pos, @bitCast(weight));
    put(bytes, pos, table);
}

fn wireEdgeLess(_: void, a: [wire.edge_len]u8, b: [wire.edge_len]u8) bool {
    const kind_a = std.mem.readInt(u32, a[4..8], .little);
    const kind_b = std.mem.readInt(u32, b[4..8], .little);
    if (kind_a != kind_b) return kind_a < kind_b;
    const target_a = std.mem.readInt(u32, a[0..4], .little);
    const target_b = std.mem.readInt(u32, b[0..4], .little);
    if (target_a != target_b) return target_a < target_b;
    const weight_a: f32 = @bitCast(std.mem.readInt(u32, a[8..12], .little));
    const weight_b: f32 = @bitCast(std.mem.readInt(u32, b[8..12], .little));
    if (weight_a != weight_b) return weight_a < weight_b;
    return std.mem.readInt(u32, a[12..16], .little) < std.mem.readInt(u32, b[12..16], .little);
}

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

test "serverless ordinal graph builder packs skewed duplicate and qualified adjacency canonically" {
    const alloc = std.testing.allocator;
    var forward = Builder{ .alloc = alloc };
    defer forward.deinit();
    var reverse = Builder{ .alloc = alloc };
    defer reverse.deinit();
    const nodes = [_][]const u8{ "hub", "a", "b", "remote" };
    const kinds = [_][]const u8{ "z", "a" };
    for (0..512) |i| {
        const j = 511 - i;
        // A high-degree hub, self-loops, repeated edges, negative weights,
        // multiple types and qualified endpoints exercise both orientations.
        try forward.addEdge("hub", nodes[i % nodes.len], kinds[i % kinds.len], @as(f32, @floatFromInt(i % 7)) - 3, if (i % 5 == 0) "other" else null);
        try reverse.addEdge("hub", nodes[j % nodes.len], kinds[j % kinds.len], @as(f32, @floatFromInt(j % 7)) - 3, if (j % 5 == 0) "other" else null);
    }
    const a = try forward.encodeAlloc(64 * 1024, .none);
    defer alloc.free(a);
    const b = try reverse.encodeAlloc(64 * 1024, .none);
    defer alloc.free(b);
    try std.testing.expectEqualSlices(u8, a, b);
    var view = try wire.viewAlloc(alloc, a, .{}, .none);
    defer view.deinit(alloc);
    var outgoing: usize = 0;
    var incoming: usize = 0;
    for (view.adjacencies) |adjacency| {
        outgoing += adjacency.out.len / wire.edge_len;
        incoming += adjacency.in.len / wire.edge_len;
    }
    try std.testing.expectEqual(@as(usize, 512), outgoing);
    try std.testing.expectEqual(@as(usize, 409), incoming);
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
