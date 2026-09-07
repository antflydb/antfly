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

//! Current graph wire: sorted node/type dictionaries and fixed ordinal edges.
//! Views borrow authenticated bytes; decoding does not allocate per edge.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const edge_type = @import("../../graph/edge_type.zig");
const bounded = @import("../bounded_decode.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
pub const wire_magic = "AFSG";
pub const wire_version: u16 = 3;
pub const header_len = 22;
pub const edge_len = 16;
pub const no_table = std.math.maxInt(u32);

pub fn viewRetainedBytes(data: []const u8) !usize {
    if (data.len < header_len or !std.mem.eql(u8, data[0..4], wire_magic)) return error.InvalidGraphSegment;
    if (std.mem.readInt(u16, data[4..6], .little) != wire_version) return error.UnsupportedGraphSegmentVersion;
    const strings = @as(u64, std.mem.readInt(u32, data[6..10], .little)) + std.mem.readInt(u32, data[10..14], .little) + std.mem.readInt(u32, data[14..18], .little);
    const adjacency_count = std.mem.readInt(u32, data[18..22], .little);
    return std.math.cast(usize, strings * @sizeOf([]const u8) + @as(u64, adjacency_count) * @sizeOf(Adjacency)) orelse error.InvalidGraphSegment;
}

const Dictionary = struct {
    map: std.StringHashMapUnmanaged(u32) = .empty,
    values: std.ArrayListUnmanaged([]const u8) = .empty,
    fn deinit(self: *@This(), alloc: Allocator) void {
        self.map.deinit(alloc);
        self.values.deinit(alloc);
    }
    fn add(self: *@This(), alloc: Allocator, value: []const u8) !void {
        const entry = try self.map.getOrPut(alloc, value);
        if (entry.found_existing) return;
        entry.value_ptr.* = std.math.cast(u32, self.values.items.len) orelse return error.GraphSegmentTooLarge;
        try self.values.append(alloc, value);
    }
    fn finish(self: *@This()) void {
        std.mem.sort([]const u8, self.values.items, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        for (self.values.items, 0..) |value, i| self.map.getPtr(value).?.* = @intCast(i);
    }
};

const Encoding = struct {
    nodes: Dictionary = .{},
    edge_types: Dictionary = .{},
    size: usize = header_len,
    fn deinit(self: *@This(), alloc: Allocator) void {
        self.nodes.deinit(alloc);
        self.edge_types.deinit(alloc);
    }
    fn init(alloc: Allocator, segment: types.Segment, cancellation: CancellationToken) !Encoding {
        var plan = Encoding{};
        errdefer plan.deinit(alloc);
        _ = std.math.cast(u32, segment.neighbor_tables.len) orelse return error.GraphSegmentTooLarge;
        _ = std.math.cast(u32, segment.adjacencies.len) orelse return error.GraphSegmentTooLarge;
        for (segment.adjacencies, 0..) |adjacency, ordinal| {
            if (ordinal % 256 == 0) try cancellation.check();
            try plan.nodes.add(alloc, adjacency.node_id);
            for ([_][]const types.Edge{ adjacency.out_edges, adjacency.in_edges }) |edges| {
                _ = std.math.cast(u32, edges.len) orelse return error.GraphSegmentTooLarge;
                for (edges, 0..) |edge, i| {
                    if (i % 4096 == 0) try cancellation.check();
                    if (edge.neighbor_table_id) |id| if (id >= segment.neighbor_tables.len) return error.InvalidGraphSegment;
                    edge_type.validateStored(edge.edge_type) catch return error.InvalidGraphSegment;
                    try plan.nodes.add(alloc, edge.neighbor_id);
                    try plan.edge_types.add(alloc, edge.edge_type);
                }
                plan.size = std.math.add(usize, plan.size, std.math.mul(usize, edges.len, edge_len) catch return error.GraphSegmentTooLarge) catch return error.GraphSegmentTooLarge;
            }
            plan.size = std.math.add(usize, plan.size, 12) catch return error.GraphSegmentTooLarge;
        }
        try cancellation.check();
        plan.nodes.finish();
        plan.edge_types.finish();
        try cancellation.check();
        for (segment.neighbor_tables) |table| {
            if (table.len == 0) return error.InvalidGraphSegment;
            try plan.addStringSize(table);
        }
        for (plan.nodes.values.items) |value| try plan.addStringSize(value);
        for (plan.edge_types.values.items) |value| try plan.addStringSize(value);
        return plan;
    }
    fn addStringSize(self: *@This(), value: []const u8) !void {
        _ = std.math.cast(u32, value.len) orelse return error.GraphSegmentTooLarge;
        self.size = std.math.add(usize, self.size, 4) catch return error.GraphSegmentTooLarge;
        self.size = std.math.add(usize, self.size, value.len) catch return error.GraphSegmentTooLarge;
    }
};

pub fn encodedSize(alloc: Allocator, segment: types.Segment) !usize {
    var plan = try Encoding.init(alloc, segment, .none);
    defer plan.deinit(alloc);
    return plan.size;
}

fn put(buf: []u8, pos: *usize, value: u32) void {
    std.mem.writeInt(u32, buf[pos.*..][0..4], value, .little);
    pos.* += 4;
}
fn putString(buf: []u8, pos: *usize, value: []const u8) void {
    put(buf, pos, @intCast(value.len));
    @memcpy(buf[pos.*..][0..value.len], value);
    pos.* += value.len;
}

pub fn encodeAlloc(alloc: Allocator, segment: types.Segment) ![]u8 {
    return encodeAllocWithLimit(alloc, segment, std.math.maxInt(usize), .none);
}

/// Build dictionaries once and enforce the output cap before allocating bytes.
pub fn encodeAllocWithLimit(alloc: Allocator, segment: types.Segment, max_bytes: usize, cancellation: CancellationToken) ![]u8 {
    var plan = try Encoding.init(alloc, segment, cancellation);
    defer plan.deinit(alloc);
    if (plan.size > max_bytes) return error.GraphSegmentTooLarge;
    const buf = try alloc.alloc(u8, plan.size);
    errdefer alloc.free(buf);
    @memcpy(buf[0..4], wire_magic);
    std.mem.writeInt(u16, buf[4..6], wire_version, .little);
    var pos: usize = 6;
    put(buf, &pos, @intCast(segment.neighbor_tables.len));
    put(buf, &pos, @intCast(plan.nodes.values.items.len));
    put(buf, &pos, @intCast(plan.edge_types.values.items.len));
    put(buf, &pos, @intCast(segment.adjacencies.len));
    for (segment.neighbor_tables) |table| putString(buf, &pos, table);
    for (plan.nodes.values.items) |node| putString(buf, &pos, node);
    for (plan.edge_types.values.items) |value| putString(buf, &pos, value);
    for (segment.adjacencies, 0..) |adjacency, ordinal| {
        if (ordinal % 256 == 0) try cancellation.check();
        put(buf, &pos, plan.nodes.map.get(adjacency.node_id).?);
        put(buf, &pos, @intCast(adjacency.out_edges.len));
        put(buf, &pos, @intCast(adjacency.in_edges.len));
        for ([_][]const types.Edge{ adjacency.out_edges, adjacency.in_edges }) |edges| for (edges, 0..) |edge, i| {
            if (i % 4096 == 0) try cancellation.check();
            put(buf, &pos, plan.nodes.map.get(edge.neighbor_id).?);
            put(buf, &pos, plan.edge_types.map.get(edge.edge_type).?);
            put(buf, &pos, @bitCast(edge.weight));
            put(buf, &pos, edge.neighbor_table_id orelse no_table);
        };
    }
    std.debug.assert(pos == buf.len);
    return buf;
}

pub const Edge = struct {
    node: u32,
    edge_type: u32,
    weight: f32,
    table: ?u32,
};
pub fn readEdge(bytes: []const u8, index: usize) Edge {
    const row = bytes[index * edge_len ..][0..edge_len];
    const table = std.mem.readInt(u32, row[12..16], .little);
    return .{ .node = std.mem.readInt(u32, row[0..4], .little), .edge_type = std.mem.readInt(u32, row[4..8], .little), .weight = @bitCast(std.mem.readInt(u32, row[8..12], .little)), .table = if (table == no_table) null else table };
}
pub const Adjacency = struct { node: u32, out: []const u8, in: []const u8 };
pub const View = struct {
    tables: []const []const u8,
    nodes: []const []const u8,
    edge_types: []const []const u8,
    adjacencies: []Adjacency,
    pub fn deinit(self: *View, alloc: Allocator) void {
        alloc.free(self.tables);
        alloc.free(self.nodes);
        alloc.free(self.edge_types);
        alloc.free(self.adjacencies);
        self.* = undefined;
    }
    pub fn retainedBytes(self: View) usize {
        return (self.tables.len + self.nodes.len + self.edge_types.len) * @sizeOf([]const u8) + self.adjacencies.len * @sizeOf(Adjacency);
    }
    pub fn decodedBytes(self: View) !usize {
        var size = self.tables.len * @sizeOf([]u8) + self.adjacencies.len * @sizeOf(types.Adjacency);
        for (self.tables) |table| size = try std.math.add(usize, size, table.len);
        for (self.adjacencies) |adjacency| {
            size = try std.math.add(usize, size, self.nodes[adjacency.node].len);
            for ([_][]const u8{ adjacency.out, adjacency.in }) |edges| {
                size = try std.math.add(usize, size, try std.math.mul(usize, edges.len / edge_len, @sizeOf(types.Edge)));
                for (0..edges.len / edge_len) |i| {
                    const edge = readEdge(edges, i);
                    size = try std.math.add(usize, size, self.nodes[edge.node].len + self.edge_types[edge.edge_type].len);
                }
            }
        }
        return size;
    }
};

const Cursor = struct {
    bytes: []const u8,
    pos: usize = header_len,
    fn take(self: *@This(), len: usize) ![]const u8 {
        if (len > self.bytes.len - self.pos) return error.InvalidGraphSegment;
        defer self.pos += len;
        return self.bytes[self.pos..][0..len];
    }
    fn int(self: *@This()) !u32 {
        return std.mem.readInt(u32, (try self.take(4))[0..4], .little);
    }
    fn strings(self: *@This(), alloc: Allocator, count: u32, sorted: bool, is_type: bool, cancellation: CancellationToken) ![][]const u8 {
        if (count > (self.bytes.len - self.pos) / 4) return error.InvalidGraphSegment;
        const values = try alloc.alloc([]const u8, count);
        errdefer alloc.free(values);
        for (values, 0..) |*value, i| {
            if (i % 256 == 0) try cancellation.check();
            value.* = try self.take(try self.int());
            if (is_type and !edge_type.isValid(value.*)) return error.InvalidGraphSegment;
            if (sorted and i > 0 and std.mem.order(u8, values[i - 1], value.*) != .lt) return error.InvalidGraphSegment;
        }
        return values;
    }
};

pub fn viewAlloc(alloc: Allocator, data: []const u8, limits: bounded.Limits, cancellation: CancellationToken) !View {
    try cancellation.check();
    _ = try bounded.Budget.init(data.len, limits);
    var limiter = try bounded.AllocationLimiter.init(alloc, limits.max_allocation_bytes);
    return readView(limiter.allocator(), data, limits.max_elements, cancellation) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.DecodedArtifactTooLarge;
        return err;
    };
}

fn readView(alloc: Allocator, data: []const u8, max_elements: usize, cancellation: CancellationToken) !View {
    if (data.len < 6 or !std.mem.eql(u8, data[0..4], wire_magic)) return error.InvalidGraphSegment;
    if (std.mem.readInt(u16, data[4..6], .little) != wire_version) return error.UnsupportedGraphSegmentVersion;
    if (data.len < header_len) return error.InvalidGraphSegment;
    const table_count = std.mem.readInt(u32, data[6..10], .little);
    const node_count = std.mem.readInt(u32, data[10..14], .little);
    const type_count = std.mem.readInt(u32, data[14..18], .little);
    const adjacency_count = std.mem.readInt(u32, data[18..22], .little);
    if (@as(u64, table_count) + node_count + type_count > (data.len - header_len) / 4 or
        adjacency_count > (data.len - header_len) / 12) return error.InvalidGraphSegment;
    var elements: u64 = @as(u64, table_count) + node_count + type_count + adjacency_count;
    if (elements > max_elements) return error.DecodedArtifactTooLarge;
    var cursor = Cursor{ .bytes = data };
    const tables = try cursor.strings(alloc, table_count, false, false, cancellation);
    errdefer alloc.free(tables);
    for (tables) |table| if (table.len == 0) return error.InvalidGraphSegment;
    const nodes = try cursor.strings(alloc, node_count, true, false, cancellation);
    errdefer alloc.free(nodes);
    const edge_types = try cursor.strings(alloc, type_count, true, true, cancellation);
    errdefer alloc.free(edge_types);
    if (adjacency_count > (data.len - cursor.pos) / 12) return error.InvalidGraphSegment;
    const adjacencies = try alloc.alloc(Adjacency, adjacency_count);
    errdefer alloc.free(adjacencies);
    for (adjacencies, 0..) |*adjacency, i| {
        if (i % 256 == 0) try cancellation.check();
        const node = try cursor.int();
        const out_count = try cursor.int();
        const in_count = try cursor.int();
        if (node >= nodes.len) return error.InvalidGraphSegment;
        elements += @as(u64, out_count) + in_count;
        if (elements > max_elements) return error.DecodedArtifactTooLarge;
        const out = try cursor.take(std.math.mul(usize, out_count, edge_len) catch return error.InvalidGraphSegment);
        const in = try cursor.take(std.math.mul(usize, in_count, edge_len) catch return error.InvalidGraphSegment);
        for ([_][]const u8{ out, in }) |edges| {
            var previous: ?Edge = null;
            for (0..edges.len / edge_len) |e| {
                if (e % 4096 == 0) try cancellation.check();
                const edge = readEdge(edges, e);
                if (edge.node >= nodes.len or edge.edge_type >= edge_types.len or !std.math.isFinite(edge.weight)) return error.InvalidGraphSegment;
                if (edge.table) |id| if (id >= tables.len) return error.InvalidGraphSegment;
                if (previous) |prior| {
                    // Dictionaries are sorted, so canonical order is numeric.
                    const order = std.math.order(prior.edge_type, edge.edge_type);
                    const node_order = std.math.order(prior.node, edge.node);
                    if (order == .gt or (order == .eq and (node_order == .gt or (node_order == .eq and prior.weight > edge.weight)))) return error.InvalidGraphSegment;
                }
                previous = edge;
            }
        }
        adjacency.* = .{ .node = node, .out = out, .in = in };
    }
    if (cursor.pos != data.len) return error.InvalidGraphSegment;
    return .{ .tables = tables, .nodes = nodes, .edge_types = edge_types, .adjacencies = adjacencies };
}

pub fn decodedRetainedBytes(alloc: Allocator, data: []const u8) !usize {
    var view = try viewAlloc(alloc, data, .{}, .none);
    defer view.deinit(alloc);
    return view.decodedBytes();
}

pub fn decodeAllocWithLimitsAndCancellation(alloc: Allocator, data: []const u8, limits: bounded.Limits, cancellation: CancellationToken) !types.Segment {
    var view = try viewAlloc(alloc, data, limits, cancellation);
    defer view.deinit(alloc);
    const owned_bytes = try view.decodedBytes();
    if (owned_bytes > limits.max_allocation_bytes -| view.retainedBytes()) return error.DecodedArtifactTooLarge;
    return decodeViewAlloc(alloc, view, cancellation);
}

/// Materialize an already validated view. The caller admits decodedBytes()
/// before this allocation; borrowed view storage remains live until return.
pub fn decodeViewAlloc(alloc: Allocator, view: View, cancellation: CancellationToken) !types.Segment {
    const tables = try alloc.alloc([]u8, view.tables.len);
    var count: usize = 0;
    errdefer {
        for (tables[0..count]) |table| alloc.free(table);
        alloc.free(tables);
    }
    for (view.tables, tables) |table, *copy| {
        copy.* = try alloc.dupe(u8, table);
        count += 1;
    }
    const adjacencies = try alloc.alloc(types.Adjacency, view.adjacencies.len);
    var initialized: usize = 0;
    errdefer {
        for (adjacencies[0..initialized]) |*adjacency| adjacency.deinit(alloc);
        alloc.free(adjacencies);
    }
    for (view.adjacencies, adjacencies, 0..) |adjacency, *copy, i| {
        if (i % 256 == 0) try cancellation.check();
        const node = try alloc.dupe(u8, view.nodes[adjacency.node]);
        errdefer alloc.free(node);
        const out = try copyEdges(alloc, view, adjacency.out, cancellation);
        errdefer {
            for (out) |*edge| edge.deinit(alloc);
            alloc.free(out);
        }
        copy.* = .{ .node_id = node, .out_edges = out, .in_edges = try copyEdges(alloc, view, adjacency.in, cancellation) };
        initialized += 1;
    }
    return .{ .neighbor_tables = tables, .adjacencies = adjacencies };
}

fn copyEdges(alloc: Allocator, view: View, bytes: []const u8, cancellation: CancellationToken) ![]types.Edge {
    const edges = try alloc.alloc(types.Edge, bytes.len / edge_len);
    var initialized: usize = 0;
    errdefer {
        for (edges[0..initialized]) |*edge| edge.deinit(alloc);
        alloc.free(edges);
    }
    for (edges, 0..) |*copy, i| {
        if (i % 4096 == 0) try cancellation.check();
        const edge = readEdge(bytes, i);
        const node = try alloc.dupe(u8, view.nodes[edge.node]);
        errdefer alloc.free(node);
        copy.* = .{ .neighbor_id = node, .edge_type = try alloc.dupe(u8, view.edge_types[edge.edge_type]), .weight = edge.weight, .neighbor_table_id = edge.table };
        initialized += 1;
    }
    return edges;
}

test "serverless packed graph ownership and ordinal validation are failure safe" {
    const alloc = std.testing.allocator;
    const edge = types.Edge{ .neighbor_id = @constCast("b"), .edge_type = @constCast("follows"), .weight = 1 };
    const segment = types.Segment{ .adjacencies = @constCast(&[_]types.Adjacency{
        .{ .node_id = @constCast("a"), .out_edges = @constCast(&[_]types.Edge{edge}), .in_edges = &.{} },
        .{ .node_id = @constCast("b"), .out_edges = &.{}, .in_edges = &.{} },
    }) };
    const Runner = struct {
        fn run(failing: Allocator, fixture: types.Segment) !void {
            const payload = try encodeAlloc(failing, fixture);
            defer failing.free(payload);
            var view = try viewAlloc(failing, payload, .{}, .none);
            defer view.deinit(failing);
            var decoded = try decodeAllocWithLimitsAndCancellation(failing, payload, .{}, .none);
            defer decoded.deinit(failing);
            try std.testing.expectEqualStrings("b", decoded.adjacencies[0].out_edges[0].neighbor_id);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Runner.run, .{segment});
    const payload = try encodeAlloc(alloc, segment);
    defer alloc.free(payload);
    for (0..payload.len) |len| {
        if (viewAlloc(alloc, payload[0..len], .{}, .none)) |valid| {
            var owned = valid;
            owned.deinit(alloc);
            return error.AcceptedTruncatedGraph;
        } else |_| {}
    }
    var view = try viewAlloc(alloc, payload, .{}, .none);
    const edge_offset = @intFromPtr(view.adjacencies[0].out.ptr) - @intFromPtr(payload.ptr);
    view.deinit(alloc);
    for ([_]usize{ 0, 4, 8, 12 }) |field| {
        const saved = std.mem.readInt(u32, payload[edge_offset + field ..][0..4], .little);
        // NaN for weight; out-of-range node/type/table ordinals otherwise.
        const corrupt: u32 = if (field == 8) 0x7fc00000 else std.math.maxInt(u32) - 1;
        std.mem.writeInt(u32, payload[edge_offset + field ..][0..4], corrupt, .little);
        try std.testing.expectError(error.InvalidGraphSegment, viewAlloc(alloc, payload, .{}, .none));
        std.mem.writeInt(u32, payload[edge_offset + field ..][0..4], saved, .little);
    }
    try std.testing.expectError(error.DecodedArtifactTooLarge, viewAlloc(alloc, payload, .{ .max_allocation_bytes = 1 }, .none));
    std.mem.writeInt(u16, payload[4..6], 2, .little);
    try std.testing.expectError(error.UnsupportedGraphSegmentVersion, viewAlloc(alloc, payload, .{}, .none));
}
