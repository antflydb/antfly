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

//! Neighbor context for asset-producer enrichments: a bounded sample of a
//! document's graph adjacency rendered into the producer input, so a
//! conceptualizer sees "Black Mountain College --started_by--> John Andrew
//! Rice" alongside the document's own fields. Only same-shard graph state is
//! consulted; cross-shard adjacency is intentionally out of scope. Admission
//! validates the configuration (closed), while the runtime proceeds with
//! empty neighbors when the graph index holds no state for a document (open).

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const default_limit: u32 = 8;
pub const max_limit: u32 = 64;

pub const Direction = enum { out, in, both };

/// Orientation of one sampled edge relative to the source document.
pub const Orientation = enum { out, in };

pub const Config = struct {
    graph_index: []const u8,
    /// Empty admits every edge type.
    edge_types: []const []const u8 = &.{},
    direction: Direction = .both,
    limit: u32 = default_limit,

    pub fn deinit(self: *Config, alloc: Allocator) void {
        alloc.free(@constCast(self.graph_index));
        for (self.edge_types) |edge_type| alloc.free(@constCast(edge_type));
        if (self.edge_types.len > 0) alloc.free(self.edge_types);
        self.* = undefined;
    }
};

pub fn parseConfigJson(alloc: Allocator, raw: []const u8) !Config {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidEnrichmentConfig,
    };
    defer parsed.deinit();
    return try parseConfigValue(alloc, parsed.value);
}

/// Strictly parse one public/catalog `neighbor_context` object. Unknown
/// fields are rejected so a typo cannot silently disable adjacency sampling.
pub fn parseConfigValue(alloc: Allocator, value: std.json.Value) !Config {
    if (value != .object) return error.InvalidEnrichmentConfig;
    var graph_index: ?[]const u8 = null;
    var direction = Direction.both;
    var limit: u32 = default_limit;
    var edge_types = std.ArrayListUnmanaged([]const u8).empty;
    var out: ?Config = null;
    defer if (out == null) {
        for (edge_types.items) |edge_type| alloc.free(@constCast(edge_type));
        edge_types.deinit(alloc);
        if (graph_index) |name| alloc.free(@constCast(name));
    };

    var iter = value.object.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "graph_index")) {
            if (field != .string or field.string.len == 0) return error.InvalidEnrichmentConfig;
            graph_index = try alloc.dupe(u8, field.string);
        } else if (std.mem.eql(u8, key, "edge_types")) {
            if (field != .array) return error.InvalidEnrichmentConfig;
            for (field.array.items) |item| {
                if (item != .string or item.string.len == 0) return error.InvalidEnrichmentConfig;
                try edge_types.append(alloc, try alloc.dupe(u8, item.string));
            }
        } else if (std.mem.eql(u8, key, "direction")) {
            if (field != .string) return error.InvalidEnrichmentConfig;
            direction = std.meta.stringToEnum(Direction, field.string) orelse
                return error.InvalidEnrichmentConfig;
        } else if (std.mem.eql(u8, key, "limit")) {
            if (field != .integer or field.integer < 1 or field.integer > max_limit)
                return error.InvalidEnrichmentConfig;
            limit = @intCast(field.integer);
        } else {
            return error.InvalidEnrichmentConfig;
        }
    }

    out = .{
        .graph_index = graph_index orelse return error.InvalidEnrichmentConfig,
        .edge_types = try edge_types.toOwnedSlice(alloc),
        .direction = direction,
        .limit = limit,
    };
    return out.?;
}

/// One adjacency sample. `neighbor` is the adjacent document key regardless
/// of edge orientation, so callers never re-derive which endpoint is theirs.
pub const NeighborEdge = struct {
    edge_type: []const u8,
    orientation: Orientation,
    neighbor: []const u8,
    weight: f64,
};

/// Render the bounded neighbor block appended to the producer input:
/// {"neighbors":[{"edge_type":...,"direction":"out","target":...,"weight":...}]}
/// Order is deterministic (edge type, then neighbor key, then orientation) so
/// the rendered input — and therefore the producer skip-state hash — is a
/// pure function of the adjacency, not of storage scan order.
pub fn renderNeighborsBlockAlloc(alloc: Allocator, edges: []const NeighborEdge, limit: u32) ![]u8 {
    const order = try alloc.alloc(usize, edges.len);
    defer alloc.free(order);
    for (order, 0..) |*index, i| index.* = i;
    std.mem.sort(usize, order, edges, struct {
        fn lessThan(items: []const NeighborEdge, a: usize, b: usize) bool {
            const lhs = items[a];
            const rhs = items[b];
            switch (std.mem.order(u8, lhs.edge_type, rhs.edge_type)) {
                .lt => return true,
                .gt => return false,
                .eq => {},
            }
            switch (std.mem.order(u8, lhs.neighbor, rhs.neighbor)) {
                .lt => return true,
                .gt => return false,
                .eq => {},
            }
            if (lhs.orientation != rhs.orientation) return lhs.orientation == .out;
            return a < b;
        }
    }.lessThan);

    const count = @min(edges.len, limit);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var neighbors = std.json.Array.init(scratch);
    for (order[0..count]) |index| {
        const edge = edges[index];
        var object = std.json.ObjectMap.empty;
        try object.put(scratch, "edge_type", .{ .string = edge.edge_type });
        try object.put(scratch, "direction", .{ .string = @tagName(edge.orientation) });
        try object.put(scratch, "target", .{ .string = edge.neighbor });
        try object.put(scratch, "weight", .{ .float = edge.weight });
        try neighbors.append(.{ .object = object });
    }
    var root = std.json.ObjectMap.empty;
    try root.put(scratch, "neighbors", .{ .array = neighbors });
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = root }, .{});
}

test "neighbor context config round trips through canonical JSON" {
    const alloc = std.testing.allocator;
    var config = try parseConfigJson(alloc,
        \\{"graph_index":"taxonomy","edge_types":["started_by","located_in"],"direction":"out","limit":4}
    );
    defer config.deinit(alloc);
    try std.testing.expectEqualStrings("taxonomy", config.graph_index);
    try std.testing.expectEqual(@as(usize, 2), config.edge_types.len);
    try std.testing.expectEqualStrings("started_by", config.edge_types[0]);
    try std.testing.expectEqual(Direction.out, config.direction);
    try std.testing.expectEqual(@as(u32, 4), config.limit);

    var defaults = try parseConfigJson(alloc, "{\"graph_index\":\"taxonomy\"}");
    defer defaults.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), defaults.edge_types.len);
    try std.testing.expectEqual(Direction.both, defaults.direction);
    try std.testing.expectEqual(default_limit, defaults.limit);
}

test "neighbor context config enforces bounds and closed fields" {
    const alloc = std.testing.allocator;
    // graph_index is required and non-empty.
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{}"));
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{\"graph_index\":\"\"}"));
    // limit is bounded to 1..max_limit.
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{\"graph_index\":\"g\",\"limit\":0}"));
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{\"graph_index\":\"g\",\"limit\":65}"));
    // direction and edge types are validated, unknown fields rejected.
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{\"graph_index\":\"g\",\"direction\":\"sideways\"}"));
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{\"graph_index\":\"g\",\"edge_types\":[\"\"]}"));
    try std.testing.expectError(error.InvalidEnrichmentConfig, parseConfigJson(alloc, "{\"graph_index\":\"g\",\"hops\":2}"));
    // The maximum limit itself is admitted.
    var config = try parseConfigJson(alloc, "{\"graph_index\":\"g\",\"limit\":64}");
    defer config.deinit(alloc);
    try std.testing.expectEqual(max_limit, config.limit);
}

test "neighbor block renders deterministically and honors the limit" {
    const alloc = std.testing.allocator;
    const edges = [_]NeighborEdge{
        .{ .edge_type = "started_by", .orientation = .out, .neighbor = "entities/person/john_andrew_rice", .weight = 0.98 },
        .{ .edge_type = "located_in", .orientation = .out, .neighbor = "entities/place/north_carolina", .weight = 0.5 },
    };
    const block = try renderNeighborsBlockAlloc(alloc, &edges, default_limit);
    defer alloc.free(block);
    try std.testing.expectEqualStrings(
        "{\"neighbors\":[{\"edge_type\":\"located_in\",\"direction\":\"out\",\"target\":\"entities/place/north_carolina\",\"weight\":0.5},{\"edge_type\":\"started_by\",\"direction\":\"out\",\"target\":\"entities/person/john_andrew_rice\",\"weight\":0.98}]}",
        block,
    );
    // Reversed input order renders the identical block.
    const reversed = [_]NeighborEdge{ edges[1], edges[0] };
    const again = try renderNeighborsBlockAlloc(alloc, &reversed, default_limit);
    defer alloc.free(again);
    try std.testing.expectEqualStrings(block, again);
    // The limit truncates after ordering, keeping the retained sample stable.
    const limited = try renderNeighborsBlockAlloc(alloc, &edges, 1);
    defer alloc.free(limited);
    try std.testing.expectEqualStrings(
        "{\"neighbors\":[{\"edge_type\":\"located_in\",\"direction\":\"out\",\"target\":\"entities/place/north_carolina\",\"weight\":0.5}]}",
        limited,
    );
    const empty = try renderNeighborsBlockAlloc(alloc, &.{}, default_limit);
    defer alloc.free(empty);
    try std.testing.expectEqualStrings("{\"neighbors\":[]}", empty);
}
