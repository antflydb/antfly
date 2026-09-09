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

//! Selected topology preparation from authenticated current-wire ranges.
//! Scratch and retained data scale with selected edges/endpoints. Unrelated
//! node strings are visited only within the touched dictionary pages.
const std = @import("std");
const Allocator = std.mem.Allocator;
const wire = @import("packed.zig");
const artifacts = @import("../artifacts/store.zig");
const refs = @import("../manifest/artifact_ref.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const Edge = struct { source: u32, target: u32 };
pub const Topology = struct {
    node_ids: []const []const u8,
    edge_types: []const []const u8,
    string_bytes: []u8,
    edge_type_offsets: []const u32,
    edges: []const Edge,
    source_node_count: usize,
    source_edge_count: usize,
    retained_bytes: usize,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.node_ids);
        alloc.free(self.edge_types);
        alloc.free(self.string_bytes);
        alloc.free(self.edge_type_offsets);
        alloc.free(self.edges);
        self.* = undefined;
    }
};

const Reader = struct {
    alloc: Allocator,
    store: *artifacts.ArtifactStore,
    source: refs.ArtifactRef,
    cancellation: CancellationToken,
    remaining: *u64,

    fn read(self: @This(), offset: u64, len: u64) ![]u8 {
        try self.cancellation.check();
        if (offset > self.source.byte_len or len > self.source.byte_len - offset) return error.InvalidGraphSegment;
        return self.store.getVerifiedRangeAllocWithBudget(self.alloc, self.source.artifact_id, self.source.byte_len, self.source.checksum, offset, std.math.cast(usize, len) orelse return error.GraphMetricBuildBudgetExceeded, self.cancellation, self.remaining) catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded => error.GraphMetricBuildBudgetExceeded,
            else => err,
        };
    }
};

fn selected(kind: []const u8, configs: anytype) bool {
    for (configs) |config| {
        if (config.edge_filter.mode == .all) return true;
        for (config.edge_filter.types) |name| if (std.mem.eql(u8, name, kind)) return true;
    }
    return false;
}

/// Caller supplies a peak-limited allocator and a shared, byte-accounted read
/// allowance. Cold authentication of the full source is charged by the store;
/// warm exact-identity reads charge only the requested ranges.
pub fn readAlloc(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, configs: anytype, limits: anytype, cancellation: CancellationToken, remaining: *u64) !?Topology {
    if (source.byte_len < wire.topology_trailer_len) return error.InvalidGraphSegment;
    const reader = Reader{ .alloc = alloc, .store = store, .source = source, .cancellation = cancellation, .remaining = remaining };
    const footer = try reader.read(source.byte_len - wire.topology_trailer_len, wire.topology_trailer_len);
    defer alloc.free(footer);
    const trailer = try wire.decodeTopologyTrailer(footer, source.byte_len);
    if (trailer.source_nodes > limits.max_nodes or trailer.source_edges > limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
    const raw = try reader.read(trailer.body_len + trailer.topology_len, trailer.directory_len);
    defer alloc.free(raw);
    const directory = (try wire.TopologyDirectory.init(raw, trailer.checksum)) orelse return null;
    if (trailer.source_nodes > directory.nodes) return error.InvalidGraphSegment;
    for (0..directory.page_offsets.len / 8) |i| {
        const offset = std.mem.readInt(u64, directory.page_offsets[i * 8 ..][0..8], .little);
        if (offset < wire.header_len or offset > trailer.body_len) return error.InvalidGraphSegment;
    }
    var iterator = directory.iterator();
    var selected_types: usize = 0;
    var selected_edges: usize = 0;
    var expected_offset = trailer.body_len;
    while (try iterator.next()) |entry| {
        if (entry.offset != expected_offset) return error.InvalidGraphSegment;
        expected_offset = std.math.add(u64, expected_offset, std.math.mul(u64, entry.edges, 8) catch return error.InvalidGraphSegment) catch return error.InvalidGraphSegment;
        if (!selected(entry.kind, configs)) continue;
        selected_types += 1;
        selected_edges = std.math.add(usize, selected_edges, std.math.cast(usize, entry.edges) orelse return error.GraphMetricBuildBudgetExceeded) catch return error.GraphMetricBuildBudgetExceeded;
    }
    if (expected_offset != trailer.body_len + trailer.topology_len or trailer.topology_len / 8 > trailer.source_edges) return error.InvalidGraphSegment;
    const edges = try alloc.alloc(Edge, selected_edges);
    errdefer alloc.free(edges);
    const dense = selected_edges > directory.nodes / 64;
    const mapping = try alloc.alloc(u32, if (dense) directory.nodes else 0);
    defer alloc.free(mapping);
    @memset(mapping, wire.no_table);
    const endpoints = try alloc.alloc(u32, if (dense) directory.nodes else try std.math.mul(usize, selected_edges, 2));
    defer alloc.free(endpoints);
    const type_offsets = try alloc.alloc(u32, selected_types + 1);
    errdefer alloc.free(type_offsets);
    const kinds = try alloc.alloc([]const u8, selected_types);
    errdefer alloc.free(kinds);
    var strings = std.ArrayListUnmanaged(u8).empty;
    defer strings.deinit(alloc);
    iterator = directory.iterator();
    var edge_index: usize = 0;
    var type_index: usize = 0;
    while (try iterator.next()) |entry| {
        if (!selected(entry.kind, configs)) continue;
        kinds[type_index] = entry.kind;
        type_offsets[type_index] = @intCast(edge_index);
        type_index += 1;
        var read_edges: u64 = 0;
        var previous: ?Edge = null;
        while (read_edges < entry.edges) {
            const count: usize = @intCast(@min(entry.edges - read_edges, 128 * 1024));
            const bytes = try reader.read(entry.offset + read_edges * 8, count * 8);
            defer alloc.free(bytes);
            for (0..count) |i| {
                if (i % 4096 == 0) try cancellation.check();
                const edge = Edge{ .source = std.mem.readInt(u32, bytes[i * 8 ..][0..4], .little), .target = std.mem.readInt(u32, bytes[i * 8 + 4 ..][0..4], .little) };
                if (edge.source >= directory.nodes or edge.target >= directory.nodes) return error.InvalidGraphSegment;
                if (previous) |prior| if (prior.source > edge.source or (prior.source == edge.source and prior.target > edge.target)) return error.InvalidGraphSegment;
                previous = edge;
                edges[edge_index] = edge;
                if (dense) {
                    mapping[edge.source] = 0;
                    mapping[edge.target] = 0;
                } else {
                    endpoints[edge_index * 2] = edge.source;
                    endpoints[edge_index * 2 + 1] = edge.target;
                }
                edge_index += 1;
            }
            read_edges += count;
        }
    }
    type_offsets[selected_types] = @intCast(edge_index);
    var unique: usize = 0;
    if (dense) {
        for (mapping, 0..) |*slot, ordinal| {
            if (ordinal % 4096 == 0) try cancellation.check();
            if (slot.* == wire.no_table) continue;
            slot.* = @intCast(unique);
            endpoints[unique] = @intCast(ordinal);
            unique += 1;
        }
    } else {
        std.mem.sort(u32, endpoints, {}, std.sort.asc(u32));
        try cancellation.check();
        for (endpoints) |ordinal| {
            if (unique != 0 and endpoints[unique - 1] == ordinal) continue;
            endpoints[unique] = ordinal;
            unique += 1;
        }
    }
    const ordinals = endpoints[0..unique];
    const nodes = try alloc.alloc([]const u8, unique);
    errdefer alloc.free(nodes);
    const lengths = try alloc.alloc(usize, unique);
    defer alloc.free(lengths);
    var selected_node: usize = 0;
    while (selected_node < unique) {
        const page = ordinals[selected_node] / wire.node_page_entries;
        var range = try directory.nodePage(page);
        var last_page = page;
        // Merge nearby selected pages into bounded reads. Sparse selection
        // must not turn one source GET into thousands of tiny cloud requests.
        for (ordinals[selected_node + 1 ..]) |ordinal| {
            const next_page = ordinal / wire.node_page_entries;
            if (next_page == last_page) continue;
            const next = try directory.nodePage(next_page);
            const end = std.math.add(u64, next.offset, next.len) catch return error.InvalidGraphSegment;
            if (next.offset < range.offset + range.len) return error.InvalidGraphSegment;
            if (next.offset - (range.offset + range.len) > 64 * 1024 or end - range.offset > 1024 * 1024) break;
            range.len = end - range.offset;
            last_page = next_page;
        }
        if (range.offset < wire.header_len or range.offset > trailer.body_len or range.len > trailer.body_len - range.offset) return error.InvalidGraphSegment;
        const bytes = try reader.read(range.offset, range.len);
        defer alloc.free(bytes);
        const first = page * wire.node_page_entries;
        const count = @min((last_page - page + 1) * wire.node_page_entries, directory.nodes - first);
        var pos: usize = 0;
        var previous: ?[]const u8 = null;
        for (0..count) |i| {
            if (bytes.len - pos < 4) return error.InvalidGraphSegment;
            const len = std.mem.readInt(u32, bytes[pos..][0..4], .little);
            pos += 4;
            if (len > bytes.len - pos) return error.InvalidGraphSegment;
            const node = bytes[pos..][0..len];
            if (previous) |prior| if (std.mem.order(u8, prior, node) != .lt) return error.InvalidGraphSegment;
            previous = node;
            if (selected_node < unique and ordinals[selected_node] == first + i) {
                lengths[selected_node] = len;
                try strings.appendSlice(alloc, node);
                selected_node += 1;
            }
            pos += len;
        }
        if (pos != bytes.len) return error.InvalidGraphSegment;
    }
    for (kinds) |kind| try strings.appendSlice(alloc, kind);
    const string_bytes = try strings.toOwnedSlice(alloc);
    errdefer alloc.free(string_bytes);
    var pos: usize = 0;
    for (nodes, lengths, 0..) |*node, len, i| {
        node.* = string_bytes[pos..][0..len];
        if (i > 0 and std.mem.order(u8, nodes[i - 1], node.*) != .lt) return error.InvalidGraphSegment;
        pos += len;
    }
    for (kinds) |*kind| {
        const len = kind.len;
        kind.* = string_bytes[pos..][0..len];
        pos += len;
    }
    for (edges, 0..) |*edge, i| {
        if (i % 4096 == 0) try cancellation.check();
        edge.source = if (dense) mapping[edge.source] else ordinalIndex(ordinals, edge.source);
        edge.target = if (dense) mapping[edge.target] else ordinalIndex(ordinals, edge.target);
    }
    return .{ .node_ids = nodes, .edge_types = kinds, .string_bytes = string_bytes, .edge_type_offsets = type_offsets, .edges = edges, .source_node_count = trailer.source_nodes, .source_edge_count = @intCast(trailer.source_edges), .retained_bytes = (nodes.len + kinds.len) * @sizeOf([]const u8) + string_bytes.len + type_offsets.len * 4 + edges.len * @sizeOf(Edge) };
}

fn ordinalIndex(ordinals: []const u32, ordinal: u32) u32 {
    const index = std.sort.lowerBound(u32, ordinals, ordinal, struct {
        fn order(a: u32, b: u32) std.math.Order {
            return std.math.order(a, b);
        }
    }.order);
    std.debug.assert(index < ordinals.len and ordinals[index] == ordinal);
    return @intCast(index);
}
