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

//! Authenticated, bounded point reads over the current graph wire. The node
//! dictionary is paged; the ordinal routing array addresses rows without a
//! graph-wide decode or a per-request hash table.
const std = @import("std");
const Allocator = std.mem.Allocator;
const wire = @import("packed.zig");
const types = @import("types.zig");
const topology = @import("topology_reader.zig");
const artifacts = @import("../artifacts/store.zig");
const refs = @import("../manifest/artifact_ref.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const Reader = struct {
    alloc: Allocator,
    context: topology.Context,
    tables: []const []const u8,
    table_bytes: []u8,
    page_bytes: []u8 = &.{},
    page: ?usize = null,
    page_nodes: [wire.node_page_entries][]const u8 = undefined,
    page_count: usize = 0,

    pub fn init(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64) !?Reader {
        return initCached(alloc, store, source, cancellation, remaining, null);
    }

    pub fn initCached(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64, cache: ?topology.ReadCache) !?Reader {
        // Queries alternate dictionary, routing, and adjacency blocks. A small
        // request-local working set avoids thrashing these independent ranges;
        // streaming topology preparation keeps its one-block configuration.
        var context = try topology.Context.initQuery(alloc, store, source, cancellation, remaining, cache);
        errdefer context.deinit();
        const directory = context.layout orelse {
            context.deinit();
            return null;
        };
        const header = try context.readAlloc(alloc, 0, wire.header_len);
        defer alloc.free(header);
        if (!std.mem.eql(u8, header[0..4], wire.wire_magic) or std.mem.readInt(u16, header[4..6], .little) != wire.wire_version)
            return error.InvalidGraphSegment;
        if (std.mem.readInt(u32, header[10..14], .little) != directory.nodes) return error.InvalidGraphSegment;
        const table_count = std.mem.readInt(u32, header[6..10], .little);
        const kind_count = std.mem.readInt(u32, header[14..18], .little);
        if (kind_count != directory.types) return error.InvalidGraphSegment;
        const first_offset = try context.control(8, 16);
        const nodes_begin = std.mem.readInt(u64, &first_offset, .little);
        if (nodes_begin < wire.header_len or nodes_begin > context.trailer.body_len) return error.InvalidGraphSegment;
        if (table_count > (nodes_begin - wire.header_len) / 4) return error.InvalidGraphSegment;
        const table_bytes = try context.readAlloc(alloc, wire.header_len, nodes_begin - wire.header_len);
        errdefer alloc.free(table_bytes);
        const tables = try alloc.alloc([]const u8, table_count);
        errdefer alloc.free(tables);
        var pos: usize = 0;
        for (tables) |*table| {
            table.* = try string(table_bytes, &pos);
            if (table.len == 0) return error.InvalidGraphSegment;
        }
        if (pos != table_bytes.len) return error.InvalidGraphSegment;
        return .{ .alloc = alloc, .context = context, .tables = tables, .table_bytes = table_bytes };
    }

    pub fn deinit(self: *Reader) void {
        self.alloc.free(self.page_bytes);
        self.alloc.free(self.tables);
        self.alloc.free(self.table_bytes);
        self.context.deinit();
        self.* = undefined;
    }

    fn string(bytes: []const u8, pos: *usize) ![]const u8 {
        if (bytes.len - pos.* < 4) return error.InvalidGraphSegment;
        const len = std.mem.readInt(u32, bytes[pos.*..][0..4], .little);
        pos.* += 4;
        if (len > bytes.len - pos.*) return error.InvalidGraphSegment;
        const result = bytes[pos.*..][0..len];
        pos.* += len;
        return result;
    }

    fn loadPage(self: *Reader, page: usize) !void {
        if (self.page == page) return;
        const directory = self.context.layout.?;
        const range = try self.context.nodePage(page);
        if (range.offset < wire.header_len or range.offset > self.context.trailer.body_len or range.len > self.context.trailer.body_len - range.offset)
            return error.InvalidGraphSegment;
        self.alloc.free(self.page_bytes);
        self.page_bytes = &.{};
        self.page = null;
        self.page_bytes = try self.context.readAlloc(self.alloc, range.offset, range.len);
        self.page_count = @min(wire.node_page_entries, directory.nodes - page * wire.node_page_entries);
        var pos: usize = 0;
        for (self.page_nodes[0..self.page_count], 0..) |*node, i| {
            node.* = try string(self.page_bytes, &pos);
            if (i > 0 and std.mem.order(u8, self.page_nodes[i - 1], node.*) != .lt) return error.InvalidGraphSegment;
        }
        if (pos != self.page_bytes.len) return error.InvalidGraphSegment;
        const fence_bytes = try self.context.nodeFence(page);
        if (self.page_nodes[0].len != std.mem.readInt(u32, fence_bytes[0..4], .little) or !std.mem.eql(u8, fence(&fence_bytes), self.page_nodes[0][0..@min(self.page_nodes[0].len, 64)])) return error.InvalidGraphSegment;
        self.page = page;
    }

    fn ordinal(self: *Reader, key: []const u8) !?u32 {
        const pages = self.context.layout.?.pages;
        if (pages == 0) return null;
        // Authenticated 64-byte fence prefixes usually identify one page with
        // no I/O. Long shared prefixes only widen the binary-search interval;
        // they never change lookup semantics or require unbounded control data.
        const prefix = key[0..@min(key.len, 64)];
        var begin: usize = 0;
        var end = pages;
        while (begin < end) {
            const middle = begin + (end - begin) / 2;
            const raw = try self.context.nodeFence(middle);
            if (std.mem.order(u8, fence(&raw), prefix) == .lt) begin = middle + 1 else end = middle;
        }
        const first = begin;
        if (first < pages and key.len <= 64) {
            const raw = try self.context.nodeFence(first);
            if (std.mem.readInt(u32, raw[0..4], .little) == key.len and std.mem.eql(u8, fence(&raw), key))
                return @intCast(first * wire.node_page_entries);
        }
        end = pages;
        while (begin < end) {
            const middle = begin + (end - begin) / 2;
            const raw = try self.context.nodeFence(middle);
            if (std.mem.order(u8, fence(&raw), prefix) != .gt) begin = middle + 1 else end = middle;
        }
        if (begin == 0) return null;
        var lower = first -| 1;
        var upper = begin;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            try self.loadPage(middle);
            if (std.mem.order(u8, self.page_nodes[self.page_count - 1], key) == .lt) lower = middle + 1 else upper = middle;
        }
        if (lower == begin) return null;
        try self.loadPage(lower);
        const index = std.sort.binarySearch([]const u8, self.page_nodes[0..self.page_count], key, compareString) orelse return null;
        return @intCast(lower * wire.node_page_entries + index);
    }

    fn fence(bytes: []const u8) []const u8 {
        return bytes[4..][0..@min(std.mem.readInt(u32, bytes[0..4], .little), 64)];
    }

    fn compareString(a: []const u8, b: []const u8) std.math.Order {
        return std.mem.order(u8, a, b);
    }

    const Row = struct { offset: u64, out: u32, in: u32 };
    pub fn containsNode(self: *Reader, key: []const u8) !bool {
        return try self.row(key) != null;
    }
    fn row(self: *Reader, key: []const u8) !?Row {
        const node = try self.ordinal(key) orelse return null;
        const routing = self.context.trailer.body_len + self.context.trailer.topology_len;
        const raw = try self.context.readAlloc(self.alloc, routing + @as(u64, node) * 8, 8);
        defer self.alloc.free(raw);
        const offset = std.mem.readInt(u64, raw[0..8], .little);
        if (offset == 0) return null;
        if (offset < wire.header_len or offset > self.context.trailer.body_len or self.context.trailer.body_len - offset < 12) return error.InvalidGraphSegment;
        const header = try self.context.readAlloc(self.alloc, offset, 12);
        defer self.alloc.free(header);
        if (std.mem.readInt(u32, header[0..4], .little) != node) return error.InvalidGraphSegment;
        const result = Row{ .offset = offset + 12, .out = std.mem.readInt(u32, header[4..8], .little), .in = std.mem.readInt(u32, header[8..12], .little) };
        if ((@as(u64, result.out) + result.in) * wire.edge_len > self.context.trailer.body_len - result.offset) return error.InvalidGraphSegment;
        return result;
    }

    fn edgeAt(self: *Reader, offset: u64, index: usize, work: *usize) !wire.Edge {
        if (work.* == 0) return error.GraphTraversalQueryBudgetExceeded;
        work.* -= 1;
        const bytes = try self.context.readAlloc(self.alloc, offset + index * wire.edge_len, wire.edge_len);
        defer self.alloc.free(bytes);
        return self.validEdge(bytes, 0);
    }

    fn validEdge(self: *Reader, bytes: []const u8, index: usize) !wire.Edge {
        const edge = wire.readEdge(bytes, index);
        if (edge.node >= self.context.layout.?.nodes or edge.edge_type >= self.context.layout.?.types or !std.math.isFinite(edge.weight)) return error.InvalidGraphSegment;
        if (edge.table) |table| if (table >= self.tables.len) return error.InvalidGraphSegment;
        return edge;
    }

    fn lowerBound(self: *Reader, offset: u64, count: usize, kind: u32, node: u32, work: *usize) !usize {
        var lower: usize = 0;
        var upper = count;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const edge = try self.edgeAt(offset, middle, work);
            if (edge.edge_type < kind or (edge.edge_type == kind and edge.node < node)) lower = middle + 1 else upper = middle;
        }
        return lower;
    }

    fn copyEdge(self: *Reader, edge: wire.Edge) !types.Edge {
        try self.loadPage(edge.node / wire.node_page_entries);
        const neighbor = try self.alloc.dupe(u8, self.page_nodes[edge.node % wire.node_page_entries]);
        errdefer self.alloc.free(neighbor);
        return .{ .neighbor_id = neighbor, .edge_type = try self.context.kindAlloc(edge.edge_type), .weight = edge.weight, .neighbor_table_id = edge.table };
    }

    /// A resumable directional scan. Transport is chunked, but edge work and
    /// string materialization are admitted only as the consumer advances. A
    /// shortest-path consumer can stop without decoding the rest of a hub.
    pub const Cursor = struct {
        const Range = struct { begin: usize, end: usize };
        reader: *Reader,
        offset: u64,
        ranges: []Range,
        range: usize = 0,
        position: usize = 0,
        bytes: []u8 = &.{},
        bytes_begin: usize = 0,
        work: *usize,

        pub fn deinit(self: *Cursor) void {
            self.reader.alloc.free(self.ranges);
            self.reader.alloc.free(self.bytes);
            self.* = undefined;
        }

        pub fn next(self: *Cursor) !?types.Edge {
            return try self.reader.copyEdge(try self.nextWire() orelse return null);
        }

        fn nextWire(self: *Cursor) !?wire.Edge {
            while (self.range < self.ranges.len) {
                const selected = self.ranges[self.range];
                self.position = @max(self.position, selected.begin);
                if (self.position == selected.end) {
                    self.range += 1;
                    continue;
                }
                try self.reader.context.reader.cancellation.check();
                if (self.work.* == 0) return error.GraphTraversalQueryBudgetExceeded;
                if (self.bytes.len == 0 or self.position < self.bytes_begin or self.position - self.bytes_begin >= self.bytes.len / wire.edge_len) {
                    self.reader.alloc.free(self.bytes);
                    self.bytes = &.{};
                    self.bytes_begin = self.position;
                    const count: usize = @min(selected.end - self.position, @min(self.work.*, 4096));
                    self.bytes = try self.reader.context.readAlloc(self.reader.alloc, self.offset + self.position * wire.edge_len, count * wire.edge_len);
                }
                self.work.* -= 1;
                const edge = try self.reader.validEdge(self.bytes, self.position - self.bytes_begin);
                self.position += 1;
                return edge;
            }
            return null;
        }
    };

    pub fn cursor(self: *Reader, key: []const u8, requested: []const []const u8, incoming: bool, work: *usize) !Cursor {
        const found = try self.row(key);
        const offset = if (found) |row_value| row_value.offset + (if (incoming) @as(u64, row_value.out) * wire.edge_len else 0) else 0;
        const count: usize = if (found) |row_value| (if (incoming) row_value.in else row_value.out) else 0;
        return self.cursorAt(offset, count, requested, work);
    }

    fn cursorAt(self: *Reader, offset: u64, count: usize, requested: []const []const u8, work: *usize) !Cursor {
        if (count == 0) return .{ .reader = self, .offset = offset, .ranges = &.{}, .work = work };
        var ranges: std.ArrayListUnmanaged(Cursor.Range) = .empty;
        errdefer ranges.deinit(self.alloc);
        if (requested.len == 0) {
            if (count != 0) try ranges.append(self.alloc, .{ .begin = 0, .end = count });
        } else for (requested, 0..) |kind, i| {
            const duplicate = for (requested[0..i]) |prior| {
                if (std.mem.eql(u8, prior, kind)) break true;
            } else false;
            if (duplicate) continue;
            const id = try self.context.kindId(kind) orelse continue;
            const begin = try self.lowerBound(offset, count, @intCast(id), 0, work);
            const end = try self.lowerBound(offset, count, @intCast(id + 1), 0, work);
            if (begin != end) try ranges.append(self.alloc, .{ .begin = begin, .end = end });
        }
        std.mem.sort(Cursor.Range, ranges.items, {}, struct {
            fn less(_: void, a: Cursor.Range, b: Cursor.Range) bool {
                return a.begin < b.begin;
            }
        }.less);
        return .{ .reader = self, .offset = offset, .ranges = try ranges.toOwnedSlice(self.alloc), .work = work };
    }

    /// Work is a shared remaining physical-edge allowance, consumed before I/O.
    /// Returned edges own their strings using this reader's admitted allocator.
    pub fn probe(self: *Reader, source: []const u8, kind: []const u8, target: []const u8, work: *usize) !?types.Edge {
        const kind_id = try self.context.kindId(kind) orelse return null;
        const target_id = try self.ordinal(target) orelse return null;
        const found = try self.row(source) orelse return null;
        const index = try self.lowerBound(found.offset, found.out, @intCast(kind_id), target_id, work);
        if (index == found.out) return null;
        const edge = try self.edgeAt(found.offset, index, work);
        if (edge.edge_type != kind_id or edge.node != target_id) return null;
        return try self.copyEdge(edge);
    }

    fn readEdges(self: *Reader, offset: u64, count: usize, requested: []const []const u8, limit: usize, work: *usize, skip_qualified: bool, skip_node: ?[]const u8) ![]types.Edge {
        var selected = try self.cursorAt(offset, count, requested, work);
        defer selected.deinit();
        var total: usize = 0;
        for (selected.ranges) |range| total = std.math.add(usize, total, range.end - range.begin) catch return error.QueryCandidateBudgetExceeded;
        if (!skip_qualified and skip_node == null and total > limit) return error.QueryCandidateBudgetExceeded;
        if (total > work.*) return error.GraphTraversalQueryBudgetExceeded;
        var result: std.ArrayListUnmanaged(types.Edge) = .empty;
        errdefer {
            for (result.items) |*edge| edge.deinit(self.alloc);
            result.deinit(self.alloc);
        }
        while (try selected.nextWire()) |edge| {
            if (skip_qualified and edge.table != null) continue;
            if (skip_node) |node| {
                try self.loadPage(edge.node / wire.node_page_entries);
                if (std.mem.eql(u8, node, self.page_nodes[edge.node % wire.node_page_entries])) continue;
            }
            if (result.items.len == limit) return error.QueryCandidateBudgetExceeded;
            try result.ensureUnusedCapacity(self.alloc, 1);
            result.appendAssumeCapacity(try self.copyEdge(edge));
        }
        return result.toOwnedSlice(self.alloc);
    }

    pub fn adjacency(self: *Reader, key: []const u8, requested: []const []const u8, direction: anytype, limit: usize, work: *usize) !?types.Adjacency {
        return self.adjacencyFiltered(key, requested, direction, limit, work, true, false);
    }

    pub fn adjacencyFiltered(self: *Reader, key: []const u8, requested: []const []const u8, direction: anytype, limit: usize, work: *usize, include_qualified: bool, deduplicate_self_loops: bool) !?types.Adjacency {
        const found = try self.row(key) orelse return null;
        const node = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(node);
        const out = try self.readEdges(found.offset, if (direction == .out or direction == .both) found.out else 0, requested, limit, work, !include_qualified, null);
        errdefer {
            for (out) |*edge| edge.deinit(self.alloc);
            self.alloc.free(out);
        }
        const incoming = try self.readEdges(found.offset + @as(u64, found.out) * wire.edge_len, if (direction == .in or direction == .both) found.in else 0, requested, limit - out.len, work, false, if (deduplicate_self_loops and direction == .both) key else null);
        return .{ .node_id = node, .out_edges = out, .in_edges = incoming };
    }
};

const TestStore = struct {
    payload: []const u8,
    calls: usize = 0,
    bytes: usize = 0,
    fn deinit(_: Allocator, _: *anyopaque) void {}
    fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts.ArtifactMetadata {
        return error.Unsupported;
    }
    fn get(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
        return error.UnexpectedFullRead;
    }
    fn range(ptr: *anyopaque, alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (offset > self.payload.len or len > self.payload.len - offset) return error.InvalidRange;
        self.calls += 1;
        self.bytes += len;
        return alloc.dupe(u8, self.payload[@intCast(offset)..][0..len]);
    }
    fn stat(_: *anyopaque, _: Allocator, _: []const u8) !artifacts.ArtifactMetadata {
        return error.Unsupported;
    }
    fn delete(_: *anyopaque, _: []const u8) !void {
        return error.Unsupported;
    }
    const vtable = artifacts.ArtifactStore.VTable{ .deinit = deinit, .put = put, .get_alloc = get, .get_range_alloc = range, .stat = stat, .delete = delete };
};

fn exerciseReader(alloc: Allocator, payload: []const u8, source: refs.ArtifactRef) !void {
    var memory = TestStore{ .payload = payload };
    var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &TestStore.vtable };
    var remaining: u64 = 1024 * 1024;
    var reader = (try Reader.init(alloc, &store, source, .none, &remaining)).?;
    defer reader.deinit();
    var work: usize = 1000;
    const Direction = enum { out, in, both };
    var result = (try reader.adjacency("a", &.{ "z", "link", "link" }, Direction.both, 8, &work)).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), result.out_edges.len);
    try std.testing.expectEqual(@as(usize, 1), result.in_edges.len);
    try std.testing.expectEqualStrings("a", result.out_edges[0].neighbor_id);
    try std.testing.expectEqualStrings("b", result.out_edges[1].neighbor_id);
    try std.testing.expectEqual(@as(f32, 2), result.out_edges[1].weight);
    try std.testing.expectEqual(@as(?u32, 0), result.out_edges[2].neighbor_table_id);
    try std.testing.expectEqualStrings("elsewhere", reader.tables[0]);
    var exact = (try reader.probe("a", "link", "b", &work)).?;
    defer exact.deinit(alloc);
    try std.testing.expectEqual(@as(f32, 2), exact.weight);
    try std.testing.expect(try reader.probe("a", "absent", "b", &work) == null);
    try std.testing.expect(try reader.adjacency("absent", &.{}, Direction.out, 8, &work) == null);
    var one: usize = 1;
    var cursor = try reader.cursor("a", &.{}, false, &one);
    defer cursor.deinit();
    var first = (try cursor.next()).?;
    defer first.deinit(alloc);
    try std.testing.expectEqualStrings("a", first.neighbor_id);
    try std.testing.expectEqual(@as(usize, 0), one);
    try std.testing.expectError(error.GraphTraversalQueryBudgetExceeded, cursor.next());
    var incoming = (try reader.adjacency("b", &.{"link"}, Direction.in, 8, &work)).?;
    defer incoming.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), incoming.out_edges.len);
    try std.testing.expectEqual(@as(usize, 1), incoming.in_edges.len);
    if (reader.adjacency("a", &.{}, Direction.out, 1, &work)) |_| return error.ExpectedBudgetFailure else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.QueryCandidateBudgetExceeded, err);
    }
    work = 0;
    if (reader.probe("a", "link", "b", &work)) |_| return error.ExpectedBudgetFailure else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(error.GraphTraversalQueryBudgetExceeded, err);
    }
    try std.testing.expectEqual(@as(usize, 4), memory.calls);
    try std.testing.expectEqual(payload.len, memory.bytes);
}

test "serverless graph paged adjacency preserves lookup semantics budgets and allocation cleanup" {
    const alloc = std.testing.allocator;
    const payload = try wire.encodeAlloc(alloc, .{
        .neighbor_tables = @constCast(&[_][]u8{@constCast("elsewhere")}),
        .adjacencies = @constCast(&[_]types.Adjacency{
            types.Adjacency{ .node_id = @constCast("a"), .out_edges = @constCast(&[_]types.Edge{
                types.Edge{ .neighbor_id = @constCast("a"), .edge_type = @constCast("link"), .weight = 1 },
                types.Edge{ .neighbor_id = @constCast("b"), .edge_type = @constCast("link"), .weight = 2 },
                types.Edge{ .neighbor_id = @constCast("b"), .edge_type = @constCast("z"), .weight = 3, .neighbor_table_id = 0 },
            }), .in_edges = @constCast(&[_]types.Edge{types.Edge{ .neighbor_id = @constCast("a"), .edge_type = @constCast("link"), .weight = 1 }}) },
            types.Adjacency{ .node_id = @constCast("b"), .out_edges = &.{}, .in_edges = @constCast(&[_]types.Edge{types.Edge{ .neighbor_id = @constCast("a"), .edge_type = @constCast("link"), .weight = 2 }}) },
        }),
    });
    defer alloc.free(payload);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    const checksum = std.fmt.bytesToHex(digest, .lower);
    const id = "sha256:" ++ checksum;
    var source = refs.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = id, .checksum = &checksum, .byte_len = payload.len };
    try wire.bindTopologyControl(&source, payload);
    try exerciseReader(alloc, payload, source);
    try std.testing.checkAllAllocationFailures(alloc, exerciseReader, .{ payload, source });

    var memory = TestStore{ .payload = payload };
    var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &TestStore.vtable };
    var remaining: u64 = 1024 * 1024;
    // The routing array is covered by the same manifest-authenticated block
    // hashes as dictionary and adjacency data.
    const trailer = try wire.decodeTopologyTrailer(payload[payload.len - wire.topology_trailer_len ..], payload.len);
    payload[@intCast(trailer.body_len + trailer.topology_len)] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, Reader.init(alloc, &store, source, .none, &remaining));
    payload[@intCast(trailer.body_len + trailer.topology_len)] ^= 1;
    remaining = wire.topology_trailer_len - 1;
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, Reader.init(alloc, &store, source, .none, &remaining));
}

test "serverless graph paged preparation coalesces thousands of small type runs" {
    const alloc = std.testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(alloc);
    defer fixture.deinit();
    var builder = @import("builder.zig").Builder{ .alloc = fixture.allocator() };
    defer builder.deinit();
    for (0..10000) |i| try builder.addEdge("a", "b", try std.fmt.allocPrint(fixture.allocator(), "kind{d:0>5}", .{i}), 1, null);
    const payload = try builder.encodeAlloc(4 * 1024 * 1024, .none);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    const checksum = std.fmt.bytesToHex(digest, .lower);
    const id = try std.fmt.allocPrint(fixture.allocator(), "sha256:{s}", .{checksum});
    var source = refs.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = id, .checksum = &checksum, .byte_len = payload.len };
    try wire.bindTopologyControl(&source, payload);
    var memory = TestStore{ .payload = payload };
    var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &TestStore.vtable };
    var remaining: u64 = 2 * 1024 * 1024;
    const Config = struct { edge_filter: struct { mode: enum { all, types } = .all, types: []const []const u8 = &.{} } = .{} };
    var prepared = (try topology.readAlloc(alloc, &store, source, &[_]Config{.{}}, .{ .max_nodes = 2, .max_edges = 10000 }, .none, &remaining)).?;
    defer prepared.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 10000), prepared.edges.len);
    try std.testing.expectEqual(@as(usize, 10000), prepared.edge_types.len);
    try std.testing.expect(memory.calls <= 8);
    try std.testing.expect(memory.bytes <= payload.len);
}

test "serverless graph paged routing survives large type directories and authenticates root and leaves" {
    const alloc = std.testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(alloc);
    defer fixture.deinit();
    var builder = @import("builder.zig").Builder{ .alloc = fixture.allocator() };
    defer builder.deinit();
    for (0..20000) |i| try builder.addEdge("a", "b", try std.fmt.allocPrint(fixture.allocator(), "kind{d:0>5}", .{i}), 1, null);
    const payload = try builder.encodeAlloc(4 * 1024 * 1024, .none);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    const checksum = std.fmt.bytesToHex(digest, .lower);
    const id = try std.fmt.allocPrint(fixture.allocator(), "sha256:{s}", .{checksum});
    var source = refs.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = id, .checksum = &checksum, .byte_len = payload.len };
    try wire.bindTopologyControl(&source, payload);
    const trailer = try wire.decodeTopologyTrailer(payload[payload.len - wire.topology_trailer_len ..], payload.len);
    try std.testing.expect(trailer.directory_len > 1024 * 1024);
    var memory = TestStore{ .payload = payload };
    var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &TestStore.vtable };
    var remaining: u64 = 4 * 1024 * 1024;
    {
        var reader = (try Reader.init(alloc, &store, source, .none, &remaining)).?;
        defer reader.deinit();
        var work: usize = 100;
        var edge = (try reader.probe("a", "kind00001", "b", &work)).?;
        defer edge.deinit(alloc);
        try std.testing.expectEqualStrings("kind00001", edge.edge_type);
        try std.testing.expect(reader.context.retainedBytes() < 1024 * 1024);
        try std.testing.expect(memory.bytes < payload.len);
        // Alternating type offsets, type labels and data checksums must not
        // fetch a directory leaf for every edge. Also crosses the 4,096-edge
        // chunk boundary that formerly overflowed its inferred integer type.
        remaining = 4 * 1024 * 1024;
        work = 20000;
        var all = (try reader.adjacency("a", &.{}, enum { out, in, both }.out, 20000, &work)).?;
        defer all.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 20000), all.out_edges.len);
        try std.testing.expectEqual(@as(usize, 0), work);
    }
    payload[@intCast(trailer.rootOffset())] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, Reader.init(alloc, &store, source, .none, &remaining));
    payload[@intCast(trailer.rootOffset())] ^= 1;
    payload[@intCast(trailer.directoryOffset())] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, Reader.init(alloc, &store, source, .none, &remaining));
}

test "serverless graph paged dictionary fences handle long shared prefixes and page boundaries" {
    const alloc = std.testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(alloc);
    defer fixture.deinit();
    const a = fixture.allocator();
    const count = 1025;
    const ids = try a.alloc([]const u8, count);
    for (ids, 0..) |*id, i| id.* = try std.fmt.allocPrint(a, "{s}/{d:0>8}", .{ &([_]u8{'x'} ** 100), i });
    var builder = @import("builder.zig").Builder{ .alloc = a };
    defer builder.deinit();
    for (ids, 0..) |id, i| try builder.addEdge(id, ids[(i + 1) % count], "link", 1, null);
    const payload = try builder.encodeAlloc(4 * 1024 * 1024, .none);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    const checksum = std.fmt.bytesToHex(digest, .lower);
    var source = refs.ArtifactRef{ .kind = .graph_segment, .name = "g", .artifact_id = try std.fmt.allocPrint(a, "sha256:{s}", .{checksum}), .checksum = &checksum, .byte_len = payload.len };
    try wire.bindTopologyControl(&source, payload);
    var memory = TestStore{ .payload = payload };
    var store = artifacts.ArtifactStore{ .allocator = alloc, .ptr = &memory, .vtable = &TestStore.vtable };
    var remaining: u64 = 8 * 1024 * 1024;
    var reader = (try Reader.init(alloc, &store, source, .none, &remaining)).?;
    defer reader.deinit();
    var work: usize = 100;
    for ([_]usize{ 0, 255, 256, 511, 512, 1023, 1024 }) |i| {
        var row = (try reader.adjacency(ids[i], &.{}, enum { out, in, both }.out, 1, &work)).?;
        defer row.deinit(alloc);
        try std.testing.expectEqualStrings(ids[(i + 1) % count], row.out_edges[0].neighbor_id);
    }
    try std.testing.expect(!try reader.containsNode(""));
    try std.testing.expect(!try reader.containsNode("z"));
    const missing = try std.fmt.allocPrint(a, "{s}/00001025", .{&([_]u8{'x'} ** 100)});
    try std.testing.expect(!try reader.containsNode(missing));
    const calls = memory.calls;
    for ([_]usize{ 0, 255, 256, 511, 512, 1023, 1024 }) |i| try std.testing.expect(try reader.containsNode(ids[i]));
    // Routing, row headers, and dictionary pages coexist in the bounded
    // request cache instead of evicting one another on every hop.
    try std.testing.expectEqual(calls, memory.calls);
}
