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
pub const wire_version: u16 = 9;
pub const header_len = 22;
pub const edge_len = 16;
pub const no_table = std.math.maxInt(u32);
pub const topology_trailer_len = 112;
pub const node_page_entries = 256;
pub const node_page_fence_bytes = 68;
pub const authentication_block_bytes = 64 * 1024;
// A bounded root authenticates independently addressable directory leaves.
// This is a wire-size bound, not a request-local allocation allowance.
pub const max_topology_directory_bytes = 256 * 1024 * 1024;
pub const directory_leaf_bytes = 64 * 1024;
const absent_directory = std.math.maxInt(u32);
pub const typed_row_flag: u64 = @as(u64, 1) << 63;

/// Index only high-degree, low-entropy rows. Reservation is bounded by 1/16
/// of the edge count (8 bytes per entry versus 16 bytes per edge). Rows with
/// more distinct types retain binary search without a graph-wide side array.
pub fn typeRunCapacity(edges: usize, kinds: usize) usize {
    return if (edges >= 1024) @min(kinds, edges / 16) else 0;
}

pub fn typeRunReservation(out: usize, incoming: usize, kinds: usize) usize {
    const capacity = typeRunCapacity(out, kinds) + typeRunCapacity(incoming, kinds);
    return if (capacity == 0) 0 else 16 + capacity * 8;
}

pub fn topologyRootSize(directory_len: usize) usize {
    return 16 + ((directory_len + directory_leaf_bytes - 1) / directory_leaf_bytes) * 32;
}

/// Directory leaves grow with the graph; the small root never requires a
/// whole-directory query read or dropping routing at a cardinality threshold.
pub fn topologyDirectorySize(kinds: []const []const u8, nodes: usize, covered_bytes: usize, routing_extra: usize) usize {
    const pages = nodes / node_page_entries + @intFromBool(nodes % node_page_entries != 0);
    var size: usize = 16 +| ((pages + 1) *| 8) +| (pages *| node_page_fence_bytes);
    const covered = covered_bytes +| (nodes *| 8) +| routing_extra;
    size +|= ((covered / authentication_block_bytes + @intFromBool(covered % authentication_block_bytes != 0)) *| 32);
    size +|= (kinds.len +| 1) *| 8;
    for (kinds) |kind| size +|= 52 +| kind.len;
    return size;
}

pub fn topologyExtensionSize(kinds: []const []const u8, nodes: usize, edges: usize, body_len: usize, routing_extra: usize) !usize {
    const covered = std.math.add(usize, body_len, std.math.mul(usize, edges, 8) catch return error.GraphSegmentTooLarge) catch return error.GraphSegmentTooLarge;
    const directory = topologyDirectorySize(kinds, nodes, covered, routing_extra);
    if (directory > max_topology_directory_bytes) return error.GraphSegmentTooLarge;
    const bytes = if (directory == 4) 0 else std.math.mul(usize, std.math.add(usize, edges, nodes) catch return error.GraphSegmentTooLarge, 8) catch return error.GraphSegmentTooLarge;
    return std.math.add(usize, std.math.add(usize, bytes, routing_extra) catch return error.GraphSegmentTooLarge, directory + topologyRootSize(directory) + topology_trailer_len) catch error.GraphSegmentTooLarge;
}

pub const TopologyTrailer = struct {
    body_len: u64,
    directory_len: u32,
    checksum: [32]u8,
    source_nodes: u32,
    source_edges: u64,
    topology_len: u64,
    adjacency_index_len: u64,
    root_checksum: [32]u8,

    pub fn directoryOffset(self: @This()) u64 {
        return self.body_len + self.topology_len + self.adjacency_index_len;
    }
    pub fn rootOffset(self: @This()) u64 {
        return self.directoryOffset() + self.directory_len;
    }
};

pub fn bindTopologyControl(ref: anytype, payload: []const u8) !void {
    if (payload.len < header_len + topology_trailer_len or
        !std.mem.eql(u8, payload[0..4], wire_magic) or
        std.mem.readInt(u16, payload[4..6], .little) != wire_version) return error.InvalidGraphSegment;
    const footer = payload[payload.len - topology_trailer_len ..];
    _ = try decodeTopologyTrailer(footer, payload.len);
    std.crypto.hash.sha2.Sha256.hash(footer, &ref.graph_topology_control_checksum, .{});
}

pub fn decodeTopologyTrailer(raw: []const u8, payload_len: u64) !TopologyTrailer {
    if (raw.len != topology_trailer_len or !std.mem.eql(u8, raw[0..4], "GTD5")) return error.InvalidGraphSegment;
    const dir_len = std.mem.readInt(u32, raw[4..8], .little);
    const body_len = std.mem.readInt(u64, raw[8..16], .little);
    const topology_len = std.mem.readInt(u64, raw[64..72], .little);
    const adjacency_index_len = std.mem.readInt(u64, raw[72..80], .little);
    if (dir_len < 4 or dir_len > max_topology_directory_bytes or body_len < header_len or
        body_len > payload_len or topology_len > payload_len - body_len or topology_len % 8 != 0 or
        adjacency_index_len > payload_len - body_len - topology_len or adjacency_index_len % 8 != 0 or
        payload_len - body_len - topology_len - adjacency_index_len != @as(u64, dir_len) + topologyRootSize(dir_len) + topology_trailer_len) return error.InvalidGraphSegment;
    if (!std.mem.eql(u8, raw[60..64], &.{ 0, 0, 0, 0 })) return error.InvalidGraphSegment;
    return .{ .body_len = body_len, .topology_len = topology_len, .adjacency_index_len = adjacency_index_len, .directory_len = dir_len, .checksum = raw[16..48].*, .root_checksum = raw[80..112].*, .source_nodes = std.mem.readInt(u32, raw[48..52], .little), .source_edges = std.mem.readInt(u64, raw[52..60], .little) };
}

test "serverless graph topology directory is bounded authenticated and distinguishes empty from unavailable" {
    const alloc = std.testing.allocator;
    const Filter = struct { mode: enum { all, types } = .all, types: []const []const u8 = &.{} };
    const payload = try encodeAlloc(alloc, .{ .adjacencies = &.{} });
    defer alloc.free(payload);
    const trailer = try decodeTopologyTrailer(payload[payload.len - topology_trailer_len ..], payload.len);
    // Topology-only readers also reject pre-directory graph controls; they
    // must not accidentally accept an older wire merely because the queried
    // projection's edge records happen to have the same shape.
    @memcpy(payload[payload.len - topology_trailer_len ..][0..4], "GTD4");
    try std.testing.expectError(error.InvalidGraphSegment, decodeTopologyTrailer(payload[payload.len - topology_trailer_len ..], payload.len));
    @memcpy(payload[payload.len - topology_trailer_len ..][0..4], "GTD5");
    const raw = payload[@intCast(trailer.directoryOffset())..][0..trailer.directory_len];
    const digest = (try selectedDirectoryChecksum(raw, trailer.checksum, Filter{})).?;
    const empty = (try selectedDirectoryChecksum(raw, trailer.checksum, Filter{ .mode = .types, .types = &.{"absent"} })).?;
    try std.testing.expectEqualSlices(u8, &digest, &empty);
    raw[0] ^= 1;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, selectedDirectoryChecksum(raw, trailer.checksum, Filter{}));
    std.mem.writeInt(u32, raw[0..4], absent_directory, .little);
    @memset(raw[4..], 0);
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &checksum, .{});
    try std.testing.expect((try selectedDirectoryChecksum(raw, checksum, Filter{})) == null);
    try std.testing.expect(topologyDirectorySize(&.{"link"}, 1024 * 1024, 1024 * 1024, 0) > 4);
    try std.testing.expect(topologyDirectorySize(&.{"link"}, 1, 1, 8 * 1024 * 1024) > 4);
    try std.testing.expect(topologyDirectorySize(&.{"link"}, 3300000, 211200030, 0) > 1024 * 1024);
    // Forged sizes are rejected before any range allocation or request.
    const footer = payload[payload.len - topology_trailer_len ..];
    std.mem.writeInt(u32, footer[4..8], max_topology_directory_bytes + 1, .little);
    try std.testing.expectError(error.InvalidGraphSegment, decodeTopologyTrailer(footer, payload.len));
}

/// Authenticated type runs and addressable pages of the original node
/// dictionary. The directory is small; edge data is never copied into it.
pub const TypeEntry = struct { kind: []const u8, edges: u64, digest: [32]u8, offset: u64 };
pub const TypeIterator = struct {
    bytes: []const u8,
    pos: usize = 0,
    pub fn next(self: *@This()) !?TypeEntry {
        if (self.pos == self.bytes.len) return null;
        const tail = self.bytes[self.pos..];
        if (tail.len < 52) return error.InvalidGraphSegment;
        const len = std.mem.readInt(u32, tail[0..4], .little);
        if (len > tail.len - 52) return error.InvalidGraphSegment;
        const meta = tail[4 + len ..];
        self.pos += 52 + len;
        return .{ .kind = tail[4..][0..len], .edges = std.mem.readInt(u64, meta[0..8], .little), .digest = meta[8..40].*, .offset = std.mem.readInt(u64, meta[40..48], .little) };
    }
};
pub const TopologyDirectory = struct {
    nodes: u32,
    page_offsets: []const u8,
    page_fences: []const u8,
    block_checksums: []const u8,
    type_offsets: []const u8,
    entries: []const u8,
    pub fn iterator(self: @This()) TypeIterator {
        return .{ .bytes = self.entries };
    }
    pub fn nodePage(self: @This(), page: usize) !struct { offset: u64, len: u64 } {
        if (page + 1 >= self.page_offsets.len / 8) return error.InvalidGraphSegment;
        const begin = std.mem.readInt(u64, self.page_offsets[page * 8 ..][0..8], .little);
        const end = std.mem.readInt(u64, self.page_offsets[(page + 1) * 8 ..][0..8], .little);
        if (end < begin) return error.InvalidGraphSegment;
        return .{ .offset = begin, .len = end - begin };
    }
    pub fn init(raw: []const u8, expected: [32]u8) !?@This() {
        if (raw.len < 4 or raw.len > max_topology_directory_bytes) return error.InvalidGraphSegment;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
        if (!std.mem.eql(u8, &digest, &expected)) return error.ArtifactIntegrityMismatch;
        const count = std.mem.readInt(u32, raw[0..4], .little);
        if (count == absent_directory) {
            for (raw[4..]) |byte| if (byte != 0) return error.InvalidGraphSegment;
            return null;
        }
        if (raw.len < 24) return error.InvalidGraphSegment;
        const nodes = std.mem.readInt(u32, raw[4..8], .little);
        const pages = std.mem.readInt(u32, raw[8..12], .little);
        if (pages != nodes / node_page_entries + @intFromBool(nodes % node_page_entries != 0) or
            @as(u64, pages) + 1 > (raw.len - 16) / 8) return error.InvalidGraphSegment;
        const offsets_end = 16 + (@as(usize, pages) + 1) * 8;
        if (pages > (raw.len - offsets_end) / node_page_fence_bytes) return error.InvalidGraphSegment;
        const end = offsets_end + @as(usize, pages) * node_page_fence_bytes;
        const blocks = std.mem.readInt(u32, raw[12..16], .little);
        if (blocks > (raw.len - end) / 32) return error.InvalidGraphSegment;
        const checksums_end = end + @as(usize, blocks) * 32;
        if (@as(u64, count) + 1 > (raw.len - checksums_end) / 8) return error.InvalidGraphSegment;
        const entries_start = checksums_end + (@as(usize, count) + 1) * 8;
        const result = @This(){ .nodes = nodes, .page_offsets = raw[16..offsets_end], .page_fences = raw[offsets_end..end], .block_checksums = raw[end..checksums_end], .type_offsets = raw[checksums_end..entries_start], .entries = raw[entries_start..] };
        var previous_fence: ?[]const u8 = null;
        for (0..pages) |page| {
            _ = try result.nodePage(page);
            const fence = result.page_fences[page * node_page_fence_bytes ..][0..node_page_fence_bytes];
            const len = @min(std.mem.readInt(u32, fence[0..4], .little), 64);
            const prefix = fence[4..][0..len];
            if (previous_fence) |prior| if (std.mem.order(u8, prior, prefix) == .gt) return error.InvalidGraphSegment;
            previous_fence = prefix;
            for (fence[4 + len ..]) |byte| if (byte != 0) return error.InvalidGraphSegment;
        }
        var entries = result.iterator();
        var previous: ?[]const u8 = null;
        var previous_end: ?u64 = null;
        var seen: u32 = 0;
        while (try entries.next()) |entry| {
            if (seen >= count) return error.InvalidGraphSegment;
            if (std.mem.readInt(u64, result.type_offsets[@as(usize, seen) * 8 ..][0..8], .little) != entries_start + entries.pos - 52 - entry.kind.len) return error.InvalidGraphSegment;
            if (!edge_type.isValid(entry.kind)) return error.InvalidGraphSegment;
            if (previous) |name| if (std.mem.order(u8, name, entry.kind) != .lt) return error.InvalidGraphSegment;
            if (previous_end) |offset| if (entry.offset != offset) return error.InvalidGraphSegment;
            previous_end = std.math.add(u64, entry.offset, std.math.mul(u64, entry.edges, 8) catch return error.InvalidGraphSegment) catch return error.InvalidGraphSegment;
            previous = entry.kind;
            seen = std.math.add(u32, seen, 1) catch return error.InvalidGraphSegment;
        }
        if (seen != count or std.mem.readInt(u64, result.type_offsets[@as(usize, count) * 8 ..][0..8], .little) != raw.len) return error.InvalidGraphSegment;
        return result;
    }
};

pub fn selectedDirectoryChecksum(raw: []const u8, expected: [32]u8, filter: anytype) !?[32]u8 {
    const directory = (try TopologyDirectory.init(raw, expected)) orelse return null;
    var entries = directory.iterator();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:selected-unweighted-topology:v1");
    while (try entries.next()) |entry| {
        const selected = filter.mode == .all or for (filter.types) |name| {
            if (std.mem.eql(u8, name, entry.kind)) break true;
        } else false;
        if (entry.edges > 0 and selected) hash.update(&entry.digest);
    }
    return hash.finalResult();
}

/// Stream adjacency twice using compact node offsets and a bounded hash cache.
/// No adjacency view or graph-wide digest array coexists with the encoder.
/// Topology edges scatter directly into their final immutable type runs.
pub fn finishEncoding(alloc: Allocator, payload: []u8, body_len: usize, directory_len: usize, routing_extra: usize, cancellation: CancellationToken) !void {
    if (payload.len < topology_trailer_len or directory_len > payload.len - topology_trailer_len or body_len < header_len or body_len > payload.len - topology_trailer_len - directory_len) return error.InvalidGraphSegment;
    const root_len = topologyRootSize(directory_len);
    if (root_len > payload.len - topology_trailer_len - directory_len) return error.InvalidGraphSegment;
    const directory_start = payload.len - topology_trailer_len - root_len - directory_len;
    const directory = payload[directory_start..][0..directory_len];
    const adjacency_index_len = std.math.add(usize, @as(usize, std.mem.readInt(u32, payload[10..14], .little)) * 8, routing_extra) catch return error.InvalidGraphSegment;
    if (adjacency_index_len > directory_start - body_len) return error.InvalidGraphSegment;
    const routing_start = directory_start - adjacency_index_len;
    @memset(payload[routing_start..directory_start], 0);
    const topology_len = routing_start - body_len;
    const type_count = std.mem.readInt(u32, payload[14..18], .little);
    var source_edges: u64 = 0;
    if (directory.len == 4) {
        std.mem.writeInt(u32, directory[0..4], absent_directory, .little);
    } else {
        const node_count = std.mem.readInt(u32, payload[10..14], .little);
        const table_count = std.mem.readInt(u32, payload[6..10], .little);
        const row_count = std.mem.readInt(u32, payload[18..22], .little);
        const offsets = try alloc.alloc(usize, node_count);
        defer alloc.free(offsets);
        const local = try alloc.alloc(bool, node_count);
        defer alloc.free(local);
        @memset(local, false);
        const State = struct { kind: []const u8, count: u64 = 0, hash: std.crypto.hash.sha2.Sha256 = undefined, start: usize = 0, cursor: usize = 0 };
        const states = try alloc.alloc(State, type_count);
        defer alloc.free(states);
        var cursor = Cursor{ .bytes = payload[0..body_len] };
        for (0..table_count) |_| {
            const name = try cursor.take(try cursor.int());
            if (name.len == 0) return error.InvalidGraphSegment;
        }
        const pages = node_count / node_page_entries + @intFromBool(node_count % node_page_entries != 0);
        const fences_start = 16 + (@as(usize, pages) + 1) * 8;
        const fences = directory[fences_start..][0 .. @as(usize, pages) * node_page_fence_bytes];
        @memset(fences, 0);
        var pos: usize = 0;
        put(directory, &pos, type_count);
        put(directory, &pos, node_count);
        put(directory, &pos, pages);
        const block_count = directory_start / authentication_block_bytes + @intFromBool(directory_start % authentication_block_bytes != 0);
        put(directory, &pos, @intCast(block_count));
        var prior_node: ?[]const u8 = null;
        for (offsets, 0..) |*offset, i| {
            if (i % node_page_entries == 0) {
                try cancellation.check();
                std.mem.writeInt(u64, directory[pos..][0..8], cursor.pos, .little);
                pos += 8;
            }
            offset.* = cursor.pos;
            const node = try cursor.take(try cursor.int());
            if (i % node_page_entries == 0) {
                const fence = fences[i / node_page_entries * node_page_fence_bytes ..][0..node_page_fence_bytes];
                std.mem.writeInt(u32, fence[0..4], @intCast(node.len), .little);
                @memcpy(fence[4..][0..@min(node.len, 64)], node[0..@min(node.len, 64)]);
            }
            if (prior_node) |previous| if (std.mem.order(u8, previous, node) != .lt) return error.InvalidGraphSegment;
            prior_node = node;
        }
        std.mem.writeInt(u64, directory[pos..][0..8], cursor.pos, .little);
        pos += 8;
        pos += fences.len;
        const block_checksums_start = pos;
        pos += block_count * 32;
        const type_offsets_start = pos;
        pos += (@as(usize, type_count) + 1) * 8;
        for (states, 0..) |*state, i| {
            const kind = try cursor.take(try cursor.int());
            if (!edge_type.isValid(kind) or (i > 0 and std.mem.order(u8, states[i - 1].kind, kind) != .lt)) return error.InvalidGraphSegment;
            state.* = .{ .kind = kind };
        }
        const rows_start = cursor.pos;
        var complete = true;
        var previous_row: ?u32 = null;
        var run_pos = routing_start + @as(usize, node_count) * 8;
        for (0..row_count) |_| {
            try cancellation.check();
            const row_offset = cursor.pos;
            const node = try cursor.int();
            const outgoing = try cursor.int();
            const incoming = try cursor.int();
            if (node >= node_count) return error.InvalidGraphSegment;
            std.mem.writeInt(u64, payload[routing_start + @as(usize, node) * 8 ..][0..8], row_offset, .little);
            if (previous_row) |previous| {
                if (node <= previous) complete = false;
            }
            previous_row = node;
            if (local[node]) complete = false;
            local[node] = true;
            source_edges += outgoing;
            const reservation = typeRunReservation(outgoing, incoming, type_count);
            if (reservation > directory_start - run_pos) return error.InvalidGraphSegment;
            const descriptor = payload[run_pos..][0..reservation];
            var entry_pos: usize = 16;
            var indexed = false;
            for ([_]u32{ outgoing, incoming }, 0..) |count, direction| {
                const bytes = try cursor.take(std.math.mul(usize, count, edge_len) catch return error.InvalidGraphSegment);
                var previous: ?Edge = null;
                const capacity = typeRunCapacity(count, type_count);
                var runs: usize = 0;
                for (0..count) |i| {
                    if (i % 4096 == 0) try cancellation.check();
                    const edge = readEdge(bytes, i);
                    if (edge.node >= node_count or edge.edge_type >= type_count or !std.math.isFinite(edge.weight)) return error.InvalidGraphSegment;
                    if (edge.table) |id| if (id >= table_count) return error.InvalidGraphSegment;
                    if (previous) |last| if (last.edge_type > edge.edge_type or (last.edge_type == edge.edge_type and
                        (last.node > edge.node or (last.node == edge.node and last.weight > edge.weight)))) return error.InvalidGraphSegment;
                    if (previous == null or previous.?.edge_type != edge.edge_type) {
                        if (runs < capacity) {
                            const entry = descriptor[entry_pos + runs * 8 ..][0..8];
                            std.mem.writeInt(u32, entry[0..4], edge.edge_type, .little);
                            std.mem.writeInt(u32, entry[4..8], @intCast(i), .little);
                        }
                        runs += 1;
                    }
                    previous = edge;
                    if (direction == 0 and edge.table == null) states[edge.edge_type].count += 1;
                }
                if (capacity > 0 and runs <= capacity) {
                    std.mem.writeInt(u32, descriptor[8 + direction * 4 ..][0..4], @intCast(runs), .little);
                    entry_pos += runs * 8;
                    indexed = true;
                }
            }
            if (indexed) {
                std.mem.writeInt(u64, descriptor[0..8], row_offset, .little);
                std.mem.writeInt(u64, payload[routing_start + @as(usize, node) * 8 ..][0..8], typed_row_flag | run_pos, .little);
            }
            run_pos += reservation;
        }
        if (cursor.pos != body_len or run_pos != directory_start) return error.InvalidGraphSegment;
        var start = body_len;
        for (states) |*state| {
            state.start = start;
            state.cursor = start;
            start = std.math.add(usize, start, std.math.mul(usize, @intCast(state.count), 8) catch return error.GraphSegmentTooLarge) catch return error.GraphSegmentTooLarge;
            state.hash = std.crypto.hash.sha2.Sha256.init(.{});
            state.hash.update("antfly:unweighted-type:v1");
            var value: [8]u8 = undefined;
            std.mem.writeInt(u64, &value, state.kind.len, .little);
            state.hash.update(&value);
            state.hash.update(state.kind);
            std.mem.writeInt(u64, &value, state.count, .little);
            state.hash.update(&value);
        }
        if (start != routing_start) return error.InvalidGraphSegment;
        const Cache = struct {
            const Entry = struct { ordinal: u32 = no_table, digest: [32]u8 = undefined };
            entries: []Entry,
            fn hash(self: @This(), bytes: []const u8, positions: []const usize, ordinal: u32) [32]u8 {
                const entry = &self.entries[ordinal % self.entries.len];
                if (entry.ordinal != ordinal) {
                    const offset = positions[ordinal];
                    const len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
                    std.crypto.hash.sha2.Sha256.hash(bytes[offset + 4 ..][0..len], &entry.digest, .{});
                    entry.ordinal = ordinal;
                }
                return entry.digest;
            }
        };
        const cache = Cache{ .entries = try alloc.alloc(Cache.Entry, @max(1, @min(node_count, 65536))) };
        defer alloc.free(cache.entries);
        @memset(cache.entries, .{});
        cursor.pos = rows_start;
        for (0..row_count) |_| {
            const node = try cursor.int();
            const outgoing = try cursor.int();
            const incoming = try cursor.int();
            const bytes = try cursor.take(@as(usize, outgoing) * edge_len);
            const source_hash = cache.hash(payload, offsets, node);
            for (0..outgoing) |i| {
                if (i % 4096 == 0) try cancellation.check();
                const edge = readEdge(bytes, i);
                if (edge.table != null) continue;
                if (!local[edge.node]) complete = false;
                const state = &states[edge.edge_type];
                state.hash.update(&source_hash);
                state.hash.update(&cache.hash(payload, offsets, edge.node));
                put(payload, &state.cursor, node);
                put(payload, &state.cursor, edge.node);
            }
            _ = try cursor.take(@as(usize, incoming) * edge_len);
        }
        for (states, 0..) |*state, type_id| {
            std.mem.writeInt(u64, directory[type_offsets_start + type_id * 8 ..][0..8], pos, .little);
            putString(directory, &pos, state.kind);
            std.mem.writeInt(u64, directory[pos..][0..8], state.count, .little);
            state.hash.final(directory[pos + 8 ..][0..32]);
            std.mem.writeInt(u64, directory[pos + 40 ..][0..8], state.start, .little);
            pos += 48;
        }
        std.mem.writeInt(u64, directory[type_offsets_start + @as(usize, type_count) * 8 ..][0..8], pos, .little);
        if (pos != directory.len) return error.InvalidGraphSegment;
        for (0..block_count) |block| {
            try cancellation.check();
            const begin = block * authentication_block_bytes;
            std.crypto.hash.sha2.Sha256.hash(payload[begin..@min(directory_start, begin + authentication_block_bytes)], directory[block_checksums_start + block * 32 ..][0..32], .{});
        }
        if (!complete) {
            @memset(directory, 0);
            std.mem.writeInt(u32, directory[0..4], absent_directory, .little);
        }
    }
    const trailer = payload[payload.len - topology_trailer_len ..];
    const root = payload[directory_start + directory_len ..][0..root_len];
    @memset(root, 0);
    @memcpy(root[0..@min(16, directory.len)], directory[0..@min(16, directory.len)]);
    for (0..(root_len - 16) / 32) |leaf| {
        try cancellation.check();
        const begin = leaf * directory_leaf_bytes;
        std.crypto.hash.sha2.Sha256.hash(directory[begin..@min(directory.len, begin + directory_leaf_bytes)], root[16 + leaf * 32 ..][0..32], .{});
    }
    @memset(trailer, 0);
    @memcpy(trailer[0..4], "GTD5");
    std.mem.writeInt(u32, trailer[4..8], @intCast(directory.len), .little);
    std.mem.writeInt(u64, trailer[8..16], body_len, .little);
    std.crypto.hash.sha2.Sha256.hash(directory, trailer[16..48], .{});
    @memcpy(trailer[48..52], payload[18..22]);
    std.mem.writeInt(u64, trailer[52..60], source_edges, .little);
    std.mem.writeInt(u64, trailer[64..72], topology_len, .little);
    std.mem.writeInt(u64, trailer[72..80], adjacency_index_len, .little);
    std.crypto.hash.sha2.Sha256.hash(root, trailer[80..112], .{});
}

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
    local_edges: usize = 0,
    routing_extra: usize = 0,
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
            for (adjacency.out_edges) |edge| if (edge.neighbor_table_id == null) {
                plan.local_edges = std.math.add(usize, plan.local_edges, 1) catch return error.GraphSegmentTooLarge;
            };
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
        for (segment.adjacencies) |adjacency| {
            plan.routing_extra = std.math.add(usize, plan.routing_extra, typeRunReservation(adjacency.out_edges.len, adjacency.in_edges.len, plan.edge_types.values.items.len)) catch return error.GraphSegmentTooLarge;
        }
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
    return std.math.add(usize, plan.size, try topologyExtensionSize(plan.edge_types.values.items, plan.nodes.values.items.len, plan.local_edges, plan.size, plan.routing_extra)) catch error.GraphSegmentTooLarge;
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
    const size = std.math.add(usize, plan.size, try topologyExtensionSize(plan.edge_types.values.items, plan.nodes.values.items.len, plan.local_edges, plan.size, plan.routing_extra)) catch return error.GraphSegmentTooLarge;
    if (size > max_bytes) return error.GraphSegmentTooLarge;
    const buf = try alloc.alloc(u8, size);
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
    std.debug.assert(pos == plan.size);
    try finishEncoding(alloc, buf, plan.size, topologyDirectorySize(plan.edge_types.values.items, plan.nodes.values.items.len, plan.size + plan.local_edges * 8, plan.routing_extra), plan.routing_extra, cancellation);
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
    // Check the version before inspecting the current-only extension layout.
    if (data.len < 6) return error.InvalidGraphSegment;
    if (std.mem.readInt(u16, data[4..6], .little) != wire_version) return error.UnsupportedGraphSegmentVersion;
    if (data.len < topology_trailer_len) return error.InvalidGraphSegment;
    const trailer = try decodeTopologyTrailer(data[data.len - topology_trailer_len ..], data.len);
    const body_len: usize = @intCast(trailer.body_len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data[@intCast(trailer.directoryOffset())..][0..trailer.directory_len], &digest, .{});
    if (!std.mem.eql(u8, &digest, &trailer.checksum)) return error.InvalidGraphSegment;
    std.crypto.hash.sha2.Sha256.hash(data[@intCast(trailer.rootOffset())..][0..topologyRootSize(trailer.directory_len)], &digest, .{});
    if (!std.mem.eql(u8, &digest, &trailer.root_checksum)) return error.InvalidGraphSegment;
    return readView(limiter.allocator(), data[0..body_len], limits.max_elements, cancellation) catch |err| {
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
