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
/// Query-owned adapter to a shared authenticated cache. Preparation remains
/// independent of serving/runtime ownership and can use uncached coalesced I/O.
pub const ReadCache = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, Allocator, *artifacts.ArtifactStore, refs.ArtifactRef, u64, usize, [32]u8, CancellationToken, *u64) anyerror![]u8,
};
pub const Topology = struct {
    node_ids: []const []const u8,
    edge_types: []const []const u8,
    string_bytes: []u8,
    edge_type_offsets: []const u32,
    edges: []const Edge,
    source_node_count: usize,
    source_edge_count: usize,
    retained_bytes: usize,
    type_checksums: []const [32]u8 = &.{},

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.node_ids);
        alloc.free(self.edge_types);
        alloc.free(self.string_bytes);
        alloc.free(self.edge_type_offsets);
        alloc.free(self.edges);
        alloc.free(self.type_checksums);
        self.* = undefined;
    }
};

const Reader = struct {
    alloc: Allocator,
    store: *artifacts.ArtifactStore,
    source: refs.ArtifactRef,
    cancellation: CancellationToken,
    remaining: *u64,
    cache: ?ReadCache = null,

    fn authenticated(self: @This(), offset: u64, len: usize, checksum: [32]u8) ![]u8 {
        if (self.cache) |cache| return cache.read(cache.ptr, self.alloc, self.store, self.source, offset, len, checksum, self.cancellation, self.remaining);
        const bytes = try self.raw(offset, len);
        errdefer self.alloc.free(bytes);
        try Context.verify(bytes, checksum);
        return bytes;
    }

    fn raw(self: @This(), offset: u64, len: usize) ![]u8 {
        if (len > self.remaining.*) return error.GraphMetricBuildBudgetExceeded;
        self.remaining.* -= len;
        const bytes = try self.store.getRangeAllocWithCancellationUsingAllocator(self.alloc, self.source.artifact_id, offset, len, self.cancellation);
        errdefer self.alloc.free(bytes);
        if (bytes.len != len) return error.ArtifactIntegrityMismatch;
        return bytes;
    }

    fn read(self: @This(), offset: u64, len: u64) ![]u8 {
        try self.cancellation.check();
        if (offset > self.source.byte_len or len > self.source.byte_len - offset) return error.InvalidGraphSegment;
        return self.store.getVerifiedRangeAllocWithBudget(self.alloc, self.source.artifact_id, self.source.byte_len, self.source.checksum, offset, std.math.cast(usize, len) orelse return error.GraphMetricBuildBudgetExceeded, self.cancellation, self.remaining) catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded => error.GraphMetricBuildBudgetExceeded,
            else => err,
        };
    }
};

/// One immutable source control shared by bounded preparation groups. A
/// manifest-bound footer authenticates the directory; the directory binds all
/// data blocks and semantic type identities. No data-range response is trusted.
pub const Context = struct {
    reader: Reader,
    trailer: wire.TopologyTrailer,
    bytes: []u8,
    directory: ?wire.TopologyDirectory,
    layout: ?Layout = null,
    leaf_bytes: [4][]u8 = @splat(&.{}),
    leaf_indices: [4]?usize = @splat(null),
    next_leaf: usize = 0,
    // One authenticated tail block survives adjacent type runs and preparation
    // groups. Large ranges remain one GET; only their boundary block is retained.
    block_bytes: []u8,
    block_offsets: [8]u64 = @splat(0),
    block_lens: [8]usize = @splat(0),
    cache_slots: usize,
    next_slot: usize = 0,

    pub const Layout = struct {
        nodes: u32,
        types: u32,
        pages: usize,
        fences: usize,
        checksums: usize,
        type_offsets: usize,
        fn init(header: []const u8, trailer: wire.TopologyTrailer) !?Layout {
            if (header.len < 16) return error.InvalidGraphSegment;
            const types = std.mem.readInt(u32, header[0..4], .little);
            if (types == std.math.maxInt(u32)) return null;
            const nodes = std.mem.readInt(u32, header[4..8], .little);
            const pages = std.mem.readInt(u32, header[8..12], .little);
            const blocks = std.mem.readInt(u32, header[12..16], .little);
            const covered = trailer.directoryOffset();
            if (pages != nodes / wire.node_page_entries + @intFromBool(nodes % wire.node_page_entries != 0) or
                blocks != (covered + wire.authentication_block_bytes - 1) / wire.authentication_block_bytes or
                trailer.adjacency_index_len < @as(u64, nodes) * 8) return error.InvalidGraphSegment;
            const fences: u64 = 16 + (@as(u64, pages) + 1) * 8;
            const checksums = fences + @as(u64, pages) * wire.node_page_fence_bytes;
            const type_offsets = checksums + @as(u64, blocks) * 32;
            if (type_offsets + (@as(u64, types) + 1) * 8 > trailer.directory_len) return error.InvalidGraphSegment;
            return .{ .nodes = nodes, .types = types, .pages = pages, .fences = @intCast(fences), .checksums = @intCast(checksums), .type_offsets = @intCast(type_offsets) };
        }
    };

    pub fn init(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64) !Context {
        return initWithCache(alloc, store, source, cancellation, remaining, 1);
    }

    pub fn initWithCache(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64, requested_slots: usize) !Context {
        return initInternal(alloc, store, source, cancellation, remaining, requested_slots, false, null);
    }

    pub fn initQuery(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64, cache: ?ReadCache) !Context {
        return initInternal(alloc, store, source, cancellation, remaining, 8, true, cache);
    }

    fn initInternal(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64, requested_slots: usize, lazy: bool, cache: ?ReadCache) !Context {
        if (requested_slots == 0 or requested_slots > 8) return error.InvalidGraphSegment;
        if (source.byte_len < wire.topology_trailer_len) return error.InvalidGraphSegment;
        try artifacts.validateSha256ArtifactIdentity(source.artifact_id, source.checksum);
        const reader = Reader{ .alloc = alloc, .store = store, .source = source, .cancellation = cancellation, .remaining = remaining, .cache = cache };
        const bound = !std.mem.eql(u8, &source.graph_topology_control_checksum, &@as([32]u8, @splat(0)));
        const footer = if (bound) try reader.authenticated(source.byte_len - wire.topology_trailer_len, wire.topology_trailer_len, source.graph_topology_control_checksum) else try reader.read(source.byte_len - wire.topology_trailer_len, wire.topology_trailer_len);
        defer alloc.free(footer);
        if (bound) try verify(footer, source.graph_topology_control_checksum);
        const trailer = try wire.decodeTopologyTrailer(footer, source.byte_len);
        const raw = if (lazy) try reader.authenticated(trailer.rootOffset(), wire.topologyRootSize(trailer.directory_len), trailer.root_checksum) else try reader.raw(trailer.directoryOffset(), trailer.directory_len);
        errdefer alloc.free(raw);
        if (lazy) try verify(raw, trailer.root_checksum);
        const directory = if (lazy) null else try wire.TopologyDirectory.init(raw, trailer.checksum);
        const layout = if (raw.len >= 16) try Layout.init(raw[0..16], trailer) else null;
        if (directory) |dir| {
            const covered = trailer.directoryOffset();
            const blocks = covered / wire.authentication_block_bytes + @intFromBool(covered % wire.authentication_block_bytes != 0);
            if (dir.block_checksums.len / 32 != blocks) return error.InvalidGraphSegment;
            if (trailer.adjacency_index_len < @as(u64, dir.nodes) * 8) return error.InvalidGraphSegment;
        }
        const covered = trailer.directoryOffset();
        const slots: usize = if (layout != null) @intCast(@min(requested_slots, (covered + wire.authentication_block_bytes - 1) / wire.authentication_block_bytes)) else 0;
        const capacity: usize = if (slots == 1) @intCast(@min(wire.authentication_block_bytes, covered)) else slots * wire.authentication_block_bytes;
        const block_bytes = try alloc.alloc(u8, capacity);
        return .{ .reader = reader, .trailer = trailer, .bytes = raw, .directory = directory, .layout = layout, .block_bytes = block_bytes, .cache_slots = slots };
    }

    pub fn deinit(self: *Context) void {
        self.reader.alloc.free(self.bytes);
        self.reader.alloc.free(self.block_bytes);
        for (self.leaf_bytes) |bytes| self.reader.alloc.free(bytes);
        self.* = undefined;
    }

    fn verify(bytes: []const u8, checksum: [32]u8) !void {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (!std.mem.eql(u8, &digest, &checksum)) return error.ArtifactIntegrityMismatch;
    }

    pub fn retainedBytes(self: Context) usize {
        var size = self.bytes.len + self.block_bytes.len;
        for (self.leaf_bytes) |bytes| size += bytes.len;
        return size;
    }

    pub fn directoryReadAlloc(self: *Context, offset: usize, len: usize) ![]u8 {
        const alloc = self.reader.alloc;
        if (offset > self.trailer.directory_len or len > self.trailer.directory_len - offset) return error.InvalidGraphSegment;
        if (self.directory != null) return alloc.dupe(u8, self.bytes[offset..][0..len]);
        if (self.layout == null) return error.InvalidGraphSegment;
        const result = try alloc.alloc(u8, len);
        errdefer alloc.free(result);
        var copied: usize = 0;
        while (copied < len) {
            try self.reader.cancellation.check();
            const at = offset + copied;
            const leaf = at / wire.directory_leaf_bytes;
            const slot = for (self.leaf_indices, 0..) |index, i| {
                if (index == leaf) break i;
            } else blk: {
                const i = self.next_leaf;
                alloc.free(self.leaf_bytes[i]);
                self.leaf_bytes[i] = &.{};
                self.leaf_indices[i] = null;
                const begin = leaf * wire.directory_leaf_bytes;
                const size = @min(wire.directory_leaf_bytes, self.trailer.directory_len - begin);
                self.leaf_bytes[i] = try self.reader.authenticated(self.trailer.directoryOffset() + begin, size, self.bytes[16 + leaf * 32 ..][0..32].*);
                try verify(self.leaf_bytes[i], self.bytes[16 + leaf * 32 ..][0..32].*);
                self.leaf_indices[i] = leaf;
                self.next_leaf = (i + 1) % self.leaf_bytes.len;
                break :blk i;
            };
            const skip = at % wire.directory_leaf_bytes;
            const n = @min(len - copied, self.leaf_bytes[slot].len - skip);
            @memcpy(result[copied..][0..n], self.leaf_bytes[slot][skip..][0..n]);
            copied += n;
        }
        return result;
    }

    pub fn control(self: *Context, comptime size: usize, offset: usize) ![size]u8 {
        const bytes = try self.directoryReadAlloc(offset, size);
        defer self.reader.alloc.free(bytes);
        return bytes[0..size].*;
    }

    pub fn nodePage(self: *Context, page: usize) !struct { offset: u64, len: u64 } {
        if (page >= self.layout.?.pages) return error.InvalidGraphSegment;
        const raw = try self.control(16, 16 + page * 8);
        const begin = std.mem.readInt(u64, raw[0..8], .little);
        const end = std.mem.readInt(u64, raw[8..16], .little);
        if (begin < wire.header_len or end < begin or end > self.trailer.body_len) return error.InvalidGraphSegment;
        return .{ .offset = begin, .len = end - begin };
    }

    pub fn nodeFence(self: *Context, page: usize) ![wire.node_page_fence_bytes]u8 {
        if (page >= self.layout.?.pages) return error.InvalidGraphSegment;
        return self.control(wire.node_page_fence_bytes, self.layout.?.fences + page * wire.node_page_fence_bytes);
    }

    pub fn kindAlloc(self: *Context, id: usize) ![]u8 {
        const layout = self.layout.?;
        if (id >= layout.types) return error.InvalidGraphSegment;
        const positions = try self.control(16, layout.type_offsets + id * 8);
        const begin = std.mem.readInt(u64, positions[0..8], .little);
        const end = std.mem.readInt(u64, positions[8..16], .little);
        const minimum = layout.type_offsets + (@as(u64, layout.types) + 1) * 8;
        if (begin < minimum or end < begin or end > self.trailer.directory_len or end - begin > 52 + @import("../../graph/edge_type.zig").max_bytes) return error.InvalidGraphSegment;
        const raw = try self.directoryReadAlloc(@intCast(begin), @intCast(end - begin));
        defer self.reader.alloc.free(raw);
        var it = wire.TypeIterator{ .bytes = raw };
        const entry = try it.next() orelse return error.InvalidGraphSegment;
        if (try it.next() != null or !@import("../../graph/edge_type.zig").isValid(entry.kind)) return error.InvalidGraphSegment;
        return self.reader.alloc.dupe(u8, entry.kind);
    }

    pub fn kindId(self: *Context, kind: []const u8) !?u32 {
        var lower: usize = 0;
        var upper: usize = self.layout.?.types;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const candidate = try self.kindAlloc(middle);
            defer self.reader.alloc.free(candidate);
            switch (std.mem.order(u8, candidate, kind)) {
                .eq => return @intCast(middle),
                .lt => lower = middle + 1,
                .gt => upper = middle,
            }
        }
        return null;
    }

    fn cachedSlot(self: *Context, offset: u64) ?usize {
        for (self.block_offsets[0..self.cache_slots], self.block_lens[0..self.cache_slots], 0..) |at, size, i| {
            if (at == offset and size != 0) return i;
        }
        return null;
    }

    /// Return an owned exact range, authenticating/coalescing only missing
    /// blocks. Cache memory belongs to the context; response memory belongs to
    /// the caller's allocator (and therefore its live-memory admission).
    pub fn readAlloc(self: *Context, alloc: Allocator, offset: u64, len: u64) ![]u8 {
        try self.reader.cancellation.check();
        const covered = self.trailer.directoryOffset();
        if (offset > covered or len > covered - offset) return error.InvalidGraphSegment;
        if (len == 0) return alloc.alloc(u8, 0);
        if (self.layout == null) return error.InvalidGraphSegment;
        const block_bytes = wire.authentication_block_bytes;
        var begin = offset / block_bytes * block_bytes;
        const end = @min(covered, (offset + len + block_bytes - 1) / block_bytes * block_bytes);
        var at = begin;
        const all_cached = while (at < offset + len) : (at += block_bytes) {
            if (self.cachedSlot(at) == null) break false;
        } else true;
        if (all_cached) {
            const result = try alloc.alloc(u8, @intCast(len));
            at = offset;
            var copied: usize = 0;
            while (copied < result.len) {
                const base = at / block_bytes * block_bytes;
                const i = self.cachedSlot(base).?;
                const skip: usize = @intCast(at - base);
                const count = @min(result.len - copied, self.block_lens[i] - skip);
                @memcpy(result[copied..][0..count], self.block_bytes[i * block_bytes + skip ..][0..count]);
                copied += count;
                at += count;
            }
            return result;
        }
        const slot = self.cachedSlot(begin);
        const cached: usize = if (slot) |i| @intCast(@min(len, self.block_lens[i] - (offset - begin))) else 0;
        const prefix_start: usize = if (slot) |i| i * block_bytes + @as(usize, @intCast(offset - begin)) else 0;
        if (cached == len) return alloc.dupe(u8, self.block_bytes[prefix_start..][0..cached]);
        const prefix = if (cached != 0) try alloc.dupe(u8, self.block_bytes[prefix_start..][0..cached]) else &.{};
        defer alloc.free(prefix);
        if (cached != 0) begin += self.block_lens[slot.?];
        var reader = self.reader;
        reader.alloc = alloc;
        const bytes = if (reader.cache != null) blk: {
            const result = try alloc.alloc(u8, @intCast(end - begin));
            errdefer alloc.free(result);
            var pos: usize = 0;
            while (pos < result.len) : (pos += block_bytes) {
                const n = @min(block_bytes, result.len - pos);
                const block: usize = @intCast(begin / block_bytes + pos / block_bytes);
                const checksum = try self.control(32, self.layout.?.checksums + block * 32);
                const part = try reader.authenticated(begin + pos, n, checksum);
                defer alloc.free(part);
                @memcpy(result[pos..][0..n], part);
            }
            break :blk result;
        } else try reader.raw(begin, @intCast(end - begin));
        errdefer alloc.free(bytes);
        var pos: usize = 0;
        while (pos < bytes.len) : (pos += block_bytes) {
            try self.reader.cancellation.check();
            const block: usize = @intCast(begin / block_bytes + pos / block_bytes);
            const checksum = try self.control(32, self.layout.?.checksums + block * 32);
            try verify(bytes[pos..@min(bytes.len, pos + block_bytes)], checksum);
        }
        const fetched_blocks = (bytes.len + block_bytes - 1) / block_bytes;
        for (fetched_blocks - @min(fetched_blocks, self.cache_slots)..fetched_blocks) |block| {
            const start = block * block_bytes;
            const size = @min(block_bytes, bytes.len - start);
            const tail_slot = self.cachedSlot(begin + start) orelse blk: {
                const next = self.next_slot;
                self.next_slot = (next + 1) % self.cache_slots;
                break :blk next;
            };
            @memcpy(self.block_bytes[tail_slot * block_bytes ..][0..size], bytes[start..][0..size]);
            self.block_offsets[tail_slot] = begin + start;
            self.block_lens[tail_slot] = size;
        }
        if (cached != 0) {
            const result = try alloc.alloc(u8, @intCast(len));
            @memcpy(result[0..cached], prefix);
            @memcpy(result[cached..], bytes[0..@intCast(len - cached)]);
            alloc.free(bytes);
            return result;
        }
        const start: usize = @intCast(offset - begin);
        std.mem.copyForwards(u8, bytes[0..@intCast(len)], bytes[start..][0..@intCast(len)]);
        return alloc.realloc(bytes, @intCast(len));
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
/// allowance. Manifest-bound sources authenticate only the touched blocks;
/// unbound current-wire sources additionally charge full-source verification.
pub fn readAlloc(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, configs: anytype, limits: anytype, cancellation: CancellationToken, remaining: *u64) !?Topology {
    var context = try Context.init(alloc, store, source, cancellation, remaining);
    defer context.deinit();
    return readPreparedAlloc(alloc, &context, configs, limits, cancellation);
}

pub fn readPreparedAlloc(alloc: Allocator, context: *Context, configs: anytype, limits: anytype, cancellation: CancellationToken) !?Topology {
    const reader = context;
    const trailer = reader.trailer;
    const directory = reader.directory orelse return null;
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
    if (selected_edges > limits.max_edges) return error.GraphMetricBuildBudgetExceeded;
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
    const checksums = try alloc.alloc([32]u8, selected_types);
    errdefer alloc.free(checksums);
    var strings = std.ArrayListUnmanaged(u8).empty;
    defer strings.deinit(alloc);
    iterator = directory.iterator();
    var edge_index: usize = 0;
    var type_index: usize = 0;
    while (try iterator.next()) |entry| {
        if (!selected(entry.kind, configs)) continue;
        kinds[type_index] = entry.kind;
        checksums[type_index] = entry.digest;
        type_offsets[type_index] = @intCast(edge_index);
        type_index += 1;
        var read_edges: u64 = 0;
        var previous: ?Edge = null;
        while (read_edges < entry.edges) {
            const count: usize = @intCast(@min(entry.edges - read_edges, 128 * 1024));
            const bytes = try reader.readAlloc(alloc, entry.offset + read_edges * 8, count * 8);
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
    if (unique > limits.max_nodes) return error.GraphMetricBuildBudgetExceeded;
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
        const bytes = try reader.readAlloc(alloc, range.offset, range.len);
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
    return .{ .node_ids = nodes, .edge_types = kinds, .string_bytes = string_bytes, .edge_type_offsets = type_offsets, .edges = edges, .type_checksums = checksums, .source_node_count = trailer.source_nodes, .source_edge_count = @intCast(trailer.source_edges), .retained_bytes = (nodes.len + kinds.len) * @sizeOf([]const u8) + string_bytes.len + type_offsets.len * 4 + edges.len * @sizeOf(Edge) + checksums.len * 32 };
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
