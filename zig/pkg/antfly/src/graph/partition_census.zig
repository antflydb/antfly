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

//! Generation-fenced partition census checkpoints. They are shared by every
//! metric on an index, and bounded by partition count rather than graph size.
const std = @import("std");
const Allocator = std.mem.Allocator;
const checksum_seed: u64 = 0xA17F_4345_4E53_0001;
pub const max_boundaries = 256;
const max_key_bytes = 1024 * 1024;
const header_len = 60;

pub const State = struct {
    generation: u64,
    edge_count: u64,
    node_count: u64,
    edges_seen: u64 = 0,
    nodes_seen: u64 = 0,
    edges_done: bool = false,
    phase: u8 = 0,
    edge_cursor: []u8 = &.{},
    node_cursor: []u8 = &.{},
    edge_boundaries: std.ArrayListUnmanaged([]u8) = .empty,
    node_boundaries: std.ArrayListUnmanaged([]u8) = .empty,
    persisted_edges: usize = 0,
    persisted_nodes: usize = 0,
    materialized: bool = false,

    pub fn boundaryCount(self: State, nodes: bool) usize {
        const pending = if (nodes) self.node_boundaries.items.len else self.edge_boundaries.items.len;
        return pending + if (self.materialized) @as(usize, 0) else if (nodes) self.persisted_nodes else self.persisted_edges;
    }

    fn boundaryKeyAlloc(alloc: Allocator, parent: []const u8, nodes: bool, index: usize) ![]u8 {
        var suffix = [_]u8{ '/', @intFromBool(nodes), 0, 0 };
        std.mem.writeInt(u16, suffix[2..4], @intCast(index), .big);
        return std.mem.concat(alloc, u8, &.{ parent, &suffix });
    }

    fn boundaryChecksum(generation: u64, nodes: bool, index: usize, key: []const u8) u64 {
        return std.hash.Wyhash.hash(checksum_seed ^ generation ^ (@as(u64, @intFromBool(nodes)) << 32) ^ index, key);
    }

    /// Only the new page's boundaries are written. Slots are reused on a
    /// generation restart, so churn cannot create unbounded abandoned records.
    pub fn persistBoundaries(self: State, alloc: Allocator, batch: anytype, parent: []const u8) !void {
        for ([_]bool{ false, true }) |nodes| {
            const count = if (nodes) self.persisted_nodes else self.persisted_edges;
            const list = if (nodes) self.node_boundaries.items else self.edge_boundaries.items;
            const pending = if (self.materialized) list[count..] else list;
            if (count + pending.len > max_boundaries) return error.InvalidGraphMetricPartitionCensus;
            for (pending, count..) |key, index| {
                if (key.len == 0 or key.len > max_key_bytes) return error.InvalidGraphMetricPartitionCensus;
                const storage_key = try boundaryKeyAlloc(alloc, parent, nodes, index);
                defer alloc.free(storage_key);
                const raw = try alloc.alloc(u8, 16 + key.len);
                defer alloc.free(raw);
                std.mem.writeInt(u64, raw[0..8], self.generation, .little);
                std.mem.writeInt(u64, raw[8..16], boundaryChecksum(self.generation, nodes, index, key), .little);
                @memcpy(raw[16..], key);
                try batch.put(storage_key, raw);
            }
        }
    }

    /// Called once, on completion, outside the write transaction. Ordinary
    /// progress steps never read or allocate previously persisted boundaries.
    pub fn materializeBoundaries(self: *State, alloc: Allocator, txn: anytype, parent: []const u8) !void {
        std.debug.assert(!self.materialized);
        var loaded: [2]std.ArrayListUnmanaged([]u8) = .{ .empty, .empty };
        defer for (&loaded) |*list| {
            for (list.items) |key| alloc.free(key);
            list.deinit(alloc);
        };
        for ([_]bool{ false, true }, &loaded) |nodes, *list| {
            const count = if (nodes) self.persisted_nodes else self.persisted_edges;
            const pending = if (nodes) &self.node_boundaries else &self.edge_boundaries;
            try list.ensureTotalCapacity(alloc, count + pending.items.len);
            for (0..count) |index| {
                const key = try boundaryKeyAlloc(alloc, parent, nodes, index);
                defer alloc.free(key);
                const raw = txn.get(key) catch |err| switch (err) {
                    error.NotFound => return error.InvalidGraphMetricPartitionCensus,
                    else => return err,
                };
                if (raw.len <= 16 or raw.len > 16 + max_key_bytes or
                    std.mem.readInt(u64, raw[0..8], .little) != self.generation or
                    std.mem.readInt(u64, raw[8..16], .little) != boundaryChecksum(self.generation, nodes, index, raw[16..]))
                    return error.InvalidGraphMetricPartitionCensus;
                list.appendAssumeCapacity(try alloc.dupe(u8, raw[16..]));
            }
            for (pending.items) |key| list.appendAssumeCapacity(try alloc.dupe(u8, key));
            for (list.items, 0..) |key, index| {
                if (index > 0 and std.mem.order(u8, list.items[index - 1], key) != .lt) return error.InvalidGraphMetricPartitionCensus;
            }
        }
        for ([_]*std.ArrayListUnmanaged([]u8){ &self.edge_boundaries, &self.node_boundaries }, &loaded) |target, *source| {
            std.mem.swap(std.ArrayListUnmanaged([]u8), target, source);
        }
        self.materialized = true;
    }

    pub fn deleteBoundaries(alloc: Allocator, batch: anytype, parent: []const u8) !void {
        for ([_]bool{ false, true }) |nodes| for (0..max_boundaries) |index| {
            const key = try boundaryKeyAlloc(alloc, parent, nodes, index);
            defer alloc.free(key);
            batch.delete(key) catch |err| if (err != error.NotFound) return err;
        };
    }

    pub fn deinit(self: *State, alloc: Allocator) void {
        alloc.free(self.edge_cursor);
        alloc.free(self.node_cursor);
        for (self.edge_boundaries.items) |key| alloc.free(key);
        for (self.node_boundaries.items) |key| alloc.free(key);
        self.edge_boundaries.deinit(alloc);
        self.node_boundaries.deinit(alloc);
        self.* = undefined;
    }

    pub fn identifies(self: State, generation: u64, edges: u64, nodes: u64) bool {
        return self.generation == generation and self.edge_count == edges and self.node_count == nodes;
    }

    pub fn encodeAlloc(self: State, alloc: Allocator) ![]u8 {
        if (self.phase > 2) return error.InvalidGraphMetricPartitionCensus;
        if (self.boundaryCount(false) > max_boundaries or self.boundaryCount(true) > max_boundaries)
            return error.InvalidGraphMetricPartitionCensus;
        var size: usize = header_len + 8;
        for ([_][]const u8{ self.edge_cursor, self.node_cursor }) |key| {
            if (key.len > max_key_bytes) return error.InvalidGraphMetricPartitionCensus;
            size += key.len;
        }
        const raw = try alloc.alloc(u8, size);
        @memset(raw[0..header_len], 0);
        @memcpy(raw[0..4], "GPC2");
        raw[4] = @intFromBool(self.edges_done);
        raw[5] = self.phase;
        for ([_]u64{ self.generation, self.edge_count, self.node_count, self.edges_seen, self.nodes_seen }, 0..) |value, i|
            std.mem.writeInt(u64, raw[8 + i * 8 ..][0..8], value, .little);
        std.mem.writeInt(u16, raw[48..50], @intCast(self.boundaryCount(false)), .little);
        std.mem.writeInt(u16, raw[50..52], @intCast(self.boundaryCount(true)), .little);
        std.mem.writeInt(u32, raw[52..56], @intCast(self.edge_cursor.len), .little);
        std.mem.writeInt(u32, raw[56..60], @intCast(self.node_cursor.len), .little);
        var pos: usize = header_len;
        for ([_][]const u8{ self.edge_cursor, self.node_cursor }) |key| {
            @memcpy(raw[pos..][0..key.len], key);
            pos += key.len;
        }
        std.mem.writeInt(u64, raw[pos..][0..8], std.hash.Wyhash.hash(checksum_seed, raw[0..pos]), .little);
        return raw;
    }

    pub fn decodeAlloc(alloc: Allocator, raw: []const u8) !?State {
        if (raw.len < header_len + 8 or !std.mem.eql(u8, raw[0..4], "GPC2") or raw[4] > 1 or raw[5] > 2 or
            !std.mem.eql(u8, raw[6..8], &.{ 0, 0 })) return null;
        const end = raw.len - 8;
        if (std.hash.Wyhash.hash(checksum_seed, raw[0..end]) != std.mem.readInt(u64, raw[end..][0..8], .little)) return null;
        var state = State{
            .generation = std.mem.readInt(u64, raw[8..16], .little),
            .edge_count = std.mem.readInt(u64, raw[16..24], .little),
            .node_count = std.mem.readInt(u64, raw[24..32], .little),
            .edges_seen = std.mem.readInt(u64, raw[32..40], .little),
            .nodes_seen = std.mem.readInt(u64, raw[40..48], .little),
            .edges_done = raw[4] != 0,
            .phase = raw[5],
            .persisted_edges = std.mem.readInt(u16, raw[48..50], .little),
            .persisted_nodes = std.mem.readInt(u16, raw[50..52], .little),
        };
        var owned = true;
        defer if (owned) state.deinit(alloc);
        if (state.edges_seen > state.edge_count or state.nodes_seen > state.node_count or
            (state.edges_done and state.edges_seen != state.edge_count)) return null;
        var pos: usize = header_len;
        for ([_]*[]u8{ &state.edge_cursor, &state.node_cursor }, [_]usize{ 52, 56 }) |cursor, offset| {
            const len = std.mem.readInt(u32, raw[offset..][0..4], .little);
            if (len > max_key_bytes or len > end - pos) return null;
            cursor.* = try alloc.dupe(u8, raw[pos..][0..len]);
            pos += len;
        }
        if (state.persisted_edges > max_boundaries or state.persisted_nodes > max_boundaries) return null;
        if (pos != end) return null;
        owned = false;
        return state;
    }
};

test "partition census owns bounded checkpoints and rejects corruption" {
    const alloc = std.testing.allocator;
    var state = State{ .generation = 7, .edge_count = 10, .node_count = 4, .edges_seen = 2 };
    defer state.deinit(alloc);
    try state.edge_boundaries.append(alloc, try alloc.dupe(u8, "edge-a"));
    state.edge_cursor = try alloc.dupe(u8, "edge-b");
    const raw = try state.encodeAlloc(alloc);
    defer alloc.free(raw);
    var decoded = (try State.decodeAlloc(alloc, raw)).?;
    defer decoded.deinit(alloc);
    try std.testing.expect(decoded.identifies(7, 10, 4));
    try std.testing.expectEqualStrings("edge-b", decoded.edge_cursor);
    raw[16] ^= 1;
    try std.testing.expect((try State.decodeAlloc(alloc, raw)) == null);
}

test "partition census owns bounded checkpoints without replaying accumulated boundary bytes" {
    const alloc = std.testing.allocator;
    const Store = struct {
        values: std.StringHashMapUnmanaged([]u8) = .empty,
        writes: usize = 0,
        reads: usize = 0,

        fn deinit(self: *@This()) void {
            var entries = self.values.iterator();
            while (entries.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.values.deinit(std.testing.allocator);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const owned = try std.testing.allocator.dupe(u8, value);
            errdefer std.testing.allocator.free(owned);
            const result = try self.values.getOrPut(std.testing.allocator, key);
            if (result.found_existing) std.testing.allocator.free(result.value_ptr.*) else {
                errdefer _ = self.values.remove(key);
                result.key_ptr.* = try std.testing.allocator.dupe(u8, key);
            }
            result.value_ptr.* = owned;
            self.writes += 1;
        }

        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            self.reads += 1;
            return self.values.get(key) orelse error.NotFound;
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            const entry = self.values.fetchRemove(key) orelse return error.NotFound;
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
    };
    var store = Store{};
    defer store.deinit();
    var state = State{ .generation = 7, .edge_count = 1048576, .node_count = 1048577 };
    defer state.deinit(alloc);
    // Legal long relationship names formerly made this progress record 32 MiB.
    const key = try alloc.alloc(u8, 128 * 1024);
    defer alloc.free(key);
    @memset(key, 'a');
    for (0..max_boundaries) |i| {
        std.mem.writeInt(u32, key[key.len - 4 ..][0..4], @intCast(i), .big);
        try state.edge_boundaries.append(alloc, try alloc.dupe(u8, key));
    }
    state.edge_cursor = try alloc.dupe(u8, key);
    try state.persistBoundaries(alloc, &store, "census");
    try std.testing.expectEqual(max_boundaries, store.writes);
    const raw = try state.encodeAlloc(alloc);
    defer alloc.free(raw);
    try std.testing.expectEqual(@as(usize, 128 * 1024 + header_len + 8), raw.len);
    var resumed = (try State.decodeAlloc(alloc, raw)).?;
    defer resumed.deinit(alloc);
    try resumed.persistBoundaries(alloc, &store, "census");
    try std.testing.expectEqual(max_boundaries, store.writes);
    try std.testing.expectEqual(@as(usize, 0), store.reads);
    try std.testing.expectEqual(@as(usize, 0), resumed.edge_boundaries.items.len);
    try resumed.materializeBoundaries(alloc, &store, "census");
    try std.testing.expectEqual(max_boundaries, store.reads);
    for (resumed.edge_boundaries.items, state.edge_boundaries.items) |a, b| try std.testing.expectEqualSlices(u8, a, b);
    try State.deleteBoundaries(alloc, &store, "census");
    try std.testing.expectEqual(@as(usize, 0), store.values.count());
    var missing = (try State.decodeAlloc(alloc, raw)).?;
    defer missing.deinit(alloc);
    try std.testing.expectError(error.InvalidGraphMetricPartitionCensus, missing.materializeBoundaries(alloc, &store, "census"));
}
