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
    edge_cursor: []u8 = &.{},
    node_cursor: []u8 = &.{},
    edge_boundaries: std.ArrayListUnmanaged([]u8) = .empty,
    node_boundaries: std.ArrayListUnmanaged([]u8) = .empty,

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
        if (self.edge_boundaries.items.len > max_boundaries or self.node_boundaries.items.len > max_boundaries)
            return error.InvalidGraphMetricPartitionCensus;
        var size: usize = header_len + 8;
        for ([_][]const u8{ self.edge_cursor, self.node_cursor }) |key| {
            if (key.len > max_key_bytes) return error.InvalidGraphMetricPartitionCensus;
            size += key.len;
        }
        for ([_][]const []u8{ self.edge_boundaries.items, self.node_boundaries.items }) |keys| for (keys) |key| {
            if (key.len == 0 or key.len > max_key_bytes) return error.InvalidGraphMetricPartitionCensus;
            size += 4 + key.len;
        };
        const raw = try alloc.alloc(u8, size);
        @memset(raw[0..header_len], 0);
        @memcpy(raw[0..4], "GPC1");
        raw[4] = @intFromBool(self.edges_done);
        for ([_]u64{ self.generation, self.edge_count, self.node_count, self.edges_seen, self.nodes_seen }, 0..) |value, i|
            std.mem.writeInt(u64, raw[8 + i * 8 ..][0..8], value, .little);
        std.mem.writeInt(u16, raw[48..50], @intCast(self.edge_boundaries.items.len), .little);
        std.mem.writeInt(u16, raw[50..52], @intCast(self.node_boundaries.items.len), .little);
        std.mem.writeInt(u32, raw[52..56], @intCast(self.edge_cursor.len), .little);
        std.mem.writeInt(u32, raw[56..60], @intCast(self.node_cursor.len), .little);
        var pos: usize = header_len;
        for ([_][]const u8{ self.edge_cursor, self.node_cursor }) |key| {
            @memcpy(raw[pos..][0..key.len], key);
            pos += key.len;
        }
        for ([_][]const []u8{ self.edge_boundaries.items, self.node_boundaries.items }) |keys| for (keys) |key| {
            std.mem.writeInt(u32, raw[pos..][0..4], @intCast(key.len), .little);
            pos += 4;
            @memcpy(raw[pos..][0..key.len], key);
            pos += key.len;
        };
        std.mem.writeInt(u64, raw[pos..][0..8], std.hash.Wyhash.hash(checksum_seed, raw[0..pos]), .little);
        return raw;
    }

    pub fn decodeAlloc(alloc: Allocator, raw: []const u8) !?State {
        if (raw.len < header_len + 8 or !std.mem.eql(u8, raw[0..4], "GPC1") or raw[4] > 1 or
            !std.mem.eql(u8, raw[5..8], &.{ 0, 0, 0 })) return null;
        const end = raw.len - 8;
        if (std.hash.Wyhash.hash(checksum_seed, raw[0..end]) != std.mem.readInt(u64, raw[end..][0..8], .little)) return null;
        var state = State{
            .generation = std.mem.readInt(u64, raw[8..16], .little),
            .edge_count = std.mem.readInt(u64, raw[16..24], .little),
            .node_count = std.mem.readInt(u64, raw[24..32], .little),
            .edges_seen = std.mem.readInt(u64, raw[32..40], .little),
            .nodes_seen = std.mem.readInt(u64, raw[40..48], .little),
            .edges_done = raw[4] != 0,
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
        for ([_]*std.ArrayListUnmanaged([]u8){ &state.edge_boundaries, &state.node_boundaries }, [_]usize{ 48, 50 }) |keys, offset| {
            const count = std.mem.readInt(u16, raw[offset..][0..2], .little);
            if (count > max_boundaries) return null;
            for (0..count) |_| {
                if (end - pos < 4) return null;
                const len = std.mem.readInt(u32, raw[pos..][0..4], .little);
                pos += 4;
                if (len == 0 or len > max_key_bytes or len > end - pos) return null;
                const key = raw[pos..][0..len];
                if (keys.items.len > 0 and std.mem.order(u8, keys.items[keys.items.len - 1], key) != .lt) return null;
                try keys.ensureUnusedCapacity(alloc, 1);
                keys.appendAssumeCapacity(try alloc.dupe(u8, key));
                pos += len;
            }
        }
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
