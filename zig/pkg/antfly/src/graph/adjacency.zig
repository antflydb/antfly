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

//! Durable packing state for one output-vector chunk. Partial tiles and input
//! cursor commit together. A completion receipt selects one immutable attempt.
const std = @import("std");
const ordinal = @import("ordinal.zig");
pub const tile_entries = 256;

pub const Receipt = struct {
    attempt: u64,
    edges: u64,
    pub fn blocks(self: Receipt) u64 {
        return self.edges / tile_entries + @intFromBool(self.edges % tile_entries != 0);
    }
    pub fn encode(self: Receipt) [20]u8 {
        var bytes: [20]u8 = undefined;
        @memcpy(bytes[0..4], "GAR1");
        std.mem.writeInt(u64, bytes[4..12], self.attempt, .little);
        std.mem.writeInt(u64, bytes[12..20], self.edges, .little);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != 20 or !std.mem.eql(u8, bytes[0..4], "GAR1")) return error.InvalidGraphMetricBuildManifest;
        const result = Receipt{ .attempt = std.mem.readInt(u64, bytes[4..12], .little), .edges = std.mem.readInt(u64, bytes[12..20], .little) };
        if (result.attempt == 0) return error.InvalidGraphMetricBuildManifest;
        return result;
    }
};

pub const State = struct {
    attempt: u64,
    blocks: u64 = 0,
    count: usize = 0,
    pending: [tile_entries]ordinal.Edge = undefined,
    /// Borrows the decoded state bytes.
    cursor: []const u8 = "",

    pub fn encode(self: *const State, alloc: std.mem.Allocator) ![]u8 {
        if (self.attempt == 0 or self.count >= tile_entries or self.cursor.len > std.math.maxInt(u32)) return error.InvalidGraphMetricBuildManifest;
        const bytes = try alloc.alloc(u8, 26 + self.count * 16 + self.cursor.len);
        @memcpy(bytes[0..4], "GAP1");
        std.mem.writeInt(u64, bytes[4..12], self.attempt, .little);
        std.mem.writeInt(u64, bytes[12..20], self.blocks, .little);
        std.mem.writeInt(u16, bytes[20..22], @intCast(self.count), .little);
        std.mem.writeInt(u32, bytes[22..26], @intCast(self.cursor.len), .little);
        for (self.pending[0..self.count], 0..) |edge, i| {
            std.mem.writeInt(u64, bytes[26 + i * 16 ..][0..8], edge.source, .little);
            std.mem.writeInt(u64, bytes[34 + i * 16 ..][0..8], edge.target, .little);
        }
        @memcpy(bytes[26 + self.count * 16 ..], self.cursor);
        return bytes;
    }

    pub fn decode(bytes: []const u8) !State {
        if (bytes.len < 26 or !std.mem.eql(u8, bytes[0..4], "GAP1")) return error.InvalidGraphMetricBuildManifest;
        var state = State{ .attempt = std.mem.readInt(u64, bytes[4..12], .little), .blocks = std.mem.readInt(u64, bytes[12..20], .little), .count = std.mem.readInt(u16, bytes[20..22], .little) };
        const cursor_len = std.mem.readInt(u32, bytes[22..26], .little);
        if (state.attempt == 0 or state.count >= tile_entries or bytes.len != 26 + state.count * 16 + @as(usize, cursor_len)) return error.InvalidGraphMetricBuildManifest;
        for (state.pending[0..state.count], 0..) |*edge, i| {
            edge.* = .{ .source = std.mem.readInt(u64, bytes[26 + i * 16 ..][0..8], .little), .target = std.mem.readInt(u64, bytes[34 + i * 16 ..][0..8], .little) };
            if (edge.source == 0 or edge.target == 0) return error.InvalidGraphMetricBuildManifest;
        }
        state.cursor = bytes[26 + state.count * 16 ..];
        return state;
    }
};

test "ordinal blocks adjacency packing state and receipts reject malformed progress" {
    var state = State{ .attempt = 2, .blocks = 3, .count = 1, .cursor = "checkpoint" };
    state.pending[0] = .{ .source = 4, .target = 5 };
    const raw = try state.encode(std.testing.allocator);
    defer std.testing.allocator.free(raw);
    const decoded = try State.decode(raw);
    try std.testing.expectEqual(@as(u64, 3), decoded.blocks);
    try std.testing.expectEqualStrings("checkpoint", decoded.cursor);
    try std.testing.expectEqual(state.pending[0], decoded.pending[0]);
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, State.decode(raw[0 .. raw.len - 1]));
    const receipt = Receipt{ .attempt = 2, .edges = 769 };
    const encoded = receipt.encode();
    try std.testing.expectEqual(@as(u64, 4), (try Receipt.decode(&encoded)).blocks());
}
