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

//! Job-local immutable topology and shuffle records. These bounded blocks use
//! numeric ordinals throughout iteration; document IDs are resolved only when
//! compiling topology or crossing the public score boundary.
const std = @import("std");
pub const max_edges = 4096;
pub const Edge = struct { source: u64, target: u64 };
pub const Value = struct {
    ordinal: u64,
    value: f64,
    pub fn lessThan(_: void, a: Value, b: Value) bool {
        return a.ordinal < b.ordinal;
    }
};

pub const Topology = struct {
    edges: []Edge,
    cursor: []u8,
    scanned: u64,
    complete: bool,
    pub fn deinit(self: *Topology, alloc: std.mem.Allocator) void {
        alloc.free(self.edges);
        alloc.free(self.cursor);
        self.* = undefined;
    }
};

pub fn encodeTopology(alloc: std.mem.Allocator, topology: Topology) ![]u8 {
    if (topology.edges.len > max_edges or topology.scanned > max_edges or topology.edges.len > topology.scanned or topology.cursor.len > std.math.maxInt(u32)) return error.InvalidGraphMetricBuildManifest;
    const out = try alloc.alloc(u8, 17 + topology.cursor.len + topology.edges.len * 16);
    @memcpy(out[0..4], "GTO1");
    std.mem.writeInt(u64, out[4..12], topology.scanned, .little);
    out[12] = @intFromBool(topology.complete);
    std.mem.writeInt(u32, out[13..17], @intCast(topology.cursor.len), .little);
    @memcpy(out[17..][0..topology.cursor.len], topology.cursor);
    for (topology.edges, 0..) |edge, i| {
        const offset = 17 + topology.cursor.len + i * 16;
        std.mem.writeInt(u64, out[offset..][0..8], edge.source, .little);
        std.mem.writeInt(u64, out[offset + 8 ..][0..8], edge.target, .little);
    }
    return out;
}

pub fn decodeTopology(alloc: std.mem.Allocator, raw: []const u8) !Topology {
    if (raw.len < 17 or !std.mem.eql(u8, raw[0..4], "GTO1") or raw[12] > 1) return error.InvalidGraphMetricBuildManifest;
    const cursor_len = std.mem.readInt(u32, raw[13..17], .little);
    if (cursor_len > raw.len - 17) return error.InvalidGraphMetricBuildManifest;
    const data = raw[17 + cursor_len ..];
    const scanned = std.mem.readInt(u64, raw[4..12], .little);
    if (data.len % 16 != 0 or data.len / 16 > max_edges or scanned > max_edges or data.len / 16 > scanned) return error.InvalidGraphMetricBuildManifest;
    const edges = try alloc.alloc(Edge, data.len / 16);
    errdefer alloc.free(edges);
    for (edges, 0..) |*edge, i| {
        edge.* = .{ .source = std.mem.readInt(u64, data[i * 16 ..][0..8], .little), .target = std.mem.readInt(u64, data[i * 16 + 8 ..][0..8], .little) };
        if (edge.source == 0 or edge.target == 0) return error.InvalidGraphMetricBuildManifest;
    }
    return .{ .edges = edges, .cursor = try alloc.dupe(u8, raw[17..][0..cursor_len]), .scanned = scanned, .complete = raw[12] == 1 };
}

pub fn encodeValues(alloc: std.mem.Allocator, values: []const Value) ![]u8 {
    if (values.len > max_edges) return error.InvalidGraphMetricBuildManifest;
    const out = try alloc.alloc(u8, values.len * 16);
    errdefer alloc.free(out);
    for (values, 0..) |value, i| {
        if (value.ordinal == 0 or !std.math.isFinite(value.value) or value.value < 0 or (i > 0 and value.ordinal <= values[i - 1].ordinal)) return error.InvalidGraphMetricScore;
        std.mem.writeInt(u64, out[i * 16 ..][0..8], value.ordinal, .little);
        std.mem.writeInt(u64, out[i * 16 + 8 ..][0..8], @bitCast(value.value), .little);
    }
    return out;
}

pub fn decodeValues(alloc: std.mem.Allocator, raw: []const u8) ![]Value {
    if (raw.len % 16 != 0 or raw.len / 16 > max_edges) return error.InvalidGraphMetricBuildManifest;
    const values = try alloc.alloc(Value, raw.len / 16);
    errdefer alloc.free(values);
    for (values, 0..) |*value, i| {
        value.* = .{ .ordinal = std.mem.readInt(u64, raw[i * 16 ..][0..8], .little), .value = @bitCast(std.mem.readInt(u64, raw[i * 16 + 8 ..][0..8], .little)) };
        if (value.ordinal == 0 or !std.math.isFinite(value.value) or value.value < 0 or (i > 0 and value.ordinal <= values[i - 1].ordinal)) return error.InvalidGraphMetricScore;
    }
    return values;
}

test "ordinal blocks round trip and reject malformed input" {
    const alloc = std.testing.allocator;
    const encoded = try encodeValues(alloc, &.{ .{ .ordinal = 1, .value = 0 }, .{ .ordinal = 900, .value = 0.7 } });
    defer alloc.free(encoded);
    const values = try decodeValues(alloc, encoded);
    defer alloc.free(values);
    try std.testing.expectEqual(@as(u64, 900), values[1].ordinal);
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decodeValues(alloc, encoded[1..]));
    try std.testing.expectError(error.InvalidGraphMetricScore, encodeValues(alloc, &.{.{ .ordinal = 1, .value = std.math.nan(f64) }}));
    var edges = [_]Edge{.{ .source = 9, .target = 4 }};
    var cursor = [_]u8{ 0, 255 };
    const bytes = try encodeTopology(alloc, .{ .edges = &edges, .cursor = &cursor, .scanned = 2, .complete = false });
    defer alloc.free(bytes);
    var decoded = try decodeTopology(alloc, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(u8, &cursor, decoded.cursor);
    try std.testing.expectEqual(@as(u64, 9), decoded.edges[0].source);
    try std.testing.expect(!decoded.complete);
}
