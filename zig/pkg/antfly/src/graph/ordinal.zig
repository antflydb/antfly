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

//! Job-local immutable adjacency, bounded fold state, and fixture value records.
//! These bounded blocks use
//! numeric ordinals throughout iteration; document IDs are resolved only when
//! compiling topology or crossing the public score boundary.
const std = @import("std");
pub const max_edges = 4096;
pub const fold_entries = 256;

/// One bounded adjacency fold. Cursor and compensated accumulators commit
/// together; attempt identity prevents a replacement from mixing executions.
pub const Fold = struct {
    attempt: u64 = 0,
    prior: u64 = 0,
    fingerprint: u64 = 0,
    position: u16 = 0,
    count: u16 = 0,
    cursor: []const u8 = "",
    sums: [fold_entries]f64 = @splat(0),
    corrections: [fold_entries]f64 = @splat(0),

    const fixed_len = 36 + fold_entries * 16;

    pub fn encode(self: *const Fold, alloc: std.mem.Allocator) ![]u8 {
        if (self.count > fold_entries or self.position > self.count or self.cursor.len > std.math.maxInt(u32)) return error.InvalidGraphMetricBuildManifest;
        const raw = try alloc.alloc(u8, fixed_len + self.cursor.len);
        errdefer alloc.free(raw);
        @memcpy(raw[0..4], "GOF1");
        std.mem.writeInt(u64, raw[4..12], self.attempt, .little);
        std.mem.writeInt(u64, raw[12..20], self.prior, .little);
        std.mem.writeInt(u64, raw[20..28], self.fingerprint, .little);
        std.mem.writeInt(u16, raw[28..30], self.position, .little);
        std.mem.writeInt(u16, raw[30..32], self.count, .little);
        std.mem.writeInt(u32, raw[32..36], @intCast(self.cursor.len), .little);
        for (self.sums, self.corrections, 0..) |sum, correction, i| {
            if (!std.math.isFinite(sum) or sum < 0 or !std.math.isFinite(correction)) return error.InvalidGraphMetricScore;
            std.mem.writeInt(u64, raw[36 + i * 16 ..][0..8], @bitCast(sum), .little);
            std.mem.writeInt(u64, raw[44 + i * 16 ..][0..8], @bitCast(correction), .little);
        }
        @memcpy(raw[fixed_len..], self.cursor);
        return raw;
    }

    /// The cursor borrows raw; the caller owns its lifetime.
    pub fn decode(raw: []const u8) !Fold {
        if (raw.len < fixed_len or !std.mem.eql(u8, raw[0..4], "GOF1") or std.mem.readInt(u32, raw[32..36], .little) != raw.len - fixed_len) return error.InvalidGraphMetricBuildManifest;
        var result = Fold{
            .attempt = std.mem.readInt(u64, raw[4..12], .little),
            .prior = std.mem.readInt(u64, raw[12..20], .little),
            .fingerprint = std.mem.readInt(u64, raw[20..28], .little),
            .position = std.mem.readInt(u16, raw[28..30], .little),
            .count = std.mem.readInt(u16, raw[30..32], .little),
            .cursor = raw[fixed_len..],
        };
        if (result.count > fold_entries or result.position > result.count) return error.InvalidGraphMetricBuildManifest;
        for (&result.sums, &result.corrections, 0..) |*sum, *correction, i| {
            sum.* = @bitCast(std.mem.readInt(u64, raw[36 + i * 16 ..][0..8], .little));
            correction.* = @bitCast(std.mem.readInt(u64, raw[44 + i * 16 ..][0..8], .little));
            if (!std.math.isFinite(sum.*) or sum.* < 0 or !std.math.isFinite(correction.*)) return error.InvalidGraphMetricScore;
        }
        return result;
    }
};
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
    var fold = Fold{ .attempt = 2, .prior = 256, .count = 2, .position = 1, .cursor = "checkpoint" };
    fold.sums[0] = 1.0e16;
    fold.corrections[0] = 1;
    const state = try fold.encode(alloc);
    defer alloc.free(state);
    const restored = try Fold.decode(state);
    try std.testing.expectEqual(@as(u64, 2), restored.attempt);
    try std.testing.expectEqual(@as(f64, 1), restored.corrections[0]);
    try std.testing.expectEqualStrings("checkpoint", restored.cursor);
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, Fold.decode(state[0 .. state.len - 1]));
    fold.position = 3;
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, fold.encode(alloc));
}
