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

const std = @import("std");
const Allocator = std.mem.Allocator;

const magic = "GEC1";
const header_len = magic.len + @sizeOf(u64) + 3 * @sizeOf(u32);
const bulk_scan_min_affected_edges: usize = 256;

pub const View = struct {
    source_priority: usize,
    edge_key: []const u8,
    state_key: []const u8,
    payload: []const u8,
};

/// Contender records are deliberately one-record-per-(edge, source-state).
/// Document-local membership records use an empty payload; globally keyed
/// records retain the edge payload used for winner selection. Updating a
/// source therefore writes only the identities that source changed.
pub fn encodeAlloc(
    alloc: Allocator,
    generation: u64,
    source_priority: usize,
    edge_key: []const u8,
    state_key: []const u8,
    payload: []const u8,
) ![]u8 {
    if (source_priority > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
    if (edge_key.len > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
    if (state_key.len > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
    var total_len = std.math.add(usize, header_len, edge_key.len) catch return error.ResourceLimitExceeded;
    total_len = std.math.add(usize, total_len, state_key.len) catch return error.ResourceLimitExceeded;
    total_len = std.math.add(usize, total_len, payload.len) catch return error.ResourceLimitExceeded;
    const out = try alloc.alloc(u8, total_len);
    @memcpy(out[0..magic.len], magic);
    std.mem.writeInt(u64, out[magic.len..][0..@sizeOf(u64)], generation, .big);
    std.mem.writeInt(u32, out[magic.len + @sizeOf(u64) ..][0..@sizeOf(u32)], @intCast(source_priority), .big);
    std.mem.writeInt(u32, out[magic.len + @sizeOf(u64) + @sizeOf(u32) ..][0..@sizeOf(u32)], @intCast(edge_key.len), .big);
    std.mem.writeInt(u32, out[magic.len + @sizeOf(u64) + 2 * @sizeOf(u32) ..][0..@sizeOf(u32)], @intCast(state_key.len), .big);
    @memcpy(out[header_len..][0..edge_key.len], edge_key);
    @memcpy(out[header_len + edge_key.len ..][0..state_key.len], state_key);
    @memcpy(out[header_len + edge_key.len + state_key.len ..], payload);
    return out;
}

/// Returns null for another index incarnation. Malformed records fail closed
/// so silent corruption cannot change source precedence.
pub fn decode(raw: []const u8, expected_generation: u64) !?View {
    if (raw.len < header_len or !std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidGraphEdgeContender;
    const generation = std.mem.readInt(u64, raw[magic.len..][0..@sizeOf(u64)], .big);
    if (generation != expected_generation) return null;
    const priority = std.mem.readInt(u32, raw[magic.len + @sizeOf(u64) ..][0..@sizeOf(u32)], .big);
    const edge_key_len = std.mem.readInt(u32, raw[magic.len + @sizeOf(u64) + @sizeOf(u32) ..][0..@sizeOf(u32)], .big);
    const state_key_len = std.mem.readInt(u32, raw[magic.len + @sizeOf(u64) + 2 * @sizeOf(u32) ..][0..@sizeOf(u32)], .big);
    if (edge_key_len > raw.len - header_len) return error.InvalidGraphEdgeContender;
    const state_start = header_len + edge_key_len;
    if (state_key_len > raw.len - state_start) return error.InvalidGraphEdgeContender;
    const payload_start = state_start + state_key_len;
    return .{
        .source_priority = priority,
        .edge_key = raw[header_len..state_start],
        .state_key = raw[state_start..payload_start],
        .payload = raw[payload_start..],
    };
}

pub fn encodeVisibleCount(generation: u64, count: usize) ![magic.len + @sizeOf(u64) + @sizeOf(u64)]u8 {
    const count_u64 = std.math.cast(u64, count) orelse return error.ResourceLimitExceeded;
    var out: [magic.len + @sizeOf(u64) + @sizeOf(u64)]u8 = undefined;
    @memcpy(out[0..magic.len], magic);
    std.mem.writeInt(u64, out[magic.len..][0..@sizeOf(u64)], generation, .big);
    std.mem.writeInt(u64, out[magic.len + @sizeOf(u64) ..][0..@sizeOf(u64)], count_u64, .big);
    return out;
}

pub fn decodeVisibleCount(raw: []const u8, expected_generation: u64) !?usize {
    if (raw.len != magic.len + @sizeOf(u64) + @sizeOf(u64) or !std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidGraphEdgeContenderCount;
    const generation = std.mem.readInt(u64, raw[magic.len..][0..@sizeOf(u64)], .big);
    if (generation != expected_generation) return null;
    return std.math.cast(usize, std.mem.readInt(u64, raw[magic.len + @sizeOf(u64) ..][0..@sizeOf(u64)], .big)) orelse error.ResourceLimitExceeded;
}

/// Range-scan the contender index when a mutation touches a meaningful share
/// of the visible graph. Small updates retain per-edge seeks; bulk replacement
/// pays for one sequential scan instead of thousands of independent seeks.
pub fn shouldBulkScan(affected_edges: usize, visible_edges: usize) bool {
    if (affected_edges < bulk_scan_min_affected_edges) return false;
    const one_eighth_visible = visible_edges / 8 + @intFromBool(visible_edges % 8 != 0);
    return affected_edges >= one_eighth_visible;
}

test "graph edge contender and visible count are generation fenced" {
    const alloc = std.testing.allocator;
    const raw = try encodeAlloc(alloc, 42, 3, "edge key", "state key", "edge payload");
    defer alloc.free(raw);
    const view = (try decode(raw, 42)).?;
    try std.testing.expectEqual(@as(usize, 3), view.source_priority);
    try std.testing.expectEqualStrings("edge key", view.edge_key);
    try std.testing.expectEqualStrings("state key", view.state_key);
    try std.testing.expectEqualStrings("edge payload", view.payload);
    try std.testing.expect((try decode(raw, 41)) == null);

    const membership = try encodeAlloc(alloc, 42, 3, "edge key", "state key", "");
    defer alloc.free(membership);
    try std.testing.expectEqual(@as(usize, 0), (try decode(membership, 42)).?.payload.len);

    const count = try encodeVisibleCount(42, 17);
    try std.testing.expectEqual(@as(?usize, 17), try decodeVisibleCount(&count, 42));
    try std.testing.expect((try decodeVisibleCount(&count, 41)) == null);
    try std.testing.expect(!shouldBulkScan(255, 1_000));
    try std.testing.expect(shouldBulkScan(256, 2_048));
    try std.testing.expect(!shouldBulkScan(256, 4_096));
}
