// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Fixed ordinal vector blocks. Presence is separate from value: a missing
//! producer output must never silently become a valid zero score.
const std = @import("std");

pub const entries = 256;
const bitmap_bytes = entries / 8;
pub const encoded_len = bitmap_bytes + entries * @sizeOf(f64);
pub const Chunk = [encoded_len]u8;

pub fn put(chunk: *Chunk, slot: usize, value: f64) !void {
    if (slot >= entries or !std.math.isFinite(value) or value < 0) return error.InvalidGraphMetricScore;
    chunk[slot / 8] |= @as(u8, 1) << @intCast(slot % 8);
    std.mem.writeInt(u64, chunk[bitmap_bytes + slot * 8 ..][0..8], @bitCast(value), .little);
}

pub fn get(chunk: []const u8, slot: usize, required: bool) !f64 {
    if (chunk.len != encoded_len or slot >= entries) return error.InvalidGraphMetricScore;
    if (chunk[slot / 8] & (@as(u8, 1) << @intCast(slot % 8)) == 0) {
        if (required) return error.InvalidGraphMetricScore;
        return 0;
    }
    const value: f64 = @bitCast(std.mem.readInt(u64, chunk[bitmap_bytes + slot * 8 ..][0..8], .little));
    if (!std.math.isFinite(value) or value < 0) return error.InvalidGraphMetricScore;
    return value;
}

test "graph metric vector chunks distinguish missing from zero and reject malformed values" {
    var chunk: Chunk = @splat(0);
    try std.testing.expectError(error.InvalidGraphMetricScore, get(&chunk, 0, true));
    try put(&chunk, 0, 0);
    try put(&chunk, entries - 1, 0.5);
    try std.testing.expectEqual(@as(f64, 0), try get(&chunk, 0, true));
    try std.testing.expectEqual(@as(f64, 0.5), try get(&chunk, entries - 1, true));
    try std.testing.expectError(error.InvalidGraphMetricScore, put(&chunk, 2, std.math.nan(f64)));
    try std.testing.expectError(error.InvalidGraphMetricScore, get(chunk[0..10], 0, true));
}
