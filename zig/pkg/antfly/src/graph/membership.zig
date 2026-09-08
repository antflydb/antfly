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

//! Canonical membership, sealed by completed initialization leaves. Blocks
//! are addressed by row number, so missing output cannot silently omit nodes.
const std = @import("std");
pub const capacity = 256;
pub const Row = struct { node: []const u8, slot: u64 };
pub const Block = struct {
    rows: [capacity]Row = undefined,
    len: usize = 0,
};
const header_len = 16;
const checksum_seed: u64 = 0xA17F_4D45_4D42_0001;

pub fn encodeAlloc(alloc: std.mem.Allocator, rows: []const Row) ![]u8 {
    if (rows.len == 0 or rows.len > capacity) return error.InvalidGraphMetricBuildManifest;
    var size: usize = header_len;
    for (rows, 0..) |row, i| {
        if (row.node.len == 0 or row.node.len > std.math.maxInt(u32) or row.slot == 0 or
            (i != 0 and (row.slot <= rows[i - 1].slot or std.mem.order(u8, rows[i - 1].node, row.node) != .lt)))
            return error.InvalidGraphMetricBuildManifest;
        size = try std.math.add(usize, size, try std.math.add(usize, 12, row.node.len));
    }
    const bytes = try alloc.alloc(u8, size);
    @memcpy(bytes[0..4], "GMB1");
    std.mem.writeInt(u32, bytes[4..8], @intCast(rows.len), .little);
    var pos: usize = header_len;
    for (rows) |row| {
        std.mem.writeInt(u64, bytes[pos..][0..8], row.slot, .little);
        std.mem.writeInt(u32, bytes[pos + 8 ..][0..4], @intCast(row.node.len), .little);
        pos += 12;
        @memcpy(bytes[pos..][0..row.node.len], row.node);
        pos += row.node.len;
    }
    std.mem.writeInt(u64, bytes[8..16], std.hash.Wyhash.hash(checksum_seed, bytes[header_len..]), .little);
    return bytes;
}

/// Rows borrow the transaction's bytes. No per-node decode allocations.
pub fn decode(bytes: []const u8) !Block {
    if (bytes.len < header_len or !std.mem.eql(u8, bytes[0..4], "GMB1")) return error.InvalidGraphMetricBuildManifest;
    const count = std.mem.readInt(u32, bytes[4..8], .little);
    if (count == 0 or count > capacity or std.mem.readInt(u64, bytes[8..16], .little) != std.hash.Wyhash.hash(checksum_seed, bytes[header_len..]))
        return error.InvalidGraphMetricBuildManifest;
    var result = Block{ .len = count };
    var pos: usize = header_len;
    for (result.rows[0..count], 0..) |*row, i| {
        if (bytes.len - pos < 12) return error.InvalidGraphMetricBuildManifest;
        const slot = std.mem.readInt(u64, bytes[pos..][0..8], .little);
        const len = std.mem.readInt(u32, bytes[pos + 8 ..][0..4], .little);
        pos += 12;
        if (slot == 0 or len == 0 or len > bytes.len - pos) return error.InvalidGraphMetricBuildManifest;
        row.* = .{ .slot = slot, .node = bytes[pos..][0..len] };
        if (i != 0 and (slot <= result.rows[i - 1].slot or std.mem.order(u8, result.rows[i - 1].node, row.node) != .lt))
            return error.InvalidGraphMetricBuildManifest;
        pos += len;
    }
    if (pos != bytes.len) return error.InvalidGraphMetricBuildManifest;
    return result;
}

/// Bind framing to the addressed block and the completed leaf's exact count.
/// A valid checksum alone cannot detect a block copied to the wrong key.
pub fn decodeSealed(bytes: []const u8, leaf: u64, block_id: u64, count: u64, lower: []const u8, upper: []const u8) !Block {
    const start = std.math.mul(u64, block_id, capacity) catch return error.InvalidGraphMetricBuildManifest;
    if (start >= count or count > std.math.maxInt(u32) or leaf >= std.math.maxInt(u32)) return error.InvalidGraphMetricBuildManifest;
    const block = try decode(bytes);
    if (block.len != @min(capacity, count - start)) return error.InvalidGraphMetricBuildManifest;
    for (block.rows[0..block.len], 0..) |row, i| {
        if (row.slot != (((leaf + 1) << 32) | (start + i))) return error.InvalidGraphMetricBuildManifest;
        if (std.mem.order(u8, row.node, lower) == .lt or
            (upper.len != 0 and std.mem.order(u8, row.node, upper) != .lt)) return error.InvalidGraphMetricBuildManifest;
    }
    return block;
}

test "graph metric membership blocks own framing and reject omissions or corruption" {
    const alloc = std.testing.allocator;
    const raw = try encodeAlloc(alloc, &.{ .{ .node = "a", .slot = 1 }, .{ .node = "z", .slot = 256 } });
    defer alloc.free(raw);
    const block = try decode(raw);
    try std.testing.expectEqual(@as(usize, 2), block.len);
    try std.testing.expectEqualStrings("z", block.rows[1].node);
    try std.testing.expectEqual(@as(u64, 256), block.rows[1].slot);
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decode(raw[0 .. raw.len - 1]));
    raw[raw.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decode(raw));
}

test "graph metric membership seals reject misplaced blocks and truncated canonical counts" {
    const alloc = std.testing.allocator;
    const raw = try encodeAlloc(alloc, &.{ .{ .node = "a", .slot = @as(u64, 1) << 32 }, .{ .node = "z", .slot = (@as(u64, 1) << 32) + 1 } });
    defer alloc.free(raw);
    _ = try decodeSealed(raw, 0, 0, 2, "", "");
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decodeSealed(raw, 1, 0, 2, "", ""));
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decodeSealed(raw, 0, 1, 258, "", ""));
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decodeSealed(raw, 0, 0, 3, "", ""));
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decodeSealed(raw, 0, 0, 2, "b", ""));
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, decodeSealed(raw, 0, 0, 2, "", "z"));
}
